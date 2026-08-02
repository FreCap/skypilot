"""Effect-free renderer for the frozen SkyServe Kubernetes object contract.

This module is the sole filesystem reader in the renderer pipeline.  It reads
five exact, content-addressed package artifacts and otherwise performs only
typed, deterministic transformations.  It deliberately imports no provider,
database, Kubernetes-client, configuration, credential, or environment code.
"""

from __future__ import annotations

import dataclasses
import dis
import hashlib
import json
import marshal
import os
import re
import stat
import sys
import types
from typing import Any, ClassVar, TypeVar

import sky as sky_package
from sky.serve import resource_action_provider_artifacts as provider_artifacts
from sky.serve import resource_actions as actions

_ARTIFACT_DIRECTORY = (
    'sky/serve/resource_action_artifacts/kubernetes_renderer_v1')
_ARTIFACT_PATHS: tuple[tuple[str, str], ...] = (
    ('outer_template', f'{_ARTIFACT_DIRECTORY}/outer_template.json'),
    ('node_fragment', f'{_ARTIFACT_DIRECTORY}/node_fragment.json'),
    ('binding_schema', f'{_ARTIFACT_DIRECTORY}/binding_schema.json'),
    ('config_access_inventory',
     f'{_ARTIFACT_DIRECTORY}/config_access_inventory.json'),
    ('admitted_object_normalization',
     f'{_ARTIFACT_DIRECTORY}/admitted_object_normalization.json'),
)
_MAX_ARTIFACT_BYTES = 65_536
_EXPLICIT_USER_LABEL_RE = re.compile(r'^[a-z0-9](?:[a-z0-9_-]{0,61}[a-z0-9])?$')
_SELECTOR_KEYS = (
    'component',
    'skypilot-cluster-name',
    'skypilot.co/cluster-record-uuid',
    'skypilot.co/serve-replica-incarnation',
)
_IDENTITY_LABEL_KEYS = (
    'skypilot-cluster-name',
    'skypilot.co/cluster-record-uuid',
    'skypilot.co/serve-replica-incarnation',
)
_EXPECTED_INVENTORIED_EXECUTABLE_SHA256_BY_PYTHON = (
    ((3, 10),
     '4ed13299d299b6472f730ae3a6e2621fd15c2922d23b5c1315bfa29b272d3f5a'),
    ((3, 11),
     '632c2d0f65fc0e0d05d86f96105a9c2ef8d7404ab8bc51e57362874cf0b88df7'),
    ((3, 12),
     '0a77cca9c96e079cf36baf2ef6b1ccbbe91cb76037c0a4170c9e8448957f3c32'),
    ((3, 13),
     '9ef7bed2ba3ee6b38545abee51b0befb74486940554ce76c06161b66038da97b'),
    ((3, 14),
     '12b50242afa886269c0156d59d6d87567e21259f7c3b2d8851e10fce14c6d988'),
)
_ArtifactDocumentT = TypeVar('_ArtifactDocumentT',
                             bound='_ExactRendererArtifactDocumentV1')


@dataclasses.dataclass(frozen=True, init=False)
class _ExactRendererArtifactDocumentV1:
    """One immutable parsed artifact with a code-frozen semantic document."""

    _canonical_bytes: bytes = dataclasses.field(repr=False)

    _EXPECTED_SHA256: ClassVar[str]
    _SCHEMA: ClassVar[str]

    def __init__(self, value: Any) -> None:
        if type(value) is not dict:
            raise TypeError('renderer artifact document must be an exact '
                            'object.')
        canonical_bytes = actions.canonical_json_bytes(value)
        if hashlib.sha256(canonical_bytes).hexdigest() != self._EXPECTED_SHA256:
            raise ValueError('renderer artifact document is not the exact '
                             f'{self._SCHEMA} contract.')
        object.__setattr__(self, '_canonical_bytes', canonical_bytes)

    @classmethod
    def from_value(cls: type[_ArtifactDocumentT],
                   value: Any) -> _ArtifactDocumentT:
        return cls(value)

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def canonical_value(self) -> dict[str, Any]:
        value = json.loads(self._canonical_bytes.decode('utf-8'))
        if type(value) is not dict:
            raise ValueError('renderer artifact lost its object root.')
        return value


class ProviderKubernetesOuterTemplateArtifactV1(_ExactRendererArtifactDocumentV1
                                               ):
    """The exact two-Service outer renderer artifact."""

    _EXPECTED_SHA256 = (
        '94d36f4cb7691199681be929dbb32ec314339cc4785078b9029c06b9a285a5b8')
    _SCHEMA = 'skypilot.serve.prebooted-direct-pod.outer-template.v1'


class ProviderKubernetesNodeFragmentArtifactV1(_ExactRendererArtifactDocumentV1
                                              ):
    """The exact direct-Pod node fragment artifact."""

    _EXPECTED_SHA256 = (
        'efd826b4ced04e58e9ac8a29342659a941de09a53fa099083da29ad6eb49e171')
    _SCHEMA = 'skypilot.serve.prebooted-direct-pod.node-fragment.v1'


class ProviderKubernetesBindingSchemaArtifactV1(_ExactRendererArtifactDocumentV1
                                               ):
    """The exact 17-entry renderer binding schema."""

    _EXPECTED_SHA256 = (
        'fa09e949696be4c2b25b1bb8c301ba8813be55b7ecf1bb01697a009cc0ecc5f4')
    _SCHEMA = 'skypilot.serve.prebooted-direct-pod.bindings.v1'


class ProviderKubernetesConfigAccessInventoryV1(_ExactRendererArtifactDocumentV1
                                               ):
    """The exact renderer access, call, transient, and effect inventory."""

    _EXPECTED_SHA256 = (
        '435daf1effa011f485eaa402cda4227b0d108d0559520fc9fd014a65057a6700')
    _SCHEMA = ('skypilot.serve.prebooted-direct-pod.config-access-inventory.v1')


@dataclasses.dataclass(frozen=True)
class ResolvedProviderKubernetesOuterTemplateArtifactV1:
    """Verified raw and parsed preimages for the outer template."""

    artifact_ref: actions.ProviderRepoArtifactRefV1
    raw_artifact: provider_artifacts.RawCanonicalRendererArtifactBytesV1
    template: ProviderKubernetesOuterTemplateArtifactV1

    def __post_init__(self) -> None:
        if type(self.artifact_ref) is not actions.ProviderRepoArtifactRefV1:
            raise TypeError('outer-template artifact reference has an invalid '
                            'type.')
        if type(self.raw_artifact
               ) is not provider_artifacts.RawCanonicalRendererArtifactBytesV1:
            raise TypeError('outer-template raw artifact has an invalid type.')
        if type(self.template) is not ProviderKubernetesOuterTemplateArtifactV1:
            raise TypeError('outer-template parsed artifact has an invalid '
                            'type.')
        if (self.raw_artifact.artifact_ref.canonical_bytes
                != self.artifact_ref.canonical_bytes or
                self.raw_artifact.raw_bytes[:-1]
                != self.template.canonical_bytes):
            raise ValueError('outer-template artifact preimages disagree.')


@dataclasses.dataclass(frozen=True)
class ResolvedProviderKubernetesNodeFragmentArtifactV1:
    """Verified raw and parsed preimages for the Pod node fragment."""

    artifact_ref: actions.ProviderRepoArtifactRefV1
    raw_artifact: provider_artifacts.RawCanonicalRendererArtifactBytesV1
    fragment: ProviderKubernetesNodeFragmentArtifactV1

    def __post_init__(self) -> None:
        if type(self.artifact_ref) is not actions.ProviderRepoArtifactRefV1:
            raise TypeError('node-fragment artifact reference has an invalid '
                            'type.')
        if type(self.raw_artifact
               ) is not provider_artifacts.RawCanonicalRendererArtifactBytesV1:
            raise TypeError('node-fragment raw artifact has an invalid type.')
        if type(self.fragment) is not ProviderKubernetesNodeFragmentArtifactV1:
            raise TypeError('node-fragment parsed artifact has an invalid '
                            'type.')
        if (self.raw_artifact.artifact_ref.canonical_bytes
                != self.artifact_ref.canonical_bytes or
                self.raw_artifact.raw_bytes[:-1]
                != self.fragment.canonical_bytes):
            raise ValueError('node-fragment artifact preimages disagree.')


@dataclasses.dataclass(frozen=True)
class ResolvedProviderKubernetesBindingSchemaArtifactV1:
    """Verified raw and parsed preimages for the binding schema."""

    artifact_ref: actions.ProviderRepoArtifactRefV1
    raw_artifact: provider_artifacts.RawCanonicalRendererArtifactBytesV1
    schema: ProviderKubernetesBindingSchemaArtifactV1

    def __post_init__(self) -> None:
        if type(self.artifact_ref) is not actions.ProviderRepoArtifactRefV1:
            raise TypeError('binding-schema artifact reference has an invalid '
                            'type.')
        if type(self.raw_artifact
               ) is not provider_artifacts.RawCanonicalRendererArtifactBytesV1:
            raise TypeError('binding-schema raw artifact has an invalid type.')
        if type(self.schema) is not ProviderKubernetesBindingSchemaArtifactV1:
            raise TypeError('binding-schema parsed artifact has an invalid '
                            'type.')
        if (self.raw_artifact.artifact_ref.canonical_bytes
                != self.artifact_ref.canonical_bytes or
                self.raw_artifact.raw_bytes[:-1]
                != self.schema.canonical_bytes):
            raise ValueError('binding-schema artifact preimages disagree.')


@dataclasses.dataclass(frozen=True)
class ResolvedProviderKubernetesConfigAccessInventoryArtifactV1:
    """Verified raw and parsed preimages for the access inventory."""

    artifact_ref: actions.ProviderRepoArtifactRefV1
    raw_artifact: provider_artifacts.RawCanonicalRendererArtifactBytesV1
    inventory: ProviderKubernetesConfigAccessInventoryV1

    def __post_init__(self) -> None:
        if type(self.artifact_ref) is not actions.ProviderRepoArtifactRefV1:
            raise TypeError('config-inventory artifact reference has an '
                            'invalid type.')
        if type(self.raw_artifact
               ) is not provider_artifacts.RawCanonicalRendererArtifactBytesV1:
            raise TypeError(
                'config-inventory raw artifact has an invalid type.')
        if type(self.inventory
               ) is not ProviderKubernetesConfigAccessInventoryV1:
            raise TypeError('config-inventory parsed artifact has an invalid '
                            'type.')
        if (self.raw_artifact.artifact_ref.canonical_bytes
                != self.artifact_ref.canonical_bytes or
                self.raw_artifact.raw_bytes[:-1]
                != self.inventory.canonical_bytes):
            raise ValueError('config-inventory artifact preimages disagree.')


@dataclasses.dataclass(frozen=True)
class ResolvedProviderKubernetesRendererArtifactSetV1:
    """The five exact typed renderer artifacts in canonical role order."""

    outer_template: ResolvedProviderKubernetesOuterTemplateArtifactV1
    node_fragment: ResolvedProviderKubernetesNodeFragmentArtifactV1
    binding_schema: ResolvedProviderKubernetesBindingSchemaArtifactV1
    config_access_inventory: (
        ResolvedProviderKubernetesConfigAccessInventoryArtifactV1)
    admitted_object_normalization: (
        provider_artifacts.ResolvedProviderKubernetesNormalizationArtifactV1)

    def __post_init__(self) -> None:
        expected_types = (
            ('outer_template',
             ResolvedProviderKubernetesOuterTemplateArtifactV1),
            ('node_fragment', ResolvedProviderKubernetesNodeFragmentArtifactV1),
            ('binding_schema',
             ResolvedProviderKubernetesBindingSchemaArtifactV1),
            ('config_access_inventory',
             ResolvedProviderKubernetesConfigAccessInventoryArtifactV1),
            ('admitted_object_normalization', provider_artifacts.
             ResolvedProviderKubernetesNormalizationArtifactV1),
        )
        for field, expected_type in expected_types:
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'resolved renderer artifact {field} has an '
                                'invalid type.')
        actual_refs = (
            self.outer_template.artifact_ref,
            self.node_fragment.artifact_ref,
            self.binding_schema.artifact_ref,
            self.config_access_inventory.artifact_ref,
            self.admitted_object_normalization.artifact_ref,
        )
        for (role, expected_path), artifact_ref in zip(_ARTIFACT_PATHS,
                                                       actual_refs):
            if artifact_ref.repo_path != expected_path:
                raise ValueError(f'resolved renderer artifact {role} has an '
                                 'unexpected repository path.')


def validate_provider_kubernetes_renderer_input_v1(
    renderer_input: actions.ProviderKubernetesRendererInputV1,
) -> actions.ProviderKubernetesRendererInputV1:
    """Revalidate the closed renderer root and every independent identity."""

    if type(renderer_input) is not actions.ProviderKubernetesRendererInputV1:
        raise TypeError('Kubernetes renderer input has an invalid type.')

    name_basis = renderer_input.name_basis
    resource_identity = renderer_input.resource_identity
    retained_source = renderer_input.retained_source
    run_source = renderer_input.seed.post_provision.job_submission.run_source
    renderer_source = renderer_input.seed.renderer.source
    request_identity = renderer_input.seed.request_identity
    topology_objects = renderer_input.seed.topology.mutable_objects
    sky_cluster_name = renderer_input.sky_cluster_name
    sky_cluster_record_uuid = renderer_input.sky_cluster_record_uuid

    expected_basis = actions.ProviderWorkloadNameBasisV1(
        version=1,
        display_name=sky_cluster_name,
        frozen_user_hash=request_identity.frozen_user_hash,
        max_length=42,
        cluster_name_hash_length=8)
    if (name_basis.canonical_bytes != expected_basis.canonical_bytes or
            sky_cluster_name != name_basis.display_name):
        raise ValueError('Kubernetes renderer name basis does not match its '
                         'independent input copies.')
    if (retained_source.canonical_bytes != renderer_source.canonical_bytes or
            retained_source.canonical_bytes != run_source.canonical_bytes):
        raise ValueError('Kubernetes renderer retained source copies are not '
                         'byte-equal.')
    if retained_source.service_incarnation != resource_identity.service_incarnation:
        raise ValueError('Kubernetes renderer service incarnation does not '
                         'match its retained source.')

    if not request_identity.original_user.isascii():
        raise ValueError('Kubernetes renderer original user must be ASCII.')
    cleaned_user = request_identity.original_user.lower()
    cleaned_user = re.sub(r'[^a-z0-9-_]', '', cleaned_user)
    cleaned_user = re.sub(r'^[0-9-]+', '', cleaned_user)
    cleaned_user = re.sub(r'-$', '', cleaned_user)
    cleaned_user = cleaned_user[:63]
    if _EXPLICIT_USER_LABEL_RE.fullmatch(cleaned_user) is None:
        raise ValueError('Kubernetes renderer original user does not project '
                         'to a canonical label value.')
    projected_request_identity = actions.ProviderKubernetesRequestIdentityV1(
        cleaned_user=cleaned_user,
        original_user=request_identity.original_user,
        frozen_user_hash=name_basis.frozen_user_hash)
    if (projected_request_identity.cleaned_user != request_identity.cleaned_user
            or projected_request_identity.original_user
            != request_identity.original_user or
            projected_request_identity.frozen_user_hash
            != request_identity.frozen_user_hash):
        raise ValueError('Kubernetes renderer request identity is not the '
                         'frozen explicit-user projection.')

    provider_cluster_name = name_basis.provider_cluster_name
    workload_name = name_basis.workload_name
    cluster_uuid = str(sky_cluster_record_uuid)
    replica_uuid = str(resource_identity.replica_incarnation)
    expected_topology = (
        (actions.ProviderObjectRoleV1.HEAD_SSH_SERVICE, f'{workload_name}-ssh',
         {
             'service-role': 'head_ssh_service',
             'skypilot-cluster-name': provider_cluster_name,
             'skypilot-user': request_identity.cleaned_user,
             'skypilot.co/cluster-record-uuid': cluster_uuid,
             'skypilot.co/serve-replica-incarnation': replica_uuid,
         }),
        (actions.ProviderObjectRoleV1.HEAD_SERVICE, workload_name, {
            'service-role': 'head_service',
            'skypilot-cluster-name': provider_cluster_name,
            'skypilot-user': request_identity.cleaned_user,
            'skypilot.co/cluster-record-uuid': cluster_uuid,
            'skypilot.co/serve-replica-incarnation': replica_uuid,
        }),
        (actions.ProviderObjectRoleV1.HEAD_POD, workload_name, {
            'component': workload_name,
            'skypilot-cluster-name': provider_cluster_name,
            'skypilot-user': request_identity.cleaned_user,
            'skypilot.co/cluster-record-uuid': cluster_uuid,
            'skypilot.co/serve-replica-incarnation': replica_uuid,
        }),
    )
    actual_topology = tuple((item.role, item.name, {
        label.key: label.value for label in item.labels
    }) for item in topology_objects)
    if actual_topology != expected_topology:
        raise ValueError('Kubernetes renderer topology is not bound to its '
                         'independent name and identity copies.')
    return renderer_input


def resolve_provider_kubernetes_renderer_artifacts_v1(
    renderer_input: actions.ProviderKubernetesRendererInputV1,
) -> ResolvedProviderKubernetesRendererArtifactSetV1:
    """Descriptor-safely resolve the five role-exact installed artifacts."""

    if type(renderer_input) is not actions.ProviderKubernetesRendererInputV1:
        raise TypeError('Kubernetes renderer input has an invalid type.')
    package_init = sky_package.__file__
    if (type(package_init) is not str or not os.path.isabs(package_init) or
            os.path.basename(package_init) != '__init__.py'):
        raise ValueError('the imported sky package location is not regular.')
    package_directory = os.path.dirname(package_init)
    if os.path.basename(package_directory) != 'sky':
        raise ValueError('the imported sky package location is not top-level.')
    distribution_root = os.path.dirname(package_directory)
    required_flags = ('O_CLOEXEC', 'O_DIRECTORY', 'O_NOFOLLOW', 'O_NONBLOCK')
    if any(not hasattr(os, flag) for flag in required_flags):
        raise RuntimeError(
            'descriptor-safe artifact resolution is unsupported.')
    read_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    nonblocking_read_flags = read_flags | os.O_NONBLOCK
    directory_flags = read_flags | os.O_DIRECTORY

    renderer = renderer_input.seed.renderer
    refs = (
        renderer.outer_template,
        renderer.node_fragment,
        renderer.binding_schema,
        renderer.config_access_inventory,
        renderer.admitted_object_normalization,
    )
    raw_artifacts: dict[
        str, provider_artifacts.RawCanonicalRendererArtifactBytesV1] = {}
    raw_bytes_by_role: dict[str, bytes] = {}
    root_fd = -1
    package_fd = -1
    package_init_fd = -1
    try:
        root_fd = os.open(distribution_root, directory_flags)
        package_fd = os.open('sky', directory_flags, dir_fd=root_fd)
        absolute_package_fd = os.open(package_directory, directory_flags)
        try:
            package_stat = os.fstat(package_fd)
            absolute_package_stat = os.fstat(absolute_package_fd)
            if ((package_stat.st_dev, package_stat.st_ino)
                    != (absolute_package_stat.st_dev,
                        absolute_package_stat.st_ino)):
                raise ValueError('the imported sky package is not bound to '
                                 'the opened distribution root.')
        finally:
            os.close(absolute_package_fd)
        package_init_fd = os.open('__init__.py',
                                  nonblocking_read_flags,
                                  dir_fd=package_fd)
        package_init_stat = os.fstat(package_init_fd)
        if not stat.S_ISREG(package_init_stat.st_mode):
            raise ValueError('the imported sky package initializer is not a '
                             'regular file.')
        for (role, expected_path), artifact_ref in zip(_ARTIFACT_PATHS, refs):
            if type(artifact_ref) is not actions.ProviderRepoArtifactRefV1:
                raise TypeError(f'renderer artifact {role} reference has an '
                                'invalid type.')
            if artifact_ref.repo_path != expected_path:
                raise ValueError(f'renderer artifact {role} path is not exact.')
            if artifact_ref.byte_size > _MAX_ARTIFACT_BYTES:
                raise ValueError(f'renderer artifact {role} exceeds its byte '
                                 'bound.')
            current_fd = os.dup(package_fd)
            file_fd = -1
            try:
                path_segments = expected_path.split('/')
                if not path_segments or path_segments[0] != 'sky':
                    raise ValueError('renderer artifact path is not rooted in '
                                     'the imported sky package.')
                relative_segments = path_segments[1:]
                for index, segment in enumerate(relative_segments):
                    is_last = index == len(relative_segments) - 1
                    next_fd = os.open(
                        segment,
                        nonblocking_read_flags if is_last else directory_flags,
                        dir_fd=current_fd)
                    os.close(current_fd)
                    current_fd = -1
                    if is_last:
                        file_fd = next_fd
                    else:
                        current_fd = next_fd
                if file_fd < 0:
                    raise ValueError('renderer artifact path has no file.')
                before = os.fstat(file_fd)
                if (not stat.S_ISREG(before.st_mode) or
                        before.st_size != artifact_ref.byte_size):
                    raise ValueError(f'renderer artifact {role} is not an '
                                     'exact regular file.')
                content = bytearray()
                while len(content) <= artifact_ref.byte_size:
                    chunk = os.read(file_fd,
                                    artifact_ref.byte_size + 1 - len(content))
                    if not chunk:
                        break
                    content.extend(chunk)
                after = os.fstat(file_fd)
                before_identity = (before.st_dev, before.st_ino, before.st_mode,
                                   before.st_size, before.st_mtime_ns,
                                   before.st_ctime_ns)
                after_identity = (after.st_dev, after.st_ino, after.st_mode,
                                  after.st_size, after.st_mtime_ns,
                                  after.st_ctime_ns)
                if before_identity != after_identity:
                    raise ValueError(f'renderer artifact {role} changed while '
                                     'being read.')
                if len(content) != artifact_ref.byte_size:
                    raise ValueError(f'renderer artifact {role} size drifted.')
                raw_bytes = bytes(content)
                raw_artifact = (
                    provider_artifacts.RawCanonicalRendererArtifactBytesV1.
                    from_verified_bytes(artifact_ref, raw_bytes))
                raw_artifacts[role] = raw_artifact
                raw_bytes_by_role[role] = raw_bytes
            finally:
                if current_fd >= 0:
                    os.close(current_fd)
                if file_fd >= 0:
                    os.close(file_fd)
    except OSError as error:
        raise ValueError('descriptor-safe renderer artifact resolution '
                         'failed.') from error
    finally:
        if package_init_fd >= 0:
            os.close(package_init_fd)
        if package_fd >= 0:
            os.close(package_fd)
        if root_fd >= 0:
            os.close(root_fd)

    outer_raw = raw_artifacts['outer_template']
    node_raw = raw_artifacts['node_fragment']
    binding_raw = raw_artifacts['binding_schema']
    inventory_raw = raw_artifacts['config_access_inventory']
    outer = ResolvedProviderKubernetesOuterTemplateArtifactV1(
        artifact_ref=outer_raw.artifact_ref,
        raw_artifact=outer_raw,
        template=ProviderKubernetesOuterTemplateArtifactV1.from_value(
            outer_raw.canonical_value()))
    node = ResolvedProviderKubernetesNodeFragmentArtifactV1(
        artifact_ref=node_raw.artifact_ref,
        raw_artifact=node_raw,
        fragment=ProviderKubernetesNodeFragmentArtifactV1.from_value(
            node_raw.canonical_value()))
    binding = ResolvedProviderKubernetesBindingSchemaArtifactV1(
        artifact_ref=binding_raw.artifact_ref,
        raw_artifact=binding_raw,
        schema=ProviderKubernetesBindingSchemaArtifactV1.from_value(
            binding_raw.canonical_value()))
    inventory = ResolvedProviderKubernetesConfigAccessInventoryArtifactV1(
        artifact_ref=inventory_raw.artifact_ref,
        raw_artifact=inventory_raw,
        inventory=ProviderKubernetesConfigAccessInventoryV1.from_value(
            inventory_raw.canonical_value()))
    normalization = (
        provider_artifacts.ResolvedProviderKubernetesNormalizationArtifactV1.
        from_verified_bytes(refs[4],
                            raw_bytes_by_role['admitted_object_normalization']))
    return ResolvedProviderKubernetesRendererArtifactSetV1(
        outer_template=outer,
        node_fragment=node,
        binding_schema=binding,
        config_access_inventory=inventory,
        admitted_object_normalization=normalization)


def validate_provider_kubernetes_config_access_inventory_v1(
    resolved_artifacts: ResolvedProviderKubernetesRendererArtifactSetV1,
) -> None:
    """Validate the exact inventory and its import-resolved public names."""

    if type(resolved_artifacts
           ) is not ResolvedProviderKubernetesRendererArtifactSetV1:
        raise TypeError('resolved renderer artifacts have an invalid type.')
    inventory = resolved_artifacts.config_access_inventory.inventory
    if type(inventory) is not ProviderKubernetesConfigAccessInventoryV1:
        raise TypeError('resolved config-access inventory has an invalid type.')
    value = inventory.canonical_value()
    listed_entrypoints = tuple(
        entry['qualified_name'] for entry in value['entrypoints'])
    actual_entrypoints = (
        construct_provider_kubernetes_execution_capsule_v1,
        validate_provider_kubernetes_renderer_input_v1,
        resolve_provider_kubernetes_renderer_artifacts_v1,
        validate_provider_kubernetes_config_access_inventory_v1,
        resolve_provider_kubernetes_bindings_v1,
        render_provider_kubernetes_objects_v1,
        validate_kubernetes_serve_three_object_body_v1,
        provider_artifacts.normalize_kubernetes_request_object_v1,
        build_provider_kubernetes_object_plans_v1,
        assemble_and_revalidate_provider_kubernetes_execution_capsule_v1,
        provider_artifacts.normalize_kubernetes_admitted_object_v1,
    )
    resolved_names = tuple(f'{entrypoint.__module__}.{entrypoint.__name__}'
                           for entrypoint in actual_entrypoints)
    if listed_entrypoints != resolved_names:
        raise ValueError('config-access inventory entrypoints do not resolve '
                         'to the exact implementation callables.')

    entrypoint_by_simple_name = {
        qualified_name.rsplit('.', 1)[1]: qualified_name
        for qualified_name in resolved_names
    }
    expected_call_graph = {
        entry['caller']: tuple(entry['callees']) for entry in value['call_graph']
    }
    inventory_qualified_name = resolved_names[3]
    for caller, entrypoint in zip(resolved_names, actual_entrypoints):
        code_objects = [entrypoint.__code__]
        seen_code_object_ids: set[int] = set()
        call_references: list[tuple[int, int, str]] = []
        all_code_objects: list[types.CodeType] = []
        while code_objects:
            code_object = code_objects.pop()
            code_object_id = id(code_object)
            if code_object_id in seen_code_object_ids:
                continue
            seen_code_object_ids.add(code_object_id)
            all_code_objects.append(code_object)
            instructions = tuple(dis.get_instructions(code_object))
            current_line = code_object.co_firstlineno
            for index, instruction in enumerate(instructions):
                positions = getattr(instruction, 'positions', None)
                position_line = getattr(positions, 'lineno', None)
                if type(position_line) is int:
                    current_line = position_line
                elif type(instruction.starts_line) is int:
                    current_line = instruction.starts_line
                if (instruction.opname == 'LOAD_CONST' and
                        type(instruction.argval) is types.CodeType):
                    code_objects.append(instruction.argval)
                if instruction.opname not in ('LOAD_GLOBAL', 'LOAD_NAME'):
                    continue
                loaded_name = instruction.argval
                if loaded_name in entrypoint_by_simple_name:
                    call_references.append(
                        (current_line, instruction.offset,
                         entrypoint_by_simple_name[loaded_name]))
                    continue
                if loaded_name != 'provider_artifacts':
                    continue
                next_index = index + 1
                while (next_index < len(instructions) and
                       instructions[next_index].opname
                       in ('CACHE', 'EXTENDED_ARG', 'NOP')):
                    next_index += 1
                if (next_index < len(instructions) and
                        instructions[next_index].opname
                        in ('LOAD_ATTR', 'LOAD_METHOD')):
                    attribute = instructions[next_index].argval
                    qualified_name = entrypoint_by_simple_name.get(attribute)
                    if qualified_name is not None:
                        call_references.append(
                            (current_line, instruction.offset, qualified_name))

        if caller == inventory_qualified_name:
            # This validator holds the closed callable tuple but invokes none
            # of its members.  Source-AST parity tests distinguish these
            # references from calls without adding a runtime source read.
            discovered_callees: tuple[str, ...] = ()
        else:
            discovered_callees = tuple(
                item[2] for item in sorted(call_references))
        if discovered_callees != expected_call_graph[caller]:
            raise ValueError('config-access inventory call graph does not '
                             'match executable code.')

        allowed_project_references = set(expected_call_graph[caller])
        if caller == inventory_qualified_name:
            allowed_project_references.update(resolved_names)
        for code_object in all_code_objects:
            instructions = tuple(dis.get_instructions(code_object))
            for index, instruction in enumerate(instructions):
                if instruction.opname.startswith('IMPORT_'):
                    raise ValueError('inventoried entrypoint contains an '
                                     'executable import.')
                if instruction.opname not in ('LOAD_GLOBAL', 'LOAD_NAME'):
                    continue
                loaded_name = instruction.argval
                if loaded_name in {
                        '__import__', 'compile', 'eval', 'exec', 'globals',
                        'locals', 'open', 'vars'
                }:
                    raise ValueError('inventoried entrypoint references a '
                                     'forbidden dynamic or filesystem source.')
                if (loaded_name == 'getattr' and
                        caller != inventory_qualified_name):
                    raise ValueError('inventoried entrypoint references '
                                     'undeclared dynamic attribute access.')
                global_value = entrypoint.__globals__.get(loaded_name)
                if type(global_value) is types.ModuleType:
                    allowed_modules = {'actions', 'provider_artifacts'}
                    if caller == resolved_names[1]:
                        allowed_modules.add('re')
                    elif caller == resolved_names[2]:
                        allowed_modules.update({'os', 'sky_package', 'stat'})
                    elif caller == inventory_qualified_name:
                        allowed_modules.update({
                            'dis', 'hashlib', 'json', 'marshal', 'sys', 'types'
                        })
                    elif caller == resolved_names[10]:
                        allowed_modules.add('ipaddress')
                    if loaded_name not in allowed_modules:
                        raise ValueError(
                            f'inventoried entrypoint {caller} references '
                            f'forbidden module source {loaded_name}.')

                    next_index = index + 1
                    while (next_index < len(instructions) and
                           instructions[next_index].opname
                           in ('CACHE', 'EXTENDED_ARG', 'NOP')):
                        next_index += 1
                    attribute = None
                    if (next_index < len(instructions) and
                            instructions[next_index].opname
                            in ('LOAD_ATTR', 'LOAD_METHOD')):
                        attribute = instructions[next_index].argval
                    if (loaded_name == 'sky_package' and
                            attribute not in (None, '__file__')):
                        raise ValueError('artifact resolution references an '
                                         'undeclared sky package attribute.')
                    if loaded_name == 'os' and attribute not in {
                            None, 'O_CLOEXEC', 'O_DIRECTORY', 'O_NOFOLLOW',
                            'O_NONBLOCK', 'O_RDONLY', 'close', 'dup', 'fstat',
                            'open', 'path', 'read'
                    }:
                        raise ValueError('artifact resolution references a '
                                         'forbidden OS source.')
                    if loaded_name == 're' and attribute != 'sub':
                        raise ValueError('input validation references an '
                                         'undeclared regular-expression API.')
                    if (loaded_name in ('actions', 'provider_artifacts') and
                            attribute is not None):
                        project_value = getattr(global_value, attribute, None)
                        if type(project_value) is types.FunctionType:
                            project_name = (f'{project_value.__module__}.'
                                            f'{project_value.__name__}')
                            if (project_name not in allowed_project_references
                                    and
                                    not (loaded_name == 'actions' and
                                         attribute == 'canonical_json_bytes')):
                                raise ValueError(
                                    'inventoried entrypoint has an '
                                    'undeclared project callee.')
                    continue

                if callable(global_value):
                    project_module = getattr(global_value, '__module__', '')
                    project_name = (f'{project_module}.'
                                    f'{getattr(global_value, "__name__", "")}')
                    if (project_module.startswith('sky.') and
                            type(global_value) is not type and
                            project_name not in allowed_project_references):
                        raise ValueError('inventoried entrypoint has an '
                                         'undeclared project callee.')

    # Hash the exact recursive executable form of every inventoried callable.
    # Location-only CodeType fields are normalized, while bytecode, constants,
    # exception tables, names, and nested functions remain marshal-exact.  The
    # expected digest is version-keyed because CPython bytecode is not stable
    # across supported minors.  Whole-module source AST seals in CI cover
    # executable top-level statements, including this validator's digest map.
    executable_preimage = bytearray()
    for qualified_name, entrypoint in zip(resolved_names, actual_entrypoints):
        qualified_name_bytes = qualified_name.encode('utf-8')
        pending_code = [(entrypoint.__code__, False)]
        sanitized_code_by_identity: dict[int, types.CodeType] = {}
        while pending_code:
            code_object, children_ready = pending_code.pop()
            if not children_ready:
                pending_code.append((code_object, True))
                for constant in code_object.co_consts:
                    if type(constant) is types.CodeType:
                        pending_code.append((constant, False))
                continue
            sanitized_constants = []
            for constant in code_object.co_consts:
                if type(constant) is types.CodeType:
                    sanitized_constants.append(
                        ('code', sanitized_code_by_identity[id(constant)]))
                    continue
                pending_constant = [(constant, False)]
                encoded_constant_by_identity: dict[int, Any] = {}
                while pending_constant:
                    constant_value, children_ready = pending_constant.pop()
                    constant_type = type(constant_value)
                    encoded_constant: Any
                    is_container = constant_type in (tuple, frozenset, slice)
                    if is_container and not children_ready:
                        pending_constant.append((constant_value, True))
                        if constant_type is slice:
                            children = (constant_value.start,
                                        constant_value.stop,
                                        constant_value.step)
                        else:
                            children = tuple(constant_value)
                        pending_constant.extend(
                            (child, False) for child in children)
                        continue
                    if constant_type is tuple:
                        encoded_constant = (
                            'tuple',
                            tuple(encoded_constant_by_identity[id(child)]
                                  for child in constant_value))
                    elif constant_type is frozenset:
                        encoded_items = tuple(
                            sorted(
                                marshal.dumps(
                                    encoded_constant_by_identity[id(child)], 2)
                                for child in constant_value))
                        encoded_constant = ('frozenset', encoded_items)
                    elif constant_type is slice:
                        encoded_constant = (
                            'slice',
                            encoded_constant_by_identity[id(
                                constant_value.start)],
                            encoded_constant_by_identity[id(
                                constant_value.stop)],
                            encoded_constant_by_identity[id(
                                constant_value.step)],
                        )
                    elif constant_value is None:
                        encoded_constant = ('none',)
                    elif constant_value is Ellipsis:
                        encoded_constant = ('ellipsis',)
                    elif constant_value is NotImplemented:
                        encoded_constant = ('not_implemented',)
                    elif constant_type in (bool, int, float, complex, str,
                                           bytes):
                        encoded_constant = (constant_type.__name__,
                                            constant_value)
                    else:
                        raise ValueError('inventoried entrypoint contains an '
                                         'unsupported executable constant.')
                    encoded_constant_by_identity[id(
                        constant_value)] = encoded_constant
                sanitized_constants.append(
                    encoded_constant_by_identity[id(constant)])
            replacement: dict[str, Any] = {
                'co_code': code_object.co_code,
                'co_consts': tuple(sanitized_constants),
                'co_filename': '',
                'co_firstlineno': 1,
            }
            if hasattr(code_object, 'co_linetable'):
                replacement['co_linetable'] = b''
            elif hasattr(code_object, 'co_lnotab'):
                replacement['co_lnotab'] = b''
            sanitized_code_by_identity[id(code_object)] = code_object.replace(
                **replacement)
        # Marshal v2 predates reference sharing, so equal code objects cannot
        # acquire different bytes from runtime string-intern identities.
        code_bytes = marshal.dumps(
            sanitized_code_by_identity[id(entrypoint.__code__)], 2)
        executable_preimage.extend(len(qualified_name_bytes).to_bytes(8, 'big'))
        executable_preimage.extend(qualified_name_bytes)
        executable_preimage.extend(len(code_bytes).to_bytes(8, 'big'))
        executable_preimage.extend(code_bytes)
    python_minor = (sys.version_info.major, sys.version_info.minor)
    matching_executable_sha256 = tuple(
        expected_sha256 for expected_minor, expected_sha256 in
        _EXPECTED_INVENTORIED_EXECUTABLE_SHA256_BY_PYTHON
        if expected_minor == python_minor)
    if len(matching_executable_sha256) != 1:
        raise RuntimeError('inventoried renderer executable fingerprint is '
                           'unsupported on this Python minor.')
    expected_executable_sha256, = matching_executable_sha256
    if (hashlib.sha256(executable_preimage).hexdigest()
            != expected_executable_sha256):
        raise ValueError('inventoried renderer executable access shape has '
                         'drifted.')


def resolve_provider_kubernetes_bindings_v1(
    renderer_input: actions.ProviderKubernetesRendererInputV1,
    resolved_artifacts: ResolvedProviderKubernetesRendererArtifactSetV1,
) -> actions.ResolvedProviderKubernetesBindingSetV1:
    """Resolve the closed 17-row binding table from its sole typed root."""

    if type(renderer_input) is not actions.ProviderKubernetesRendererInputV1:
        raise TypeError('Kubernetes renderer input has an invalid type.')
    if type(resolved_artifacts
           ) is not ResolvedProviderKubernetesRendererArtifactSetV1:
        raise TypeError('resolved renderer artifacts have an invalid type.')
    binding_schema = resolved_artifacts.binding_schema.schema
    if type(binding_schema) is not ProviderKubernetesBindingSchemaArtifactV1:
        raise TypeError('resolved binding schema has an invalid type.')

    topology = renderer_input.seed.topology.mutable_objects
    head_ssh_labels = {label.key: label.value for label in topology[0].labels}
    head_labels = {label.key: label.value for label in topology[1].labels}
    head_pod_labels = {label.key: label.value for label in topology[2].labels}
    selector = {key: head_pod_labels[key] for key in _SELECTOR_KEYS}
    values: tuple[Any, ...] = (
        head_labels,
        topology[1].name,
        head_pod_labels,
        topology[2].name,
        selector,
        head_ssh_labels,
        topology[0].name,
        renderer_input.seed.resources.image_pull_policy,
        renderer_input.seed.request_identity.original_user,
        renderer_input.seed.resources.pod_cpu_limit,
        renderer_input.seed.resources.pod_cpu_request,
        renderer_input.seed.resources.pod_memory_limit,
        renderer_input.seed.resources.pod_memory_request,
        str(renderer_input.resource_identity.replica_id),
        renderer_input.seed.scope.namespace,
        renderer_input.seed.resources.image.qualification.requested_reference,
        renderer_input.seed.principals.workload.name,
    )
    schema_rows = binding_schema.canonical_value()['bindings']
    bindings = tuple(
        actions.ResolvedProviderKubernetesBindingV1(
            sequence=index,
            name=row['name'],
            json_type=row['json_type'],
            value=actions.CanonicalJsonValue.from_value(value))
        for index, (row, value) in enumerate(zip(schema_rows, values)))
    return actions.ResolvedProviderKubernetesBindingSetV1(
        version=1,
        contract=('skypilot.serve.prebooted-direct-pod.resolved-bindings.v1'),
        bindings=bindings)


def render_provider_kubernetes_objects_v1(
    renderer_input: actions.ProviderKubernetesRendererInputV1,
    resolved_artifacts: ResolvedProviderKubernetesRendererArtifactSetV1,
) -> tuple[actions.CanonicalJsonObject, ...]:
    """Substitute only resolved typed markers and emit three request bodies."""

    if type(renderer_input) is not actions.ProviderKubernetesRendererInputV1:
        raise TypeError('Kubernetes renderer input has an invalid type.')
    if type(resolved_artifacts
           ) is not ResolvedProviderKubernetesRendererArtifactSetV1:
        raise TypeError('resolved renderer artifacts have an invalid type.')
    resolved_bindings = resolve_provider_kubernetes_bindings_v1(
        renderer_input, resolved_artifacts)
    binding_values = {
        binding.name: binding.value for binding in resolved_bindings.bindings
    }
    expected_counts = {
        row['name']: len(row['targets']) for row in
        resolved_artifacts.binding_schema.schema.canonical_value()['bindings']
    }
    actual_counts = {name: 0 for name in expected_counts}
    templates = {
        'outer_template':
            resolved_artifacts.outer_template.template.canonical_value(),
        'node_fragment':
            resolved_artifacts.node_fragment.fragment.canonical_value(),
    }
    for artifact in templates.values():
        root = [artifact]
        stack: list[tuple[Any, Any]] = [(root, 0)]
        while stack:
            parent, key = stack.pop()
            item = parent[key]
            if type(item) is dict and set(item) == {'$binding'}:
                binding_name = item['$binding']
                if type(binding_name
                       ) is not str or binding_name not in binding_values:
                    raise ValueError('renderer template contains an unlisted '
                                     'binding marker.')
                parent[key] = binding_values[binding_name].canonical_value()
                actual_counts[binding_name] += 1
                continue
            if type(item) is dict:
                if '$binding' in item:
                    raise ValueError('renderer binding marker is not a whole '
                                     'value.')
                stack.extend(
                    (item, child_key) for child_key in reversed(tuple(item)))
            elif type(item) is list:
                stack.extend(
                    (item, index) for index in reversed(range(len(item))))
        if root[0] is not artifact:
            raise ValueError('renderer artifact root cannot be a marker.')
    if actual_counts != expected_counts:
        raise ValueError('renderer template marker use does not match the '
                         'binding schema.')

    outer = templates['outer_template']
    node = templates['node_fragment']
    service_templates = outer['service_templates']
    bodies = (
        service_templates[0]['body'],
        service_templates[1]['body'],
        node['body'],
    )
    return tuple(
        actions.CanonicalJsonObject.from_value(body) for body in bodies)


def validate_kubernetes_serve_three_object_body_v1(
    renderer_input: actions.ProviderKubernetesRendererInputV1,
    rendered_bodies: tuple[actions.CanonicalJsonObject, ...],
) -> tuple[actions.ValidatedKubernetesServeThreeObjectBodyV1, ...]:
    """Validate all schema and RendererInput-dependent body projections."""

    if type(renderer_input) is not actions.ProviderKubernetesRendererInputV1:
        raise TypeError('Kubernetes renderer input has an invalid type.')
    if (type(rendered_bodies) is not tuple or len(rendered_bodies) != 3 or any(
            type(body) is not actions.CanonicalJsonObject
            for body in rendered_bodies)):
        raise TypeError('rendered bodies must be the exact three-object tuple.')

    topology = renderer_input.seed.topology.mutable_objects
    roles = tuple(
        entry.role for entry in actions.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1)
    validated = tuple(
        actions.ValidatedKubernetesServeThreeObjectBodyV1(role=role, body=body)
        for role, body in zip(roles, rendered_bodies))

    pod_labels = {label.key: label.value for label in topology[2].labels}
    expected_selector = {key: pod_labels[key] for key in _SELECTOR_KEYS}
    for index, item in enumerate(validated):
        body = item.body.canonical_value()
        metadata = body['metadata']
        expected_labels = {
            label.key: label.value for label in topology[index].labels
        }
        if (metadata['namespace'] != renderer_input.seed.scope.namespace or
                metadata['name'] != topology[index].name or
                metadata['labels'] != expected_labels):
            raise ValueError('rendered body identity does not match its '
                             'RendererInput topology.')
        if item.role is actions.ProviderObjectRoleV1.HEAD_POD:
            if metadata['annotations'] != {
                    'skypilot-user':
                        renderer_input.seed.request_identity.original_user
            }:
                raise ValueError('rendered Pod annotation does not match its '
                                 'RendererInput request identity.')
            spec = body['spec']
            if (spec['serviceAccount']
                    != renderer_input.seed.principals.workload.name or
                    spec['serviceAccountName']
                    != renderer_input.seed.principals.workload.name):
                raise ValueError('rendered Pod service account does not match '
                                 'its RendererInput principal.')
            container = spec['containers'][0]
            expected_resources = {
                'limits': {
                    'cpu': renderer_input.seed.resources.pod_cpu_limit,
                    'memory': renderer_input.seed.resources.pod_memory_limit,
                },
                'requests': {
                    'cpu': renderer_input.seed.resources.pod_cpu_request,
                    'memory': renderer_input.seed.resources.pod_memory_request,
                },
            }
            matching_environment = [
                entry for entry in container['env']
                if entry['name'] == 'SKYPILOT_SERVE_REPLICA_ID'
            ]
            if len(matching_environment) != 1:
                raise ValueError('rendered Pod replica environment is not '
                                 'unique.')
            replica_environment, = matching_environment
            if replica_environment['value'] != str(
                    renderer_input.resource_identity.replica_id):
                raise ValueError('rendered Pod replica projection does not '
                                 'match RendererInput.')
            if (container['image'] != renderer_input.seed.resources.image.
                    qualification.requested_reference or
                    container['imagePullPolicy']
                    != renderer_input.seed.resources.image_pull_policy or
                    container['resources'] != expected_resources):
                raise ValueError('rendered Pod resource projection '
                                 'does not match RendererInput.')
        elif body['spec']['selector'] != expected_selector:
            raise ValueError('rendered Service selector does not match the '
                             'RendererInput head Pod labels.')
    return validated


def build_provider_kubernetes_object_plans_v1(
    validated_bodies: tuple[actions.ValidatedKubernetesServeThreeObjectBodyV1,
                            ...],
    request_normalizations: tuple[
        actions.ProviderKubernetesRequestNormalizationV1, ...],
    normalization_artifact: (
        provider_artifacts.ResolvedProviderKubernetesNormalizationArtifactV1),
) -> tuple[actions.ProviderKubernetesObjectPlanV1, ...]:
    """Build the three exact object plans from validated typed transients."""

    role_map = actions.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1
    if (type(validated_bodies) is not tuple or
            len(validated_bodies) != len(role_map) or any(
                type(body)
                is not actions.ValidatedKubernetesServeThreeObjectBodyV1
                for body in validated_bodies)):
        raise TypeError('validated bodies must be the exact three-role tuple.')
    if (type(request_normalizations) is not tuple or
            len(request_normalizations) != len(role_map) or any(
                type(normalization)
                is not actions.ProviderKubernetesRequestNormalizationV1
                for normalization in request_normalizations)):
        raise TypeError('request normalizations must be the exact three-role '
                        'tuple.')
    if type(normalization_artifact) is not (
            provider_artifacts.ResolvedProviderKubernetesNormalizationArtifactV1
    ):
        raise TypeError('object-plan normalization artifact has an invalid '
                        'type.')
    actual_roles = tuple(body.role for body in validated_bodies)
    expected_roles = tuple(entry.role for entry in role_map)
    if actual_roles != expected_roles:
        raise ValueError('validated bodies are not in exact role order.')
    expected_intents = (
        'allocate_single_stack_cluster_ip',
        'headless_single_stack',
        'schedule_one_node',
    )
    if tuple(normalization.requested_allocation_intent
             for normalization in request_normalizations) != expected_intents:
        raise ValueError('request normalizations do not match exact role '
                         'allocation intent order.')
    comparison_contract = normalization_artifact.contract.canonical_value(
    )['comparison_contract']
    if comparison_contract != 'kubernetes_admitted_object_v1':
        raise ValueError('object-plan comparison contract is unsupported.')

    plans = []
    for entry, validated_body, normalization in zip(role_map, validated_bodies,
                                                    request_normalizations):
        body = validated_body.body
        raw_body = body.canonical_value()
        expected_semantic = body.canonical_value()
        if entry.role is actions.ProviderObjectRoleV1.HEAD_SERVICE:
            del expected_semantic['spec']['clusterIP']
        if (normalization.requested_semantic.canonical_bytes
                != actions.canonical_json_bytes(expected_semantic)):
            raise ValueError('request normalization semantic does not match '
                             'its validated request body.')
        metadata = raw_body['metadata']
        labels = metadata['labels']
        identity_labels = tuple(
            actions.ProviderLabelV1(key=key, value=labels[key])
            for key in _IDENTITY_LABEL_KEYS)
        plans.append(
            actions.ProviderKubernetesObjectPlanV1(
                sequence=entry.plan_sequence,
                role=entry.role,
                api_version='v1',
                kind=entry.kind,
                namespace=metadata['namespace'],
                name=metadata['name'],
                required_identity_labels=identity_labels,
                request_body=body,
                request_body_sha256=body.sha256,
                requested_semantic=normalization.requested_semantic,
                requested_semantic_sha256=normalization.requested_semantic.
                sha256,
                comparison_contract=comparison_contract,
                normalization_profile=normalization_artifact.artifact_ref))
    return tuple(plans)


def assemble_and_revalidate_provider_kubernetes_execution_capsule_v1(
    renderer_input: actions.ProviderKubernetesRendererInputV1,
    object_plans: tuple[actions.ProviderKubernetesObjectPlanV1, ...],
) -> actions.ProviderKubernetesExecutionCapsuleV1:
    """Append plans to the unchanged seed and rerun full capsule validation."""

    if type(renderer_input) is not actions.ProviderKubernetesRendererInputV1:
        raise TypeError('Kubernetes renderer input has an invalid type.')
    role_map = actions.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1
    if (type(object_plans) is not tuple or len(object_plans) != len(role_map) or
            any(
                type(plan) is not actions.ProviderKubernetesObjectPlanV1
                for plan in object_plans)):
        raise TypeError('object plans must be the exact three-role tuple.')
    expected = tuple(
        (entry.plan_sequence, entry.role, entry.kind) for entry in role_map)
    actual = tuple(
        (plan.sequence, plan.role, plan.kind) for plan in object_plans)
    if actual != expected:
        raise ValueError('object plans are not in exact role order.')
    capsule_value = renderer_input.seed.canonical_value()
    capsule_value['objects'] = [plan.canonical_value() for plan in object_plans]
    return actions.ProviderKubernetesExecutionCapsuleV1.from_value(
        capsule_value)


def construct_provider_kubernetes_execution_capsule_v1(
    renderer_input: actions.ProviderKubernetesRendererInputV1,
) -> actions.ProviderKubernetesExecutionCapsuleV1:
    """Execute the exact four-stage, nonrecursive pure construction graph."""

    if type(renderer_input) is not actions.ProviderKubernetesRendererInputV1:
        raise TypeError('Kubernetes renderer input has an invalid type.')
    if renderer_input.version != 1 or renderer_input.contract != (
            'validated_launch_spec_v1'):
        raise ValueError('Kubernetes renderer input root contract is invalid.')
    validated_input = validate_provider_kubernetes_renderer_input_v1(
        renderer_input)
    resolved_artifacts = resolve_provider_kubernetes_renderer_artifacts_v1(
        validated_input)
    validate_provider_kubernetes_config_access_inventory_v1(resolved_artifacts)
    rendered_bodies = render_provider_kubernetes_objects_v1(
        validated_input, resolved_artifacts)
    validated_bodies = validate_kubernetes_serve_three_object_body_v1(
        validated_input, rendered_bodies)
    roles = tuple(
        entry.role for entry in actions.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1)
    request_normalizations = tuple(
        provider_artifacts.normalize_kubernetes_request_object_v1(
            role, body, resolved_artifacts.admitted_object_normalization)
        for role, body in zip(roles, validated_bodies))
    object_plans = build_provider_kubernetes_object_plans_v1(
        validated_bodies, request_normalizations,
        resolved_artifacts.admitted_object_normalization)
    return assemble_and_revalidate_provider_kubernetes_execution_capsule_v1(
        validated_input, object_plans)
