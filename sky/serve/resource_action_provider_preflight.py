"""Pure static evidence and initial authority-preflight evaluation.

This module is itself the v1 Pod-template projector artifact.  Bootstrap first
descriptor-validates its installed source bytes against the manifest before it
calls the named projector below.  Nothing here imports provider clients,
request submission, a database store, or a mutation entrypoint.
"""

from __future__ import annotations

from collections.abc import Callable
import hashlib
import json
import os
import stat
from typing import Any

import sky
from sky.serve import resource_actions
from sky.server import constants as server_constants
from sky.server.requests import registry as request_registry
from sky.server.requests import resource_actions as kernel_actions
from sky.server.requests.serializers import decoders
from sky.server.requests.serializers import encoders

_MANIFEST_PATH = '/etc/skypilot/resource-action-authority/manifest.json'
_QUALIFICATION_PATH = (
    '/etc/skypilot/resource-action-authority/qualification.json')
_POD_TEMPLATE_CONTRACT_REPO_PATH = (
    'sky/serve/resource_action_provider_preflight.py')
_RENDERER_ARTIFACT_INVENTORY_REPO_PATH = (
    'sky/serve/resource_action_artifacts/provider_authority_v1/'
    'renderer_artifact_inventory.json')
_CALLABLE_INVENTORY_REPO_PATH = (
    'sky/serve/resource_action_artifacts/provider_authority_v1/'
    'callable_inventory.json')
_RENDERER_ARTIFACT_INVENTORY_CONTRACT = (
    'provider_kubernetes_renderer_artifact_inventory_v1')
_CALLABLE_INVENTORY_CONTRACT = 'provider_authority_callable_inventory_v1'
_RENDERER_ROLE_PATHS: tuple[tuple[str, str], ...] = (
    ('outer_template',
     'sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
     'outer_template.json'),
    ('node_fragment',
     'sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
     'node_fragment.json'),
    ('binding_schema',
     'sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
     'binding_schema.json'),
    ('config_access_inventory',
     'sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
     'config_access_inventory.json'),
    ('admitted_object_normalization',
     'sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
     'admitted_object_normalization.json'),
)
_MAX_STATIC_JSON_BYTES = 65_536
_READ_CHUNK_BYTES = 64 * 1024
_QUALIFICATION_KEYS = frozenset({
    'version', 'requested_reference', 'oci_manifest_digest',
    'oci_config_digest', 'source_commit', 'platform'
})
_INVENTORY_ROOT_KEYS = frozenset({'version', 'contract', 'artifacts'})
_INVENTORY_ARTIFACT_KEYS = frozenset(
    {'role', 'repo_path', 'byte_size', 'sha256'})
_CALLABLE_ROOT_KEYS = frozenset({'version', 'contract', 'handlers'})
_CALLABLE_HANDLER_KEYS = frozenset({
    'name', 'module', 'qualname', 'execution_class', 'claim_scope',
    'replay_policy', 'cancellation_policy', 'aliases', 'result_codec'
})
_RESULT_CODEC_KEYS = frozenset({'encoder', 'decoder', 'strict_return_value'})
_CODEC_IDENTITY_KEYS = frozenset({'mode', 'module', 'qualname'})


class ProviderAuthorityStaticEvidenceError(ValueError):
    """Projected or installed authority evidence failed closed validation."""


def _labels(
        items: tuple[resource_actions.ProviderLabelV1, ...]) -> dict[str, str]:
    return {item.key: item.value for item in items}


def _annotations(
        items: tuple[resource_actions.ProviderAnnotationV1,
                     ...]) -> dict[str, str]:
    return {item.key: item.value for item in items}


def project_provider_authority_worker_pod_template_v1(
    release_inputs: resource_actions.
    ProviderAuthorityWorkerPodTemplateReleaseInputsV1,
) -> resource_actions.CanonicalJsonObject:
    """Construct the sole placeholder PodTemplateSpec for one release input."""

    if type(release_inputs) is not (
            resource_actions.ProviderAuthorityWorkerPodTemplateReleaseInputsV1):
        raise TypeError('release_inputs has an invalid type.')

    literal_env = [{
        'name': item.name,
        'value': item.value,
    } for item in release_inputs.literal_env]
    secret_env = [{
        'name': item.name,
        'valueFrom': {
            'secretKeyRef': {
                'name': item.secret_name,
                'key': item.key,
            }
        },
    } for item in release_inputs.secret_env]
    downward_env = [{
        'name': item.env,
        'valueFrom': {
            'fieldRef': {
                'apiVersion': 'v1',
                'fieldPath': item.field_path,
            }
        },
    } for item in release_inputs.downward_api_fields]

    manifest = release_inputs.manifest_config_map
    qualification = release_inputs.qualification_config_map
    auth = release_inputs.auth_secret
    tls = release_inputs.tls_secret
    volume_mounts: list[dict[str, Any]] = [
        {
            'name': 'authority-manifest',
            'mountPath': manifest.mount_path,
            'subPath': 'manifest.json',
            'readOnly': True,
        },
        {
            'name': 'authority-qualification',
            'mountPath': qualification.mount_path,
            'subPath': 'qualification.json',
            'readOnly': True,
        },
        {
            'name': 'authority-auth',
            'mountPath': os.path.dirname(auth.mount_path),
            'readOnly': True,
        },
        {
            'name': 'authority-tls',
            'mountPath': os.path.dirname(tls.cert_path),
            'readOnly': True,
        },
        {
            'name': 'skypilot-role-runtime',
            'mountPath': '/var/run/skypilot',
        },
        {
            'name': 'kube-api-access',
            'mountPath': '/var/run/secrets/kubernetes.io/serviceaccount',
            'readOnly': True,
        },
    ]
    volumes: list[dict[str, Any]] = [
        {
            'name': 'authority-manifest',
            'configMap': {
                'name': manifest.name,
                'defaultMode': 292,
                'items': [{
                    'key': manifest.key,
                    'path': 'manifest.json',
                }],
            },
        },
        {
            'name': 'authority-qualification',
            'configMap': {
                'name': qualification.name,
                'defaultMode': 292,
                'items': [{
                    'key': qualification.key,
                    'path': 'qualification.json',
                }],
            },
        },
        {
            'name': 'authority-auth',
            'secret': {
                'secretName': auth.name,
                'defaultMode': 256,
                'items': [{
                    'key': auth.key,
                    'path': 'tokens',
                }],
            },
        },
        {
            'name': 'authority-tls',
            'secret': {
                'secretName': tls.name,
                'defaultMode': 256,
                'items': [{
                    'key': tls.cert_key,
                    'path': 'tls.crt',
                }, {
                    'key': tls.private_key_key,
                    'path': 'tls.key',
                }, {
                    'key': tls.ca_key,
                    'path': 'ca.crt',
                }],
            },
        },
        {
            'name': 'skypilot-role-runtime',
            'emptyDir': {},
        },
        {
            'name': 'kube-api-access',
            'projected': {
                'defaultMode': 420,
                'sources': [{
                    'serviceAccountToken': {
                        'expirationSeconds': 3607,
                        'path': 'token',
                    },
                }, {
                    'configMap': {
                        'name': 'kube-root-ca.crt',
                        'items': [{
                            'key': 'ca.crt',
                            'path': 'ca.crt',
                        }],
                    },
                }, {
                    'downwardAPI': {
                        'items': [{
                            'path': 'namespace',
                            'fieldRef': {
                                'apiVersion': 'v1',
                                'fieldPath': 'metadata.namespace',
                            },
                        }],
                    },
                }],
            },
        },
    ]
    container: dict[str, Any] = {
        'name': release_inputs.container_name,
        'image': release_inputs.image,
        'imagePullPolicy': release_inputs.image_pull_policy,
        'command': list(release_inputs.command),
        'args': list(release_inputs.args),
        'env': literal_env + secret_env + downward_env,
        'ports': [{
            'name': 'health',
            'containerPort': int(release_inputs.health_port),
            'protocol': 'TCP',
        }, {
            'name': 'preflight',
            'containerPort': int(release_inputs.preflight_port),
            'protocol': 'TCP',
        }],
        'resources': release_inputs.resources.canonical_value(),
        'securityContext':
            release_inputs.container_security_context.canonical_value(),
        'terminationMessagePath': '/dev/termination-log',
        'terminationMessagePolicy': 'File',
        'lifecycle': {
            'preStop': {
                'exec': {
                    'command': [
                        '/bin/sh', '-c', 'touch /var/run/skypilot/draining'
                    ]
                }
            }
        },
        'startupProbe': {
            'httpGet': {
                'path': '/bootstrapz',
                'port': 'health',
                'scheme': 'HTTP',
            },
            'failureThreshold': 60,
            'periodSeconds': 10,
            'successThreshold': 1,
            'timeoutSeconds': 1,
        },
        'livenessProbe': {
            'httpGet': {
                'path': '/livez',
                'port': 'health',
                'scheme': 'HTTP',
            },
            'failureThreshold': 3,
            'periodSeconds': 10,
            'successThreshold': 1,
            'timeoutSeconds': 1,
        },
        'readinessProbe': {
            'httpGet': {
                'path': '/bootstrapz',
                'port': 'health',
                'scheme': 'HTTP',
            },
            'failureThreshold': 3,
            'periodSeconds': 5,
            'successThreshold': 1,
            'timeoutSeconds': 1,
        },
        'volumeMounts': volume_mounts,
    }
    pod_spec: dict[str, Any] = {
        'automountServiceAccountToken': False,
        'serviceAccount': release_inputs.service_account_name,
        'serviceAccountName': release_inputs.service_account_name,
        'terminationGracePeriodSeconds':
            release_inputs.termination_grace_period_seconds,
        'restartPolicy': 'Always',
        'dnsPolicy': 'ClusterFirst',
        'enableServiceLinks': False,
        'hostNetwork': False,
        'hostPID': False,
        'hostIPC': False,
        'preemptionPolicy': 'PreemptLowerPriority',
        'priority': 0,
        'schedulerName': (release_inputs.scheduler_name or 'default-scheduler'),
        'securityContext':
            release_inputs.pod_security_context.canonical_value(),
        'containers': [container],
        'volumes': volumes,
    }
    if release_inputs.image_pull_secrets:
        pod_spec['imagePullSecrets'] = [{
            'name': name
        } for name in release_inputs.image_pull_secrets]
    if release_inputs.node_selector:
        pod_spec['nodeSelector'] = _labels(release_inputs.node_selector)
    pod_spec['tolerations'] = [
        item.canonical_value() for item in release_inputs.tolerations
    ] + [{
        'effect': 'NoExecute',
        'key': 'node.kubernetes.io/not-ready',
        'operator': 'Exists',
        'tolerationSeconds': 300,
    }, {
        'effect': 'NoExecute',
        'key': 'node.kubernetes.io/unreachable',
        'operator': 'Exists',
        'tolerationSeconds': 300,
    }]
    if release_inputs.affinity is not None:
        pod_spec['affinity'] = release_inputs.affinity.canonical_value()
    if release_inputs.topology_spread_constraints:
        pod_spec['topologySpreadConstraints'] = [
            item.canonical_value()
            for item in release_inputs.topology_spread_constraints
        ]
    if release_inputs.priority_class_name is not None:
        pod_spec['priorityClassName'] = release_inputs.priority_class_name
    if release_inputs.runtime_class_name is not None:
        pod_spec['runtimeClassName'] = release_inputs.runtime_class_name

    annotations = _annotations(
        release_inputs.pod_annotations_without_manifest_hash)
    annotations['skypilot.co/resource-action-manifest-sha256'] = (
        '$MANIFEST_SHA256')
    return resource_actions.CanonicalJsonObject({
        'metadata': {
            'labels': _labels(release_inputs.pod_labels),
            'annotations': annotations,
        },
        'spec': pod_spec,
    })


def materialize_provider_authority_worker_pod_template_v1(
    release_inputs: resource_actions.
    ProviderAuthorityWorkerPodTemplateReleaseInputsV1,
    manifest_sha256: str,
) -> resource_actions.CanonicalJsonObject:
    """Replace the sole placeholder with one validated manifest hash."""

    if (type(manifest_sha256) is not str or len(manifest_sha256) != 64 or
            any(character not in '0123456789abcdef'
                for character in manifest_sha256)):
        raise ValueError('manifest_sha256 must be lowercase SHA-256 hex.')
    projected = project_provider_authority_worker_pod_template_v1(
        release_inputs).canonical_value()
    annotations = projected['metadata']['annotations']
    if annotations['skypilot.co/resource-action-manifest-sha256'] != (
            '$MANIFEST_SHA256'):
        raise ValueError('Pod-template manifest placeholder is absent.')
    annotations['skypilot.co/resource-action-manifest-sha256'] = (
        manifest_sha256)
    return resource_actions.CanonicalJsonObject(projected)


def _json_without_duplicates(contents: bytes, *, name: str) -> Any:

    def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ProviderAuthorityStaticEvidenceError(
                    f'{name} contains a duplicate JSON key.')
            result[key] = value
        return result

    def _forbid_float(_: str) -> Any:
        raise ProviderAuthorityStaticEvidenceError(
            f'{name} contains a floating-point value.')

    try:
        value = json.loads(contents.decode('utf-8'),
                           object_pairs_hook=_object,
                           parse_float=_forbid_float,
                           parse_constant=_forbid_float)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ProviderAuthorityStaticEvidenceError(
            f'{name} is not valid canonical UTF-8 JSON.') from e
    if resource_actions.canonical_json_bytes(value) != contents:
        raise ProviderAuthorityStaticEvidenceError(
            f'{name} bytes are not canonical JSON.')
    return value


def _read_fixed_regular_file(path: str, *, name: str,
                             maximum_bytes: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, 'O_CLOEXEC', 0) | getattr(
        os, 'O_NOFOLLOW', 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as e:
        raise ProviderAuthorityStaticEvidenceError(
            f'{name} cannot be opened safely.') from e
    try:
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_size < 1 or
                before.st_size > maximum_bytes):
            raise ProviderAuthorityStaticEvidenceError(
                f'{name} is not a bounded regular file.')
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(_READ_CHUNK_BYTES, remaining))
            if not chunk:
                raise ProviderAuthorityStaticEvidenceError(
                    f'{name} changed while it was read.')
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ProviderAuthorityStaticEvidenceError(
                f'{name} grew while it was read.')
        after = os.fstat(descriptor)
        stable = (before.st_dev, before.st_ino, before.st_size,
                  before.st_mtime_ns, before.st_ctime_ns)
        if stable != (after.st_dev, after.st_ino, after.st_size,
                      after.st_mtime_ns, after.st_ctime_ns):
            raise ProviderAuthorityStaticEvidenceError(
                f'{name} changed while it was read.')
        return b''.join(chunks)
    finally:
        os.close(descriptor)


def _distribution_root() -> str:
    # ``__file__`` is the already imported, fixed package member.  Repository
    # references are then resolved only beneath its distribution root.
    return os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _read_installed_artifact(
        reference: resource_actions.ProviderRepoArtifactRefV1) -> bytes:
    if type(reference) is not resource_actions.ProviderRepoArtifactRefV1:
        raise TypeError('Installed authority artifact reference has an '
                        'invalid type.')
    if reference.byte_size > _MAX_STATIC_JSON_BYTES:
        raise ProviderAuthorityStaticEvidenceError(
            'Installed authority artifact exceeds its byte bound.')
    required_flags = ('O_CLOEXEC', 'O_DIRECTORY', 'O_NOFOLLOW', 'O_NONBLOCK')
    if any(not hasattr(os, flag) for flag in required_flags):
        raise ProviderAuthorityStaticEvidenceError(
            'Descriptor-safe installed artifact resolution is unsupported.')
    directory_flags = (os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC |
                       os.O_NOFOLLOW)
    read_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        root_descriptor = os.open(_distribution_root(), directory_flags)
    except OSError as e:
        raise ProviderAuthorityStaticEvidenceError(
            'Installed authority distribution root cannot be opened safely.'
        ) from e
    current_descriptor = root_descriptor
    try:
        components = reference.repo_path.split('/')
        for component in components[:-1]:
            next_descriptor = os.open(component,
                                      directory_flags,
                                      dir_fd=current_descriptor)
            if current_descriptor != root_descriptor:
                os.close(current_descriptor)
            current_descriptor = next_descriptor
        file_descriptor = os.open(components[-1],
                                  read_flags,
                                  dir_fd=current_descriptor)
        try:
            before = os.fstat(file_descriptor)
            if (not stat.S_ISREG(before.st_mode) or
                    before.st_size != reference.byte_size):
                raise ProviderAuthorityStaticEvidenceError(
                    'Installed authority artifact size or type differs from '
                    'its manifest reference.')
            contents = bytearray()
            remaining = before.st_size
            while remaining:
                chunk = os.read(file_descriptor,
                                min(_READ_CHUNK_BYTES, remaining))
                if not chunk:
                    raise ProviderAuthorityStaticEvidenceError(
                        'Installed authority artifact changed while read.')
                contents.extend(chunk)
                remaining -= len(chunk)
            if os.read(file_descriptor, 1):
                raise ProviderAuthorityStaticEvidenceError(
                    'Installed authority artifact grew while read.')
            after = os.fstat(file_descriptor)
            stable = (before.st_dev, before.st_ino, before.st_size,
                      before.st_mtime_ns, before.st_ctime_ns)
            if stable != (after.st_dev, after.st_ino, after.st_size,
                          after.st_mtime_ns, after.st_ctime_ns):
                raise ProviderAuthorityStaticEvidenceError(
                    'Installed authority artifact changed while read.')
            raw_bytes = bytes(contents)
            if hashlib.sha256(raw_bytes).hexdigest() != reference.sha256:
                raise ProviderAuthorityStaticEvidenceError(
                    'Installed authority artifact hash differs from its '
                    'manifest reference.')
            return raw_bytes
        finally:
            os.close(file_descriptor)
    except OSError as e:
        raise ProviderAuthorityStaticEvidenceError(
            'Installed authority artifact cannot be resolved safely.') from e
    finally:
        if current_descriptor != root_descriptor:
            os.close(current_descriptor)
        os.close(root_descriptor)


def _closed_dictionary(value: Any, *, name: str,
                       keys: frozenset[str]) -> dict[str, Any]:
    if type(value) is not dict:
        raise ProviderAuthorityStaticEvidenceError(
            f'{name} must be an exact JSON object.')
    if set(value) != keys:
        raise ProviderAuthorityStaticEvidenceError(
            f'{name} has unknown or missing fields.')
    return value


def _parse_canonical_inventory(contents: bytes, *, name: str) -> dict[str, Any]:
    if (not contents.endswith(b'\n') or contents.endswith(b'\n\n') or
            b'\r' in contents):
        raise ProviderAuthorityStaticEvidenceError(
            f'{name} requires exactly one final LF.')
    value = _json_without_duplicates(contents[:-1], name=name)
    return _closed_dictionary(
        value,
        name=name,
        keys=(_INVENTORY_ROOT_KEYS if name == 'renderer artifact inventory' else
              _CALLABLE_ROOT_KEYS))


def validate_provider_authority_renderer_artifact_inventory_v1(
        contents: bytes
) -> tuple[resource_actions.ProviderRepoArtifactRefV1, ...]:
    """Validate the role-exact five-file installed renderer inventory."""

    if type(contents) is not bytes:
        raise TypeError('Renderer artifact inventory contents must be bytes.')
    value = _parse_canonical_inventory(contents,
                                       name='renderer artifact inventory')
    if (type(value['version']) is not int or value['version'] != 1 or
            value['contract'] != _RENDERER_ARTIFACT_INVENTORY_CONTRACT):
        raise ProviderAuthorityStaticEvidenceError(
            'Renderer artifact inventory contract is unsupported.')
    rows = value['artifacts']
    if type(rows) is not list or len(rows) != len(_RENDERER_ROLE_PATHS):
        raise ProviderAuthorityStaticEvidenceError(
            'Renderer artifact inventory must have exactly five roles.')
    references: list[resource_actions.ProviderRepoArtifactRefV1] = []
    for row, (expected_role, expected_path) in zip(rows, _RENDERER_ROLE_PATHS):
        parsed = _closed_dictionary(row,
                                    name='renderer artifact inventory row',
                                    keys=_INVENTORY_ARTIFACT_KEYS)
        if parsed['role'] != expected_role:
            raise ProviderAuthorityStaticEvidenceError(
                'Renderer artifact inventory role order is invalid.')
        try:
            reference = resource_actions.ProviderRepoArtifactRefV1.from_value({
                key: parsed[key] for key in ('repo_path', 'byte_size', 'sha256')
            })
        except (TypeError, ValueError) as e:
            raise ProviderAuthorityStaticEvidenceError(
                'Renderer artifact inventory reference is invalid.') from e
        if reference.repo_path != expected_path:
            raise ProviderAuthorityStaticEvidenceError(
                'Renderer artifact inventory role uses an unexpected path.')
        _read_installed_artifact(reference)
        references.append(reference)
    if (len({row['role'] for row in rows}) != len(rows) or len(
        {reference.repo_path for reference in references}) != len(references)):
        raise ProviderAuthorityStaticEvidenceError(
            'Renderer artifact inventory roles and paths must be distinct.')
    return tuple(references)


def _callable_identity(callable_value: Any, *, mode: str) -> dict[str, str]:
    module = getattr(callable_value, '__module__', None)
    qualname = getattr(callable_value, '__qualname__', None)
    if (type(module) is not str or type(qualname) is not str or
            '<locals>' in qualname):
        raise ProviderAuthorityStaticEvidenceError(
            'Authority callable identity is not stable and importable.')
    return {'mode': mode, 'module': module, 'qualname': qualname}


def project_provider_authority_worker_callable_inventory_v1(
) -> resource_actions.CanonicalJsonObject:
    """Project the actual resolved four-handler and result-codec registry."""

    expected_names = (
        resource_actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1)
    authority_registrations = tuple(
        registration for registration in request_registry.registered_handlers()
        if registration.claim_scope is
        request_registry.HandlerClaimScope.RESOURCE_ACTION_AUTHORITY)
    registrations_by_name = {
        registration.name: registration
        for registration in authority_registrations
    }
    if (len(authority_registrations) != len(expected_names) or
            set(registrations_by_name) != set(expected_names)):
        raise ProviderAuthorityStaticEvidenceError(
            'Actual authority handler registry is not the closed four-name '
            'inventory.')
    handler_rows: list[dict[str, Any]] = []
    for handler_name in expected_names:
        try:
            registration = request_registry.resolve_handler(handler_name)
        except (TypeError, ValueError) as e:
            raise ProviderAuthorityStaticEvidenceError(
                'Authority handler cannot be resolved from the registry.'
            ) from e
        if registration is not registrations_by_name[handler_name]:
            raise ProviderAuthorityStaticEvidenceError(
                'Authority handler resolution differs from the actual '
                'runtime registry inventory.')
        full_request_name = server_constants.REQUEST_NAME_PREFIX + handler_name
        encoder = encoders.get_encoder(full_request_name)
        decoder = decoders.get_decoder(full_request_name)
        handler_rows.append({
            'name': registration.name,
            'module': registration.func.__module__,
            'qualname': registration.func.__qualname__,
            'execution_class': registration.execution_class.value,
            'claim_scope': registration.claim_scope.value,
            'replay_policy': registration.replay_policy.value,
            'cancellation_policy': registration.cancellation_policy.value,
            'aliases': list(registration.aliases),
            'result_codec': {
                'encoder': _callable_identity(
                    encoder,
                    mode=('default' if encoder is encoders.default_encoder else
                          'registered')),
                'decoder': _callable_identity(
                    decoder,
                    mode=('default'
                          if decoder is decoders.default_decode_handler else
                          'registered')),
                'strict_return_value':
                    encoders.requires_strict_return_value(full_request_name),
            },
        })
    return resource_actions.CanonicalJsonObject({
        'version': 1,
        'contract': _CALLABLE_INVENTORY_CONTRACT,
        'handlers': handler_rows,
    })


def validate_provider_authority_callable_inventory_v1(contents: bytes) -> None:
    """Require the installed inventory to equal the actual runtime registry."""

    if type(contents) is not bytes:
        raise TypeError('Callable inventory contents must be bytes.')
    value = _parse_canonical_inventory(contents, name='callable inventory')
    if (type(value['version']) is not int or value['version'] != 1 or
            value['contract'] != _CALLABLE_INVENTORY_CONTRACT):
        raise ProviderAuthorityStaticEvidenceError(
            'Callable inventory contract is unsupported.')
    rows = value['handlers']
    expected_names = list(
        resource_actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1)
    if type(rows) is not list or len(rows) != len(expected_names):
        raise ProviderAuthorityStaticEvidenceError(
            'Callable inventory must have exactly four handlers.')
    for row, expected_name in zip(rows, expected_names):
        parsed = _closed_dictionary(row,
                                    name='callable inventory handler',
                                    keys=_CALLABLE_HANDLER_KEYS)
        if parsed['name'] != expected_name:
            raise ProviderAuthorityStaticEvidenceError(
                'Callable inventory handler order is invalid.')
        aliases = parsed['aliases']
        if type(aliases) is not list or any(
                type(alias) is not str for alias in aliases):
            raise ProviderAuthorityStaticEvidenceError(
                'Callable inventory aliases must be an exact text array.')
        result_codec = _closed_dictionary(
            parsed['result_codec'],
            name='callable inventory result codec',
            keys=_RESULT_CODEC_KEYS)
        if type(result_codec['strict_return_value']) is not bool:
            raise ProviderAuthorityStaticEvidenceError(
                'Callable inventory strict result-codec flag must be boolean.')
        for field in ('encoder', 'decoder'):
            codec = _closed_dictionary(result_codec[field],
                                       name=f'callable inventory {field}',
                                       keys=_CODEC_IDENTITY_KEYS)
            if (codec['mode'] not in ('default', 'registered') or any(
                    type(codec[key]) is not str
                    for key in ('module', 'qualname'))):
                raise ProviderAuthorityStaticEvidenceError(
                    'Callable inventory codec identity is invalid.')
    if len({row['name'] for row in rows}) != len(rows):
        raise ProviderAuthorityStaticEvidenceError(
            'Callable inventory handler names must be distinct.')
    projected = project_provider_authority_worker_callable_inventory_v1()
    if projected.canonical_bytes != contents[:-1]:
        raise ProviderAuthorityStaticEvidenceError(
            'Callable inventory differs from the actual runtime registry.')


def _validate_qualification_artifact(
    manifest: resource_actions.ProviderAuthorityWorkerCohortManifestV1,
) -> None:
    reference = manifest.image.qualification_artifact
    if reference.mount_path != _QUALIFICATION_PATH:
        raise ProviderAuthorityStaticEvidenceError(
            'Qualification artifact does not use the fixed mount path.')
    contents = _read_fixed_regular_file(_QUALIFICATION_PATH,
                                        name='authority qualification artifact',
                                        maximum_bytes=_MAX_STATIC_JSON_BYTES)
    if (len(contents) != reference.byte_size or
            hashlib.sha256(contents).hexdigest() != reference.sha256):
        raise ProviderAuthorityStaticEvidenceError(
            'Qualification artifact content address differs from the '
            'manifest reference.')
    if (not contents.endswith(b'\n') or contents.endswith(b'\n\n') or
            b'\r' in contents):
        raise ProviderAuthorityStaticEvidenceError(
            'Qualification artifact requires exactly one final LF.')
    value = _json_without_duplicates(contents[:-1],
                                     name='authority qualification artifact')
    if type(value) is not dict or set(value) != _QUALIFICATION_KEYS:
        raise ProviderAuthorityStaticEvidenceError(
            'Qualification artifact has unknown or missing fields.')
    if value['version'] != 1 or type(value['version']) is not int:
        raise ProviderAuthorityStaticEvidenceError(
            'Qualification artifact version is unsupported.')
    source_commit = value['source_commit']
    if (type(source_commit) is not str or len(source_commit) != 40 or any(
            character not in '0123456789abcdef' for character in source_commit)
            or type(sky.__commit__) is not str or len(sky.__commit__) != 40 or
            any(character not in '0123456789abcdef'
                for character in sky.__commit__)):
        raise ProviderAuthorityStaticEvidenceError(
            'Qualification source commit and running release commit must be '
            '40 lowercase hexadecimal characters.')
    expected = {
        'requested_reference': manifest.image.requested_reference,
        'oci_manifest_digest': manifest.image.oci_manifest_digest,
        'oci_config_digest': manifest.image.oci_config_digest,
        'source_commit': sky.__commit__,
        'platform': 'linux/amd64',
    }
    if any(
            type(value[key]) is not str or value[key] != expected_value
            for key, expected_value in expected.items()):
        raise ProviderAuthorityStaticEvidenceError(
            'Qualification artifact does not bind the running qualified '
            'image.')


def load_provider_authority_worker_static_evidence_v1(
) -> resource_actions.ProviderAuthorityWorkerCohortManifestV1:
    """Load and validate the complete fixed static authority evidence graph."""

    manifest_contents = _read_fixed_regular_file(
        _MANIFEST_PATH,
        name='authority cohort manifest',
        maximum_bytes=_MAX_STATIC_JSON_BYTES)
    manifest_value = _json_without_duplicates(manifest_contents,
                                              name='authority cohort manifest')
    try:
        manifest = (resource_actions.ProviderAuthorityWorkerCohortManifestV1.
                    from_value(manifest_value))
    except (TypeError, ValueError) as e:
        raise ProviderAuthorityStaticEvidenceError(
            'Authority cohort manifest contract is invalid.') from e
    if manifest.canonical_bytes != manifest_contents:
        raise ProviderAuthorityStaticEvidenceError(
            'Authority cohort manifest bytes are not canonical.')
    if manifest.pod_template_contract.repo_path != (
            _POD_TEMPLATE_CONTRACT_REPO_PATH):
        raise ProviderAuthorityStaticEvidenceError(
            'Authority Pod-template contract names an unsupported artifact.')
    if manifest.artifact_inventory.repo_path != (
            _RENDERER_ARTIFACT_INVENTORY_REPO_PATH):
        raise ProviderAuthorityStaticEvidenceError(
            'Authority renderer inventory names an unsupported artifact.')
    if manifest.callable_inventory.repo_path != _CALLABLE_INVENTORY_REPO_PATH:
        raise ProviderAuthorityStaticEvidenceError(
            'Authority callable inventory names an unsupported artifact.')
    installed = (manifest.pod_template_contract, manifest.artifact_inventory,
                 manifest.callable_inventory)
    if len({reference.repo_path for reference in installed}) != len(installed):
        raise ProviderAuthorityStaticEvidenceError(
            'Authority installed artifact roles must be distinct.')
    _read_installed_artifact(manifest.pod_template_contract)
    renderer_inventory_contents = _read_installed_artifact(
        manifest.artifact_inventory)
    callable_inventory_contents = _read_installed_artifact(
        manifest.callable_inventory)
    validate_provider_authority_renderer_artifact_inventory_v1(
        renderer_inventory_contents)
    validate_provider_authority_callable_inventory_v1(
        callable_inventory_contents)
    _validate_qualification_artifact(manifest)
    projected = project_provider_authority_worker_pod_template_v1(
        manifest.pod_template_binding.release_inputs)
    if projected.sha256 != manifest.pod_template_binding.expected_template_sha256:
        raise ProviderAuthorityStaticEvidenceError(
            'Authority Pod-template projection hash differs from its binding.')
    return manifest


class InitialProviderPreflightEvaluator:
    """P2a evaluator: unavailable before acceptance, typed NR afterwards."""

    def __init__(
        self,
        accepted_manifest: Callable[
            [],
            resource_actions.ProviderAuthorityWorkerCohortManifestV1 | None],
    ) -> None:
        if not callable(accepted_manifest):
            raise TypeError('accepted_manifest must be callable.')
        self._accepted_manifest = accepted_manifest

    def __call__(
        self, request: resource_actions.ProviderAuthorityPreflightRequestV1
    ) -> resource_actions.ProviderAuthorityPreflightResponseV1 | None:
        if type(request
               ) is not resource_actions.ProviderAuthorityPreflightRequestV1:
            raise TypeError('preflight request has an invalid type.')
        accepted_manifest = self._accepted_manifest()
        if accepted_manifest is None:
            return None
        if type(accepted_manifest) is not (
                resource_actions.ProviderAuthorityWorkerCohortManifestV1):
            raise TypeError('accepted manifest callback returned an invalid '
                            'type.')
        # A mismatched expected manifest is still represented by the sole P2a
        # typed negative result.  P2b may refine this into complete evidence.
        if request.action_kind is kernel_actions.ActionKind.LAUNCH:
            return (resource_actions.ProviderLaunchAuthorityPreflightResponseV1.
                    unavailable(request))
        return (resource_actions.ProviderDownAuthorityPreflightResponseV1.
                unavailable(request))


__all__ = [
    'InitialProviderPreflightEvaluator',
    'ProviderAuthorityStaticEvidenceError',
    'load_provider_authority_worker_static_evidence_v1',
    'materialize_provider_authority_worker_pod_template_v1',
    'project_provider_authority_worker_callable_inventory_v1',
    'project_provider_authority_worker_pod_template_v1',
    'validate_provider_authority_callable_inventory_v1',
    'validate_provider_authority_renderer_artifact_inventory_v1',
]
