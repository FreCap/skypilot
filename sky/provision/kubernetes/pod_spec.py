"""Pure construction of concrete Kubernetes Pod specifications.

Provider discovery and configuration resolution happen before this module is
called.  :func:`finalize_pod_spec` owns the deterministic mutations that turn a
prepared cluster-level Pod template into the exact head or worker Pod submitted
to Kubernetes.
"""

from collections.abc import Mapping
import copy
import dataclasses
from decimal import Decimal
from decimal import InvalidOperation
import posixpath
from typing import Any, Literal

from sky.provision import constants
from sky.provision.kubernetes import constants as k8s_constants
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.utils import config_utils

PodRole = Literal['head', 'worker']

SERVE_WORKER_PROJECTION_PROTOCOL_VERSION = 4
_SUPPORTED_SERVE_WORKER_PROJECTION_PROTOCOL_VERSIONS = frozenset({1, 2, 3, 4})
_STRICT_SERVE_WORKER_PROJECTION_PROTOCOL_VERSIONS = frozenset({2, 3, 4})
_SCRATCH_SERVE_WORKER_PROJECTION_PROTOCOL_VERSIONS = frozenset({3, 4})
_RUNTIME_READY_SERVE_WORKER_PROJECTION_PROTOCOL_VERSIONS = frozenset({4})
SERVE_WORKER_SCRATCH_MOUNT_PATH = '/tmp'
SERVE_WORKER_SCRATCH_VOLUME_NAME = 'skypilot-serve-worker-tmp'
SERVE_WORKER_RUNTIME_READY_MARKER = ('/tmp/skypilot-serve-worker-runtime-ready')
SERVE_WORKER_RUNTIME_READY_POD_UID_ENV_VAR = 'SKYPILOT_POD_UID'
# The startup probe starts only after image pull and container creation.  This
# bound covers the in-container SSH, environment, SkyPilot, and Ray bootstrap;
# provider scheduling retains its separate projected provision_timeout.
SERVE_WORKER_RUNTIME_STARTUP_TIMEOUT_SECONDS = 30 * 60
_SERVE_WORKER_RUNTIME_PROBE_PERIOD_SECONDS = 2
_SERVE_WORKER_RUNTIME_PROBE_TIMEOUT_SECONDS = 1
_SERVE_WORKER_RUNTIME_STARTUP_FAILURE_THRESHOLD = (
    SERVE_WORKER_RUNTIME_STARTUP_TIMEOUT_SECONDS //
    _SERVE_WORKER_RUNTIME_PROBE_PERIOD_SECONDS)
_SERVE_WORKER_RUNTIME_READINESS_FAILURE_THRESHOLD = 1
_SERVE_WORKER_SCRATCH_NONE_KEYS = frozenset({'kind'})
_SERVE_WORKER_SCRATCH_MEMORY_KEYS = frozenset(
    {'kind', 'mount_path', 'volume_name', 'size_limit_bytes'})
_KUBERNETES_QUANTITY_MAX = 2**63 - 1
_KUBERNETES_QUANTITY_EXPONENTS = {
    'n': -3,
    'u': -2,
    'm': -1,
    'K': 1,
    'k': 1,
    'M': 2,
    'G': 3,
    'T': 4,
    'P': 5,
    'E': 6,
}


class ProjectedAcceleratorContractError(ValueError):
    """A projected worker Pod has an ambiguous resource-request shape."""


class ProjectedScratchContractError(ValueError):
    """A projected worker Pod has an ambiguous scratch-volume shape."""


class ProjectedRuntimeReadinessContractError(ValueError):
    """A projected worker Pod has an ambiguous runtime-readiness shape."""


@dataclasses.dataclass(frozen=True)
class ProjectedAcceleratorContract:
    """Whole-Pod accelerator ownership observed at one contract boundary."""

    matches: bool
    ray_node_container_count: int
    ray_node_resource_contract_matches: bool
    unexpected_accelerator_resources: dict[str, object]
    dynamic_resource_claims: dict[str, object]


@dataclasses.dataclass(frozen=True)
class ProjectedScratchContract:
    """Whole-Pod scratch ownership observed at one contract boundary."""

    matches: bool
    expected: dict[str, object]
    actual: dict[str, object]


@dataclasses.dataclass(frozen=True)
class ProjectedRuntimeReadinessContract:
    """Whole-Pod bootstrap readiness observed at one contract boundary."""

    matches: bool
    expected: dict[str, object]
    actual: dict[str, object]


def validate_serve_worker_projection_protocol_version(
    value: object,
    *,
    allow_none: bool = False,
) -> int | None:
    """Validate one persisted Serve worker projection protocol marker."""
    if value is None and allow_none:
        return None
    if (type(value) is not int or
            value not in _SUPPORTED_SERVE_WORKER_PROJECTION_PROTOCOL_VERSIONS):
        raise ValueError('SkyServe worker projection protocol version must be '
                         '1, 2, 3, or 4.')
    return value


def serve_worker_projection_protocol_has_strict_admission(
        value: object) -> bool:
    """Whether a protocol owns scheduler and Kueue admission surfaces."""
    version = validate_serve_worker_projection_protocol_version(value,
                                                                allow_none=True)
    return version in _STRICT_SERVE_WORKER_PROJECTION_PROTOCOL_VERSIONS


def serve_worker_projection_protocol_has_scratch(value: object) -> bool:
    """Whether a protocol carries the closed worker scratch contract."""
    version = validate_serve_worker_projection_protocol_version(value,
                                                                allow_none=True)
    return version in _SCRATCH_SERVE_WORKER_PROJECTION_PROTOCOL_VERSIONS


def serve_worker_projection_protocol_has_provision_timeout(
        value: object) -> bool:
    """Whether a protocol carries the terminal provisioning timeout."""
    version = validate_serve_worker_projection_protocol_version(value,
                                                                allow_none=True)
    return version in _SCRATCH_SERVE_WORKER_PROJECTION_PROTOCOL_VERSIONS


def serve_worker_projection_protocol_has_runtime_readiness(
        value: object) -> bool:
    """Whether a protocol requires UID-bound bootstrap readiness."""
    version = validate_serve_worker_projection_protocol_version(value,
                                                                allow_none=True)
    return version in _RUNTIME_READY_SERVE_WORKER_PROJECTION_PROTOCOL_VERSIONS


def validate_projected_worker_provision_timeout(value: object) -> int:
    """Validate the closed v3/v4 provisioning-wait contract."""
    if type(value) is not int or value < -1:
        raise ValueError('Projected worker provision_timeout must be -1 or a '
                         'non-negative integer.')
    return value


def worker_scratch_mount_path_collides(value: object) -> bool:
    """Whether an absolute mount path overlaps the server-owned /tmp tree."""
    if not isinstance(value, str) or not value.startswith('/'):
        return False
    # Force one POSIX root before normalization so alternate spellings such as
    # //tmp, /tmp/, and /var/../tmp cannot bypass collision detection.  A
    # nested mount would hide part of the bounded memory-backed filesystem, so
    # the complete /tmp subtree is one reserved identity.
    normalized = posixpath.normpath('/' + value.lstrip('/'))
    return (normalized == SERVE_WORKER_SCRATCH_MOUNT_PATH or
            normalized.startswith(f'{SERVE_WORKER_SCRATCH_MOUNT_PATH}/'))


def validate_projected_worker_scratch(value: object) -> dict[str, object]:
    """Validate and copy the closed server-owned worker scratch contract."""
    if not isinstance(value, dict):
        raise ProjectedScratchContractError(
            'Worker scratch contract must be a mapping.')
    kind = value.get('kind')
    if kind == 'none':
        if set(value) != _SERVE_WORKER_SCRATCH_NONE_KEYS:
            raise ProjectedScratchContractError(
                'Worker scratch kind none cannot contain other fields.')
        return {'kind': 'none'}
    if kind != 'memory' or set(value) != _SERVE_WORKER_SCRATCH_MEMORY_KEYS:
        raise ProjectedScratchContractError(
            'Worker scratch contract must be exactly none or memory-backed '
            '/tmp.')
    if value['mount_path'] != SERVE_WORKER_SCRATCH_MOUNT_PATH:
        raise ProjectedScratchContractError(
            'Worker memory scratch mount_path must be exactly '
            f'{SERVE_WORKER_SCRATCH_MOUNT_PATH!r}.')
    if value['volume_name'] != SERVE_WORKER_SCRATCH_VOLUME_NAME:
        raise ProjectedScratchContractError(
            'Worker memory scratch volume_name must be exactly '
            f'{SERVE_WORKER_SCRATCH_VOLUME_NAME!r}.')
    size_limit_bytes = value['size_limit_bytes']
    if (type(size_limit_bytes) is not int or size_limit_bytes < 1 or
            size_limit_bytes > _KUBERNETES_QUANTITY_MAX):
        raise ProjectedScratchContractError(
            'Worker memory scratch size_limit_bytes must be a positive '
            'Kubernetes quantity byte count.')
    return {
        'kind': 'memory',
        'mount_path': SERVE_WORKER_SCRATCH_MOUNT_PATH,
        'volume_name': SERVE_WORKER_SCRATCH_VOLUME_NAME,
        'size_limit_bytes': size_limit_bytes,
    }


def _pod_api_field(owner: object, yaml_name: str, api_name: str) -> Any:
    """Reads one YAML or Kubernetes-client-model field without coercion."""
    if isinstance(owner, Mapping):
        return owner.get(yaml_name)
    try:
        state = vars(owner)
    except TypeError:
        return None
    if api_name in state:
        return state[api_name]
    # ``exec`` is exposed by the Kubernetes client as the ``_exec`` property,
    # backed by a name-mangled attribute.  Restrict property access to real
    # descriptors so a loose mock cannot synthesize a missing field.
    descriptor = getattr(type(owner), api_name, None)
    if isinstance(descriptor, property):
        return getattr(owner, api_name)
    # kubernetes.client models expose public properties backed by private
    # fields (for example ``containers`` -> ``_containers``). Read the stored
    # value directly so generic mocks cannot synthesize a truthy missing field.
    return state.get(f'_{api_name}')


def _present_json_fields(value: object) -> set[str]:
    """Return non-null JSON fields for mappings or Kubernetes API models."""
    if isinstance(value, Mapping):
        return {
            str(key)
            for key, field_value in value.items()
            if field_value is not None
        }
    attribute_map = getattr(value, 'attribute_map', None)
    if not isinstance(attribute_map, Mapping):
        return set()
    return {
        str(json_field)
        for attribute, json_field in attribute_map.items()
        if _pod_api_field(value, str(json_field), str(attribute)) is not None
    }


def _parse_kubernetes_quantity(value: object) -> Decimal:
    """Parse the SI forms used by Kubernetes Quantity without an SDK import."""
    if isinstance(value, bool):
        raise ValueError('Boolean is not a Kubernetes quantity.')
    if isinstance(value, (int, float, Decimal)):
        return Decimal(value)
    quantity = str(value)
    number = quantity
    suffix = None
    if len(quantity) >= 2 and quantity[-1] == 'i':
        if quantity[-2] in _KUBERNETES_QUANTITY_EXPONENTS:
            number = quantity[:-2]
            suffix = quantity[-2:]
    elif quantity and quantity[-1] in _KUBERNETES_QUANTITY_EXPONENTS:
        number = quantity[:-1]
        suffix = quantity[-1:]
    try:
        parsed = Decimal(number)
    except InvalidOperation as error:
        raise ValueError(f'Invalid Kubernetes quantity {value!r}.') from error
    if suffix is None:
        return parsed
    if suffix == 'ki':
        raise ValueError(f'Unknown Kubernetes quantity suffix in {value!r}.')
    base = 1024 if suffix.endswith('i') else 1000
    return parsed * (base**_KUBERNETES_QUANTITY_EXPONENTS[suffix[0]])


def _observe_projected_worker_scratch(pod_spec: object) -> dict[str, object]:
    """Extract every reserved scratch identity from one Pod spec shape."""
    malformed = pod_spec is None
    volumes = _pod_api_field(pod_spec, 'volumes', 'volumes')
    if volumes is None:
        volumes = []
    reserved_volumes = []
    if isinstance(volumes, (list, tuple)):
        for volume in volumes:
            name = _pod_api_field(volume, 'name', 'name')
            if name != SERVE_WORKER_SCRATCH_VOLUME_NAME:
                continue
            empty_dir = _pod_api_field(volume, 'emptyDir', 'empty_dir')
            size_limit = _pod_api_field(empty_dir, 'sizeLimit', 'size_limit')
            reserved_volumes.append({
                'name': name,
                'medium': _pod_api_field(empty_dir, 'medium', 'medium'),
                'size_limit': (None if size_limit is None else str(size_limit)),
                'extra_fields':
                    sorted(_present_json_fields(volume) - {'name', 'emptyDir'}),
                'empty_dir_extra_fields': sorted(
                    _present_json_fields(empty_dir) - {'medium', 'sizeLimit'}),
            })
    else:
        malformed = True

    reserved_mounts = []
    reserved_devices = []
    ray_node_container_count = 0
    container_fields = (
        ('containers', 'containers', 'containers'),
        ('initContainers', 'init_containers', 'initContainers'),
        ('ephemeralContainers', 'ephemeral_containers', 'ephemeralContainers'),
    )
    for yaml_field, api_field, scope in container_fields:
        containers = _pod_api_field(pod_spec, yaml_field, api_field)
        if containers is None:
            if scope == 'containers':
                malformed = True
            containers = []
        if not isinstance(containers, (list, tuple)):
            malformed = True
            continue
        for container in containers:
            container_name = _pod_api_field(container, 'name', 'name')
            if scope == 'containers' and container_name == 'ray-node':
                ray_node_container_count += 1
            mounts = _pod_api_field(container, 'volumeMounts', 'volume_mounts')
            if mounts is None:
                mounts = []
            if not isinstance(mounts, (list, tuple)):
                malformed = True
                continue
            for mount in mounts:
                name = _pod_api_field(mount, 'name', 'name')
                mount_path = _pod_api_field(mount, 'mountPath', 'mount_path')
                if (name != SERVE_WORKER_SCRATCH_VOLUME_NAME and
                        not worker_scratch_mount_path_collides(mount_path)):
                    continue
                reserved_mounts.append({
                    'scope': scope,
                    'container_name': container_name,
                    'name': name,
                    'mount_path': mount_path,
                    'read_only': _pod_api_field(mount, 'readOnly', 'read_only'),
                    'sub_path': _pod_api_field(mount, 'subPath', 'sub_path'),
                    'sub_path_expr': _pod_api_field(mount, 'subPathExpr',
                                                    'sub_path_expr'),
                    'mount_propagation': _pod_api_field(mount,
                                                        'mountPropagation',
                                                        'mount_propagation'),
                    'recursive_read_only': _pod_api_field(
                        mount, 'recursiveReadOnly', 'recursive_read_only'),
                    'extra_fields': sorted(
                        _present_json_fields(mount) - {
                            'name', 'mountPath', 'readOnly', 'subPath',
                            'subPathExpr', 'mountPropagation',
                            'recursiveReadOnly'
                        }),
                })
            devices = _pod_api_field(container, 'volumeDevices',
                                     'volume_devices')
            if devices is None:
                devices = []
            if not isinstance(devices, (list, tuple)):
                malformed = True
                continue
            for device in devices:
                if (_pod_api_field(device, 'name',
                                   'name') == SERVE_WORKER_SCRATCH_VOLUME_NAME):
                    reserved_devices.append({
                        'scope': scope,
                        'container_name': container_name,
                        'name': SERVE_WORKER_SCRATCH_VOLUME_NAME,
                        'device_path': _pod_api_field(device, 'devicePath',
                                                      'device_path'),
                    })
    return {
        'malformed': malformed,
        'ray_node_container_count': ray_node_container_count,
        'volumes': reserved_volumes,
        'mounts': reserved_mounts,
        'devices': reserved_devices,
    }


def _projected_worker_scratch_matches(actual: dict[str, object],
                                      expected: dict[str, object]) -> bool:
    if actual['malformed'] or actual['ray_node_container_count'] != 1:
        return False
    volumes = actual['volumes']
    mounts = actual['mounts']
    devices = actual['devices']
    assert isinstance(volumes, list)
    assert isinstance(mounts, list)
    assert isinstance(devices, list)
    if expected['kind'] == 'none':
        return not volumes and not mounts and not devices
    volume_matches = False
    if len(volumes) == 1:
        scratch_volume = volumes[0]
        assert isinstance(scratch_volume, dict)
        try:
            parsed_size = _parse_kubernetes_quantity(
                scratch_volume['size_limit'])
        except (TypeError, ValueError):
            parsed_size = None
        volume_matches = (scratch_volume['name']
                          == SERVE_WORKER_SCRATCH_VOLUME_NAME and
                          scratch_volume['medium'] == 'Memory' and
                          parsed_size == expected['size_limit_bytes'] and
                          not scratch_volume['extra_fields'] and
                          not scratch_volume['empty_dir_extra_fields'])
    if len(mounts) != 1:
        return False
    scratch_mount = mounts[0]
    assert isinstance(scratch_mount, dict)
    mount_matches = (
        scratch_mount['scope'] == 'containers' and
        scratch_mount['container_name'] == 'ray-node' and
        scratch_mount['name'] == SERVE_WORKER_SCRATCH_VOLUME_NAME and
        scratch_mount['mount_path'] == SERVE_WORKER_SCRATCH_MOUNT_PATH and
        scratch_mount['read_only'] in (None, False) and
        scratch_mount['sub_path'] in (None, '') and
        scratch_mount['sub_path_expr'] in (None, '') and
        scratch_mount['mount_propagation'] in (None, '') and
        scratch_mount['recursive_read_only'] in (None, 'Disabled') and
        not scratch_mount['extra_fields'])
    return volume_matches and mount_matches and not devices


def enforce_projected_worker_scratch_contract(
    pod_spec: object,
    expected_scratch: object,
    *,
    rewrite: bool,
) -> ProjectedScratchContract:
    """Own the complete memory-backed ``/tmp`` surface for one worker Pod.

    ``rewrite=True`` canonicalizes mutable YAML and refuses every pre-existing
    collision with the reserved volume name or the normalized ``/tmp`` tree.
    ``rewrite=False`` observes the same surfaces on an admitted Kubernetes API
    object without mutation and returns an exact attestation result.
    """
    expected = validate_projected_worker_scratch(expected_scratch)
    if rewrite:
        if not isinstance(pod_spec, dict):
            raise ProjectedScratchContractError(
                'Projected SkyServe Kubernetes Pod spec must be a mapping.')
        memory_backed = expected['kind'] == 'memory'
        expected_volume = None if not memory_backed else {
            'name': SERVE_WORKER_SCRATCH_VOLUME_NAME,
            'emptyDir': {
                'medium': 'Memory',
                'sizeLimit': str(expected['size_limit_bytes']),
            },
        }
        expected_mount = None if not memory_backed else {
            'name': SERVE_WORKER_SCRATCH_VOLUME_NAME,
            'mountPath': SERVE_WORKER_SCRATCH_MOUNT_PATH,
        }
        volumes = pod_spec.get('volumes')
        if volumes is None:
            volumes = []
        if not isinstance(volumes, list) or any(
                not isinstance(volume, dict) for volume in volumes):
            raise ProjectedScratchContractError(
                'Projected SkyServe Kubernetes Pod volumes must be a list of '
                'mappings.')
        owned_volumes = [
            volume for volume in volumes
            if volume.get('name') == SERVE_WORKER_SCRATCH_VOLUME_NAME
        ]
        if owned_volumes and (not memory_backed or len(owned_volumes) != 1 or
                              owned_volumes[0] != expected_volume):
            raise ProjectedScratchContractError(
                'Projected SkyServe worker scratch volume identity collides '
                'with an existing Pod volume.')

        runtime_containers = []
        for container_field in ('containers', 'initContainers',
                                'ephemeralContainers'):
            containers = pod_spec.get(container_field)
            if containers is None:
                containers = []
            if not isinstance(containers, list) or any(
                    not isinstance(container, dict)
                    for container in containers):
                raise ProjectedScratchContractError(
                    f'Projected SkyServe Kubernetes {container_field} must '
                    'be a list of mappings.')
            for container in containers:
                is_runtime = (container_field == 'containers' and
                              container.get('name') == 'ray-node')
                if is_runtime:
                    runtime_containers.append(container)
                mounts = container.get('volumeMounts')
                if mounts is None:
                    mounts = []
                if not isinstance(mounts, list) or any(
                        not isinstance(mount, dict) for mount in mounts):
                    raise ProjectedScratchContractError(
                        'Projected SkyServe Kubernetes volumeMounts must be a '
                        'list of mappings.')
                matching_mounts = []
                for mount in mounts:
                    owns_identity = (mount.get('name')
                                     == SERVE_WORKER_SCRATCH_VOLUME_NAME or
                                     worker_scratch_mount_path_collides(
                                         mount.get('mountPath')))
                    if owns_identity:
                        matching_mounts.append(mount)
                if matching_mounts and (not memory_backed or not is_runtime or
                                        len(matching_mounts) != 1 or
                                        matching_mounts[0] != expected_mount):
                    raise ProjectedScratchContractError(
                        'Projected SkyServe worker scratch mount identity '
                        'collides with an existing container mount.')
                devices = container.get('volumeDevices')
                if devices is None:
                    devices = []
                if not isinstance(devices, list) or any(
                        not isinstance(device, dict) for device in devices):
                    raise ProjectedScratchContractError(
                        'Projected SkyServe Kubernetes volumeDevices must be a '
                        'list of mappings.')
                if any(
                        device.get('name') == SERVE_WORKER_SCRATCH_VOLUME_NAME
                        for device in devices):
                    raise ProjectedScratchContractError(
                        'Projected SkyServe worker scratch volume identity '
                        'collides with a container volumeDevice.')

        if memory_backed:
            if len(runtime_containers) != 1:
                raise ProjectedScratchContractError(
                    'Projected SkyServe Kubernetes Pods must contain exactly '
                    'one ray-node container.')
            volumes[:] = [
                volume for volume in volumes
                if volume.get('name') != SERVE_WORKER_SCRATCH_VOLUME_NAME
            ]
            assert expected_volume is not None
            volumes.append(expected_volume)
            pod_spec['volumes'] = volumes
            runtime_mounts = runtime_containers[0].get('volumeMounts')
            if runtime_mounts is None:
                runtime_mounts = []
            assert isinstance(runtime_mounts, list)
            assert expected_mount is not None
            runtime_mounts[:] = [
                mount for mount in runtime_mounts if mount != expected_mount
            ]
            runtime_mounts.append(expected_mount)
            runtime_containers[0]['volumeMounts'] = runtime_mounts
    actual = _observe_projected_worker_scratch(pod_spec)
    return ProjectedScratchContract(
        matches=_projected_worker_scratch_matches(actual, expected),
        expected=expected,
        actual=actual,
    )


def _projected_worker_runtime_ready_probe() -> dict[str, object]:
    command = [
        '/bin/sh', '-c',
        ('test -n "$SKYPILOT_POD_UID" && '
         'test "$(cat '
         f'{SERVE_WORKER_RUNTIME_READY_MARKER} 2>/dev/null)" = '
         '"$SKYPILOT_POD_UID"')
    ]
    return {
        'exec': {
            'command': command,
        },
        'initialDelaySeconds': 0,
        'periodSeconds': _SERVE_WORKER_RUNTIME_PROBE_PERIOD_SECONDS,
        'timeoutSeconds': _SERVE_WORKER_RUNTIME_PROBE_TIMEOUT_SECONDS,
        'successThreshold': 1,
    }


def _expected_projected_worker_runtime_readiness() -> dict[str, object]:
    startup_probe = _projected_worker_runtime_ready_probe()
    startup_probe['failureThreshold'] = (
        _SERVE_WORKER_RUNTIME_STARTUP_FAILURE_THRESHOLD)
    readiness_probe = _projected_worker_runtime_ready_probe()
    readiness_probe['failureThreshold'] = (
        _SERVE_WORKER_RUNTIME_READINESS_FAILURE_THRESHOLD)
    startup_exec = startup_probe['exec']
    readiness_exec = readiness_probe['exec']
    assert isinstance(startup_exec, dict)
    assert isinstance(readiness_exec, dict)
    return {
        'restart_policy': 'Never',
        'pod_uid_env': [{
            'name': SERVE_WORKER_RUNTIME_READY_POD_UID_ENV_VAR,
            'value_from': {
                'field_ref': {
                    'api_version': 'v1',
                    'field_path': 'metadata.uid',
                    'extra_fields': [],
                },
                'extra_fields': [],
            },
            'extra_fields': [],
        }],
        'startup_probe': {
            **startup_probe,
            'extra_fields': [],
            'exec': {
                **startup_exec,
                'extra_fields': [],
            },
        },
        'readiness_probe': {
            **readiness_probe,
            'extra_fields': [],
            'exec': {
                **readiness_exec,
                'extra_fields': [],
            },
        },
        'ray_node_container_count': 1,
    }


def _observe_projected_worker_runtime_probe(
        probe: object) -> dict[str, object] | None:
    if probe is None:
        return None
    exec_action = _pod_api_field(probe, 'exec', '_exec')
    command = _pod_api_field(exec_action, 'command', 'command')
    if isinstance(command, (list, tuple)):
        command = list(command)
    initial_delay_seconds = _pod_api_field(probe, 'initialDelaySeconds',
                                           'initial_delay_seconds')
    return {
        'exec': {
            'command': command,
            'extra_fields':
                sorted(_present_json_fields(exec_action) - {'command'}),
        },
        # The API may omit the zero-valued default from a read response.
        'initialDelaySeconds':
            (0 if initial_delay_seconds is None else initial_delay_seconds),
        'periodSeconds': _pod_api_field(probe, 'periodSeconds',
                                        'period_seconds'),
        'timeoutSeconds': _pod_api_field(probe, 'timeoutSeconds',
                                         'timeout_seconds'),
        'successThreshold': _pod_api_field(probe, 'successThreshold',
                                           'success_threshold'),
        'failureThreshold': _pod_api_field(probe, 'failureThreshold',
                                           'failure_threshold'),
        'extra_fields': sorted(
            _present_json_fields(probe) - {
                'exec', 'initialDelaySeconds', 'periodSeconds',
                'timeoutSeconds', 'successThreshold', 'failureThreshold'
            }),
    }


def _observe_projected_worker_runtime_env(entry: object) -> dict[str, object]:
    value_from = _pod_api_field(entry, 'valueFrom', 'value_from')
    field_ref = _pod_api_field(value_from, 'fieldRef', 'field_ref')
    api_version = _pod_api_field(field_ref, 'apiVersion', 'api_version')
    return {
        'name': _pod_api_field(entry, 'name', 'name'),
        'value_from': {
            'field_ref': {
                # Kubernetes defaults this omitted field to v1.
                'api_version': 'v1' if api_version is None else api_version,
                'field_path': _pod_api_field(field_ref, 'fieldPath',
                                             'field_path'),
                'extra_fields': sorted(
                    _present_json_fields(field_ref) -
                    {'apiVersion', 'fieldPath'}),
            },
            'extra_fields':
                sorted(_present_json_fields(value_from) - {'fieldRef'}),
        },
        'extra_fields':
            sorted(_present_json_fields(entry) - {'name', 'valueFrom'}),
    }


def _observe_projected_worker_runtime_readiness(
        pod_spec: object) -> dict[str, object]:
    containers = _pod_api_field(pod_spec, 'containers', 'containers')
    if not isinstance(containers, (list, tuple)):
        containers = []
    runtime_containers = [
        container for container in containers
        if _pod_api_field(container, 'name', 'name') == 'ray-node'
    ]
    runtime = runtime_containers[0] if len(runtime_containers) == 1 else None
    env = _pod_api_field(runtime, 'env', 'env')
    if not isinstance(env, (list, tuple)):
        env = []
    pod_uid_env = [
        _observe_projected_worker_runtime_env(entry)
        for entry in env
        if _pod_api_field(entry, 'name', 'name') ==
        SERVE_WORKER_RUNTIME_READY_POD_UID_ENV_VAR
    ]
    return {
        'restart_policy': _pod_api_field(pod_spec, 'restartPolicy',
                                         'restart_policy'),
        'pod_uid_env': pod_uid_env,
        'startup_probe': _observe_projected_worker_runtime_probe(
            _pod_api_field(runtime, 'startupProbe', 'startup_probe')),
        'readiness_probe': _observe_projected_worker_runtime_probe(
            _pod_api_field(runtime, 'readinessProbe', 'readiness_probe')),
        'ray_node_container_count': len(runtime_containers),
    }


def enforce_projected_worker_runtime_readiness_contract(
    pod_spec: object,
    *,
    rewrite: bool,
) -> ProjectedRuntimeReadinessContract:
    """Own the UID-bound bootstrap readiness surface for one worker Pod.

    The marker writer lives in the canonical Kubernetes bootstrap template.
    This contract owns its downward-API UID input and both kubelet probes, so a
    merge, webhook, or same-name replacement cannot turn mere Running state
    into projected-worker provisioning success.
    """
    expected = _expected_projected_worker_runtime_readiness()
    if rewrite:
        if not isinstance(pod_spec, dict):
            raise ProjectedRuntimeReadinessContractError(
                'Projected SkyServe Kubernetes Pod spec must be a mapping.')
        restart_policy = pod_spec.get('restartPolicy')
        if restart_policy not in (None, 'Never'):
            raise ProjectedRuntimeReadinessContractError(
                'Projected SkyServe worker runtime readiness requires '
                'restartPolicy Never.')
        pod_spec['restartPolicy'] = 'Never'
        containers = pod_spec.get('containers')
        if (not isinstance(containers, list) or any(
                not isinstance(container, dict) for container in containers)):
            raise ProjectedRuntimeReadinessContractError(
                'Projected SkyServe Kubernetes containers must be a list of '
                'mappings.')
        runtime_containers = [
            container for container in containers
            if container.get('name') == 'ray-node'
        ]
        if len(runtime_containers) != 1:
            raise ProjectedRuntimeReadinessContractError(
                'Projected SkyServe Kubernetes Pods must contain exactly one '
                'ray-node container.')
        runtime = runtime_containers[0]
        env = runtime.get('env')
        if env is None:
            env = []
        if (not isinstance(env, list) or
                any(not isinstance(entry, dict) for entry in env)):
            raise ProjectedRuntimeReadinessContractError(
                'Projected SkyServe Kubernetes env must be a list of '
                'mappings.')
        uid_entries = [
            entry for entry in env
            if entry.get('name') == SERVE_WORKER_RUNTIME_READY_POD_UID_ENV_VAR
        ]
        expected_uid = expected['pod_uid_env']
        if uid_entries and [
                _observe_projected_worker_runtime_env(entry)
                for entry in uid_entries
        ] != expected_uid:
            raise ProjectedRuntimeReadinessContractError(
                'Projected SkyServe worker Pod UID environment identity '
                'collides with the runtime-readiness contract.')
        env[:] = [
            entry for entry in env
            if entry.get('name') != SERVE_WORKER_RUNTIME_READY_POD_UID_ENV_VAR
        ]
        env.append({
            'name': SERVE_WORKER_RUNTIME_READY_POD_UID_ENV_VAR,
            'valueFrom': {
                'fieldRef': {
                    'apiVersion': 'v1',
                    'fieldPath': 'metadata.uid',
                },
            },
        })
        runtime['env'] = env
        for yaml_field, expected_key in (('startupProbe', 'startup_probe'),
                                         ('readinessProbe', 'readiness_probe')):
            existing = runtime.get(yaml_field)
            if (existing is not None and
                    _observe_projected_worker_runtime_probe(existing)
                    != expected[expected_key]):
                raise ProjectedRuntimeReadinessContractError(
                    'Projected SkyServe worker runtime probe identity '
                    f'collides at {yaml_field}.')
            observed_expected = expected[expected_key]
            assert isinstance(observed_expected, dict)
            probe = {
                key: copy.deepcopy(value)
                for key, value in observed_expected.items()
                if key != 'extra_fields'
            }
            exec_action = probe['exec']
            assert isinstance(exec_action, dict)
            exec_action.pop('extra_fields', None)
            runtime[yaml_field] = probe
    actual = _observe_projected_worker_runtime_readiness(pod_spec)
    return ProjectedRuntimeReadinessContract(matches=actual == expected,
                                             expected=expected,
                                             actual=actual)


def _resource_mapping(owner: object, section: str, location: str,
                      rewrite: bool) -> dict[str, Any] | Mapping[str, Any]:
    resources = _pod_api_field(owner, 'resources', 'resources')
    if resources is None and rewrite:
        if not isinstance(owner, dict):
            raise ProjectedAcceleratorContractError(
                f'{location} must be a mapping.')
        resources = {}
        owner['resources'] = resources
    if resources is None:
        return {}
    if rewrite and not isinstance(resources, dict):
        raise ProjectedAcceleratorContractError(
            f'{location} resources must be a mapping.')
    values = _pod_api_field(resources, section, section)
    if values is None and rewrite:
        assert isinstance(resources, dict)
        values = {}
        resources[section] = values
    if values is None:
        return {}
    if not isinstance(values, Mapping):
        raise ProjectedAcceleratorContractError(
            f'{location} resource requests and limits must be mappings.')
    if rewrite and not isinstance(values, dict):
        raise ProjectedAcceleratorContractError(
            f'{location} resource requests and limits must be mutable '
            'mappings.')
    return values


def _nonempty_resource_claims(owner: object, location: str) -> object | None:
    resources = _pod_api_field(owner, 'resources', 'resources')
    if resources is None:
        return None
    claims = _pod_api_field(resources, 'claims', 'claims')
    if claims is None:
        return None
    if not isinstance(claims, (list, tuple)):
        raise ProjectedAcceleratorContractError(
            f'{location} resource claims must be a list.')
    return claims or None


def enforce_projected_accelerator_contract(
    pod_spec: object,
    expected_resource_key: str,
    expected_accelerator_count: object,
    *,
    rewrite: bool,
) -> ProjectedAcceleratorContract:
    """Owns every accelerator-request surface for a projected worker Pod.

    ``rewrite=True`` canonicalizes a mutable YAML Pod spec: all supported
    accelerator resources are removed from every Pod surface and the exact
    projected request is installed on the sole ``ray-node`` container.
    Dynamic Resource Allocation is rejected because an opaque claim can select
    a device outside this resource-key contract.

    ``rewrite=False`` performs the same traversal on an admitted Kubernetes
    object without mutation and returns the complete attestation result.
    """
    if rewrite and not isinstance(pod_spec, dict):
        raise ProjectedAcceleratorContractError(
            'Projected SkyServe Kubernetes Pod spec must be a mapping.')
    accelerator_resource_keys = {
        *kubernetes_utils.SUPPORTED_GPU_RESOURCE_KEYS.values(),
        kubernetes_utils.TPU_RESOURCE_KEY,
        expected_resource_key,
    }
    expected_quantity = str(expected_accelerator_count)
    dynamic_resource_claims: dict[str, object] = {}
    pod_claims = _pod_api_field(pod_spec, 'resourceClaims', 'resource_claims')
    if pod_claims is not None:
        if not isinstance(pod_claims, (list, tuple)):
            raise ProjectedAcceleratorContractError(
                'Pod resourceClaims must be a list.')
        if pod_claims:
            dynamic_resource_claims['pod'] = pod_claims

    raw_containers = _pod_api_field(pod_spec, 'containers', 'containers')
    if not isinstance(raw_containers, list):
        raise ProjectedAcceleratorContractError(
            'Projected SkyServe Kubernetes containers must be a list.')
    raw_init_containers = _pod_api_field(pod_spec, 'initContainers',
                                         'init_containers')
    if raw_init_containers is None:
        raw_init_containers = []
    if not isinstance(raw_init_containers, list):
        raise ProjectedAcceleratorContractError(
            'Projected SkyServe Kubernetes initContainers must be a list.')
    if rewrite and (any(not isinstance(container, dict)
                        for container in raw_containers) or
                    any(not isinstance(container, dict)
                        for container in raw_init_containers)):
        raise ProjectedAcceleratorContractError(
            'Projected SkyServe Kubernetes containers and initContainers '
            'must contain mappings.')

    unexpected: dict[str, object] = {}
    ray_node_container_count = 0
    ray_node_resource_contract_matches = False

    def _inspect_container(container: object, location: str,
                           is_runtime_container: bool) -> None:
        nonlocal ray_node_container_count
        nonlocal ray_node_resource_contract_matches
        claims = _nonempty_resource_claims(container, location)
        if claims is not None:
            dynamic_resource_claims[location] = claims
        requests = _resource_mapping(container, 'requests', location, rewrite)
        limits = _resource_mapping(container, 'limits', location, rewrite)
        if rewrite:
            assert isinstance(requests, dict)
            assert isinstance(limits, dict)
            for resource_key in accelerator_resource_keys:
                requests.pop(resource_key, None)
                limits.pop(resource_key, None)
            if is_runtime_container:
                requests[expected_resource_key] = expected_accelerator_count
                limits[expected_resource_key] = expected_accelerator_count
                ray_node_resource_contract_matches = True
            return
        entries: dict[str, dict[str, str]] = {}
        for section, values in (('requests', requests), ('limits', limits)):
            selected = {
                str(key): str(value)
                for key, value in values.items()
                if key in accelerator_resource_keys
            }
            if selected:
                entries[section] = selected
        if is_runtime_container:
            ray_node_resource_contract_matches = entries == {
                'requests': {
                    expected_resource_key: expected_quantity,
                },
                'limits': {
                    expected_resource_key: expected_quantity,
                },
            }
        elif entries:
            unexpected[location] = entries

    for index, container in enumerate(raw_containers):
        name = _pod_api_field(container, 'name', 'name')
        is_runtime_container = name == 'ray-node'
        if is_runtime_container:
            ray_node_container_count += 1
        _inspect_container(container, f'container[{index}]',
                           is_runtime_container)
    for index, container in enumerate(raw_init_containers):
        _inspect_container(container, f'init_container[{index}]', False)

    pod_resources = _pod_api_field(pod_spec, 'resources', 'resources')
    if pod_resources is not None:
        if rewrite and not isinstance(pod_resources, dict):
            raise ProjectedAcceleratorContractError(
                'Projected SkyServe Kubernetes Pod resources must be a '
                'mapping.')
        for section in ('requests', 'limits'):
            values = _pod_api_field(pod_resources, section, section)
            if values is None:
                continue
            if not isinstance(values, Mapping):
                raise ProjectedAcceleratorContractError(
                    'Projected SkyServe Kubernetes Pod resource requests and '
                    'limits must be mappings.')
            selected = {
                str(key): str(value)
                for key, value in values.items()
                if key in accelerator_resource_keys
            }
            if rewrite:
                if not isinstance(values, dict):
                    raise ProjectedAcceleratorContractError(
                        'Projected SkyServe Kubernetes Pod resource requests '
                        'and limits must be mutable mappings.')
                for resource_key in accelerator_resource_keys:
                    values.pop(resource_key, None)
            elif selected:
                unexpected[f'pod_resources.{section}'] = selected

    overhead = _pod_api_field(pod_spec, 'overhead', 'overhead')
    if overhead is not None:
        if not isinstance(overhead, Mapping):
            raise ProjectedAcceleratorContractError(
                'Projected SkyServe Kubernetes Pod overhead must be a '
                'mapping.')
        selected = {
            str(key): str(value)
            for key, value in overhead.items()
            if key in accelerator_resource_keys
        }
        if rewrite:
            if not isinstance(overhead, dict):
                raise ProjectedAcceleratorContractError(
                    'Projected SkyServe Kubernetes Pod overhead must be a '
                    'mutable mapping.')
            for resource_key in accelerator_resource_keys:
                overhead.pop(resource_key, None)
        elif selected:
            unexpected['overhead'] = selected

    matches = (ray_node_container_count == 1 and
               ray_node_resource_contract_matches and not unexpected and
               not dynamic_resource_claims)
    return ProjectedAcceleratorContract(
        matches=matches,
        ray_node_container_count=ray_node_container_count,
        ray_node_resource_contract_matches=(ray_node_resource_contract_matches),
        unexpected_accelerator_resources=unexpected,
        dynamic_resource_claims=dynamic_resource_claims,
    )


def _head_service_selector(cluster_name: str) -> dict[str, str]:
    """Returns the canonical selector shared by head Pods and Services."""
    return {'component': f'{cluster_name}-head'}


def _configure_runtime_class(pod_spec: dict[str,
                                            Any], nvidia_runtime_exists: bool,
                             needs_gpus_nvidia: bool) -> None:
    """Sets or strips runtimeClassName on ``pod_spec`` in place.

    A falsy runtimeClassName means the user explicitly disabled the runtime
    class.  Kubernetes rejects an empty runtimeClassName, and the explicit
    override must also suppress the automatic ``nvidia`` assignment.
    """
    spec = pod_spec['spec']
    if 'runtimeClassName' in spec and not spec['runtimeClassName']:
        del spec['runtimeClassName']
        return
    if (nvidia_runtime_exists and needs_gpus_nvidia and
            'runtimeClassName' not in spec):
        spec['runtimeClassName'] = 'nvidia'


def finalize_pod_spec(
    base_pod_spec: Mapping[str, Any],
    *,
    role: PodRole,
    pod_name: str,
    cluster_name_on_cloud: str,
    node_count: int,
    nvidia_runtime_exists: bool,
    needs_gpus: bool,
    needs_gpus_nvidia: bool,
    gpu_resource_key: str,
    needs_tpu: bool,
    resolved_base_affinity: Mapping[str, Any] | None,
    docker_config: kubernetes_utils.DockerConfig | None,
    docker_pvc_name: str | None,
    context: str | None,
    namespace: str,
    deployment_name: str | None = None,
) -> dict[str, Any]:
    """Builds one concrete Pod spec without mutating any input.

    All provider-dependent values are explicit arguments.  In particular,
    ``nvidia_runtime_exists`` and ``gpu_resource_key`` are discovered before
    this boundary, ``docker_pvc_name`` is resolved from user state, and
    ``resolved_base_affinity`` is the template affinity after the
    ``allowed_nodes`` policy has resolved any node names or IPs.
    """
    pod_spec: dict[str, Any] = copy.deepcopy(dict(base_pod_spec))
    spec = pod_spec['spec']

    if resolved_base_affinity is not None:
        spec['affinity'] = copy.deepcopy(resolved_base_affinity)

    _configure_runtime_class(pod_spec, nvidia_runtime_exists, needs_gpus_nvidia)

    metadata = pod_spec['metadata']
    labels = metadata['labels']
    metadata['name'] = pod_name
    if role == 'head':
        labels.update(constants.HEAD_NODE_TAGS)
        labels.update(_head_service_selector(cluster_name_on_cloud))
    else:
        labels.update(constants.WORKER_NODE_TAGS)
        labels['component'] = pod_name

    if deployment_name is not None:
        labels[k8s_constants.TAG_SKYPILOT_DEPLOYMENT_NAME] = deployment_name

    if docker_config is not None:
        kubernetes_utils.inject_docker_cache_volume(
            pod_spec=pod_spec,
            docker_config=docker_config,
            pvc_name=docker_pvc_name,
            context=context,
            namespace=namespace,
        )

    # Keep placement fields identical for head and workers so Kueue can merge
    # them into one PodSet for queued provisioning.  Only role metadata differs.
    if node_count > 1:
        # Prefer distinct physical nodes while allowing co-location when the
        # cluster has no other schedulable capacity.
        pod_spec_config = config_utils.Config(spec.get('affinity', {}))
        existing_rules = pod_spec_config.get_nested(
            ('podAntiAffinity',
             'preferredDuringSchedulingIgnoredDuringExecution'), [])
        existing_rules.append({
            'weight': 100,
            'podAffinityTerm': {
                'labelSelector': {
                    'matchExpressions': [{
                        'key': constants.TAG_SKYPILOT_CLUSTER_NAME,
                        'operator': 'In',
                        'values': [cluster_name_on_cloud],
                    }],
                },
                'topologyKey': 'kubernetes.io/hostname',
            },
        })
        pod_spec_config.set_nested(
            ('podAntiAffinity',
             'preferredDuringSchedulingIgnoredDuringExecution'), existing_rules)
        spec['affinity'] = pod_spec_config

    # GKE TPU slice nodes carry google.com/tpu=present:NoSchedule.
    if needs_tpu:
        existing_tolerations = spec.get('tolerations', [])
        spec['tolerations'] = existing_tolerations + [{
            'key': kubernetes_utils.TPU_RESOURCE_KEY,
            'operator': 'Equal',
            'value': 'present',
            'effect': 'NoSchedule',
        }]

    # DWS-created GPU nodes may carry a resource-key NoSchedule taint.  This is
    # harmless for non-DWS clusters and preserves the existing scheduling path.
    if needs_gpus:
        existing_tolerations = spec.get('tolerations', [])
        spec['tolerations'] = existing_tolerations + [{
            'key': gpu_resource_key,
            'operator': 'Exists',
            'effect': 'NoSchedule',
        }]

    return pod_spec
