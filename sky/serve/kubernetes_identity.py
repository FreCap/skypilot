"""Immutable server-owned Kubernetes projections for SkyServe versions."""

from collections.abc import Mapping
import copy
import json
import re
from typing import Any
import urllib.parse

from sky import clouds
from sky import global_user_state
from sky import skypilot_config
from sky import task as task_lib
from sky.clouds import kubernetes as kubernetes_cloud
from sky.models import VolumeConfig
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.serve import spot_placer
from sky.utils import volume as volume_utils

PLACEMENT_PROJECTION_PROTOCOL_VERSION = 1

_STORAGE_BROKER_KEYS = frozenset({
    'endpoint',
    'audience',
    'api_version',
    'grant_uri_prefix',
    'authenticated_worker_role_arns',
    'kms_key_id',
    'transfer_authorization_limit_bytes',
})
_LEGACY_STORAGE_BROKER_KEYS = (_STORAGE_BROKER_KEYS -
                               {'transfer_authorization_limit_bytes'})
STORAGE_BROKER_TRANSFER_AUTHORIZATION_LIMIT_BYTES = 3_000_000_000_000
_AWS_ROLE_ARN_PATTERN = re.compile(
    r'^arn:(?:aws|aws-us-gov|aws-cn):iam::[0-9]{12}:'
    r'role/[A-Za-z0-9+=,.@_/-]+$')
_DNS_NAME_PATTERN = re.compile(
    r'(?=.{1,253}$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}'
    r'[A-Za-z0-9])?)(?:\.(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}'
    r'[A-Za-z0-9])?))*')
_S3_BUCKET_PATTERN = re.compile(r'(?=.{3,63}$)[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?')
_S3_RESERVED_BUCKET_PREFIXES = ('xn--', 'sthree-', 'amzn-s3-demo-')
_S3_RESERVED_BUCKET_SUFFIXES = ('-s3alias', '--ol-s3', '.mrap', '--x-s3',
                                '--table-s3')

_CONTROLLER_KEYS = frozenset({
    'workspace',
    'kubernetes_context',
    'namespace',
    'service_account_name',
    'priority_class_name',
    'lb_data_plane_auth',
})
_CONTROLLER_LB_DATA_PLANE_AUTH_KEYS = frozenset(
    {'secret_name', 'secret_key', 'mount_path'})
CONTROLLER_LB_DATA_PLANE_AUTH_MOUNT_PATH = (
    '/etc/skypilot/serve-auth/lb-data-plane/tokens')
_WORKER_KEYS = frozenset({
    'candidate_id',
    'kubernetes_context',
    'namespace',
    'service_account_name',
    'priority_class_name',
    'priority_value',
    'preemption_policy',
    'pod_identity_role_arn',
    'accelerator_name',
    'accelerator_count',
    'accelerator_scheduling',
    'cache',
})
_ACCELERATOR_SCHEDULING_KEYS = frozenset(
    {'label_key', 'label_values', 'resource_key'})
_MAX_ACCELERATOR_LABEL_VALUES = 16
_ATTESTATION_KEYS = frozenset({
    'attestation_id',
    'device_source_pattern',
    'filesystem_type',
    'required_bytes_per_replica',
    'required_inodes_per_replica',
    'max_replicas_per_node',
    'reserved_bytes_per_node',
    'reserved_inodes_per_node',
    'usable_bytes_per_node',
    'usable_inodes_per_node',
})
_POSITIVE_ATTESTATION_KEYS = frozenset({
    'required_bytes_per_replica',
    'required_inodes_per_replica',
    'max_replicas_per_node',
    'usable_bytes_per_node',
    'usable_inodes_per_node',
})
_NONNEGATIVE_ATTESTATION_KEYS = frozenset({
    'reserved_bytes_per_node',
    'reserved_inodes_per_node',
})
_CACHE_NONE_KEYS = frozenset({'kind'})
_CACHE_NODE_LOCAL_KEYS = frozenset({
    'kind',
    'mount_path',
    'volume_name',
    'host_path',
    'attestation',
})
_CONTROLLER_EMPTY_DIR_KEYS = frozenset({
    'kind', 'mount_path', 'required_bytes', 'required_inodes',
    'size_limit_bytes'
})
_CONTROLLER_NODE_LOCAL_KEYS = frozenset({
    'kind', 'mount_path', 'volume_name', 'host_path', 'required_bytes',
    'required_inodes', 'attestation'
})

CACHE_ENV_VAR = 'SKYPILOT_SERVE_CACHE_KIND'
CACHE_ENV_PREFIX = 'SKYPILOT_SERVE_CACHE_'
_CACHE_ATTESTATION_ENV_KEYS = {
    'attestation_id': 'ATTESTATION_ID',
    'device_source_pattern': 'DEVICE_SOURCE_PATTERN',
    'filesystem_type': 'FILESYSTEM_TYPE',
    'required_bytes_per_replica': 'REQUIRED_BYTES_PER_REPLICA',
    'required_inodes_per_replica': 'REQUIRED_INODES_PER_REPLICA',
    'max_replicas_per_node': 'MAX_REPLICAS_PER_NODE',
    'reserved_bytes_per_node': 'RESERVED_BYTES_PER_NODE',
    'reserved_inodes_per_node': 'RESERVED_INODES_PER_NODE',
    'usable_bytes_per_node': 'USABLE_BYTES_PER_NODE',
    'usable_inodes_per_node': 'USABLE_INODES_PER_NODE',
}
CONTROLLER_CACHE_ENV_PREFIX = 'SKYPILOT_CONTROLLER_WORK_CACHE_'


def _strict_nonempty_string(value: Any, description: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f'{description} must be a non-empty string.')
    return value


def _strict_positive_int(value: Any, description: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f'{description} must be a positive integer.')
    return value


def _strict_nonnegative_int(value: Any, description: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f'{description} must be a non-negative integer.')
    return value


def _validate_accelerator_scheduling(
        value: Any, description: str) -> dict[str, str | list[str]]:
    """Validate one immutable Kubernetes accelerator scheduling contract."""
    if not isinstance(value,
                      dict) or set(value) != _ACCELERATOR_SCHEDULING_KEYS:
        raise ValueError(f'{description} must contain exactly '
                         f'{sorted(_ACCELERATOR_SCHEDULING_KEYS)!r}.')
    label_key = _strict_nonempty_string(value['label_key'],
                                        f'{description} label_key')
    resource_key = _strict_nonempty_string(value['resource_key'],
                                           f'{description} resource_key')
    label_values = value['label_values']
    if (not isinstance(label_values, list) or
            not 1 <= len(label_values) <= _MAX_ACCELERATOR_LABEL_VALUES):
        raise ValueError(
            f'{description} label_values must contain between 1 and '
            f'{_MAX_ACCELERATOR_LABEL_VALUES} values.')
    copied_values: list[str] = []
    for label_value in label_values:
        label_value = _strict_nonempty_string(label_value,
                                              f'{description} label value')
        valid, reason = kubernetes_cloud.Kubernetes.is_label_valid(
            label_key, label_value)
        if not valid:
            raise ValueError(f'{description} has an invalid Kubernetes '
                             f'accelerator label: {reason}')
        copied_values.append(label_value)
    if len(set(copied_values)) != len(copied_values):
        raise ValueError(f'{description} label_values must be unique.')
    valid, reason = kubernetes_cloud.Kubernetes.is_label_valid(resource_key, '')
    if (not valid or '/' not in resource_key or resource_key.startswith(
        ('kubernetes.io/', 'k8s.io/'))):
        raise ValueError(f'{description} resource_key must be a valid '
                         f'Kubernetes extended resource name: {reason}')
    return {
        'label_key': label_key,
        'label_values': copied_values,
        'resource_key': resource_key,
    }


def _validate_accelerator_scheduling_map(
        value: Any) -> dict[str, dict[str, str | list[str]]]:
    """Validate the server-owned logical-accelerator scheduling map."""
    if not isinstance(value, dict) or not value:
        raise ValueError('Kubernetes serve_worker_accelerator_scheduling must '
                         'be a non-empty mapping.')
    result: dict[str, dict[str, str | list[str]]] = {}
    normalized_names: set[str] = set()
    claimed_labels: dict[tuple[str, str, str], str] = {}
    for accelerator_name, descriptor in value.items():
        accelerator_name = _strict_nonempty_string(
            accelerator_name, 'Serve worker accelerator name')
        if accelerator_name.strip() != accelerator_name:
            raise ValueError('Serve worker accelerator names must not have '
                             'leading or trailing whitespace.')
        normalized_name = accelerator_name.lower()
        if normalized_name in normalized_names:
            raise ValueError('Kubernetes serve_worker_accelerator_scheduling '
                             'accelerator names must be case-insensitively '
                             'unique.')
        normalized_names.add(normalized_name)
        validated = _validate_accelerator_scheduling(
            descriptor,
            f'Serve worker accelerator {accelerator_name!r} scheduling')
        for label_value in validated['label_values']:
            assert isinstance(label_value, str)
            claim = (str(validated['label_key']), label_value,
                     str(validated['resource_key']))
            previous = claimed_labels.get(claim)
            if previous is not None:
                raise ValueError(
                    'Kubernetes serve_worker_accelerator_scheduling has '
                    f'ambiguous label value {label_value!r} for '
                    f'{previous!r} and {accelerator_name!r}.')
            claimed_labels[claim] = accelerator_name
        result[accelerator_name] = validated
    return result


def _validate_uri_path(path: str, description: str, *,
                       require_path: bool) -> None:
    """Reject ambiguous or traversal-capable URI path encodings."""
    if ('\\' in path or '%' in path or (require_path and path in ('', '/'))):
        raise ValueError(f'{description} has an invalid path.')
    if not path:
        return
    if not path.startswith('/') or '//' in path:
        raise ValueError(f'{description} path must be canonical.')
    for raw_segment in path[1:].split('/'):
        if not raw_segment or re.search(r'%(?![0-9A-Fa-f]{2})', raw_segment):
            raise ValueError(f'{description} path must be canonical.')
        decoded = raw_segment
        for _ in range(8):
            next_decoded = urllib.parse.unquote(decoded)
            if next_decoded == decoded:
                break
            decoded = next_decoded
        else:
            raise ValueError(
                f'{description} path encoding is too deeply nested.')
        if decoded in ('.', '..') or '/' in decoded or '\\' in decoded:
            raise ValueError(f'{description} path cannot contain ambiguous '
                             'or traversal segments.')


def _validate_https_endpoint(value: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
        hostname = parsed.hostname
    except ValueError as e:
        raise ValueError(
            'Storage-broker endpoint has an invalid authority.') from e
    if (parsed.scheme != 'https' or not parsed.netloc or
            any(character.isspace() for character in value) or
            parsed.username is not None or parsed.password is not None or
            parsed.query or parsed.fragment or hostname is None or
            _DNS_NAME_PATTERN.fullmatch(hostname) is None):
        raise ValueError('Storage-broker endpoint must be an absolute HTTPS '
                         'URL with a valid hostname and without credentials, '
                         'query, or fragment.')
    expected_authority = hostname if port is None else f'{hostname}:{port}'
    if parsed.netloc.lower() != expected_authority.lower():
        raise ValueError(
            'Storage-broker endpoint has a non-canonical authority.')
    _validate_uri_path(parsed.path,
                       'Storage-broker endpoint',
                       require_path=False)
    return value


def _validate_s3_prefix(value: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
        hostname = parsed.hostname
    except ValueError as e:
        raise ValueError('Storage-broker grant_uri_prefix has an invalid '
                         'authority.') from e
    bucket = parsed.netloc
    if (parsed.scheme != 's3' or hostname is None or port is not None or
            parsed.username is not None or parsed.password is not None or
            parsed.query or parsed.fragment or bucket != hostname or
            _S3_BUCKET_PATTERN.fullmatch(bucket) is None or '..' in bucket or
            '.-' in bucket or '-.' in bucket or
            bucket.startswith(_S3_RESERVED_BUCKET_PREFIXES) or
            bucket.endswith(_S3_RESERVED_BUCKET_SUFFIXES) or
            re.fullmatch(r'[0-9]+(?:\.[0-9]+){3}', bucket) is not None or
            any(character.isspace() for character in value)):
        raise ValueError('Storage-broker grant_uri_prefix must be an absolute '
                         'canonical S3 bucket/prefix URI without credentials, '
                         'port, query, or fragment.')
    _validate_uri_path(parsed.path,
                       'Storage-broker grant_uri_prefix',
                       require_path=True)
    return value


def validate_storage_broker_projection(
    value: Any,
    *,
    allow_none: bool = True,
    allow_legacy_without_transfer_limit: bool = False,
) -> dict[str, str | int | list[str]] | None:
    """Strictly validate a non-secret storage-grant broker descriptor."""
    if value is None:
        if allow_none:
            return None
        raise ValueError('Storage-broker projection must not be null.')
    keys = set(value) if isinstance(value, dict) else set()
    legacy = (allow_legacy_without_transfer_limit and
              keys == _LEGACY_STORAGE_BROKER_KEYS)
    if (not isinstance(value, dict) or
        (keys != _STORAGE_BROKER_KEYS and not legacy)):
        raise ValueError('Storage-broker projection must contain exactly '
                         f'{sorted(_STORAGE_BROKER_KEYS)!r}.')
    endpoint = _strict_nonempty_string(value['endpoint'],
                                       'Storage-broker endpoint')
    endpoint = _validate_https_endpoint(endpoint)
    grant_prefix = _strict_nonempty_string(value['grant_uri_prefix'],
                                           'Storage-broker grant_uri_prefix')
    grant_prefix = _validate_s3_prefix(grant_prefix)
    if type(value['api_version']) is not int or value['api_version'] != 3:
        raise ValueError('Storage-broker api_version must be exactly 3.')
    role_arns = value['authenticated_worker_role_arns']
    if (not isinstance(role_arns, list) or not 1 <= len(role_arns) <= 16):
        raise ValueError('Storage-broker authenticated_worker_role_arns must '
                         'contain between 1 and 16 entries.')
    validated_role_arns: list[str] = []
    for role_arn in role_arns:
        role_arn = _strict_nonempty_string(
            role_arn, 'Storage-broker authenticated_worker_role_arns entry')
        if _AWS_ROLE_ARN_PATTERN.fullmatch(role_arn) is None:
            raise ValueError('Every storage-broker '
                             'authenticated_worker_role_arns entry must be '
                             'an AWS IAM role ARN.')
        validated_role_arns.append(role_arn)
    if len(validated_role_arns) != len(set(validated_role_arns)):
        raise ValueError('Storage-broker authenticated_worker_role_arns must '
                         'not contain duplicates.')
    result: dict[str, str | int | list[str]] = {
        'endpoint': endpoint,
        'audience': _strict_nonempty_string(value['audience'],
                                            'Storage-broker audience'),
        'api_version': 3,
        'grant_uri_prefix': grant_prefix,
        'authenticated_worker_role_arns': validated_role_arns,
        'kms_key_id': _strict_nonempty_string(value['kms_key_id'],
                                              'Storage-broker kms_key_id'),
    }
    if legacy:
        return result
    transfer_limit = value['transfer_authorization_limit_bytes']
    if (type(transfer_limit) is not int or transfer_limit
            != STORAGE_BROKER_TRANSFER_AUTHORIZATION_LIMIT_BYTES):
        raise ValueError(
            'Storage-broker transfer_authorization_limit_bytes must be '
            f'exactly {STORAGE_BROKER_TRANSFER_AUTHORIZATION_LIMIT_BYTES}.')
    result['transfer_authorization_limit_bytes'] = transfer_limit
    return result


def build_storage_broker_projection(
    *,
    workspace: str | None,
) -> dict[str, str | int | list[str]] | None:
    """Freeze the server/workspace-owned storage broker for one version."""
    resolved_workspace = workspace or skypilot_config.get_active_workspace()
    configured = skypilot_config.get_nested(keys=('workspaces',
                                                  resolved_workspace, 'serve',
                                                  'storage_broker'),
                                            default_value=None)
    if configured is None:
        configured = skypilot_config.get_nested(keys=('serve',
                                                      'storage_broker'),
                                                default_value=None)
    return validate_storage_broker_projection(configured)


def validate_controller_job_projection(
    value: Any,
    *,
    allow_none: bool = True,
) -> dict[str, Any] | None:
    """Strictly validate and copy one controller-home projection."""
    if value is None:
        if allow_none:
            return None
        raise ValueError('Controller-job projection must not be null.')
    if not isinstance(value, dict) or set(value) != _CONTROLLER_KEYS:
        raise ValueError('Controller-job projection must contain exactly '
                         f'{sorted(_CONTROLLER_KEYS)!r}.')
    for key in ('workspace', 'kubernetes_context', 'namespace',
                'service_account_name'):
        _strict_nonempty_string(value[key], f'Controller-job projection {key}')
    priority = value['priority_class_name']
    if priority is not None:
        _strict_nonempty_string(
            priority, 'Controller-job projection priority_class_name')
    auth = value['lb_data_plane_auth']
    if (not isinstance(auth, dict) or
            set(auth) != _CONTROLLER_LB_DATA_PLANE_AUTH_KEYS):
        raise ValueError('Controller-job projection lb_data_plane_auth must '
                         'contain exactly secret_name, secret_key, and '
                         'mount_path.')
    secret_name = _strict_nonempty_string(
        auth['secret_name'], 'Controller data-plane auth secret_name')
    if (len(secret_name) > 253 or re.fullmatch(
            r'[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?', secret_name) is None):
        raise ValueError('Controller data-plane auth secret_name must be a '
                         'Kubernetes DNS subdomain.')
    secret_key = _strict_nonempty_string(
        auth['secret_key'], 'Controller data-plane auth secret_key')
    if (len(secret_key) > 253 or
            re.fullmatch(r'[-._A-Za-z0-9]+', secret_key) is None):
        raise ValueError('Controller data-plane auth secret_key is invalid.')
    if auth['mount_path'] != CONTROLLER_LB_DATA_PLANE_AUTH_MOUNT_PATH:
        raise ValueError(
            'Controller data-plane auth mount_path is unsupported.')
    projected = {key: value[key] for key in sorted(_CONTROLLER_KEYS)}
    projected['lb_data_plane_auth'] = {
        'secret_name': secret_name,
        'secret_key': secret_key,
        'mount_path': CONTROLLER_LB_DATA_PLANE_AUTH_MOUNT_PATH,
    }
    return projected


def _validate_cache_attestation(value: Any) -> dict[str, str | int]:
    if not isinstance(value, dict) or set(value) != _ATTESTATION_KEYS:
        raise ValueError('Cache attestation must contain exactly '
                         f'{sorted(_ATTESTATION_KEYS)!r}.')
    for key in ('attestation_id', 'device_source_pattern', 'filesystem_type'):
        _strict_nonempty_string(value[key], f'Cache attestation {key}')
    pattern = value['device_source_pattern']
    if not pattern.startswith('^') or not pattern.endswith('$'):
        raise ValueError('Cache attestation device_source_pattern must be '
                         'anchored with ^ and $.')
    try:
        re.compile(pattern)
    except re.error as e:
        raise ValueError('Cache attestation device_source_pattern must be a '
                         'valid regular expression.') from e
    for key in _POSITIVE_ATTESTATION_KEYS:
        _strict_positive_int(value[key], f'Cache attestation {key}')
    for key in _NONNEGATIVE_ATTESTATION_KEYS:
        _strict_nonnegative_int(value[key], f'Cache attestation {key}')
    max_packing = value['max_replicas_per_node']
    if (value['required_bytes_per_replica'] * max_packing
            > value['usable_bytes_per_node']):
        raise ValueError('Cache attestation byte budget cannot satisfy maximum '
                         'replica packing.')
    if (value['required_inodes_per_replica'] * max_packing
            > value['usable_inodes_per_node']):
        raise ValueError('Cache attestation inode budget cannot satisfy '
                         'maximum replica packing.')
    return {key: value[key] for key in sorted(_ATTESTATION_KEYS)}


def validate_cache_projection(value: Any) -> dict[str, Any]:
    """Strictly validate one worker cache projection."""
    if not isinstance(value, dict):
        raise ValueError('Worker cache projection must be a mapping.')
    kind = value.get('kind')
    if kind == 'none':
        if set(value) != _CACHE_NONE_KEYS:
            raise ValueError('Cache kind none cannot contain other fields.')
        return {'kind': 'none'}
    if kind != 'node_local' or set(value) != _CACHE_NODE_LOCAL_KEYS:
        raise ValueError('Worker cache projection must be exactly kind none '
                         'or a complete node_local projection.')
    mount_path = _strict_nonempty_string(value['mount_path'],
                                         'Cache mount_path')
    host_path = _strict_nonempty_string(value['host_path'], 'Cache host_path')
    if not mount_path.startswith('/') or not host_path.startswith('/'):
        raise ValueError('Cache mount_path and host_path must be absolute.')
    volume_name = _strict_nonempty_string(value['volume_name'],
                                          'Cache volume_name')
    return {
        'kind': 'node_local',
        'mount_path': mount_path,
        'volume_name': volume_name,
        'host_path': host_path,
        'attestation': _validate_cache_attestation(value['attestation']),
    }


def validate_controller_work_cache_projection(
    value: Any,
    *,
    allow_none: bool = True,
) -> dict[str, Any] | None:
    """Strictly validate one server-created controller cache projection."""
    if value is None:
        if allow_none:
            return None
        raise ValueError('Controller work-cache projection must not be null.')
    if not isinstance(value, dict):
        raise ValueError('Controller work-cache projection must be a mapping.')
    kind = value.get('kind')
    if kind == 'empty_dir':
        if set(value) != _CONTROLLER_EMPTY_DIR_KEYS:
            raise ValueError('Controller empty_dir cache must contain exactly '
                             f'{sorted(_CONTROLLER_EMPTY_DIR_KEYS)!r}.')
        mount_path = _strict_nonempty_string(value['mount_path'],
                                             'Controller cache mount_path')
        if not mount_path.startswith('/'):
            raise ValueError('Controller cache mount_path must be absolute.')
        required_bytes = _strict_positive_int(
            value['required_bytes'], 'Controller cache required_bytes')
        required_inodes = _strict_positive_int(
            value['required_inodes'], 'Controller cache required_inodes')
        size_limit_bytes = _strict_positive_int(
            value['size_limit_bytes'], 'Controller cache size_limit_bytes')
        if size_limit_bytes < required_bytes:
            raise ValueError('Controller emptyDir size limit must satisfy its '
                             'required byte budget.')
        return {
            'kind': kind,
            'mount_path': mount_path,
            'required_bytes': required_bytes,
            'required_inodes': required_inodes,
            'size_limit_bytes': size_limit_bytes,
        }
    if kind != 'node_local' or set(value) != _CONTROLLER_NODE_LOCAL_KEYS:
        raise ValueError('Controller work-cache projection must be a complete '
                         'empty_dir or node_local projection.')
    mount_path = _strict_nonempty_string(value['mount_path'],
                                         'Controller cache mount_path')
    host_path = _strict_nonempty_string(value['host_path'],
                                        'Controller cache host_path')
    if not mount_path.startswith('/') or not host_path.startswith('/'):
        raise ValueError('Controller cache mount_path and host_path must be '
                         'absolute.')
    required_bytes = _strict_positive_int(value['required_bytes'],
                                          'Controller cache required_bytes')
    required_inodes = _strict_positive_int(value['required_inodes'],
                                           'Controller cache required_inodes')
    attestation = _validate_cache_attestation(value['attestation'])
    attested_bytes = _strict_positive_int(
        attestation['required_bytes_per_replica'],
        'Cache attestation required_bytes_per_replica')
    attested_inodes = _strict_positive_int(
        attestation['required_inodes_per_replica'],
        'Cache attestation required_inodes_per_replica')
    if required_bytes > attested_bytes:
        raise ValueError('Controller cache byte requirement exceeds its '
                         'attested per-replica budget.')
    if required_inodes > attested_inodes:
        raise ValueError('Controller cache inode requirement exceeds its '
                         'attested per-replica budget.')
    return {
        'kind': kind,
        'mount_path': mount_path,
        'volume_name': _strict_nonempty_string(value['volume_name'],
                                               'Controller cache volume_name'),
        'host_path': host_path,
        'required_bytes': required_bytes,
        'required_inodes': required_inodes,
        'attestation': attestation,
    }


def validate_worker_placement_projections(
    value: Any,
    *,
    allow_none: bool = True,
) -> list[dict[str, Any]] | None:
    """Strictly validate and copy worker candidate projections."""
    if value is None:
        if allow_none:
            return None
        raise ValueError('Worker placement projections must not be null.')
    if not isinstance(value, list) or not value:
        raise ValueError('Worker placement projections must be a non-empty '
                         'list.')
    validated = []
    candidate_ids = set()
    selection_keys = set()
    scheduling_by_accelerator: dict[tuple[str, str], dict[str, Any]] = {}
    accelerator_by_scheduling_label: dict[tuple[str, str, str, str], str] = {}
    for projection in value:
        if (not isinstance(projection, dict) or
                set(projection) != _WORKER_KEYS):
            raise ValueError('Worker placement projection must contain '
                             f'exactly {sorted(_WORKER_KEYS)!r}.')
        candidate_id = _strict_nonempty_string(projection['candidate_id'],
                                               'Worker candidate_id')
        if not re.fullmatch(r'kubernetes-[0-9]{4}', candidate_id):
            raise ValueError('Worker candidate_id must use the '
                             'kubernetes-NNNN format.')
        if candidate_id in candidate_ids:
            raise ValueError('Worker candidate IDs must be unique.')
        candidate_ids.add(candidate_id)
        for key in ('kubernetes_context', 'namespace', 'service_account_name',
                    'accelerator_name', 'pod_identity_role_arn'):
            _strict_nonempty_string(projection[key], f'Worker placement {key}')
        if _AWS_ROLE_ARN_PATTERN.fullmatch(
                projection['pod_identity_role_arn']) is None:
            raise ValueError('Worker placement pod_identity_role_arn must be '
                             'an AWS IAM role ARN.')
        priority = projection['priority_class_name']
        if priority is not None:
            _strict_nonempty_string(priority,
                                    'Worker placement priority_class_name')
        priority_value = projection['priority_value']
        preemption_policy = projection['preemption_policy']
        if priority is None:
            if priority_value is not None or preemption_policy is not None:
                raise ValueError('A worker placement without a priority class '
                                 'must have null priority_value and '
                                 'preemption_policy.')
        else:
            if (type(priority_value) is not int or
                    priority_value < -2147483648 or
                    priority_value > 1000000000):
                raise ValueError('Worker placement priority_value must be a '
                                 'Kubernetes priority integer.')
            if preemption_policy not in ('Never', 'PreemptLowerPriority'):
                raise ValueError('Worker placement preemption_policy must be '
                                 'Never or PreemptLowerPriority.')
        accelerator_count = _strict_positive_int(
            projection['accelerator_count'], 'Worker accelerator_count')
        selection_key = (projection['kubernetes_context'],
                         projection['accelerator_name'].lower(),
                         accelerator_count)
        if selection_key in selection_keys:
            raise ValueError('Worker placement projections must be unique by '
                             'Kubernetes context, accelerator name, and count.')
        selection_keys.add(selection_key)
        scheduling = _validate_accelerator_scheduling(
            projection['accelerator_scheduling'],
            f'Worker placement {candidate_id} accelerator_scheduling')
        logical_accelerator_key = (projection['kubernetes_context'],
                                   projection['accelerator_name'].lower())
        prior_scheduling = scheduling_by_accelerator.get(
            logical_accelerator_key)
        if prior_scheduling is not None and prior_scheduling != scheduling:
            raise ValueError('One logical worker accelerator must have one '
                             'immutable scheduling contract per Kubernetes '
                             'context.')
        scheduling_by_accelerator[logical_accelerator_key] = scheduling
        for label_value in scheduling['label_values']:
            assert isinstance(label_value, str)
            scheduling_label = (projection['kubernetes_context'],
                                str(scheduling['label_key']), label_value,
                                str(scheduling['resource_key']))
            prior_accelerator = accelerator_by_scheduling_label.get(
                scheduling_label)
            if (prior_accelerator is not None and prior_accelerator
                    != projection['accelerator_name'].lower()):
                raise ValueError('Worker placement projections assign one '
                                 'Kubernetes scheduling label to multiple '
                                 'logical accelerators.')
            accelerator_by_scheduling_label[scheduling_label] = projection[
                'accelerator_name'].lower()
        validated.append({
            'candidate_id': candidate_id,
            'kubernetes_context': projection['kubernetes_context'],
            'namespace': projection['namespace'],
            'service_account_name': projection['service_account_name'],
            'priority_class_name': priority,
            'priority_value': priority_value,
            'preemption_policy': preemption_policy,
            'pod_identity_role_arn': projection['pod_identity_role_arn'],
            'accelerator_name': projection['accelerator_name'],
            'accelerator_count': accelerator_count,
            'accelerator_scheduling': scheduling,
            'cache': validate_cache_projection(projection['cache']),
        })
    return validated


def _effective_pod_config(context: str, cluster_config_overrides: dict[str,
                                                                       Any],
                          workspace: str | None) -> dict[str, Any]:
    # Identity/priority are platform-owned. Caller cluster overrides are
    # deliberately ignored rather than copied into immutable metadata.
    del cluster_config_overrides
    value = skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        region=context,
        keys=('pod_config',),
        workspace=workspace,
        default_value={})
    return value if isinstance(value, dict) else {}


def _project_location(context: str, cluster_config_overrides: dict[str, Any],
                      workspace: str | None) -> dict[str, str | None]:
    service_account_name = kubernetes_cloud.get_service_account_name(
        context, cluster_config_overrides={}, workspace=workspace)
    effective_pod_config = _effective_pod_config(context,
                                                 cluster_config_overrides,
                                                 workspace)
    if not isinstance(effective_pod_config, dict):
        raise ValueError('Effective Kubernetes pod_config must be a mapping.')
    pod_spec = effective_pod_config.get('spec', {})
    if not isinstance(pod_spec, dict):
        raise ValueError('Effective Kubernetes pod_config.spec must be a '
                         'mapping.')
    if 'serviceAccountName' in pod_spec:
        service_account_name = pod_spec['serviceAccountName']
    projection: dict[str, str | None] = {
        'kubernetes_context': context,
        'namespace': kubernetes_utils.get_namespace(context=context,
                                                    workspace=workspace,
                                                    override_configs=None),
        'service_account_name': service_account_name,
        'priority_class_name': pod_spec.get('priorityClassName'),
    }
    for key in ('kubernetes_context', 'namespace', 'service_account_name'):
        _strict_nonempty_string(projection[key], f'Projected location {key}')
    priority = projection['priority_class_name']
    if priority is not None:
        _strict_nonempty_string(priority,
                                'Projected location priority_class_name')
    return projection


def _project_worker_location(context: str, cluster_config_overrides: dict[str,
                                                                          Any],
                             workspace: str | None) -> dict[str, Any]:
    """Resolve worker identity plus the narrow expected priority contract."""
    projection = _project_location(context, cluster_config_overrides, workspace)
    # Worker priority is intentionally not inferred from pod_config. Inference
    # clusters may assign it in an admission mutation, so the API server needs
    # a narrow server-owned expectation it can freeze before the Pod exists.
    expected_priority = (skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        region=context,
        keys=('serve_worker_priority_class_name',),
        workspace=workspace,
        default_value=None))
    if expected_priority is not None:
        expected_priority = _strict_nonempty_string(
            expected_priority, 'Kubernetes Serve worker priority class name')
    projection['priority_class_name'] = expected_priority
    expected_priority_value = (
        skypilot_config.get_effective_workspace_region_config(
            cloud='kubernetes',
            region=context,
            keys=('serve_worker_priority_value',),
            workspace=workspace,
            default_value=None))
    expected_preemption_policy = (
        skypilot_config.get_effective_workspace_region_config(
            cloud='kubernetes',
            region=context,
            keys=('serve_worker_preemption_policy',),
            workspace=workspace,
            default_value=None))
    if expected_priority is None:
        if (expected_priority_value is not None or
                expected_preemption_policy is not None):
            raise ValueError('Kubernetes Serve worker priority value and '
                             'preemption policy require a priority class.')
    else:
        if (type(expected_priority_value) is not int or
                expected_priority_value < -2147483648 or
                expected_priority_value > 1000000000):
            raise ValueError('Kubernetes Serve worker priority class requires '
                             'an explicit server-owned priority value.')
        if expected_preemption_policy not in ('Never', 'PreemptLowerPriority'):
            raise ValueError('Kubernetes Serve worker priority class requires '
                             'an explicit server-owned preemption policy.')
    projection['priority_value'] = expected_priority_value
    projection['preemption_policy'] = expected_preemption_policy
    role_arn = skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        region=context,
        keys=('serve_worker_pod_identity_role_arn',),
        workspace=workspace,
        default_value=None)
    if role_arn is not None:
        role_arn = _strict_nonempty_string(
            role_arn, 'Kubernetes Serve worker Pod Identity role ARN')
        if _AWS_ROLE_ARN_PATTERN.fullmatch(role_arn) is None:
            raise ValueError('Kubernetes Serve worker Pod Identity role must '
                             'be an AWS IAM role ARN.')
    projection['pod_identity_role_arn'] = role_arn
    return projection


def _is_exact_kubernetes_cloud(cloud: clouds.Cloud | None) -> bool:
    return (isinstance(cloud, clouds.Kubernetes) and
            not isinstance(cloud, clouds.SSH))


def _controller_location(
        service_workspace: str | None) -> tuple[str, str] | None:
    resolved_service_workspace = (service_workspace or
                                  skypilot_config.get_active_workspace())
    controller_workspace = (
        skypilot_config.get_effective_workspace_region_config(
            cloud='kubernetes',
            keys=('serve_controller_workspace',),
            workspace=resolved_service_workspace,
            default_value=None))
    legacy_context = skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        keys=('serve_controller_context',),
        workspace=resolved_service_workspace,
        default_value=None)
    if controller_workspace is None:
        if legacy_context is not None:
            raise ValueError(
                'kubernetes.serve_controller_context requires an explicit '
                'kubernetes.serve_controller_workspace; controller identity '
                'must not be resolved from the inference workspace.')
        return None
    controller_workspace = _strict_nonempty_string(
        controller_workspace, 'Kubernetes Serve controller workspace')
    if controller_workspace == resolved_service_workspace:
        raise ValueError('Kubernetes Serve controller workspace must be '
                         'separate from the service inference workspace.')
    configured_workspace = skypilot_config.get_nested(
        keys=('workspaces', controller_workspace), default_value=None)
    if not isinstance(configured_workspace, dict):
        raise ValueError('Kubernetes Serve controller workspace '
                         f'{controller_workspace!r} is not configured.')
    context = skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        keys=('serve_controller_context',),
        workspace=controller_workspace,
        default_value=None)
    context = _strict_nonempty_string(
        context, 'Kubernetes Serve controller context in workspace '
        f'{controller_workspace!r}')
    return controller_workspace, context


def build_controller_job_projection(
    task: task_lib.Task,
    *,
    workspace: str | None,
) -> dict[str, Any] | None:
    """Build the exact server-owned controller home, if configured."""
    del task
    location = _controller_location(workspace)
    if location is None:
        return None
    controller_workspace, context = location
    configured_auth = skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        region=context,
        keys=('serve_controller_lb_data_plane_auth',),
        workspace=controller_workspace,
        default_value=None)
    if not isinstance(configured_auth, dict):
        raise ValueError('Kubernetes Serve controller workspace/context must '
                         'configure serve_controller_lb_data_plane_auth.')
    projected = {
        'workspace': controller_workspace,
        **_project_location(context, {}, controller_workspace),
        'lb_data_plane_auth': {
            'secret_name': configured_auth.get('secret_name'),
            'secret_key': configured_auth.get('secret_key'),
            'mount_path': CONTROLLER_LB_DATA_PLANE_AUTH_MOUNT_PATH,
        },
    }
    return validate_controller_job_projection(projected, allow_none=False)


def build_controller_work_cache_projection(
    task: task_lib.Task,
    *,
    workspace: str | None,
) -> dict[str, Any] | None:
    """Build the controller home's server-owned disposable work cache."""
    del task
    location = _controller_location(workspace)
    if location is None:
        return None
    controller_workspace, context = location
    configured = skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        region=context,
        keys=('serve_controller_work_cache',),
        workspace=controller_workspace,
        default_value=None)
    if configured is None:
        return None
    if not isinstance(configured, dict):
        raise ValueError('Server-owned controller work cache must be a '
                         'mapping.')
    projected = copy.deepcopy(configured)
    if configured.get('kind') == 'node_local':
        volume_name = configured.get('volume_name')
        mount_path = configured.get('mount_path')
        if not isinstance(volume_name, str) or not isinstance(mount_path, str):
            raise ValueError('Node-local controller cache requires volume_name '
                             'and mount_path.')
        if not any(
                isinstance(entry, dict) and entry.get('volume_name') ==
                volume_name and mount_path in entry.get('mount_paths', [])
                for entry in _effective_auto_mounts(context,
                                                    controller_workspace)):
            raise ValueError('Node-local controller cache must be present in '
                             'the controller context auto_mounts.')
        volume_config = global_user_state.get_volume_configs_by_names(
            [volume_name]).get(volume_name)
        if (volume_config is None or
                volume_config.type != volume_utils.VolumeType.HOSTPATH.value or
                volume_config.region != context):
            raise ValueError('Node-local controller cache volume must be a '
                             'registered hostPath in the controller context.')
        projected['host_path'] = volume_config.config.get('host_path')
    return validate_controller_work_cache_projection(projected,
                                                     allow_none=False)


def _effective_auto_mounts(context: str,
                           workspace: str | None) -> list[dict[str, Any]]:
    value = skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        region=context,
        keys=('auto_mounts',),
        workspace=workspace,
        default_value=[])
    return value if isinstance(value, list) else []


def _project_cache(context: str, workspace: str | None) -> dict[str, Any]:
    configured = skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        region=context,
        keys=('serve_worker_cache',),
        workspace=workspace,
        default_value={'kind': 'none'})
    if not isinstance(configured, dict):
        raise ValueError('Server-owned Serve worker cache must be a mapping.')
    if configured.get('kind') == 'none':
        return validate_cache_projection(configured)
    if configured.get('kind') != 'node_local':
        raise ValueError('Unsupported server-owned Serve worker cache kind.')
    volume_name = configured.get('volume_name')
    mount_path = configured.get('mount_path')
    if not isinstance(volume_name, str) or not isinstance(mount_path, str):
        raise ValueError('Node-local cache requires volume_name and '
                         'mount_path.')
    auto_mount_match = any(
        isinstance(entry, dict) and entry.get('volume_name') == volume_name and
        mount_path in entry.get('mount_paths', [])
        for entry in _effective_auto_mounts(context, workspace))
    if not auto_mount_match:
        raise ValueError('Node-local Serve cache must be present in the exact '
                         'context auto_mounts configuration.')
    volume_config: VolumeConfig | None = (
        global_user_state.get_volume_configs_by_names([volume_name
                                                      ]).get(volume_name))
    if (volume_config is None or
            volume_config.type != volume_utils.VolumeType.HOSTPATH.value or
            volume_config.region != context):
        raise ValueError('Node-local Serve cache volume must be a registered '
                         'hostPath in the exact Kubernetes context.')
    host_path = volume_config.config.get('host_path')
    projection = {
        'kind': 'node_local',
        'mount_path': mount_path,
        'volume_name': volume_name,
        'host_path': host_path,
        'attestation': configured.get('attestation'),
    }
    return validate_cache_projection(projection)


def _project_accelerator_scheduling(
    context: str,
    accelerator_name: str,
    workspace: str | None,
) -> dict[str, str | list[str]]:
    """Freeze one server-owned accelerator label and allocation contract."""
    configured = skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        region=context,
        keys=('serve_worker_accelerator_scheduling',),
        workspace=workspace,
        default_value=None)
    scheduling_map = _validate_accelerator_scheduling_map(configured)
    matches = [
        descriptor for logical_name, descriptor in scheduling_map.items()
        if logical_name.lower() == accelerator_name.lower()
    ]
    if len(matches) != 1:
        raise ValueError('Kubernetes context '
                         f'{context!r} has no exact server-owned accelerator '
                         f'scheduling contract for {accelerator_name!r}.')
    return copy.deepcopy(matches[0])


def _catalog_candidates(
    task: task_lib.Task,
    placement_catalog: dict[str, Any] | None,
) -> list[tuple[int, clouds.Cloud | None, str | None,
                Mapping[str, int | float] | None, dict[str, Any]]]:
    resources = list(task.resources)
    if not resources:
        return []
    if placement_catalog is None:
        # ``resources.any_of`` is represented by a set. Its identity-based
        # iteration order changes across process restarts, but the assigned
        # index participates in immutable candidate IDs. Canonicalize the
        # complete resource shape before assigning those IDs.
        resources.sort(key=lambda resource: json.dumps(resource.to_yaml_config(
            redact_secrets=True),
                                                       sort_keys=True,
                                                       separators=(',', ':'),
                                                       default=str))
        return [(index, resource.cloud, resource.region, resource.accelerators,
                 resource.cluster_config_overrides)
                for index, resource in enumerate(resources)]
    catalog = spot_placer.PlacementCatalog.from_dict(placement_catalog)
    cluster_config_overrides = resources[0].cluster_config_overrides
    return [(index, location.cloud, location.region, location.accelerators,
             cluster_config_overrides)
            for index, (location, _) in enumerate(catalog.entries)]


def catalog_missing_task_shapes(
    task: task_lib.Task,
    placement_catalog: dict[str, Any],
) -> set[tuple[str, str, int]]:
    """Return exact Kubernetes task shapes absent from a persisted catalog.

    Catalog feasibility is the immutable launch boundary. Live capacity in a
    context cannot stand in for a catalog entry with the requested accelerator
    name and count.
    """
    declared: set[tuple[str, str, int]] = set()
    for resource in task.resources or []:
        if not _is_exact_kubernetes_cloud(resource.cloud):
            continue
        context = resource.region
        accelerators = resource.accelerators
        if (not isinstance(context, str) or not context or
                not isinstance(accelerators, dict) or len(accelerators) != 1):
            raise ValueError('Every Kubernetes placement resource must pin a '
                             'context and exactly one accelerator shape.')
        accelerator_name, accelerator_count = next(iter(accelerators.items()))
        if (not isinstance(accelerator_name, str) or not accelerator_name or
                isinstance(accelerator_count, bool) or
                not isinstance(accelerator_count,
                               (int, float)) or accelerator_count < 1 or
                not float(accelerator_count).is_integer()):
            raise ValueError('Every Kubernetes placement resource must use a '
                             'positive whole accelerator count.')
        declared.add((context, accelerator_name, int(accelerator_count)))

    present: set[tuple[str, str, int]] = set()

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            cloud = node.get('cloud')
            context = node.get('region')
            accelerators = node.get('accelerators')
            if (isinstance(cloud, str) and cloud.lower() == 'kubernetes' and
                    isinstance(context, str) and context and
                    isinstance(accelerators, dict) and len(accelerators) == 1):
                name, count = next(iter(accelerators.items()))
                if (isinstance(name, str) and name and
                        not isinstance(count, bool) and
                        isinstance(count, (int, float)) and count >= 1 and
                        float(count).is_integer()):
                    present.add((context, name, int(count)))
            for value in node.values():
                _walk(value)
        elif isinstance(node, list):
            for value in node:
                _walk(value)

    _walk(placement_catalog)
    return declared - present


def build_worker_placement_projections(
    task: task_lib.Task,
    *,
    workspace: str | None,
    placement_catalog: dict[str, Any] | None = None,
) -> list[dict[str, Any]] | None:
    """Build exact per-candidate worker identities, or null if ambiguous."""
    # A persisted catalog can materialize an originally unconstrained cloud
    # into concrete locations.  That must not manufacture a trusted
    # Kubernetes projection: the task itself is the immutable declaration of
    # which candidates are eligible for platform-owned identity.
    if any(resource.cloud is None for resource in (task.resources or [])):
        return None
    candidates = _catalog_candidates(task, placement_catalog)
    if not candidates or any(cloud is None for _, cloud, _, _, _ in candidates):
        return None
    projections = []
    for index, cloud, context, accelerators, overrides in candidates:
        if not _is_exact_kubernetes_cloud(cloud):
            continue
        if not isinstance(context, str) or not context:
            return None
        if not isinstance(accelerators, dict) or len(accelerators) != 1:
            return None
        accelerator_name, accelerator_count = next(iter(accelerators.items()))
        if (not isinstance(accelerator_name, str) or
                type(accelerator_count) is not int or accelerator_count < 1):
            return None
        identity = _project_worker_location(context, overrides, workspace)
        if identity['pod_identity_role_arn'] is None:
            return None
        projections.append({
            'candidate_id': f'kubernetes-{index:04d}',
            **identity,
            'accelerator_name': accelerator_name,
            'accelerator_count': accelerator_count,
            'accelerator_scheduling': _project_accelerator_scheduling(
                context, accelerator_name, workspace),
            'cache': _project_cache(context, workspace),
        })
    if not projections:
        return None
    return validate_worker_placement_projections(projections, allow_none=False)


def worker_projection_for_context(
    projections: list[dict[str, Any]] | None,
    context: str,
    accelerators: dict[str, int | float] | None,
) -> dict[str, Any] | None:
    """Select one frozen worker projection for an exact launch candidate."""
    validated = validate_worker_placement_projections(projections)
    if validated is None or not isinstance(accelerators,
                                           dict) or len(accelerators) != 1:
        return None
    name, count = next(iter(accelerators.items()))
    matches = [
        projection for projection in validated
        if projection['kubernetes_context'] == context and
        projection['accelerator_name'].lower() == str(name).lower() and
        projection['accelerator_count'] == count
    ]
    if len(matches) != 1:
        return None
    return copy.deepcopy(matches[0])


def cache_environment(projection: dict[str, Any]) -> dict[str, str]:
    """Return server-owned runtime cache environment for one worker."""
    cache = validate_cache_projection(projection['cache'])
    env = {CACHE_ENV_VAR: cache['kind']}
    if cache['kind'] == 'none':
        return env
    env[f'{CACHE_ENV_PREFIX}MOUNT_PATH'] = cache['mount_path']
    for key, suffix in _CACHE_ATTESTATION_ENV_KEYS.items():
        env[f'{CACHE_ENV_PREFIX}{suffix}'] = str(cache['attestation'][key])
    return env


def controller_cache_environment(projection: dict[str, Any]) -> dict[str, str]:
    """Return runtime environment for a projected campaign-controller Job."""
    cache = validate_controller_work_cache_projection(projection,
                                                      allow_none=False)
    assert cache is not None
    env = {
        f'{CONTROLLER_CACHE_ENV_PREFIX}KIND': cache['kind'],
        f'{CONTROLLER_CACHE_ENV_PREFIX}MOUNT_PATH': cache['mount_path'],
        f'{CONTROLLER_CACHE_ENV_PREFIX}REQUIRED_BYTES': str(
            cache['required_bytes']),
        f'{CONTROLLER_CACHE_ENV_PREFIX}REQUIRED_INODES': str(
            cache['required_inodes']),
    }
    if cache['kind'] == 'empty_dir':
        env[f'{CONTROLLER_CACHE_ENV_PREFIX}SIZE_LIMIT_BYTES'] = str(
            cache['size_limit_bytes'])
        return env
    for key, suffix in _CACHE_ATTESTATION_ENV_KEYS.items():
        controller_suffix = 'ID' if key == 'attestation_id' else suffix
        env[f'{CONTROLLER_CACHE_ENV_PREFIX}ATTESTATION_{controller_suffix}'] = str(
            cache['attestation'][key])
    return env
