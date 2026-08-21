"""Derived API-server runtime profiles and fail-closed local-byte guards."""

import ipaddress
import os
import re
from typing import Any, Iterable
import urllib.parse

from sky import exceptions
from sky.catalog import common as service_catalog_common
from sky.skylet import constants

_REQUEST_BACKEND_ENV_VAR = 'SKYPILOT_API_REQUEST_BACKEND'
_SERVER_ROLE_ENV_VAR = 'SKYPILOT_API_SERVER_ROLE'
_POSTGRES_REQUEST_BACKEND = 'postgres'
_ROLE_SPLIT_SERVER_ROLES = frozenset({'api', 'executor', 'controller'})
_REMOTE_OBJECT_SCHEMES = frozenset({
    'cos',
    'cw',
    'gs',
    'hf',
    'http',
    'https',
    'minio',
    'nebius',
    'oci',
    'r2',
    's3',
    'vastdata',
})
_REMOTE_GIT_SCHEMES = frozenset({'git', 'http', 'https', 'ssh'})
_SCP_GIT_URL = re.compile(r'^(?:[^@/:]+@)?(?P<host>[^/:]+):(?P<path>.+)$')

GUARDED_HA_LOCAL_ARTIFACT_ERROR_CODE = (
    'guarded_ha_local_artifacts_unsupported')


def _is_remote_host(host: str | None) -> bool:
    if host is None or host.lower() in {'localhost', 'localhost.localdomain'}:
        return False
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return True
    return not (address.is_loopback or address.is_unspecified)


def _is_remote_uri(source: str, allowed_schemes: frozenset[str]) -> bool:
    """Whether a source is an approved URI with a remote authority."""
    try:
        parsed = urllib.parse.urlsplit(source)
    except ValueError:
        return False
    return (parsed.scheme.lower() in allowed_schemes and bool(parsed.netloc) and
            _is_remote_host(parsed.hostname))


def _is_remote_object_uri(source: str) -> bool:
    return _is_remote_uri(source, _REMOTE_OBJECT_SCHEMES)


def _is_remote_workdir(workdir: Any) -> bool:
    """Whether a serialized Git workdir is independently fetchable."""
    if not isinstance(workdir, dict):
        return False
    url = workdir.get('url')
    if not isinstance(url, str):
        return False
    if _is_remote_uri(url, _REMOTE_GIT_SCHEMES):
        return True
    if '://' in url:
        return False
    scp_match = _SCP_GIT_URL.fullmatch(url)
    return (scp_match is not None and _is_remote_host(scp_match.group('host')))


def guarded_ha_ephemeral_artifacts_enabled() -> bool:
    """Whether this process is in role-split PostgreSQL HA without storage.

    This profile is deliberately derived from the three independently
    projected deployment facts.  It is not a feature selector that can drift
    from the actual request, role, or filesystem topology.
    """
    storage_enabled = os.environ.get(
        constants.SKYPILOT_API_SERVER_STORAGE_ENABLED, 'true').lower()
    return (os.environ.get(_REQUEST_BACKEND_ENV_VAR)
            == _POSTGRES_REQUEST_BACKEND and
            os.environ.get(_SERVER_ROLE_ENV_VAR) in _ROLE_SPLIT_SERVER_ROLES and
            storage_enabled == 'false')


class GuardedHALocalArtifactError(exceptions.NotSupportedError):
    """A guarded HA request requires bytes unavailable across role pods."""

    def __init__(self, operation: str) -> None:
        self.operation = operation
        super().__init__(
            f'{operation} is unavailable in PostgreSQL guarded HA because '
            'pod-local files are disposable and are not shared between API '
            'server roles. Use an immutable image, an approved remote object '
            'URI, or PostgreSQL-backed status/result APIs instead.')


def reject_local_artifact_operation(operation: str) -> None:
    """Fail closed when ``operation`` would publish or consume local bytes."""
    if guarded_ha_ephemeral_artifacts_enabled():
        raise GuardedHALocalArtifactError(operation)


def _serialized_sources_are_remote(source: Any) -> bool:
    if source is None:
        return True
    sources: Iterable[Any]
    if isinstance(source, str):
        sources = [source]
    elif isinstance(source, (list, tuple)):
        sources = source
    else:
        return False
    return all(
        isinstance(item, str) and _is_remote_object_uri(item)
        for item in sources)


def validate_serialized_task_artifact_inputs(
    configs: Iterable[Any],
    *,
    product: str,
) -> None:
    """Reject serialized local inputs before parsing can stat their paths."""
    if not guarded_ha_ephemeral_artifacts_enabled():
        return
    for index, config in enumerate(configs):
        if config is None:
            continue
        if not isinstance(config, dict):
            raise GuardedHALocalArtifactError(
                f'{product} task {index} malformed local-input envelope')
        workdir = config.get('workdir')
        if workdir is not None and not _is_remote_workdir(workdir):
            raise GuardedHALocalArtifactError(
                f'{product} task {index} local workdir')
        if config.get('file_mounts_mapping'):
            raise GuardedHALocalArtifactError(
                f'{product} task {index} local file mounts')
        file_mounts = config.get('file_mounts') or {}
        if not isinstance(file_mounts, dict):
            raise GuardedHALocalArtifactError(
                f'{product} task {index} malformed file mounts')
        for source in file_mounts.values():
            if isinstance(source, dict):
                source = source.get('source')
            if not _serialized_sources_are_remote(source):
                raise GuardedHALocalArtifactError(
                    f'{product} task {index} local file or storage mounts')
        for service_key in ('service', 'pool'):
            service = config.get(service_key)
            if isinstance(service, dict) and service.get('tls') is not None:
                raise GuardedHALocalArtifactError(
                    f'{product} task {index} local TLS credentials')


def validate_task_artifact_inputs(
    tasks: Iterable[Any],
    *,
    product: str,
    modified_catalogs_present: bool,
) -> None:
    """Reject final policy-mutated tasks that require process-local bytes."""
    if not guarded_ha_ephemeral_artifacts_enabled():
        return
    for index, task in enumerate(tasks):
        if task.workdir is not None and not _is_remote_workdir(task.workdir):
            raise GuardedHALocalArtifactError(
                f'{product} task {index} local workdir')
        if getattr(task, 'file_mounts_mapping', None):
            raise GuardedHALocalArtifactError(
                f'{product} task {index} local file mounts')
        for source in (getattr(task, 'file_mounts', None) or {}).values():
            if (not isinstance(source, str) or
                    not _is_remote_object_uri(source)):
                raise GuardedHALocalArtifactError(
                    f'{product} task {index} local file mounts')
        for storage in (getattr(task, 'storage_mounts', None) or {}).values():
            sources = getattr(storage, 'source', None)
            if isinstance(sources, str):
                sources = [sources]
            if sources is None:
                continue
            if (not isinstance(sources, (list, tuple)) or
                    any(not isinstance(source, str) or
                        not _is_remote_object_uri(source)
                        for source in sources)):
                raise GuardedHALocalArtifactError(
                    f'{product} task {index} local storage sources')
        service = getattr(task, 'service', None)
        if (service is not None and
                getattr(service, 'tls_credential', None) is not None):
            raise GuardedHALocalArtifactError(
                f'{product} task {index} local TLS credentials')
    if modified_catalogs_present:
        raise GuardedHALocalArtifactError(
            f'{product} process-local modified service catalogs')


def validate_final_task_artifact_inputs(tasks: Iterable[Any], *,
                                        product: str) -> None:
    """Validate a final policy-mutated DAG using the packaged server state."""
    if not guarded_ha_ephemeral_artifacts_enabled():
        return
    validate_task_artifact_inputs(
        tasks,
        product=product,
        modified_catalogs_present=bool(
            service_catalog_common.get_modified_catalog_file_mounts()),
    )
