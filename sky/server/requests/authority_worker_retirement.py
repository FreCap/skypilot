"""Surviving-API verifier for authority-worker cohort tombstones.

This maintenance path never lists or mutates Kubernetes objects.  It first
uses PostgreSQL's fenced zero-reference transition for stale, never-accepted
cohorts, then commits retirement only after exact-name GETs prove both bound
objects are absent.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
import dataclasses
import hashlib
import json
import os
import re
import stat
from typing import Any, Protocol
import uuid

from sky import sky_logging
from sky.serve import resource_action_state
from sky.serve import resource_actions as actions
from sky.server.requests import resource_actions as kernel_actions

INSTALLATION_ID_ENV_VAR = ('SKYPILOT_RESOURCE_ACTION_AUTHORITY_INSTALLATION_ID')
COHORT_SUFFIXES_ENV_VAR = (
    'SKYPILOT_RESOURCE_ACTION_AUTHORITY_COHORT_SUFFIXES_JSON')
RETIREMENT_TOMBSTONES_ENV_VAR = (
    'SKYPILOT_RESOURCE_ACTION_AUTHORITY_RETIREMENT_TOMBSTONES_JSON')
RELEASE_PREFLIGHT_ENV_VAR = (
    'SKYPILOT_RESOURCE_ACTION_AUTHORITY_RELEASE_PREFLIGHT_JSON')
_POD_NAMESPACE_ENV_VAR = 'SKYPILOT_POD_NAMESPACE'
_RELEASE_NAME_ENV_VAR = 'SKYPILOT_RELEASE_NAME'
_RELEASE_PREFLIGHT_MANIFEST_ROOT = (
    '/etc/skypilot/resource-action-authority/release-preflight')
_DNS_LABEL_RE = re.compile(r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$')
_MAX_MANIFEST_BYTES = 65_536
_SCAN_STATES = (
    actions.WorkerCohortLifecycleState.REGISTERING,
    actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED,
)
_MAINTENANCE_INTERVAL_SECONDS = 30.0

logger = sky_logging.init_logger(__name__)


class AuthorityWorkerRetirementInvariantViolation(RuntimeError):
    """Retirement evidence conflicts with one immutable release identity."""


@dataclasses.dataclass(frozen=True)
class AuthorityWorkerRetirementScope:
    """Exact central-database and Kubernetes scope owned by one API release."""

    installation_id: str
    namespace: str
    helm_full_name: str
    cohort_suffixes: tuple[str, ...]
    retirement_tombstones: tuple[str, ...]

    def __post_init__(self) -> None:
        try:
            installation_id = uuid.UUID(self.installation_id)
        except (AttributeError, TypeError, ValueError) as e:
            raise ValueError('Authority installation ID is not a UUID.') from e
        if str(installation_id) != self.installation_id:
            raise ValueError('Authority installation ID is not canonical.')
        for name, value in (('namespace', self.namespace),
                            ('helm_full_name', self.helm_full_name)):
            if type(value) is not str or _DNS_LABEL_RE.fullmatch(value) is None:
                raise ValueError(f'Authority retirement {name} is not a DNS '
                                 'label.')
        for name, values in (('cohort_suffixes', self.cohort_suffixes),
                             ('retirement_tombstones',
                              self.retirement_tombstones)):
            if (type(values) is not tuple or any(
                    type(value) is not str or
                    _DNS_LABEL_RE.fullmatch(value) is None or len(value) > 42
                    for value in values) or
                    values != tuple(sorted(set(values)))):
                raise ValueError(f'Authority retirement {name} is not a sorted '
                                 'unique DNS-label inventory.')
        if not set(self.cohort_suffixes).isdisjoint(self.retirement_tombstones):
            raise ValueError('Live cohort and tombstone inventories overlap.')
        if not self.cohort_suffixes and not self.retirement_tombstones:
            raise ValueError('Authority retirement scope has no cohort names.')

    @property
    def singleton_name(self) -> str:
        release_scope = hashlib.sha256(
            f'{self.namespace}\n{self.helm_full_name}'.encode()).hexdigest()
        return (f'resource-action-authority-retirement:'
                f'{self.installation_id}:{release_scope}')

    @staticmethod
    def _suffix_inventory(raw: Any, *, name: str) -> tuple[str, ...]:
        if type(raw) is not str:
            raise ValueError(f'{name} environment value is absent.')
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f'{name} is not JSON.') from e
        if type(value) is not list or json.dumps(
                value, sort_keys=True, separators=(',', ':')) != raw:
            raise ValueError(f'{name} is not a canonical JSON array.')
        return tuple(value)

    @classmethod
    def from_environment(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> AuthorityWorkerRetirementScope:
        source = os.environ if environ is None else environ
        installation_id = source.get(INSTALLATION_ID_ENV_VAR)
        namespace = source.get(_POD_NAMESPACE_ENV_VAR)
        helm_full_name = source.get(_RELEASE_NAME_ENV_VAR)
        if any(
                type(value) is not str or not value
                for value in (installation_id, namespace, helm_full_name)):
            raise ValueError(
                'Authority retirement scope environment is incomplete.')
        assert isinstance(installation_id, str)
        assert isinstance(namespace, str)
        assert isinstance(helm_full_name, str)
        return cls(installation_id=installation_id,
                   namespace=namespace,
                   helm_full_name=helm_full_name,
                   cohort_suffixes=cls._suffix_inventory(
                       source.get(COHORT_SUFFIXES_ENV_VAR),
                       name=COHORT_SUFFIXES_ENV_VAR),
                   retirement_tombstones=cls._suffix_inventory(
                       source.get(RETIREMENT_TOMBSTONES_ENV_VAR),
                       name=RETIREMENT_TOMBSTONES_ENV_VAR))


@dataclasses.dataclass(frozen=True)
class AuthorityWorkerReleaseManifestFile:
    """One chart-fixed live cohort manifest mounted into the migration hook."""

    cohort_suffix: str
    path: str

    def __post_init__(self) -> None:
        if (type(self.cohort_suffix) is not str or
                _DNS_LABEL_RE.fullmatch(self.cohort_suffix) is None or
                len(self.cohort_suffix) > 42):
            raise ValueError('Authority release cohort suffix is invalid.')
        expected = (f'{_RELEASE_PREFLIGHT_MANIFEST_ROOT}/'
                    f'{self.cohort_suffix}/manifest.json')
        if type(self.path) is not str or self.path != expected:
            raise ValueError('Authority release manifest path is not fixed.')


def _read_release_manifest(path: str) -> bytes:
    """Descriptor-read one immutable, chart-mounted manifest file."""
    required_flags = ('O_CLOEXEC', 'O_NOFOLLOW', 'O_NONBLOCK')
    if any(not hasattr(os, name) for name in required_flags):
        raise RuntimeError('Secure authority manifest reads are unsupported.')
    descriptor = os.open(
        path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_size < 1 or
                before.st_size > _MAX_MANIFEST_BYTES or before.st_mode & 0o022):
            raise ValueError(
                'Authority release manifest is not an immutable regular file.')
        contents = bytearray()
        while len(contents) < before.st_size:
            chunk = os.read(descriptor, before.st_size - len(contents))
            if not chunk:
                raise ValueError('Authority release manifest was truncated.')
            contents.extend(chunk)
        if os.read(descriptor, 1):
            raise ValueError('Authority release manifest grew during read.')
        after = os.fstat(descriptor)
        signature = lambda item: (item.st_dev, item.st_ino, item.st_mode, item.
                                  st_size, item.st_mtime_ns, item.st_ctime_ns)
        if signature(before) != signature(after):
            raise ValueError('Authority release manifest changed during read.')
        return bytes(contents)
    finally:
        os.close(descriptor)


@dataclasses.dataclass(frozen=True)
class AuthorityWorkerReleasePreflight:
    """Proposed Helm release inventory checked before any object mutation."""

    version: int
    namespace: str
    helm_release_name: str
    helm_full_name: str
    installation_id: str
    enabled: bool
    live_manifest_files: tuple[AuthorityWorkerReleaseManifestFile, ...]
    tombstone_suffixes: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError('Authority release preflight version is invalid.')
        if type(self.enabled) is not bool:
            raise ValueError('Authority release preflight header is invalid.')
        for name, value in (('namespace', self.namespace),
                            ('helm_release_name', self.helm_release_name),
                            ('helm_full_name', self.helm_full_name)):
            if type(value) is not str or _DNS_LABEL_RE.fullmatch(value) is None:
                raise ValueError(f'Authority release {name} is not a DNS '
                                 'label.')
        if type(self.installation_id) is not str:
            raise ValueError('Authority release installation ID is invalid.')
        if self.enabled:
            try:
                installation_id = uuid.UUID(self.installation_id)
            except ValueError as e:
                raise ValueError(
                    'Authority release installation ID is invalid.') from e
            if str(installation_id) != self.installation_id:
                raise ValueError(
                    'Authority release installation ID is not canonical.')
        elif self.installation_id:
            raise ValueError('Disabled authority release has an installation '
                             'ID.')
        if (type(self.live_manifest_files) is not tuple or any(
                type(value) is not AuthorityWorkerReleaseManifestFile
                for value in self.live_manifest_files)):
            raise ValueError('Authority release manifest files are invalid.')
        live_suffixes = tuple(
            value.cohort_suffix for value in self.live_manifest_files)
        if live_suffixes != tuple(sorted(set(live_suffixes))):
            raise ValueError('Authority release manifest files are not sorted '
                             'and unique.')
        if (type(self.tombstone_suffixes) is not tuple or any(
                type(value) is not str or
                _DNS_LABEL_RE.fullmatch(value) is None or len(value) > 42
                for value in self.tombstone_suffixes) or
                self.tombstone_suffixes != tuple(
                    sorted(set(self.tombstone_suffixes)))):
            raise ValueError('Authority release tombstones are not a sorted '
                             'unique DNS-label inventory.')
        if not set(live_suffixes).isdisjoint(self.tombstone_suffixes):
            raise ValueError('Authority release inventories overlap.')
        if len(live_suffixes) + len(self.tombstone_suffixes) > 256:
            raise ValueError('Authority release inventory exceeds 256 cohorts.')
        if self.enabled and not live_suffixes and not self.tombstone_suffixes:
            raise ValueError('Enabled authority release has no inventory.')
        if not self.enabled and (live_suffixes or self.tombstone_suffixes):
            raise ValueError('Disabled authority release has an inventory.')

    @classmethod
    def from_json(cls, raw: str) -> AuthorityWorkerReleasePreflight:
        if type(raw) is not str:
            raise ValueError('Authority release preflight is not text.')
        try:
            value = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as e:
            raise ValueError('Authority release preflight is not JSON.') from e
        keys = {
            'version', 'namespace', 'helm_release_name', 'helm_full_name',
            'installation_id', 'enabled', 'live_manifest_files',
            'tombstone_suffixes'
        }
        if (type(value) is not dict or set(value) != keys or json.dumps(
                value, sort_keys=True, separators=(',', ':')) != raw):
            raise ValueError('Authority release preflight is noncanonical.')
        live_values = value['live_manifest_files']
        tombstones = value['tombstone_suffixes']
        if type(live_values) is not list or type(tombstones) is not list:
            raise ValueError('Authority release inventories must be arrays.')
        files = []
        for item in live_values:
            if type(item) is not dict or set(item) != {'cohort_suffix', 'path'}:
                raise ValueError(
                    'Authority release manifest file entry is invalid.')
            files.append(
                AuthorityWorkerReleaseManifestFile(
                    cohort_suffix=item['cohort_suffix'], path=item['path']))
        return cls(version=value['version'],
                   namespace=value['namespace'],
                   helm_release_name=value['helm_release_name'],
                   helm_full_name=value['helm_full_name'],
                   installation_id=value['installation_id'],
                   enabled=value['enabled'],
                   live_manifest_files=tuple(files),
                   tombstone_suffixes=tuple(tombstones))

    def load_live_manifests(
        self,) -> tuple[actions.ProviderAuthorityWorkerCohortManifestV1, ...]:
        """Read and validate the exact canonical manifests named by Helm."""
        manifests = []
        for manifest_file in self.live_manifest_files:
            contents = _read_release_manifest(manifest_file.path)
            try:
                value = json.loads(contents)
                manifest = (actions.ProviderAuthorityWorkerCohortManifestV1.
                            from_value(value))
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError,
                    ValueError) as e:
                raise ValueError(
                    'Authority release manifest is invalid JSON.') from e
            release_inputs = manifest.pod_template_binding.release_inputs
            if (manifest.canonical_bytes != contents or
                    manifest.namespace != self.namespace or
                    release_inputs.helm_full_name != self.helm_full_name or
                    release_inputs.cohort_suffix
                    != manifest_file.cohort_suffix):
                raise ValueError(
                    'Authority release manifest differs from its Helm fence.')
            manifests.append(manifest)
        return tuple(manifests)

    @classmethod
    def from_environment(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> AuthorityWorkerReleasePreflight:
        source = os.environ if environ is None else environ
        raw = source.get(RELEASE_PREFLIGHT_ENV_VAR)
        if type(raw) is not str:
            raise ValueError('Authority release preflight is absent.')
        return cls.from_json(raw)


def is_configured(environ: Mapping[str, str] | None = None) -> bool:
    source = os.environ if environ is None else environ
    return INSTALLATION_ID_ENV_VAR in source


class AuthorityWorkerRetirementStore(Protocol):
    """Durable operations used by the authority retirement verifier."""

    def preflight_authority_release(
        self,
        namespace: str,
        helm_release_name: str,
        helm_full_name: str,
        installation_id: str,
        enabled: bool,
        live_manifests: tuple[actions.ProviderAuthorityWorkerCohortManifestV1,
                              ...],
        tombstone_suffixes: tuple[str, ...],
    ) -> resource_action_state.AuthorityReleaseRecord | None:
        ...

    def list_worker_cohorts_for_installation(
        self,
        installation_id: str,
        lifecycle_states: tuple[actions.WorkerCohortLifecycleState, ...],
        *,
        limit: int = 128,
    ) -> tuple[resource_action_state.WorkerCohortRecord, ...]:
        ...

    def authorize_stale_worker_cohort_removal(
        self,
        cohort_identity: actions.WorkerCohortIdentityV1,
        expected_revision: int,
        expected_registration_attestations: actions.
        WorkerCohortRegistrationSetV1,
    ) -> resource_action_state.WorkerCohortTransition:
        ...

    def retire_worker_cohort(
        self,
        cohort_identity: actions.WorkerCohortIdentityV1,
        expected_revision: int,
        expected_registration_attestations: actions.
        WorkerCohortRegistrationSetV1,
        *,
        deployment_not_found: bool,
        service_account_not_found: bool,
    ) -> resource_action_state.WorkerCohortTransition:
        ...


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


class ExactAuthorityWorkerTombstoneObserver:
    """GET only the two immutable names and distinguish exact NotFound."""

    def __init__(self, core_api: Any, apps_api: Any,
                 api_exception_type: type[BaseException]) -> None:
        self._core_api = core_api
        self._apps_api = apps_api
        self._api_exception_type = api_exception_type

    def _is_not_found(self, read: Any, *, api_version: str, kind: str,
                      namespace: str, name: str, uid: str) -> bool:
        try:
            value = read(name, namespace)
        except self._api_exception_type as e:
            if getattr(e, 'status', None) == 404:
                return True
            raise
        metadata = _field(value, 'metadata')
        actual = (_field(value, 'api_version'), _field(value, 'kind'),
                  _field(metadata,
                         'namespace'), _field(metadata,
                                              'name'), _field(metadata, 'uid'))
        expected = (api_version, kind, namespace, name, uid)
        if actual != expected:
            raise AuthorityWorkerRetirementInvariantViolation(
                f'Live {kind} identity differs from its cohort tombstone.')
        return False

    def exact_not_found_pair(
        self,
        record: resource_action_state.WorkerCohortRecord,
    ) -> tuple[bool, bool]:
        manifest = record.cohort_identity.manifest
        deployment_not_found = self._is_not_found(
            self._apps_api.read_namespaced_deployment,
            api_version='apps/v1',
            kind='Deployment',
            namespace=manifest.namespace,
            name=manifest.deployment_name,
            uid=record.cohort_identity.deployment_uid)
        service_account_not_found = self._is_not_found(
            self._core_api.read_namespaced_service_account,
            api_version='v1',
            kind='ServiceAccount',
            namespace=manifest.namespace,
            name=manifest.service_account_name,
            uid=record.cohort_identity.service_account_uid)
        return deployment_not_found, service_account_not_found


@dataclasses.dataclass(frozen=True)
class AuthorityWorkerRetirementPass:
    scanned: int
    authorized: int
    retired: int


class AuthorityWorkerRetirementVerifier:
    """Apply the two fenced retirement stages for one exact installation."""

    def __init__(self, scope: AuthorityWorkerRetirementScope,
                 store: AuthorityWorkerRetirementStore,
                 observer: ExactAuthorityWorkerTombstoneObserver) -> None:
        self._scope = scope
        self._store = store
        self._observer = observer

    def _require_scope(
        self,
        record: resource_action_state.WorkerCohortRecord,
    ) -> str:
        manifest = record.cohort_identity.manifest
        release_inputs = manifest.pod_template_binding.release_inputs
        parts = record.cohort_id.split(':')
        if len(parts) != 4:
            raise AuthorityWorkerRetirementInvariantViolation(
                'Central cohort row has a malformed full identity.')
        prefix, installation_id, scope_digest, suffix = parts
        expected_digest = hashlib.sha256(
            f'{self._scope.namespace}\n{self._scope.helm_full_name}\n{suffix}'.
            encode()).hexdigest()
        if (prefix != 'ra' or installation_id != self._scope.installation_id or
                scope_digest != expected_digest or
                manifest.namespace != self._scope.namespace or
                release_inputs.helm_full_name != self._scope.helm_full_name or
                release_inputs.cohort_suffix != suffix):
            raise AuthorityWorkerRetirementInvariantViolation(
                'Central cohort row is outside the API release scope.')
        if suffix in self._scope.cohort_suffixes:
            return 'live'
        if suffix in self._scope.retirement_tombstones:
            return 'tombstone'
        raise AuthorityWorkerRetirementInvariantViolation(
            'Central cohort row is absent from the chart-fixed inventory.')

    def run_once(self) -> AuthorityWorkerRetirementPass:
        records = self._store.list_worker_cohorts_for_installation(
            self._scope.installation_id, _SCAN_STATES)
        authorized = 0
        retired = 0
        for initial in records:
            try:
                inventory = self._require_scope(initial)
            except AuthorityWorkerRetirementInvariantViolation as e:
                logger.error(f'Authority cohort {initial.cohort_id!r} failed '
                             f'retirement scope validation: {e}')
                continue
            record = initial
            if record.lifecycle_state is (
                    actions.WorkerCohortLifecycleState.REGISTERING):
                if inventory != 'live':
                    logger.error(
                        f'Authority cohort {record.cohort_id!r} is still '
                        'REGISTERING after entering the chart tombstone '
                        'inventory; leaving it untouched.')
                    continue
                try:
                    transition = (
                        self._store.authorize_stale_worker_cohort_removal(
                            record.cohort_identity, record.revision,
                            record.registration_attestations))
                except (kernel_actions.ActionConflict,
                        kernel_actions.StaleRevision):
                    continue
                record = transition.record
                authorized += int(not transition.adopted)
            if record.lifecycle_state is not (
                    actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED):
                logger.error(
                    f'Authority cohort {record.cohort_id!r} returned an '
                    'unexpected lifecycle state; leaving it untouched.')
                continue
            if inventory != 'tombstone':
                # The operator must first move the authorized suffix into the
                # chart tombstone inventory, removing only its exact objects
                # while granting this API role exact-name GETs.
                continue
            try:
                deployment_absent, service_account_absent = (
                    self._observer.exact_not_found_pair(record))
            except AuthorityWorkerRetirementInvariantViolation as e:
                logger.error(f'Authority cohort {record.cohort_id!r} has '
                             f'conflicting tombstone identity: {e}')
                continue
            except Exception as e:  # pylint: disable=broad-except
                logger.warning(f'Authority cohort {record.cohort_id!r} '
                               f'tombstone GET is unavailable: {e}')
                continue
            if not deployment_absent or not service_account_absent:
                continue
            try:
                transition = self._store.retire_worker_cohort(
                    record.cohort_identity,
                    record.revision,
                    record.registration_attestations,
                    deployment_not_found=True,
                    service_account_not_found=True)
            except (kernel_actions.ActionConflict,
                    kernel_actions.StaleRevision):
                continue
            retired += int(not transition.adopted)
        return AuthorityWorkerRetirementPass(len(records), authorized, retired)


def validate_release_preflight(
    preflight: AuthorityWorkerReleasePreflight,
    store: AuthorityWorkerRetirementStore | None = None,
) -> resource_action_state.AuthorityReleaseRecord | None:
    """Reject an unsafe Helm inventory before its objects can be applied."""
    if type(preflight) is not AuthorityWorkerReleasePreflight:
        raise TypeError('preflight has an invalid type.')
    if store is None:
        store = resource_action_state.PostgresServeResourceActionStateStore()
    return store.preflight_authority_release(
        preflight.namespace, preflight.helm_release_name,
        preflight.helm_full_name, preflight.installation_id, preflight.enabled,
        preflight.load_live_manifests(), preflight.tombstone_suffixes)


def build_default_verifier(
    scope: AuthorityWorkerRetirementScope,
) -> AuthorityWorkerRetirementVerifier:
    """Build the in-cluster GET-only verifier for the surviving API role."""
    # The adapter import is isolated to the configured API maintenance path and
    # the explicit in-cluster context forbids an ambient kubeconfig fallback.
    # pylint: disable=import-outside-toplevel
    from sky.adaptors import kubernetes
    context = kubernetes.in_cluster_context_name()
    observer = ExactAuthorityWorkerTombstoneObserver(
        kubernetes.core_api(context), kubernetes.apps_api(context),
        kubernetes.api_exception())
    return AuthorityWorkerRetirementVerifier(
        scope, resource_action_state.PostgresServeResourceActionStateStore(),
        observer)


async def retirement_verifier_daemon() -> None:
    """Run bounded retirement passes under the API distributed singleton."""
    scope = AuthorityWorkerRetirementScope.from_environment()
    verifier = build_default_verifier(scope)
    while True:
        try:
            result = await asyncio.to_thread(verifier.run_once)
            if result.authorized or result.retired:
                logger.info('Authority-worker retirement pass committed '
                            f'authorized={result.authorized} '
                            f'retired={result.retired}.')
        except asyncio.CancelledError:
            logger.info('Authority-worker retirement verifier cancelled.')
            raise
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Authority-worker retirement verifier failed: {e}')
        await asyncio.sleep(_MAINTENANCE_INTERVAL_SECONDS)


__all__ = [
    'AuthorityWorkerRetirementInvariantViolation',
    'AuthorityWorkerRetirementPass',
    'AuthorityWorkerReleasePreflight',
    'AuthorityWorkerReleaseManifestFile',
    'AuthorityWorkerRetirementScope',
    'AuthorityWorkerRetirementVerifier',
    'COHORT_SUFFIXES_ENV_VAR',
    'ExactAuthorityWorkerTombstoneObserver',
    'INSTALLATION_ID_ENV_VAR',
    'RETIREMENT_TOMBSTONES_ENV_VAR',
    'RELEASE_PREFLIGHT_ENV_VAR',
    'build_default_verifier',
    'is_configured',
    'retirement_verifier_daemon',
    'validate_release_preflight',
]
