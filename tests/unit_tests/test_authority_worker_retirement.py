"""Fail-closed tests for API-owned authority-worker retirement."""
# pylint: disable=protected-access

import copy
import dataclasses
import datetime
import hashlib
import json
from unittest import mock

import pytest
import serve_resource_action_test_fixtures as authority_fixtures

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_state
from sky.serve import resource_actions as actions
from sky.server.requests import authority_worker_retirement as retirement

_NOW = datetime.datetime(2026, 8, 2, 1, 2, 3, 4, tzinfo=datetime.timezone.utc)


class _ApiError(Exception):
    """Minimal Kubernetes ApiException substitute with an HTTP status."""

    def __init__(self, status: int):
        super().__init__(f'Kubernetes API returned {status}.')
        self.status = status


def _scope(
    *,
    live: tuple[str, ...] = (authority_fixtures.COHORT_SUFFIX,),
    tombstones: tuple[str, ...] = (),
) -> retirement.AuthorityWorkerRetirementScope:
    return retirement.AuthorityWorkerRetirementScope(
        installation_id=authority_fixtures.INSTALLATION_ID,
        namespace=authority_fixtures.NAMESPACE,
        helm_full_name=authority_fixtures.HELM_FULL_NAME,
        cohort_suffixes=live,
        retirement_tombstones=tombstones)


def _registrations() -> actions.WorkerCohortRegistrationSetV1:
    cohort = actions.WorkerCohortIdentityV1.from_value(
        authority_fixtures.authority_cohort_value())
    registration = actions.ProviderAuthorityWorkerRegistrationV1.from_value({
        'worker': authority_fixtures.authority_worker_value(),
        'pod_ready': True,
        'deployment_spec_replicas': 2,
        'deployment_status_observed_generation': 5,
        'deployment_status_replicas': 2,
        'deployment_updated_replicas': 2,
        'deployment_ready_replicas': 2,
        'deployment_available_replicas': 2,
        'deployment_unavailable_replicas': 0,
        'registered_at': '2026-08-02T01:02:03.000004Z',
    })
    return actions.WorkerCohortRegistrationSetV1(
        version=1,
        cohort_identity_sha256=cohort.sha256,
        workers=(registration,))


def _record(
    state: actions.WorkerCohortLifecycleState,
    *,
    revision: int = 1,
) -> resource_action_state.WorkerCohortRecord:
    return resource_action_state.WorkerCohortRecord(
        cohort_identity=actions.WorkerCohortIdentityV1.from_value(
            authority_fixtures.authority_cohort_value()),
        registration_attestations=_registrations(),
        lifecycle_state=state,
        revision=revision,
        created_at=_NOW,
        state_changed_at=_NOW,
        retired_at=None)


def _transition(
    record: resource_action_state.WorkerCohortRecord,
    state: actions.WorkerCohortLifecycleState,
) -> resource_action_state.WorkerCohortTransition:
    retired_at = (_NOW if state is actions.WorkerCohortLifecycleState.RETIRED
                  else None)
    return resource_action_state.WorkerCohortTransition(
        dataclasses.replace(record,
                            lifecycle_state=state,
                            revision=record.revision + 1,
                            state_changed_at=_NOW,
                            retired_at=retired_at))


def _store(*records: resource_action_state.WorkerCohortRecord,) -> mock.Mock:
    store = mock.Mock()
    store.list_worker_cohorts_for_installation.return_value = tuple(records)
    return store


def _observer(
        core_api: mock.Mock, apps_api: mock.Mock
) -> retirement.ExactAuthorityWorkerTombstoneObserver:
    return retirement.ExactAuthorityWorkerTombstoneObserver(
        core_api, apps_api, _ApiError)


def _release_preflight_value(root: str) -> dict:
    return {
        'version': 1,
        'namespace': authority_fixtures.NAMESPACE,
        'helm_release_name': 'stable-release',
        'helm_full_name': authority_fixtures.HELM_FULL_NAME,
        'installation_id': authority_fixtures.INSTALLATION_ID,
        'enabled': True,
        'live_manifest_files': [{
            'cohort_suffix': authority_fixtures.COHORT_SUFFIX,
            'path': (f'{root}/{authority_fixtures.COHORT_SUFFIX}/'
                     'manifest.json'),
        }],
        'tombstone_suffixes': [],
    }


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def _cleanup_environment() -> dict[str, str]:
    return {
        retirement.ACCEPTED_V1_CLEANUP_MODE_ENV_VAR: 'true',
        retirement.ACCEPTED_V1_CLEANUP_DESELECTED_SUFFIXES_ENV_VAR:
            _canonical_json([authority_fixtures.COHORT_SUFFIX]),
        retirement.INSTALLATION_ID_ENV_VAR: authority_fixtures.INSTALLATION_ID,
        retirement.COHORT_SUFFIXES_ENV_VAR: _canonical_json(
            [authority_fixtures.COHORT_SUFFIX]),
        retirement.RETIREMENT_TOMBSTONES_ENV_VAR: _canonical_json([]),
        'SKYPILOT_POD_NAMESPACE': authority_fixtures.NAMESPACE,
        'SKYPILOT_RELEASE_NAME': authority_fixtures.HELM_FULL_NAME,
        'SKYPILOT_API_SERVER_ROLE': 'api',
        'SKYPILOT_API_REQUEST_BACKEND': 'postgres',
        'SKYPILOT_STATE_DB_MIGRATION_MODE': 'verify',
        'IS_SKYPILOT_SERVER': 'true',
    }


def test_accepted_v1_cleanup_environment_is_inert_and_exact() -> None:
    scope = retirement.AuthorityWorkerAcceptedV1CleanupScope.from_environment(
        _cleanup_environment())

    assert scope.retirement_scope == _scope()
    assert scope.deselected_cohort_suffixes == (
        authority_fixtures.COHORT_SUFFIX,)


@pytest.mark.parametrize(('name', 'value', 'match'), [
    ('SKYPILOT_API_SERVER_ROLE', 'all', 'SERVER_ROLE'),
    ('SKYPILOT_STATE_DB_MIGRATION_MODE', 'upgrade', 'MIGRATION_MODE'),
    ('SKYPILOT_RESOURCE_ACTION_AUTHORITY_ENABLED', 'true',
     'forbids private authority'),
    ('SKYPILOT_RESOURCE_ACTION_AUTHORITY_ACTIVE_COHORT', 'v1',
     'forbids an active cohort'),
])
def test_accepted_v1_cleanup_environment_rejects_runtime_authority(
        name: str, value: str, match: str) -> None:
    environ = _cleanup_environment()
    environ[name] = value

    with pytest.raises(ValueError, match=match):
        retirement.AuthorityWorkerAcceptedV1CleanupScope.from_environment(
            environ)


def test_accepted_v1_cleanup_environment_requires_complete_deselection(
) -> None:
    environ = _cleanup_environment()
    environ[retirement.ACCEPTED_V1_CLEANUP_DESELECTED_SUFFIXES_ENV_VAR] = '[]'

    with pytest.raises(ValueError, match='nonempty'):
        retirement.AuthorityWorkerAcceptedV1CleanupScope.from_environment(
            environ)


def test_accepted_v1_cleanup_advances_exact_deselected_record() -> None:
    initial = _record(actions.WorkerCohortLifecycleState.ACCEPTING, revision=7)
    authorized = dataclasses.replace(
        initial,
        lifecycle_state=actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED,
        revision=9)
    store = _store(initial)
    store.authorize_accepted_v1_worker_cohort_removal.return_value = (
        resource_action_state.WorkerCohortTransition(authorized))
    scope = retirement.AuthorityWorkerAcceptedV1CleanupScope(
        retirement_scope=_scope(),
        deselected_cohort_suffixes=(authority_fixtures.COHORT_SUFFIX,))

    result = retirement.AuthorityWorkerAcceptedV1Cleanup(scope,
                                                         store).run_once()

    assert result == retirement.AuthorityWorkerAcceptedV1CleanupPass(
        scanned=1, authorized=1)
    store.list_worker_cohorts_for_installation.assert_called_once_with(
        authority_fixtures.INSTALLATION_ID,
        (actions.WorkerCohortLifecycleState.ACCEPTING,
         actions.WorkerCohortLifecycleState.DRAINING),
        limit=256)
    store.authorize_accepted_v1_worker_cohort_removal.assert_called_once_with(
        initial.cohort_identity, initial.revision, initial.lifecycle_state,
        initial.registration_attestations)


def test_accepted_v1_cleanup_fails_closed_for_tombstoned_nonterminal_record(
) -> None:
    initial = _record(actions.WorkerCohortLifecycleState.DRAINING)
    store = _store(initial)
    scope = retirement.AuthorityWorkerAcceptedV1CleanupScope(
        retirement_scope=_scope(live=('other-v1',),
                                tombstones=(authority_fixtures.COHORT_SUFFIX,)),
        deselected_cohort_suffixes=('other-v1',))

    with pytest.raises(retirement.AuthorityWorkerRetirementInvariantViolation,
                       match='without an exact live deselection fence'):
        retirement.AuthorityWorkerAcceptedV1Cleanup(scope, store).run_once()


def test_release_preflight_reads_exact_manifest_and_calls_durable_fence(
        tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = str(tmp_path / 'release-preflight')
    monkeypatch.setattr(retirement, '_RELEASE_PREFLIGHT_MANIFEST_ROOT', root)
    manifest = actions.ProviderAuthorityWorkerCohortManifestV1.from_value(
        authority_fixtures.authority_manifest_value())
    path = tmp_path / 'release-preflight' / authority_fixtures.COHORT_SUFFIX
    path.mkdir(parents=True)
    (path / 'manifest.json').write_bytes(manifest.canonical_bytes)
    preflight = retirement.AuthorityWorkerReleasePreflight.from_json(
        _canonical_json(_release_preflight_value(root)))
    store = mock.Mock()
    record = mock.sentinel.release_record
    store.preflight_authority_release.return_value = record
    read_manifest = mock.Mock(wraps=retirement._read_release_manifest)
    monkeypatch.setattr(retirement, '_read_release_manifest', read_manifest)

    assert retirement.validate_release_preflight(preflight, store) is record
    read_manifest.assert_called_once_with(str(path / 'manifest.json'))
    store.preflight_authority_release.assert_called_once_with(
        authority_fixtures.NAMESPACE, 'stable-release',
        authority_fixtures.HELM_FULL_NAME, authority_fixtures.INSTALLATION_ID,
        True, (manifest,), ())


def test_release_preflight_dispatches_exact_numeric_v2_to_additive_store(
        tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = str(tmp_path / 'release-preflight')
    monkeypatch.setattr(retirement, '_RELEASE_PREFLIGHT_MANIFEST_ROOT', root)
    value = copy.deepcopy(authority_fixtures.authority_manifest_value())
    value['version'] = 2
    value['claim_contract'] = 'frozen_action_cohort_join_v2'
    manifest = authority.ProviderAuthorityWorkerCohortManifestV2.from_value(
        value)
    path = tmp_path / 'release-preflight' / authority_fixtures.COHORT_SUFFIX
    path.mkdir(parents=True)
    (path / 'manifest.json').write_bytes(manifest.canonical_bytes)
    preflight = retirement.AuthorityWorkerReleasePreflight.from_json(
        _canonical_json(_release_preflight_value(root)))
    store_v1 = mock.Mock()
    store_v2 = mock.Mock()
    record = mock.sentinel.release_record_v2
    store_v2.preflight_authority_release_v2.return_value = record
    read_manifest = mock.Mock(wraps=retirement._read_release_manifest)
    monkeypatch.setattr(retirement, '_read_release_manifest', read_manifest)

    assert retirement.validate_release_preflight(preflight, store_v1,
                                                 store_v2) is record
    read_manifest.assert_called_once_with(str(path / 'manifest.json'))
    store_v1.preflight_authority_release.assert_not_called()
    store_v2.preflight_authority_release_v2.assert_called_once_with(
        authority_fixtures.NAMESPACE, 'stable-release',
        authority_fixtures.HELM_FULL_NAME, authority_fixtures.INSTALLATION_ID,
        True, (manifest,), ())


@pytest.mark.parametrize('version', (True, 2.0, '2', 3))
def test_release_preflight_dispatch_rejects_nonexact_manifest_version(
        tmp_path, monkeypatch: pytest.MonkeyPatch, version: object) -> None:
    root = str(tmp_path / 'release-preflight')
    monkeypatch.setattr(retirement, '_RELEASE_PREFLIGHT_MANIFEST_ROOT', root)
    value = copy.deepcopy(authority_fixtures.authority_manifest_value())
    value['version'] = version
    path = tmp_path / 'release-preflight' / authority_fixtures.COHORT_SUFFIX
    path.mkdir(parents=True)
    (path / 'manifest.json').write_bytes(
        json.dumps(value, sort_keys=True, separators=(',', ':')).encode())
    preflight = retirement.AuthorityWorkerReleasePreflight.from_json(
        _canonical_json(_release_preflight_value(root)))

    with pytest.raises(ValueError, match='integer 1 or 2'):
        retirement.validate_release_preflight(preflight, mock.Mock(),
                                              mock.Mock())


@pytest.mark.parametrize(('field', 'value', 'match'), [
    ('version', True, 'version is invalid'),
    ('live_manifest_files', 'not-an-array', 'inventories must be arrays'),
    ('tombstone_suffixes', 'not-an-array', 'inventories must be arrays'),
])
def test_release_preflight_rejects_boolean_version_and_nonarray_inventories(
        tmp_path, monkeypatch: pytest.MonkeyPatch, field: str, value: object,
        match: str) -> None:
    root = str(tmp_path / 'release-preflight')
    monkeypatch.setattr(retirement, '_RELEASE_PREFLIGHT_MANIFEST_ROOT', root)
    payload = _release_preflight_value(root)
    payload[field] = value

    with pytest.raises(ValueError, match=match):
        retirement.AuthorityWorkerReleasePreflight.from_json(
            _canonical_json(payload))


def test_release_preflight_rejects_unfixed_path(
        tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = str(tmp_path / 'release-preflight')
    monkeypatch.setattr(retirement, '_RELEASE_PREFLIGHT_MANIFEST_ROOT', root)
    payload = _release_preflight_value(root)
    payload['live_manifest_files'][0]['path'] = f'{root}/../manifest.json'

    with pytest.raises(ValueError, match='path is not fixed'):
        retirement.AuthorityWorkerReleasePreflight.from_json(
            _canonical_json(payload))


def test_release_preflight_rejects_text_subclass() -> None:

    class _Text(str):
        """Adversarial string subclass rejected at the parser boundary."""

        pass

    with pytest.raises(ValueError, match='not text'):
        retirement.AuthorityWorkerReleasePreflight.from_json(_Text('{}'))


def test_release_preflight_rejects_noncanonical_or_writable_manifest(
        tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = str(tmp_path / 'release-preflight')
    monkeypatch.setattr(retirement, '_RELEASE_PREFLIGHT_MANIFEST_ROOT', root)
    path = tmp_path / 'release-preflight' / authority_fixtures.COHORT_SUFFIX
    path.mkdir(parents=True)
    manifest_path = path / 'manifest.json'
    manifest_path.write_text(
        json.dumps(authority_fixtures.authority_manifest_value(), indent=2))
    preflight = retirement.AuthorityWorkerReleasePreflight.from_json(
        _canonical_json(_release_preflight_value(root)))

    with pytest.raises(ValueError, match='differs from its Helm fence'):
        preflight.load_live_manifests()

    manifest = actions.ProviderAuthorityWorkerCohortManifestV1.from_value(
        authority_fixtures.authority_manifest_value())
    manifest_path.write_bytes(manifest.canonical_bytes)
    manifest_path.chmod(0o666)
    with pytest.raises(ValueError, match='immutable regular file'):
        preflight.load_live_manifests()


def test_disabled_release_preflight_is_an_empty_stable_anchor_request() -> None:
    payload = {
        'version': 1,
        'namespace': authority_fixtures.NAMESPACE,
        'helm_release_name': 'stable-release',
        'helm_full_name': authority_fixtures.HELM_FULL_NAME,
        'installation_id': '',
        'enabled': False,
        'live_manifest_files': [],
        'tombstone_suffixes': [],
    }
    preflight = retirement.AuthorityWorkerReleasePreflight.from_json(
        _canonical_json(payload))
    store = mock.Mock()
    store.preflight_authority_release.return_value = None

    assert retirement.validate_release_preflight(preflight, store) is None
    store.preflight_authority_release.assert_called_once_with(
        authority_fixtures.NAMESPACE, 'stable-release',
        authority_fixtures.HELM_FULL_NAME, '', False, (), ())


def test_scope_from_environment_requires_canonical_fixed_inventory() -> None:
    environ = {
        retirement.INSTALLATION_ID_ENV_VAR: authority_fixtures.INSTALLATION_ID,
        'SKYPILOT_POD_NAMESPACE': authority_fixtures.NAMESPACE,
        'SKYPILOT_RELEASE_NAME': authority_fixtures.HELM_FULL_NAME,
        retirement.COHORT_SUFFIXES_ENV_VAR: '["p2a-v1"]',
        retirement.RETIREMENT_TOMBSTONES_ENV_VAR: '["retired-a","retired-z"]',
    }

    scope = retirement.AuthorityWorkerRetirementScope.from_environment(environ)

    assert scope.cohort_suffixes == ('p2a-v1',)
    assert scope.retirement_tombstones == ('retired-a', 'retired-z')
    release_digest = hashlib.sha256(
        f'{authority_fixtures.NAMESPACE}\n'
        f'{authority_fixtures.HELM_FULL_NAME}'.encode('utf-8')).hexdigest()
    assert scope.singleton_name == (
        'resource-action-authority-retirement:'
        f'{authority_fixtures.INSTALLATION_ID}:{release_digest}')


@pytest.mark.parametrize(('key', 'value', 'match'), [
    (retirement.INSTALLATION_ID_ENV_VAR,
     authority_fixtures.INSTALLATION_ID.upper(), 'not canonical'),
    (retirement.COHORT_SUFFIXES_ENV_VAR, '["p2a-v1", "p2a-v2"]',
     'canonical JSON array'),
    (retirement.COHORT_SUFFIXES_ENV_VAR, '["p2a-v2","p2a-v1"]',
     'sorted unique'),
    (retirement.COHORT_SUFFIXES_ENV_VAR, '["p2a-v1","p2a-v1"]',
     'sorted unique'),
    (retirement.RETIREMENT_TOMBSTONES_ENV_VAR, '["p2a-v1"]',
     'inventories overlap'),
])
def test_scope_from_environment_rejects_noncanonical_values(
        key: str, value: str, match: str) -> None:
    environ = {
        retirement.INSTALLATION_ID_ENV_VAR: authority_fixtures.INSTALLATION_ID,
        'SKYPILOT_POD_NAMESPACE': authority_fixtures.NAMESPACE,
        'SKYPILOT_RELEASE_NAME': authority_fixtures.HELM_FULL_NAME,
        retirement.COHORT_SUFFIXES_ENV_VAR: '["p2a-v1"]',
        retirement.RETIREMENT_TOMBSTONES_ENV_VAR: '[]',
    }
    environ[key] = value

    with pytest.raises(ValueError, match=match):
        retirement.AuthorityWorkerRetirementScope.from_environment(environ)


def test_scope_from_environment_requires_complete_nonempty_scope() -> None:
    environ = {
        retirement.INSTALLATION_ID_ENV_VAR: authority_fixtures.INSTALLATION_ID,
        'SKYPILOT_POD_NAMESPACE': authority_fixtures.NAMESPACE,
        'SKYPILOT_RELEASE_NAME': authority_fixtures.HELM_FULL_NAME,
        retirement.COHORT_SUFFIXES_ENV_VAR: '[]',
        retirement.RETIREMENT_TOMBSTONES_ENV_VAR: '[]',
    }

    with pytest.raises(ValueError, match='has no cohort names'):
        retirement.AuthorityWorkerRetirementScope.from_environment(environ)

    del environ['SKYPILOT_RELEASE_NAME']
    with pytest.raises(ValueError, match='environment is incomplete'):
        retirement.AuthorityWorkerRetirementScope.from_environment(environ)


def test_live_registering_cohort_is_authorized_without_kubernetes_get() -> None:
    initial = _record(actions.WorkerCohortLifecycleState.REGISTERING)
    store = _store(initial)
    store.authorize_stale_worker_cohort_removal.return_value = _transition(
        initial, actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED)
    observer = mock.Mock()
    verifier = retirement.AuthorityWorkerRetirementVerifier(
        _scope(), store, observer)

    result = verifier.run_once()

    assert result == retirement.AuthorityWorkerRetirementPass(scanned=1,
                                                              authorized=1,
                                                              retired=0)
    store.list_worker_cohorts_for_installation.assert_called_once_with(
        authority_fixtures.INSTALLATION_ID, retirement._SCAN_STATES)  # pylint: disable=protected-access
    store.authorize_stale_worker_cohort_removal.assert_called_once_with(
        initial.cohort_identity, initial.revision,
        initial.registration_attestations)
    store.retire_worker_cohort.assert_not_called()
    observer.exact_not_found_pair.assert_not_called()


def test_tombstone_exact_404_pair_commits_retirement() -> None:
    initial = _record(actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED)
    store = _store(initial)
    store.retire_worker_cohort.return_value = _transition(
        initial, actions.WorkerCohortLifecycleState.RETIRED)
    core_api = mock.Mock()
    apps_api = mock.Mock()
    apps_api.read_namespaced_deployment.side_effect = _ApiError(404)
    core_api.read_namespaced_service_account.side_effect = _ApiError(404)
    verifier = retirement.AuthorityWorkerRetirementVerifier(
        _scope(live=(), tombstones=(authority_fixtures.COHORT_SUFFIX,)), store,
        _observer(core_api, apps_api))

    result = verifier.run_once()

    assert result == retirement.AuthorityWorkerRetirementPass(scanned=1,
                                                              authorized=0,
                                                              retired=1)
    apps_api.read_namespaced_deployment.assert_called_once_with(
        authority_fixtures.DEPLOYMENT_NAME, authority_fixtures.NAMESPACE)
    core_api.read_namespaced_service_account.assert_called_once_with(
        authority_fixtures.DEPLOYMENT_NAME, authority_fixtures.NAMESPACE)
    store.retire_worker_cohort.assert_called_once_with(
        initial.cohort_identity,
        initial.revision,
        initial.registration_attestations,
        deployment_not_found=True,
        service_account_not_found=True)


def test_tombstone_403_fails_closed_and_later_record_continues() -> None:
    initial = _record(actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED)
    store = _store(initial, initial)
    store.retire_worker_cohort.return_value = _transition(
        initial, actions.WorkerCohortLifecycleState.RETIRED)
    core_api = mock.Mock()
    apps_api = mock.Mock()
    apps_api.read_namespaced_deployment.side_effect = [
        _ApiError(403),
        _ApiError(404),
    ]
    core_api.read_namespaced_service_account.side_effect = _ApiError(404)
    verifier = retirement.AuthorityWorkerRetirementVerifier(
        _scope(live=(), tombstones=(authority_fixtures.COHORT_SUFFIX,)), store,
        _observer(core_api, apps_api))

    result = verifier.run_once()

    assert result == retirement.AuthorityWorkerRetirementPass(scanned=2,
                                                              authorized=0,
                                                              retired=1)
    assert apps_api.read_namespaced_deployment.call_count == 2
    core_api.read_namespaced_service_account.assert_called_once_with(
        authority_fixtures.DEPLOYMENT_NAME, authority_fixtures.NAMESPACE)
    store.retire_worker_cohort.assert_called_once()


def test_tombstone_identity_mismatch_fails_closed_and_later_continues() -> None:
    initial = _record(actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED)
    store = _store(initial, initial)
    store.retire_worker_cohort.return_value = _transition(
        initial, actions.WorkerCohortLifecycleState.RETIRED)
    core_api = mock.Mock()
    apps_api = mock.Mock()
    wrong_deployment = {
        'api_version': 'apps/v1',
        'kind': 'Deployment',
        'metadata': {
            'namespace': authority_fixtures.NAMESPACE,
            'name': authority_fixtures.DEPLOYMENT_NAME,
            'uid': 'replacement-deployment-uid',
        },
    }
    apps_api.read_namespaced_deployment.side_effect = [
        wrong_deployment,
        _ApiError(404),
    ]
    core_api.read_namespaced_service_account.side_effect = _ApiError(404)
    verifier = retirement.AuthorityWorkerRetirementVerifier(
        _scope(live=(), tombstones=(authority_fixtures.COHORT_SUFFIX,)), store,
        _observer(core_api, apps_api))

    result = verifier.run_once()

    assert result == retirement.AuthorityWorkerRetirementPass(scanned=2,
                                                              authorized=0,
                                                              retired=1)
    assert apps_api.read_namespaced_deployment.call_count == 2
    core_api.read_namespaced_service_account.assert_called_once()
    store.retire_worker_cohort.assert_called_once()


def test_registering_tombstone_fails_closed_and_later_record_continues(
) -> None:
    registering = _record(actions.WorkerCohortLifecycleState.REGISTERING)
    authorized = _record(actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED,
                         revision=2)
    store = _store(registering, authorized)
    store.retire_worker_cohort.return_value = _transition(
        authorized, actions.WorkerCohortLifecycleState.RETIRED)
    core_api = mock.Mock()
    apps_api = mock.Mock()
    apps_api.read_namespaced_deployment.side_effect = _ApiError(404)
    core_api.read_namespaced_service_account.side_effect = _ApiError(404)
    verifier = retirement.AuthorityWorkerRetirementVerifier(
        _scope(live=(), tombstones=(authority_fixtures.COHORT_SUFFIX,)), store,
        _observer(core_api, apps_api))

    result = verifier.run_once()

    assert result == retirement.AuthorityWorkerRetirementPass(scanned=2,
                                                              authorized=0,
                                                              retired=1)
    store.authorize_stale_worker_cohort_removal.assert_not_called()
    store.retire_worker_cohort.assert_called_once()


def test_live_removal_authorized_cohort_never_reads_kubernetes() -> None:
    initial = _record(actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED)
    store = _store(initial)
    observer = mock.Mock()
    verifier = retirement.AuthorityWorkerRetirementVerifier(
        _scope(), store, observer)

    result = verifier.run_once()

    assert result == retirement.AuthorityWorkerRetirementPass(scanned=1,
                                                              authorized=0,
                                                              retired=0)
    store.authorize_stale_worker_cohort_removal.assert_not_called()
    store.retire_worker_cohort.assert_not_called()
    observer.exact_not_found_pair.assert_not_called()


def test_row_absent_from_release_inventory_is_untouched() -> None:
    initial = _record(actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED)
    store = _store(initial)
    observer = mock.Mock()
    verifier = retirement.AuthorityWorkerRetirementVerifier(
        _scope(live=('other-v1',)), store, observer)

    result = verifier.run_once()

    assert result == retirement.AuthorityWorkerRetirementPass(scanned=1,
                                                              authorized=0,
                                                              retired=0)
    store.authorize_stale_worker_cohort_removal.assert_not_called()
    store.retire_worker_cohort.assert_not_called()
    observer.exact_not_found_pair.assert_not_called()
