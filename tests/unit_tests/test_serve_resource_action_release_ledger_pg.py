"""Real-PostgreSQL tests for the revision-034 authority release ledger."""
# pylint: disable=redefined-outer-name,protected-access,unused-import

import concurrent.futures
import copy
import threading

import pytest
import serve_resource_action_test_fixtures as authority_fixtures
import sqlalchemy
from test_serve_resource_action_serve033_store_pg import postgres_engine
import test_serve_resource_action_serve033_store_pg as store_fixtures

from sky.serve import resource_action_state
from sky.serve import resource_action_state_schema
from sky.serve import resource_actions as actions
from sky.serve import serve_state_schema
from sky.server.requests import postgres as request_postgres
from sky.server.requests import resource_actions as kernel_actions


@pytest.fixture
def release_store(postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    serve_state_schema.Base.metadata.create_all(postgres_engine)
    resource_action_state_schema.RESOURCE_ACTION_STATE_METADATA.create_all(
        postgres_engine)
    resource_action_state_schema.RESOURCE_ACTION_AUTHORITY_RELEASE_METADATA.create_all(
        postgres_engine)
    request_postgres._METADATA.create_all(postgres_engine)
    return (postgres_engine,
            resource_action_state.PostgresServeResourceActionStateStore(
                postgres_engine))


def _manifest() -> actions.ProviderAuthorityWorkerCohortManifestV1:
    return actions.ProviderAuthorityWorkerCohortManifestV1.from_value(
        authority_fixtures.authority_manifest_value())


def _preflight_live(
    store: resource_action_state.PostgresServeResourceActionStateStore,
) -> resource_action_state.AuthorityReleaseRecord:
    record = store.preflight_authority_release(
        authority_fixtures.NAMESPACE, authority_fixtures.HELM_FULL_NAME,
        authority_fixtures.HELM_FULL_NAME, authority_fixtures.INSTALLATION_ID,
        True, (_manifest(),), ())
    assert record is not None
    return record


def _cohort_registration(engine):
    cohort, registrations = store_fixtures._cohort_and_registrations(
        engine, 'pod-a')
    return cohort, registrations


def _set_cohort_state(engine,
                      state: actions.WorkerCohortLifecycleState) -> None:
    table = resource_action_state_schema.WORKER_COHORTS
    values = {
        'lifecycle_state': state.value,
        'revision': table.c.revision + 1,
        'state_changed_at': sqlalchemy.func.clock_timestamp(),
        'retired_at': None,
    }
    if state is actions.WorkerCohortLifecycleState.RETIRED:
        now = store_fixtures._database_now(engine)
        values['state_changed_at'] = now
        values['retired_at'] = now
    with engine.begin() as connection:
        updated = connection.execute(
            sqlalchemy.update(table).where(
                table.c.cohort_id == authority_fixtures.COHORT_ID).values(
                    **values))
        assert updated.rowcount == 1


def test_release_identity_and_canonical_inventories_are_durable(
        release_store) -> None:
    engine, store = release_store
    with pytest.raises(ValueError, match='at least one live or tombstone'):
        store.preflight_authority_release(authority_fixtures.NAMESPACE,
                                          authority_fixtures.HELM_FULL_NAME,
                                          authority_fixtures.HELM_FULL_NAME,
                                          authority_fixtures.INSTALLATION_ID,
                                          True, (), ())
    first = _preflight_live(store)
    adopted = _preflight_live(store)
    assert adopted.revision == first.revision
    assert adopted.installation_id == authority_fixtures.INSTALLATION_ID
    assert adopted.live_manifests[0].canonical_bytes == _manifest(
    ).canonical_bytes

    releases = resource_action_state_schema.AUTHORITY_RELEASES
    cohorts = resource_action_state_schema.AUTHORITY_RELEASE_COHORTS
    with engine.connect() as connection:
        release_row = connection.execute(
            sqlalchemy.select(releases)).mappings().one()
        cohort_row = connection.execute(
            sqlalchemy.select(cohorts)).mappings().one()
    assert release_row['live_inventory_sha256'] == actions.canonical_sha256(
        [_manifest().canonical_value()])
    assert release_row[
        'tombstone_inventory_sha256'] == actions.canonical_sha256([])
    assert cohort_row['manifest_sha256'] == _manifest().sha256

    with pytest.raises(kernel_actions.ActionConflict,
                       match='full name is immutable'):
        store.preflight_authority_release(authority_fixtures.NAMESPACE,
                                          authority_fixtures.HELM_FULL_NAME,
                                          'changed-name',
                                          authority_fixtures.INSTALLATION_ID,
                                          True, (), ('p2a-v1',))

    disabled = store.preflight_authority_release(
        authority_fixtures.NAMESPACE, authority_fixtures.HELM_FULL_NAME,
        authority_fixtures.HELM_FULL_NAME, '', False, (), ())
    assert disabled is not None and not disabled.enabled
    with pytest.raises(kernel_actions.ActionConflict,
                       match='already bound to another Helm release'):
        store.preflight_authority_release(authority_fixtures.NAMESPACE,
                                          'another-release',
                                          'another-full-name',
                                          authority_fixtures.INSTALLATION_ID,
                                          True, (), ('p2a-v1',))


def test_suffix_manifest_binding_is_permanent(release_store) -> None:
    _, store = release_store
    _preflight_live(store)
    store.preflight_authority_release(authority_fixtures.NAMESPACE,
                                      authority_fixtures.HELM_FULL_NAME,
                                      authority_fixtures.HELM_FULL_NAME, '',
                                      False, (), ())
    changed_value = copy.deepcopy(authority_fixtures.authority_manifest_value())
    changed_value['image']['qualification_artifact']['byte_size'] = 18
    changed_value['image']['qualification_artifact']['sha256'] = '4' * 64
    changed = actions.ProviderAuthorityWorkerCohortManifestV1.from_value(
        changed_value)
    with pytest.raises(kernel_actions.ActionConflict,
                       match='different immutable manifest bytes'):
        store.preflight_authority_release(authority_fixtures.NAMESPACE,
                                          authority_fixtures.HELM_FULL_NAME,
                                          authority_fixtures.HELM_FULL_NAME,
                                          authority_fixtures.INSTALLATION_ID,
                                          True, (changed,), ())


def test_lifecycle_inventory_matrix_and_disabled_gate(release_store) -> None:
    engine, store = release_store
    _preflight_live(store)
    cohort, registration = _cohort_registration(engine)
    store.register_worker_cohort(cohort, registration)

    with pytest.raises(kernel_actions.ActionConflict,
                       match='live authority cohort'):
        store.preflight_authority_release(authority_fixtures.NAMESPACE,
                                          authority_fixtures.HELM_FULL_NAME,
                                          authority_fixtures.HELM_FULL_NAME,
                                          authority_fixtures.INSTALLATION_ID,
                                          True, (),
                                          (authority_fixtures.COHORT_SUFFIX,))

    _set_cohort_state(engine,
                      actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED)
    # REMOVAL_AUTHORIZED deliberately supports both upgrades: it may remain a
    # live object first, then become a tombstone in the next Helm release.
    _preflight_live(store)
    tombstoned = store.preflight_authority_release(
        authority_fixtures.NAMESPACE, authority_fixtures.HELM_FULL_NAME,
        authority_fixtures.HELM_FULL_NAME, authority_fixtures.INSTALLATION_ID,
        True, (), (authority_fixtures.COHORT_SUFFIX,))
    assert tombstoned is not None
    assert tombstoned.tombstone_suffixes == (authority_fixtures.COHORT_SUFFIX,)

    with pytest.raises(kernel_actions.ActionConflict,
                       match='cannot be disabled'):
        store.preflight_authority_release(authority_fixtures.NAMESPACE,
                                          authority_fixtures.HELM_FULL_NAME,
                                          authority_fixtures.HELM_FULL_NAME, '',
                                          False, (), ())

    _set_cohort_state(engine, actions.WorkerCohortLifecycleState.RETIRED)
    with pytest.raises(kernel_actions.ActionConflict,
                       match='cannot become live again'):
        _preflight_live(store)
    store.preflight_authority_release(authority_fixtures.NAMESPACE,
                                      authority_fixtures.HELM_FULL_NAME,
                                      authority_fixtures.HELM_FULL_NAME,
                                      authority_fixtures.INSTALLATION_ID, True,
                                      (), (authority_fixtures.COHORT_SUFFIX,))
    disabled = store.preflight_authority_release(
        authority_fixtures.NAMESPACE, authority_fixtures.HELM_FULL_NAME,
        authority_fixtures.HELM_FULL_NAME, '', False, (), ())
    assert disabled is not None and not disabled.enabled


def test_retirement_rechecks_current_tombstone_fence(release_store) -> None:
    engine, store = release_store
    _preflight_live(store)
    cohort, registration = _cohort_registration(engine)
    store.register_worker_cohort(cohort, registration)
    _set_cohort_state(engine,
                      actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED)
    tombstoned = store.preflight_authority_release(
        authority_fixtures.NAMESPACE, authority_fixtures.HELM_FULL_NAME,
        authority_fixtures.HELM_FULL_NAME, authority_fixtures.INSTALLATION_ID,
        True, (), (authority_fixtures.COHORT_SUFFIX,))
    assert tombstoned is not None
    authorized = store.get_worker_cohort(cohort.cohort_id)
    assert authorized is not None

    # If rollback wins the release-row lock, a stale 404 observation cannot
    # retire the now-live cohort.
    _preflight_live(store)
    with pytest.raises(kernel_actions.ActionConflict,
                       match='no longer in the current exact tombstone'):
        store.retire_worker_cohort(cohort,
                                   authorized.revision,
                                   authorized.registration_attestations,
                                   deployment_not_found=True,
                                   service_account_not_found=True)
    assert store.get_worker_cohort(cohort.cohort_id).lifecycle_state is (
        actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED)

    # If retirement wins, the later rollback sees RETIRED and cannot recreate
    # that permanently bound suffix as live.
    store.preflight_authority_release(authority_fixtures.NAMESPACE,
                                      authority_fixtures.HELM_FULL_NAME,
                                      authority_fixtures.HELM_FULL_NAME,
                                      authority_fixtures.INSTALLATION_ID, True,
                                      (), (authority_fixtures.COHORT_SUFFIX,))
    retired = store.retire_worker_cohort(cohort,
                                         authorized.revision,
                                         authorized.registration_attestations,
                                         deployment_not_found=True,
                                         service_account_not_found=True)
    assert retired.record.lifecycle_state is (
        actions.WorkerCohortLifecycleState.RETIRED)
    with pytest.raises(kernel_actions.ActionConflict,
                       match='cannot become live again'):
        _preflight_live(store)


def test_preflight_bounds_and_registration_fence(release_store) -> None:
    engine, store = release_store
    no_anchor = store.preflight_authority_release('empty-namespace',
                                                  'empty-release',
                                                  'empty-full-name', '', False,
                                                  (), ())
    assert no_anchor is None
    tombstones = tuple(f'cohort-{index:03d}' for index in range(257))
    with pytest.raises(ValueError, match='exceeds 256'):
        store.preflight_authority_release(authority_fixtures.NAMESPACE,
                                          authority_fixtures.HELM_FULL_NAME,
                                          authority_fixtures.HELM_FULL_NAME,
                                          authority_fixtures.INSTALLATION_ID,
                                          True, (), tombstones)

    cohort, registration = _cohort_registration(engine)
    with pytest.raises(kernel_actions.ActionConflict,
                       match='no durable Helm release binding'):
        store.register_worker_cohort(cohort, registration)
    _preflight_live(store)
    store.preflight_authority_release(authority_fixtures.NAMESPACE,
                                      authority_fixtures.HELM_FULL_NAME,
                                      authority_fixtures.HELM_FULL_NAME, '',
                                      False, (), ())
    with pytest.raises(kernel_actions.ActionConflict,
                       match='no longer permits'):
        store.register_worker_cohort(cohort, registration)


def test_registration_waits_for_and_honors_proposed_inventory(
        release_store) -> None:
    engine, store = release_store
    _preflight_live(store)
    cohort, registration = _cohort_registration(engine)
    preflight_has_release_lock = threading.Event()
    allow_preflight_to_commit = threading.Event()

    class PausingPreflightStore(
            resource_action_state.PostgresServeResourceActionStateStore):

        def _locked_release_worker_rows(self, *args, **kwargs):
            preflight_has_release_lock.set()
            assert allow_preflight_to_commit.wait(timeout=10)
            return super()._locked_release_worker_rows(*args, **kwargs)

    preflight_store = PausingPreflightStore(engine)

    def disable_release():
        return preflight_store.preflight_authority_release(
            authority_fixtures.NAMESPACE, authority_fixtures.HELM_FULL_NAME,
            authority_fixtures.HELM_FULL_NAME, '', False, (), ())

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        disable_future = executor.submit(disable_release)
        assert preflight_has_release_lock.wait(timeout=10)
        registration_future = executor.submit(store.register_worker_cohort,
                                              cohort, registration)
        # The registration must remain behind the release-row lock until the
        # proposed disabled inventory commits.
        with pytest.raises(concurrent.futures.TimeoutError):
            registration_future.result(timeout=0.2)
        allow_preflight_to_commit.set()
        disabled = disable_future.result(timeout=10)
        assert disabled is not None and not disabled.enabled
        with pytest.raises(kernel_actions.ActionConflict,
                           match='no longer permits'):
            registration_future.result(timeout=10)


def test_release_ledger_metadata_has_exact_keys_and_uniqueness() -> None:
    releases = resource_action_state_schema.AUTHORITY_RELEASES
    cohorts = resource_action_state_schema.AUTHORITY_RELEASE_COHORTS
    assert tuple(column.name for column in releases.primary_key.columns) == (
        'namespace', 'helm_release_name')
    assert tuple(column.name for column in cohorts.primary_key.columns) == (
        'namespace', 'helm_release_name', 'cohort_suffix')
    release_uniques = {
        tuple(column.name
              for column in constraint.columns)
        for constraint in releases.constraints
        if isinstance(constraint, sqlalchemy.UniqueConstraint)
    }
    cohort_uniques = {
        tuple(column.name
              for column in constraint.columns)
        for constraint in cohorts.constraints
        if isinstance(constraint, sqlalchemy.UniqueConstraint)
    }
    assert release_uniques == {('installation_id',)}
    assert cohort_uniques == {('cohort_id',)}


def test_release_ledger_database_rejects_enabled_empty_inventory(
        release_store) -> None:
    engine, store = release_store
    _preflight_live(store)
    releases = resource_action_state_schema.AUTHORITY_RELEASES
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(releases).values(
                    live_manifests=[],
                    live_inventory_sha256=actions.canonical_sha256([]),
                    tombstone_suffixes=[],
                    tombstone_inventory_sha256=actions.canonical_sha256([])))


def test_release_preflight_rejects_suppressed_update_readback(
        release_store) -> None:
    engine, store = release_store
    first = _preflight_live(store)
    releases = resource_action_state_schema.AUTHORITY_RELEASES
    with engine.begin() as connection:
        connection.exec_driver_sql(
            'CREATE FUNCTION suppress_release_update() RETURNS trigger '
            "LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'")
        connection.exec_driver_sql(
            f'CREATE TRIGGER suppress_release_update BEFORE UPDATE ON '
            f'{releases.name} FOR EACH ROW EXECUTE FUNCTION '
            'suppress_release_update()')

    with pytest.raises(kernel_actions.InvariantViolation,
                       match='did not commit the exact proposed fence'):
        store.preflight_authority_release(authority_fixtures.NAMESPACE,
                                          authority_fixtures.HELM_FULL_NAME,
                                          authority_fixtures.HELM_FULL_NAME, '',
                                          False, (), ())

    with engine.connect() as connection:
        row = connection.execute(sqlalchemy.select(releases)).mappings().one()
    assert row['enabled']
    assert row['revision'] == first.revision
