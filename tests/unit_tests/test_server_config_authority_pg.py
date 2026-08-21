"""Real-PostgreSQL contracts for guarded-HA server config authority."""
# pylint: disable=protected-access,redefined-outer-name

import concurrent.futures
import os
import threading

import casbin
import pytest
import sqlalchemy
import sqlalchemy_adapter
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky import skypilot_config
from sky.skylet import constants
from sky.users import permission as permission_lib
from sky.utils import controller_constants
from sky.utils import yaml_utils
from sky.utils.db import migration_utils


@pytest.fixture
def config_store(postgres_engine, monkeypatch, tmp_path):
    """Install the current config/Casbin schema in an isolated PG database."""
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    skypilot_config.Base.metadata.create_all(postgres_engine)
    sqlalchemy_adapter.Base.metadata.create_all(postgres_engine)

    monkeypatch.setenv(skypilot_config.ENV_VAR_SERVER_CONFIG_MODE,
                       skypilot_config.SERVER_CONFIG_MODE_POSTGRES)
    monkeypatch.setenv(
        constants.ENV_VAR_DB_CONNECTION_URI,
        postgres_engine.url.render_as_string(hide_password=False))
    monkeypatch.setenv(constants.ENV_VAR_IS_SKYPILOT_SERVER, 'true')
    monkeypatch.setenv(constants.ENV_VAR_STATE_DB_MIGRATION_MODE, 'verify')
    monkeypatch.setattr(skypilot_config._db_manager, '_engine', postgres_engine)
    monkeypatch.setattr(skypilot_config, '_global_config_context',
                        skypilot_config.ConfigContext())
    monkeypatch.setattr(skypilot_config, '_CONFIG_UPDATE_HOOKS', [])
    monkeypatch.setattr(skypilot_config, '_CENTRAL_CONFIG_RELOAD_LOCK_PATH',
                        str(tmp_path / 'central-config.lock'))
    return postgres_engine


def _seed_authority(engine,
                    config: dict,
                    *,
                    revision: int = 1) -> (skypilot_config.ServerConfigRecord):
    value = yaml_utils.dump_yaml_str(config)
    identity = skypilot_config.ServerConfigIdentity(
        revision=revision,
        digest=skypilot_config._config_value_digest(value),
    )
    receipt_value = skypilot_config._permission_generation_value(0, identity)
    with engine.begin() as connection:
        connection.execute(sqlalchemy.insert(
            skypilot_config.config_yaml_table), [{
                'key': skypilot_config.API_SERVER_CONFIG_KEY,
                'value': value,
                'revision': identity.revision,
                'digest': identity.digest,
            }, {
                'key': skypilot_config.WORKSPACE_PERMISSION_GENERATION_KEY,
                'value': receipt_value,
                'revision': 1,
                'digest': skypilot_config._config_value_digest(receipt_value),
            }])
    skypilot_config._reload_config_as_server()
    record = skypilot_config._get_server_config_record_from_db()
    assert record is not None
    return record


def _read_workspace_rules(engine) -> set[tuple[str, str, str]]:
    rule = sqlalchemy_adapter.CasbinRule.__table__
    with engine.connect() as connection:
        return {(str(row.v0), str(row.v1), str(row.v2))
                for row in connection.execute(
                    sqlalchemy.select(rule.c.v0, rule.c.v1, rule.c.v2).where(
                        rule.c.ptype == 'p', rule.c.v2 == '*',
                        rule.c.v1.not_like('/%')))}


def _permission_service_with_postgres_enforcer(engine):
    adapter = sqlalchemy_adapter.Adapter(engine,
                                         db_class=sqlalchemy_adapter.CasbinRule)
    model_path = os.path.join(os.path.dirname(permission_lib.__file__),
                              'model.conf')
    service = permission_lib.PermissionService()
    service.enforcer = casbin.SyncedEnforcer(model_path, adapter)
    return service


def _set_label(config: dict, label: str) -> None:
    config.setdefault('aws', {}).setdefault('labels', {})[label] = 'true'


def test_two_writers_retry_without_lost_update(config_store, monkeypatch):
    initial = _seed_authority(config_store, {})
    monkeypatch.setattr(skypilot_config, '_reload_central_config_after_commit',
                        lambda: None)
    barrier = threading.Barrier(2)

    def writer(label: str) -> None:
        expected = initial.identity
        barrier.wait()
        while True:
            try:
                skypilot_config.mutate_postgres_server_config(
                    lambda config: _set_label(config, label),
                    expected_identity=expected)
                return
            except skypilot_config.StaleServerConfigError:
                current = skypilot_config._get_server_config_record_from_db()
                assert current is not None
                expected = current.identity

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(writer, ('writer-a', 'writer-b')))

    current = skypilot_config._get_server_config_record_from_db()
    assert current is not None
    assert current.identity.revision == initial.identity.revision + 2
    assert current.config['aws']['labels'] == {
        'writer-a': 'true',
        'writer-b': 'true',
    }


def test_stale_cas_performs_zero_dml(config_store, monkeypatch):
    initial = _seed_authority(config_store, {})
    monkeypatch.setattr(skypilot_config, '_reload_central_config_after_commit',
                        lambda: None)
    dml: list[str] = []

    def record_dml(_conn, _cursor, statement, _parameters, _context,
                   _executemany):
        verb = statement.lstrip().split(None, 1)[0].upper()
        if verb in ('INSERT', 'UPDATE', 'DELETE'):
            dml.append(verb)

    sqlalchemy.event.listen(config_store, 'before_cursor_execute', record_dml)
    try:
        stale = skypilot_config.ServerConfigIdentity(
            revision=initial.identity.revision + 1,
            digest=initial.identity.digest)
        with pytest.raises(skypilot_config.StaleServerConfigError):
            skypilot_config.mutate_postgres_server_config(
                lambda config: _set_label(config, 'must-not-write'),
                expected_identity=stale)
    finally:
        sqlalchemy.event.remove(config_store, 'before_cursor_execute',
                                record_dml)
    assert dml == []
    assert skypilot_config._get_server_config_record_from_db() == initial


def test_joined_repository_failure_rolls_back_every_write(
        config_store, monkeypatch):
    initial = _seed_authority(config_store, {})
    with config_store.begin() as connection:
        connection.exec_driver_sql(
            'CREATE TABLE joined_write (name TEXT PRIMARY KEY)')
    postcommit = []
    monkeypatch.setattr(skypilot_config, '_reload_central_config_after_commit',
                        lambda: postcommit.append(True))

    def fail_hook(session, _current, _next):
        session.execute(
            sqlalchemy.text(
                "INSERT INTO joined_write(name) VALUES ('rolled-back')"))
        raise RuntimeError('fail before commit')

    with pytest.raises(RuntimeError, match='fail before commit'):
        skypilot_config.mutate_postgres_server_config(
            lambda config: _set_label(config, 'rolled-back'),
            expected_identity=initial.identity,
            transaction_hook=fail_hook)

    assert skypilot_config._get_server_config_record_from_db() == initial
    with config_store.connect() as connection:
        assert connection.execute(
            sqlalchemy.text('SELECT name FROM joined_write')).all() == []
    assert postcommit == []


def test_committed_config_recovers_after_postcommit_reload_failure(
        config_store, monkeypatch):
    initial = _seed_authority(config_store, {})
    reload_after_commit = skypilot_config._reload_central_config_after_commit

    def fail_reload():
        raise RuntimeError('fail after commit before reload')

    monkeypatch.setattr(skypilot_config, '_reload_central_config_after_commit',
                        fail_reload)
    with pytest.raises(RuntimeError, match='fail after commit before reload'):
        skypilot_config.mutate_postgres_server_config(
            lambda config: _set_label(config, 'committed'),
            expected_identity=initial.identity)

    committed = skypilot_config._get_server_config_record_from_db()
    assert committed is not None
    assert committed.identity.revision == initial.identity.revision + 1
    assert committed.config['aws']['labels'] == {'committed': 'true'}
    assert (
        skypilot_config.get_loaded_server_config_identity() == initial.identity)

    # A subsequent server reload recovers solely from the committed PG row;
    # no filesystem acknowledgement or repair write is required.
    monkeypatch.setattr(skypilot_config, '_reload_central_config_after_commit',
                        reload_after_commit)
    skypilot_config._reload_config_as_server()
    assert skypilot_config.get_loaded_server_config_identity() == (
        committed.identity)
    assert skypilot_config.get_nested(('aws', 'labels'), None) == {
        'committed': 'true'
    }


def test_committed_workspace_state_recovers_after_postcommit_reload_failure(
        config_store, monkeypatch):
    initial = _seed_authority(config_store, {'workspaces': {'default': {}}})
    service = _permission_service_with_postgres_enforcer(config_store)
    service._observed_workspace_permission_generation = 0
    reload_after_commit = skypilot_config._reload_central_config_after_commit

    def fail_reload():
        raise RuntimeError('workspace fail after commit before reload')

    monkeypatch.setattr(skypilot_config, '_reload_central_config_after_commit',
                        fail_reload)

    def policy_hook(session, _current, next_record):
        return service.replace_workspace_policies_in_session(
            session, {'team-a': ['user-a']}, next_record.identity)

    with pytest.raises(RuntimeError,
                       match='workspace fail after commit before reload'):
        skypilot_config.mutate_postgres_server_config(
            lambda config: config.setdefault('workspaces', {}).update(
                {'team-a': {
                    'private': True,
                    'allowed_users': ['user-a'],
                }}),
            expected_identity=initial.identity,
            transaction_hook=policy_hook)

    committed = skypilot_config._get_server_config_record_from_db()
    assert committed is not None
    assert committed.config['workspaces']['team-a']['allowed_users'] == [
        'user-a'
    ]
    assert _read_workspace_rules(config_store) == {('user-a', 'team-a', '*')}
    receipt = skypilot_config.get_workspace_permission_generation()
    assert receipt.generation == 1
    assert receipt.config_identity == committed.identity
    assert (
        skypilot_config.get_loaded_server_config_identity() == initial.identity)
    assert service.enforcer is not None
    assert not service.enforcer.enforce('user-a', 'team-a', '*')

    # The next authorization read reloads both committed authorities before it
    # decides, despite the writer dying between commit and local reload.
    monkeypatch.setattr(skypilot_config, '_reload_central_config_after_commit',
                        reload_after_commit)
    skypilot_config._reload_config_as_server()
    assert service._ensure_workspace_permission_generation_current() == 1
    assert service.enforcer.enforce('user-a', 'team-a', '*')


def test_workspace_config_policy_and_generation_commit_atomically(
        config_store, monkeypatch):
    initial = _seed_authority(config_store, {'workspaces': {'default': {}}})
    service = permission_lib.PermissionService()
    monkeypatch.setattr(skypilot_config, '_reload_central_config_after_commit',
                        lambda: None)

    def modify(config):
        config.setdefault('workspaces', {})['team-a'] = {
            'private': True,
            'allowed_users': ['user-a'],
        }

    def policy_hook(session, _current, next_record):
        return service.replace_workspace_policies_in_session(
            session, {'team-a': ['user-a']}, next_record.identity)

    committed, generation = skypilot_config.mutate_postgres_server_config(
        modify,
        expected_identity=initial.identity,
        transaction_hook=policy_hook)

    assert generation == 1
    assert committed.config['workspaces']['team-a']['allowed_users'] == [
        'user-a'
    ]
    assert _read_workspace_rules(config_store) == {('user-a', 'team-a', '*')}
    receipt = skypilot_config.get_workspace_permission_generation()
    assert receipt.generation == 1
    assert receipt.config_identity == committed.identity


def test_workspace_hook_failure_rolls_back_config_policy_and_generation(
        config_store, monkeypatch):
    initial = _seed_authority(config_store, {'workspaces': {'default': {}}})
    service = permission_lib.PermissionService()
    monkeypatch.setattr(skypilot_config, '_reload_central_config_after_commit',
                        lambda: None)

    def policy_hook(session, _current, next_record):
        service.replace_workspace_policies_in_session(session,
                                                      {'team-a': ['user-a']},
                                                      next_record.identity)
        raise RuntimeError('policy failpoint')

    with pytest.raises(RuntimeError, match='policy failpoint'):
        skypilot_config.mutate_postgres_server_config(
            lambda config: config.setdefault('workspaces', {}).update(
                {'team-a': {
                    'private': True,
                    'allowed_users': ['user-a'],
                }}),
            expected_identity=initial.identity,
            transaction_hook=policy_hook)

    assert skypilot_config._get_server_config_record_from_db() == initial
    assert _read_workspace_rules(config_store) == set()
    assert skypilot_config.get_workspace_permission_generation().generation == 0


def test_concurrent_workspace_writers_preserve_config_and_rules(
        config_store, monkeypatch):
    initial = _seed_authority(config_store, {'workspaces': {'default': {}}})
    monkeypatch.setattr(skypilot_config, '_reload_central_config_after_commit',
                        lambda: None)
    barrier = threading.Barrier(2)

    def writer(name: str) -> None:
        expected = initial.identity
        service = permission_lib.PermissionService()
        barrier.wait()
        while True:
            try:
                skypilot_config.mutate_postgres_server_config(
                    lambda config: config.setdefault('workspaces', {}).update({
                        name: {
                            'private': True,
                            'allowed_users': [f'user-{name}'],
                        }
                    }),
                    expected_identity=expected,
                    transaction_hook=lambda session, _current, next_record:
                    service.replace_workspace_policies_in_session(
                        session, {name: [f'user-{name}']}, next_record.identity
                    ))
                return
            except skypilot_config.StaleServerConfigError:
                current = skypilot_config._get_server_config_record_from_db()
                assert current is not None
                expected = current.identity

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(writer, ('team-a', 'team-b')))

    current = skypilot_config._get_server_config_record_from_db()
    assert current is not None
    assert set(current.config['workspaces']) == {'default', 'team-a', 'team-b'}
    assert _read_workspace_rules(config_store) == {
        ('user-team-a', 'team-a', '*'),
        ('user-team-b', 'team-b', '*'),
    }
    receipt = skypilot_config.get_workspace_permission_generation()
    assert receipt.generation == 2
    assert receipt.config_identity == current.identity


def test_legacy_role_update_preserves_and_refreshes_workspace_rules(
        config_store, monkeypatch, tmp_path):
    initial = _seed_authority(config_store, {'workspaces': {'default': {}}})
    repository = permission_lib.PermissionService()
    monkeypatch.setattr(skypilot_config, '_reload_central_config_after_commit',
                        lambda: None)
    committed, generation = skypilot_config.mutate_postgres_server_config(
        lambda config: config.setdefault('workspaces', {}).update(
            {'team-a': {
                'private': True,
                'allowed_users': ['user-a'],
            }}),
        expected_identity=initial.identity,
        transaction_hook=lambda session, _current, next_record:
        repository.replace_workspace_policies_in_session(
            session, {'team-a': ['user-a']}, next_record.identity))
    assert generation == 1
    skypilot_config._reload_config_as_server()

    writer = _permission_service_with_postgres_enforcer(config_store)
    assert writer.enforcer is not None
    assert writer.enforcer.add_grouping_policy('user-a', 'user')
    reader = _permission_service_with_postgres_enforcer(config_store)
    reader._observed_workspace_permission_generation = generation
    monkeypatch.setattr(permission_lib, 'POLICY_UPDATE_LOCK_PATH',
                        str(tmp_path / 'legacy-policy.lock'))
    monkeypatch.setattr(writer, 'invalidate_user_permission_cache',
                        lambda _user_id: None)

    writer.update_role('user-a', 'admin')

    assert _read_workspace_rules(config_store) == {('user-a', 'team-a', '*')}
    assert skypilot_config.get_workspace_permission_generation().generation == (
        generation)
    assert committed.identity == skypilot_config.get_loaded_server_config_identity(
    )
    # D6 still owns role mutation, so D1 preserves its read freshness with a
    # generation-bracketed full policy load on a workspace cache miss.
    reader._ensure_workspace_permission_generation_current(force_reload=True)
    assert reader.enforcer is not None
    assert reader.enforcer.get_roles_for_user('user-a') == ['admin']


def test_bootstrap_and_upgrade_seed_preserve_the_retained_winner(
        config_store, monkeypatch):
    monkeypatch.setenv(constants.ENV_VAR_STATE_DB_MIGRATION_MODE, 'bootstrap')
    skypilot_config.initialize_postgres_server_config_authority()
    seeded = skypilot_config._get_server_config_record_from_db()
    assert seeded is not None
    assert dict(seeded.config) == {}
    receipt = skypilot_config.get_workspace_permission_generation()
    assert receipt.generation == 0
    assert receipt.config_identity == seeded.identity

    retained_value = yaml_utils.dump_yaml_str({'active_workspace': 'retained'})
    retained_identity = skypilot_config.ServerConfigIdentity(
        revision=9, digest=skypilot_config._config_value_digest(retained_value))
    with config_store.begin() as connection:
        connection.execute(
            sqlalchemy.update(skypilot_config.config_yaml_table).where(
                skypilot_config.config_yaml_table.c.key ==
                skypilot_config.API_SERVER_CONFIG_KEY).values(
                    value=retained_value,
                    revision=retained_identity.revision,
                    digest=retained_identity.digest))
        receipt_value = skypilot_config._permission_generation_value(
            0, retained_identity)
        connection.execute(
            sqlalchemy.update(skypilot_config.config_yaml_table).where(
                skypilot_config.config_yaml_table.c.key ==
                skypilot_config.WORKSPACE_PERMISSION_GENERATION_KEY).values(
                    value=receipt_value,
                    revision=2,
                    digest=skypilot_config._config_value_digest(receipt_value)))
    monkeypatch.setenv(constants.ENV_VAR_STATE_DB_MIGRATION_MODE, 'upgrade')
    skypilot_config.initialize_postgres_server_config_authority()
    retained = skypilot_config._get_server_config_record_from_db()
    assert retained is not None
    assert retained.identity == retained_identity
    assert retained.config['active_workspace'] == 'retained'


def test_verify_mode_is_zero_dml(config_store, monkeypatch):
    _seed_authority(config_store, {'active_workspace': 'retained'}, revision=4)
    dml: list[str] = []

    def record_dml(_conn, _cursor, statement, _parameters, _context,
                   _executemany):
        verb = statement.lstrip().split(None, 1)[0].upper()
        if verb in ('INSERT', 'UPDATE', 'DELETE'):
            dml.append(verb)

    sqlalchemy.event.listen(config_store, 'before_cursor_execute', record_dml)
    try:
        skypilot_config.initialize_postgres_server_config_authority()
    finally:
        sqlalchemy.event.remove(config_store, 'before_cursor_execute',
                                record_dml)
    assert dml == []


def test_verify_accepts_permission_receipt_bound_to_older_config(
        config_store, monkeypatch):
    initial = _seed_authority(config_store, {})
    monkeypatch.setattr(skypilot_config, '_reload_central_config_after_commit',
                        lambda: None)
    committed, _ = skypilot_config.mutate_postgres_server_config(
        lambda config: _set_label(config, 'non-workspace-only'),
        expected_identity=initial.identity)
    receipt = skypilot_config.get_workspace_permission_generation()
    assert receipt.generation == 0
    assert receipt.config_identity == initial.identity
    assert committed.identity.revision == initial.identity.revision + 1

    # A config-only change does not invalidate an unchanged workspace-policy
    # snapshot, so runtime verification must accept its older binding.
    skypilot_config.initialize_postgres_server_config_authority()


@pytest.mark.parametrize('invalid_state', [
    'missing-config',
    'bad-config-digest',
    'missing-receipt',
    'bad-receipt-digest',
    'mismatched-receipt-binding',
    'future-receipt-binding',
])
def test_verify_fails_closed_on_missing_or_invalid_rows(config_store,
                                                        invalid_state):
    value = yaml_utils.dump_yaml_str({})
    identity = skypilot_config.ServerConfigIdentity(
        revision=1, digest=skypilot_config._config_value_digest(value))
    receipt_config_identity = identity
    if invalid_state == 'mismatched-receipt-binding':
        receipt_config_identity = skypilot_config.ServerConfigIdentity(
            revision=identity.revision, digest='f' * 64)
    elif invalid_state == 'future-receipt-binding':
        receipt_config_identity = skypilot_config.ServerConfigIdentity(
            revision=identity.revision + 1, digest=identity.digest)
    receipt_value = skypilot_config._permission_generation_value(
        0, receipt_config_identity)
    rows = []
    if invalid_state != 'missing-config':
        rows.append({
            'key': skypilot_config.API_SERVER_CONFIG_KEY,
            'value': value,
            'revision': 1,
            'digest':
                ('0' *
                 64 if invalid_state == 'bad-config-digest' else identity.digest
                ),
        })
    if invalid_state != 'missing-receipt':
        rows.append({
            'key': skypilot_config.WORKSPACE_PERMISSION_GENERATION_KEY,
            'value': receipt_value,
            'revision': 1,
            'digest': ('0' * 64 if invalid_state == 'bad-receipt-digest' else
                       skypilot_config._config_value_digest(receipt_value)),
        })
    with config_store.begin() as connection:
        connection.execute(sqlalchemy.insert(skypilot_config.config_yaml_table),
                           rows)
    with pytest.raises((RuntimeError, ValueError)):
        skypilot_config.initialize_postgres_server_config_authority()


def test_reload_dispatches_central_and_scoped_child_roles(
        config_store, monkeypatch, tmp_path):
    _seed_authority(config_store, {'active_workspace': 'postgres'})
    child_path = tmp_path / 'child-config.yaml'
    child_path.write_text('active_workspace: child\n', encoding='utf-8')
    monkeypatch.setenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, str(child_path))

    child_markers = (
        constants.IS_SKYPILOT_SERVE_CONTROLLER,
        controller_constants.MANAGED_JOB_ID_ENV_VAR,
        controller_constants.MANAGED_JOB_CONTROLLER_SLOT_ID_ENV_VAR,
        controller_constants.MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT_ENV_VAR,
    )
    for marker in child_markers:
        monkeypatch.delenv(marker, raising=False)

    for role in ('api', 'controller', 'executor', 'image-copy-worker'):
        monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', role)
        skypilot_config.reload_config()
        assert skypilot_config.get_nested(('active_workspace',),
                                          None) == ('postgres')
        assert skypilot_config.get_loaded_server_config_identity().revision == 1

    # A boolean false Serve marker and an incomplete Jobs identity are central,
    # not broad file-precedence escape hatches.
    monkeypatch.setenv(constants.IS_SKYPILOT_SERVE_CONTROLLER, 'false')
    monkeypatch.setenv(controller_constants.MANAGED_JOB_ID_ENV_VAR, '42')
    skypilot_config.reload_config()
    assert skypilot_config.get_nested(('active_workspace',), None) == 'postgres'

    monkeypatch.setenv(constants.IS_SKYPILOT_SERVE_CONTROLLER, 'true')
    skypilot_config.reload_config()
    assert skypilot_config.get_nested(('active_workspace',), None) == 'child'

    monkeypatch.setenv(constants.IS_SKYPILOT_SERVE_CONTROLLER, 'false')
    monkeypatch.setenv(
        controller_constants.MANAGED_JOB_CONTROLLER_SLOT_ID_ENV_VAR, 'slot-1')
    monkeypatch.setenv(
        controller_constants.MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT_ENV_VAR, '1')
    skypilot_config.reload_config()
    assert skypilot_config.get_nested(('active_workspace',), None) == 'child'


def test_002_migration_backfills_retained_identity(config_store):
    with config_store.begin() as connection:
        connection.exec_driver_sql('DROP TABLE config_yaml')
        connection.exec_driver_sql('CREATE TABLE config_yaml ('
                                   'key TEXT PRIMARY KEY, value TEXT)')
        connection.execute(
            sqlalchemy.text('INSERT INTO config_yaml(key, value) '
                            'VALUES (:key, :value)'), {
                                'key': skypilot_config.API_SERVER_CONFIG_KEY,
                                'value': 'active_workspace: retained\n',
                            })
        connection.exec_driver_sql(
            'CREATE TABLE alembic_version_sky_config_db ('
            'version_num VARCHAR(32) NOT NULL)')
        connection.exec_driver_sql(
            "INSERT INTO alembic_version_sky_config_db VALUES ('001')")

    migration_utils.safe_alembic_upgrade(
        config_store,
        migration_utils.SKYPILOT_CONFIG_DB_NAME,
        migration_utils.SKYPILOT_CONFIG_VERSION,
        mode='upgrade')

    with config_store.connect() as connection:
        row = connection.execute(
            sqlalchemy.text('SELECT value, revision, digest FROM config_yaml '
                            'WHERE key = :key'), {
                                'key': skypilot_config.API_SERVER_CONFIG_KEY
                            }).one()
    assert row.value == 'active_workspace: retained\n'
    assert row.revision == 1
    assert row.digest == skypilot_config._config_value_digest(row.value)
    columns = {
        column['name']: column for column in sqlalchemy.inspect(
            config_store).get_columns('config_yaml')
    }
    assert columns['revision']['nullable'] is False
    assert columns['digest']['nullable'] is False

    # A pre-D1 binary updates only ``value``. The migration fence must reject
    # that mixed-version write instead of letting it silently invalidate the
    # retained digest while old and new pods overlap.
    with pytest.raises(sqlalchemy.exc.DBAPIError):
        with config_store.begin() as connection:
            connection.execute(
                sqlalchemy.text('UPDATE config_yaml SET value = :value '
                                'WHERE key = :key'),
                {
                    'key': skypilot_config.API_SERVER_CONFIG_KEY,
                    'value': 'active_workspace: old-writer\n',
                })
    with config_store.connect() as connection:
        retained = connection.execute(
            sqlalchemy.text('SELECT value, revision, digest FROM config_yaml '
                            'WHERE key = :key'), {
                                'key': skypilot_config.API_SERVER_CONFIG_KEY
                            }).one()
    assert retained.value == row.value
    assert retained.revision == row.revision
    assert retained.digest == row.digest

    next_value = 'active_workspace: canonical-writer\n'
    with config_store.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                'UPDATE config_yaml '
                'SET value = :value, revision = :revision, digest = :digest '
                'WHERE key = :key'), {
                    'key': skypilot_config.API_SERVER_CONFIG_KEY,
                    'value': next_value,
                    'revision': retained.revision + 1,
                    'digest': skypilot_config._config_value_digest(next_value),
                })
