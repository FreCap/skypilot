"""PostgreSQL session-lock leases for placement normalization authority."""

# pylint: disable=protected-access,redefined-outer-name,unused-import

import pytest
import sqlalchemy
from sqlalchemy import orm
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import placement_contract_normalization
from sky.serve import placement_normalization_authority

_LOCK_NAME = placement_contract_normalization._ADVISORY_LOCK_NAME


def _try_session_lock(connection: sqlalchemy.engine.Connection) -> bool:
    return connection.execute(
        sqlalchemy.text('SELECT pg_catalog.pg_try_advisory_lock('
                        'pg_catalog.hashtextextended(:name, 0))'), {
                            'name': _LOCK_NAME
                        }).scalar_one()


def _unlock_session(connection: sqlalchemy.engine.Connection) -> bool:
    return connection.execute(
        sqlalchemy.text('SELECT pg_catalog.pg_advisory_unlock('
                        'pg_catalog.hashtextextended(:name, 0))'), {
                            'name': _LOCK_NAME
                        }).scalar_one()


def test_active_probe_rejects_transaction_only_ownership(empty_postgres):
    with empty_postgres.connect() as connection, connection.begin():
        connection.execute(
            sqlalchemy.text('SELECT pg_catalog.pg_advisory_xact_lock('
                            'pg_catalog.hashtextextended(:name, 0))'),
            {'name': _LOCK_NAME})
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='lost its session advisory authority'):
            placement_normalization_authority.reassert_writer_session_lock(
                connection, _LOCK_NAME)


def test_active_probe_rejects_reentrant_pooled_ownership(empty_postgres):
    with empty_postgres.connect() as connection, connection.begin():
        assert _try_session_lock(connection) is True
        assert _try_session_lock(connection) is True
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='multiple session advisory authority holds'):
            placement_normalization_authority.reassert_writer_session_lock(
                connection, _LOCK_NAME)
        assert _unlock_session(connection) is False


def test_exact_release_rejects_and_consumes_extra_hold(empty_postgres):
    with empty_postgres.connect() as connection, connection.begin():
        assert _try_session_lock(connection) is True
        assert _try_session_lock(connection) is True
        with pytest.raises(
                placement_normalization_authority.
                PlacementNormalizationAuthorityError,
                match='retained an extra session advisory authority hold'):
            placement_normalization_authority.release_writer_session_lock(
                connection, _LOCK_NAME)
        assert _unlock_session(connection) is False


def test_uncertain_acquisition_invalidates_physical_connection(
        empty_postgres, monkeypatch):

    def interrupt_after_possible_side_effect(*_args, **_kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(placement_normalization_authority,
                        'assert_writer_database_authority',
                        interrupt_after_possible_side_effect)
    connection = empty_postgres.connect()
    with pytest.raises(KeyboardInterrupt):
        placement_contract_normalization._acquire_writer_session_lock(
            connection)
    assert connection.invalidated
    connection.close()

    with empty_postgres.connect() as contender, contender.begin():
        assert _try_session_lock(contender) is True
        assert _unlock_session(contender) is True


def test_writer_lease_releases_exact_hold_after_transaction(
        empty_postgres, monkeypatch):

    def assert_authority(
        connection: sqlalchemy.engine.Connection,
        lock_name: str,
    ) -> (placement_normalization_authority.
          PlacementNormalizationDatabaseAuthority):
        placement_normalization_authority.reassert_writer_session_lock(
            connection, lock_name)
        return (placement_normalization_authority.
                PlacementNormalizationDatabaseAuthority('public', None))

    monkeypatch.setattr(placement_normalization_authority,
                        'assert_writer_database_authority', assert_authority)
    with placement_contract_normalization._writer_database_authority(
            empty_postgres) as lease:
        connection = lease.connection
        assert lease.authority.schema == 'public'
        with connection.begin():
            placement_normalization_authority.assert_writer_session_lock(
                connection, _LOCK_NAME)
    assert connection.closed

    with empty_postgres.connect() as contender, contender.begin():
        assert contender.exec_driver_sql(
            'SHOW transaction_isolation').scalar_one() == 'read committed'
        assert _try_session_lock(contender) is True
        assert _unlock_session(contender) is True


def test_writer_lease_ignores_hostile_lock_functions_and_search_path(
        empty_postgres, monkeypatch):
    hostile_schema = 'hostile_normalization_lock'
    with empty_postgres.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {hostile_schema}')
        connection.exec_driver_sql(f"""
            CREATE FUNCTION {hostile_schema}.hashtextextended(text, bigint)
            RETURNS bigint LANGUAGE sql IMMUTABLE AS 'SELECT 1::bigint';
            CREATE FUNCTION {hostile_schema}.pg_try_advisory_lock(bigint)
            RETURNS boolean LANGUAGE sql AS 'SELECT true';
            CREATE FUNCTION {hostile_schema}.pg_try_advisory_xact_lock(bigint)
            RETURNS boolean LANGUAGE sql AS 'SELECT true';
            CREATE FUNCTION {hostile_schema}.pg_advisory_unlock(bigint)
            RETURNS boolean LANGUAGE sql AS 'SELECT true';
            """)

    hostile_engine = sqlalchemy.create_engine(
        empty_postgres.url,
        connect_args={'options': f'-csearch_path={hostile_schema},pg_catalog'})

    def assert_authority(
        connection: sqlalchemy.engine.Connection,
        lock_name: str,
    ) -> (placement_normalization_authority.
          PlacementNormalizationDatabaseAuthority):
        placement_normalization_authority.reassert_writer_session_lock(
            connection, lock_name)
        return (placement_normalization_authority.
                PlacementNormalizationDatabaseAuthority('public', None))

    monkeypatch.setattr(placement_normalization_authority,
                        'assert_writer_database_authority', assert_authority)
    try:
        with placement_contract_normalization._writer_database_authority(
                hostile_engine) as lease:
            with orm.Session(bind=lease.connection) as session, session.begin():
                placement_normalization_authority.bind_session_to_authority(
                    session, lease.authority)
                assert session.execute(
                    sqlalchemy.text(
                        "SELECT pg_catalog.current_setting('search_path')")
                ).scalar_one() == 'pg_catalog'
                placement_normalization_authority.assert_writer_session_lock(
                    lease.connection, _LOCK_NAME)
    finally:
        hostile_engine.dispose()
        with empty_postgres.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {hostile_schema} CASCADE')

    with empty_postgres.connect() as contender, contender.begin():
        assert _try_session_lock(contender) is True
        assert _unlock_session(contender) is True


def test_uncertain_release_invalidates_physical_connection(
        empty_postgres, monkeypatch):
    invalidated_connections: list[sqlalchemy.engine.Connection] = []
    original_invalidate = sqlalchemy.engine.Connection.invalidate

    def record_invalidation(connection: sqlalchemy.engine.Connection,
                            exception=None) -> None:
        invalidated_connections.append(connection)
        original_invalidate(connection, exception)

    def acquire(
        connection: sqlalchemy.engine.Connection,
    ) -> (placement_normalization_authority.
          PlacementNormalizationDatabaseAuthority):
        with connection.begin():
            assert _try_session_lock(connection) is True
        return (placement_normalization_authority.
                PlacementNormalizationDatabaseAuthority('public', None))

    def interrupt_before_observed_release(*_args, **_kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(placement_contract_normalization,
                        '_acquire_writer_session_lock', acquire)
    monkeypatch.setattr(placement_normalization_authority,
                        'release_writer_session_lock',
                        interrupt_before_observed_release)
    monkeypatch.setattr(sqlalchemy.engine.Connection, 'invalidate',
                        record_invalidation)
    borrowed = None
    with pytest.raises(KeyboardInterrupt):
        with placement_contract_normalization._writer_database_authority(
                empty_postgres) as lease:
            borrowed = lease.connection
            assert lease.authority.schema == 'public'
    assert borrowed is not None
    assert invalidated_connections == [borrowed]
    assert borrowed.closed

    with empty_postgres.connect() as contender, contender.begin():
        assert _try_session_lock(contender) is True
        assert _unlock_session(contender) is True
