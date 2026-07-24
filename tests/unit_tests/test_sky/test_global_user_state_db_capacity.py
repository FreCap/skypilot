"""Tests for central PostgreSQL connection-capacity discovery."""
# pylint: disable=protected-access

from unittest import mock

from sky import global_user_state


def test_get_max_db_connections_excludes_postgres_reserved_slots():
    engine = mock.MagicMock()
    engine.dialect.name = 'postgresql'
    session = mock.MagicMock()
    session.execute.return_value.one.return_value = ('100', '3', '4')
    session_context = mock.MagicMock()
    session_context.__enter__.return_value = session

    with mock.patch.object(global_user_state._db_manager,
                           'get_engine',
                           return_value=engine), mock.patch(
                               'sky.global_user_state.sqlalchemy.orm.Session',
                               return_value=session_context):
        capacity = global_user_state.get_max_db_connections()

    assert capacity == 93
    statement = str(session.execute.call_args.args[0])
    assert "current_setting('reserved_connections', true)" in statement


def test_get_max_db_connections_supports_pre_postgres_16():
    engine = mock.MagicMock()
    engine.dialect.name = 'postgresql'
    session = mock.MagicMock()
    session.execute.return_value.one.return_value = ('100', '3', None)
    session_context = mock.MagicMock()
    session_context.__enter__.return_value = session

    with mock.patch.object(global_user_state._db_manager,
                           'get_engine',
                           return_value=engine), mock.patch(
                               'sky.global_user_state.sqlalchemy.orm.Session',
                               return_value=session_context):
        capacity = global_user_state.get_max_db_connections()

    assert capacity == 97
