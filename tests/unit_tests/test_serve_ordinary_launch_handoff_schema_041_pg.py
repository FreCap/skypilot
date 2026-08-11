"""PostgreSQL contracts for ordinary-launch handoff schema revision 041."""
# pylint: disable=protected-access,redefined-outer-name,unused-import

import datetime
import uuid

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import ordinary_launch_handoff
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_ordinary_launch_handoff_schema_041_pg')

_TABLE = ordinary_launch_handoff.serve_ordinary_launch_handoff_events_table
_RECORD_A = uuid.UUID('11111111-1111-4111-8111-111111111111')
_RECORD_B = uuid.UUID('22222222-2222-4222-8222-222222222222')
_ROUTE_A = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
_ROUTE_B = uuid.UUID('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb')


def _config(engine):
    return migration_utils.get_alembic_config(engine,
                                              migration_utils.SERVE_DB_NAME)


def _upgrade(engine, revision: str) -> None:
    alembic_command.upgrade(_config(engine), revision)


def _revision(engine) -> str | None:
    return migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME)


@pytest.fixture
def serve041(empty_postgres):
    _upgrade(empty_postgres, '041')
    assert _revision(empty_postgres) == '041'
    return empty_postgres


def _event(
    kind: ordinary_launch_handoff.EventKind,
    *,
    record_id: uuid.UUID = _RECORD_A,
    route_epoch: uuid.UUID = _ROUTE_A,
    observed_at: datetime.datetime | None = None,
    request_id: str | None = None,
    service_job_id: int | None = None,
    terminal_status: ordinary_launch_handoff.TerminalStatus | None = None
) -> dict[str, object]:
    if (kind == ordinary_launch_handoff.EventKind.API_TERMINAL and
            terminal_status is None):
        terminal_status = ordinary_launch_handoff.TerminalStatus.SUCCEEDED
    values: dict[str, object] = {
        'event_id': uuid.uuid4(),
        'event_kind': kind.value,
        'service_name': 'svc',
        'service_version': 1,
        'replica_id': 1 if record_id == _RECORD_A else 2,
        'replica_record_id': record_id,
        'controller_route_epoch': route_epoch,
        'ordinary_request_id': request_id,
        'service_job_id': service_job_id,
        'terminal_status':
            (None if terminal_status is None else terminal_status.value),
        'input_digest': 'a' * 64,
    }
    if observed_at is not None:
        values['observed_at'] = observed_at
    return values


def test_serve041_lineage_and_sqlite_ceiling() -> None:
    sqlite = sqlalchemy.create_engine('sqlite://')
    scripts = alembic_script.ScriptDirectory.from_config(_config(sqlite))
    revision = scripts.get_revision('041')

    assert scripts.get_heads() == ['041']
    assert revision.down_revision == '040'
    assert migration_utils.SERVE_VERSION == '041'
    assert migration_utils.serve_target_version(sqlite) == '037'


def test_serve041_catalog_is_closed_and_append_only(serve041) -> None:
    inspector = sqlalchemy.inspect(serve041)
    assert [column['name'] for column in inspector.get_columns(_TABLE.name)
           ] == list(_TABLE.c.keys())
    assert {
        check['name'] for check in inspector.get_check_constraints(_TABLE.name)
    } == {
        'serve_ordinary_launch_event_kind',
        'serve_ordinary_launch_input_digest',
        'serve_ordinary_launch_replica_id',
        'serve_ordinary_launch_request_id',
        'serve_ordinary_launch_service_job_id',
        'serve_ordinary_launch_service_name',
        'serve_ordinary_launch_service_version',
        'serve_ordinary_launch_terminal_status',
    }
    assert {index['name'] for index in inspector.get_indexes(_TABLE.name)} == {
        'serve_ordinary_launch_handoff_record_idx',
        'serve_ordinary_launch_handoff_request_idx',
        'serve_ordinary_launch_handoff_retention_idx',
    }

    with serve041.begin() as connection:
        connection.execute(_TABLE.insert().values(
            **_event(ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
                     request_id='request-a')))
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='handoff events are append-only'):
        with serve041.begin() as connection:
            connection.execute(_TABLE.update().values(service_name='rewritten'))
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='handoff events are append-only'):
        with serve041.begin() as connection:
            connection.exec_driver_sql(f'TRUNCATE TABLE {_TABLE.name}')


def test_serve041_terminal_status_is_closed_and_kind_scoped(serve041) -> None:
    with serve041.begin() as connection:
        for terminal_status in ordinary_launch_handoff.TerminalStatus:
            connection.execute(_TABLE.insert().values(
                **_event(ordinary_launch_handoff.EventKind.API_TERMINAL,
                         request_id=f'request-{terminal_status.value.lower()}',
                         terminal_status=terminal_status)))

    invalid_rows = [
        _event(ordinary_launch_handoff.EventKind.API_TERMINAL,
               request_id='request-missing-status'),
        _event(ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
               request_id='request-active',
               terminal_status=ordinary_launch_handoff.TerminalStatus.FAILED),
    ]
    invalid_rows[0]['terminal_status'] = None
    for row in invalid_rows:
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            with serve041.begin() as connection:
                connection.execute(_TABLE.insert().values(**row))


def test_writer_uses_database_clock_and_prunes_after_60_days(
        serve041, monkeypatch) -> None:
    monkeypatch.setattr(ordinary_launch_handoff.serve_state,
                        'get_database_engine', lambda: serve041)
    event = ordinary_launch_handoff._event(
        event_kind=ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
        service_name='svc',
        service_version=1,
        replica_id=1,
        replica_record_id=str(_RECORD_A),
        controller_route_epoch=str(_ROUTE_A),
        ordinary_request_id='request-current',
        service_job_id=None,
        input_digest='a' * 64)
    with serve041.begin() as connection:
        before = connection.execute(
            sqlalchemy.text('SELECT clock_timestamp()')).scalar_one()
        connection.execute(_TABLE.insert().values(
            **_event(ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
                     observed_at=before - datetime.timedelta(days=61),
                     request_id='request-expired')))

    assert ordinary_launch_handoff._write_event(event)

    with serve041.connect() as connection:
        after = connection.execute(
            sqlalchemy.text('SELECT clock_timestamp()')).scalar_one()
        rows = connection.execute(
            sqlalchemy.select(_TABLE.c.event_id, _TABLE.c.observed_at,
                              _TABLE.c.ordinary_request_id)).all()
    assert len(rows) == 1
    assert rows[0].event_id == event.event_id
    assert rows[0].ordinary_request_id == 'request-current'
    assert before <= rows[0].observed_at <= after


def test_summary_reports_restart_and_duplicate_evidence(serve041,
                                                        monkeypatch) -> None:
    monkeypatch.setattr(ordinary_launch_handoff.serve_state,
                        'get_database_engine', lambda: serve041)
    with serve041.begin() as connection:
        now = connection.execute(
            sqlalchemy.text('SELECT CURRENT_TIMESTAMP')).scalar_one()
        minute = datetime.timedelta(minutes=1)
        rows = [
            _event(ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
                   observed_at=now - 10 * minute,
                   request_id='request-a1'),
            _event(ordinary_launch_handoff.EventKind.API_TERMINAL,
                   observed_at=now - 9 * minute,
                   request_id='request-a1'),
            _event(
                ordinary_launch_handoff.EventKind.CONTROLLER_START_NONTERMINAL,
                route_epoch=_ROUTE_B,
                observed_at=now - 8 * minute),
            _event(ordinary_launch_handoff.EventKind.RESTART_REDRIVE,
                   route_epoch=_ROUTE_B,
                   observed_at=now - 7 * minute),
            _event(ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
                   route_epoch=_ROUTE_B,
                   observed_at=now - 6 * minute,
                   request_id='request-a2'),
            _event(ordinary_launch_handoff.EventKind.SERVICE_JOB_OBSERVED,
                   observed_at=now - 5 * minute,
                   request_id='request-a1',
                   service_job_id=101),
            _event(ordinary_launch_handoff.EventKind.SERVICE_JOB_OBSERVED,
                   route_epoch=_ROUTE_B,
                   observed_at=now - 4 * minute,
                   request_id='request-a2',
                   service_job_id=102),
            _event(ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
                   record_id=_RECORD_B,
                   observed_at=now - 5 * minute,
                   request_id='request-b1'),
            _event(
                ordinary_launch_handoff.EventKind.CONTROLLER_START_NONTERMINAL,
                record_id=_RECORD_B,
                route_epoch=_ROUTE_B,
                observed_at=now - 4 * minute),
            _event(ordinary_launch_handoff.EventKind.RESTART_REDRIVE,
                   record_id=_RECORD_B,
                   route_epoch=_ROUTE_B,
                   observed_at=now - 3 * minute),
            _event(ordinary_launch_handoff.EventKind.OWNER_LOSS_CANCELLED,
                   record_id=_RECORD_B,
                   route_epoch=_ROUTE_B,
                   observed_at=now - 2 * minute,
                   request_id='request-b1'),
            _event(ordinary_launch_handoff.EventKind.
                   CLEANUP_RETRY_AFTER_ROUTE_EPOCH_CHANGE,
                   record_id=_RECORD_B,
                   route_epoch=_ROUTE_B,
                   observed_at=now - minute),
        ]
        connection.execute(_TABLE.insert(), rows)

    summary = ordinary_launch_handoff.get_summary()

    assert summary['available'] is True
    assert summary['retention_days'] == 60
    assert summary['eligible_ordinary_launches'] == 2
    assert summary['controller_starts_during_nonterminal_launches'] == 2
    assert summary[
        'replica_records_with_multiple_requests_before_projection'] == 1
    assert summary['restart_redrives_with_active_predecessor'] == 1
    assert summary[
        'restart_redrives_with_terminal_unprojected_predecessor'] == 1
    assert summary['replica_records_with_duplicate_service_jobs'] == 1
    assert summary['owner_loss_cancellations'] == 1
    assert summary['cleanup_retries_after_route_epoch_change'] == 1


def test_downgrade_retains_evidence_and_update_fence(serve041) -> None:
    with serve041.begin() as connection:
        connection.execute(_TABLE.insert().values(
            **_event(ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
                     request_id='request-a')))
    alembic_command.downgrade(_config(serve041), '040')

    assert _revision(serve041) == '040'
    assert sqlalchemy.inspect(serve041).has_table(_TABLE.name)
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='handoff events are append-only'):
        with serve041.begin() as connection:
            connection.execute(_TABLE.update().values(service_name='changed'))
