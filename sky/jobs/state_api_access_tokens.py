"""Persistence repository for managed-job API access-token ownership."""

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky.jobs.state_schema import api_access_token_table
from sky.jobs.state_schema import spot_table
from sky.jobs.state_storage import db_manager as _db_manager
from sky.jobs.status_types import ManagedJobStatus
from sky.utils.db import db_utils
from sky.utils.db import retries as db_retries

# Bound parameters per token upsert while keeping all chunks in one transaction.
_API_ACCESS_TOKEN_UPSERT_BATCH_SIZE = 1000


def set_api_access_token_ids(job_ids: list[int], token_id: str) -> None:
    """Store one API access token ID for a batch of managed jobs."""
    unique_job_ids = list(dict.fromkeys(job_ids))
    if not unique_job_ids:
        return

    engine = _db_manager.get_engine()
    dialect_map = {
        db_utils.SQLAlchemyDialect.SQLITE.value: sqlite.insert,
        db_utils.SQLAlchemyDialect.POSTGRESQL.value: postgresql.insert,
    }
    insert_func = dialect_map.get(engine.dialect.name)
    if insert_func is None:
        raise ValueError(f'Unsupported database dialect: {engine.dialect.name}')
    with orm.Session(engine) as session:
        for offset in range(0, len(unique_job_ids),
                            _API_ACCESS_TOKEN_UPSERT_BATCH_SIZE):
            job_id_batch = unique_job_ids[offset:offset +
                                          _API_ACCESS_TOKEN_UPSERT_BATCH_SIZE]
            insert_stmt = insert_func(api_access_token_table).values([{
                'job_id': job_id,
                'token_id': token_id,
            } for job_id in job_id_batch])
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[api_access_token_table.c.job_id],
                set_={
                    api_access_token_table.c.token_id:
                        insert_stmt.excluded.token_id
                })
            session.execute(upsert_stmt)
        session.commit()


@db_retries.retry
def get_releasable_api_access_token_id(job_id: int) -> str | None:
    """Return this job's token only when every associated job is terminal."""
    engine = _db_manager.get_engine()
    owner = api_access_token_table.alias('token_owner')
    sibling = api_access_token_table.alias('token_sibling')
    sibling_tasks = sibling.outerjoin(
        spot_table, sibling.c.job_id == spot_table.c.spot_job_id)
    terminal_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    unreleasable_sibling = sqlalchemy.exists(
        sqlalchemy.select(1).select_from(sibling_tasks).where(
            sibling.c.token_id == owner.c.token_id,
            sqlalchemy.or_(spot_table.c.status.is_(None),
                           spot_table.c.status.not_in(terminal_values))))
    query = sqlalchemy.select(owner.c.token_id).where(owner.c.job_id == job_id,
                                                      ~unreleasable_sibling)
    with orm.Session(engine) as session:
        return session.execute(query).scalar_one_or_none()


# Preserve reflection and function pickle lookup through the historical facade.
set_api_access_token_ids.__module__ = 'sky.jobs.state'
get_releasable_api_access_token_id.__module__ = 'sky.jobs.state'
get_releasable_api_access_token_id.__wrapped__.__module__ = 'sky.jobs.state'
