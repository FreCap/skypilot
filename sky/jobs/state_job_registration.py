"""Persistence repository for managed-job initial-row registration."""

from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky.jobs.state_schema import job_info_table
from sky.jobs.state_storage import db_manager as _db_manager
from sky.jobs.status_types import ManagedJobScheduleState
from sky.utils.db import db_utils


def set_job_info_without_job_id(name: str,
                                workspace: str,
                                entrypoint: str,
                                pool: str | None,
                                pool_hash: str | None,
                                user_hash: str | None,
                                execution: str | None = None,
                                is_batch: bool = False,
                                file_mounts_blob_id: str | None = None) -> int:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')

        insert_stmt = insert_func(job_info_table).values(
            name=name,
            schedule_state=ManagedJobScheduleState.INACTIVE.value,
            workspace=workspace,
            entrypoint=entrypoint,
            pool=pool,
            pool_hash=pool_hash,
            user_hash=user_hash,
            execution=execution,
            is_batch=is_batch,
            file_mounts_blob_id=file_mounts_blob_id,
        )

        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            result = session.execute(insert_stmt)
            ret = result.lastrowid
            session.commit()
            return ret
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            result = session.execute(
                insert_stmt.returning(job_info_table.c.spot_job_id))
            ret = result.scalar()
            session.commit()
            return ret  # pyright: ignore[reportReturnType]
        else:
            raise ValueError('Unsupported database dialect')


def set_job_info(job_id: int,
                 name: str,
                 workspace: str,
                 entrypoint: str,
                 pool: str | None,
                 pool_hash: str | None,
                 user_hash: str | None = None,
                 execution: str | None = None,
                 is_batch: bool = False):
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')
        insert_stmt = insert_func(job_info_table).values(
            spot_job_id=job_id,
            name=name,
            schedule_state=ManagedJobScheduleState.INACTIVE.value,
            workspace=workspace,
            entrypoint=entrypoint,
            pool=pool,
            pool_hash=pool_hash,
            user_hash=user_hash,
            execution=execution,
            is_batch=is_batch,
        )
        session.execute(insert_stmt)
        session.commit()


# Preserve reflection and function pickle lookup through the historical facade.
set_job_info_without_job_id.__module__ = 'sky.jobs.state'
set_job_info.__module__ = 'sky.jobs.state'
