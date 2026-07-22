"""Shared database lifecycle for managed jobs state repositories."""
import sqlalchemy
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import orm

from sky.utils import common_utils
from sky.utils.db import db_utils
from sky.utils.db import migration_utils


def create_table(engine: sqlalchemy.engine.Engine):
    # Enable WAL mode to avoid locking issues.
    # See: issue #3863, #1441 and PR #1509
    # https://github.com/microsoft/WSL/issues/2395
    # TODO(romilb): We do not enable WAL for WSL because of known issue in WSL.
    #  This may cause the database locked problem from WSL issue #1441.
    if (engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value and
            not common_utils.is_wsl()):
        try:
            with orm.Session(engine) as session:
                session.execute(sqlalchemy.text('PRAGMA journal_mode=WAL'))
                session.execute(sqlalchemy.text('PRAGMA synchronous=1'))
                session.commit()
        except sqlalchemy_exc.OperationalError as e:
            if 'database is locked' not in str(e):
                raise
            # If the database is locked, it is OK to continue, as the WAL mode
            # is not critical and is likely to be enabled by other processes.

    migration_utils.safe_alembic_upgrade(
        engine,
        migration_utils.SPOT_JOBS_DB_NAME,
        migration_utils.SPOT_JOBS_VERSION,
        mode=migration_utils.configured_migration_mode())


db_manager = db_utils.DatabaseManager('spot_jobs', create_table)
initialize_and_get_db = db_manager.get_engine
