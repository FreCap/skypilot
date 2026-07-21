"""Shared auth-session storage for the CLI browser-login flow."""
from collections.abc import Callable
import time

import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky import global_user_state
from sky.server import constants as server_constants
from sky.utils import common_utils
from sky.utils.db import db_utils


class AuthSessionStore:
    """Store short-lived auth sessions in the global-state database."""

    def __init__(
        self,
        engine_provider: Callable[
            [],
            sqlalchemy.engine.Engine] = global_user_state.initialize_and_get_db,
    ):
        self._engine_provider = engine_provider

    def create_session(self, code_challenge: str, token: str) -> None:
        """Create or atomically replace an authorized session."""
        engine = self._engine_provider()
        created_at = time.time()
        expiry_time = created_at - server_constants.AUTH_SESSION_TIMEOUT_SECONDS
        table = global_user_state.auth_session_table

        if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            insert_statement = postgresql.insert(table)
        elif engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_statement = sqlite.insert(table)
        else:
            raise ValueError(
                f'Unsupported database dialect: {engine.dialect.name}')

        insert_statement = insert_statement.values(
            code_challenge=code_challenge,
            token=token,
            created_at=created_at,
        )
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=[table.c.code_challenge],
            set_={
                table.c.token: insert_statement.excluded.token,
                table.c.created_at: insert_statement.excluded.created_at,
            })

        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.delete(table).where(
                    table.c.created_at < expiry_time))
            connection.execute(upsert_statement)

    def poll_session(self, code_verifier: str) -> str | None:
        """Atomically consume and return an unexpired session token."""
        code_challenge = common_utils.compute_code_challenge(code_verifier)
        expiry_threshold = (time.time() -
                            server_constants.AUTH_SESSION_TIMEOUT_SECONDS)
        table = global_user_state.auth_session_table
        statement = (sqlalchemy.delete(table).where(
            table.c.code_challenge == code_challenge, table.c.created_at
            > expiry_threshold).returning(table.c.token))

        engine = self._engine_provider()
        with engine.begin() as connection:
            row = connection.execute(statement).first()
        return row.token if row is not None else None


auth_session_store = AuthSessionStore()
