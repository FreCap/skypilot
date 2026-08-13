"""Add commit-ordered reserved-fill observation authority.

Revision ID: 044
Revises: 043
Create Date: 2026-08-12

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.serve import pool_capacity_observation_schema as observation_schema
from sky.utils.db import db_utils

revision: str = '044'
down_revision: str | Sequence[str] | None = '043'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_PROTOCOL_STATE = 'reserved_fill_protocol_state'
_OBSERVATIONS = 'demand_capacity_observations'
_ROUNDS = 'reserved_fill_rounds'
_CLAIM_SETS = 'reserved_fill_service_claim_sets'
_GATE_GUARD_FUNCTION = 'skyserve044_guard_reconciliation_gate'
_GATE_GUARD_TRIGGER = 'skyserve044_reconciliation_gate_guard'


def _require_observation_relations(bind: sa.engine.Connection) -> None:
    inspector = sa.inspect(bind)
    missing = [
        relation for relation in (
            _PROTOCOL_STATE,
            _OBSERVATIONS,
            _ROUNDS,
            _CLAIM_SETS,
        ) if not inspector.has_table(relation)
    ]
    if missing:
        raise RuntimeError('Cannot install Serve044 observation authority; '
                           f'missing relations: {missing!r}.')


def _add_missing_columns(bind: sa.engine.Connection, table: str,
                         additions: tuple[sa.Column, ...]) -> None:
    columns = {
        str(column['name']) for column in sa.inspect(bind).get_columns(table)
    }
    for column in additions:
        if column.name not in columns:
            op.add_column(table, column)


def _add_check(bind: sa.engine.Connection, table: str, name: str,
               expression: str) -> None:
    checks = {
        str(constraint['name'])
        for constraint in sa.inspect(bind).get_check_constraints(table)
        if constraint['name'] is not None
    }
    if name not in checks:
        op.create_check_constraint(name, table, expression)


def _add_observation_identity(bind: sa.engine.Connection) -> None:
    inspector = sa.inspect(bind)
    unique_constraints = {
        str(constraint['name'])
        for constraint in inspector.get_unique_constraints(_OBSERVATIONS)
        if constraint['name'] is not None
    }
    if (observation_schema.OBSERVATION_IDENTITY_UNIQUE_NAME
            not in unique_constraints):
        op.create_unique_constraint(
            observation_schema.OBSERVATION_IDENTITY_UNIQUE_NAME,
            _OBSERVATIONS,
            ['pool_key', 'observation_generation'],
        )

    indexes = {
        str(index['name'])
        for index in sa.inspect(bind).get_indexes(_OBSERVATIONS)
        if index['name'] is not None
    }
    if (observation_schema.OBSERVATION_POOL_GENERATION_INDEX_NAME
            not in indexes):
        op.create_index(
            observation_schema.OBSERVATION_POOL_GENERATION_INDEX_NAME,
            _OBSERVATIONS,
            ['pool_key', sa.text('observation_generation DESC')],
        )
    if (observation_schema.OBSERVATION_POOL_COMPLETED_INDEX_NAME
            not in indexes):
        op.create_index(
            observation_schema.OBSERVATION_POOL_COMPLETED_INDEX_NAME,
            _OBSERVATIONS,
            ['pool_key', sa.text('observation_generation DESC')],
            postgresql_where=sa.text(
                "observation_status IN ('SUCCESS', 'BLACKOUT')"),
        )


def _install_observation_authority(bind: sa.engine.Connection) -> None:
    _require_observation_relations(bind)
    _add_missing_columns(bind, _PROTOCOL_STATE,
                         observation_schema.protocol_authority_columns())
    _add_missing_columns(bind, _OBSERVATIONS,
                         observation_schema.observation_authority_columns())
    _add_missing_columns(
        bind, _ROUNDS,
        observation_schema.round_observation_provenance_columns())
    _add_missing_columns(bind, _CLAIM_SETS,
                         observation_schema.allocation_publication_columns())
    _add_check(bind, _PROTOCOL_STATE,
               observation_schema.PROTOCOL_SEQUENCE_CHECK_NAME,
               observation_schema.PROTOCOL_SEQUENCE_CHECK_SQL)
    _add_check(bind, _PROTOCOL_STATE,
               observation_schema.RECONCILIATION_GATE_CHECK_NAME,
               observation_schema.RECONCILIATION_GATE_CHECK_SQL)
    _add_check(bind, _OBSERVATIONS,
               observation_schema.OBSERVATION_AUTHORITY_SHAPE_CHECK_NAME,
               observation_schema.OBSERVATION_AUTHORITY_SHAPE_CHECK_SQL)
    _add_check(bind, _OBSERVATIONS,
               observation_schema.OBSERVATION_AUTHORITY_BOUNDS_CHECK_NAME,
               observation_schema.OBSERVATION_AUTHORITY_BOUNDS_CHECK_SQL)
    _add_check(bind, _ROUNDS,
               observation_schema.ROUND_OBSERVATION_PROVENANCE_CHECK_NAME,
               observation_schema.ROUND_OBSERVATION_PROVENANCE_CHECK_SQL)
    _add_check(bind, _CLAIM_SETS,
               observation_schema.ALLOCATION_PUBLICATION_CHECK_NAME,
               observation_schema.ALLOCATION_PUBLICATION_CHECK_SQL)
    _add_observation_identity(bind)
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_GATE_GUARD_FUNCTION}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            IF NEW.reconciliation_gate_generation <>
                   OLD.reconciliation_gate_generation
               OR NEW.reconciliation_gate_state <>
                      OLD.reconciliation_gate_state THEN
                IF NEW.reconciliation_gate_generation <
                       OLD.reconciliation_gate_generation THEN
                    RAISE EXCEPTION
                        'SkyServe reconciliation gate generation cannot decrease';
                END IF;
                IF OLD.reconciliation_gate_state = 'SEQUENCED_ACTIVE'
                   AND NEW.reconciliation_gate_state <>
                           'SEQUENCED_ACTIVE' THEN
                    RAISE EXCEPTION
                        'SkyServe reconciliation gate cannot return to legacy';
                END IF;
                IF NEW.reconciliation_gate_state <>
                       OLD.reconciliation_gate_state
                   AND NEW.reconciliation_gate_generation <=
                           OLD.reconciliation_gate_generation THEN
                    RAISE EXCEPTION
                        'SkyServe reconciliation gate transition requires a new generation';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $function$
    """)
    op.execute(f'DROP TRIGGER IF EXISTS {_GATE_GUARD_TRIGGER} '
               f'ON {_PROTOCOL_STATE}')
    op.execute(f"""
        CREATE TRIGGER {_GATE_GUARD_TRIGGER}
        BEFORE UPDATE OF reconciliation_gate_state,
                         reconciliation_gate_generation
        ON {_PROTOCOL_STATE}
        FOR EACH ROW
        EXECUTE FUNCTION {_GATE_GUARD_FUNCTION}()
    """)
    op.execute(f'ALTER TABLE {_PROTOCOL_STATE} ENABLE ALWAYS TRIGGER '
               f'{_GATE_GUARD_TRIGGER}')


def upgrade() -> None:
    """Install inactive sequenced reconciliation authority on PostgreSQL."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    _install_observation_authority(bind)


def downgrade() -> None:
    raise RuntimeError(
        'Serve044 is forward-only. Keep observation and allocation evidence '
        'and iterate application images through the sequenced fix-forward '
        'maintenance path.')
