"""Add the explicit placement normalization and retirement ledger.

Revision ID: 037
Revises: 036
Create Date: 2026-08-07

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.serve import serve_state_schema

# revision identifiers, used by Alembic.
revision: str = '037'
down_revision: str | Sequence[str] | None = '036'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SERVICES = 'services'
_VERSION_SPECS = 'version_specs'
_RETIREMENT_CHECK = 'ck_version_specs_retirement_all_or_none'
_RETIREMENT_EXPRESSION = (
    '((retired_at IS NULL AND retired_yaml_content IS NULL AND '
    'retirement_reason IS NULL AND retirement_run_id IS NULL) OR '
    '(retired_at IS NOT NULL AND yaml_content IS NULL AND '
    'retired_yaml_content IS NOT NULL AND retirement_reason IS NOT NULL AND '
    'retirement_run_id IS NOT NULL))')
_RUN_FOREIGN_KEYS = {
    _SERVICES: {
        'fk_services_placement_normalization_requested_run': 'placement_normalization_requested_run_id',
        'fk_services_placement_normalization_loaded_run': 'placement_normalization_loaded_run_id',
    },
    _VERSION_SPECS: {
        'fk_version_specs_retirement_run': 'retirement_run_id',
    },
}


def _version_retirement_prerequisite_columns() -> tuple[sa.Column, ...]:
    """Return legacy-layout columns required by the retirement constraint."""
    return (sa.Column('yaml_content', sa.Text(), nullable=True),)


def _service_receipt_columns() -> tuple[sa.Column, ...]:
    return (
        sa.Column('placement_normalization_requested_run_id',
                  sa.Uuid(as_uuid=True),
                  nullable=True),
        sa.Column('placement_normalization_loaded_run_id',
                  sa.Uuid(as_uuid=True),
                  nullable=True),
        sa.Column('placement_normalization_loaded_image_commit',
                  sa.Text(),
                  nullable=True),
        sa.Column('placement_normalization_loaded_controller_pid',
                  sa.Integer(),
                  nullable=True),
        sa.Column('placement_normalization_loaded_controller_ip',
                  sa.Text(),
                  nullable=True),
        sa.Column('placement_normalization_loaded_boot_id',
                  sa.Text(),
                  nullable=True),
        sa.Column('placement_normalization_loaded_at',
                  sa.Float(),
                  nullable=True),
    )


def _version_retirement_columns() -> tuple[sa.Column, ...]:
    return (
        sa.Column('retired_yaml_content', sa.Text(), nullable=True),
        sa.Column('retired_at', sa.Float(), nullable=True),
        sa.Column('retirement_reason', sa.Text(), nullable=True),
        sa.Column('retirement_run_id', sa.Uuid(as_uuid=True), nullable=True),
    )


def _add_missing_columns(bind: sa.engine.Connection, table: str,
                         additions: tuple[sa.Column, ...]) -> None:
    columns = {
        str(column['name']) for column in sa.inspect(bind).get_columns(table)
    }
    for column in additions:
        if column.name not in columns:
            op.add_column(table, column)


def _add_postgres_retirement_check(bind: sa.engine.Connection) -> None:
    # The central/API-server database is PostgreSQL-only.  Controller-local
    # SQLite databases receive the additive nullable columns but cannot safely
    # retrofit a CHECK without rebuilding the whole version history table.
    if bind.dialect.name != 'postgresql':
        return
    checks = {
        str(constraint['name'])
        for constraint in sa.inspect(bind).get_check_constraints(_VERSION_SPECS)
        if constraint['name'] is not None
    }
    if _RETIREMENT_CHECK not in checks:
        op.create_check_constraint(_RETIREMENT_CHECK, _VERSION_SPECS,
                                   _RETIREMENT_EXPRESSION)


def _add_postgres_run_foreign_keys(bind: sa.engine.Connection) -> None:
    # See the SQLite note in ``_add_postgres_retirement_check``.  The
    # authoritative PostgreSQL ledger rejects dangling generation receipts;
    # rebuilding a controller-local SQLite history table is unnecessary.
    if bind.dialect.name != 'postgresql':
        return
    inspector = sa.inspect(bind)
    for table, expected in _RUN_FOREIGN_KEYS.items():
        foreign_keys = {
            str(constraint['name'])
            for constraint in inspector.get_foreign_keys(table)
            if constraint['name'] is not None
        }
        for name, column in expected.items():
            if name not in foreign_keys:
                op.create_foreign_key(name,
                                      table,
                                      'placement_normalization_runs', [column],
                                      ['run_id'],
                                      ondelete='RESTRICT')


def upgrade() -> None:
    """Install inert ledger state without rewriting any persisted spec."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table in (_SERVICES, _VERSION_SPECS):
        if not inspector.has_table(table):
            raise RuntimeError(
                'Cannot add placement normalization state because the '
                f'{table} table is missing.')

    # Revision 001 creates the current metadata graph on fresh databases, so
    # these creates and column additions are deliberately idempotent.
    serve_state_schema.placement_normalization_runs_table.create(
        bind, checkfirst=True)
    serve_state_schema.placement_normalization_rows_table.create(
        bind, checkfirst=True)
    _add_missing_columns(bind, _SERVICES, _service_receipt_columns())
    # A short-lived pre-merge revision-016 layout omitted yaml_content even
    # though later canonical revisions assumed it existed.  Repair that
    # deployed shape before installing the retirement CHECK that references
    # the column.
    _add_missing_columns(bind, _VERSION_SPECS,
                         _version_retirement_prerequisite_columns())
    _add_missing_columns(bind, _VERSION_SPECS, _version_retirement_columns())
    _add_postgres_run_foreign_keys(bind)
    _add_postgres_retirement_check(bind)


def downgrade() -> None:
    """Retain normalization and retirement evidence during rollback."""
    raise RuntimeError(
        'SkyServe schema 037 is additive and cannot be downgraded. Placement '
        'normalization receipts and retired version history may be the only '
        'durable evidence that a legacy contract cannot be loaded again.')
