"""Add durable resource-action execution and settlement history.

Revision ID: 039
Revises: 038
Create Date: 2026-08-04

"""
# pylint: disable=invalid-name,protected-access
from collections.abc import Sequence
import importlib
import typing

from alembic import op
import sqlalchemy as sa

from sky.serve import resource_action_m4_state_schema as m4_schema
from sky.serve import resource_action_state_schema
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '039'
down_revision: str | Sequence[str] | None = '038'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_POLICIES = m4_schema.AUTHORITY_POLICY_EPOCHS.name
_COHORTS = resource_action_state_schema.WORKER_COHORTS.name
_HANDOFFS = m4_schema.WORKER_REGISTRATION_HANDOFFS.name
_LEASES = m4_schema.WORKER_REGISTRATION_LEASES.name
_REFS = resource_action_state_schema.WORKER_COHORT_REFS.name
_SHADOW_PARENTS = resource_action_state_schema.SHADOW_SAMPLES.name
_SHADOW_CHILDREN = resource_action_state_schema.SHADOW_ATTEMPTS.name

_PROCESS = m4_schema.WORKER_PROCESS_SUPERSESSIONS.name
_SHADOW_EXECUTION = m4_schema.SHADOW_EXECUTION_HISTORY.name
_GC_CURSOR = m4_schema.API_INSTANCE_GC_CURSORS.name
_LINEAGE = m4_schema.EXECUTION_AUTHORITY_LINEAGE.name
_SELECTORS = m4_schema.ATTEMPT_TERMINAL_AUTHORITY.name
_SHADOW_TERMINAL = m4_schema.SHADOW_REQUEST_TERMINAL_HISTORY.name
_FALLBACK = m4_schema.SHADOW_ADMISSION_FALLBACK_HISTORY.name
_FALLBACK_PROGRESS = m4_schema.SHADOW_ADMISSION_FALLBACK_PROGRESS_HISTORY.name
_SETTLEMENT = m4_schema.SHADOW_SETTLEMENT_HISTORY.name

_LEASE_OWNER_COLUMNS = (
    'execution_owner',
    'execution_owner_sha256',
    'execution_owner_api_instance_id',
)
_SHADOW_PARENT_COLUMNS = (
    'execution_route',
    'private_fallback_reason',
    'private_fallback_evidence',
    'private_fallback_evidence_sha256',
)
_LEASE_OWNER_INDEX = ('uq_serve_ra_worker_registration_leases_execution_owner')

_OLD_LOCK_ORDER = (
    _POLICIES,
    _COHORTS,
    _HANDOFFS,
    _LEASES,
    _REFS,
    _SHADOW_PARENTS,
    _SHADOW_CHILDREN,
)

# Complete-catalog adoption uses one merged global schedule.  Do not append a
# 039 suffix to _OLD_LOCK_ORDER: doing so would acquire class 4/10 after 17.
_COMPLETE_LOCK_ORDER = (
    _POLICIES,
    _COHORTS,
    _HANDOFFS,
    _PROCESS,
    _LEASES,
    _REFS,
    _SHADOW_PARENTS,
    _SHADOW_CHILDREN,
    _SHADOW_EXECUTION,
    _GC_CURSOR,
    _LINEAGE,
    _SELECTORS,
    _SHADOW_TERMINAL,
    _FALLBACK,
    _FALLBACK_PROGRESS,
    _SETTLEMENT,
)

_CREATE_ORDER = (
    m4_schema.WORKER_PROCESS_SUPERSESSIONS,
    m4_schema.API_INSTANCE_GC_CURSORS,
    m4_schema.EXECUTION_AUTHORITY_LINEAGE,
    m4_schema.ATTEMPT_TERMINAL_AUTHORITY,
    m4_schema.SHADOW_EXECUTION_HISTORY,
    m4_schema.SHADOW_REQUEST_TERMINAL_HISTORY,
    m4_schema.SHADOW_ADMISSION_FALLBACK_HISTORY,
    m4_schema.SHADOW_ADMISSION_FALLBACK_PROGRESS_HISTORY,
    m4_schema.SHADOW_SETTLEMENT_HISTORY,
)


def _serve034_module() -> typing.Any:
    return importlib.import_module(
        'sky.schemas.db.serve_state.034_authority_release_ledger')


def _serve038_module() -> typing.Any:
    return importlib.import_module(
        'sky.schemas.db.serve_state.038_serve_resource_action_authority')


def _column_map(bind: sa.engine.Connection,
                table: str) -> dict[str, dict[str, typing.Any]]:
    return {
        str(column['name']): column
        for column in sa.inspect(bind).get_columns(table)
    }


def _index_names(bind: sa.engine.Connection, table: str) -> set[str]:
    return {
        str(index['name'])
        for index in sa.inspect(bind).get_indexes(table)
        if index['name'] is not None
    }


def _check_names(bind: sa.engine.Connection, table: str) -> set[str]:
    return {
        str(check['name'])
        for check in sa.inspect(bind).get_check_constraints(table)
        if check['name'] is not None
    }


def _has_rows(bind: sa.engine.Connection, table: str) -> bool:
    quoted = bind.dialect.identifier_preparer.quote(table)
    return bool(
        bind.execute(sa.text(
            f'SELECT EXISTS (SELECT 1 FROM {quoted} LIMIT 1)')).scalar_one())


def _verify_exact_table(bind: sa.engine.Connection, table: sa.Table) -> None:
    _serve038_module()._verify_exact_table(bind, table)


def _verify_038_catalog() -> None:
    """Verify the complete shipped 038 catalog without empty-state gates."""
    bind = op.get_bind()
    serve038 = _serve038_module()
    serve038._assert_common_relation_envelope(bind, 'services', post_038=True)
    serve038._assert_common_relation_envelope(bind,
                                              'version_specs',
                                              post_038=True)
    serve038._assert_common_relation_envelope(bind, 'replicas', post_038=True)
    for table in m4_schema.SERVE038_METADATA.sorted_tables:
        _verify_exact_table(bind, table)
    for table in m4_schema.SERVE038_ALTERED_RELATION_METADATA.sorted_tables:
        _verify_exact_table(bind, table)
    # These Serve033 relations are unchanged by 038 and are not part of the
    # three-table 038 altered metadata graph.
    _verify_exact_table(bind, resource_action_state_schema.SHADOW_SAMPLES)
    _verify_exact_table(bind, resource_action_state_schema.SHADOW_ATTEMPTS)
    _verify_exact_table(bind,
                        resource_action_state_schema.SHADOW_COVERAGE_ATTEMPTS)
    serve038._assert_serve034_catalog()
    serve038._assert_serve037_catalog(post_038=True)


def _verify_039_catalog() -> None:
    """Exact-reflect the final default-bearing Serve039 catalog."""
    bind = op.get_bind()
    serve038 = _serve038_module()
    serve038._assert_common_relation_envelope(bind, 'services', post_038=True)
    serve038._assert_common_relation_envelope(bind,
                                              'version_specs',
                                              post_038=True)
    serve038._assert_common_relation_envelope(bind, 'replicas', post_038=True)

    for table in m4_schema.SERVE038_METADATA.sorted_tables:
        if table.name != _LEASES:
            _verify_exact_table(bind, table)
    for table in m4_schema.SERVE038_ALTERED_RELATION_METADATA.sorted_tables:
        _verify_exact_table(bind, table)
    _verify_exact_table(bind,
                        resource_action_state_schema.SHADOW_COVERAGE_ATTEMPTS)
    for table in m4_schema.SERVE039_ALTERED_RELATION_TABLES:
        _verify_exact_table(bind, table)
    for table in m4_schema.SERVE039_METADATA.sorted_tables:
        _verify_exact_table(bind, table)
    serve038._assert_serve034_catalog()
    serve038._assert_serve037_catalog(post_038=True)


def _detect_catalog_mode() -> str:
    """Return ``038`` or ``complete_039``; reject every partial hybrid."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    new_presence = {
        table.name: inspector.has_table(table.name)
        for table in m4_schema.SERVE039_METADATA.sorted_tables
    }
    lease_columns = set(_column_map(bind, _LEASES))
    parent_columns = set(_column_map(bind, _SHADOW_PARENTS))
    lease_extension = set(_LEASE_OWNER_COLUMNS).intersection(lease_columns)
    parent_extension = set(_SHADOW_PARENT_COLUMNS).intersection(parent_columns)
    child_checks = _check_names(bind, _SHADOW_CHILDREN)
    new_child_check = bool({
        'serve039_shadow_child_execution_kind_ck',
        'serve039_shadow_child_phase_shape_ck',
    }.intersection(child_checks))
    owner_index = _LEASE_OWNER_INDEX in _index_names(bind, _LEASES)

    if (not any(new_presence.values()) and not lease_extension and
            not parent_extension and not new_child_check and not owner_index):
        _verify_038_catalog()
        return '038'

    if (not all(new_presence.values()) or
            lease_extension != set(_LEASE_OWNER_COLUMNS) or
            parent_extension != set(_SHADOW_PARENT_COLUMNS) or
            not new_child_check or not owner_index):
        raise RuntimeError(
            'SkyServe schema 039 refuses a partially installed catalog.')
    _verify_039_catalog()
    return 'complete_039'


def _lock_relations(relations: tuple[str, ...]) -> None:
    bind = op.get_bind()
    quote = bind.dialect.identifier_preparer.quote
    for relation in relations:
        bind.exec_driver_sql(
            f'LOCK TABLE {quote(relation)} IN ACCESS EXCLUSIVE MODE')


def _assert_old_stamp_adoptable() -> None:
    bind = op.get_bind()
    for table in m4_schema.SERVE039_METADATA.sorted_tables:
        if _has_rows(bind, table.name):
            raise RuntimeError(
                'SkyServe schema 039 cannot adopt nonempty pre-existing '
                f'relation {table.name!r}.')
    lease = bind.dialect.identifier_preparer.quote(_LEASES)
    owner_predicate = ' OR '.join(
        f'{bind.dialect.identifier_preparer.quote(column)} IS NOT NULL'
        for column in _LEASE_OWNER_COLUMNS)
    if bind.execute(
            sa.text(f'SELECT EXISTS (SELECT 1 FROM {lease} WHERE '
                    f'{owner_predicate} LIMIT 1)')).scalar_one():
        raise RuntimeError(
            'SkyServe schema 039 cannot adopt a lease execution owner at the '
            'old 038 stamp.')
    parent = bind.dialect.identifier_preparer.quote(_SHADOW_PARENTS)
    if bind.execute(
            sa.text(f'SELECT EXISTS (SELECT 1 FROM {parent} WHERE '
                    "execution_route <> 'LEGACY_CONTROLLER' OR "
                    'private_fallback_reason IS NOT NULL OR '
                    'private_fallback_evidence IS NOT NULL OR '
                    'private_fallback_evidence_sha256 IS NOT NULL '
                    'LIMIT 1)')).scalar_one():
        raise RuntimeError(
            'SkyServe schema 039 cannot adopt private or fallback shadow '
            'history at the old 038 stamp.')


def _drop_check_if_present(table: str, name: str) -> None:
    if name in _check_names(op.get_bind(), table):
        op.drop_constraint(name, table, type_='check')


def _add_lease_owner_contract() -> None:
    for column in m4_schema.worker_registration_lease_execution_owner_columns():
        op.add_column(_LEASES, column)
    _drop_check_if_present(_LEASES, 'serve038_worker_lease_closed_shape_ck')
    constraint = (
        m4_schema.worker_registration_lease_execution_owner_check_constraints()
        [0])
    op.create_check_constraint(str(constraint.name), _LEASES,
                               str(constraint.sqltext))
    op.create_index(
        _LEASE_OWNER_INDEX,
        _LEASES, ['execution_owner_api_instance_id'],
        unique=True,
        postgresql_where=sa.text('execution_owner_api_instance_id IS NOT NULL'))


def _add_shadow_parent_contract() -> None:
    columns = m4_schema.shadow_parent_execution_route_columns()
    # Install all columns nullable first.  The compatibility default is active
    # before the deterministic pre-039 legacy backfill and remains in 039.
    route = columns[0]
    op.add_column(
        _SHADOW_PARENTS,
        sa.Column(route.name,
                  route.type,
                  nullable=True,
                  server_default=route.server_default.arg))
    for column in columns[1:]:
        op.add_column(_SHADOW_PARENTS, column)
    quoted = op.get_bind().dialect.identifier_preparer.quote(_SHADOW_PARENTS)
    op.get_bind().execute(
        sa.text(f'UPDATE {quoted} SET '
                "execution_route = 'LEGACY_CONTROLLER', "
                'private_fallback_reason = NULL, '
                'private_fallback_evidence = NULL, '
                'private_fallback_evidence_sha256 = NULL'))
    constraint = m4_schema.shadow_parent_execution_route_check_constraints()[0]
    op.create_check_constraint(str(constraint.name), _SHADOW_PARENTS,
                               str(constraint.sqltext))
    op.alter_column(_SHADOW_PARENTS,
                    'execution_route',
                    existing_type=sa.Text(),
                    nullable=False,
                    existing_server_default='LEGACY_CONTROLLER')


def _replace_shadow_child_contract() -> None:
    _drop_check_if_present(_SHADOW_CHILDREN,
                           'ck_serve_ra_shadow_attempts_execution')
    _drop_check_if_present(_SHADOW_CHILDREN,
                           'ck_serve_ra_shadow_attempts_phase_shape')
    for constraint in m4_schema.shadow_child_execution_check_constraints():
        op.create_check_constraint(str(constraint.name), _SHADOW_CHILDREN,
                                   str(constraint.sqltext))


def _create_039_relations() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table in _CREATE_ORDER:
        if inspector.has_table(table.name):
            raise RuntimeError(
                'SkyServe schema 039 found an unexpected pre-existing '
                f'relation {table.name!r}.')
        table.create(bind, checkfirst=False)


def upgrade() -> None:
    """Install the PostgreSQL-only Serve039 historical authority catalog."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'SkyServe schema 039 is PostgreSQL-only and must not be stamped '
            f'on dialect {bind.dialect.name!r}.')
    current = op.get_context().get_current_revision()
    if current != '038':
        raise RuntimeError('SkyServe schema 039 requires the exact Serve038 '
                           f'head; found {current!r}.')

    mode = _detect_catalog_mode()
    _lock_relations(_OLD_LOCK_ORDER if mode == '038' else _COMPLETE_LOCK_ORDER)
    if mode == 'complete_039':
        _verify_039_catalog()
        _assert_old_stamp_adoptable()
        return

    _verify_038_catalog()
    _add_lease_owner_contract()
    _add_shadow_parent_contract()
    _replace_shadow_child_contract()
    _create_039_relations()
    _verify_039_catalog()


def downgrade() -> None:
    """Retain immutable execution history on application rollback."""
    raise RuntimeError(
        'SkyServe schema 039 is additive and cannot be downgraded. Roll back '
        'the application against the retained historical-authority catalog '
        'instead.')
