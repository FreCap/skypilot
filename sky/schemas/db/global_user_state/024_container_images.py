"""Add managed container image distribution state.

Revision ID: 024
Revises: 023
Create Date: 2026-07-13

"""
# pylint: disable=invalid-name
from collections.abc import Sequence
import time
import uuid

from alembic import op
import sqlalchemy

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '024'
down_revision: str | Sequence[str] | None = '023'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CATALOG_ROW_ID = 'authority'
_MIGRATION_LOCK = 'skypilot:global-user-state:024:container-images'
_TABLE_NAMES = (
    'container_image_catalog',
    'container_image_profile_revisions',
    'container_image_profile_custody',
    'container_image_operations',
    'container_images',
    'container_image_sources',
    'container_image_publications',
    'container_image_provider_budgets',
    'container_image_registry_shards',
    'container_image_locations',
    'container_image_demands',
    'container_image_consumer_watermarks',
    'container_image_workers',
)
_DROP_TABLE_NAMES = (
    'container_image_workers',
    'container_image_consumer_watermarks',
    'container_image_demands',
    'container_image_publications',
    'container_image_locations',
    'container_image_registry_shards',
    'container_image_provider_budgets',
    'container_image_sources',
    'container_images',
    'container_image_operations',
    'container_image_profile_custody',
    'container_image_profile_revisions',
    'container_image_catalog',
)
_CLUSTER_BINDING_COLUMN_NAMES = (
    'container_image_binding_known',
    'container_image_consumer_kind',
    'container_image_consumer_owner',
)
_PUBLICATION_HISTORY_INDEX = (
    'ix_container_image_publications_workspace_history')
_DEMAND_HISTORY_INDEX = 'ix_container_image_demands_artifact_history'
_CANARY_QUEUE_INDEX = 'ix_container_image_operations_canary_queue'
_CANARY_CLAIMABLE_EXPRESSION = (
    "CASE WHEN kind = 'PROFILE_CANARY' AND state = 'PENDING' THEN updated_at "
    "WHEN kind = 'PROFILE_CANARY' AND state = 'RUNNING' THEN "
    'GREATEST(lease_expires_at, updated_at) ELSE NULL END')
_PREVIEW_COMPATIBLE_INDEXES = (
    (_PUBLICATION_HISTORY_INDEX, 'container_image_publications',
     ('workspace', 'created_at', 'id'), None),
    ('ix_container_image_profile_qualification_queue',
     'container_image_profile_revisions', ('updated_at', 'id'),
     "state IN ('QUALIFYING', 'ACTIVE')"),
    (_CANARY_QUEUE_INDEX, 'container_image_operations',
     ('canary_claimable_at', 'id'), 'canary_claimable_at IS NOT NULL'),
    ('ix_container_image_locations_inventory_digest',
     'container_image_locations', ('shard_id', 'runtime_digest'), None),
    ('ix_container_image_publications_fanout', 'container_image_publications',
     ('canonical_location_id',
      'id'), "state = 'PENDING' AND canonical_location_id IS NOT NULL"),
    ('ix_container_image_publications_operation',
     'container_image_publications', ('operation_id',), None),
    ('ix_container_image_publications_workspace_state_history',
     'container_image_publications', ('workspace', 'state', 'created_at',
                                      'id'), None),
    ('ix_container_image_publications_workspace_release_history',
     'container_image_publications', ('workspace', 'requested_release',
                                      'created_at', 'id'), None),
    ('ix_container_image_publications_failed_reservation_expiry',
     'container_image_publications', ('reservation_expires_at', 'id'),
     "state = 'FAILED' AND reservation_active IS TRUE"),
    ('ix_container_image_publications_terminal_expiry',
     'container_image_publications', ('record_expires_at', 'id'),
     'reservation_active IS FALSE AND record_expires_at IS NOT NULL'),
    ('ix_container_image_publications_ready_history',
     'container_image_publications', ('image_id', 'updated_at', 'id'),
     "state = 'READY' AND reservation_active IS TRUE"),
    (_DEMAND_HISTORY_INDEX, 'container_image_demands',
     ('workspace', 'image_id', 'created_at', 'id'), None),
    ('ix_container_image_demands_reconciliation_queue',
     'container_image_demands', ('updated_at', 'id'),
     "state IN ('WARMING', 'READY', 'FAILED')"),
    ('ix_container_image_demands_compaction_queue', 'container_image_demands',
     ('expires_at',
      'id'), "state IN ('SUPERSEDED', 'RELEASED') AND expires_at IS NOT NULL"),
    ('ix_container_image_workers_heartbeat', 'container_image_workers',
     ('heartbeat_at', 'id'), None),
)

_SCHEMA_SHAPE_QUERIES = {
    # Compare the visible column order while ignoring attnum holes left by a
    # preview database that dropped a superseded column before convergence.
    'columns': """SELECT table_name, column_name,
                         row_number() OVER (
                             PARTITION BY table_name
                             ORDER BY ordinal_position),
                         data_type, udt_schema, udt_name, is_nullable,
                         column_default, is_generated, generation_expression,
                         is_identity, identity_generation, collation_name,
                         character_maximum_length, numeric_precision,
                         numeric_scale
                  FROM information_schema.columns
                  WHERE table_schema = :schema
                    AND table_name = ANY(CAST(:table_names AS TEXT[]))
                  ORDER BY table_name, ordinal_position""",
    'constraints': """SELECT relation.relname, constraint_row.conname,
                             constraint_row.contype,
                             constraint_row.convalidated,
                             constraint_row.condeferrable,
                             constraint_row.condeferred,
                             pg_get_constraintdef(constraint_row.oid, false)
                      FROM pg_constraint AS constraint_row
                      JOIN pg_class AS relation
                        ON relation.oid = constraint_row.conrelid
                      JOIN pg_namespace AS namespace
                        ON namespace.oid = relation.relnamespace
                      WHERE namespace.nspname = :schema
                        AND relation.relname = ANY(
                            CAST(:table_names AS TEXT[]))
                      ORDER BY relation.relname, constraint_row.conname""",
    'indexes': """SELECT table_relation.relname, index_relation.relname,
                         index_row.indisunique, index_row.indisprimary,
                         index_row.indisvalid, index_row.indisready,
                         index_row.indislive,
                         pg_get_indexdef(index_relation.oid, 0, false)
                  FROM pg_index AS index_row
                  JOIN pg_class AS table_relation
                    ON table_relation.oid = index_row.indrelid
                  JOIN pg_class AS index_relation
                    ON index_relation.oid = index_row.indexrelid
                  JOIN pg_namespace AS namespace
                    ON namespace.oid = table_relation.relnamespace
                  WHERE namespace.nspname = :schema
                    AND table_relation.relname = ANY(
                        CAST(:table_names AS TEXT[]))
                  ORDER BY table_relation.relname, index_relation.relname""",
}

_CLUSTER_BINDING_SHAPE_QUERY = """SELECT column_name, data_type, udt_schema,
                         udt_name, is_nullable, column_default, is_generated,
                         generation_expression, is_identity,
                         identity_generation, collation_name,
                         character_maximum_length, numeric_precision,
                         numeric_scale
                  FROM information_schema.columns
                  WHERE table_schema = :schema
                    AND table_name = 'clusters'
                    AND column_name = ANY(CAST(:column_names AS TEXT[]))
                  ORDER BY column_name"""


def _lock_migration() -> None:
    op.execute(
        sqlalchemy.text('SELECT pg_advisory_xact_lock(hashtext(:name))').
        bindparams(name=_MIGRATION_LOCK))


def _ensure_auth_sessions_table(bind: sqlalchemy.engine.Connection) -> None:
    """Converge databases stamped by the former image revision 023."""
    if sqlalchemy.inspect(bind).has_table('auth_sessions'):
        return
    op.create_table(
        'auth_sessions',
        sqlalchemy.Column('code_challenge', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('token', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('created_at', sqlalchemy.Float, nullable=False),
    )


def _add_cluster_binding_columns(bind: sqlalchemy.engine.Connection) -> None:
    existing_columns = {
        column['name']
        for column in sqlalchemy.inspect(bind).get_columns('clusters')
    }
    columns = (
        ('container_image_binding_known', sqlalchemy.Integer(), '0'),
        ('container_image_consumer_kind', sqlalchemy.Text(), None),
        ('container_image_consumer_owner', sqlalchemy.Text(), None),
    )
    for column_name, column_type, server_default in columns:
        if column_name in existing_columns:
            continue
        db_utils.add_column_to_table_alembic('clusters',
                                             column_name,
                                             column_type,
                                             server_default=server_default)


def _drop_cluster_binding_columns() -> None:
    db_utils.drop_column_from_table_alembic('clusters',
                                            'container_image_consumer_owner')
    db_utils.drop_column_from_table_alembic('clusters',
                                            'container_image_consumer_kind')
    db_utils.drop_column_from_table_alembic('clusters',
                                            'container_image_binding_known')


def _cluster_binding_shape(
    bind: sqlalchemy.engine.Connection,
    schema_name: str,
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        tuple(row) for row in bind.execute(
            sqlalchemy.text(_CLUSTER_BINDING_SHAPE_QUERY), {
                'schema': schema_name,
                'column_names': list(_CLUSTER_BINDING_COLUMN_NAMES),
            }).all())


def _validate_cluster_binding_columns(bind: sqlalchemy.engine.Connection,
                                      schema_name: str) -> None:
    """Compares adopted cluster columns with the migration's literal DDL."""
    actual = _cluster_binding_shape(bind, schema_name)
    original_search_path = bind.execute(
        sqlalchemy.text("SELECT current_setting('search_path')")).scalar_one()
    reference_schema = None
    try:
        bind.execute(
            sqlalchemy.text(
                "SELECT set_config('search_path', 'pg_temp', true)"))
        bind.exec_driver_sql('CREATE TABLE clusters (name TEXT PRIMARY KEY)')
        _add_cluster_binding_columns(bind)
        reference_schema = bind.execute(
            sqlalchemy.text("""SELECT namespace.nspname
                 FROM pg_namespace AS namespace
                 WHERE namespace.oid = pg_my_temp_schema()""")).scalar_one()
        expected = _cluster_binding_shape(bind, str(reference_schema))
    finally:
        bind.execute(
            sqlalchemy.text("SELECT set_config('search_path', :path, true)"),
            {'path': original_search_path})
        if reference_schema is not None:
            qualified = _qualified_name(bind, str(reference_schema), 'clusters')
            bind.exec_driver_sql(f'DROP TABLE IF EXISTS {qualified} CASCADE')
    if expected != actual:
        raise RuntimeError(
            'Migration 024 found structurally incompatible cluster binding '
            f'columns: expected={expected!r}; actual={actual!r}.')


def _normalize_schema_reference(value: object, schema_name: str) -> object:
    if not isinstance(value, str):
        return value
    quoted_schema = f'"{schema_name.replace(chr(34), chr(34) * 2)}"'
    normalized = value.replace(f'{quoted_schema}.',
                               '<schema>.').replace(f'{schema_name}.',
                                                    '<schema>.')
    if schema_name.startswith('pg_temp_'):
        normalized = normalized.replace('"pg_temp".', '<schema>.').replace(
            'pg_temp.', '<schema>.')
    return normalized


def _schema_shape(
    bind: sqlalchemy.engine.Connection,
    schema_name: str,
) -> dict[str, tuple[tuple[object, ...], ...]]:
    params = {
        'schema': schema_name,
        'table_names': list(_TABLE_NAMES),
    }
    return {
        section: tuple(
            tuple(
                _normalize_schema_reference(value, schema_name)
                for value in row)
            for row in bind.execute(sqlalchemy.text(query), params).all()
        ) for section, query in _SCHEMA_SHAPE_QUERIES.items()
    }


def _schema_mismatch_details(
    expected: dict[str, tuple[tuple[object, ...], ...]],
    actual: dict[str, tuple[tuple[object, ...], ...]],
) -> str:
    details = []
    for section in _SCHEMA_SHAPE_QUERIES:
        expected_rows = {tuple(row[:2]): row for row in expected[section]}
        actual_rows = {tuple(row[:2]): row for row in actual[section]}
        missing = sorted(set(expected_rows).difference(actual_rows))
        unexpected = sorted(set(actual_rows).difference(expected_rows))
        changed = sorted(
            key for key in set(expected_rows).intersection(actual_rows)
            if expected_rows[key] != actual_rows[key])
        parts = []
        for label, keys in (('missing', missing), ('unexpected', unexpected),
                            ('changed', changed)):
            if keys:
                rendered = ','.join(
                    '.'.join(str(value) for value in key) for key in keys)
                parts.append(f'{label}={rendered}')
        if changed:
            first = changed[0]
            parts.append(f'first_changed_expected={expected_rows[first]!r}')
            parts.append(f'first_changed_actual={actual_rows[first]!r}')
        if parts:
            details.append(f'{section}[{"; ".join(parts)}]')
    return '; '.join(details)


def _qualified_name(bind: sqlalchemy.engine.Connection, schema_name: str,
                    table_name: str) -> str:
    preparer = bind.dialect.identifier_preparer
    return (f'{preparer.quote_schema(schema_name)}.'
            f'{preparer.quote(table_name)}')


def _upgrade_preview_publication_operation_link(
        bind: sqlalchemy.engine.Connection, schema_name: str) -> None:
    """Converges only the shipped preview FK shape to bounded retention."""
    inspector = sqlalchemy.inspect(bind)
    columns = {
        column['name']: column
        for column in inspector.get_columns('container_image_publications',
                                            schema=schema_name)
    }
    matching = [
        foreign_key for foreign_key in inspector.get_foreign_keys(
            'container_image_publications', schema=schema_name)
        if foreign_key.get('constrained_columns') == ['operation_id']
    ]
    if len(matching) != 1 or 'operation_id' not in columns:
        return
    foreign_key = matching[0]
    ondelete = str(foreign_key.get('options', {}).get('ondelete', '')).upper()
    nullable = bool(columns['operation_id']['nullable'])
    if nullable and ondelete == 'SET NULL':
        return
    old_shape = (not nullable and not ondelete and
                 foreign_key.get('referred_table')
                 == 'container_image_operations' and
                 foreign_key.get('referred_columns') == ['id'] and
                 foreign_key.get('name') is not None)
    if not old_shape:
        return
    constraint_name = str(foreign_key['name'])
    op.drop_constraint(constraint_name,
                       'container_image_publications',
                       schema=schema_name,
                       type_='foreignkey')
    op.alter_column('container_image_publications',
                    'operation_id',
                    schema=schema_name,
                    existing_type=sqlalchemy.Text(),
                    nullable=True)
    op.create_foreign_key(constraint_name,
                          'container_image_publications',
                          'container_image_operations', ['operation_id'],
                          ['id'],
                          source_schema=schema_name,
                          ondelete='SET NULL')


def _upgrade_preview_canary_queue(bind: sqlalchemy.engine.Connection,
                                  schema_name: str) -> None:
    """Adds the generated canary due-time projection to an older preview."""
    inspector = sqlalchemy.inspect(bind)
    columns = {
        column['name']
        for column in inspector.get_columns('container_image_operations',
                                            schema=schema_name)
    }
    if 'canary_claimable_at' in columns:
        return
    indexes = {
        index['name']
        for index in inspector.get_indexes('container_image_operations',
                                           schema=schema_name)
    }
    if _CANARY_QUEUE_INDEX in indexes:
        op.drop_index(_CANARY_QUEUE_INDEX,
                      table_name='container_image_operations',
                      schema=schema_name)
    op.add_column('container_image_operations',
                  sqlalchemy.Column(
                      'canary_claimable_at', sqlalchemy.BigInteger,
                      sqlalchemy.Computed(_CANARY_CLAIMABLE_EXPRESSION,
                                          persisted=True)),
                  schema=schema_name)


def _backfill_and_validate_profile_custody(bind: sqlalchemy.engine.Connection,
                                           schema_name: str) -> None:
    """Adopts READY preview history into one immutable custody row per key."""
    publications = _qualified_name(bind, schema_name,
                                   'container_image_publications')
    revisions = _qualified_name(bind, schema_name,
                                'container_image_profile_revisions')
    custody = _qualified_name(bind, schema_name,
                              'container_image_profile_custody')
    conflict = bind.execute(
        sqlalchemy.text(f"""
            SELECT revision.workspace, revision.profile
            FROM {publications} AS publication
            JOIN {revisions} AS revision
              ON revision.id = publication.profile_revision_id
            WHERE publication.state = 'READY'
            GROUP BY revision.workspace, revision.profile
            HAVING count(DISTINCT revision.physical_manifest_hash) > 1
            LIMIT 1
        """)).first()
    if conflict is not None:
        raise RuntimeError(
            'Migration 024 preview adoption found conflicting canonical '
            'physical custody in READY publication history.')
    bind.execute(
        sqlalchemy.text(f"""
            INSERT INTO {custody} (
                workspace, profile, physical_manifest_hash,
                first_profile_revision_id, acquired_at
            )
            SELECT first_ready.workspace, first_ready.profile,
                   first_ready.physical_manifest_hash,
                   first_ready.profile_revision_id, first_ready.acquired_at
            FROM (
                SELECT DISTINCT ON (revision.workspace, revision.profile)
                       revision.workspace, revision.profile,
                       revision.physical_manifest_hash,
                       revision.id AS profile_revision_id,
                       publication.updated_at AS acquired_at
                FROM {publications} AS publication
                JOIN {revisions} AS revision
                  ON revision.id = publication.profile_revision_id
                WHERE publication.state = 'READY'
                ORDER BY revision.workspace, revision.profile,
                         publication.updated_at, publication.id
            ) AS first_ready
            ON CONFLICT (workspace, profile) DO NOTHING
        """))
    mismatch = bind.execute(
        sqlalchemy.text(f"""
            SELECT 1
            FROM {publications} AS publication
            JOIN {revisions} AS revision
              ON revision.id = publication.profile_revision_id
            JOIN {custody} AS marker
              ON marker.workspace = revision.workspace
             AND marker.profile = revision.profile
            WHERE publication.state = 'READY'
              AND marker.physical_manifest_hash <>
                  revision.physical_manifest_hash
            LIMIT 1
        """)).first()
    invalid_marker = bind.execute(
        sqlalchemy.text(f"""
            SELECT 1
            FROM {custody} AS marker
            JOIN {revisions} AS revision
              ON revision.id = marker.first_profile_revision_id
            WHERE marker.workspace <> revision.workspace
               OR marker.profile <> revision.profile
               OR marker.physical_manifest_hash <>
                  revision.physical_manifest_hash
            LIMIT 1
        """)).first()
    if mismatch is not None or invalid_marker is not None:
        raise RuntimeError(
            'Migration 024 preview adoption found an invalid canonical '
            'profile custody marker.')


def _validate_catalog_singleton(bind: sqlalchemy.engine.Connection,
                                schema_name: str) -> None:
    catalog_name = _qualified_name(bind, schema_name, 'container_image_catalog')
    rows = bind.execute(
        sqlalchemy.text(
            f'SELECT id, authority_id, created_at FROM {catalog_name}')).all()
    valid = (len(rows) == 1 and rows[0][0] == _CATALOG_ROW_ID and
             bool(rows[0][1]) and isinstance(rows[0][2], int) and
             rows[0][2] > 0)
    if valid:
        try:
            uuid.UUID(str(rows[0][1]))
        except ValueError:
            valid = False
    if not valid:
        raise RuntimeError(
            'Migration 024 preview adoption requires exactly one catalog '
            'authority row with the expected ID, a UUID authority, and a '
            'positive creation time.')


def _drop_reference_tables(bind: sqlalchemy.engine.Connection,
                           schema_name: str) -> None:
    for table_name in _DROP_TABLE_NAMES:
        qualified = _qualified_name(bind, schema_name, table_name)
        bind.exec_driver_sql(f'DROP TABLE IF EXISTS {qualified} CASCADE')


def _validate_preview_schema(bind: sqlalchemy.engine.Connection,
                             schema_name: str) -> None:
    """Compares preview state with this migration's literal PostgreSQL DDL."""
    actual = _schema_shape(bind, schema_name)
    original_search_path = bind.execute(
        sqlalchemy.text("SELECT current_setting('search_path')")).scalar_one()
    reference_schema = None
    try:
        bind.execute(
            sqlalchemy.text(
                "SELECT set_config('search_path', 'pg_temp', true)"))
        # Unqualified CREATE TABLE in pg_temp creates transaction-local
        # temporary tables. This needs no database-level CREATE SCHEMA grant.
        _create_tables()
        reference_schema = bind.execute(
            sqlalchemy.text("""SELECT namespace.nspname
                 FROM pg_namespace AS namespace
                 WHERE namespace.oid = pg_my_temp_schema()""")).scalar_one()
        expected = _schema_shape(bind, str(reference_schema))
    finally:
        bind.execute(
            sqlalchemy.text("SELECT set_config('search_path', :path, true)"),
            {'path': original_search_path})
        if reference_schema is not None:
            _drop_reference_tables(bind, str(reference_schema))
    mismatch = _schema_mismatch_details(expected, actual)
    if mismatch:
        raise RuntimeError(
            'Migration 024 found structurally incompatible managed image '
            f'preview state: {mismatch}.')
    _validate_catalog_singleton(bind, schema_name)


def _ensure_preview_compatible_indexes(bind: sqlalchemy.engine.Connection,
                                       schema_name: str) -> None:
    """Adds only known post-preview indexes before exact shape validation."""
    inspector = sqlalchemy.inspect(bind)
    existing_by_table = {
        table_name: {
            index['name']
            for index in inspector.get_indexes(table_name, schema=schema_name)
        } for table_name in
        {definition[1] for definition in _PREVIEW_COMPATIBLE_INDEXES}
    }
    for name, table_name, columns, predicate in _PREVIEW_COMPATIBLE_INDEXES:
        if name in existing_by_table[table_name]:
            # Exact shape validation below rejects a same-name malformed index.
            continue
        op.create_index(name,
                        table_name,
                        list(columns),
                        schema=schema_name,
                        postgresql_where=(sqlalchemy.text(predicate)
                                          if predicate is not None else None))


def _create_profile_custody_table(schema_name: str | None = None) -> None:
    op.create_table(
        'container_image_profile_custody',
        sqlalchemy.Column('workspace', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('profile', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('physical_manifest_hash',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column(
            'first_profile_revision_id',
            sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_profile_revisions.id'),
            nullable=False),
        sqlalchemy.Column('acquired_at', sqlalchemy.BigInteger, nullable=False),
        schema=schema_name,
    )


def _create_tables() -> None:
    op.create_table(
        'container_image_catalog',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('authority_id',
                          sqlalchemy.Text,
                          nullable=False,
                          unique=True),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
    )
    op.create_table(
        'container_image_profile_revisions',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('profile', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('revision', sqlalchemy.Integer, nullable=False),
        sqlalchemy.Column('desired_generation',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('config_hash', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('config_json', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('terraform_hash', sqlalchemy.Text),
        sqlalchemy.Column('physical_manifest_hash',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('attestations_json',
                          sqlalchemy.Text,
                          nullable=False,
                          server_default='{}'),
        sqlalchemy.Column('attestations_hash', sqlalchemy.Text),
        sqlalchemy.Column('qualified_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('failed_code', sqlalchemy.Text),
        sqlalchemy.Column('canary_window_day', sqlalchemy.Text),
        sqlalchemy.Column('canary_reserved_microusd',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('max_daily_canary_microusd',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint('workspace',
                                    'profile',
                                    'revision',
                                    name='uq_container_image_profile_revision'),
        sqlalchemy.UniqueConstraint(
            'workspace',
            'profile',
            'desired_generation',
            name='uq_container_image_profile_generation'),
        sqlalchemy.CheckConstraint(
            "state IN ('QUALIFYING', 'ACTIVE', 'FAILED', 'SUPERSEDED', "
            "'RETIRED')",
            name='ck_container_image_profile_revision_state'),
        sqlalchemy.CheckConstraint('revision > 0 AND desired_generation > 0',
                                   name='ck_container_image_profile_positive'),
        sqlalchemy.CheckConstraint(
            'canary_reserved_microusd >= 0 AND '
            'max_daily_canary_microusd >= 0 AND '
            'canary_reserved_microusd <= max_daily_canary_microusd',
            name='ck_container_image_profile_canary_budget'),
    )
    op.create_index('uq_container_image_profile_desired',
                    'container_image_profile_revisions',
                    ['workspace', 'profile'],
                    unique=True,
                    postgresql_where=sqlalchemy.text("state = 'QUALIFYING'"))
    op.create_index('uq_container_image_profile_active',
                    'container_image_profile_revisions',
                    ['workspace', 'profile'],
                    unique=True,
                    postgresql_where=sqlalchemy.text("state = 'ACTIVE'"))
    op.create_index('ix_container_image_profile_state',
                    'container_image_profile_revisions',
                    ['state', 'updated_at', 'id'])
    op.create_index(
        'ix_container_image_profile_qualification_queue',
        'container_image_profile_revisions', ['updated_at', 'id'],
        postgresql_where=sqlalchemy.text("state IN ('QUALIFYING', 'ACTIVE')"))
    op.create_index('ix_container_image_profile_history',
                    'container_image_profile_revisions',
                    ['workspace', 'created_at', 'id'])

    _create_profile_custody_table()

    op.create_table(
        'container_image_operations',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('authority_id', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('scope', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('actor_hash', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('kind', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('idempotency_key', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('request_hash', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('result_kind', sqlalchemy.Text),
        sqlalchemy.Column('result_id', sqlalchemy.Text),
        sqlalchemy.Column('result_json', sqlalchemy.Text),
        sqlalchemy.Column('error_code', sqlalchemy.Text),
        sqlalchemy.Column('lease_token', sqlalchemy.Text),
        sqlalchemy.Column('lease_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('child_launch_id', sqlalchemy.Text),
        sqlalchemy.Column('teardown_deadline', sqlalchemy.BigInteger),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('terminal_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column(
            'canary_claimable_at', sqlalchemy.BigInteger,
            sqlalchemy.Computed(_CANARY_CLAIMABLE_EXPRESSION, persisted=True)),
        sqlalchemy.UniqueConstraint(
            'authority_id',
            'scope',
            'actor_hash',
            'kind',
            'idempotency_key',
            name='uq_container_image_operation_idempotency'),
        sqlalchemy.CheckConstraint(
            "state IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')",
            name='ck_container_image_operation_state'),
        sqlalchemy.CheckConstraint(
            'length(idempotency_key) BETWEEN 16 AND 128',
            name='ck_container_image_operation_key_length'),
        sqlalchemy.CheckConstraint(
            "(kind = 'PROFILE_CANARY' AND state = 'RUNNING' AND lease_token "
            "IS NOT NULL AND lease_expires_at IS NOT NULL AND "
            "teardown_deadline IS NOT NULL) OR NOT (lease_token IS NOT NULL "
            "OR lease_expires_at IS NOT NULL OR child_launch_id IS NOT NULL "
            "OR teardown_deadline IS NOT NULL)",
            name='ck_container_image_operation_canary_lease'),
        sqlalchemy.CheckConstraint(
            "(state IN ('SUCCEEDED', 'FAILED') AND terminal_expires_at IS NOT "
            "NULL) OR (state IN ('PENDING', 'RUNNING') AND "
            "terminal_expires_at IS NULL)",
            name='ck_container_image_operation_terminal_expiry'),
    )
    op.create_index('ix_container_image_operations_lookup',
                    'container_image_operations', ['scope', 'updated_at', 'id'])
    op.create_index(
        _CANARY_QUEUE_INDEX,
        'container_image_operations', ['canary_claimable_at', 'id'],
        postgresql_where=sqlalchemy.text('canary_claimable_at IS NOT NULL'))
    op.create_index(
        'ix_container_image_operations_expiry',
        'container_image_operations', ['terminal_expires_at', 'id'],
        postgresql_where=sqlalchemy.text('terminal_expires_at IS NOT NULL'))

    op.create_table(
        'container_images',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('runtime_digest', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('platform', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('config_digest', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('manifest_media_type',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('manifest_size_bytes',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('declared_size_bytes',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('creator_user_hash', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('producer_kind', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('producer_spec_hash', sqlalchemy.Text),
        sqlalchemy.Column('builder_version', sqlalchemy.Text),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint(
            'workspace',
            'runtime_digest',
            'platform',
            name='uq_container_images_runtime_identity'),
        sqlalchemy.CheckConstraint(
            'manifest_size_bytes >= 0 AND declared_size_bytes >= 0',
            name='ck_container_images_nonnegative_sizes'),
    )
    op.create_index('ix_container_images_workspace_created', 'container_images',
                    ['workspace', 'created_at', 'id'])

    op.create_table(
        'container_image_sources',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('image_id',
                          sqlalchemy.Text,
                          sqlalchemy.ForeignKey('container_images.id'),
                          nullable=False),
        sqlalchemy.Column('source_ref', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('source_root_digest', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('source_root_media_type',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('requested_platform', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('selected_child_digest',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('source_auth_binding_id', sqlalchemy.Text),
        sqlalchemy.Column('source_auth_fingerprint', sqlalchemy.Text),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint('workspace',
                                    'source_ref',
                                    'requested_platform',
                                    name='uq_container_image_source_selection'),
    )
    op.create_index('ix_container_image_sources_image',
                    'container_image_sources', ['image_id', 'created_at', 'id'])

    op.create_table(
        'container_image_provider_budgets',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('provider', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('partition', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('account', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('region', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('api_family', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('applied_rate_milli',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('burst_milli', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('tokens_milli', sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('refilled_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('blocked_until', sqlalchemy.BigInteger),
        sqlalchemy.Column('throttle_count',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint('provider',
                                    'partition',
                                    'account',
                                    'region',
                                    'api_family',
                                    name='uq_container_image_provider_budget'),
        sqlalchemy.CheckConstraint(
            'applied_rate_milli > 0 AND burst_milli > 0 AND tokens_milli >= 0 '
            'AND tokens_milli <= burst_milli',
            name='ck_container_image_provider_budget_tokens'),
    )
    op.create_index('ix_container_image_provider_budgets_blocked',
                    'container_image_provider_budgets', ['blocked_until', 'id'])

    op.create_table(
        'container_image_registry_shards',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('profile', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column(
            'profile_revision_id', sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_profile_revisions.id')),
        sqlalchemy.Column('target_id', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('provider', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('partition', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('account', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('region', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('shard_generation',
                          sqlalchemy.Integer,
                          nullable=False),
        sqlalchemy.Column('shard_index', sqlalchemy.Integer, nullable=False),
        sqlalchemy.Column('target_fingerprint', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('physical_fingerprint',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('eviction_enabled',
                          sqlalchemy.Boolean,
                          nullable=False),
        sqlalchemy.Column('registry', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('repository_name', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('repository_arn', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('max_manifests',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('max_declared_bytes',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('reserved_manifests',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('reserved_declared_bytes',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('observed_manifests',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('max_in_flight', sqlalchemy.Integer, nullable=False),
        sqlalchemy.Column('in_flight',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('qualified_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('last_dispatch_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('copy_next_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('inventory_epoch',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('inventory_cursor', sqlalchemy.Text),
        sqlalchemy.Column('inventory_started_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('inventory_completed_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('inventory_finalizing',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()),
        sqlalchemy.Column('inventory_lease_token', sqlalchemy.Text),
        sqlalchemy.Column('inventory_lease_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('inventory_interval_seconds',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='600'),
        sqlalchemy.Column('inventory_next_at',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint(
            'workspace',
            'profile',
            'target_id',
            'shard_generation',
            'shard_index',
            name='uq_container_image_registry_shard_slot'),
        sqlalchemy.UniqueConstraint(
            'physical_fingerprint',
            name='uq_container_image_registry_physical'),
        sqlalchemy.CheckConstraint(
            "state IN ('PENDING', 'READY', 'FULL', 'DRIFTED', 'DISABLED')",
            name='ck_container_image_registry_shard_state'),
        sqlalchemy.CheckConstraint(
            'shard_generation >= 0 AND shard_index >= 0 AND max_manifests > 0 '
            'AND max_declared_bytes > 0 AND reserved_manifests >= 0 AND '
            'reserved_manifests <= max_manifests AND '
            'reserved_declared_bytes >= 0 AND '
            'reserved_declared_bytes <= max_declared_bytes AND '
            'observed_manifests >= 0 AND max_in_flight > 0 AND in_flight >= 0 '
            'AND in_flight <= max_in_flight',
            name='ck_container_image_registry_shard_capacity'),
        sqlalchemy.CheckConstraint(
            '(inventory_lease_token IS NULL AND inventory_lease_expires_at IS '
            'NULL) OR (inventory_lease_token IS NOT NULL AND '
            'inventory_lease_expires_at IS NOT NULL)',
            name='ck_container_image_registry_inventory_lease'),
        sqlalchemy.CheckConstraint(
            'inventory_finalizing IS FALSE OR inventory_completed_at IS NOT '
            'NULL',
            name='ck_container_image_registry_inventory_finalizing'),
        sqlalchemy.CheckConstraint(
            'inventory_interval_seconds > 0 AND inventory_next_at >= 0',
            name='ck_container_image_registry_inventory_schedule'),
    )
    op.create_index('ix_container_image_registry_shard_dispatch',
                    'container_image_registry_shards', [
                        'workspace', 'profile', 'target_id', 'state',
                        'last_dispatch_at', 'id'
                    ])
    op.create_index(
        'ix_container_image_registry_shard_copy_queue',
        'container_image_registry_shards', ['copy_next_at', 'id'],
        postgresql_where=sqlalchemy.text('copy_next_at IS NOT NULL'))
    op.create_index('ix_container_image_registry_shard_inventory',
                    'container_image_registry_shards', [
                        sqlalchemy.text('inventory_finalizing DESC'),
                        'inventory_next_at', 'id'
                    ],
                    postgresql_where=sqlalchemy.text(
                        "state IN ('PENDING', 'READY', 'FULL', 'DRIFTED')"))

    op.create_table(
        'container_image_locations',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('image_id',
                          sqlalchemy.Text,
                          sqlalchemy.ForeignKey('container_images.id'),
                          nullable=False),
        sqlalchemy.Column(
            'shard_id',
            sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_registry_shards.id'),
            nullable=False),
        sqlalchemy.Column('target_fingerprint', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('physical_fingerprint',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('runtime_digest', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('canonical', sqlalchemy.Boolean, nullable=False),
        sqlalchemy.Column(
            'canonical_location_id', sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_locations.id')),
        sqlalchemy.Column('target_ref', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('lease_kind', sqlalchemy.Text),
        sqlalchemy.Column('lease_token', sqlalchemy.Text),
        sqlalchemy.Column('lease_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('attempt_count',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('next_retry_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('error_code', sqlalchemy.Text),
        sqlalchemy.Column('last_verified_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('last_used_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('inventory_epoch_seen', sqlalchemy.BigInteger),
        sqlalchemy.Column(
            'copy_claimable_at', sqlalchemy.BigInteger,
            sqlalchemy.Computed(
                "CASE WHEN state = 'PENDING' THEN COALESCE(next_retry_at, "
                "updated_at) WHEN state IN ('COPYING', 'VERIFYING') THEN "
                'lease_expires_at ELSE NULL END',
                persisted=True)),
        sqlalchemy.Column('reserved_declared_bytes',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint('image_id',
                                    'target_fingerprint',
                                    'runtime_digest',
                                    name='uq_container_image_location_target'),
        sqlalchemy.UniqueConstraint(
            'shard_id',
            'target_ref',
            name='uq_container_image_location_target_ref'),
        sqlalchemy.CheckConstraint(
            "state IN ('PENDING', 'COPYING', 'VERIFYING', 'READY', 'FAILED', "
            "'MISSING', 'EVICTING', 'EVICTED', 'QUARANTINED')",
            name='ck_container_image_location_state'),
        sqlalchemy.CheckConstraint(
            '(canonical IS TRUE AND canonical_location_id IS NULL) OR '
            '(canonical IS FALSE AND canonical_location_id IS NOT NULL)',
            name='ck_container_image_location_canonical_relation'),
        sqlalchemy.CheckConstraint(
            "(state IN ('COPYING', 'VERIFYING', 'EVICTING') AND lease_kind IS "
            "NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT "
            "NULL) OR (state NOT IN ('COPYING', 'VERIFYING', 'EVICTING') AND "
            "lease_kind IS NULL AND lease_token IS NULL AND lease_expires_at "
            "IS NULL)",
            name='ck_container_image_location_lease'),
        sqlalchemy.CheckConstraint(
            "lease_kind IS NULL OR lease_kind IN ('COPY', 'VERIFY', 'EVICT', "
            "'DELETE', 'READBACK')",
            name='ck_container_image_location_lease_kind'),
        sqlalchemy.CheckConstraint(
            "canonical IS FALSE OR state NOT IN ('EVICTING', 'EVICTED', "
            "'QUARANTINED')",
            name='ck_container_image_location_canonical_permanent'),
        sqlalchemy.CheckConstraint(
            'attempt_count >= 0 AND reserved_declared_bytes >= 0',
            name='ck_container_image_location_nonnegative'),
    )
    op.create_index('ix_container_image_locations_copy_pending',
                    'container_image_locations',
                    ['shard_id', 'copy_claimable_at', 'id'],
                    postgresql_where=sqlalchemy.text("state = 'PENDING'"))
    op.create_index(
        'ix_container_image_locations_copy_recovery',
        'container_image_locations', ['shard_id', 'copy_claimable_at', 'id'],
        postgresql_where=sqlalchemy.text("state IN ('COPYING', 'VERIFYING')"))
    op.create_index('ix_container_image_locations_shard_readiness',
                    'container_image_locations',
                    ['shard_id', 'state', 'updated_at', 'id'])
    op.create_index('ix_container_image_locations_inventory_digest',
                    'container_image_locations', ['shard_id', 'runtime_digest'])
    op.create_index(
        'ix_container_image_locations_inventory_confirmation',
        'container_image_locations',
        ['shard_id', 'last_verified_at', 'id', 'inventory_epoch_seen'],
        postgresql_where=sqlalchemy.text(
            "state = 'READY' AND last_verified_at IS NOT NULL"))
    op.create_index(
        'ix_container_image_locations_eviction',
        'container_image_locations', [
            'shard_id', 'state',
            sqlalchemy.text(
                'COALESCE(last_used_at, last_verified_at, created_at)'), 'id'
        ],
        postgresql_where=sqlalchemy.text('canonical IS FALSE'))
    op.create_index('ix_container_image_locations_canonical',
                    'container_image_locations',
                    ['canonical_location_id', 'state', 'id'])
    op.create_index('ix_container_image_locations_artifact',
                    'container_image_locations',
                    ['image_id', 'created_at', 'id'])
    op.create_index('ix_container_image_locations_failed_canonical',
                    'container_image_locations', ['updated_at', 'id'],
                    postgresql_where=sqlalchemy.text(
                        "canonical IS TRUE AND state = 'FAILED'"))

    op.create_table(
        'container_image_publications',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column(
            'operation_id', sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_operations.id',
                                  ondelete='SET NULL')),
        sqlalchemy.Column(
            'profile_revision_id',
            sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_profile_revisions.id'),
            nullable=False),
        sqlalchemy.Column('requested_release', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('reservation_active',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.true()),
        sqlalchemy.Column('source_ref', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('source_root_digest', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('requested_platform', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('source_auth_binding_id', sqlalchemy.Text),
        sqlalchemy.Column('source_auth_fingerprint', sqlalchemy.Text),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('inspection_lease_token', sqlalchemy.Text),
        sqlalchemy.Column('inspection_lease_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('attempt_count',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('next_retry_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('error_code', sqlalchemy.Text),
        sqlalchemy.Column('image_id', sqlalchemy.Text,
                          sqlalchemy.ForeignKey('container_images.id')),
        sqlalchemy.Column('source_id', sqlalchemy.Text,
                          sqlalchemy.ForeignKey('container_image_sources.id')),
        sqlalchemy.Column(
            'canonical_location_id', sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_locations.id')),
        sqlalchemy.Column(
            'inspection_claimable_at', sqlalchemy.BigInteger,
            sqlalchemy.Computed(
                "CASE WHEN canonical_location_id IS NULL AND state = "
                "'PENDING' THEN COALESCE(next_retry_at, updated_at) WHEN "
                "canonical_location_id IS NULL AND state = 'INSPECTING' "
                'THEN inspection_lease_expires_at ELSE NULL END',
                persisted=True)),
        sqlalchemy.Column('reservation_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('record_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.CheckConstraint(
            "state IN ('PENDING', 'INSPECTING', 'READY', 'FAILED')",
            name='ck_container_image_publication_state'),
        sqlalchemy.CheckConstraint(
            "(state = 'INSPECTING' AND inspection_lease_token IS NOT NULL AND "
            "inspection_lease_expires_at IS NOT NULL) OR (state <> "
            "'INSPECTING' AND inspection_lease_token IS NULL AND "
            "inspection_lease_expires_at IS NULL)",
            name='ck_container_image_publication_inspection_lease'),
        sqlalchemy.CheckConstraint(
            '(canonical_location_id IS NULL AND image_id IS NULL AND source_id '
            'IS NULL) OR (canonical_location_id IS NOT NULL AND image_id IS '
            'NOT NULL AND source_id IS NOT NULL)',
            name='ck_container_image_publication_binding'),
        sqlalchemy.CheckConstraint(
            "state <> 'READY' OR (reservation_active IS TRUE AND image_id IS "
            "NOT NULL AND canonical_location_id IS NOT NULL)",
            name='ck_container_image_publication_ready'),
        sqlalchemy.CheckConstraint(
            "state <> 'INSPECTING' OR canonical_location_id IS NULL",
            name='ck_container_image_publication_inspecting_unbound'),
    )
    op.create_index(
        'uq_container_image_publication_release_reservation',
        'container_image_publications', ['workspace', 'requested_release'],
        unique=True,
        postgresql_where=sqlalchemy.text('reservation_active IS TRUE'))
    op.create_index(
        'ix_container_image_publications_inspection_queue',
        'container_image_publications', ['inspection_claimable_at', 'id'],
        postgresql_where=sqlalchemy.text('inspection_claimable_at IS NOT NULL'))
    op.create_index('ix_container_image_publications_canonical_queue',
                    'container_image_publications',
                    ['canonical_location_id', 'state', 'id'])
    op.create_index(
        'ix_container_image_publications_fanout',
        'container_image_publications', ['canonical_location_id', 'id'],
        postgresql_where=sqlalchemy.text(
            "state = 'PENDING' AND canonical_location_id IS NOT NULL"))
    op.create_index('ix_container_image_publications_image',
                    'container_image_publications',
                    ['image_id', 'created_at', 'id'])
    op.create_index('ix_container_image_publications_operation',
                    'container_image_publications', ['operation_id'])
    op.create_index(_PUBLICATION_HISTORY_INDEX, 'container_image_publications',
                    ['workspace', 'created_at', 'id'])
    op.create_index('ix_container_image_publications_workspace_state_history',
                    'container_image_publications',
                    ['workspace', 'state', 'created_at', 'id'])
    op.create_index('ix_container_image_publications_workspace_release_history',
                    'container_image_publications',
                    ['workspace', 'requested_release', 'created_at', 'id'])
    op.create_index('ix_container_image_publications_active_image',
                    'container_image_publications',
                    ['image_id', 'created_at', 'id'],
                    postgresql_where=sqlalchemy.text(
                        "state = 'READY' AND reservation_active IS TRUE"))
    op.create_index('ix_container_image_publications_ready_release',
                    'container_image_publications',
                    ['workspace', 'requested_release', 'image_id'],
                    postgresql_where=sqlalchemy.text("state = 'READY'"))
    op.create_index(
        'ix_container_image_publications_expiry',
        'container_image_publications', ['record_expires_at', 'id'],
        postgresql_where=sqlalchemy.text('record_expires_at IS NOT NULL'))
    op.create_index('ix_container_image_publications_failed_reservation_expiry',
                    'container_image_publications',
                    ['reservation_expires_at', 'id'],
                    postgresql_where=sqlalchemy.text(
                        "state = 'FAILED' AND reservation_active IS TRUE"))
    op.create_index(
        'ix_container_image_publications_terminal_expiry',
        'container_image_publications', ['record_expires_at', 'id'],
        postgresql_where=sqlalchemy.text(
            'reservation_active IS FALSE AND record_expires_at IS NOT NULL'))
    op.create_index('ix_container_image_publications_ready_history',
                    'container_image_publications',
                    ['image_id', 'updated_at', 'id'],
                    postgresql_where=sqlalchemy.text(
                        "state = 'READY' AND reservation_active IS TRUE"))

    op.create_table(
        'container_image_demands',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('authority_id', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('consumer_kind', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('consumer_owner', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('request_id', sqlalchemy.Text),
        sqlalchemy.Column('consumer_generation',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('target_key', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('owner_epoch', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('retry_epoch',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('image_id',
                          sqlalchemy.Text,
                          sqlalchemy.ForeignKey('container_images.id'),
                          nullable=False),
        sqlalchemy.Column('runtime_digest', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column(
            'profile_revision_id',
            sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_profile_revisions.id'),
            nullable=False),
        sqlalchemy.Column('target_fingerprint', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('location_id',
                          sqlalchemy.Text,
                          sqlalchemy.ForeignKey('container_image_locations.id'),
                          nullable=False),
        sqlalchemy.Column('placement_json', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('pull_plan_json', sqlalchemy.Text),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('error_code', sqlalchemy.Text),
        sqlalchemy.Column('consumer_attached',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()),
        sqlalchemy.Column('first_terminal_observed_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('last_terminal_observed_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('terminal_observation_count',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('terminal_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint('workspace',
                                    'consumer_kind',
                                    'consumer_owner',
                                    'consumer_generation',
                                    'target_key',
                                    name='uq_container_image_demand_identity'),
        sqlalchemy.CheckConstraint(
            "state IN ('WARMING', 'READY', 'FAILED', 'SUPERSEDED', 'RELEASED')",
            name='ck_container_image_demand_state'),
        sqlalchemy.CheckConstraint(
            'consumer_generation >= 0 AND owner_epoch >= 0 AND retry_epoch >= '
            '0 AND terminal_observation_count >= 0',
            name='ck_container_image_demand_nonnegative'),
        sqlalchemy.CheckConstraint(
            "(state = 'READY' AND pull_plan_json IS NOT NULL) OR state <> "
            "'READY'",
            name='ck_container_image_demand_ready_plan'),
    )
    op.create_index('ix_container_image_demands_location_fence',
                    'container_image_demands', ['location_id', 'state', 'id'])
    op.create_index('ix_container_image_demands_consumer',
                    'container_image_demands', [
                        'workspace', 'consumer_kind', 'consumer_owner',
                        'consumer_generation', 'target_key'
                    ])
    op.create_index(_DEMAND_HISTORY_INDEX, 'container_image_demands',
                    ['workspace', 'image_id', 'created_at', 'id'])
    op.create_index('ix_container_image_demands_owner_epoch',
                    'container_image_demands',
                    ['consumer_kind', 'owner_epoch', 'state'])
    op.create_index(
        'ix_container_image_demands_cluster_request',
        'container_image_demands', ['request_id', 'state', 'id'],
        postgresql_where=sqlalchemy.text(
            "consumer_kind = 'cluster' AND consumer_attached IS false AND "
            'request_id IS NOT NULL'))
    op.create_index('ix_container_image_demands_terminal',
                    'container_image_demands', ['state', 'expires_at', 'id'])
    op.create_index('ix_container_image_demands_reconcile',
                    'container_image_demands', ['state', 'updated_at', 'id'],
                    postgresql_where=sqlalchemy.text(
                        "state IN ('WARMING', 'READY', 'FAILED')"))
    op.create_index('ix_container_image_demands_reconciliation_queue',
                    'container_image_demands', ['updated_at', 'id'],
                    postgresql_where=sqlalchemy.text(
                        "state IN ('WARMING', 'READY', 'FAILED')"))
    op.create_index(
        'ix_container_image_demands_compaction_queue',
        'container_image_demands', ['expires_at', 'id'],
        postgresql_where=sqlalchemy.text(
            "state IN ('SUPERSEDED', 'RELEASED') AND expires_at IS NOT NULL"))

    op.create_table(
        'container_image_consumer_watermarks',
        sqlalchemy.Column('workspace', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('consumer_kind', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('consumer_owner', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('controller_epoch', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('controller_sequence', sqlalchemy.BigInteger),
        sqlalchemy.Column('owner_epoch', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('max_seen_generation',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('max_terminal_generation',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='-1'),
        sqlalchemy.Column('owner_deleted_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.CheckConstraint(
            '(controller_sequence IS NULL OR controller_sequence >= 0) AND '
            'owner_epoch >= 0 AND max_seen_generation >= 0 AND '
            'max_terminal_generation >= -1 AND '
            'max_terminal_generation <= max_seen_generation',
            name='ck_container_image_consumer_watermark_generation'),
        sqlalchemy.CheckConstraint(
            'length(controller_epoch) BETWEEN 1 AND 1024',
            name='ck_container_image_consumer_controller_epoch'),
    )
    op.create_index(
        'ix_container_image_consumer_watermarks_compaction',
        'container_image_consumer_watermarks', ['owner_deleted_at'],
        postgresql_where=sqlalchemy.text('owner_deleted_at IS NOT NULL'))

    op.create_table(
        'container_image_workers',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('kind', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('version', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('started_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('heartbeat_at', sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('last_success_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('in_flight',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('max_in_flight', sqlalchemy.Integer, nullable=False),
        sqlalchemy.Column(
            'grant_budget_id', sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_provider_budgets.id')),
        sqlalchemy.Column('grant_tokens_milli',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('grant_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.CheckConstraint("kind IN ('COPY', 'LIFECYCLE', 'CANARY')",
                                   name='ck_container_image_worker_kind'),
        sqlalchemy.CheckConstraint(
            'in_flight >= 0 AND max_in_flight > 0 AND in_flight <= '
            'max_in_flight AND grant_tokens_milli >= 0',
            name='ck_container_image_worker_capacity'),
        sqlalchemy.CheckConstraint(
            '(grant_budget_id IS NULL AND grant_tokens_milli = 0 AND '
            'grant_expires_at IS NULL) OR (grant_budget_id IS NOT NULL AND '
            'grant_tokens_milli > 0 AND grant_expires_at IS NOT NULL)',
            name='ck_container_image_worker_grant'),
    )
    op.create_index('ix_container_image_workers_kind_heartbeat',
                    'container_image_workers', ['kind', 'heartbeat_at', 'id'])
    op.create_index('ix_container_image_workers_heartbeat',
                    'container_image_workers', ['heartbeat_at', 'id'])


def upgrade():
    """Bind cluster consumers and create PostgreSQL image-plane state."""
    bind = op.get_bind()
    is_postgres = (
        bind.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value)
    if is_postgres:
        _lock_migration()
    _ensure_auth_sessions_table(bind)
    _add_cluster_binding_columns(bind)
    if not is_postgres:
        return
    schema_name = bind.execute(
        sqlalchemy.text('SELECT current_schema()')).scalar_one_or_none()
    if not schema_name:
        raise RuntimeError(
            'Migration 024 could not identify the managed image schema.')
    _validate_cluster_binding_columns(bind, str(schema_name))
    existing_tables = set(sqlalchemy.inspect(bind).get_table_names())
    existing_image_tables = {
        table_name for table_name in existing_tables
        if table_name.startswith('container_image')
    }
    if existing_image_tables:
        missing_tables = set(_TABLE_NAMES).difference(existing_image_tables)
        unexpected_tables = existing_image_tables.difference(_TABLE_NAMES)
        preview_missing_custody = missing_tables == {
            'container_image_profile_custody'
        }
        if (missing_tables and
                not preview_missing_custody) or unexpected_tables:
            details = []
            if missing_tables:
                details.append('missing tables: ' +
                               ', '.join(sorted(missing_tables)))
            if unexpected_tables:
                details.append('unexpected tables: ' +
                               ', '.join(sorted(unexpected_tables)))
            raise RuntimeError(
                'Migration 024 found incomplete managed image state; ' +
                '; '.join(details) + '.')
        # A preview database stamped with the former image revision 023 has
        # already applied this exact image schema. Compare it with a temporary
        # reference built from this migration's literal DDL before adopting it.
        # Revision 024 then creates the base revision 023 auth table above.
        if preview_missing_custody:
            _create_profile_custody_table(str(schema_name))
        _upgrade_preview_publication_operation_link(bind, str(schema_name))
        _upgrade_preview_canary_queue(bind, str(schema_name))
        _ensure_preview_compatible_indexes(bind, str(schema_name))
        _backfill_and_validate_profile_custody(bind, str(schema_name))
        _validate_preview_schema(bind, str(schema_name))
        return
    _create_tables()
    bind.execute(
        sqlalchemy.text(
            'INSERT INTO container_image_catalog '
            '(id, authority_id, created_at) VALUES (:id, :authority, :created)'
        ), {
            'id': _CATALOG_ROW_ID,
            'authority': str(uuid.uuid4()),
            'created': int(time.time()),
        })


def downgrade():
    """Drop unshipped image state after a literal empty-state proof."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        _drop_cluster_binding_columns()
        return
    _lock_migration()
    for table_name in _TABLE_NAMES[1:]:
        count = bind.execute(
            sqlalchemy.text(f'SELECT count(*) FROM {table_name}')).scalar_one()
        if count != 0:
            raise RuntimeError(
                'Migration 024 downgrade requires all operational managed '
                f'image tables to be empty; {table_name} contains rows.')
    catalog_rows = bind.execute(
        sqlalchemy.text(
            'SELECT id, authority_id FROM container_image_catalog')).all()
    if (len(catalog_rows) != 1 or catalog_rows[0][0] != _CATALOG_ROW_ID or
            not catalog_rows[0][1]):
        raise RuntimeError(
            'Migration 024 downgrade requires exactly the expected catalog '
            'authority singleton.')
    bind.execute(
        sqlalchemy.text('DELETE FROM container_image_catalog WHERE id = :id'),
        {'id': _CATALOG_ROW_ID})
    for table_name in _DROP_TABLE_NAMES:
        op.drop_table(table_name)
    _drop_cluster_binding_columns()
