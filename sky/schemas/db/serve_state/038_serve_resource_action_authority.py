"""Add durable SkyServe resource-action authority state.

Revision ID: 038
Revises: 037
Create Date: 2026-08-03

"""
# pylint: disable=invalid-name,protected-access
from collections.abc import Sequence
import importlib
import re
import typing

from alembic import op
import sqlalchemy as sa

from sky.serve import resource_action_m4_state_schema as m4_schema
from sky.serve import resource_action_state_schema
from sky.serve import serve_state_schema
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '038'
down_revision: str | Sequence[str] | None = '037'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SERVICES = 'services'
_VERSION_SPECS = 'version_specs'
_REPLICAS = 'replicas'
_WORKER_COHORTS = resource_action_state_schema.WORKER_COHORTS.name
_WORKER_COHORT_REFS = resource_action_state_schema.WORKER_COHORT_REFS.name
_SHADOW_COVERAGE = resource_action_state_schema.SHADOW_COVERAGE.name
_API_SERVER_INSTANCES = 'api_server_instances'

_SERVE035_RESERVED_FILL_TABLES = (
    serve_state_schema.reserved_fill_protocol_state_table,
    serve_state_schema.reserved_fill_service_claim_sets_table,
    serve_state_schema.reserved_fill_pool_claims_table,
    serve_state_schema.reserved_fill_rounds_table,
)

_SERVE037_PLACEMENT_TABLES = (
    serve_state_schema.placement_normalization_runs_table,
    serve_state_schema.placement_normalization_rows_table,
)

# The common runtime metadata is the steady-state schema, while a sequential
# upgrade reaches revision 038 before Serve042, Serve047, and Serve049 install
# their PostgreSQL-only launch-binding and route-projection contract. Project
# those future-owned fields and checks out unconditionally: revision 038 must
# reject even a complete early lookalike, leaving each later migration as its
# sole DDL owner.
_POST_SERVE038_FUTURE_COLUMNS = {
    _SERVICES: frozenset({
        'controller_incarnation',
        'controller_owner_epoch',
        'ordinary_launch_binding_capable',
        'ordinary_launch_binding_mode',
        'ordinary_launch_binding_epoch',
        'non_pool_launch_binding_capable',
        'non_pool_launch_controller_incarnation',
        'non_pool_launch_binding_protocol_version',
        'non_pool_launch_capability_profile_set_digest',
        'non_pool_launch_capability_cohort_epoch',
        'non_pool_launch_receipt_protocol_version',
        'route_source_mode',
        'route_source_epoch',
        'route_projection_capable',
        'route_projection_controller_incarnation',
        'route_projection_protocol_version',
    }),
    _REPLICAS: frozenset({'ordinary_launch_association_id'}),
}

_POST_SERVE038_FUTURE_CHECKS = {
    _SERVICES: frozenset({
        'serve049_route_source_mode_ck',
        'serve049_route_source_epoch_ck',
        'serve049_route_capability_shape_ck',
        'serve049_route_projected_capability_ck',
    }),
}

_CHECK_ATTRIBUTE_PATTERN = re.compile(r' :(varattno|varattnosyn) (-?[0-9]+)')

_ALTERED_RELATIONS = (
    _SERVICES,
    _VERSION_SPECS,
    _REPLICAS,
    _WORKER_COHORTS,
    _WORKER_COHORT_REFS,
    _SHADOW_COVERAGE,
)

_COLUMN_FACTORIES: dict[str, typing.Callable[[], tuple[sa.Column, ...]]] = {
    _SERVICES: m4_schema.service_candidate_columns,
    _VERSION_SPECS: m4_schema.version_spec_identity_columns,
    _REPLICAS: m4_schema.replica_spec_identity_columns,
    _WORKER_COHORTS: m4_schema.cohort_candidate_columns,
    _WORKER_COHORT_REFS: m4_schema.cohort_ref_authority_columns,
    _SHADOW_COVERAGE: m4_schema.coverage_candidate_columns,
}

_CHECK_FACTORIES: dict[str, typing.Callable[[], tuple[
    sa.CheckConstraint, ...]]] = {
        _SERVICES: m4_schema.service_candidate_check_constraints,
        _VERSION_SPECS: m4_schema.version_spec_identity_check_constraints,
        _REPLICAS: m4_schema.replica_spec_identity_check_constraints,
        _WORKER_COHORTS: m4_schema.cohort_lifecycle_check_constraints,
        _WORKER_COHORT_REFS: m4_schema.cohort_ref_authority_check_constraints,
        _SHADOW_COVERAGE: m4_schema.coverage_candidate_check_constraints,
    }


def _serve033_module() -> typing.Any:
    return importlib.import_module(
        'sky.schemas.db.serve_state.033_serve_resource_action_coverage')


def _serve034_module() -> typing.Any:
    return importlib.import_module(
        'sky.schemas.db.serve_state.034_authority_release_ledger')


def _serve039_module() -> typing.Any:
    return importlib.import_module(
        'sky.schemas.db.serve_state.039_serve_resource_action_execution_history'
    )


def _serve037_module() -> typing.Any:
    return importlib.import_module(
        'sky.schemas.db.serve_state.037_placement_normalization_ledger')


def _column_map(bind: sa.engine.Connection,
                table: str) -> dict[str, dict[str, typing.Any]]:
    return {
        str(column['name']): column
        for column in sa.inspect(bind).get_columns(table)
    }


def _canonicalize_check_attribute_numbers(
    checks: dict[str, tuple[str, bool, bool]],
    attribute_names: dict[int, str],
) -> dict[str, tuple[str, bool, bool]]:
    """Make exact CHECK parse trees independent of physical column order.

    PostgreSQL stores a CHECK expression's column references as physical
    ``attnum`` values.  A sequentially migrated Serve database legitimately
    has different attribute numbers from the temporary reference table used
    to parse the shipped expression (especially after columns were appended
    and later candidates were added).  Replacing only those reference numbers
    with their resolved column names preserves the rest of the node tree
    exactly, including operators, casts, constants, types, and behavior flags.
    """

    def _replace(match: re.Match[str]) -> str:
        kind = match.group(1)
        number = int(match.group(2))
        name = attribute_names.get(number)
        if name is None:
            return match.group(0)
        return f' :{kind} {name}'

    return {
        name: (_CHECK_ATTRIBUTE_PATTERN.sub(_replace,
                                            node_tree), validated, no_inherit
              ) for name, (node_tree, validated, no_inherit) in checks.items()
    }


def _relation_attribute_names(bind: sa.engine.Connection,
                              relation: str) -> dict[int, str]:
    rows = bind.execute(
        sa.text('SELECT attribute.attnum, attribute.attname '
                'FROM pg_catalog.pg_attribute AS attribute '
                'WHERE attribute.attrelid = '
                'pg_catalog.to_regclass(:relation) '
                'AND attribute.attnum > 0 AND NOT attribute.attisdropped'),
        {'relation': relation})
    return {int(number): str(name) for number, name in rows}


def _canonical_check_node_trees(
    bind: sa.engine.Connection,
    relation: str,
) -> dict[str, tuple[str, bool, bool]]:
    serve034 = _serve034_module()
    return _canonicalize_check_attribute_numbers(
        serve034._check_node_trees(bind, relation),
        _relation_attribute_names(bind, relation))


def _expected_check_node_trees(
    bind: sa.engine.Connection,
    reference: sa.Table,
) -> dict[str, tuple[str, bool, bool]]:
    serve034 = _serve034_module()
    _, checks, _ = serve034._expected_expression_catalogs(bind, reference)
    attribute_names = {
        index: column.name
        for index, column in enumerate(reference.columns, start=1)
    }
    return _canonicalize_check_attribute_numbers(checks, attribute_names)


def _normalized_sql(value: typing.Any) -> str:
    normalized = ''.join(character for character in str(value).lower()
                         if not character.isspace() and character not in '()"')
    return (normalized.replace('::text', '').replace('=anyarray[',
                                                     'in').replace(']', ''))


def _has_owned_sequence_default(bind: sa.engine.Connection, relation: str,
                                column: str) -> bool:
    """Return whether a column has PostgreSQL's exact SERIAL default."""
    row = bind.execute(
        sa.text(
            'SELECT pg_get_expr(definition.adbin, definition.adrelid, TRUE) '
            'AS expression, pg_get_serial_sequence('
            'format(\'%I.%I\', current_schema(), :relation), :column) '
            'AS sequence_name '
            'FROM pg_catalog.pg_attribute AS attribute '
            'JOIN pg_catalog.pg_attrdef AS definition '
            'ON definition.adrelid = attribute.attrelid '
            'AND definition.adnum = attribute.attnum '
            'WHERE attribute.attrelid = pg_catalog.to_regclass(:relation) '
            'AND attribute.attname = :column'), {
                'relation': relation,
                'column': column,
            }).mappings().one_or_none()
    if row is None or row['sequence_name'] is None:
        return False
    sequence_name = str(row['sequence_name'])
    # pg_get_expr() omits the current schema while
    # pg_get_serial_sequence() returns its qualified name.
    spellings = (sequence_name, sequence_name.rsplit('.', maxsplit=1)[-1])
    return row['expression'] in {
        f"nextval('{spelling}'::regclass)" for spelling in spellings
    }


def _has_rows(bind: sa.engine.Connection, table: str) -> bool:
    quoted = bind.dialect.identifier_preparer.quote(table)
    return bool(
        bind.execute(sa.text(
            f'SELECT EXISTS (SELECT 1 FROM {quoted} LIMIT 1)')).scalar_one())


def _assert_revision_037() -> None:
    current = op.get_context().get_current_revision()
    if current != '037':
        raise RuntimeError('SkyServe schema 038 requires the exact Serve037 '
                           f'head; found {current!r}.')


def _adopt_complete_039_replay() -> bool:
    """Recognize an empty exact 039 catalog after a lost older stamp."""
    bind = op.get_bind()
    presence = tuple(
        sa.inspect(bind).has_table(table.name)
        for table in m4_schema.SERVE039_METADATA.sorted_tables)
    if not presence or not all(presence):
        return False
    # Revision 039 repeats these checks under its merged lock schedule before
    # adopting the old stamp.  Checking here prevents revision 038 from
    # interpreting a complete future catalog as a hostile partial 038 install.
    serve039 = _serve039_module()
    serve039._verify_039_catalog()
    serve039._assert_old_stamp_adoptable()
    return True


def _assert_column_compatible(bind: sa.engine.Connection, table: str,
                              actual: dict[str, typing.Any],
                              expected: sa.Column) -> None:
    expected_type = str(expected.type.compile(dialect=bind.dialect)).upper()
    actual_type = str(actual['type'].compile(dialect=bind.dialect)).upper()
    expected_default = expected.server_default
    actual_default = actual.get('default')
    if (actual_type != expected_type or
            bool(actual['nullable']) != expected.nullable or
        (actual_default is None) != (expected_default is None)):
        raise RuntimeError(
            'SkyServe schema 038 found an incompatible pre-existing column '
            f'{table}.{expected.name}.')


def _assert_candidate_column_catalog() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table, factory in _COLUMN_FACTORIES.items():
        if not inspector.has_table(table):
            raise RuntimeError(
                f'SkyServe schema 038 requires existing relation {table!r}.')
        columns = _column_map(bind, table)
        expected_names = {column.name for column in factory()}
        present = expected_names.intersection(columns)
        if table in (_WORKER_COHORT_REFS, _SHADOW_COVERAGE) and present:
            raise RuntimeError(
                'SkyServe schema 038 refuses a partially installed altered '
                f'relation catalog for {table!r}.')
        if table != _WORKER_COHORTS and present and present != expected_names:
            raise RuntimeError(
                'SkyServe schema 038 refuses a partial candidate-column set '
                f'for {table!r}.')
        # The three revision-001 bootstrap sets may be wholly absent or wholly
        # present.  Only the single nullable cohort timestamp may be adopted
        # independently after an unknown DDL acknowledgement.
        for expected in factory():
            if expected.name in present:
                _assert_column_compatible(bind, table, columns[expected.name],
                                          expected)


def _verify_indexes(bind: sa.engine.Connection, table: sa.Table) -> None:
    inspector = sa.inspect(bind)
    actual = {
        str(index['name']): index
        for index in inspector.get_indexes(table.name)
        if index.get('duplicates_constraint') is None
    }
    expected = {str(index.name): index for index in table.indexes}
    if set(actual) != set(expected):
        raise RuntimeError('SkyServe schema 038 found incompatible indexes '
                           f'for {table.name!r}.')
    for name, expected_index in expected.items():
        actual_index = actual[name]
        expected_where = expected_index.dialect_options['postgresql']['where']
        actual_where = (actual_index.get('dialect_options') or
                        {}).get('postgresql_where')
        if (bool(actual_index['unique']) != bool(expected_index.unique) or
                tuple(actual_index['column_names']) != tuple(
                    column.name for column in expected_index.columns) or
                _normalized_sql(actual_where)
                != _normalized_sql(expected_where)):
            raise RuntimeError('SkyServe schema 038 found incompatible index '
                               f'{name!r}.')
        state = bind.execute(
            sa.text('SELECT table_namespace.nspname AS table_schema, '
                    'index_namespace.nspname AS index_schema, '
                    'table_relation.relname AS table_name, '
                    'index_row.indisvalid, index_row.indisready, '
                    'index_row.indislive, index_row.indisunique, '
                    'index_row.indisprimary, index_row.indisexclusion, '
                    'index_row.indexprs IS NULL AS expression_free, '
                    'access_method.amname, index_row.indnkeyatts, '
                    'index_row.indnatts, '
                    'ARRAY(SELECT pg_get_indexdef(index_row.indexrelid, '
                    'position, TRUE) FROM generate_series('
                    '1, index_row.indnkeyatts) AS position) AS key_columns, '
                    'NOT EXISTS (SELECT 1 FROM unnest('
                    'index_row.indoption::smallint[]) AS option(value) '
                    'WHERE value <> 0) AS default_ordering, '
                    'pg_get_expr(index_row.indpred, index_row.indrelid, TRUE) '
                    'AS predicate '
                    'FROM pg_catalog.pg_index AS index_row '
                    'JOIN pg_catalog.pg_class AS index_relation '
                    'ON index_relation.oid = index_row.indexrelid '
                    'JOIN pg_catalog.pg_namespace AS index_namespace '
                    'ON index_namespace.oid = index_relation.relnamespace '
                    'JOIN pg_catalog.pg_class AS table_relation '
                    'ON table_relation.oid = index_row.indrelid '
                    'JOIN pg_catalog.pg_namespace AS table_namespace '
                    'ON table_namespace.oid = table_relation.relnamespace '
                    'JOIN pg_catalog.pg_am AS access_method '
                    'ON access_method.oid = index_relation.relam '
                    'WHERE index_namespace.nspname = current_schema() '
                    'AND index_relation.relname = :name'), {
                        'name': name
                    }).mappings().one_or_none()
        expected_columns = tuple(
            column.name for column in expected_index.columns)
        if (state is None or state['table_schema'] != state['index_schema'] or
                state['table_name'] != table.name or
                not bool(state['indisvalid']) or
                not bool(state['indisready']) or not bool(state['indislive']) or
                bool(state['indisunique']) != bool(expected_index.unique) or
                bool(state['indisprimary']) or bool(state['indisexclusion']) or
                not bool(state['expression_free']) or
                state['amname'] != 'btree' or
                int(state['indnkeyatts']) != len(expected_columns) or
                int(state['indnatts']) != len(expected_columns) or
                tuple(state['key_columns'] or ()) != expected_columns or
                not bool(state['default_ordering']) or _normalized_sql(
                    state['predicate']) != _normalized_sql(expected_where)):
            raise RuntimeError('SkyServe schema 038 found incompatible '
                               f'PostgreSQL index state for {name!r}.')


def _verify_exact_table(bind: sa.engine.Connection, table: sa.Table) -> None:
    """Verify a shipped table while normalizing PostgreSQL conventions."""
    serve034 = _serve034_module()
    inspector = sa.inspect(bind)
    (expected_defaults, _,
     expected_column_catalog) = (serve034._expected_expression_catalogs(
         bind, table))
    if (serve034._relation_behavior(bind, table.name)
            != serve034._EXPECTED_RELATION_BEHAVIOR):
        raise RuntimeError('SkyServe schema 038 found incompatible relation '
                           f'behavior for {table.name!r}.')
    columns = _column_map(bind, table.name)
    if set(columns) != set(table.c.keys()):
        raise RuntimeError('SkyServe schema 038 found an incompatible column '
                           f'inventory for {table.name!r}.')
    for expected in table.columns:
        actual = columns[expected.name]
        expected_type = str(expected.type.compile(dialect=bind.dialect)).upper()
        actual_type = str(actual['type'].compile(dialect=bind.dialect)).upper()
        # PostgreSQL emits SQLAlchemy Float as DOUBLE PRECISION and reflects
        # that concrete spelling.  Keep every other type comparison exact.
        compatible_type = (actual_type == expected_type or
                           (expected_type == 'FLOAT' and
                            actual_type == 'DOUBLE PRECISION'))
        if (not compatible_type or
                bool(actual['nullable']) != expected.nullable):
            raise RuntimeError(
                'SkyServe schema 038 found an incompatible column '
                f'{table.name}.{expected.name}.')
    actual_defaults = serve034._default_node_trees(bind, table.name)
    # SQLAlchemy emits an owned sequence for an integral PostgreSQL primary
    # key even when the Table has no explicit server_default.  The reference
    # catalog intentionally omits key constraints, so normalize only the exact
    # SERIAL expression that PostgreSQL reports as owned by that key column.
    for column in table.primary_key.columns:
        if (expected_defaults.get(column.name) is None and
                isinstance(column.type, sa.Integer) and
                _has_owned_sequence_default(bind, table.name, column.name)):
            actual_defaults[column.name] = None
    if actual_defaults != expected_defaults:
        raise RuntimeError('SkyServe schema 038 found incompatible column '
                           f'defaults for {table.name!r}.')
    if (serve034._column_semantic_catalog(bind, table.name)
            != expected_column_catalog):
        raise RuntimeError('SkyServe schema 038 found incompatible column '
                           f'semantics for {table.name!r}.')

    primary = inspector.get_pk_constraint(table.name)
    expected_primary_name = table.primary_key.name or f'{table.name}_pkey'
    if (primary.get('name') != expected_primary_name or tuple(
            primary.get('constrained_columns') or
        ()) != tuple(column.name for column in table.primary_key.columns)):
        raise RuntimeError('SkyServe schema 038 found an incompatible primary '
                           f'key for {table.name!r}.')
    expected_uniques = {
        str(constraint.name):
            tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, sa.UniqueConstraint)
    }
    actual_uniques = {
        str(constraint['name']): tuple(constraint['column_names'])
        for constraint in inspector.get_unique_constraints(table.name)
    }
    if actual_uniques != expected_uniques:
        raise RuntimeError('SkyServe schema 038 found incompatible unique '
                           f'constraints for {table.name!r}.')
    expected_flags = serve034._expected_noncheck_constraint_flags(table)
    unnamed_primary = expected_flags.pop('None', None)
    if unnamed_primary is not None:
        expected_flags[expected_primary_name] = unnamed_primary
    if serve034._noncheck_constraint_flags(bind, table.name) != expected_flags:
        raise RuntimeError('SkyServe schema 038 found incompatible key '
                           f'constraint behavior for {table.name!r}.')
    if (_canonical_check_node_trees(bind, table.name)
            != _expected_check_node_trees(bind, table)):
        raise RuntimeError('SkyServe schema 038 found incompatible check '
                           f'constraints for {table.name!r}.')
    actual_foreign_keys = {
        str(constraint['name']): (
            tuple(constraint['constrained_columns']),
            constraint.get('referred_schema'),
            str(constraint['referred_table']),
            tuple(constraint['referred_columns']),
            (constraint.get('options') or {}).get('onupdate'),
            (constraint.get('options') or {}).get('ondelete'),
            (constraint.get('options') or {}).get('deferrable'),
            (constraint.get('options') or {}).get('initially'),
            (constraint.get('options') or {}).get('match'),
        ) for constraint in inspector.get_foreign_keys(table.name)
    }
    if actual_foreign_keys != serve034._expected_foreign_keys(table):
        raise RuntimeError('SkyServe schema 038 found incompatible foreign '
                           f'keys for {table.name!r}.')
    _verify_indexes(bind, table)


def _verify_worker_cohort_serve033_catalog(bind: sa.engine.Connection) -> None:
    """Allow only the exact nullable 038 timestamp as an adopted addition."""
    expected = resource_action_state_schema.WORKER_COHORTS
    columns = _column_map(bind, expected.name)
    removal = m4_schema.cohort_candidate_columns()[0]
    if removal.name not in columns:
        _verify_exact_table(bind, expected)
        return
    _assert_column_compatible(bind, expected.name, columns[removal.name],
                              removal)
    expected_metadata = sa.MetaData()
    expected_with_timestamp = expected.to_metadata(expected_metadata)
    expected_with_timestamp.append_column(removal)
    _verify_exact_table(bind, expected_with_timestamp)


def _common_runtime_table(table_name: str) -> sa.Table:
    runtime = {
        _SERVICES: serve_state_schema.services_table,
        _VERSION_SPECS: serve_state_schema.version_specs_table,
        _REPLICAS: serve_state_schema.replicas_table,
    }[table_name]
    # Project later migration-owned columns out of Serve038's historical
    # relation contract. Keep the closed inventory shared with the independent
    # catalog constructor below so fresh and replay validation cannot drift.
    future_owned_columns = _POST_SERVE038_FUTURE_COLUMNS.get(
        table_name, frozenset())
    if not future_owned_columns:
        return runtime
    projected_metadata = sa.MetaData()
    # ``to_metadata()`` preserves string-declared foreign keys; copy all
    # referenced relations into the private metadata so their targets remain
    # resolvable while the projected common table is verified.
    for foreign_key in runtime.foreign_keys:
        referenced_table = foreign_key.column.table
        if referenced_table.name not in projected_metadata.tables:
            referenced_table.to_metadata(projected_metadata)
    projected = runtime.to_metadata(projected_metadata)
    future_owned_checks = _POST_SERVE038_FUTURE_CHECKS.get(
        table_name, frozenset())
    for constraint in tuple(projected.constraints):
        if constraint.name in future_owned_checks:
            projected.constraints.remove(constraint)
    for column_name in future_owned_columns:
        column = projected.c.get(column_name)
        if column is not None:
            projected._columns.remove(column)
    return projected


def _common_revision_038_table(table_name: str,
                               metadata: sa.MetaData) -> sa.Table:
    """Project steady-state metadata onto the exact revision-038 contract."""
    # Clone the complete graph so foreign-key targets remain resolvable when
    # the historical envelope compares constraint semantics.
    for table in serve_state_schema.Base.metadata.sorted_tables:
        table.to_metadata(metadata)
    expected = metadata.tables[table_name]
    future_owned_checks = _POST_SERVE038_FUTURE_CHECKS.get(
        table_name, frozenset())
    for constraint in tuple(expected.constraints):
        if constraint.name in future_owned_checks:
            expected.constraints.remove(constraint)
    for column_name in _POST_SERVE038_FUTURE_COLUMNS.get(
            table_name, frozenset()):
        column = expected.c.get(column_name)
        if column is not None:
            expected._columns.remove(column)
    return expected


def _common_expected_checks(
        bind: sa.engine.Connection, table_name: str, *,
        post_038: bool) -> dict[str, tuple[str, bool, bool]]:
    serve033 = _serve033_module()
    constraints: list[sa.CheckConstraint] = []
    if not post_038 and table_name == _SERVICES:
        constraints.extend(
            sa.CheckConstraint(expression, name=name)
            for name, expression in serve033._SERVICE_CHECKS.items())
    if table_name == _REPLICAS:
        constraints.extend(
            sa.CheckConstraint(expression, name=name)
            for name, expression in serve033._REPLICA_CHECKS.items())
    if table_name == _VERSION_SPECS:
        serve037 = _serve037_module()
        constraints.append(
            sa.CheckConstraint(serve037._RETIREMENT_EXPRESSION,
                               name=serve037._RETIREMENT_CHECK))
    if post_038:
        constraints.extend(_CHECK_FACTORIES[table_name]())
    reference_metadata = sa.MetaData()
    reference = sa.Table(
        table_name, reference_metadata,
        *(sa.Column(str(column['name']), column['type'])
          for column in sa.inspect(bind).get_columns(table_name)), *constraints)
    return _expected_check_node_trees(bind, reference)


def _common_expected_index_table(bind: sa.engine.Connection,
                                 table_name: str) -> sa.Table:
    serve033 = _serve033_module()
    metadata = sa.MetaData()
    expected = _common_revision_038_table(table_name, metadata)
    actual_columns = set(_column_map(bind, table_name))
    for column in tuple(expected.c):
        if column.name not in actual_columns:
            expected._columns.remove(column)
    if table_name == _REPLICAS:
        for name, column_name in serve033._REPLICA_UNIQUE_INDEXES.items():
            sa.Index(name,
                     expected.c[column_name],
                     unique=True,
                     postgresql_where=sa.text(f'{column_name} IS NOT NULL'))
    return expected


def _assert_common_relation_envelope(bind: sa.engine.Connection,
                                     table_name: str, *,
                                     post_038: bool) -> None:
    """Reject common-table columns, keys, checks, indexes, and behavior drift."""
    serve034 = _serve034_module()
    expected = _common_revision_038_table(table_name, sa.MetaData())
    actual_columns = set(_column_map(bind, table_name))
    expected_columns = set(expected.c.keys())
    candidate_columns = {
        column.name for column in _COLUMN_FACTORIES[table_name]()
    }
    if not post_038 and not candidate_columns.issubset(actual_columns):
        expected_columns -= candidate_columns
    if actual_columns != expected_columns:
        missing = sorted(expected_columns - actual_columns)
        unexpected = sorted(actual_columns - expected_columns)
        raise RuntimeError(
            'SkyServe schema 038 found an incompatible column '
            f'inventory for {table_name!r}: missing={missing!r}, '
            f'unexpected={unexpected!r}.')
    if (serve034._relation_behavior(bind, table_name)
            != serve034._EXPECTED_RELATION_BEHAVIOR):
        raise RuntimeError('SkyServe schema 038 found incompatible relation '
                           f'behavior for {table_name!r}.')
    primary = sa.inspect(bind).get_pk_constraint(table_name)
    expected_primary_columns = tuple(
        column.name for column in expected.primary_key.columns)
    expected_primary_name = f'{table_name}_pkey'
    if (primary.get('name') != expected_primary_name or
            tuple(primary.get('constrained_columns') or
                  ()) != expected_primary_columns):
        raise RuntimeError('SkyServe schema 038 found an incompatible primary '
                           f'key for {table_name!r}.')
    expected_flags = serve034._expected_noncheck_constraint_flags(expected)
    # SQLAlchemy leaves conventionally named primary keys unnamed in the
    # runtime metadata, while PostgreSQL materializes them as
    # ``<table>_pkey``.  The explicit primary-key shape check above pins the
    # same contract; normalize that one metadata placeholder before comparing
    # the full pg_constraint flags.
    unnamed_primary = expected_flags.pop('None', None)
    if unnamed_primary is not None:
        expected_flags[expected_primary_name] = unnamed_primary
    if serve034._noncheck_constraint_flags(bind, table_name) != expected_flags:
        raise RuntimeError('SkyServe schema 038 found incompatible key '
                           f'constraints for {table_name!r}.')
    actual_foreign_keys = {
        str(constraint['name']): (
            tuple(constraint['constrained_columns']),
            constraint.get('referred_schema'),
            str(constraint['referred_table']),
            tuple(constraint['referred_columns']),
            (constraint.get('options') or {}).get('onupdate'),
            (constraint.get('options') or {}).get('ondelete'),
            (constraint.get('options') or {}).get('deferrable'),
            (constraint.get('options') or {}).get('initially'),
            (constraint.get('options') or {}).get('match'),
        ) for constraint in sa.inspect(bind).get_foreign_keys(table_name)
    }
    if actual_foreign_keys != serve034._expected_foreign_keys(expected):
        raise RuntimeError('SkyServe schema 038 found incompatible foreign '
                           f'keys for {table_name!r}.')
    actual_checks = _canonical_check_node_trees(bind, table_name)
    expected_checks = _common_expected_checks(bind,
                                              table_name,
                                              post_038=post_038)
    if actual_checks != expected_checks:
        raise RuntimeError('SkyServe schema 038 found incompatible check '
                           f'constraints for {table_name!r}.')
    _verify_indexes(bind, _common_expected_index_table(bind, table_name))


def _assert_serve033_common_catalog_exact() -> None:
    """Verify the action-owned common-table slice with PG semantic trees."""
    bind = op.get_bind()
    serve033 = _serve033_module()
    serve034 = _serve034_module()
    catalogs = (
        (_SERVICES, resource_action_state_schema.service_columns(),
         serve033._SERVICE_CHECKS),
        (_REPLICAS, (*resource_action_state_schema.replica_columns(),
                     *resource_action_state_schema.replica_coverage_columns()),
         serve033._REPLICA_CHECKS),
    )
    for table_name in (_SERVICES, _VERSION_SPECS, _REPLICAS):
        _assert_common_relation_envelope(bind, table_name, post_038=False)
    for table_name, columns, checks in catalogs:
        reference_metadata = sa.MetaData()
        reference = sa.Table(
            table_name, reference_metadata, *columns,
            *(sa.CheckConstraint(expression, name=name)
              for name, expression in checks.items()))
        (expected_defaults, _,
         expected_semantics) = serve034._expected_expression_catalogs(
             bind, reference)
        check_reference_metadata = sa.MetaData()
        check_reference = sa.Table(
            table_name, check_reference_metadata,
            *(sa.Column(str(column['name']), column['type'])
              for column in sa.inspect(bind).get_columns(table_name)),
            *(sa.CheckConstraint(expression, name=name)
              for name, expression in checks.items()))
        expected_checks = _expected_check_node_trees(bind, check_reference)
        actual_defaults = serve034._default_node_trees(bind, table_name)
        actual_checks = _canonical_check_node_trees(bind, table_name)
        actual_semantics = serve034._column_semantic_catalog(bind, table_name)
        for column in reference.c:
            if (actual_defaults.get(column.name)
                    != expected_defaults[column.name] or actual_semantics.get(
                        column.name) != expected_semantics[column.name]):
                raise RuntimeError(
                    'SkyServe schema 038 found an incompatible Serve033 '
                    f'column semantic for {table_name}.{column.name}.')
        if actual_checks != expected_checks:
            raise RuntimeError(
                'SkyServe schema 038 found an incompatible Serve033 check '
                f'constraint inventory for {table_name!r}.')


def _assert_serve033_catalog() -> None:
    serve033 = _serve033_module()
    bind = op.get_bind()
    serve033._assert_upstream_032_catalog()
    serve033._verify_postgres_common_contract()
    _assert_serve033_common_catalog_exact()
    for table in serve033._action_tables():
        if table.name == _WORKER_COHORTS:
            _verify_worker_cohort_serve033_catalog(bind)
        else:
            # Candidate fields on references/coverage cannot precede their
            # new FK/non-null contract.  Refuse such a hybrid rather than
            # weakening the exact shipped Serve033 validation.
            _verify_exact_table(bind, table)


def _assert_serve034_catalog() -> None:
    bind = op.get_bind()
    metadata = (
        resource_action_state_schema.RESOURCE_ACTION_AUTHORITY_RELEASE_METADATA)
    for table in metadata.sorted_tables:
        _verify_exact_table(bind, table)


def _assert_serve037_catalog(*, post_038: bool = False) -> None:
    """Verify the reserved-fill, controller-config, and placement baseline."""
    bind = op.get_bind()
    for table in _SERVE035_RESERVED_FILL_TABLES:
        _verify_exact_table(bind, table)
    for table in _SERVE037_PLACEMENT_TABLES:
        _verify_exact_table(bind, table)
    # The Serve036 controller configuration and Serve037 normalization fields
    # live on common runtime tables.  The relation envelope proves their full
    # column, CHECK, FK, index, and PostgreSQL behavior contracts.
    for table_name in (_SERVICES, _VERSION_SPECS, _REPLICAS):
        _assert_common_relation_envelope(bind, table_name, post_038=post_038)


def _assert_preexisting_038_tables() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table in m4_schema.SERVE038_METADATA.sorted_tables:
        if not inspector.has_table(table.name):
            continue
        _verify_exact_table(bind, table)
        if _has_rows(bind, table.name):
            raise RuntimeError('SkyServe schema 038 cannot adopt nonempty '
                               f'pre-existing authority table {table.name!r}.')


def _lock_altered_relations() -> None:
    bind = op.get_bind()
    quote = bind.dialect.identifier_preparer.quote
    for relation in _ALTERED_RELATIONS:
        bind.exec_driver_sql(
            f'LOCK TABLE {quote(relation)} IN ACCESS EXCLUSIVE MODE')


def _lock_cross_schema_cleanup_guard() -> None:
    """Freeze any retained API008 authority-instance inventory."""
    bind = op.get_bind()
    if not sa.inspect(bind).has_table(_API_SERVER_INSTANCES):
        # A fresh central installation creates the independent API-request
        # lineage after Serve.  No table means no retained authority instance.
        return
    columns = set(_column_map(bind, _API_SERVER_INSTANCES))
    if 'role' not in columns:
        raise RuntimeError('SkyServe schema 038 found an incompatible '
                           'api_server_instances cleanup guard.')
    quote = bind.dialect.identifier_preparer.quote
    bind.exec_driver_sql(
        f'LOCK TABLE {quote(_API_SERVER_INSTANCES)} IN ACCESS EXCLUSIVE MODE')


def _column_predicate(table: str, columns: tuple[str, ...]) -> str | None:
    present = set(_column_map(op.get_bind(), table))
    selected = [column for column in columns if column in present]
    if not selected:
        return None
    return ' OR '.join(f'{column} IS NOT NULL' for column in selected)


def _assert_zero_matching(table: str, predicate: str, message: str) -> None:
    bind = op.get_bind()
    quoted = bind.dialect.identifier_preparer.quote(table)
    if bind.execute(
            sa.text(f'SELECT EXISTS (SELECT 1 FROM {quoted} WHERE '
                    f'{predicate} LIMIT 1)')).scalar_one():
        raise RuntimeError(message)


def _assert_activation_inventory() -> None:
    """Recheck the complete dark-state gate under all six table locks."""
    _assert_zero_matching(
        _SERVICES, "resource_action_mode <> 'legacy'",
        'SkyServe schema 038 requires zero shadow or authoritative services.')
    candidate_predicates = (
        (_SERVICES, (
            'resource_action_candidate_epoch',
            'resource_action_candidate_policy_sha256',
            'resource_action_candidate_binding_sha256',
        )),
        (_VERSION_SPECS, (
            'resource_action_spec_identity',
            'resource_action_spec_identity_sha256',
        )),
        (_REPLICAS, ('resource_action_spec_identity_sha256',)),
        (_WORKER_COHORT_REFS, (
            'authority_policy_epoch',
            'authority_policy_sha256',
            'authority_binding_sha256',
        )),
    )
    for table, columns in candidate_predicates:
        predicate = _column_predicate(table, columns)
        if predicate is not None:
            _assert_zero_matching(
                table, predicate,
                'SkyServe schema 038 requires every pre-existing candidate '
                f'field in {table!r} to be null.')
    if _has_rows(op.get_bind(), _SHADOW_COVERAGE):
        raise RuntimeError(
            'SkyServe schema 038 requires zero shadow coverage rows.')
    cohort_columns = set(_column_map(op.get_bind(), _WORKER_COHORTS))
    null_removal_history = ('removal_authorized_at IS NULL'
                            if 'removal_authorized_at' in cohort_columns else
                            'TRUE')
    _assert_zero_matching(
        _WORKER_COHORTS,
        "((jsonb_typeof(registration_attestations) = 'object' AND "
        "((registration_attestations -> 'version')::text = '1') IS TRUE "
        "AND lifecycle_state = 'RETIRED' AND "
        f'{null_removal_history})) IS NOT TRUE',
        'SkyServe schema 038 requires every pre-existing worker cohort to be '
        'exact retired V1 null-time history.')
    if sa.inspect(op.get_bind()).has_table(_API_SERVER_INSTANCES):
        _assert_zero_matching(
            _API_SERVER_INSTANCES, "role = 'authority-worker'",
            'SkyServe schema 038 requires zero stale or fresh authority-worker '
            'API server-instance rows.')
    for table in m4_schema.SERVE038_METADATA.sorted_tables:
        if sa.inspect(op.get_bind()).has_table(table.name) and _has_rows(
                op.get_bind(), table.name):
            raise RuntimeError(
                'SkyServe schema 038 requires every authority '
                f'table to be empty; found rows in {table.name}.')


def _add_or_validate_columns(table: str, columns: tuple[sa.Column,
                                                        ...]) -> None:
    bind = op.get_bind()
    existing = _column_map(bind, table)
    for column in columns:
        if column.name in existing:
            _assert_column_compatible(bind, table, existing[column.name],
                                      column)
            continue
        op.add_column(table, column)
        existing = _column_map(bind, table)


def _drop_check_if_present(table: str, name: str) -> None:
    checks = {
        str(constraint['name'])
        for constraint in sa.inspect(op.get_bind()).get_check_constraints(table)
        if constraint['name'] is not None
    }
    if name in checks:
        op.drop_constraint(name, table, type_='check')


def _replace_checks(table: str, constraints: tuple[sa.CheckConstraint,
                                                   ...]) -> None:
    for constraint in constraints:
        name = str(constraint.name)
        _drop_check_if_present(table, name)
        op.create_check_constraint(name, table, str(constraint.sqltext))


def _install_existing_relation_contract() -> None:
    for table, factory in _COLUMN_FACTORIES.items():
        _add_or_validate_columns(table, factory())

    _drop_check_if_present(_SERVICES, 'ck_services_resource_action_mode')
    _drop_check_if_present(_SERVICES,
                           'ck_services_resource_action_mode_timestamp')
    for table, factory in _CHECK_FACTORIES.items():
        _replace_checks(table, factory())


def _create_or_adopt_038_tables() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    # The policy table must exist before the altered reference acquires its
    # four-column immutable binding FK.
    policy = m4_schema.AUTHORITY_POLICY_EPOCHS
    if not inspector.has_table(policy.name):
        policy.create(bind, checkfirst=False)
    _verify_exact_table(bind, policy)

    foreign_key_name = 'fk_serve_ra_worker_cohort_refs_authority_policy'
    existing_foreign_keys = {
        str(constraint['name'])
        for constraint in sa.inspect(bind).get_foreign_keys(_WORKER_COHORT_REFS)
        if constraint['name'] is not None
    }
    if foreign_key_name in existing_foreign_keys:
        op.drop_constraint(foreign_key_name,
                           _WORKER_COHORT_REFS,
                           type_='foreignkey')
    foreign_key = m4_schema.cohort_ref_authority_foreign_key()
    op.create_foreign_key(
        str(foreign_key.name),
        _WORKER_COHORT_REFS,
        m4_schema.AUTHORITY_POLICY_EPOCHS.name, [
            'service_hash', 'authority_policy_epoch', 'authority_policy_sha256',
            'authority_binding_sha256'
        ], [
            'service_hash', 'policy_epoch', 'policy_sha256',
            'authority_binding_sha256'
        ],
        ondelete='RESTRICT')

    for table in m4_schema.SERVE038_METADATA.sorted_tables:
        if table is policy:
            continue
        if not sa.inspect(bind).has_table(table.name):
            table.create(bind, checkfirst=False)
        _verify_exact_table(bind, table)


def _assert_columns_installed() -> None:
    bind = op.get_bind()
    serve034 = _serve034_module()
    for table, factory in _COLUMN_FACTORIES.items():
        columns = _column_map(bind, table)
        expected_columns = factory()
        for expected in expected_columns:
            actual = columns.get(expected.name)
            if actual is None:
                raise RuntimeError('SkyServe schema 038 failed to install '
                                   f'{table}.{expected.name}.')
            _assert_column_compatible(bind, table, actual, expected)
        reference_metadata = sa.MetaData()
        reference = sa.Table(table, reference_metadata, *factory())
        (expected_defaults, _,
         expected_semantics) = serve034._expected_expression_catalogs(
             bind, reference)
        actual_defaults = serve034._default_node_trees(bind, table)
        actual_semantics = serve034._column_semantic_catalog(bind, table)
        for expected in expected_columns:
            if (actual_defaults.get(expected.name)
                    != expected_defaults[expected.name] or actual_semantics.get(
                        expected.name) != expected_semantics[expected.name]):
                raise RuntimeError(
                    'SkyServe schema 038 failed its reflected column '
                    f'semantic postcondition for {table}.{expected.name}.')


def _assert_checks_installed() -> None:
    bind = op.get_bind()
    for table, factory in _CHECK_FACTORIES.items():
        reflected_columns = sa.inspect(bind).get_columns(table)
        reference_metadata = sa.MetaData()
        reference = sa.Table(
            table, reference_metadata,
            *(sa.Column(str(column['name']), column['type'])
              for column in reflected_columns), *factory())
        expected_checks = _expected_check_node_trees(bind, reference)
        actual_checks = _canonical_check_node_trees(bind, table)
        for name, expected in expected_checks.items():
            if actual_checks.get(name) != expected:
                raise RuntimeError(
                    'SkyServe schema 038 failed its reflected check '
                    f'postcondition {name!r}.')


def _assert_reference_foreign_key() -> None:
    expected = {
        'name': 'fk_serve_ra_worker_cohort_refs_authority_policy',
        'columns': ('service_hash', 'authority_policy_epoch',
                    'authority_policy_sha256', 'authority_binding_sha256'),
        'referred_table': m4_schema.AUTHORITY_POLICY_EPOCHS.name,
        'referred_columns': ('service_hash', 'policy_epoch', 'policy_sha256',
                             'authority_binding_sha256'),
        'ondelete': 'RESTRICT',
    }
    constraints = {
        str(constraint['name']): constraint for constraint in sa.inspect(
            op.get_bind()).get_foreign_keys(_WORKER_COHORT_REFS)
    }
    actual = constraints.get(expected['name'])
    if (actual is None or
            tuple(actual['constrained_columns']) != expected['columns'] or
            str(actual['referred_table']) != expected['referred_table'] or
            tuple(actual['referred_columns']) != expected['referred_columns'] or
        (actual.get('options') or {}).get('ondelete') != expected['ondelete']):
        raise RuntimeError('SkyServe schema 038 failed its reflected '
                           'authority-policy reference FK postcondition.')


def _verify_postcondition() -> None:
    bind = op.get_bind()
    _assert_columns_installed()
    _assert_checks_installed()
    _assert_reference_foreign_key()
    for table_name in (_SERVICES, _VERSION_SPECS, _REPLICAS):
        _assert_common_relation_envelope(bind, table_name, post_038=True)
    for table in m4_schema.SERVE038_ALTERED_RELATION_METADATA.sorted_tables:
        _verify_exact_table(bind, table)
    for table in m4_schema.SERVE038_METADATA.sorted_tables:
        _verify_exact_table(bind, table)
    # The migration never changes the immutable release ledger.  Re-reflect it
    # after every feature DDL statement before Alembic stamps the new head.
    _assert_serve034_catalog()
    _assert_serve037_catalog(post_038=True)


def upgrade() -> None:
    """Install the PostgreSQL-only durable authority-state catalog."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'SkyServe schema 038 is PostgreSQL-only and must not be stamped '
            f'on dialect {bind.dialect.name!r}.')

    # These validations intentionally precede every target relation lock or
    # mutation.  They reject an incomplete historical ledger and hostile
    # lookalike 038 object rather than completing it speculatively.
    _assert_revision_037()
    if _adopt_complete_039_replay():
        return
    _assert_candidate_column_catalog()
    _assert_serve033_catalog()
    _assert_serve034_catalog()
    _assert_serve037_catalog()
    _assert_preexisting_038_tables()

    _lock_altered_relations()
    _lock_cross_schema_cleanup_guard()
    _assert_activation_inventory()
    _install_existing_relation_contract()
    _create_or_adopt_038_tables()
    _verify_postcondition()


def downgrade() -> None:
    """Retain authority history on application rollback."""
    raise RuntimeError(
        'SkyServe schema 038 is additive and cannot be downgraded. Roll back '
        'the application against the retained authority-state catalog instead.')
