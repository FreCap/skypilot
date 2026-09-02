"""Explicit PostgreSQL authority for placement-normalization manifests.

Revision 040 is the database boundary that makes normalization manifests
append-only.  This module is the application-facing interface to that boundary:
it resolves the revision's canonical schema without using ``search_path``,
invokes the schema-qualified catalog assertion, and returns typed gate state.
"""

import dataclasses
import importlib
import uuid

import sqlalchemy
from sqlalchemy import orm

AUTHORITY_REVISION = '040'
# Keep this set closed: a later Serve migration is accepted only after review
# confirms that it is additive with respect to the frozen revision-040
# relation/function contract.  The catalog proof below remains the trust root;
# the Alembic head merely identifies a recognized descendant installation.
RECOGNIZED_ADDITIVE_REVISIONS = frozenset(
    ('040', '041', '042', '043', '044', '045', '046', '047', '048', '049',
     '050', '051', '052', '053', '054', '055', '056', '057', '058', '059',
     '060', '061', '062', '063', '064', '065', '066', '067'))
AUTHORITY_FUNCTION = ('skyserve040_assert_placement_normalization_authority')
AUTHORITY_GATE = 'placement_normalization_write_fence'
RUNS_RELATION = 'placement_normalization_runs'
ROWS_RELATION = 'placement_normalization_rows'
VERSION_RELATION = 'alembic_version_serve_state_db'

OPERATOR_RELATIONS = (
    'api_requests',
    'api_resource_actions',
    'api_server_instances',
    'ephemeral_storage_cleanup_intents',
    AUTHORITY_GATE,
    RUNS_RELATION,
    ROWS_RELATION,
    VERSION_RELATION,
    'replicas',
    'serve_resource_action_shadow_samples',
    'services',
    'version_specs',
)


class PlacementNormalizationAuthorityError(RuntimeError):
    """The exact revision-040 PostgreSQL authority could not be proved."""


@dataclasses.dataclass(frozen=True)
class PlacementNormalizationDatabaseAuthority:
    """Canonical schema and immutable terminal state proved by revision 040."""

    schema: str
    terminal_run_id: uuid.UUID | None

    @property
    def is_open(self) -> bool:
        return self.terminal_run_id is None


_REVISION_MODULE = (
    'sky.schemas.db.serve_state.040_placement_normalization_immutability')


def _qualified(connection: sqlalchemy.engine.Connection, schema: str,
               name: str) -> str:
    quote = connection.dialect.identifier_preparer.quote
    return f'{quote(schema)}.{quote(name)}'


def _resolve_authority_identity(
        connection: sqlalchemy.engine.Connection) -> tuple[str, str]:
    """Resolve one revision-owned schema and exact assertion definition."""
    rows = connection.execute(
        sqlalchemy.text("""
            SELECT namespace.nspname, assertion.prosrc,
                   assertion.prosecdef, assertion.provolatile,
                   assertion.proparallel, assertion.proisstrict,
                   assertion.proleakproof, assertion.proconfig,
                   NOT EXISTS (
                       SELECT 1
                       FROM pg_catalog.aclexplode(COALESCE(
                           assertion.proacl,
                           pg_catalog.acldefault('f', assertion.proowner)))
                            AS access
                       WHERE access.grantee <> assertion.proowner
                   ) AS owner_only_acl
            FROM pg_catalog.pg_namespace AS namespace
            JOIN pg_catalog.pg_proc AS assertion
              ON assertion.pronamespace = namespace.oid
             AND assertion.proname = :assertion_name
             AND assertion.pronargs = 0
             AND assertion.prokind = 'f'
             AND assertion.proretset = false
             AND assertion.prorettype =
                     'pg_catalog.bool'::pg_catalog.regtype
             AND pg_catalog.pg_get_function_identity_arguments(
                     assertion.oid) = ''
             AND pg_catalog.pg_get_function_result(assertion.oid) = 'boolean'
            JOIN pg_catalog.pg_language AS language
              ON language.oid = assertion.prolang
             AND language.lanname = 'plpgsql'
            JOIN pg_catalog.pg_class AS version_relation
              ON version_relation.relnamespace = namespace.oid
             AND version_relation.relname = :version_relation
             AND version_relation.relkind = 'r'
             AND version_relation.relpersistence = 'p'
             AND version_relation.relowner = assertion.proowner
            JOIN pg_catalog.pg_class AS gate_relation
              ON gate_relation.relnamespace = namespace.oid
             AND gate_relation.relname = :gate_relation
             AND gate_relation.relkind = 'r'
             AND gate_relation.relpersistence = 'p'
             AND gate_relation.relowner = assertion.proowner
            JOIN pg_catalog.pg_class AS runs_relation
              ON runs_relation.relnamespace = namespace.oid
             AND runs_relation.relname = :runs_relation
             AND runs_relation.relkind = 'r'
             AND runs_relation.relpersistence = 'p'
             AND runs_relation.relowner = assertion.proowner
            JOIN pg_catalog.pg_class AS rows_relation
              ON rows_relation.relnamespace = namespace.oid
             AND rows_relation.relname = :rows_relation
             AND rows_relation.relkind = 'r'
             AND rows_relation.relpersistence = 'p'
             AND rows_relation.relowner = assertion.proowner
            WHERE namespace.nspname <> 'pg_catalog'
              AND namespace.nspname <> 'information_schema'
              AND namespace.nspname !~ '^pg_(temp|toast)'
            ORDER BY namespace.nspname, assertion.oid
            """), {
            'assertion_name': AUTHORITY_FUNCTION,
            'version_relation': VERSION_RELATION,
            'gate_relation': AUTHORITY_GATE,
            'runs_relation': RUNS_RELATION,
            'rows_relation': ROWS_RELATION,
        }).mappings().all()
    if len(rows) != 1:
        raise PlacementNormalizationAuthorityError(
            'Expected exactly one persistent revision-040 placement-'
            f'normalization authority schema; found {len(rows)}.')
    row = rows[0]
    schema = row['nspname']
    body = row['prosrc']
    quote = connection.dialect.identifier_preparer.quote
    expected_config = (f'search_path=pg_catalog, {quote(schema)}',)
    if (type(schema) is not str or not schema or type(body) is not str or
            row['prosecdef'] is not True or row['provolatile'] != 'v' or
            row['proparallel'] != 'u' or row['proisstrict'] is not False or
            row['proleakproof'] is not False or tuple(row['proconfig'] or
                                                      ()) != expected_config or
            row['owner_only_acl'] is not True):
        raise PlacementNormalizationAuthorityError(
            'Revision-040 assertion function envelope is invalid.')
    return schema, body


def _verify_version_relation(connection: sqlalchemy.engine.Connection,
                             schema: str) -> None:
    """Verify the exact Alembic identity relation before trusting its row."""
    envelope = connection.execute(
        sqlalchemy.text("""
            SELECT relation.relkind, relation.relpersistence,
                   relation.relrowsecurity, relation.relforcerowsecurity,
                   relation.relispartition, relation.relreplident,
                   relation.relhassubclass, relation.relhasrules,
                   NOT EXISTS (
                       SELECT 1
                       FROM pg_catalog.aclexplode(COALESCE(
                           relation.relacl,
                           pg_catalog.acldefault('r', relation.relowner)))
                            AS access
                       WHERE access.grantee <> relation.relowner
                   ) AS owner_only_acl,
                   (SELECT count(*)
                    FROM pg_catalog.pg_attribute AS attribute
                    WHERE attribute.attrelid = relation.oid
                      AND attribute.attnum > 0) AS column_count,
                   EXISTS (
                       SELECT 1
                       FROM pg_catalog.pg_attribute AS attribute
                       WHERE attribute.attrelid = relation.oid
                         AND attribute.attnum = 1
                         AND attribute.attname = 'version_num'
                         AND attribute.atttypid =
                             'pg_catalog.varchar'::pg_catalog.regtype
                         AND attribute.atttypmod = 36
                         AND attribute.attcollation =
                             'pg_catalog.default'::pg_catalog.regcollation
                         AND attribute.attnotnull
                         AND attribute.attidentity = ''
                         AND attribute.attgenerated = ''
                         AND attribute.attacl IS NULL
                         AND NOT attribute.attisdropped
                         AND attribute.attinhcount = 0
                         AND attribute.attislocal
                   ) AS exact_column,
                   NOT EXISTS (
                       SELECT 1 FROM pg_catalog.pg_attrdef AS definition
                       WHERE definition.adrelid = relation.oid
                   ) AS no_defaults,
                   (SELECT count(*)
                    FROM pg_catalog.pg_constraint AS constraint_row
                    WHERE constraint_row.conrelid = relation.oid
                   ) = 1 AS one_constraint,
                   EXISTS (
                       SELECT 1
                       FROM pg_catalog.pg_constraint AS primary_key
                       WHERE primary_key.conrelid = relation.oid
                         AND primary_key.connamespace = namespace.oid
                         AND primary_key.conname =
                             'alembic_version_serve_state_db_pkc'
                         AND primary_key.contype = 'p'
                         AND primary_key.conkey = ARRAY[1]::smallint[]
                         AND NOT primary_key.condeferrable
                         AND NOT primary_key.condeferred
                         AND primary_key.convalidated
                         AND primary_key.conparentid = 0
                   ) AS exact_primary_key,
                   (SELECT count(*)
                    FROM pg_catalog.pg_index AS index_row
                    WHERE index_row.indrelid = relation.oid
                   ) = 1 AS one_index,
                   EXISTS (
                       SELECT 1
                       FROM pg_catalog.pg_index AS index_row
                       JOIN pg_catalog.pg_class AS index_relation
                         ON index_relation.oid = index_row.indexrelid
                       JOIN pg_catalog.pg_constraint AS primary_key
                         ON primary_key.conindid = index_row.indexrelid
                       WHERE index_row.indrelid = relation.oid
                         AND primary_key.conrelid = relation.oid
                         AND primary_key.conname =
                             'alembic_version_serve_state_db_pkc'
                         AND index_relation.relnamespace = namespace.oid
                         AND index_relation.relname =
                             'alembic_version_serve_state_db_pkc'
                         AND index_relation.relowner = relation.relowner
                         AND index_relation.relkind = 'i'
                         AND index_relation.relpersistence = 'p'
                         AND index_relation.relam = (
                             SELECT access_method.oid
                             FROM pg_catalog.pg_am AS access_method
                             WHERE access_method.amname = 'btree')
                         AND index_relation.reloptions IS NULL
                         AND index_row.indisunique
                         AND index_row.indisprimary
                         AND index_row.indisvalid
                         AND index_row.indisready
                         AND index_row.indislive
                         AND index_row.indimmediate
                         AND NOT index_row.indisexclusion
                         AND NOT index_row.indisclustered
                         AND NOT index_row.indisreplident
                         AND NOT index_row.indcheckxmin
                         AND NOT COALESCE(
                             (pg_catalog.to_jsonb(index_row) ->>
                                 'indnullsnotdistinct')::boolean,
                             false)
                         AND index_row.indnatts = 1
                         AND index_row.indnkeyatts = 1
                         AND index_row.indkey::text = '1'
                         AND index_row.indexprs IS NULL
                         AND index_row.indpred IS NULL
                         AND NOT EXISTS (
                             SELECT 1
                             FROM pg_catalog.aclexplode(COALESCE(
                                 index_relation.relacl,
                                 pg_catalog.acldefault(
                                     'r', index_relation.relowner))) AS access
                             WHERE access.grantee <> index_relation.relowner)
                         AND NOT EXISTS (
                             SELECT 1
                             FROM pg_catalog.unnest(
                                 index_row.indclass::oid[])
                                  AS opclass_oid(oid)
                             JOIN pg_catalog.pg_opclass AS opclass
                               ON opclass.oid = opclass_oid.oid
                             WHERE NOT opclass.opcdefault
                                OR opclass.opcmethod <>
                                       index_relation.relam)
                         AND NOT EXISTS (
                             SELECT 1
                             FROM pg_catalog.unnest(
                                 index_row.indkey::smallint[])
                                  WITH ORDINALITY
                                  AS key_column(attnum, position)
                             JOIN pg_catalog.unnest(
                                 index_row.indcollation::oid[])
                                  WITH ORDINALITY
                                  AS key_collation(
                                      collation_oid, position)
                               USING (position)
                             JOIN pg_catalog.pg_attribute AS attribute
                               ON attribute.attrelid = index_row.indrelid
                              AND attribute.attnum = key_column.attnum
                             WHERE key_collation.collation_oid <>
                                       attribute.attcollation)
                         AND NOT EXISTS (
                             SELECT 1
                             FROM pg_catalog.unnest(
                                 index_row.indoption::smallint[])
                                  AS index_option(value)
                             WHERE index_option.value <> 0)
                   ) AS exact_primary_index,
                   NOT EXISTS (
                       SELECT 1 FROM pg_catalog.pg_trigger AS trigger
                       WHERE trigger.tgrelid = relation.oid
                         AND NOT trigger.tgisinternal
                   ) AS no_triggers,
                   NOT EXISTS (
                       SELECT 1 FROM pg_catalog.pg_inherits AS inheritance
                       WHERE inheritance.inhrelid = relation.oid
                          OR inheritance.inhparent = relation.oid
                   ) AS no_inheritance,
                   NOT EXISTS (
                       SELECT 1 FROM pg_catalog.pg_rewrite AS rewrite
                       WHERE rewrite.ev_class = relation.oid
                   ) AS no_rewrites
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = :schema
              AND relation.relname = :relation
            """), {
            'schema': schema,
            'relation': VERSION_RELATION,
        }).mappings().one_or_none()
    if envelope is None or any((
            envelope['relkind'] != 'r',
            envelope['relpersistence'] != 'p',
            envelope['relrowsecurity'] is not False,
            envelope['relforcerowsecurity'] is not False,
            envelope['relispartition'] is not False,
            envelope['relreplident'] != 'd',
            envelope['relhassubclass'] is not False,
            envelope['relhasrules'] is not False,
            envelope['owner_only_acl'] is not True,
            envelope['column_count'] != 1,
            envelope['exact_column'] is not True,
            envelope['no_defaults'] is not True,
            envelope['one_constraint'] is not True,
            envelope['exact_primary_key'] is not True,
            envelope['one_index'] is not True,
            envelope['exact_primary_index'] is not True,
            envelope['no_triggers'] is not True,
            envelope['no_inheritance'] is not True,
            envelope['no_rewrites'] is not True,
    )):
        raise PlacementNormalizationAuthorityError(
            'Revision-040 Alembic version relation envelope is invalid.')
    version_relation = _qualified(connection, schema, VERSION_RELATION)
    revisions = connection.exec_driver_sql(
        f'SELECT version_num FROM {version_relation} '
        'ORDER BY version_num').scalars().all()
    if (len(revisions) != 1 or
            revisions[0] not in RECOGNIZED_ADDITIVE_REVISIONS):
        raise PlacementNormalizationAuthorityError(
            'Placement-normalization authority schema does not own one '
            'recognized additive revision at or after '
            f'{AUTHORITY_REVISION}; found {revisions!r}.')


def _expected_assertion_body(connection: sqlalchemy.engine.Connection,
                             schema: str) -> str:
    """Regenerate the body from the compiled frozen revision-040 contract."""
    try:
        revision_module = importlib.import_module(_REVISION_MODULE)
        expected = revision_module.expected_runtime_assertion_body(
            connection, schema)
    except (ImportError, AttributeError, RuntimeError,
            sqlalchemy.exc.SQLAlchemyError) as exc:
        raise PlacementNormalizationAuthorityError(
            'Compiled revision-040 assertion contract is unavailable.') from exc
    if type(expected) is not str or not expected:
        raise PlacementNormalizationAuthorityError(
            'Compiled revision-040 assertion body is invalid.')
    return expected


def _reject_temporary_shadows(connection: sqlalchemy.engine.Connection,
                              protected_relations: tuple[str, ...]) -> None:
    relation_parameters = {
        f'relation_{index}': relation
        for index, relation in enumerate(protected_relations)
    }
    relation_placeholders = ', '.join(
        f':relation_{index}' for index in range(len(protected_relations)))
    shadows = connection.execute(
        sqlalchemy.text("""
            SELECT object_kind, object_name
            FROM (
                SELECT 'relation'::text AS object_kind,
                       relation.relname AS object_name
                FROM pg_catalog.pg_class AS relation
                WHERE relation.relnamespace = pg_catalog.pg_my_temp_schema()
                  AND relation.relname IN (""" + relation_placeholders + """)
                UNION ALL
                SELECT 'function'::text AS object_kind,
                       procedure.proname AS object_name
                FROM pg_catalog.pg_proc AS procedure
                WHERE procedure.pronamespace = pg_catalog.pg_my_temp_schema()
                  AND procedure.proname = :assertion_name
            ) AS shadow
            ORDER BY object_kind, object_name
            """), {
            **relation_parameters,
            'assertion_name': AUTHORITY_FUNCTION,
        }).all()
    if shadows:
        raise PlacementNormalizationAuthorityError(
            'Placement-normalization session contains temporary authority '
            f'shadows: {[(row[0], row[1]) for row in shadows]!r}.')


def _assert_database_authority(
    connection: sqlalchemy.engine.Connection,
    *,
    protected_relations: tuple[str, ...],
    require_open: bool,
) -> PlacementNormalizationDatabaseAuthority:
    """Prove revision 040 on this connection and return its canonical state."""
    if connection.dialect.name != 'postgresql':
        raise PlacementNormalizationAuthorityError(
            'Placement-normalization database authority is PostgreSQL-only.')
    if not protected_relations or any(
            type(name) is not str or not name for name in protected_relations):
        raise ValueError('protected_relations must be non-empty names.')
    try:
        # Catalog proof must not inherit caller-controlled function, operator,
        # aggregate, or type resolution.  The canonical application tables are
        # schema-bound separately below.
        connection.exec_driver_sql(
            "SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)")
        schema, observed_body = _resolve_authority_identity(connection)
        _verify_version_relation(connection, schema)
        expected_body = _expected_assertion_body(connection, schema)
        if observed_body != expected_body:
            raise PlacementNormalizationAuthorityError(
                'Revision-040 assertion body differs from the compiled '
                'contract.')
        _reject_temporary_shadows(connection, protected_relations)
        gate = _qualified(connection, schema, AUTHORITY_GATE)
        connection.exec_driver_sql(f'LOCK TABLE {gate} IN ACCESS SHARE MODE')
        locked_schema, locked_body = _resolve_authority_identity(connection)
        if locked_schema != schema or locked_body != expected_body:
            raise PlacementNormalizationAuthorityError(
                'Revision-040 authority identity changed while acquiring its '
                'gate lock.')
        _verify_version_relation(connection, schema)
        authority_function = _qualified(connection, schema, AUTHORITY_FUNCTION)
        asserted = connection.exec_driver_sql(
            f'SELECT {authority_function}()').scalar_one()
        if asserted is not True:
            raise PlacementNormalizationAuthorityError(
                'Revision-040 database authority assertion did not return '
                'true.')
        gate_rows = connection.exec_driver_sql(
            'SELECT terminal_run_id '
            f'FROM {gate} WHERE singleton IS TRUE').scalars().all()
    except sqlalchemy.exc.SQLAlchemyError as exc:
        raise PlacementNormalizationAuthorityError(
            'Placement-normalization database authority is absent or '
            'invalid.') from exc
    if len(gate_rows) != 1:
        raise PlacementNormalizationAuthorityError(
            'Revision-040 database authority has no exact singleton gate.')
    terminal_run_id = gate_rows[0]
    if terminal_run_id is not None and not isinstance(terminal_run_id,
                                                      uuid.UUID):
        raise PlacementNormalizationAuthorityError(
            'Revision-040 terminal run identity is not a UUID.')
    authority = PlacementNormalizationDatabaseAuthority(
        schema=schema, terminal_run_id=terminal_run_id)
    if require_open and not authority.is_open:
        raise PlacementNormalizationAuthorityError(
            'Revision-040 placement-normalization authority is terminal.')
    return authority


def assert_reader_database_authority(
    connection: sqlalchemy.engine.Connection
) -> PlacementNormalizationDatabaseAuthority:
    """Prove exact open-or-terminal authority for one canonical read."""
    return _assert_database_authority(connection,
                                      protected_relations=OPERATOR_RELATIONS,
                                      require_open=False)


def assert_writer_database_authority(
        connection: sqlalchemy.engine.Connection,
        lock_name: str) -> PlacementNormalizationDatabaseAuthority:
    """Prove exact open authority and the writer's session advisory lock."""
    authority = _assert_database_authority(
        connection, protected_relations=OPERATOR_RELATIONS, require_open=True)
    reassert_writer_session_lock(connection, lock_name)
    return authority


def assert_downgrade_database_authority(
    connection: sqlalchemy.engine.Connection
) -> PlacementNormalizationDatabaseAuthority:
    """Prove the exact open authority targeted by revision-040 downgrade."""
    return _assert_database_authority(connection,
                                      protected_relations=OPERATOR_RELATIONS,
                                      require_open=True)


def assert_writer_session_lock(connection: sqlalchemy.engine.Connection,
                               lock_name: str) -> None:
    """Require this PostgreSQL backend to own the exact bigint advisory key."""
    if not isinstance(lock_name, str) or not lock_name:
        raise ValueError('lock_name must be a non-empty string.')
    owned = connection.execute(
        sqlalchemy.text("""
            WITH expected AS (
                SELECT pg_catalog.hashtextextended(:lock_name, 0) AS lock_key
            )
            SELECT count(*)
            FROM pg_catalog.pg_locks AS held, expected
            WHERE held.locktype = 'advisory'
              AND held.pid = pg_catalog.pg_backend_pid()
              AND held.mode = 'ExclusiveLock'
              AND held.granted
              AND held.objsubid = 1
              AND held.classid::bigint =
                    ((expected.lock_key >> 32) & 4294967295::bigint)
              AND held.objid::bigint =
                    (expected.lock_key & 4294967295::bigint)
            """), {
            'lock_name': lock_name
        }).scalar_one()
    if owned != 1:
        raise PlacementNormalizationAuthorityError(
            'Placement-normalization writer session does not own its exact '
            'advisory authority.')


def reassert_writer_session_lock(connection: sqlalchemy.engine.Connection,
                                 lock_name: str) -> None:
    """Actively prove exactly one session hold and restore it atomically.

    PostgreSQL merges identical session- and transaction-level advisory
    ownership into one ``pg_locks`` row, so a passive catalog query cannot
    prove the session form exists or detect a reentrant stale session hold.
    Acquire the transaction form first, release exactly one session hold,
    require a second release to find none, and then restore one session hold.
    The transaction hold prevents another backend from stealing the key while
    it is being probed.
    """
    if not isinstance(lock_name, str) or not lock_name:
        raise ValueError('lock_name must be a non-empty string.')
    transaction_hold = connection.execute(
        sqlalchemy.text('SELECT pg_catalog.pg_try_advisory_xact_lock('
                        'pg_catalog.hashtextextended(:lock_name, 0))'), {
                            'lock_name': lock_name
                        }).scalar_one()
    if transaction_hold is not True:
        raise PlacementNormalizationAuthorityError(
            'Placement-normalization writer could not fence its session '
            'advisory authority probe.')
    released = connection.execute(
        sqlalchemy.text('SELECT pg_catalog.pg_advisory_unlock('
                        'pg_catalog.hashtextextended(:lock_name, 0))'), {
                            'lock_name': lock_name
                        }).scalar_one()
    if released is not True:
        raise PlacementNormalizationAuthorityError(
            'Placement-normalization writer lost its session advisory '
            'authority.')
    released_extra = connection.execute(
        sqlalchemy.text('SELECT pg_catalog.pg_advisory_unlock('
                        'pg_catalog.hashtextextended(:lock_name, 0))'), {
                            'lock_name': lock_name
                        }).scalar_one()
    if released_extra is not False:
        raise PlacementNormalizationAuthorityError(
            'Placement-normalization writer owned multiple session advisory '
            'authority holds.')
    reacquired = connection.execute(
        sqlalchemy.text('SELECT pg_catalog.pg_try_advisory_lock('
                        'pg_catalog.hashtextextended(:lock_name, 0))'), {
                            'lock_name': lock_name
                        }).scalar_one()
    if reacquired is not True:
        raise PlacementNormalizationAuthorityError(
            'Placement-normalization writer could not restore its session '
            'advisory authority.')
    assert_writer_session_lock(connection, lock_name)


def release_writer_session_lock(connection: sqlalchemy.engine.Connection,
                                lock_name: str) -> None:
    """Release exactly one writer hold and prove no session hold remains."""
    if not isinstance(lock_name, str) or not lock_name:
        raise ValueError('lock_name must be a non-empty string.')
    released = connection.execute(
        sqlalchemy.text('SELECT pg_catalog.pg_advisory_unlock('
                        'pg_catalog.hashtextextended(:lock_name, 0))'), {
                            'lock_name': lock_name
                        }).scalar_one()
    if released is not True:
        raise PlacementNormalizationAuthorityError(
            'Placement-normalization writer session advisory authority was '
            'already absent.')
    released_extra = connection.execute(
        sqlalchemy.text('SELECT pg_catalog.pg_advisory_unlock('
                        'pg_catalog.hashtextextended(:lock_name, 0))'), {
                            'lock_name': lock_name
                        }).scalar_one()
    if released_extra is not False:
        raise PlacementNormalizationAuthorityError(
            'Placement-normalization writer retained an extra session '
            'advisory authority hold.')


def bind_session_to_authority(
        session: orm.Session,
        authority: PlacementNormalizationDatabaseAuthority) -> None:
    """Bind every schema-less SQLAlchemy table to the proved schema."""
    if not isinstance(authority, PlacementNormalizationDatabaseAuthority):
        raise TypeError('authority must be a database-authority value.')
    connection = session.connection().execution_options(
        schema_translate_map={None: authority.schema})
    connection.exec_driver_sql(
        "SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)")
