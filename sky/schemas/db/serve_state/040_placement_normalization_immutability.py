"""Make the placement-normalization ledger an append-only authority.

Revision ID: 040
Revises: 039
Create Date: 2026-08-08

"""
# pylint: disable=invalid-name
from collections.abc import Iterator
from collections.abc import Sequence
import contextlib
import dataclasses
import re
from typing import Any
import uuid

from alembic import op
import sqlalchemy as sa

from sky.serve import placement_normalization_authority
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '040'
down_revision: str | Sequence[str] | None = '039'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_RUNS = 'placement_normalization_runs'
_ROWS = 'placement_normalization_rows'
_GATE = 'placement_normalization_write_fence'
_SERVICES = 'services'
_VERSION_SPECS = 'version_specs'
_ADVISORY_LOCK_NAME = 'skyserve-placement-contract-normalization-v1'

_IMMUTABILITY_FUNCTION = ('skyserve040_reject_placement_normalization_mutation')
_RUN_LOCK_FUNCTION = 'skyserve040_lock_placement_normalization_run'
_RUN_ADMISSION_FUNCTION = 'skyserve040_admit_placement_normalization_run'
_TERMINAL_ACTIVATION_FUNCTION = (
    'skyserve040_activate_terminal_normalization_run')
_GATE_DML_GUARD_FUNCTION = (
    'skyserve040_reject_placement_normalization_fence_dml')
_GATE_UPDATE_GUARD_FUNCTION = (
    'skyserve040_guard_placement_normalization_fence_update')
_RUNTIME_ASSERT_FUNCTION = (
    'skyserve040_assert_placement_normalization_authority')

_RUN_IMMUTABILITY_TRIGGER = (
    'skyserve040_placement_normalization_runs_immutable')
_ROW_IMMUTABILITY_TRIGGER = (
    'skyserve040_placement_normalization_rows_immutable')
_RUN_LOCK_TRIGGER = 'skyserve040_placement_normalization_runs_lock'
_RUN_ADMISSION_TRIGGER = 'skyserve040_placement_normalization_runs_admit'
_TERMINAL_ACTIVATION_TRIGGER = (
    'skyserve040_placement_normalization_rows_activate_terminal')
_GATE_DML_GUARD_TRIGGER = (
    'skyserve040_placement_normalization_write_fence_dml_guard')
_GATE_UPDATE_GUARD_TRIGGER = (
    'skyserve040_placement_normalization_write_fence_update_guard')

_GATE_PRIMARY_KEY = 'skyserve040_normalization_fence_pk'
_GATE_SINGLETON_CHECK = 'skyserve040_normalization_fence_singleton_ck'
_GATE_GENERATION_CHECK = 'skyserve040_normalization_fence_generation_ck'
_GATE_ADMISSION_PAIR_CHECK = (
    'skyserve040_normalization_fence_admission_pair_ck')
_GATE_GENERATION_SHAPE_CHECK = (
    'skyserve040_normalization_fence_generation_shape_ck')
_GATE_TERMINAL_PAIR_CHECK = ('skyserve040_normalization_fence_terminal_pair_ck')
_GATE_TERMINAL_SHAPE_CHECK = (
    'skyserve040_normalization_fence_terminal_shape_ck')
_GATE_LATEST_RUN_FOREIGN_KEY = ('skyserve040_normalization_fence_latest_run_fk')
_GATE_TERMINAL_RUN_FOREIGN_KEY = (
    'skyserve040_normalization_fence_terminal_run_fk')

_APPEND_ONLY_MESSAGE = (
    'SkyServe placement-normalization ledger is append-only')
_BUSY_MESSAGE = 'SkyServe placement-normalization authority is busy'
_TERMINAL_FENCE_MESSAGE = (
    'SkyServe terminal placement-normalization fence forbids later runs')
_TERMINAL_ACTIVATION_MESSAGE = (
    'SkyServe terminal placement-normalization activation is invalid or late')
_PRIVATE_GATE_MESSAGE = (
    'SkyServe placement-normalization write fence is migration-private')
_RUNTIME_ASSERT_MESSAGE = (
    'SkyServe placement-normalization database authority is absent or invalid')
_TERMINAL_DOWNGRADE_MESSAGE = (
    'SkyServe schema 040 cannot be downgraded after terminal retirement')

_REQUIRED_COLUMNS = {
    _RUNS: frozenset({'run_id', 'mode', 'normalizer_version'}),
    _ROWS: frozenset({
        'run_id',
        'service_name',
        'version',
        'classification',
        'outcome',
    }),
}

_GATE_CHECK_EXPRESSIONS = {
    _GATE_SINGLETON_CHECK: 'singleton',
    _GATE_GENERATION_CHECK: 'generation >= 0',
    _GATE_ADMISSION_PAIR_CHECK: '(latest_run_id IS NULL) = (admitted_xid IS NULL)',
    _GATE_GENERATION_SHAPE_CHECK:
        '(generation = 0) = '
        '(latest_run_id IS NULL AND admitted_xid IS NULL)',
    _GATE_TERMINAL_PAIR_CHECK: '(terminal_run_id IS NULL) = (terminal_xid IS NULL)',
    _GATE_TERMINAL_SHAPE_CHECK:
        'terminal_run_id IS NULL OR '
        '(latest_run_id IS NOT NULL AND terminal_run_id = latest_run_id '
        'AND admitted_xid IS NOT NULL AND terminal_xid = admitted_xid)',
}

_GATE_COLUMN_NAMES = (
    'singleton',
    'generation',
    'latest_run_id',
    'admitted_xid',
    'terminal_run_id',
    'terminal_xid',
)

_RUN_COLUMN_NAMES = (
    'run_id',
    'mode',
    'normalizer_version',
    'schema_revision',
    'release_version',
    'started_at',
    'completed_at',
    'row_bound',
    'row_count',
    'classification_counts',
    'pre_inventory_sha256',
    'post_inventory_sha256',
    'freeze_evidence_sha256',
)
_ROW_COLUMN_NAMES = (
    'run_id',
    'service_name',
    'version',
    'classification',
    'outcome',
    'original_spec_sha256',
    'result_spec_sha256',
    'original_row_sha256',
    'result_row_sha256',
    'original_column_sha256s',
    'result_column_sha256s',
    'contract_projection',
    'service_hash',
    'service_lifecycle_epoch',
    'dependency_facts',
)

_RUN_PRIMARY_KEY = 'placement_normalization_runs_pkey'
_ROW_PRIMARY_KEY = 'placement_normalization_rows_pkey'
_ROW_RUN_FOREIGN_KEY = 'fk_placement_normalization_rows_run'
_ROW_VERSION_INDEX = 'placement_normalization_rows_version_idx'


@dataclasses.dataclass(frozen=True)
class _TriggerContract:
    relation: str
    function: str
    trigger_type: int
    constraint: bool = False
    deferrable: bool = False
    initially_deferred: bool = False


@dataclasses.dataclass(frozen=True)
class _ForeignKeyContract:
    referencing_relation: str
    referencing_columns: tuple[str, ...]
    referenced_relation: str
    referenced_columns: tuple[str, ...]
    update_action: str = 'a'
    delete_action: str = 'r'
    match_type: str = 's'


@dataclasses.dataclass(frozen=True)
class _RITriggerSemantic:
    function: str
    source_is_referencing: bool
    trigger_type: int
    generated_name_kind: str


# pg_trigger.tgtype bits: ROW=1, BEFORE=2, INSERT=4, DELETE=8,
# UPDATE=16, and TRUNCATE=32.  AFTER is represented by the absence of BEFORE.
_TRIGGER_CONTRACTS = {
    _RUN_IMMUTABILITY_TRIGGER: _TriggerContract(_RUNS, _IMMUTABILITY_FUNCTION,
                                                2 + 8 + 16 + 32),
    _ROW_IMMUTABILITY_TRIGGER: _TriggerContract(_ROWS, _IMMUTABILITY_FUNCTION,
                                                2 + 8 + 16 + 32),
    _RUN_LOCK_TRIGGER: _TriggerContract(_RUNS, _RUN_LOCK_FUNCTION, 1 + 2 + 4),
    _RUN_ADMISSION_TRIGGER: _TriggerContract(_RUNS, _RUN_ADMISSION_FUNCTION,
                                             1 + 4),
    _TERMINAL_ACTIVATION_TRIGGER: _TriggerContract(
        _ROWS,
        _TERMINAL_ACTIVATION_FUNCTION,
        1 + 4,
        constraint=True,
        deferrable=True,
        initially_deferred=True),
    _GATE_DML_GUARD_TRIGGER: _TriggerContract(_GATE, _GATE_DML_GUARD_FUNCTION,
                                              2 + 4 + 8 + 32),
    _GATE_UPDATE_GUARD_TRIGGER: _TriggerContract(_GATE,
                                                 _GATE_UPDATE_GUARD_FUNCTION,
                                                 1 + 2 + 16),
}

_FOREIGN_KEY_CONTRACTS = {
    _GATE_LATEST_RUN_FOREIGN_KEY: _ForeignKeyContract(_GATE, ('latest_run_id',),
                                                      _RUNS, ('run_id',)),
    _GATE_TERMINAL_RUN_FOREIGN_KEY: _ForeignKeyContract(_GATE,
                                                        ('terminal_run_id',),
                                                        _RUNS, ('run_id',)),
    _ROW_RUN_FOREIGN_KEY: _ForeignKeyContract(_ROWS, ('run_id',), _RUNS,
                                              ('run_id',)),
}
_REFERENCED_AUTHORITY_FOREIGN_KEY_CONTRACTS = {
    'fk_services_placement_normalization_requested_run': _ForeignKeyContract(
        _SERVICES, ('placement_normalization_requested_run_id',), _RUNS,
        ('run_id',)),
    'fk_services_placement_normalization_loaded_run': _ForeignKeyContract(
        _SERVICES, ('placement_normalization_loaded_run_id',), _RUNS,
        ('run_id',)),
    'fk_version_specs_retirement_run': _ForeignKeyContract(
        _VERSION_SPECS, ('retirement_run_id',), _RUNS, ('run_id',)),
}
_ALL_AUDITED_FOREIGN_KEY_CONTRACTS = {
    **_FOREIGN_KEY_CONTRACTS,
    **_REFERENCED_AUTHORITY_FOREIGN_KEY_CONTRACTS,
}
_RI_TRIGGER_SEMANTICS = (
    _RITriggerSemantic('RI_FKey_check_ins', True, 1 + 4, 'c'),
    _RITriggerSemantic('RI_FKey_check_upd', True, 1 + 16, 'c'),
    _RITriggerSemantic('RI_FKey_restrict_del', False, 1 + 8, 'a'),
    _RITriggerSemantic('RI_FKey_noaction_upd', False, 1 + 16, 'a'),
)
_REFERENCED_SIDE_RI_TRIGGER_SEMANTICS = tuple(
    semantic for semantic in _RI_TRIGGER_SEMANTICS
    if not semantic.source_is_referencing)
_EXPECTED_RI_TRIGGER_COUNT = (len(_FOREIGN_KEY_CONTRACTS) *
                              len(_RI_TRIGGER_SEMANTICS))
_EXPECTED_REFERENCED_AUTHORITY_RI_TRIGGER_COUNT = (
    len(_REFERENCED_AUTHORITY_FOREIGN_KEY_CONTRACTS) *
    len(_REFERENCED_SIDE_RI_TRIGGER_SEMANTICS))
_EXPECTED_AUTHORITY_RELATION_INTERNAL_TRIGGER_COUNT = (
    _EXPECTED_RI_TRIGGER_COUNT +
    _EXPECTED_REFERENCED_AUTHORITY_RI_TRIGGER_COUNT)

_TRIGGER_FUNCTION_NAMES = frozenset({
    _IMMUTABILITY_FUNCTION,
    _RUN_LOCK_FUNCTION,
    _RUN_ADMISSION_FUNCTION,
    _TERMINAL_ACTIVATION_FUNCTION,
    _GATE_DML_GUARD_FUNCTION,
    _GATE_UPDATE_GUARD_FUNCTION,
})
_FUNCTION_NAMES = _TRIGGER_FUNCTION_NAMES | frozenset(
    {_RUNTIME_ASSERT_FUNCTION})


def _quote(bind: sa.engine.Connection, identifier: str) -> str:
    return bind.dialect.identifier_preparer.quote(identifier)


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@contextlib.contextmanager
def _frozen_catalog_search_path(bind: sa.engine.Connection) -> Iterator[None]:
    """Compile expected PostgreSQL catalogs without caller name resolution."""
    previous = bind.execute(
        sa.text(
            "SELECT pg_catalog.current_setting('search_path')")).scalar_one()
    bind.execute(
        sa.text("SELECT pg_catalog.set_config('search_path', "
                "'pg_catalog, pg_temp', true)"))
    try:
        yield
    except BaseException:
        # PostgreSQL rejects every statement after a transaction-level error,
        # so a best-effort restore must not replace the original failure.
        try:
            bind.execute(
                sa.text("SELECT pg_catalog.set_config("
                        "'search_path', :search_path, true)"),
                {'search_path': previous})
        except sa.exc.DBAPIError:
            pass
        raise
    else:
        bind.execute(
            sa.text("SELECT pg_catalog.set_config("
                    "'search_path', :search_path, true)"),
            {'search_path': previous})


def _current_schema(bind: sa.engine.Connection) -> str:
    schema = bind.execute(
        sa.text('SELECT pg_catalog.current_schema()')).scalar_one()
    if not isinstance(schema, str) or not schema:
        raise RuntimeError(
            'SkyServe schema 040 requires one explicit current schema.')
    return schema


def _qualified(bind: sa.engine.Connection, schema: str, name: str) -> str:
    return f'{_quote(bind, schema)}.{_quote(bind, name)}'


def _append_only_body() -> str:
    return f"""
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        MESSAGE = '{_APPEND_ONLY_MESSAGE}';
    RETURN NULL;
END;
""".strip()


def _run_lock_body() -> str:
    return f"""
BEGIN
    IF NOT pg_catalog.pg_try_advisory_xact_lock(
        pg_catalog.hashtextextended('{_ADVISORY_LOCK_NAME}', 0)) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = '{_BUSY_MESSAGE}';
    END IF;
    RETURN NEW;
END;
""".strip()


def _run_admission_body(bind: sa.engine.Connection, schema: str) -> str:
    gate = _qualified(bind, schema, _GATE)
    return f"""
DECLARE
    admitted_generation bigint;
    current_xid xid8 := pg_catalog.pg_current_xact_id();
BEGIN
    UPDATE {gate} AS write_fence
    SET generation = write_fence.generation + 1,
        latest_run_id = NEW.run_id,
        admitted_xid = current_xid
    WHERE write_fence.singleton
      AND write_fence.terminal_run_id IS NULL
      AND write_fence.terminal_xid IS NULL
      AND write_fence.admitted_xid IS DISTINCT FROM current_xid
    RETURNING write_fence.generation INTO admitted_generation;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = '{_TERMINAL_FENCE_MESSAGE}';
    END IF;
    RETURN NEW;
END;
""".strip()


def _terminal_activation_body(bind: sa.engine.Connection, schema: str) -> str:
    gate = _qualified(bind, schema, _GATE)
    runs = _qualified(bind, schema, _RUNS)
    rows = _qualified(bind, schema, _ROWS)
    return f"""
DECLARE
    activated_run_id uuid;
    current_xid xid8 := pg_catalog.pg_current_xact_id();
BEGIN
    IF NEW.classification <> 'historical_physical_per_gpu'
       AND NEW.outcome <> 'retired' THEN
        RETURN NEW;
    END IF;
    IF NEW.classification IS DISTINCT FROM 'historical_physical_per_gpu'
       OR NEW.outcome IS DISTINCT FROM 'retired' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = '{_TERMINAL_ACTIVATION_MESSAGE}';
    END IF;
    UPDATE {gate} AS write_fence
    SET terminal_run_id = NEW.run_id,
        terminal_xid = current_xid
    WHERE write_fence.singleton
      AND write_fence.latest_run_id = NEW.run_id
      AND write_fence.admitted_xid = current_xid
      AND ((write_fence.terminal_run_id IS NULL
            AND write_fence.terminal_xid IS NULL)
           OR (write_fence.terminal_run_id = NEW.run_id
               AND write_fence.terminal_xid = current_xid))
      AND EXISTS (
          SELECT 1
          FROM {runs} AS terminal_run
          WHERE terminal_run.run_id = NEW.run_id
            AND terminal_run.mode = 'retire_terminal_historical'
            AND terminal_run.normalizer_version ~ '^4:[0-9a-f]{{40}}$'
            AND terminal_run.xmin = current_xid::xid
      )
      AND EXISTS (
          SELECT 1
          FROM {rows} AS terminal_row
          WHERE terminal_row.run_id = NEW.run_id
            AND terminal_row.service_name = NEW.service_name
            AND terminal_row.version = NEW.version
            AND terminal_row.classification =
                'historical_physical_per_gpu'
            AND terminal_row.outcome = 'retired'
            AND terminal_row.xmin = current_xid::xid
      )
    RETURNING write_fence.terminal_run_id INTO activated_run_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = '{_TERMINAL_ACTIVATION_MESSAGE}';
    END IF;
    RETURN NEW;
END;
""".strip()


def _gate_dml_guard_body() -> str:
    return f"""
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        MESSAGE = '{_PRIVATE_GATE_MESSAGE}';
    RETURN NULL;
END;
""".strip()


def _gate_update_guard_body(bind: sa.engine.Connection, schema: str) -> str:
    runs = _qualified(bind, schema, _RUNS)
    rows = _qualified(bind, schema, _ROWS)
    return f"""
DECLARE
    current_xid xid8 := pg_catalog.pg_current_xact_id();
    admission_transition boolean;
    activation_transition boolean;
BEGIN
    admission_transition :=
        pg_catalog.pg_trigger_depth() = 2
        AND OLD.singleton AND NEW.singleton
        AND OLD.terminal_run_id IS NULL
        AND OLD.terminal_xid IS NULL
        AND NEW.terminal_run_id IS NULL
        AND NEW.terminal_xid IS NULL
        AND NEW.generation = OLD.generation + 1
        AND NEW.latest_run_id IS NOT NULL
        AND NEW.latest_run_id IS DISTINCT FROM OLD.latest_run_id
        AND NEW.admitted_xid = current_xid
        AND EXISTS (
            SELECT 1
            FROM {runs} AS source_run
            WHERE source_run.run_id = NEW.latest_run_id
              AND source_run.xmin = current_xid::xid
        );
    activation_transition :=
        pg_catalog.pg_trigger_depth() = 2
        AND OLD.singleton AND NEW.singleton
        AND NEW.generation = OLD.generation
        AND NEW.latest_run_id IS NOT DISTINCT FROM OLD.latest_run_id
        AND NEW.admitted_xid IS NOT DISTINCT FROM OLD.admitted_xid
        AND NEW.latest_run_id IS NOT NULL
        AND NEW.admitted_xid = current_xid
        AND NEW.terminal_run_id = NEW.latest_run_id
        AND NEW.terminal_xid = current_xid
        AND ((OLD.terminal_run_id IS NULL AND OLD.terminal_xid IS NULL)
             OR (OLD.terminal_run_id = NEW.terminal_run_id
                 AND OLD.terminal_xid = NEW.terminal_xid))
        AND EXISTS (
            SELECT 1
            FROM {runs} AS terminal_run
            WHERE terminal_run.run_id = NEW.latest_run_id
              AND terminal_run.mode = 'retire_terminal_historical'
              AND terminal_run.normalizer_version ~ '^4:[0-9a-f]{{40}}$'
              AND terminal_run.xmin = current_xid::xid
        )
        AND EXISTS (
            SELECT 1
            FROM {rows} AS terminal_row
            WHERE terminal_row.run_id = NEW.latest_run_id
              AND terminal_row.classification =
                  'historical_physical_per_gpu'
              AND terminal_row.outcome = 'retired'
              AND terminal_row.xmin = current_xid::xid
        );
    IF NOT admission_transition AND NOT activation_transition THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = '{_PRIVATE_GATE_MESSAGE}';
    END IF;
    RETURN NEW;
END;
""".strip()


def _function_bodies(bind: sa.engine.Connection,
                     schema: str) -> dict[str, tuple[str, str]]:
    return {
        _IMMUTABILITY_FUNCTION: ('trigger', _append_only_body()),
        _RUN_LOCK_FUNCTION: ('trigger', _run_lock_body()),
        _RUN_ADMISSION_FUNCTION: ('trigger', _run_admission_body(bind, schema)),
        _TERMINAL_ACTIVATION_FUNCTION:
            ('trigger', _terminal_activation_body(bind, schema)),
        _GATE_DML_GUARD_FUNCTION: ('trigger', _gate_dml_guard_body()),
        _GATE_UPDATE_GUARD_FUNCTION: ('trigger',
                                      _gate_update_guard_body(bind, schema)),
    }


def _runtime_assert_body(bind: sa.engine.Connection, schema: str,
                         trigger_bodies: dict[str, tuple[str, str]]) -> str:
    gate = _qualified(bind, schema, _GATE)
    runs = _qualified(bind, schema, _RUNS)
    rows = _qualified(bind, schema, _ROWS)
    expected_defaults, expected_checks, expected_columns = (
        _expected_gate_expression_catalogs(bind))
    expected_ledger_catalogs = _expected_ledger_expression_catalogs(bind)
    if any(value is not None for value in expected_defaults.values()):
        raise RuntimeError(
            'SkyServe schema 040 expected an internal write-fence default.')
    if any(value is not None
           for defaults, _, _ in expected_ledger_catalogs.values()
           for value in defaults.values()):
        raise RuntimeError(
            'SkyServe schema 040 expected an internal ledger default.')
    relation_names_tuple = (_GATE, _RUNS, _ROWS)
    reference_relation_names_tuple = (_SERVICES, _VERSION_SPECS)
    function_names_tuple = tuple(sorted(_FUNCTION_NAMES))
    relation_indexes = {
        name: index for index, name in enumerate(relation_names_tuple, start=1)
    }
    function_indexes = {
        name: index for index, name in enumerate(function_names_tuple, start=1)
    }
    reference_relation_indexes = {
        name: index
        for index, name in enumerate(reference_relation_names_tuple, start=1)
    }
    relation_oid_values = ', '.join(
        'pg_catalog.to_regclass('
        f'{_literal(_qualified(bind, schema, name))})::oid'
        for name in relation_names_tuple)
    reference_relation_oid_values = ', '.join(
        'pg_catalog.to_regclass('
        f'{_literal(_qualified(bind, schema, name))})::oid'
        for name in reference_relation_names_tuple)
    function_oid_values = ', '.join(
        'pg_catalog.to_regprocedure('
        f'{_literal(_qualified(bind, schema, name) + "()")})::oid'
        for name in function_names_tuple)
    expected_functions = ', '.join(
        _literal(name) for name in function_names_tuple)
    function_body_cases = '\n'.join(
        '            WHEN canonical_function_oids['
        f'{function_indexes[name]}] THEN '
        f'{_literal(body)}'
        for name, (_, body) in sorted(trigger_bodies.items()))
    trigger_cases = '\n'.join(
        '              OR (trigger.tgname = '
        f'{_literal(name)} AND trigger.tgrelid = '
        'canonical_relation_oids['
        f'{relation_indexes[contract.relation]}] AND trigger.tgfoid = '
        'canonical_function_oids['
        f'{function_indexes[contract.function]}] AND trigger.tgtype = '
        f'{contract.trigger_type} AND '
        f"trigger.tgconstraint {'<>' if contract.constraint else '='} 0 "
        f"AND trigger.tgdeferrable = {str(contract.deferrable).lower()} "
        'AND trigger.tginitdeferred = '
        f'{str(contract.initially_deferred).lower()})'
        for name, contract in sorted(_TRIGGER_CONTRACTS.items()))

    def relation_oid_sql(relation: str) -> str:
        if relation in relation_indexes:
            return f'canonical_relation_oids[{relation_indexes[relation]}]'
        return ('reference_relation_oids['
                f'{reference_relation_indexes[relation]}]')

    def attribute_numbers_sql(relation_oid: str, columns: tuple[str,
                                                                ...]) -> str:
        attnums = ', '.join(
            '(SELECT attribute.attnum '
            'FROM pg_catalog.pg_attribute AS attribute '
            f'WHERE attribute.attrelid = {relation_oid} '
            f'AND attribute.attname = {_literal(column)} '
            'AND attribute.attnum > 0 AND NOT attribute.attisdropped)'
            for column in columns)
        return f'ARRAY[{attnums}]::smallint[]'

    def ri_trigger_sql(
        foreign_keys: dict[str, _ForeignKeyContract],
        semantics: tuple[_RITriggerSemantic, ...],
    ) -> tuple[str, str]:
        cases = []
        filters = []
        for foreign_key_name, foreign_key in sorted(foreign_keys.items()):
            referencing_oid = relation_oid_sql(foreign_key.referencing_relation)
            referenced_oid = relation_oid_sql(foreign_key.referenced_relation)
            referencing_attnums = attribute_numbers_sql(
                referencing_oid, foreign_key.referencing_columns)
            referenced_attnums = attribute_numbers_sql(
                referenced_oid, foreign_key.referenced_columns)
            source_oids = {
                referencing_oid
                if semantic.source_is_referencing else referenced_oid
                for semantic in semantics
            }
            source_filter = ' OR '.join(f'trigger.tgrelid = {source_oid}'
                                        for source_oid in sorted(source_oids))
            filters.append(
                '              OR (foreign_key.conname = '
                f'{_literal(foreign_key_name)} AND foreign_key.conrelid = '
                f'{referencing_oid} AND foreign_key.confrelid = '
                f'{referenced_oid} AND ({source_filter}))')
            for semantic in semantics:
                source_oid = (referencing_oid if semantic.source_is_referencing
                              else referenced_oid)
                constraint_oid = (referenced_oid
                                  if semantic.source_is_referencing else
                                  referencing_oid)
                function_signature = (
                    _qualified(bind, 'pg_catalog', semantic.function) + '()')
                generated_name_pattern = (
                    f'^RI_ConstraintTrigger_{semantic.generated_name_kind}_'
                    '[0-9]+$')
                cases.append(
                    '              OR (foreign_key.conname = '
                    f'{_literal(foreign_key_name)} AND '
                    f'foreign_key.conrelid = {referencing_oid} AND '
                    f'foreign_key.confrelid = {referenced_oid} AND '
                    f'foreign_key.conkey = {referencing_attnums} AND '
                    f'foreign_key.confkey = {referenced_attnums} AND '
                    f'foreign_key.confupdtype = '
                    f'{_literal(foreign_key.update_action)} AND '
                    f'foreign_key.confdeltype = '
                    f'{_literal(foreign_key.delete_action)} AND '
                    f'foreign_key.confmatchtype = '
                    f'{_literal(foreign_key.match_type)} AND '
                    'NOT foreign_key.condeferrable AND '
                    'NOT foreign_key.condeferred AND '
                    'foreign_key.convalidated AND '
                    'foreign_key.conislocal AND '
                    'foreign_key.coninhcount = 0 AND '
                    'foreign_key.connoinherit AND '
                    'foreign_key.conparentid = 0 AND '
                    'foreign_key.contypid = 0 AND '
                    'foreign_key.conbin IS NULL AND '
                    'foreign_key.conexclop IS NULL AND '
                    '(pg_catalog.to_jsonb(foreign_key) ->> '
                    "'confdelsetcols') IS NULL AND "
                    'foreign_key.conindid = ('
                    'SELECT index_catalog.indexrelid '
                    'FROM pg_catalog.pg_index AS index_catalog '
                    f'WHERE index_catalog.indrelid = {referenced_oid} '
                    'AND index_catalog.indisprimary) AND '
                    'foreign_key.conpfeqop = ARRAY['
                    'pg_catalog.to_regoperator('
                    "'pg_catalog.=(pg_catalog.uuid,pg_catalog.uuid)')::oid] "
                    'AND foreign_key.conppeqop = ARRAY['
                    'pg_catalog.to_regoperator('
                    "'pg_catalog.=(pg_catalog.uuid,pg_catalog.uuid)')::oid] "
                    'AND foreign_key.conffeqop = ARRAY['
                    'pg_catalog.to_regoperator('
                    "'pg_catalog.=(pg_catalog.uuid,pg_catalog.uuid)')::oid] "
                    'AND '
                    f'trigger.tgrelid = {source_oid} AND '
                    f'trigger.tgconstrrelid = {constraint_oid} AND '
                    'trigger.tgfoid = pg_catalog.to_regprocedure('
                    f'{_literal(function_signature)})::oid AND '
                    f'trigger.tgtype = {semantic.trigger_type} AND '
                    f'trigger.tgname ~ {_literal(generated_name_pattern)})')
        return '\n'.join(cases), '\n'.join(filters)

    ri_trigger_cases_sql, ri_foreign_key_filter_sql = ri_trigger_sql(
        _FOREIGN_KEY_CONTRACTS, _RI_TRIGGER_SEMANTICS)
    referenced_ri_trigger_cases_sql, referenced_ri_foreign_key_filter_sql = (
        ri_trigger_sql(_REFERENCED_AUTHORITY_FOREIGN_KEY_CONTRACTS,
                       _REFERENCED_SIDE_RI_TRIGGER_SEMANTICS))
    relation_cases = '\n'.join(
        '              OR (relation.oid = canonical_relation_oids['
        f'{relation_indexes[name]}] AND relation.relname = {_literal(name)})'
        for name in relation_names_tuple)
    column_cases = '\n'.join(
        '              OR (attribute.attnum = '
        f'{index} AND attribute.attname = {_literal(name)} '
        f'AND attribute.atttypid = {expected_columns[name][0]} '
        f'AND attribute.atttypmod = {expected_columns[name][1]} '
        f'AND attribute.attcollation = {expected_columns[name][2]} '
        'AND attribute.attnotnull = '
        f'{str(expected_columns[name][3]).lower()} '
        f'AND attribute.attidentity = {_literal(expected_columns[name][4])} '
        f'AND attribute.attgenerated = {_literal(expected_columns[name][5])} '
        'AND attribute.attacl IS NULL '
        'AND NOT attribute.attisdropped AND attribute.attinhcount = 0 '
        'AND attribute.attislocal)'
        for index, name in enumerate(_GATE_COLUMN_NAMES, start=1))
    check_cases = '\n'.join(
        '              OR (constraint_catalog.conname = '
        f'{_literal(name)} AND pg_catalog.regexp_replace('
        "constraint_catalog.conbin::text, ' :location -?[0-9]+', '', 'g') "
        f'= {_literal(node_tree)} '
        'AND constraint_catalog.convalidated = '
        f'{str(validated).lower()} '
        'AND constraint_catalog.connoinherit = '
        f'{str(no_inherit).lower()})'
        for name, (node_tree, validated,
                   no_inherit) in sorted(expected_checks.items()))
    ledger_catalog_checks: list[str] = []
    for relation, column_names in ((_RUNS, _RUN_COLUMN_NAMES),
                                   (_ROWS, _ROW_COLUMN_NAMES)):
        _, ledger_checks, ledger_columns = expected_ledger_catalogs[relation]
        relation_index = relation_indexes[relation]
        ledger_column_cases = '\n'.join(
            '                  OR (attribute.attnum = '
            f'{index} AND attribute.attname = {_literal(name)} '
            f'AND attribute.atttypid = {ledger_columns[name][0]} '
            f'AND attribute.atttypmod = {ledger_columns[name][1]} '
            f'AND attribute.attcollation = {ledger_columns[name][2]} '
            'AND attribute.attnotnull = '
            f'{str(ledger_columns[name][3]).lower()} '
            f'AND attribute.attidentity = {_literal(ledger_columns[name][4])} '
            f'AND attribute.attgenerated = {_literal(ledger_columns[name][5])} '
            'AND attribute.attacl IS NULL '
            'AND NOT attribute.attisdropped '
            'AND attribute.attinhcount = 0 AND attribute.attislocal)'
            for index, name in enumerate(column_names, start=1))
        ledger_check_cases = '\n'.join(
            '                  OR (constraint_catalog.conname = '
            f'{_literal(name)} AND pg_catalog.regexp_replace('
            "constraint_catalog.conbin::text, ' :location -?[0-9]+', "
            "'', 'g') = "
            f'{_literal(node_tree)} AND '
            'constraint_catalog.convalidated = '
            f'{str(validated).lower()} AND '
            'constraint_catalog.connoinherit = '
            f'{str(no_inherit).lower()})'
            for name, (node_tree, validated,
                       no_inherit) in sorted(ledger_checks.items()))
        ledger_catalog_checks.append(f"""
       OR (SELECT count(*)
           FROM pg_catalog.pg_attribute AS attribute
           WHERE attribute.attrelid =
                     canonical_relation_oids[{relation_index}]
             AND attribute.attnum > 0) <> {len(column_names)}
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_attribute AS attribute
           WHERE attribute.attrelid =
                     canonical_relation_oids[{relation_index}]
             AND attribute.attnum > 0
             AND NOT (false
{ledger_column_cases}
             ))
       OR EXISTS (
           SELECT 1 FROM pg_catalog.pg_attrdef AS definition
           WHERE definition.adrelid =
                     canonical_relation_oids[{relation_index}])
       OR (SELECT count(*)
           FROM pg_catalog.pg_constraint AS constraint_catalog
           WHERE constraint_catalog.conrelid =
                     canonical_relation_oids[{relation_index}]
             AND constraint_catalog.contype = 'c') <>
          {len(ledger_checks)}
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_constraint AS constraint_catalog
           WHERE constraint_catalog.conrelid =
                     canonical_relation_oids[{relation_index}]
             AND constraint_catalog.contype = 'c'
             AND (constraint_catalog.connamespace <>
                      canonical_namespace_oid
                  OR constraint_catalog.condeferrable
                  OR constraint_catalog.condeferred
                  OR constraint_catalog.conparentid <> 0
                  OR NOT (false
{ledger_check_cases}
                  )))
""".rstrip())
    ledger_catalog_conditions = '\n'.join(ledger_catalog_checks)
    ledger_index_cases = '\n'.join(
        '              OR (index_catalog.indrelid = '
        f'canonical_relation_oids[{relation_indexes[relation]}] '
        f'AND index_relation.relname = {_literal(name)} '
        f'AND index_catalog.indkey::text = {_literal(keys)} '
        f'AND index_catalog.indnkeyatts = {len(keys.split())} '
        f'AND index_catalog.indnatts = {len(keys.split())} '
        f'AND index_catalog.indisunique = {str(unique).lower()} '
        f'AND index_catalog.indisprimary = {str(primary).lower()})'
        for relation, name, keys, unique, primary in (
            (_RUNS, _RUN_PRIMARY_KEY, '1', True, True),
            (_ROWS, _ROW_PRIMARY_KEY, '1 2 3', True, True),
            (_ROWS, _ROW_VERSION_INDEX, '2 3', False, False),
        ))
    expected_trigger_count = len(_TRIGGER_CONTRACTS)
    expected_function_count = len(_FUNCTION_NAMES)
    search_path = f'search_path=pg_catalog, {_quote(bind, schema)}'
    return f"""
DECLARE
    canonical_namespace_oid oid :=
        pg_catalog.to_regnamespace({_literal(schema)})::oid;
    canonical_relation_oids oid[] := ARRAY[{relation_oid_values}]::oid[];
    reference_relation_oids oid[] :=
        ARRAY[{reference_relation_oid_values}]::oid[];
    canonical_function_oids oid[] := ARRAY[{function_oid_values}]::oid[];
    gate_owner oid;
BEGIN
    IF canonical_namespace_oid IS NULL
       OR pg_catalog.array_position(canonical_relation_oids, NULL) IS NOT NULL
       OR pg_catalog.array_position(reference_relation_oids, NULL) IS NOT NULL
       OR pg_catalog.array_position(canonical_function_oids, NULL) IS NOT NULL
    THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = '{_RUNTIME_ASSERT_MESSAGE}';
    END IF;
    SELECT relation.relowner INTO gate_owner
    FROM pg_catalog.pg_class AS relation
    WHERE relation.oid = canonical_relation_oids[1];
    IF gate_owner IS NULL
       OR (SELECT count(*)
           FROM pg_catalog.pg_class AS relation
           WHERE relation.oid = ANY(canonical_relation_oids)) <> 3
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_class AS relation
           WHERE relation.oid = ANY(canonical_relation_oids)
             AND (relation.relnamespace <> canonical_namespace_oid
                  OR relation.relkind <> 'r'
                  OR relation.relpersistence <> 'p'
                  OR relation.relrowsecurity
                  OR relation.relforcerowsecurity
                  OR relation.relispartition
                  OR relation.relreplident <> 'd'
                  OR relation.relhassubclass
                  OR relation.relhasrules
                  OR relation.relowner <> gate_owner
                  OR EXISTS (
                      SELECT 1
                      FROM pg_catalog.aclexplode(COALESCE(
                          relation.relacl,
                          pg_catalog.acldefault('r', relation.relowner)))
                           AS access
                      WHERE access.grantee <> relation.relowner)
                  OR NOT (false
{relation_cases}
                  )))
       OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_inherits AS inheritance
        WHERE inheritance.inhrelid = ANY(canonical_relation_oids)
           OR inheritance.inhparent = ANY(canonical_relation_oids)
       )
       OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_rewrite AS rewrite
        WHERE rewrite.ev_class = ANY(canonical_relation_oids)
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = '{_RUNTIME_ASSERT_MESSAGE}';
    END IF;
    IF (SELECT count(*) FROM {gate}) <> 1 OR NOT EXISTS (
        SELECT 1 FROM {gate} AS write_fence
        WHERE write_fence.singleton
          AND write_fence.generation >= 0
          AND ((write_fence.generation = 0
                AND write_fence.latest_run_id IS NULL
                AND write_fence.admitted_xid IS NULL)
               OR (write_fence.generation > 0
                   AND write_fence.latest_run_id IS NOT NULL
                   AND write_fence.admitted_xid IS NOT NULL
                   AND EXISTS (
                       SELECT 1
                       FROM {runs} AS latest_run
                       WHERE latest_run.run_id = write_fence.latest_run_id
                         AND latest_run.xmin =
                             write_fence.admitted_xid::xid)))
          AND (
              (write_fence.terminal_run_id IS NULL
               AND write_fence.terminal_xid IS NULL
               AND NOT EXISTS (
                   SELECT 1
                   FROM {rows} AS terminal_row
                   JOIN {runs} AS terminal_run
                     ON terminal_run.run_id = terminal_row.run_id
                   WHERE terminal_run.mode =
                             'retire_terminal_historical'
                     AND terminal_run.normalizer_version ~
                             '^4:[0-9a-f]{{40}}$'
                     AND terminal_row.classification =
                             'historical_physical_per_gpu'
                     AND terminal_row.outcome = 'retired'))
              OR
              (write_fence.terminal_run_id = write_fence.latest_run_id
               AND write_fence.terminal_xid = write_fence.admitted_xid
               AND EXISTS (
                   SELECT 1
                   FROM {runs} AS terminal_run
                   WHERE terminal_run.run_id =
                             write_fence.terminal_run_id
                     AND terminal_run.mode =
                             'retire_terminal_historical'
                     AND terminal_run.normalizer_version ~
                             '^4:[0-9a-f]{{40}}$'
                     AND terminal_run.xmin =
                             write_fence.terminal_xid::xid)
               AND EXISTS (
                   SELECT 1
                   FROM {rows} AS terminal_row
                   WHERE terminal_row.run_id =
                             write_fence.terminal_run_id
                     AND terminal_row.classification =
                             'historical_physical_per_gpu'
                     AND terminal_row.outcome = 'retired'
                     AND terminal_row.xmin =
                             write_fence.terminal_xid::xid)
               AND NOT EXISTS (
                   SELECT 1
                   FROM {rows} AS terminal_row
                   JOIN {runs} AS terminal_run
                     ON terminal_run.run_id = terminal_row.run_id
                   WHERE terminal_run.mode =
                             'retire_terminal_historical'
                     AND terminal_run.normalizer_version ~
                             '^4:[0-9a-f]{{40}}$'
                     AND terminal_row.classification =
                             'historical_physical_per_gpu'
                     AND terminal_row.outcome = 'retired'
                     AND (terminal_row.run_id <>
                              write_fence.terminal_run_id
                          OR terminal_row.xmin <>
                              write_fence.terminal_xid::xid)))
          )
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = '{_RUNTIME_ASSERT_MESSAGE}';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM {rows} AS manifest_row
        LEFT JOIN {runs} AS manifest_run
          ON manifest_run.run_id = manifest_row.run_id
        WHERE manifest_run.run_id IS NULL
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = '{_RUNTIME_ASSERT_MESSAGE}';
    END IF;
    IF (SELECT count(*)
        FROM pg_catalog.pg_attribute AS attribute
        WHERE attribute.attrelid = canonical_relation_oids[1]
          AND attribute.attnum > 0) <> {len(_GATE_COLUMN_NAMES)}
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_attribute AS attribute
           WHERE attribute.attrelid = canonical_relation_oids[1]
             AND attribute.attnum > 0
             AND NOT (false
{column_cases}
             ))
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_attrdef AS definition
           WHERE definition.adrelid = canonical_relation_oids[1])
       OR (SELECT count(*)
           FROM pg_catalog.pg_constraint AS constraint_catalog
           WHERE constraint_catalog.conrelid = canonical_relation_oids[1]
             AND constraint_catalog.contype = 'c') <>
          {len(_GATE_CHECK_EXPRESSIONS)}
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_constraint AS constraint_catalog
           WHERE constraint_catalog.conrelid = canonical_relation_oids[1]
             AND constraint_catalog.contype = 'c'
             AND (constraint_catalog.connamespace <>
                      canonical_namespace_oid
                  OR constraint_catalog.condeferrable
                  OR constraint_catalog.condeferred
                  OR constraint_catalog.conparentid <> 0
                  OR NOT (false
{check_cases}
                  )))
       OR (SELECT count(*)
           FROM pg_catalog.pg_constraint AS constraint_catalog
           WHERE constraint_catalog.conrelid = canonical_relation_oids[1]
             AND constraint_catalog.contype IN ('p', 'f')) <> 3
       OR NOT EXISTS (
           SELECT 1
           FROM pg_catalog.pg_constraint AS primary_key
           WHERE primary_key.conrelid = canonical_relation_oids[1]
             AND primary_key.connamespace = canonical_namespace_oid
             AND primary_key.conname = {_literal(_GATE_PRIMARY_KEY)}
             AND primary_key.contype = 'p'
             AND primary_key.conkey = ARRAY[1]::smallint[]
             AND NOT primary_key.condeferrable
             AND NOT primary_key.condeferred
             AND primary_key.convalidated
             AND primary_key.conparentid = 0)
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_constraint AS foreign_key
           WHERE foreign_key.conrelid = canonical_relation_oids[1]
             AND foreign_key.contype = 'f'
             AND NOT (
                 foreign_key.connamespace = canonical_namespace_oid
                 AND foreign_key.confrelid = canonical_relation_oids[2]
                 AND foreign_key.confkey = ARRAY[(
                     SELECT run_id_attribute.attnum::smallint
                     FROM pg_catalog.pg_attribute AS run_id_attribute
                     WHERE run_id_attribute.attrelid =
                               canonical_relation_oids[2]
                       AND run_id_attribute.attname = 'run_id'
                       AND run_id_attribute.attnum > 0
                       AND NOT run_id_attribute.attisdropped
                 )]::smallint[]
                 AND foreign_key.confupdtype = 'a'
                 AND foreign_key.confdeltype = 'r'
                 AND foreign_key.confmatchtype = 's'
                 AND NOT foreign_key.condeferrable
                 AND NOT foreign_key.condeferred
                 AND foreign_key.convalidated
                 AND foreign_key.conparentid = 0
                 AND ((foreign_key.conname =
                           {_literal(_GATE_LATEST_RUN_FOREIGN_KEY)}
                       AND foreign_key.conkey = ARRAY[3]::smallint[])
                      OR (foreign_key.conname =
                              {_literal(_GATE_TERMINAL_RUN_FOREIGN_KEY)}
                          AND foreign_key.conkey = ARRAY[5]::smallint[]))))
       OR (SELECT count(*)
           FROM pg_catalog.pg_index AS index_catalog
           WHERE index_catalog.indrelid = canonical_relation_oids[1]) <> 1
       OR NOT EXISTS (
           SELECT 1
           FROM pg_catalog.pg_index AS index_catalog
           JOIN pg_catalog.pg_class AS index_relation
             ON index_relation.oid = index_catalog.indexrelid
           JOIN pg_catalog.pg_constraint AS primary_key
             ON primary_key.conindid = index_catalog.indexrelid
           WHERE index_catalog.indrelid = canonical_relation_oids[1]
             AND primary_key.conrelid = canonical_relation_oids[1]
             AND primary_key.conname = {_literal(_GATE_PRIMARY_KEY)}
             AND index_relation.relnamespace = canonical_namespace_oid
             AND index_relation.relname = {_literal(_GATE_PRIMARY_KEY)}
             AND index_relation.relowner = gate_owner
             AND index_relation.relkind = 'i'
             AND index_relation.relpersistence = 'p'
             AND index_relation.reloptions IS NULL
             AND index_relation.relam = (
                 SELECT access_method.oid
                 FROM pg_catalog.pg_am AS access_method
                 WHERE access_method.amname = 'btree')
             AND index_catalog.indisunique
             AND index_catalog.indisprimary
             AND index_catalog.indisvalid
             AND index_catalog.indisready
             AND index_catalog.indislive
             AND index_catalog.indimmediate
             AND NOT index_catalog.indisexclusion
             AND NOT index_catalog.indisclustered
             AND NOT index_catalog.indisreplident
             AND NOT index_catalog.indcheckxmin
             AND NOT COALESCE((pg_catalog.to_jsonb(index_catalog) ->>
                                  'indnullsnotdistinct')::boolean, false)
             AND index_catalog.indnatts = 1
             AND index_catalog.indnkeyatts = 1
             AND index_catalog.indkey::text = '1'
             AND index_catalog.indexprs IS NULL
             AND index_catalog.indpred IS NULL
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
                     index_catalog.indclass::oid[]) AS opclass_oid(oid)
                 JOIN pg_catalog.pg_opclass AS opclass
                   ON opclass.oid = opclass_oid.oid
                 WHERE NOT opclass.opcdefault
                    OR opclass.opcmethod <> index_relation.relam)
             AND NOT EXISTS (
                 SELECT 1
                 FROM pg_catalog.unnest(
                     index_catalog.indkey::smallint[])
                      WITH ORDINALITY AS key_column(attnum, position)
                 JOIN pg_catalog.unnest(
                     index_catalog.indcollation::oid[])
                      WITH ORDINALITY
                      AS key_collation(collation_oid, position)
                   USING (position)
                 JOIN pg_catalog.pg_attribute AS attribute
                   ON attribute.attrelid = index_catalog.indrelid
                  AND attribute.attnum = key_column.attnum
                 WHERE key_collation.collation_oid <>
                           attribute.attcollation)
             AND NOT EXISTS (
                 SELECT 1
                 FROM pg_catalog.unnest(
                     index_catalog.indoption::smallint[])
                      AS index_option(value)
                 WHERE index_option.value <> 0))
    THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = '{_RUNTIME_ASSERT_MESSAGE}';
    END IF;
    IF false
{ledger_catalog_conditions}
    THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = '{_RUNTIME_ASSERT_MESSAGE}: ledger expressions';
    END IF;
    IF (SELECT count(*)
           FROM pg_catalog.pg_constraint AS constraint_catalog
           WHERE constraint_catalog.conrelid = canonical_relation_oids[2]) <>
          5
       OR NOT EXISTS (
           SELECT 1
           FROM pg_catalog.pg_constraint AS primary_key
           WHERE primary_key.conrelid = canonical_relation_oids[2]
             AND primary_key.connamespace = canonical_namespace_oid
             AND primary_key.conname = {_literal(_RUN_PRIMARY_KEY)}
             AND primary_key.contype = 'p'
             AND primary_key.conkey = ARRAY[1]::smallint[]
             AND NOT primary_key.condeferrable
             AND NOT primary_key.condeferred
             AND primary_key.convalidated
             AND primary_key.conparentid = 0)
       OR (SELECT count(*)
           FROM pg_catalog.pg_constraint AS constraint_catalog
           WHERE constraint_catalog.conrelid = canonical_relation_oids[3]) <>
          6
       OR NOT EXISTS (
           SELECT 1
           FROM pg_catalog.pg_constraint AS primary_key
           WHERE primary_key.conrelid = canonical_relation_oids[3]
             AND primary_key.connamespace = canonical_namespace_oid
             AND primary_key.conname = {_literal(_ROW_PRIMARY_KEY)}
             AND primary_key.contype = 'p'
             AND primary_key.conkey = ARRAY[1, 2, 3]::smallint[]
             AND NOT primary_key.condeferrable
             AND NOT primary_key.condeferred
             AND primary_key.convalidated
             AND primary_key.conparentid = 0)
       OR NOT EXISTS (
           SELECT 1
           FROM pg_catalog.pg_constraint AS foreign_key
           WHERE foreign_key.conrelid = canonical_relation_oids[3]
             AND foreign_key.connamespace = canonical_namespace_oid
             AND foreign_key.conname = {_literal(_ROW_RUN_FOREIGN_KEY)}
             AND foreign_key.contype = 'f'
             AND foreign_key.conkey = ARRAY[1]::smallint[]
             AND foreign_key.confrelid = canonical_relation_oids[2]
             AND foreign_key.confkey = ARRAY[1]::smallint[]
             AND foreign_key.confupdtype = 'a'
             AND foreign_key.confdeltype = 'r'
             AND foreign_key.confmatchtype = 's'
             AND NOT foreign_key.condeferrable
             AND NOT foreign_key.condeferred
             AND foreign_key.convalidated
             AND foreign_key.conparentid = 0)
       OR NOT EXISTS (
           SELECT 1
           FROM pg_catalog.pg_constraint AS constraint_trigger
           WHERE constraint_trigger.conrelid = canonical_relation_oids[3]
             AND constraint_trigger.connamespace = canonical_namespace_oid
             AND constraint_trigger.conname =
                     {_literal(_TERMINAL_ACTIVATION_TRIGGER)}
             AND constraint_trigger.contype = 't'
             AND constraint_trigger.conkey IS NULL
             AND constraint_trigger.confrelid = 0
             AND constraint_trigger.conindid = 0
             AND constraint_trigger.condeferrable
             AND constraint_trigger.condeferred
             AND constraint_trigger.convalidated
             AND constraint_trigger.conparentid = 0)
       OR (SELECT count(*)
           FROM pg_catalog.pg_index AS index_catalog
           WHERE index_catalog.indrelid = ANY(
                     canonical_relation_oids[2:3])) <> 3
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_index AS index_catalog
           JOIN pg_catalog.pg_class AS index_relation
             ON index_relation.oid = index_catalog.indexrelid
           WHERE index_catalog.indrelid = ANY(
                     canonical_relation_oids[2:3])
             AND (index_relation.relnamespace <> canonical_namespace_oid
                  OR index_relation.relowner <> gate_owner
                  OR index_relation.relkind <> 'i'
                  OR index_relation.relpersistence <> 'p'
                  OR index_relation.reloptions IS NOT NULL
                  OR index_relation.relam <> (
                      SELECT access_method.oid
                      FROM pg_catalog.pg_am AS access_method
                      WHERE access_method.amname = 'btree')
                  OR NOT index_catalog.indisvalid
                  OR NOT index_catalog.indisready
                  OR NOT index_catalog.indislive
                  OR NOT index_catalog.indimmediate
                  OR index_catalog.indisexclusion
                  OR index_catalog.indisclustered
                  OR index_catalog.indisreplident
                  OR index_catalog.indcheckxmin
                  OR COALESCE((pg_catalog.to_jsonb(index_catalog) ->>
                                   'indnullsnotdistinct')::boolean, false)
                  OR index_catalog.indexprs IS NOT NULL
                  OR index_catalog.indpred IS NOT NULL
                  OR EXISTS (
                      SELECT 1
                      FROM pg_catalog.aclexplode(COALESCE(
                          index_relation.relacl,
                          pg_catalog.acldefault(
                              'r', index_relation.relowner))) AS access
                      WHERE access.grantee <> index_relation.relowner)
                  OR EXISTS (
                      SELECT 1
                      FROM pg_catalog.unnest(
                          index_catalog.indclass::oid[]) AS opclass_oid(oid)
                      JOIN pg_catalog.pg_opclass AS opclass
                        ON opclass.oid = opclass_oid.oid
                      WHERE NOT opclass.opcdefault
                         OR opclass.opcmethod <> index_relation.relam)
                  OR EXISTS (
                      SELECT 1
                      FROM pg_catalog.unnest(
                          index_catalog.indkey::smallint[])
                           WITH ORDINALITY AS key_column(attnum, position)
                      JOIN pg_catalog.unnest(
                          index_catalog.indcollation::oid[])
                           WITH ORDINALITY
                           AS key_collation(collation_oid, position)
                        USING (position)
                      JOIN pg_catalog.pg_attribute AS attribute
                        ON attribute.attrelid = index_catalog.indrelid
                       AND attribute.attnum = key_column.attnum
                      WHERE key_collation.collation_oid <>
                                attribute.attcollation)
                  OR EXISTS (
                      SELECT 1
                      FROM pg_catalog.unnest(
                          index_catalog.indoption::smallint[])
                           AS index_option(value)
                      WHERE index_option.value <> 0)
                  OR NOT (false
{ledger_index_cases}
                  )))
    THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = '{_RUNTIME_ASSERT_MESSAGE}';
    END IF;
    IF (SELECT count(*)
           FROM pg_catalog.pg_trigger AS trigger
           JOIN pg_catalog.pg_constraint AS foreign_key
             ON foreign_key.oid = trigger.tgconstraint
           WHERE foreign_key.connamespace = canonical_namespace_oid
             AND (false
{ri_foreign_key_filter_sql}
             )) <> {_EXPECTED_RI_TRIGGER_COUNT}
       OR (SELECT count(DISTINCT (
                      foreign_key.conname, procedure.proname))
           FROM pg_catalog.pg_trigger AS trigger
           JOIN pg_catalog.pg_constraint AS foreign_key
             ON foreign_key.oid = trigger.tgconstraint
           JOIN pg_catalog.pg_proc AS procedure
             ON procedure.oid = trigger.tgfoid
           WHERE foreign_key.connamespace = canonical_namespace_oid
             AND (false
{ri_foreign_key_filter_sql}
             )) <> {_EXPECTED_RI_TRIGGER_COUNT}
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_trigger AS trigger
           JOIN pg_catalog.pg_constraint AS foreign_key
             ON foreign_key.oid = trigger.tgconstraint
           JOIN pg_catalog.pg_proc AS procedure
             ON procedure.oid = trigger.tgfoid
           WHERE foreign_key.connamespace = canonical_namespace_oid
             AND (false
{ri_foreign_key_filter_sql}
             )
             AND (foreign_key.contype <> 'f'
                  OR NOT trigger.tgisinternal
                  OR trigger.tgenabled <> 'O'
                  OR trigger.tgqual IS NOT NULL
                  OR trigger.tgnargs <> 0
                  OR trigger.tgargs <> '\\x'::bytea
                  OR trigger.tgattr::text <> ''
                  OR trigger.tgoldtable IS NOT NULL
                  OR trigger.tgnewtable IS NOT NULL
                  OR trigger.tgparentid <> 0
                  OR trigger.tgdeferrable
                  OR trigger.tginitdeferred
                  OR NOT (false
{ri_trigger_cases_sql}
                  )))
       OR (SELECT count(*)
           FROM pg_catalog.pg_trigger AS trigger
           WHERE trigger.tgrelid = ANY(canonical_relation_oids)
             AND trigger.tgisinternal) <>
          {_EXPECTED_AUTHORITY_RELATION_INTERNAL_TRIGGER_COUNT}
       OR (SELECT count(*)
           FROM pg_catalog.pg_trigger AS trigger
           JOIN pg_catalog.pg_constraint AS foreign_key
             ON foreign_key.oid = trigger.tgconstraint
           WHERE foreign_key.connamespace = canonical_namespace_oid
             AND (false
{referenced_ri_foreign_key_filter_sql}
             )) <> {_EXPECTED_REFERENCED_AUTHORITY_RI_TRIGGER_COUNT}
       OR (SELECT count(DISTINCT (
                      foreign_key.conname, procedure.proname))
           FROM pg_catalog.pg_trigger AS trigger
           JOIN pg_catalog.pg_constraint AS foreign_key
             ON foreign_key.oid = trigger.tgconstraint
           JOIN pg_catalog.pg_proc AS procedure
             ON procedure.oid = trigger.tgfoid
           WHERE foreign_key.connamespace = canonical_namespace_oid
             AND (false
{referenced_ri_foreign_key_filter_sql}
             )) <> {_EXPECTED_REFERENCED_AUTHORITY_RI_TRIGGER_COUNT}
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_trigger AS trigger
           JOIN pg_catalog.pg_constraint AS foreign_key
             ON foreign_key.oid = trigger.tgconstraint
           JOIN pg_catalog.pg_proc AS procedure
             ON procedure.oid = trigger.tgfoid
           WHERE foreign_key.connamespace = canonical_namespace_oid
             AND (false
{referenced_ri_foreign_key_filter_sql}
             )
             AND (foreign_key.contype <> 'f'
                  OR NOT trigger.tgisinternal
                  OR trigger.tgenabled <> 'O'
                  OR trigger.tgqual IS NOT NULL
                  OR trigger.tgnargs <> 0
                  OR trigger.tgargs <> '\\x'::bytea
                  OR trigger.tgattr::text <> ''
                  OR trigger.tgoldtable IS NOT NULL
                  OR trigger.tgnewtable IS NOT NULL
                  OR trigger.tgparentid <> 0
                  OR trigger.tgdeferrable
                  OR trigger.tginitdeferred
                  OR NOT (false
{referenced_ri_trigger_cases_sql}
                  )))
    THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = '{_RUNTIME_ASSERT_MESSAGE}';
    END IF;
    IF (SELECT count(*)
        FROM pg_catalog.pg_trigger AS trigger
        WHERE trigger.tgrelid = ANY(canonical_relation_oids)
          AND NOT trigger.tgisinternal) <> {expected_trigger_count}
       OR (SELECT count(DISTINCT trigger.tgname)
           FROM pg_catalog.pg_trigger AS trigger
           WHERE trigger.tgrelid = ANY(canonical_relation_oids)
             AND NOT trigger.tgisinternal) <> {expected_trigger_count}
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_trigger AS trigger
           WHERE trigger.tgrelid = ANY(canonical_relation_oids)
             AND NOT trigger.tgisinternal
             AND (trigger.tgenabled <> 'A'
                  OR trigger.tgqual IS NOT NULL
                  OR trigger.tgnargs <> 0
                  OR trigger.tgargs <> '\\x'::bytea
                  OR trigger.tgattr::text <> ''
                  OR trigger.tgoldtable IS NOT NULL
                  OR trigger.tgnewtable IS NOT NULL
                  OR trigger.tgparentid <> 0
                  OR trigger.tgconstrrelid <> 0
                  OR NOT (false
{trigger_cases}
                  ))
       ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = '{_RUNTIME_ASSERT_MESSAGE}';
    END IF;
    IF (SELECT count(*)
        FROM pg_catalog.pg_proc AS procedure
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = procedure.pronamespace
        WHERE namespace.nspname = {_literal(schema)}
          AND procedure.proname IN ({expected_functions})) <>
          {expected_function_count}
       OR EXISTS (
           SELECT 1
           FROM pg_catalog.pg_proc AS procedure
           WHERE procedure.oid = ANY(canonical_function_oids)
             AND (procedure.pronargs <> 0
                  OR procedure.proowner <> gate_owner
                  OR procedure.pronamespace <> canonical_namespace_oid
                  OR procedure.prolang <> (
                      SELECT language.oid
                      FROM pg_catalog.pg_language AS language
                      WHERE language.lanname = 'plpgsql')
                  OR NOT procedure.prosecdef
                  OR procedure.provolatile <> 'v'
                  OR procedure.proparallel <> 'u'
                  OR procedure.proisstrict
                  OR procedure.proleakproof
                  OR procedure.proretset
                  OR procedure.prokind <> 'f'
                  OR (procedure.oid = canonical_function_oids[
                        {function_indexes[_RUNTIME_ASSERT_FUNCTION]}]
                      AND pg_catalog.pg_get_function_result(procedure.oid) <>
                          'boolean')
                  OR (procedure.oid <> canonical_function_oids[
                        {function_indexes[_RUNTIME_ASSERT_FUNCTION]}]
                      AND pg_catalog.pg_get_function_result(procedure.oid) <>
                          'trigger')
                  OR procedure.proconfig IS DISTINCT FROM
                     ARRAY[{_literal(search_path)}]::text[]
                  OR EXISTS (
                      SELECT 1
                      FROM pg_catalog.aclexplode(COALESCE(
                          procedure.proacl,
                          pg_catalog.acldefault('f', procedure.proowner)))
                           AS access
                      WHERE access.grantee <> procedure.proowner)
                  OR (procedure.oid <> canonical_function_oids[
                        {function_indexes[_RUNTIME_ASSERT_FUNCTION]}]
                      AND procedure.prosrc IS DISTINCT FROM
                          CASE procedure.oid
{function_body_cases}
                          END))
       ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = '{_RUNTIME_ASSERT_MESSAGE}';
    END IF;
    RETURN TRUE;
END;
""".strip()


def _expected_functions(bind: sa.engine.Connection,
                        schema: str) -> dict[str, tuple[str, str]]:
    functions = _function_bodies(bind, schema)
    functions[_RUNTIME_ASSERT_FUNCTION] = ('boolean',
                                           _runtime_assert_body(
                                               bind, schema, functions))
    return functions


def expected_runtime_assertion_body(bind: sa.engine.Connection,
                                    schema: str) -> str:
    """Return the frozen application-side trust root for revision 040."""
    return _expected_functions(bind, schema)[_RUNTIME_ASSERT_FUNCTION][1]


def _normalized_pg_node_tree(value: str | None) -> str | None:
    if value is None:
        return None
    return re.sub(r' :location -?[0-9]+', '', value)


def _default_node_trees(bind: sa.engine.Connection, schema: str,
                        relation: str) -> dict[str, str | None]:
    rows = bind.execute(
        sa.text('SELECT attribute.attname, definition.adbin::text '
                'FROM pg_catalog.pg_attribute AS attribute '
                'JOIN pg_catalog.pg_class AS relation '
                'ON relation.oid = attribute.attrelid '
                'JOIN pg_catalog.pg_namespace AS namespace '
                'ON namespace.oid = relation.relnamespace '
                'LEFT JOIN pg_catalog.pg_attrdef AS definition '
                'ON definition.adrelid = attribute.attrelid '
                'AND definition.adnum = attribute.attnum '
                'WHERE namespace.nspname = :schema '
                'AND relation.relname = :relation '
                'AND attribute.attnum > 0 AND NOT attribute.attisdropped'), {
                    'schema': schema,
                    'relation': relation,
                })
    return {
        str(name): _normalized_pg_node_tree(node_tree)
        for name, node_tree in rows
    }


def _column_semantic_catalog(
    bind: sa.engine.Connection,
    schema: str,
    relation: str,
) -> dict[str, tuple[int, int, int, bool, str, str, bool]]:
    rows = bind.execute(
        sa.text('SELECT attribute.attname, attribute.atttypid, '
                'attribute.atttypmod, attribute.attcollation, '
                'attribute.attnotnull, attribute.attidentity, '
                'attribute.attgenerated, attribute.attacl IS NULL '
                'FROM pg_catalog.pg_attribute AS attribute '
                'JOIN pg_catalog.pg_class AS relation '
                'ON relation.oid = attribute.attrelid '
                'JOIN pg_catalog.pg_namespace AS namespace '
                'ON namespace.oid = relation.relnamespace '
                'WHERE namespace.nspname = :schema '
                'AND relation.relname = :relation '
                'AND attribute.attnum > 0 AND NOT attribute.attisdropped'), {
                    'schema': schema,
                    'relation': relation,
                })
    return {
        str(name): (int(type_oid), int(type_modifier), int(collation_oid),
                    bool(not_null), str(identity), str(generated),
                    bool(no_column_acl))
        for (name, type_oid, type_modifier, collation_oid, not_null, identity,
             generated, no_column_acl) in rows
    }


def _check_node_trees(
    bind: sa.engine.Connection,
    schema: str,
    relation: str,
) -> dict[str, tuple[str, bool, bool]]:
    rows = bind.execute(
        sa.text('SELECT constraint_catalog.conname, '
                'constraint_catalog.conbin::text, '
                'constraint_catalog.convalidated, '
                'constraint_catalog.connoinherit '
                'FROM pg_catalog.pg_constraint AS constraint_catalog '
                'JOIN pg_catalog.pg_class AS relation '
                'ON relation.oid = constraint_catalog.conrelid '
                'JOIN pg_catalog.pg_namespace AS namespace '
                'ON namespace.oid = relation.relnamespace '
                "WHERE constraint_catalog.contype = 'c' "
                'AND namespace.nspname = :schema '
                'AND relation.relname = :relation'), {
                    'schema': schema,
                    'relation': relation,
                })
    return {
        str(name): (_normalized_pg_node_tree(node_tree) or
                    '', bool(validated), bool(no_inherit)
                   ) for name, node_tree, validated, no_inherit in rows
    }


def _gate_columns_sql() -> str:
    checks = ',\n'.join(f'    CONSTRAINT {name} CHECK ({expression})'
                        for name, expression in _GATE_CHECK_EXPRESSIONS.items())
    return f"""
    singleton boolean NOT NULL,
    generation bigint NOT NULL,
    latest_run_id uuid,
    admitted_xid xid8,
    terminal_run_id uuid,
    terminal_xid xid8,
    CONSTRAINT {_GATE_PRIMARY_KEY} PRIMARY KEY (singleton),
{checks}
""".strip()


def _run_columns_sql() -> str:
    return """
    run_id uuid NOT NULL,
    mode text NOT NULL,
    normalizer_version text NOT NULL,
    schema_revision text NOT NULL,
    release_version text NOT NULL,
    started_at double precision NOT NULL,
    completed_at double precision NOT NULL,
    row_bound integer NOT NULL,
    row_count integer NOT NULL,
    classification_counts jsonb NOT NULL,
    pre_inventory_sha256 text NOT NULL,
    post_inventory_sha256 text NOT NULL,
    freeze_evidence_sha256 text NOT NULL,
    CONSTRAINT ck_placement_normalization_run_mode
        CHECK (mode IN ('apply_supported', 'retire_terminal_historical')),
    CONSTRAINT ck_placement_normalization_run_times
        CHECK (completed_at >= started_at),
    CONSTRAINT ck_placement_normalization_run_row_bound
        CHECK (row_bound >= 0 AND row_count >= 0 AND row_count <= row_bound),
    CONSTRAINT ck_placement_normalization_run_digests
        CHECK (length(pre_inventory_sha256) = 64 AND
               length(post_inventory_sha256) = 64 AND
               length(freeze_evidence_sha256) = 64)
""".strip()


def _row_columns_sql() -> str:
    return """
    run_id uuid NOT NULL,
    service_name text NOT NULL,
    version integer NOT NULL,
    classification text NOT NULL,
    outcome text NOT NULL,
    original_spec_sha256 text NOT NULL,
    result_spec_sha256 text NOT NULL,
    original_row_sha256 text NOT NULL,
    result_row_sha256 text NOT NULL,
    original_column_sha256s jsonb NOT NULL,
    result_column_sha256s jsonb NOT NULL,
    contract_projection jsonb,
    service_hash text,
    service_lifecycle_epoch integer,
    dependency_facts jsonb NOT NULL,
    CONSTRAINT ck_placement_normalization_row_outcome
        CHECK (outcome IN ('unchanged', 'changed', 'retired')),
    CONSTRAINT ck_placement_normalization_row_classification
        CHECK (length(classification) > 0),
    CONSTRAINT ck_placement_normalization_row_digests
        CHECK (length(original_spec_sha256) = 64 AND
               length(result_spec_sha256) = 64 AND
               length(original_row_sha256) = 64 AND
               length(result_row_sha256) = 64)
""".strip()


def _create_gate(bind: sa.engine.Connection, schema: str) -> None:
    gate = _qualified(bind, schema, _GATE)
    runs = _qualified(bind, schema, _RUNS)
    bind.exec_driver_sql(f"""
CREATE TABLE {gate} (
{_gate_columns_sql()},
    CONSTRAINT {_GATE_LATEST_RUN_FOREIGN_KEY}
        FOREIGN KEY (latest_run_id) REFERENCES {runs} (run_id)
        ON DELETE RESTRICT NOT DEFERRABLE,
    CONSTRAINT {_GATE_TERMINAL_RUN_FOREIGN_KEY}
        FOREIGN KEY (terminal_run_id) REFERENCES {runs} (run_id)
        ON DELETE RESTRICT NOT DEFERRABLE
)
""".strip())
    bind.exec_driver_sql(f'REVOKE ALL ON TABLE {gate} FROM PUBLIC')
    bind.exec_driver_sql(f'INSERT INTO {gate} '
                         '(singleton, generation, latest_run_id, admitted_xid, '
                         'terminal_run_id, terminal_xid) '
                         'VALUES (TRUE, 0, NULL, NULL, NULL, NULL)')


def _expected_gate_expression_catalogs(
    bind: sa.engine.Connection
) -> tuple[dict[str, str | None], dict[str, tuple[str, bool, bool]], dict[
        str, tuple[int, int, int, bool, str, str, bool]]]:
    with _frozen_catalog_search_path(bind):
        reference_name = f'skyserve040_expected_{uuid.uuid4().hex}'
        quoted = _quote(bind, reference_name)
        bind.exec_driver_sql(
            f'CREATE TEMPORARY TABLE {quoted} (\n{_gate_columns_sql()}\n)')
        try:
            schema = bind.execute(
                sa.text('SELECT n.nspname FROM pg_catalog.pg_class c '
                        'JOIN pg_catalog.pg_namespace n '
                        'ON n.oid = c.relnamespace '
                        'WHERE c.oid = pg_catalog.to_regclass(:relation)'), {
                            'relation': reference_name
                        }).scalar_one()
            return (_default_node_trees(bind, str(schema), reference_name),
                    _check_node_trees(bind, str(schema), reference_name),
                    _column_semantic_catalog(bind, str(schema), reference_name))
        finally:
            bind.exec_driver_sql(f'DROP TABLE {quoted}')


def _expected_ledger_expression_catalogs(
    bind: sa.engine.Connection,
) -> dict[str, tuple[dict[str, str | None], dict[str, tuple[str, bool, bool]],
                     dict[str, tuple[int, int, int, bool, str, str, bool]]]]:
    """Build frozen ledger catalogs without consulting the live ledgers."""
    with _frozen_catalog_search_path(bind):
        result = {}
        for relation, columns_sql in ((_RUNS, _run_columns_sql()),
                                      (_ROWS, _row_columns_sql())):
            reference_name = f'skyserve040_expected_{uuid.uuid4().hex}'
            quoted = _quote(bind, reference_name)
            bind.exec_driver_sql(
                f'CREATE TEMPORARY TABLE {quoted} (\n{columns_sql}\n)')
            try:
                schema = bind.execute(
                    sa.text('SELECT n.nspname FROM pg_catalog.pg_class c '
                            'JOIN pg_catalog.pg_namespace n '
                            'ON n.oid = c.relnamespace '
                            'WHERE c.oid = pg_catalog.to_regclass(:relation)'),
                    {
                        'relation': reference_name
                    }).scalar_one()
                result[relation] = (
                    _default_node_trees(bind, str(schema), reference_name),
                    _check_node_trees(bind, str(schema), reference_name),
                    _column_semantic_catalog(bind, str(schema), reference_name),
                )
            finally:
                bind.exec_driver_sql(f'DROP TABLE {quoted}')
        return result


def _relation_row(bind: sa.engine.Connection, schema: str,
                  relation: str) -> dict[str, Any]:
    row = bind.execute(
        sa.text(
            'SELECT relation.oid, relation.relkind, '
            'relation.relpersistence, relation.relrowsecurity, '
            'relation.relforcerowsecurity, relation.relispartition, '
            'relation.relreplident, relation.relhassubclass, '
            'relation.relhasrules, relation.relowner, '
            'NOT EXISTS (SELECT 1 FROM pg_catalog.aclexplode(COALESCE('
            "relation.relacl, pg_catalog.acldefault('r', relation.relowner))) "
            'AS access WHERE access.grantee <> relation.relowner) '
            'AS owner_only_acl '
            'FROM pg_catalog.pg_class AS relation '
            'JOIN pg_catalog.pg_namespace AS namespace '
            'ON namespace.oid = relation.relnamespace '
            'WHERE namespace.nspname = :schema '
            'AND relation.relname = :relation'), {
                'schema': schema,
                'relation': relation,
            }).mappings().one_or_none()
    if row is None:
        raise RuntimeError(
            f'SkyServe schema 040 could not resolve relation {relation!r}.')
    return dict(row)


def _verify_relation_envelope(bind: sa.engine.Connection, schema: str,
                              relation: str) -> None:
    row = _relation_row(bind, schema, relation)
    if (str(row['relkind']) != 'r' or str(row['relpersistence']) != 'p' or
            bool(row['relrowsecurity']) or bool(row['relforcerowsecurity']) or
            bool(row['relispartition']) or str(row['relreplident']) != 'd' or
            bool(row['relhassubclass']) or bool(row['relhasrules']) or
            not bool(row['owner_only_acl'])):
        raise RuntimeError('SkyServe schema 040 found an incompatible relation '
                           f'envelope for {relation!r}.')
    relation_oid = int(row['oid'])
    inherited = bind.execute(
        sa.text('SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits '
                'WHERE inhrelid = :oid OR inhparent = :oid)'), {
                    'oid': relation_oid
                }).scalar_one()
    rewritten = bind.execute(
        sa.text('SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_rewrite '
                'WHERE ev_class = :oid)'), {
                    'oid': relation_oid
                }).scalar_one()
    if inherited or rewritten:
        raise RuntimeError(
            'SkyServe schema 040 refuses inherited or rewrite-enabled '
            f'relation {relation!r}.')


def _verify_gate_relation(bind: sa.engine.Connection, schema: str) -> None:
    _verify_relation_envelope(bind, schema, _GATE)
    expected_defaults, expected_checks, expected_columns = (
        _expected_gate_expression_catalogs(bind))
    if (_default_node_trees(bind, schema, _GATE) != expected_defaults or
            _check_node_trees(bind, schema, _GATE) != expected_checks or
            _column_semantic_catalog(bind, schema, _GATE) != expected_columns):
        raise RuntimeError(
            'SkyServe schema 040 found an incompatible write-fence column, '
            'default, or CHECK catalog.')

    inspector = sa.inspect(bind)
    primary = inspector.get_pk_constraint(_GATE, schema=schema)
    if (primary.get('name') != _GATE_PRIMARY_KEY or
            tuple(primary.get('constrained_columns') or ()) != ('singleton',)):
        raise RuntimeError(
            'SkyServe schema 040 found an incompatible write-fence primary '
            'key.')
    foreign_keys = {
        str(foreign_key['name']): foreign_key
        for foreign_key in inspector.get_foreign_keys(_GATE, schema=schema)
    }
    expected_foreign_keys = {
        _GATE_LATEST_RUN_FOREIGN_KEY: ('latest_run_id',),
        _GATE_TERMINAL_RUN_FOREIGN_KEY: ('terminal_run_id',),
    }
    if set(foreign_keys) != set(expected_foreign_keys):
        raise RuntimeError(
            'SkyServe schema 040 found an incompatible write-fence foreign '
            'key inventory.')
    for name, columns in expected_foreign_keys.items():
        foreign_key = foreign_keys[name]
        options = foreign_key.get('options') or {}
        if (tuple(foreign_key.get('constrained_columns') or ()) != columns or
                foreign_key.get('referred_table') != _RUNS or
                tuple(foreign_key.get('referred_columns') or
                      ()) != ('run_id',) or
                str(options.get('ondelete', '')).upper() != 'RESTRICT' or
                bool(options.get('deferrable', False)) or
                options.get('initially') is not None):
            raise RuntimeError(
                'SkyServe schema 040 found an incompatible write-fence '
                f'foreign key {name!r}.')

    indexes = inspector.get_indexes(_GATE, schema=schema)
    if indexes:
        # SQLAlchemy omits the primary-key index on supported PostgreSQL
        # versions; any reflected secondary index is outside this contract.
        raise RuntimeError(
            'SkyServe schema 040 found an unexpected write-fence index.')


def _gate_rows(bind: sa.engine.Connection, schema: str) -> list[dict[str, Any]]:
    gate = _qualified(bind, schema, _GATE)
    return [
        dict(row) for row in bind.execute(
            sa.text('SELECT singleton, generation, latest_run_id, '
                    'admitted_xid, terminal_run_id, terminal_xid '
                    f'FROM {gate}')).mappings()
    ]


def _verify_gate_data(bind: sa.engine.Connection,
                      schema: str,
                      *,
                      require_seed: bool = False,
                      require_open: bool = False) -> dict[str, Any]:
    rows = _gate_rows(bind, schema)
    if len(rows) != 1:
        raise RuntimeError(
            'SkyServe schema 040 requires exactly one write-fence row.')
    row = rows[0]
    generation = row['generation']
    latest_run_id = row['latest_run_id']
    admitted_xid = row['admitted_xid']
    terminal_run_id = row['terminal_run_id']
    terminal_xid = row['terminal_xid']
    coherent = (
        row['singleton'] is True and type(generation) is int and
        generation >= 0 and
        ((generation == 0 and latest_run_id is None and admitted_xid is None) or
         (generation > 0 and latest_run_id is not None and
          admitted_xid is not None)) and
        ((terminal_run_id is None and terminal_xid is None) or
         (terminal_run_id == latest_run_id and
          str(terminal_xid) == str(admitted_xid))))
    if not coherent:
        raise RuntimeError(
            'SkyServe schema 040 found an incoherent write-fence row.')
    if require_seed and not (generation == 0 and latest_run_id is None and
                             admitted_xid is None and
                             terminal_run_id is None and terminal_xid is None):
        raise RuntimeError(
            'SkyServe schema 040 did not install the exact write-fence seed.')
    if require_open and terminal_run_id is not None:
        raise RuntimeError(_TERMINAL_DOWNGRADE_MESSAGE)
    return row


def _function_rows(bind: sa.engine.Connection,
                   schema: str) -> dict[str, dict[str, Any]]:
    rows = bind.execute(
        sa.text('SELECT procedure.proname, procedure.pronargs, '
                'procedure.prosrc, procedure.prosecdef, '
                'procedure.provolatile, procedure.proparallel, '
                'procedure.proisstrict, procedure.proleakproof, '
                'procedure.proretset, procedure.prokind, procedure.proconfig, '
                'procedure.proowner, language.lanname, '
                'pg_catalog.pg_get_function_result(procedure.oid) AS result, '
                'NOT EXISTS (SELECT 1 FROM pg_catalog.aclexplode(COALESCE('
                "procedure.proacl, pg_catalog.acldefault('f', "
                'procedure.proowner))) AS access '
                'WHERE access.grantee <> procedure.proowner) AS owner_only_acl '
                'FROM pg_catalog.pg_proc AS procedure '
                'JOIN pg_catalog.pg_namespace AS namespace '
                'ON namespace.oid = procedure.pronamespace '
                'JOIN pg_catalog.pg_language AS language '
                'ON language.oid = procedure.prolang '
                'WHERE namespace.nspname = :schema '
                'AND procedure.proname = ANY(CAST(:names AS text[]))'), {
                    'schema': schema,
                    'names': list(_FUNCTION_NAMES),
                }).mappings()
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row['proname'])
        if name in result:
            raise RuntimeError(
                'SkyServe schema 040 found an overloaded trigger or runtime '
                f'function {name!r}.')
        result[name] = dict(row)
    return result


def _trigger_rows(bind: sa.engine.Connection,
                  schema: str) -> dict[str, dict[str, Any]]:
    rows = bind.execute(
        sa.text(
            'SELECT trigger.tgname, relation.relname, trigger.tgtype, '
            'trigger.tgenabled, procedure.proname, trigger.tgnargs, '
            "encode(trigger.tgargs, 'hex') AS tgargs, "
            'trigger.tgconstraint, trigger.tgdeferrable, '
            'trigger.tginitdeferred, trigger.tgconstrrelid, '
            'trigger.tgoldtable, trigger.tgnewtable, trigger.tgparentid, '
            'trigger.tgqual IS NULL AS no_when, trigger.tgattr::text AS tgattr '
            'FROM pg_catalog.pg_trigger AS trigger '
            'JOIN pg_catalog.pg_class AS relation '
            'ON relation.oid = trigger.tgrelid '
            'JOIN pg_catalog.pg_namespace AS namespace '
            'ON namespace.oid = relation.relnamespace '
            'JOIN pg_catalog.pg_proc AS procedure '
            'ON procedure.oid = trigger.tgfoid '
            'WHERE namespace.nspname = :schema '
            'AND relation.relname = ANY(CAST(:relations AS text[])) '
            'AND NOT trigger.tgisinternal'), {
                'schema': schema,
                'relations': [_RUNS, _ROWS, _GATE],
            }).mappings()
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row['tgname'])
        if name in result:
            raise RuntimeError(
                'SkyServe schema 040 found duplicate same-name triggers '
                f'{name!r}.')
        result[name] = dict(row)
    return result


def _ri_trigger_rows(
    bind: sa.engine.Connection,
    schema: str,
    foreign_key_names: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    if foreign_key_names is None:
        foreign_key_names = tuple(_FOREIGN_KEY_CONTRACTS)
    if not foreign_key_names or any(
            name not in _ALL_AUDITED_FOREIGN_KEY_CONTRACTS
            for name in foreign_key_names):
        raise ValueError('foreign_key_names must identify audited FKs.')
    rows = [
        dict(row) for row in bind.execute(
            sa.text(
                'SELECT foreign_key.oid AS foreign_key_oid, '
                'foreign_key.conname AS foreign_key_name, '
                'foreign_key.contype AS foreign_key_type, '
                'foreign_key.conrelid AS foreign_key_conrelid, '
                'foreign_key.confrelid AS foreign_key_confrelid, '
                'foreign_key.conkey, foreign_key.confkey, '
                'foreign_key.confupdtype, foreign_key.confdeltype, '
                'foreign_key.confmatchtype, foreign_key.condeferrable, '
                'foreign_key.condeferred, foreign_key.convalidated, '
                'foreign_key.conparentid AS foreign_key_parent_id, '
                'foreign_key.conislocal, foreign_key.coninhcount, '
                'foreign_key.connoinherit, foreign_key.contypid, '
                'foreign_key.conbin IS NULL AS no_constraint_expression, '
                'foreign_key.conexclop IS NULL AS no_exclusion_operators, '
                '(pg_catalog.to_jsonb(foreign_key) ->> '
                "'confdelsetcols') IS NULL AS no_delete_set_columns, "
                'foreign_key.conindid, foreign_key.conpfeqop, '
                'foreign_key.conppeqop, foreign_key.conffeqop, '
                'foreign_key_namespace.nspname AS foreign_key_namespace, '
                'referencing_relation.relname AS referencing_relation, '
                'referenced_relation.relname AS referenced_relation, '
                'trigger.oid AS trigger_oid, trigger.tgname, '
                'trigger.tgrelid, trigger.tgconstrrelid, trigger.tgfoid, '
                'trigger.tgtype, trigger.tgenabled, trigger.tgisinternal, '
                'trigger.tgnargs, encode(trigger.tgargs, \'hex\') AS tgargs, '
                'trigger.tgconstraint, trigger.tgdeferrable, '
                'trigger.tginitdeferred, trigger.tgoldtable, '
                'trigger.tgnewtable, trigger.tgparentid, '
                'trigger.tgqual IS NULL AS no_when, '
                'trigger.tgattr::text AS tgattr, '
                'source_relation.relname AS source_relation, '
                'source_namespace.nspname AS source_namespace, '
                'constraint_relation.relname AS constraint_relation, '
                'constraint_namespace.nspname AS constraint_namespace, '
                'procedure.oid AS procedure_oid, procedure.proname, '
                'procedure_namespace.nspname AS procedure_namespace '
                'FROM pg_catalog.pg_constraint AS foreign_key '
                'JOIN pg_catalog.pg_namespace AS foreign_key_namespace '
                'ON foreign_key_namespace.oid = foreign_key.connamespace '
                'JOIN pg_catalog.pg_class AS referencing_relation '
                'ON referencing_relation.oid = foreign_key.conrelid '
                'JOIN pg_catalog.pg_class AS referenced_relation '
                'ON referenced_relation.oid = foreign_key.confrelid '
                'JOIN pg_catalog.pg_trigger AS trigger '
                'ON trigger.tgconstraint = foreign_key.oid '
                'JOIN pg_catalog.pg_class AS source_relation '
                'ON source_relation.oid = trigger.tgrelid '
                'JOIN pg_catalog.pg_namespace AS source_namespace '
                'ON source_namespace.oid = source_relation.relnamespace '
                'JOIN pg_catalog.pg_class AS constraint_relation '
                'ON constraint_relation.oid = trigger.tgconstrrelid '
                'JOIN pg_catalog.pg_namespace AS constraint_namespace '
                'ON constraint_namespace.oid = '
                'constraint_relation.relnamespace '
                'JOIN pg_catalog.pg_proc AS procedure '
                'ON procedure.oid = trigger.tgfoid '
                'JOIN pg_catalog.pg_namespace AS procedure_namespace '
                'ON procedure_namespace.oid = procedure.pronamespace '
                'WHERE foreign_key_namespace.nspname = :schema '
                'AND foreign_key.conname = ANY(CAST(:names AS text[])) '
                'ORDER BY foreign_key.conname, procedure.proname'), {
                    'schema': schema,
                    'names': list(foreign_key_names),
                }).mappings()
    ]
    return [
        row for row in rows if
        str(row['referencing_relation']) == _ALL_AUDITED_FOREIGN_KEY_CONTRACTS[
            str(row['foreign_key_name'])].referencing_relation and
        str(row['referenced_relation']) == _ALL_AUDITED_FOREIGN_KEY_CONTRACTS[
            str(row['foreign_key_name'])].referenced_relation
    ]


def _relation_oid(bind: sa.engine.Connection, schema: str,
                  relation: str) -> int:
    oid = bind.execute(sa.text('SELECT pg_catalog.to_regclass(:relation)::oid'),
                       {
                           'relation': _qualified(bind, schema, relation)
                       }).scalar_one()
    if oid is None:
        raise RuntimeError('SkyServe schema 040 could not resolve relation '
                           f'{schema!r}.{relation!r}.')
    return int(oid)


def _column_attnums(bind: sa.engine.Connection, relation_oid: int,
                    columns: tuple[str, ...]) -> tuple[int, ...]:
    rows = bind.execute(
        sa.text('SELECT attribute.attname, attribute.attnum '
                'FROM pg_catalog.pg_attribute AS attribute '
                'WHERE attribute.attrelid = :relation_oid '
                'AND attribute.attname = ANY(CAST(:columns AS text[])) '
                'AND attribute.attnum > 0 AND NOT attribute.attisdropped'), {
                    'relation_oid': relation_oid,
                    'columns': list(columns),
                }).all()
    by_name = {str(name): int(attnum) for name, attnum in rows}
    if set(by_name) != set(columns):
        raise RuntimeError('SkyServe schema 040 could not resolve an audited '
                           'foreign-key column inventory.')
    return tuple(by_name[column] for column in columns)


def _verify_ri_trigger_catalog(
    bind: sa.engine.Connection,
    schema: str,
    *,
    expected_enabled: str,
    foreign_key_names: tuple[str, ...] | None = None,
    semantics: tuple[_RITriggerSemantic, ...] = _RI_TRIGGER_SEMANTICS,
) -> None:
    if expected_enabled not in ('A', 'O'):
        raise ValueError(f'Unsupported trigger mode {expected_enabled!r}.')
    if foreign_key_names is None:
        foreign_key_names = tuple(_FOREIGN_KEY_CONTRACTS)
    if not foreign_key_names or any(
            name not in _ALL_AUDITED_FOREIGN_KEY_CONTRACTS
            for name in foreign_key_names):
        raise ValueError('foreign_key_names must identify audited FKs.')
    if not semantics or any(
            semantic not in _RI_TRIGGER_SEMANTICS for semantic in semantics):
        raise ValueError('semantics must identify standard RI triggers.')

    expected_keys = {(foreign_key_name, semantic.function)
                     for foreign_key_name in foreign_key_names
                     for semantic in semantics}
    observed: dict[tuple[str, str], dict[str, Any]] = {}
    expected_functions = {semantic.function for semantic in semantics}
    for row in _ri_trigger_rows(bind, schema, foreign_key_names):
        key = (str(row['foreign_key_name']), str(row['proname']))
        if key[1] not in expected_functions:
            continue
        if key in observed:
            raise RuntimeError(
                'SkyServe schema 040 found duplicate semantic RI triggers '
                f'for {key!r}.')
        observed[key] = row
    if set(observed) != expected_keys:
        raise RuntimeError(
            'SkyServe schema 040 found an incompatible internal RI trigger '
            'inventory.')

    function_oids = {}
    for semantic in semantics:
        signature = _qualified(bind, 'pg_catalog', semantic.function) + '()'
        function_oid = bind.execute(
            sa.text('SELECT pg_catalog.to_regprocedure(:signature)::oid'), {
                'signature': signature
            }).scalar_one()
        if function_oid is None:
            raise RuntimeError('SkyServe schema 040 could not resolve the '
                               f'internal RI function {semantic.function!r}.')
        function_oids[semantic.function] = int(function_oid)

    semantic_by_function = {
        semantic.function: semantic for semantic in semantics
    }
    relation_names = {
        relation for foreign_key_name in foreign_key_names
        for relation in (_ALL_AUDITED_FOREIGN_KEY_CONTRACTS[foreign_key_name].
                         referencing_relation,
                         _ALL_AUDITED_FOREIGN_KEY_CONTRACTS[foreign_key_name].
                         referenced_relation)
    }
    relation_oids = {
        relation: _relation_oid(bind, schema, relation)
        for relation in relation_names
    }
    runs_primary_index_oid = int(
        bind.execute(
            sa.text('SELECT index_catalog.indexrelid '
                    'FROM pg_catalog.pg_index AS index_catalog '
                    'WHERE index_catalog.indrelid = :runs_oid '
                    'AND index_catalog.indisprimary'), {
                        'runs_oid': relation_oids[_RUNS]
                    }).scalar_one())
    uuid_equality_operator_oid = int(
        bind.execute(
            sa.text('SELECT pg_catalog.to_regoperator('
                    "'pg_catalog.=(pg_catalog.uuid,pg_catalog.uuid)')::oid")).
        scalar_one())
    for key, row in observed.items():
        foreign_key_name, function = key
        foreign_key = _ALL_AUDITED_FOREIGN_KEY_CONTRACTS[foreign_key_name]
        semantic = semantic_by_function[function]
        source_relation = (foreign_key.referencing_relation
                           if semantic.source_is_referencing else
                           foreign_key.referenced_relation)
        constraint_relation = (foreign_key.referenced_relation
                               if semantic.source_is_referencing else
                               foreign_key.referencing_relation)
        referencing_relation_oid = relation_oids[
            foreign_key.referencing_relation]
        referenced_relation_oid = relation_oids[foreign_key.referenced_relation]
        referencing_attnums = _column_attnums(bind, referencing_relation_oid,
                                              foreign_key.referencing_columns)
        referenced_attnums = _column_attnums(bind, referenced_relation_oid,
                                             foreign_key.referenced_columns)
        expected_equality_operators = (uuid_equality_operator_oid,)
        generated_name = (
            f'RI_ConstraintTrigger_{semantic.generated_name_kind}_[0-9]+')
        if (str(row['foreign_key_type']) != 'f' or
                str(row['foreign_key_namespace']) != schema or
                int(row['foreign_key_conrelid']) != referencing_relation_oid or
                int(row['foreign_key_confrelid']) != referenced_relation_oid or
                str(row['referencing_relation'])
                != foreign_key.referencing_relation or
                str(row['referenced_relation'])
                != foreign_key.referenced_relation or
                tuple(int(value) for value in row['conkey'])
                != referencing_attnums or
                tuple(int(value) for value in row['confkey'])
                != referenced_attnums or
                str(row['confupdtype']) != foreign_key.update_action or
                str(row['confdeltype']) != foreign_key.delete_action or
                str(row['confmatchtype']) != foreign_key.match_type or
                bool(row['condeferrable']) or bool(row['condeferred']) or
                not bool(row['convalidated']) or
                int(row['foreign_key_parent_id']) != 0 or
                not bool(row['conislocal']) or int(row['coninhcount']) != 0 or
                not bool(row['connoinherit']) or int(row['contypid']) != 0 or
                not bool(row['no_constraint_expression']) or
                not bool(row['no_exclusion_operators']) or
                not bool(row['no_delete_set_columns']) or
                int(row['conindid']) != runs_primary_index_oid or
                tuple(int(value) for value in row['conpfeqop'])
                != expected_equality_operators or
                tuple(int(value) for value in row['conppeqop'])
                != expected_equality_operators or
                tuple(int(value) for value in row['conffeqop'])
                != expected_equality_operators or
                int(row['tgconstraint']) != int(row['foreign_key_oid']) or
                str(row['source_namespace']) != schema or
                str(row['source_relation']) != source_relation or
                str(row['constraint_namespace']) != schema or
                str(row['constraint_relation']) != constraint_relation or int(
                    row['tgconstrrelid']) == 0 or int(
                        row['procedure_oid']) != function_oids[function] or str(
                            row['procedure_namespace']) != 'pg_catalog' or int(
                                row['tgtype']) != semantic.trigger_type or str(
                                    row['tgenabled']) != expected_enabled or
                not bool(row['tgisinternal']) or int(row['tgnargs']) != 0 or
                str(row['tgargs']) != '' or bool(row['tgdeferrable']) or bool(
                    row['tginitdeferred']) or row['tgoldtable'] is not None or
                row['tgnewtable'] is not None or int(row['tgparentid']) != 0 or
                not bool(row['no_when']) or str(row['tgattr']) != '' or
                re.fullmatch(generated_name, str(row['tgname'])) is None):
            raise RuntimeError(
                'SkyServe schema 040 found an incompatible internal RI '
                f'trigger for {foreign_key_name!r}/{function!r}.')


def _verify_authority_relation_internal_trigger_count(
        bind: sa.engine.Connection, schema: str) -> None:
    observed = bind.execute(
        sa.text('SELECT count(*) '
                'FROM pg_catalog.pg_trigger AS trigger '
                'JOIN pg_catalog.pg_class AS relation '
                'ON relation.oid = trigger.tgrelid '
                'JOIN pg_catalog.pg_namespace AS namespace '
                'ON namespace.oid = relation.relnamespace '
                'WHERE namespace.nspname = :schema '
                'AND relation.relname = ANY(CAST(:relations AS text[])) '
                'AND trigger.tgisinternal'), {
                    'schema': schema,
                    'relations': [_GATE, _RUNS, _ROWS],
                }).scalar_one()
    if int(observed) != _EXPECTED_AUTHORITY_RELATION_INTERNAL_TRIGGER_COUNT:
        raise RuntimeError(
            'SkyServe schema 040 found an incompatible internal trigger '
            'inventory on the authority relations.')


def _verify_no_orphan_manifest_rows(bind: sa.engine.Connection,
                                    schema: str) -> None:
    runs = _qualified(bind, schema, _RUNS)
    rows = _qualified(bind, schema, _ROWS)
    orphan_exists = bind.execute(
        sa.text(f'SELECT EXISTS ('
                f'SELECT 1 FROM {rows} AS manifest_row '
                f'LEFT JOIN {runs} AS manifest_run '
                'ON manifest_run.run_id = manifest_row.run_id '
                'WHERE manifest_run.run_id IS NULL)')).scalar_one()
    if orphan_exists:
        raise RuntimeError('SkyServe schema 040 found an orphan placement-'
                           'normalization manifest row.')


def _verify_catalog(bind: sa.engine.Connection, schema: str) -> None:
    _verify_gate_relation(bind, schema)
    for relation in (_RUNS, _ROWS):
        _verify_relation_envelope(bind, schema, relation)
    _verify_ri_trigger_catalog(bind, schema, expected_enabled='O')
    _verify_ri_trigger_catalog(
        bind,
        schema,
        expected_enabled='O',
        foreign_key_names=tuple(_REFERENCED_AUTHORITY_FOREIGN_KEY_CONTRACTS),
        semantics=_REFERENCED_SIDE_RI_TRIGGER_SEMANTICS)
    _verify_authority_relation_internal_trigger_count(bind, schema)
    _verify_no_orphan_manifest_rows(bind, schema)
    gate_owner = int(_relation_row(bind, schema, _GATE)['relowner'])
    expected_functions = _expected_functions(bind, schema)
    functions = _function_rows(bind, schema)
    if set(functions) != set(expected_functions):
        raise RuntimeError(
            'SkyServe schema 040 found an incomplete function catalog.')
    expected_search_path = (f'search_path=pg_catalog, '
                            f'{_quote(bind, schema)}',)
    for name, (expected_result, expected_body) in expected_functions.items():
        function = functions[name]
        if (int(function['pronargs']) != 0 or
                str(function['result']) != expected_result or
                str(function['lanname']) != 'plpgsql' or
                not bool(function['prosecdef']) or
                str(function['provolatile']) != 'v' or
                str(function['proparallel']) != 'u' or
                bool(function['proisstrict']) or
                bool(function['proleakproof']) or bool(function['proretset']) or
                str(function['prokind']) != 'f' or
                tuple(function['proconfig'] or ()) != expected_search_path or
                int(function['proowner']) != gate_owner or
                not bool(function['owner_only_acl']) or
                str(function['prosrc']) != expected_body):
            raise RuntimeError(
                'SkyServe schema 040 found an incompatible function '
                f'{name!r}.')

    triggers = _trigger_rows(bind, schema)
    if set(triggers) != set(_TRIGGER_CONTRACTS):
        raise RuntimeError(
            'SkyServe schema 040 found an incompatible trigger inventory.')
    for name, expected in _TRIGGER_CONTRACTS.items():
        trigger = triggers[name]
        constraint_oid = int(trigger['tgconstraint'])
        if (str(trigger['relname']) != expected.relation or
                str(trigger['proname']) != expected.function or
                int(trigger['tgtype']) != expected.trigger_type or
                str(trigger['tgenabled']) != 'A' or
                int(trigger['tgnargs']) != 0 or str(trigger['tgargs']) != '' or
            (constraint_oid != 0) != expected.constraint or
                bool(trigger['tgdeferrable']) != expected.deferrable or bool(
                    trigger['tginitdeferred']) != expected.initially_deferred or
                int(trigger['tgconstrrelid']) != 0 or
                trigger['tgoldtable'] is not None or
                trigger['tgnewtable'] is not None or
                int(trigger['tgparentid']) != 0 or
                not bool(trigger['no_when']) or str(trigger['tgattr']) != ''):
            raise RuntimeError(
                'SkyServe schema 040 found an incompatible trigger '
                f'{name!r}.')

    runtime_assertion = _qualified(bind, schema, _RUNTIME_ASSERT_FUNCTION)
    asserted = bind.exec_driver_sql(
        f'SELECT {runtime_assertion}()').scalar_one()
    if asserted is not True:
        raise RuntimeError(
            'SkyServe schema 040 runtime catalog assertion did not return '
            'TRUE.')


def _assert_prerequisites(bind: sa.engine.Connection, schema: str) -> None:
    inspector = sa.inspect(bind)
    for relation, required_columns in _REQUIRED_COLUMNS.items():
        if not inspector.has_table(relation, schema=schema):
            raise RuntimeError(
                'SkyServe schema 040 requires the placement-normalization '
                f'relation {relation!r}.')
        columns = {
            str(column['name'])
            for column in inspector.get_columns(relation, schema=schema)
        }
        if not required_columns.issubset(columns):
            raise RuntimeError(
                'SkyServe schema 040 found an incompatible column inventory '
                f'for {relation!r}.')


def _has_qualifying_terminal_row(bind: sa.engine.Connection,
                                 schema: str) -> bool:
    runs = _qualified(bind, schema, _RUNS)
    rows = _qualified(bind, schema, _ROWS)
    return bool(
        bind.execute(
            sa.text(
                f'SELECT EXISTS (SELECT 1 FROM {rows} AS terminal_row '
                f'JOIN {runs} AS terminal_run '
                'ON terminal_run.run_id = terminal_row.run_id '
                "WHERE terminal_run.mode = 'retire_terminal_historical' "
                "AND terminal_run.normalizer_version ~ '^4:[0-9a-f]{{40}}$' "
                "AND terminal_row.classification = "
                "'historical_physical_per_gpu' "
                "AND terminal_row.outcome = 'retired')")).scalar_one())


def _gate_exists(bind: sa.engine.Connection, schema: str) -> bool:
    return bool(
        bind.execute(
            sa.text('SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class c '
                    'JOIN pg_catalog.pg_namespace n '
                    'ON n.oid = c.relnamespace '
                    'WHERE n.nspname = :schema AND c.relname = :relation)'), {
                        'schema': schema,
                        'relation': _GATE,
                    }).scalar_one())


def _assert_uninstalled(bind: sa.engine.Connection, schema: str) -> None:
    if (_gate_exists(bind, schema) or _function_rows(bind, schema) or
            _trigger_rows(bind, schema)):
        raise RuntimeError(
            'SkyServe schema 040 refuses a partial or pre-existing '
            'placement-normalization authority catalog.')


def _lock_relations(bind: sa.engine.Connection, schema: str,
                    relations: tuple[str, ...]) -> None:
    for relation in relations:
        bind.exec_driver_sql(f'LOCK TABLE {_qualified(bind, schema, relation)} '
                             'IN ACCESS EXCLUSIVE MODE')


def _create_function(bind: sa.engine.Connection, schema: str, name: str,
                     result: str, body: str) -> None:
    function = _qualified(bind, schema, name)
    bind.exec_driver_sql(
        f'CREATE FUNCTION {function}() RETURNS {result} '
        'LANGUAGE plpgsql VOLATILE SECURITY DEFINER '
        f'SET search_path = pg_catalog, {_quote(bind, schema)} '
        f'AS $skyserve040${body}$skyserve040$')
    bind.exec_driver_sql(f'REVOKE ALL ON FUNCTION {function}() FROM PUBLIC')


def _create_functions(bind: sa.engine.Connection, schema: str) -> None:
    for name, (result, body) in _expected_functions(bind, schema).items():
        _create_function(bind, schema, name, result, body)


def _create_triggers(bind: sa.engine.Connection, schema: str) -> None:
    runs = _qualified(bind, schema, _RUNS)
    rows = _qualified(bind, schema, _ROWS)
    gate = _qualified(bind, schema, _GATE)
    functions = {
        name: _qualified(bind, schema, name) for name in _FUNCTION_NAMES
    }
    bind.exec_driver_sql(
        f'CREATE TRIGGER {_quote(bind, _RUN_IMMUTABILITY_TRIGGER)} '
        f'BEFORE UPDATE OR DELETE OR TRUNCATE ON {runs} FOR EACH STATEMENT '
        f'EXECUTE FUNCTION {functions[_IMMUTABILITY_FUNCTION]}()')
    bind.exec_driver_sql(
        f'CREATE TRIGGER {_quote(bind, _ROW_IMMUTABILITY_TRIGGER)} '
        f'BEFORE UPDATE OR DELETE OR TRUNCATE ON {rows} FOR EACH STATEMENT '
        f'EXECUTE FUNCTION {functions[_IMMUTABILITY_FUNCTION]}()')
    bind.exec_driver_sql(f'CREATE TRIGGER {_quote(bind, _RUN_LOCK_TRIGGER)} '
                         f'BEFORE INSERT ON {runs} FOR EACH ROW '
                         f'EXECUTE FUNCTION {functions[_RUN_LOCK_FUNCTION]}()')
    bind.exec_driver_sql(
        f'CREATE TRIGGER {_quote(bind, _RUN_ADMISSION_TRIGGER)} '
        f'AFTER INSERT ON {runs} FOR EACH ROW '
        f'EXECUTE FUNCTION {functions[_RUN_ADMISSION_FUNCTION]}()')
    bind.exec_driver_sql(
        f'CREATE CONSTRAINT TRIGGER '
        f'{_quote(bind, _TERMINAL_ACTIVATION_TRIGGER)} '
        f'AFTER INSERT ON {rows} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW '
        f'EXECUTE FUNCTION {functions[_TERMINAL_ACTIVATION_FUNCTION]}()')
    bind.exec_driver_sql(
        f'CREATE TRIGGER {_quote(bind, _GATE_DML_GUARD_TRIGGER)} '
        f'BEFORE INSERT OR DELETE OR TRUNCATE ON {gate} FOR EACH STATEMENT '
        f'EXECUTE FUNCTION {functions[_GATE_DML_GUARD_FUNCTION]}()')
    bind.exec_driver_sql(
        f'CREATE TRIGGER {_quote(bind, _GATE_UPDATE_GUARD_TRIGGER)} '
        f'BEFORE UPDATE ON {gate} FOR EACH ROW '
        f'EXECUTE FUNCTION {functions[_GATE_UPDATE_GUARD_FUNCTION]}()')
    for name, contract in _TRIGGER_CONTRACTS.items():
        relation = _qualified(bind, schema, contract.relation)
        bind.exec_driver_sql(
            f'ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {_quote(bind, name)}'
        )


def _drop_catalog(bind: sa.engine.Connection, schema: str) -> None:
    for name, contract in reversed(tuple(_TRIGGER_CONTRACTS.items())):
        relation = _qualified(bind, schema, contract.relation)
        bind.exec_driver_sql(f'DROP TRIGGER {_quote(bind, name)} ON {relation}')
    for name in reversed(tuple(_expected_functions(bind, schema))):
        bind.exec_driver_sql(
            f'DROP FUNCTION {_qualified(bind, schema, name)}()')
    bind.exec_driver_sql(f'DROP TABLE {_qualified(bind, schema, _GATE)}')


def _try_authority_lock(bind: sa.engine.Connection) -> None:
    acquired = bind.execute(
        sa.text('SELECT pg_catalog.pg_try_advisory_xact_lock('
                'pg_catalog.hashtextextended(:name, 0))'), {
                    'name': _ADVISORY_LOCK_NAME
                }).scalar_one()
    if acquired is not True:
        raise RuntimeError(_BUSY_MESSAGE)


def upgrade() -> None:
    """Install the PostgreSQL-only append-only normalization authority."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'SkyServe schema 040 is PostgreSQL-only and must not be stamped '
            f'on dialect {bind.dialect.name!r}.')
    current = op.get_context().get_current_revision()
    if current != '039':
        raise RuntimeError('SkyServe schema 040 requires the exact Serve039 '
                           f'head; found {current!r}.')
    schema = _current_schema(bind)
    with _frozen_catalog_search_path(bind):
        _lock_relations(bind, schema, (_RUNS, _ROWS))
        _assert_prerequisites(bind, schema)
        _assert_uninstalled(bind, schema)
        if _has_qualifying_terminal_row(bind, schema):
            raise RuntimeError(
                'SkyServe schema 040 must be installed before protocol-4 '
                'terminal retirement.')
        _create_gate(bind, schema)
        _create_functions(bind, schema)
        _create_triggers(bind, schema)
        _verify_catalog(bind, schema)
        _verify_gate_data(bind, schema, require_seed=True)


def downgrade() -> None:
    """Remove revision 040 only while its terminal authority remains open."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'SkyServe schema 040 downgrade is PostgreSQL-only; found '
            f'{bind.dialect.name!r}.')
    # Keep catalog resolution frozen while proving and removing the authority,
    # but restore Alembic's caller path before it updates its version table.
    with _frozen_catalog_search_path(bind):
        _try_authority_lock(bind)
        authority = (placement_normalization_authority.
                     assert_downgrade_database_authority(bind))
        schema = authority.schema
        _lock_relations(bind, schema, (_GATE, _RUNS, _ROWS))
        _verify_catalog(bind, schema)
        _verify_gate_data(bind, schema, require_open=True)
        if _has_qualifying_terminal_row(bind, schema):
            raise RuntimeError(_TERMINAL_DOWNGRADE_MESSAGE)
        _drop_catalog(bind, schema)
        _assert_uninstalled(bind, schema)
