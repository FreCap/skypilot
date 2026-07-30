"""Transactional repository primitives for capacity schema revision 001.

Revision 001 deliberately has no production projector or lifecycle writer.
The row insertion helpers in this module exist only for schema/repository test
fixtures.  A production projection API requires the separately reviewed C2
payload and source-mapping contract.
"""

import contextlib
import enum
import re
from typing import Any, Iterator, Mapping, Sequence, TypeAlias
import uuid

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky.physical_capacity import canonical
from sky.physical_capacity import models
from sky.physical_capacity import schema
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

_SHA256_PATTERN = re.compile(r'^[0-9a-f]{64}$')

_Executor: TypeAlias = sqlalchemy.engine.Connection | orm.Session
_Values: TypeAlias = Mapping[str, Any]
_Identity: TypeAlias = tuple[str, ...]


class ImmutableRowConflictError(RuntimeError):
    """Raised when an idempotency key names different immutable row values."""


def initialize_schema(
    engine: sqlalchemy.engine.Engine,
    mode: migration_utils.MigrationMode | None = None,
) -> None:
    """Initialize or verify the PostgreSQL-only capacity schema."""
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'The physical-capacity state store requires PostgreSQL.')
    migration_utils.safe_alembic_upgrade(
        engine,
        migration_utils.CAPACITY_STATE_DB_NAME,
        migration_utils.CAPACITY_STATE_VERSION,
        mode=(migration_utils.configured_migration_mode()
              if mode is None else mode),
    )


# Do not pass engine_namespace: capacity intentionally shares the ordinary
# central PostgreSQL engine and its strict process-local connection budget.
_db_manager = db_utils.DatabaseManager(
    migration_utils.CAPACITY_STATE_DB_NAME,
    initialize_schema,
)


def initialize_and_get_db() -> sqlalchemy.engine.Engine:
    """Lazily initialize and return the shared capacity database engine."""
    return _db_manager.get_engine()


@contextlib.contextmanager
def transaction(executor: _Executor | None = None,) -> Iterator[_Executor]:
    """Yield a caller-owned transaction or open one on the lazy engine.

    A supplied ``Connection`` or ``Session`` is used as-is: this helper never
    commits it, closes it, or opens a nested transaction.  With no supplied
    executor, the helper owns one engine transaction and commits on success.
    """
    if executor is not None:
        yield executor
        return
    with initialize_and_get_db().begin() as connection:
        yield connection


def _execute(executor: _Executor, statement: Any):
    return executor.execute(statement)


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, enum.Enum):
        return value.value
    return value


def _comparable(value: Any) -> Any:
    value = _normalize_scalar(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _validate_hash(field: str, value: object) -> None:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f'{field} must be a lowercase 64-character SHA-256 digest.')


def _validate_payload(field: str, value: object,
                      domain: canonical.CanonicalDomain) -> None:
    if not isinstance(value, dict):
        raise ValueError(f'{field} must be a canonical root object.')
    # Encoding, rather than validation alone, applies the 65,536-byte envelope
    # bound as well as the generic depth/item/string limits.
    canonical.canonical_json_bytes(value, domain=domain)


_ENUM_FIELDS: dict[str, dict[str, type[enum.Enum]]] = {
    'capacity_projection_scans': {
        'source_kind': models.ProjectionSourceKind,
        'state': models.ProjectionScanState,
    },
    'capacity_groups': {
        'owner_kind': models.OwnerKind,
        'writer_fence_kind': models.WriterFenceKind,
        'source_kind': models.ProjectionSourceKind,
        'projection_confidence': models.ProjectionConfidence,
        'lifecycle_state': models.GroupLifecycleState,
        'created_by_actor_type': models.ActorType,
        'updated_by_actor_type': models.ActorType,
    },
    'capacity_group_intents': {
        'created_by_actor_type': models.ActorType,
    },
    'capacity_allocations': {
        'source_kind': models.AllocationSourceKind,
        'identity_confidence': models.AllocationIdentityConfidence,
        'lifecycle_state': models.GroupLifecycleState,
        'projection_state': models.AllocationProjectionState,
        'observed_state': models.AllocationObservedState,
        'observation_certainty': models.ObservationCertainty,
    },
    'capacity_allocation_desires': {
        'desired_state': models.DesiredState,
        'release_gate': models.ReleaseGate,
        'reason_code': models.DesireReasonCode,
    },
}

_PAYLOAD_FIELDS: dict[str, canonical.CanonicalDomain] = {
    'cursor': canonical.CanonicalDomain.PROJECTION_CURSOR,
    'placement_contract': canonical.CanonicalDomain.PLACEMENT_CONTRACT,
    'topology': canonical.CanonicalDomain.TOPOLOGY,
    'physical_spec': canonical.CanonicalDomain.PHYSICAL_SPEC,
}

_UNHASHED_PAYLOAD_FIELDS = frozenset({'finding_counts'})
_SCHEMA_VERSION_FIELDS = frozenset(
    {'cursor_schema_version', 'schema_version', 'spec_schema_version'})

_DIGEST_FIELDS = frozenset({
    'source_partition_hash',
    'writer_controller_fingerprint',
    'source_incarnation_hash',
    'placement_contract_hash',
    'intent_hash',
    'source_fingerprint',
    'physical_spec_hash',
})

_ALLOCATION_SCAN_SOURCE_KIND = {
    models.AllocationSourceKind.SERVE_REPLICA.value:
        models.ProjectionSourceKind.SERVE_SERVICE.value,
    models.AllocationSourceKind.POOL_WORKER.value:
        models.ProjectionSourceKind.SERVE_POOL.value,
    models.AllocationSourceKind.MANAGED_JOB_CLUSTER.value:
        models.ProjectionSourceKind.MANAGED_JOB_TASK.value,
}


def _prepare_values(table: sqlalchemy.Table, values: _Values) -> dict[str, Any]:
    unknown_columns = set(values) - set(table.c.keys())
    if unknown_columns:
        raise ValueError(f'Unknown {table.name} column(s): '
                         f'{", ".join(sorted(unknown_columns))}.')

    prepared = {key: _normalize_scalar(value) for key, value in values.items()}
    if 'workspace' in prepared:
        canonical.validate_bounded_string(
            prepared['workspace'],
            max_bytes=canonical.MAX_WORKSPACE_IDENTIFIER_BYTES,
            field='workspace')
    if 'source_key' in prepared:
        canonical.validate_bounded_string(
            prepared['source_key'],
            max_bytes=canonical.MAX_SOURCE_KEY_BYTES,
            field='source_key')
    for field in ('owner_id', 'owner_incarnation'):
        value = prepared.get(field)
        if value is not None:
            canonical.validate_bounded_string(
                value,
                max_bytes=canonical.MAX_SOURCE_IDENTIFIER_BYTES,
                field=field)
    if prepared.get('error_code') is not None:
        canonical.validate_bounded_string(
            prepared['error_code'],
            max_bytes=canonical.MAX_ERROR_CODE_BYTES,
            field='error_code')

    for field, enum_type in _ENUM_FIELDS.get(table.name, {}).items():
        if field not in prepared:
            continue
        try:
            prepared[field] = enum_type(prepared[field]).value
        except (TypeError, ValueError) as e:
            raise ValueError(
                f'Invalid {table.name}.{field}: {prepared[field]!r}.') from e

    for field, domain in _PAYLOAD_FIELDS.items():
        if field in prepared and prepared[field] is not None:
            _validate_payload(field, prepared[field], domain)
    for field in _UNHASHED_PAYLOAD_FIELDS:
        if field in prepared and prepared[field] is not None:
            if not isinstance(prepared[field], dict):
                raise ValueError(f'{field} must be a canonical root object.')
            canonical.canonical_payload_json_bytes(prepared[field])
    for field in _SCHEMA_VERSION_FIELDS:
        if field not in prepared or prepared[field] is None:
            continue
        value = prepared[field]
        if not isinstance(value, int) or isinstance(value, bool) or value != 1:
            raise ValueError(
                f'Revision 001 repository helpers require {field}=1.')
    for field in _DIGEST_FIELDS:
        if field in prepared and prepared[field] is not None:
            _validate_hash(field, prepared[field])

    if (table.name in ('capacity_groups', 'capacity_allocations') and
            prepared.get('lifecycle_state')
            == models.GroupLifecycleState.RETIRED.value):
        raise ValueError(
            'Revision 001 repository helpers cannot write retired rows.')

    if ('placement_contract' in prepared and
            'placement_contract_hash' in prepared):
        expected_hash = canonical.canonical_hash(
            prepared['placement_contract'],
            domain=canonical.CanonicalDomain.PLACEMENT_CONTRACT)
        if prepared['placement_contract_hash'] != expected_hash:
            raise ValueError('placement_contract_hash does not match its '
                             'canonical payload.')
    if ('physical_spec' in prepared and
            prepared['physical_spec'] is not None and
            'physical_spec_hash' in prepared):
        expected_hash = canonical.canonical_hash(
            prepared['physical_spec'],
            domain=canonical.CanonicalDomain.PHYSICAL_SPEC)
        if prepared['physical_spec_hash'] != expected_hash:
            raise ValueError(
                'physical_spec_hash does not match its canonical payload.')
    return prepared


def _identity_predicate(table: sqlalchemy.Table, values: _Values,
                        identity: _Identity):
    if not all(column in values and values[column] is not None
               for column in identity):
        return None
    return sqlalchemy.and_(
        *(table.c[column] == values[column] for column in identity))


def _find_conflicting_row(
    executor: _Executor,
    table: sqlalchemy.Table,
    values: _Values,
    identities: Sequence[_Identity],
) -> dict[str, Any] | None:
    for identity in identities:
        predicate = _identity_predicate(table, values, identity)
        if predicate is None:
            continue
        row = _execute(
            executor,
            sqlalchemy.select(table).where(predicate).with_for_update(),
        ).mappings().one_or_none()
        if row is not None:
            return dict(row)
    return None


def _validate_scan_provenance(executor: _Executor, table: sqlalchemy.Table,
                              values: _Values) -> None:
    scan_id = values.get('last_seen_scan_id')
    if scan_id is None:
        return
    scan = _execute(
        executor,
        sqlalchemy.select(
            schema.PROJECTION_SCANS.c.workspace,
            schema.PROJECTION_SCANS.c.source_kind,
        ).where(schema.PROJECTION_SCANS.c.scan_id == scan_id),
    ).mappings().one_or_none()
    if scan is None:
        raise ValueError(f'Unknown last_seen_scan_id {scan_id}.')

    if table is schema.GROUPS:
        expected_source_kind = values.get('source_kind')
    elif table is schema.ALLOCATIONS:
        allocation_source_kind = values.get('source_kind')
        expected_source_kind = (
            _ALLOCATION_SCAN_SOURCE_KIND.get(allocation_source_kind)
            if isinstance(allocation_source_kind, str) else None)
    else:
        raise ValueError(
            f'{table.name} cannot carry last-seen scan provenance.')
    if expected_source_kind is None:
        raise ValueError(
            f'{table.name}.source_kind is required with last_seen_scan_id.')
    if (scan['workspace'] != values.get('workspace') or
            scan['source_kind'] != expected_source_kind):
        raise ValueError(
            'last_seen_scan_id must match the row workspace and source kind.')


def _assert_same_supplied_values(table: sqlalchemy.Table, existing: _Values,
                                 proposed: _Values) -> None:
    mismatched = [
        field for field, value in proposed.items()
        if _comparable(existing[field]) != _comparable(value)
    ]
    if mismatched:
        raise ImmutableRowConflictError(
            f'{table.name} idempotency collision changed immutable field(s): '
            f'{", ".join(sorted(mismatched))}.')


def _insert_immutable(
    executor: _Executor,
    table: sqlalchemy.Table,
    values: _Values,
    identities: Sequence[_Identity],
) -> dict[str, Any]:
    prepared = _prepare_values(table, values)
    if 'last_seen_scan_id' in prepared:
        _validate_scan_provenance(executor, table, prepared)
    statement = (postgresql.insert(table).values(
        **prepared).on_conflict_do_nothing().returning(*table.c))
    inserted = _execute(executor, statement).mappings().one_or_none()
    if inserted is not None:
        return dict(inserted)

    existing = _find_conflicting_row(executor, table, prepared, identities)
    if existing is None:
        raise ImmutableRowConflictError(
            f'{table.name} conflicted with an unrecognized unique identity.')
    _assert_same_supplied_values(table, existing, prepared)
    return existing


def insert_scan_for_test(values: _Values,
                         *,
                         executor: _Executor | None = None) -> dict[str, Any]:
    """Insert one generic scan fixture idempotently."""
    with transaction(executor) as txn:
        return _insert_immutable(
            txn,
            schema.PROJECTION_SCANS,
            values,
            (
                ('scan_id',),
                ('workspace', 'source_kind', 'source_partition_hash'),
            ),
        )


def insert_group_for_test(values: _Values,
                          *,
                          executor: _Executor | None = None) -> dict[str, Any]:
    """Insert one generic group fixture idempotently.

    A new group must share a caller transaction with its first intent because
    the schema's current-intent foreign key is intentionally non-null.
    """
    identities: list[_Identity] = [
        ('group_id',),
        ('workspace', 'source_kind', 'source_key', 'source_incarnation_hash'),
    ]
    if values.get('projection_confidence') in (
            models.ProjectionConfidence.EXACT,
            models.ProjectionConfidence.EXACT.value):
        identities.append(
            ('workspace', 'owner_kind', 'owner_id', 'owner_incarnation'))
    with transaction(executor) as txn:
        return _insert_immutable(txn, schema.GROUPS, values, identities)


def insert_intent_for_test(values: _Values,
                           *,
                           executor: _Executor | None = None) -> dict[str, Any]:
    """Insert one immutable generic intent fixture idempotently."""
    with transaction(executor) as txn:
        return _insert_immutable(
            txn,
            schema.GROUP_INTENTS,
            values,
            (('group_id', 'intent_generation'),),
        )


def insert_allocation_for_test(
    values: _Values,
    *,
    executor: _Executor | None = None,
) -> dict[str, Any]:
    """Insert one immutable generic allocation fixture idempotently."""
    identities: list[_Identity] = [
        ('allocation_id',),
        ('group_id', 'source_kind', 'source_key', 'source_incarnation_hash'),
    ]
    if values.get('cluster_hash') is not None:
        identities.append(('cluster_hash',))
    with transaction(executor) as txn:
        return _insert_immutable(txn, schema.ALLOCATIONS, values, identities)


def insert_desire_for_test(values: _Values,
                           *,
                           executor: _Executor | None = None) -> dict[str, Any]:
    """Insert one immutable generic allocation-desire fixture idempotently."""
    with transaction(executor) as txn:
        return _insert_immutable(
            txn,
            schema.ALLOCATION_DESIRES,
            values,
            (
                ('group_id', 'intent_generation', 'allocation_id'),
                ('group_id', 'workspace', 'intent_generation', 'ordinal'),
            ),
        )


def publish_initial_projection_for_test(
    *,
    group: _Values,
    intent: _Values,
    allocations: Sequence[_Values] = (),
    desires: Sequence[_Values] = (),
    executor: _Executor | None = None,
) -> dict[str, Any]:
    """Atomically publish one generic initial group/intent fixture."""
    with transaction(executor) as txn:
        group_row = insert_group_for_test(group, executor=txn)
        intent_row = insert_intent_for_test(intent, executor=txn)
        if (group_row['group_id'] != intent_row['group_id'] or
                group_row['workspace'] != intent_row['workspace'] or
                group_row['current_intent_generation']
                != intent_row['intent_generation']):
            raise ValueError(
                'Initial group and intent must identify the same current '
                'generation and workspace.')
        for allocation in allocations:
            insert_allocation_for_test(allocation, executor=txn)
        for desire in desires:
            insert_desire_for_test(desire, executor=txn)
        return intent_row


def advance_intent_for_test(
    *,
    group_id: uuid.UUID | str,
    workspace: str,
    intent: _Values,
    allocations: Sequence[_Values] = (),
    desires: Sequence[_Values] = (),
    executor: _Executor | None = None,
) -> dict[str, Any]:
    """Advance a locked generic fixture intent with A-to-B-to-A semantics."""
    with transaction(executor) as txn:
        group_row = _execute(
            txn,
            sqlalchemy.select(schema.GROUPS).where(
                schema.GROUPS.c.group_id == group_id,
                schema.GROUPS.c.workspace == workspace,
            ).with_for_update(),
        ).mappings().one_or_none()
        if group_row is None:
            raise KeyError(f'Unknown capacity group {group_id}.')

        current_intent = _execute(
            txn,
            sqlalchemy.select(schema.GROUP_INTENTS).where(
                schema.GROUP_INTENTS.c.group_id == group_id,
                schema.GROUP_INTENTS.c.intent_generation ==
                group_row['current_intent_generation'],
            ),
        ).mappings().one()
        proposed = dict(intent)
        if proposed.get('intent_hash') == current_intent['intent_hash']:
            return dict(current_intent)

        next_generation = group_row['current_intent_generation'] + 1
        supplied_generation = proposed.get('intent_generation', next_generation)
        if supplied_generation != next_generation:
            raise ImmutableRowConflictError(
                'A changed intent must use current_intent_generation + 1.')
        proposed.update({
            'group_id': group_id,
            'workspace': workspace,
            'intent_generation': next_generation,
        })
        intent_row = insert_intent_for_test(proposed, executor=txn)

        for allocation_values in allocations:
            allocation = dict(allocation_values)
            allocation.setdefault('group_id', group_id)
            allocation.setdefault('workspace', workspace)
            allocation.setdefault('created_by_intent_generation',
                                  next_generation)
            insert_allocation_for_test(allocation, executor=txn)
        for desire_values in desires:
            desire = dict(desire_values)
            desire.setdefault('group_id', group_id)
            desire.setdefault('workspace', workspace)
            desire.setdefault('intent_generation', next_generation)
            insert_desire_for_test(desire, executor=txn)

        _execute(
            txn,
            schema.GROUPS.update().where(
                schema.GROUPS.c.group_id == group_id,
                schema.GROUPS.c.workspace == workspace,
                schema.GROUPS.c.current_intent_generation ==
                group_row['current_intent_generation'],
            ).values(current_intent_generation=next_generation,
                     updated_at=sqlalchemy.func.clock_timestamp()),
        )
        return intent_row
