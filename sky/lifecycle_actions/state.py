"""Read-only access to the inert lifecycle action store foundation."""

import dataclasses
import datetime
import uuid

import sqlalchemy

from sky.utils.db import db_utils
from sky.utils.db import migration_utils

_STORE_KEY = 'global'
_PILOT_DOMAIN = 'VOLUME'
_PILOT_OPERATION_SUBSET = 'KUBERNETES_PVC_OWNED_LIFECYCLE_V1'
_PILOT_STORE_MODE = 'CENTRAL_POSTGRESQL'


@dataclasses.dataclass(frozen=True)
class StoreIdentitySnapshot:
    """Immutable identity of the central lifecycle store."""

    store_key: str
    store_uuid: uuid.UUID
    schema_version: int
    writer_authority_digest: str | None
    created_at: datetime.datetime


@dataclasses.dataclass(frozen=True)
class OwnershipScopeSnapshot:
    """Immutable view of the Kubernetes PVC pilot ownership scope."""

    domain: str
    operation_subset: str
    store_mode: str
    routing_mode: str
    minimum_lifecycle_version: int
    ownership_epoch: int
    authority_generation: int
    writer_implementation_digest: str | None
    reconciler_implementation_digest: str | None
    updated_at: datetime.datetime


@dataclasses.dataclass(frozen=True)
class FoundationSnapshot:
    """Immutable lifecycle store identity and pilot scope snapshot."""

    store_identity: StoreIdentitySnapshot
    ownership_scope: OwnershipScopeSnapshot


def _require_postgresql(engine: sqlalchemy.engine.Engine) -> None:
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('The lifecycle action store requires PostgreSQL.')


def _validate_timestamp(value: object, finite: object,
                        field: str) -> datetime.datetime:
    if (not isinstance(value, datetime.datetime) or value.tzinfo is None or
            value.utcoffset() is None or finite is not True):
        raise RuntimeError(
            f'Lifecycle foundation has an invalid {field} timestamp.')
    return value


def _read_foundation_from_engine(
        engine: sqlalchemy.engine.Engine) -> FoundationSnapshot:
    _require_postgresql(engine)
    with engine.connect().execution_options(
            isolation_level='REPEATABLE READ') as connection:
        transaction = connection.begin()
        try:
            connection.exec_driver_sql('SET TRANSACTION READ ONLY')
            store_rows = connection.execute(
                sqlalchemy.text("""
                    SELECT store_key,
                           store_uuid,
                           schema_version,
                           writer_authority_digest,
                           created_at,
                           isfinite(created_at) AS created_at_finite
                    FROM lifecycle_store_identity
                    ORDER BY store_key
                """)).mappings().all()
            scope_rows = connection.execute(
                sqlalchemy.text("""
                    SELECT domain,
                           operation_subset,
                           store_mode,
                           routing_mode,
                           minimum_lifecycle_version,
                           ownership_epoch,
                           authority_generation,
                           writer_implementation_digest,
                           reconciler_implementation_digest,
                           updated_at,
                           isfinite(updated_at) AS updated_at_finite
                    FROM lifecycle_ownership_scopes
                    WHERE domain = :domain
                      AND operation_subset = :operation_subset
                      AND store_mode = :store_mode
                """), {
                    'domain': _PILOT_DOMAIN,
                    'operation_subset': _PILOT_OPERATION_SUBSET,
                    'store_mode': _PILOT_STORE_MODE,
                }).mappings().all()
            transaction.commit()
        except BaseException:
            transaction.rollback()
            raise

    if len(store_rows) != 1:
        raise RuntimeError('Lifecycle foundation requires exactly one store '
                           f'identity; found {len(store_rows)}.')
    store_row = store_rows[0]
    store_uuid = store_row['store_uuid']
    if (store_row['store_key'] != _STORE_KEY or
            not isinstance(store_uuid, uuid.UUID) or store_uuid.version != 4 or
            store_uuid.variant != uuid.RFC_4122 or
            store_row['schema_version'] != 1 or
            store_row['writer_authority_digest'] is not None):
        raise RuntimeError('Lifecycle store identity is malformed or '
                           'incompatible with schema version 1.')
    store = StoreIdentitySnapshot(
        store_key=store_row['store_key'],
        store_uuid=store_uuid,
        schema_version=store_row['schema_version'],
        writer_authority_digest=store_row['writer_authority_digest'],
        created_at=_validate_timestamp(store_row['created_at'],
                                       store_row['created_at_finite'],
                                       'created_at'),
    )

    if len(scope_rows) != 1:
        raise RuntimeError('Lifecycle foundation requires exactly one volume '
                           f'pilot scope; found {len(scope_rows)}.')
    scope_row = scope_rows[0]
    if (scope_row['domain'] != _PILOT_DOMAIN or
            scope_row['operation_subset'] != _PILOT_OPERATION_SUBSET or
            scope_row['store_mode'] != _PILOT_STORE_MODE or
            scope_row['routing_mode'] != 'DARK' or
            scope_row['minimum_lifecycle_version'] != 0 or
            scope_row['ownership_epoch'] != 1 or
            scope_row['authority_generation'] != 0 or
            scope_row['writer_implementation_digest'] is not None or
            scope_row['reconciler_implementation_digest'] is not None):
        raise RuntimeError('Lifecycle ownership scope is not the exact inert '
                           'M3-S2 pilot.')
    scope = OwnershipScopeSnapshot(
        domain=scope_row['domain'],
        operation_subset=scope_row['operation_subset'],
        store_mode=scope_row['store_mode'],
        routing_mode=scope_row['routing_mode'],
        minimum_lifecycle_version=scope_row['minimum_lifecycle_version'],
        ownership_epoch=scope_row['ownership_epoch'],
        authority_generation=scope_row['authority_generation'],
        writer_implementation_digest=scope_row['writer_implementation_digest'],
        reconciler_implementation_digest=scope_row[
            'reconciler_implementation_digest'],
        updated_at=_validate_timestamp(scope_row['updated_at'],
                                       scope_row['updated_at_finite'],
                                       'updated_at'),
    )
    return FoundationSnapshot(store_identity=store, ownership_scope=scope)


def _initialize_schema(
    engine: sqlalchemy.engine.Engine,
    mode: migration_utils.MigrationMode | None = None,
) -> None:
    """Initialize or verify the PostgreSQL-only lifecycle foundation."""
    _require_postgresql(engine)
    migration_utils.safe_alembic_upgrade(
        engine,
        migration_utils.LIFECYCLE_ACTIONS_DB_NAME,
        migration_utils.LIFECYCLE_ACTIONS_VERSION,
        mode=(migration_utils.configured_migration_mode()
              if mode is None else mode),
    )
    _read_foundation_from_engine(engine)


# Do not pass engine_namespace: lifecycle actions intentionally share the
# ordinary central PostgreSQL engine and its process-local connection budget.
_db_manager = db_utils.DatabaseManager(
    migration_utils.LIFECYCLE_ACTIONS_DB_NAME,
    _initialize_schema,
)


def initialize_and_verify() -> None:
    """Initialize the lazy store and fail closed on an invalid foundation."""
    _db_manager.get_engine()


def read_foundation() -> FoundationSnapshot:
    """Read the current store identity and exact inert pilot scope."""
    engine = _db_manager.get_engine()
    return _read_foundation_from_engine(engine)


__all__ = [
    'FoundationSnapshot',
    'OwnershipScopeSnapshot',
    'StoreIdentitySnapshot',
    'initialize_and_verify',
    'read_foundation',
]
