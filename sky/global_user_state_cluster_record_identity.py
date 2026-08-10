"""Persistence gateway for action-aware cluster-record identities."""

from collections.abc import Callable
from collections.abc import Mapping
import dataclasses
import enum
import json
import pickle
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils


class ClusterRecordIdentityWriteOutcome(enum.Enum):
    """Successful outcomes of the action-aware cluster identity primitive."""

    INSERTED = 'inserted'
    ADOPTED = 'adopted'


class ClusterRecordIdentityConflictError(RuntimeError):
    """A cluster name or record UUID is already committed incompatibly."""


class ClusterRecordHandleChangedError(ClusterRecordIdentityConflictError):
    """The handle changed while an exact-record action was being prepared."""


class ClusterRecordRemovalOutcome(enum.Enum):
    """Successful outcomes of expected-identity cluster-row removal."""

    REMOVED_EXACT = 'removed_exact'
    ALREADY_ABSENT = 'already_absent'


@dataclasses.dataclass(frozen=True)
class ClusterRecordIdentitySnapshot:
    """One exact action-aware cluster row read under its resource locks."""

    cluster_name: str
    cluster_record_uuid: uuid.UUID
    serialized_handle: bytes
    handle: Any


# Preserve the historical public and pickle identities exposed by the facade.
for _public_type in (
        ClusterRecordIdentityWriteOutcome,
        ClusterRecordIdentityConflictError,
        ClusterRecordHandleChangedError,
        ClusterRecordRemovalOutcome,
        ClusterRecordIdentitySnapshot,
):
    _public_type.__module__ = 'sky.global_user_state'


def canonical_cluster_record_uuid(value: uuid.UUID | str) -> uuid.UUID:
    """Validates one UUID value without accepting alternate text spellings."""
    if isinstance(value, uuid.UUID):
        return value
    if not isinstance(value, str):
        raise TypeError('cluster_record_uuid must be a UUID or canonical UUID '
                        'text.')
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as e:
        raise ValueError(
            'cluster_record_uuid must be canonical UUID text.') from e
    if str(parsed) != value:
        raise ValueError('cluster_record_uuid must be canonical UUID text.')
    return parsed


def lock_cluster_record_uuid_in_session(session: orm.Session,
                                        record_uuid: uuid.UUID) -> None:
    """Serializes claims of one cluster-record UUID across cluster names."""
    lock_key = json.dumps(
        ('resource_action_cluster_record_uuid', str(record_uuid)),
        separators=(',', ':'))
    session.execute(
        sqlalchemy.text('SELECT pg_advisory_xact_lock('
                        'hashtextextended(CAST(:lock_key AS text), 0))'),
        {'lock_key': lock_key})


def commit_cluster_record_identity_in_session(
    session: orm.Session,
    cluster_table: sqlalchemy.Table,
    lifecycle_locker: Callable[[orm.Session, str], None],
    uuid_locker: Callable[[orm.Session, uuid.UUID], None],
    cluster_name: str,
    cluster_record_uuid: uuid.UUID | str,
    *,
    insert_values: Mapping[str, Any] | None = None,
) -> ClusterRecordIdentityWriteOutcome:
    """Insert or exactly adopt one identity in a caller-owned transaction.

    A caller inserting a missing row must either pass all ordinary insert
    values or finish populating it before committing this same transaction.
    """
    if not isinstance(cluster_name, str):
        raise TypeError('cluster_name must be text.')
    if not cluster_name:
        raise ValueError('cluster_name must be nonempty.')
    parsed_uuid = canonical_cluster_record_uuid(cluster_record_uuid)
    bind = session.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'Action-aware cluster identity requires the central PostgreSQL '
            'database.')

    # The name lock is shared with ordinary cluster upserts and removal.  The
    # UUID lock closes the independent same-UUID/different-name race before the
    # partial unique index becomes the last line of defense.
    lifecycle_locker(session, cluster_name)
    uuid_locker(session, parsed_uuid)

    name_row = session.execute(
        sqlalchemy.select(
            cluster_table.c.name, cluster_table.c.cluster_record_uuid).where(
                cluster_table.c.name ==
                cluster_name).with_for_update()).mappings().one_or_none()
    if name_row is not None:
        committed_uuid = name_row['cluster_record_uuid']
        if committed_uuid == parsed_uuid:
            return ClusterRecordIdentityWriteOutcome.ADOPTED
        observed = ('null' if committed_uuid is None else str(committed_uuid))
        raise ClusterRecordIdentityConflictError(
            f'Cluster {cluster_name!r} has incompatible cluster-record UUID '
            f'{observed}; expected {parsed_uuid}.')

    uuid_row = session.execute(
        sqlalchemy.select(cluster_table.c.name).where(
            cluster_table.c.cluster_record_uuid ==
            parsed_uuid).with_for_update()).mappings().one_or_none()
    if uuid_row is not None:
        raise ClusterRecordIdentityConflictError(
            f'Cluster-record UUID {parsed_uuid} is already committed '
            f'to cluster {uuid_row["name"]!r}, not {cluster_name!r}.')

    values = dict(insert_values or {})
    unexpected_identity = {'name', 'cluster_record_uuid'}.intersection(values)
    if unexpected_identity:
        raise ValueError('insert_values must omit identity-owned columns: ' +
                         ', '.join(sorted(unexpected_identity)))
    insert_statement = postgresql.insert(cluster_table).values(
        name=cluster_name, cluster_record_uuid=parsed_uuid, **values)
    inserted = session.execute(
        insert_statement.on_conflict_do_nothing()).rowcount
    if inserted != 1:
        # Every action-aware writer takes both advisory locks above, so a
        # conflict here means an out-of-contract writer raced the commitment.
        raise ClusterRecordIdentityConflictError(
            f'Cluster identity for {cluster_name!r} and '
            f'{parsed_uuid} changed concurrently.')
    return ClusterRecordIdentityWriteOutcome.INSERTED


def read_cluster_record_identity_in_session(
    session: orm.Session,
    cluster_table: sqlalchemy.Table,
    lifecycle_locker: Callable[[orm.Session, str], None],
    uuid_locker: Callable[[orm.Session, uuid.UUID], None],
    cluster_name: str,
    expected_cluster_record_uuid: uuid.UUID | str,
) -> ClusterRecordIdentitySnapshot | None:
    """Read one exact action-aware cluster row in a caller transaction.

    The absence result is authoritative only for the row in this PostgreSQL
    transaction.  A present legacy/null or differently identified same-name
    row is a conflict, never an absence result.
    """
    if not isinstance(cluster_name, str):
        raise TypeError('cluster_name must be text.')
    if not cluster_name:
        raise ValueError('cluster_name must be nonempty.')
    parsed_uuid = canonical_cluster_record_uuid(expected_cluster_record_uuid)
    bind = session.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'Action-aware cluster identity requires the central PostgreSQL '
            'database.')

    lifecycle_locker(session, cluster_name)
    uuid_locker(session, parsed_uuid)
    row = session.execute(
        sqlalchemy.select(
            cluster_table.c.cluster_record_uuid,
            cluster_table.c.handle,
        ).where(cluster_table.c.name ==
                cluster_name).with_for_update()).mappings().one_or_none()
    if row is None:
        return None
    observed_uuid = row['cluster_record_uuid']
    if observed_uuid != parsed_uuid:
        observed = ('null' if observed_uuid is None else str(observed_uuid))
        raise ClusterRecordIdentityConflictError(
            f'Cluster {cluster_name!r} has incompatible cluster-record UUID '
            f'{observed}; expected {parsed_uuid}.')
    serialized_handle = row['handle']
    if not isinstance(serialized_handle, bytes) or not serialized_handle:
        raise ClusterRecordIdentityConflictError(
            f'Cluster {cluster_name!r} with cluster-record UUID '
            f'{parsed_uuid} has no exact persisted handle.')
    try:
        handle = pickle.loads(serialized_handle)
    except Exception as e:  # pylint: disable=broad-except
        raise ClusterRecordIdentityConflictError(
            f'Cluster {cluster_name!r} with cluster-record UUID '
            f'{parsed_uuid} has an unreadable persisted handle.') from e
    return ClusterRecordIdentitySnapshot(
        cluster_name=cluster_name,
        cluster_record_uuid=parsed_uuid,
        serialized_handle=serialized_handle,
        handle=handle,
    )
