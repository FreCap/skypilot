"""Catalog, publication, and idempotent operation persistence.

Provider I/O is intentionally absent from this module.  Every function is a
bounded PostgreSQL transaction or read projection.
"""
# pylint: disable=missing-class-docstring

from __future__ import annotations

import dataclasses
import json
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky import global_user_state
from sky.container_images import models
from sky.container_images import schema
from sky.utils.db import db_utils

_CATALOG_ROW_ID = 'authority'
_OPERATION_RETENTION_SECONDS = 30 * 24 * 60 * 60
_FAILED_RESERVATION_SECONDS = 30 * 24 * 60 * 60
_FAILED_PUBLICATION_RETENTION_SECONDS = 90 * 24 * 60 * 60
_CATALOG_CHILD_SUMMARY_LIMIT = 10

PUBLIC_OPERATION_KINDS = frozenset({
    'PUBLISH',
    'PREPARE',
    'RETRY_PUBLICATION',
    'RETRY_LOCATION',
})
ADMIN_OPERATION_KINDS = frozenset({
    'PROFILE_QUALIFY',
    'PROFILE_CANARY',
})
ALL_OPERATION_KINDS = PUBLIC_OPERATION_KINDS | ADMIN_OPERATION_KINDS


class IdempotencyKeyReusedError(ValueError):
    """The same operation key was submitted with another request body."""


class ReleaseConflictError(ValueError):
    """A live immutable release reservation has different identity."""


class ManagedImageDatabaseRequiredError(RuntimeError):
    """Managed image state cannot run without the central PostgreSQL store."""


@dataclasses.dataclass(frozen=True)
class OperationRecord:
    id: str
    authority_id: str
    scope: str
    actor_hash: str
    kind: str
    idempotency_key: str
    request_hash: str
    state: models.ImageOperationState
    result_kind: str | None
    result_id: str | None
    result: dict[str, Any] | None
    error_code: str | None
    lease_token: str | None
    lease_expires_at: int | None
    child_launch_id: str | None
    teardown_deadline: int | None
    created_at: int
    updated_at: int
    terminal_expires_at: int | None
    canary_child_evidence: dict[str, Any] | None = None


@dataclasses.dataclass(frozen=True)
class ArtifactRecord:
    id: str
    workspace: str
    runtime_digest: str
    platform: str
    config_digest: str
    manifest_media_type: str
    manifest_size_bytes: int
    declared_size_bytes: int
    creator_user_hash: str
    producer_kind: str
    producer_spec_hash: str | None
    builder_version: str | None
    created_at: int
    updated_at: int


@dataclasses.dataclass(frozen=True)
class SourceRecord:
    id: str
    workspace: str
    image_id: str
    source_ref: str
    source_root_digest: str
    source_root_media_type: str
    requested_platform: str
    selected_child_digest: str
    source_auth_binding_id: str | None
    source_auth_fingerprint: str | None
    created_at: int


@dataclasses.dataclass(frozen=True)
class PublicationRecord:
    id: str
    workspace: str
    operation_id: str | None
    profile_revision_id: str
    requested_release: str
    reservation_active: bool
    source_ref: str
    source_root_digest: str
    requested_platform: str
    source_auth_binding_id: str | None
    source_auth_fingerprint: str | None
    state: models.ImagePublicationState
    inspection_lease_token: str | None
    inspection_lease_expires_at: int | None
    attempt_count: int
    next_retry_at: int | None
    error_code: str | None
    image_id: str | None
    source_id: str | None
    canonical_location_id: str | None
    reservation_expires_at: int | None
    record_expires_at: int | None
    created_at: int
    updated_at: int

    @property
    def published_release(self) -> str | None:
        if self.state != models.ImagePublicationState.READY:
            return None
        return self.requested_release


def engine() -> sqlalchemy.engine.Engine:
    result = global_user_state.initialize_and_get_db()
    if result.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ManagedImageDatabaseRequiredError(
            'Managed container image state requires the central PostgreSQL '
            'database.')
    return result


def database_epoch_expression(*, now: int | None = None) -> Any:
    """Returns the database wall clock used by lease-fenced mutations.

    PostgreSQL transaction and statement timestamps can predate a blocking row
    lock. ``clock_timestamp()`` is evaluated when the mutation evaluates its
    fence. An explicit literal keeps deterministic state-machine tests cheap.
    """
    if now is not None:
        return sqlalchemy.literal(now, type_=sqlalchemy.BigInteger())
    return sqlalchemy.cast(
        sqlalchemy.func.floor(
            sqlalchemy.extract('epoch', sqlalchemy.func.clock_timestamp())),
        sqlalchemy.BigInteger)


def database_epoch(session: orm.Session, *, now: int | None = None) -> int:
    """Reads a fresh database epoch after callers acquire blocking locks."""
    if now is not None:
        return now
    return int(
        session.execute(sqlalchemy.select(
            database_epoch_expression())).scalar_one())


def read_database_epoch(*, now: int | None = None) -> int:
    """Samples the central clock for a non-mutating eligibility decision."""
    with orm.Session(engine()) as session:
        return database_epoch(session, now=now)


def _operation(row: sqlalchemy.engine.RowMapping) -> OperationRecord:
    result_json = row['result_json']
    canary_child_evidence_json = row['canary_child_evidence_json']
    return OperationRecord(
        id=str(row['id']),
        authority_id=str(row['authority_id']),
        scope=str(row['scope']),
        actor_hash=str(row['actor_hash']),
        kind=str(row['kind']),
        idempotency_key=str(row['idempotency_key']),
        request_hash=str(row['request_hash']),
        state=models.ImageOperationState(str(row['state'])),
        result_kind=row['result_kind'],
        result_id=row['result_id'],
        result=json.loads(result_json) if result_json is not None else None,
        error_code=row['error_code'],
        lease_token=row['lease_token'],
        lease_expires_at=row['lease_expires_at'],
        child_launch_id=row['child_launch_id'],
        teardown_deadline=row['teardown_deadline'],
        created_at=int(row['created_at']),
        updated_at=int(row['updated_at']),
        terminal_expires_at=row['terminal_expires_at'],
        canary_child_evidence=(json.loads(canary_child_evidence_json)
                               if canary_child_evidence_json is not None else
                               None),
    )


def _artifact(row: sqlalchemy.engine.RowMapping) -> ArtifactRecord:
    return ArtifactRecord(
        id=str(row['id']),
        workspace=str(row['workspace']),
        runtime_digest=str(row['runtime_digest']),
        platform=str(row['platform']),
        config_digest=str(row['config_digest']),
        manifest_media_type=str(row['manifest_media_type']),
        manifest_size_bytes=int(row['manifest_size_bytes']),
        declared_size_bytes=int(row['declared_size_bytes']),
        creator_user_hash=str(row['creator_user_hash']),
        producer_kind=str(row['producer_kind']),
        producer_spec_hash=row['producer_spec_hash'],
        builder_version=row['builder_version'],
        created_at=int(row['created_at']),
        updated_at=int(row['updated_at']),
    )


def _source(row: sqlalchemy.engine.RowMapping) -> SourceRecord:
    return SourceRecord(
        id=str(row['id']),
        workspace=str(row['workspace']),
        image_id=str(row['image_id']),
        source_ref=str(row['source_ref']),
        source_root_digest=str(row['source_root_digest']),
        source_root_media_type=str(row['source_root_media_type']),
        requested_platform=str(row['requested_platform']),
        selected_child_digest=str(row['selected_child_digest']),
        source_auth_binding_id=row['source_auth_binding_id'],
        source_auth_fingerprint=row['source_auth_fingerprint'],
        created_at=int(row['created_at']),
    )


def _publication(row: sqlalchemy.engine.RowMapping) -> PublicationRecord:
    return PublicationRecord(
        id=str(row['id']),
        workspace=str(row['workspace']),
        operation_id=(str(row['operation_id'])
                      if row['operation_id'] is not None else None),
        profile_revision_id=str(row['profile_revision_id']),
        requested_release=str(row['requested_release']),
        reservation_active=bool(row['reservation_active']),
        source_ref=str(row['source_ref']),
        source_root_digest=str(row['source_root_digest']),
        requested_platform=str(row['requested_platform']),
        source_auth_binding_id=row['source_auth_binding_id'],
        source_auth_fingerprint=row['source_auth_fingerprint'],
        state=models.ImagePublicationState(str(row['state'])),
        inspection_lease_token=row['inspection_lease_token'],
        inspection_lease_expires_at=row['inspection_lease_expires_at'],
        attempt_count=int(row['attempt_count']),
        next_retry_at=row['next_retry_at'],
        error_code=row['error_code'],
        image_id=row['image_id'],
        source_id=row['source_id'],
        canonical_location_id=row['canonical_location_id'],
        reservation_expires_at=row['reservation_expires_at'],
        record_expires_at=row['record_expires_at'],
        created_at=int(row['created_at']),
        updated_at=int(row['updated_at']),
    )


def get_catalog_authority_id() -> str:
    """Returns the migration-owned catalog authority or fails closed."""
    table = schema.catalog
    with orm.Session(engine()) as session:
        rows = session.execute(
            sqlalchemy.select(table).limit(2)).mappings().all()
    valid = (len(rows) == 1 and str(rows[0]['id']) == _CATALOG_ROW_ID and
             isinstance(rows[0]['created_at'], int) and
             int(rows[0]['created_at']) > 0)
    authority = str(rows[0]['authority_id']) if len(rows) == 1 else ''
    if valid:
        try:
            uuid.UUID(authority)
        except ValueError:
            valid = False
    if not valid:
        raise RuntimeError(
            'Managed image catalog authority is missing or malformed. Run '
            'the central database migration before serving image traffic.')
    return authority


def begin_operation(session: orm.Session,
                    *,
                    authority_id: str,
                    scope: str,
                    actor_hash: str,
                    kind: str,
                    idempotency_key: str,
                    request_hash: str,
                    now: int | None = None) -> tuple[OperationRecord, bool]:
    """Creates or converges one operation inside the caller transaction."""
    if not 16 <= len(idempotency_key.encode()) <= 128:
        raise ValueError('Idempotency keys must contain 16 through 128 bytes.')
    current = database_epoch(session, now=now)
    table = schema.operations
    operation_id = str(uuid.uuid4())
    result = session.execute(
        postgresql.insert(table).values(
            id=operation_id,
            authority_id=authority_id,
            scope=scope,
            actor_hash=actor_hash,
            kind=kind,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            state=models.ImageOperationState.PENDING.value,
            created_at=current,
            updated_at=current).on_conflict_do_nothing(index_elements=[
                table.c.authority_id, table.c.scope, table.c.actor_hash,
                table.c.kind, table.c.idempotency_key
            ]).returning(table))
    row = result.mappings().first()
    created = row is not None
    if row is None:
        row = session.execute(
            sqlalchemy.select(table).where(
                table.c.authority_id == authority_id, table.c.scope == scope,
                table.c.actor_hash == actor_hash, table.c.kind == kind,
                table.c.idempotency_key ==
                idempotency_key).with_for_update()).mappings().one()
        if str(row['request_hash']) != request_hash:
            raise IdempotencyKeyReusedError('IDEMPOTENCY_KEY_REUSED')
    return _operation(row), created


def get_operation(
    operation_id: str | None,
    scope: str,
    *,
    allowed_kinds: frozenset[str] | None = None,
) -> OperationRecord | None:
    if operation_id is None:
        return None
    if allowed_kinds is not None and not allowed_kinds:
        return None
    table = schema.operations
    predicates = [table.c.id == operation_id, table.c.scope == scope]
    if allowed_kinds is not None:
        predicates.append(table.c.kind.in_(sorted(allowed_kinds)))
    with orm.Session(engine()) as session:
        row = session.execute(
            sqlalchemy.select(table).where(*predicates)).mappings().first()
    return _operation(row) if row is not None else None


def _operation_lease_predicates(
    table: sqlalchemy.Table,
    *,
    lease_token: str | None,
    deadline_expired: bool | None,
    now: int | None,
) -> tuple[Any, ...]:
    if lease_token is None:
        if deadline_expired is not None:
            raise ValueError('A canary deadline fence requires a lease token.')
        return (table.c.state.in_([
            models.ImageOperationState.PENDING.value,
            models.ImageOperationState.RUNNING.value,
        ]),)
    clock = database_epoch_expression(now=now)
    predicates: list[Any] = [
        table.c.kind == 'PROFILE_CANARY',
        table.c.state == models.ImageOperationState.RUNNING.value,
        table.c.lease_token == lease_token,
        table.c.lease_expires_at.is_not(None),
        table.c.lease_expires_at > clock,
        table.c.teardown_deadline.is_not(None),
    ]
    if deadline_expired is True:
        predicates.append(table.c.teardown_deadline <= clock)
    elif deadline_expired is False:
        predicates.append(table.c.teardown_deadline > clock)
    return tuple(predicates)


def create_or_get_operation(
        *,
        authority_id: str,
        scope: str,
        actor_hash: str,
        kind: str,
        idempotency_key: str,
        request_hash: str,
        now: int | None = None) -> tuple[OperationRecord, bool]:
    with orm.Session(engine()) as session, session.begin():
        return begin_operation(session,
                               authority_id=authority_id,
                               scope=scope,
                               actor_hash=actor_hash,
                               kind=kind,
                               idempotency_key=idempotency_key,
                               request_hash=request_hash,
                               now=now)


def bind_operation_result(operation_id: str,
                          *,
                          result_kind: str,
                          result_id: str,
                          result: dict[str, Any],
                          terminal_state: models.ImageOperationState |
                          None = None,
                          error_code: str | None = None,
                          now: int | None = None) -> OperationRecord:
    with orm.Session(engine()) as session, session.begin():
        current = database_epoch(session, now=now)
        values: dict[str, Any] = {
            'result_kind': result_kind,
            'result_id': result_id,
            'result_json': json.dumps(result,
                                      sort_keys=True,
                                      separators=(',', ':')),
            'updated_at': current,
        }
        if terminal_state is not None:
            if terminal_state not in (models.ImageOperationState.SUCCEEDED,
                                      models.ImageOperationState.FAILED):
                raise ValueError('Operation terminal state is invalid.')
            values.update(state=terminal_state.value,
                          error_code=error_code,
                          canary_child_evidence_json=None,
                          terminal_expires_at=(current +
                                               _OPERATION_RETENTION_SECONDS))
        row = session.execute(schema.operations.update().where(
            schema.operations.c.id == operation_id,
            schema.operations.c.state.in_([
                models.ImageOperationState.PENDING.value,
                models.ImageOperationState.RUNNING.value,
            ])).values(**values).returning(
                schema.operations)).mappings().first()
        if row is None:
            row = session.execute(
                sqlalchemy.select(schema.operations).where(
                    schema.operations.c.id == operation_id)).mappings().one()
            if (str(row['result_kind']) != result_kind or
                    str(row['result_id']) != result_id):
                raise ValueError('Operation result is immutable.')
        return _operation(row)


def complete_operation(session: orm.Session,
                       operation_id: str,
                       *,
                       result_kind: str,
                       result_id: str,
                       result: dict[str, Any],
                       lease_token: str | None = None,
                       deadline_expired: bool | None = None,
                       now: int | None = None) -> bool:
    current = database_epoch(session, now=now)
    table = schema.operations
    row_count = session.execute(schema.operations.update().where(
        table.c.id == operation_id,
        *_operation_lease_predicates(
            table,
            lease_token=lease_token,
            deadline_expired=deadline_expired,
            now=now)).values(state=models.ImageOperationState.SUCCEEDED.value,
                             result_kind=result_kind,
                             result_id=result_id,
                             result_json=json.dumps(result,
                                                    sort_keys=True,
                                                    separators=(',', ':')),
                             error_code=None,
                             lease_token=None,
                             lease_expires_at=None,
                             child_launch_id=None,
                             canary_child_evidence_json=None,
                             teardown_deadline=None,
                             updated_at=current,
                             terminal_expires_at=current +
                             _OPERATION_RETENTION_SECONDS)).rowcount
    return row_count == 1


def fail_operation(session: orm.Session,
                   operation_id: str,
                   error_code: str,
                   *,
                   result_kind: str | None = None,
                   result_id: str | None = None,
                   result: dict[str, Any] | None = None,
                   lease_token: str | None = None,
                   deadline_expired: bool | None = None,
                   now: int | None = None) -> bool:
    current = database_epoch(session, now=now)
    table = schema.operations
    row_count = session.execute(schema.operations.update().where(
        table.c.id == operation_id,
        *_operation_lease_predicates(
            table,
            lease_token=lease_token,
            deadline_expired=deadline_expired,
            now=now)).values(state=models.ImageOperationState.FAILED.value,
                             result_kind=result_kind,
                             result_id=result_id,
                             result_json=(json.dumps(
                                 result, sort_keys=True, separators=(',', ':'))
                                          if result is not None else None),
                             error_code=error_code,
                             lease_token=None,
                             lease_expires_at=None,
                             child_launch_id=None,
                             canary_child_evidence_json=None,
                             teardown_deadline=None,
                             updated_at=current,
                             terminal_expires_at=current +
                             _OPERATION_RETENTION_SECONDS)).rowcount
    return row_count == 1


def create_publication(
        *,
        authority_id: str,
        workspace: str,
        actor_hash: str,
        idempotency_key: str,
        request_hash: str,
        profile_revision_id: str,
        release: str,
        source_ref: str,
        source_root_digest: str,
        requested_platform: str,
        source_auth_binding_id: str | None,
        source_auth_fingerprint: str | None,
        now: int | None = None) -> tuple[OperationRecord, PublicationRecord]:
    """Commits one release reservation and its idempotent operation."""
    release = models.validate_release_label(release, 'Image release')
    source_ref = models.validate_oci_reference(source_ref, 'Image source')
    source_root_digest = models.validate_sha256_digest(source_root_digest,
                                                       'Source root digest')
    requested_platform = models.validate_oci_platform(requested_platform,
                                                      'Requested platform')
    table = schema.publications
    with orm.Session(engine()) as session, session.begin():
        current = database_epoch(session, now=now)
        operation, created = begin_operation(session,
                                             authority_id=authority_id,
                                             scope=workspace,
                                             actor_hash=actor_hash,
                                             kind='PUBLISH',
                                             idempotency_key=idempotency_key,
                                             request_hash=request_hash,
                                             now=current)
        if not created:
            if operation.result_id is None:
                raise RuntimeError(
                    'A converged publish operation has no publication result.')
            row = session.execute(
                sqlalchemy.select(table).where(
                    table.c.id == operation.result_id)).mappings().one()
            return operation, _publication(row)

        publication_id = str(uuid.uuid4())
        row = session.execute(
            postgresql.insert(table).values(
                id=publication_id,
                workspace=workspace,
                operation_id=operation.id,
                profile_revision_id=profile_revision_id,
                requested_release=release,
                reservation_active=True,
                source_ref=source_ref,
                source_root_digest=source_root_digest,
                requested_platform=requested_platform,
                source_auth_binding_id=source_auth_binding_id,
                source_auth_fingerprint=source_auth_fingerprint,
                state=models.ImagePublicationState.PENDING.value,
                attempt_count=0,
                created_at=current,
                updated_at=current).on_conflict_do_nothing(
                    index_elements=[
                        table.c.workspace, table.c.requested_release
                    ],
                    index_where=table.c.reservation_active.is_(True)).returning(
                        table)).mappings().first()
        if row is None:
            existing = session.execute(
                sqlalchemy.select(table).where(
                    table.c.workspace == workspace,
                    table.c.requested_release == release,
                    table.c.reservation_active.is_(
                        True)).with_for_update()).mappings().one()
            same_identity = (
                str(existing['source_root_digest']) == source_root_digest and
                str(existing['requested_platform']) == requested_platform and
                str(existing['profile_revision_id']) == profile_revision_id)
            if not same_identity:
                raise ReleaseConflictError('RELEASE_CONFLICT')
            publication = _publication(existing)
            operation_values: dict[str, Any] = {
                'result_kind': 'publication',
                'result_id': publication.id,
                'result_json': json.dumps(
                    {
                        'publication_id': publication.id,
                        'state': publication.state.value,
                    },
                    sort_keys=True),
                'updated_at': current,
            }
            if publication.state == models.ImagePublicationState.READY:
                operation_values.update(
                    state=models.ImageOperationState.SUCCEEDED.value,
                    terminal_expires_at=(current +
                                         _OPERATION_RETENTION_SECONDS))
            elif publication.state == models.ImagePublicationState.FAILED:
                operation_values.update(
                    state=models.ImageOperationState.FAILED.value,
                    error_code=publication.error_code,
                    terminal_expires_at=(current +
                                         _OPERATION_RETENTION_SECONDS))
            session.execute(schema.operations.update().where(
                schema.operations.c.id == operation.id).values(
                    **operation_values))
            refreshed = session.execute(
                sqlalchemy.select(schema.operations).where(
                    schema.operations.c.id == operation.id)).mappings().one()
            return _operation(refreshed), publication
        session.execute(schema.operations.update().where(
            schema.operations.c.id == operation.id).values(
                result_kind='publication',
                result_id=publication_id,
                result_json=json.dumps(
                    {
                        'publication_id': publication_id,
                        'state': models.ImagePublicationState.PENDING.value,
                    },
                    sort_keys=True),
                updated_at=current))
        refreshed = session.execute(
            sqlalchemy.select(schema.operations).where(
                schema.operations.c.id == operation.id)).mappings().one()
        return _operation(refreshed), _publication(row)


def get_publication(publication_id: str,
                    workspace: str) -> PublicationRecord | None:
    with orm.Session(engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.publications).where(
                schema.publications.c.id == publication_id,
                schema.publications.c.workspace ==
                workspace)).mappings().first()
    return _publication(row) if row is not None else None


def get_ready_release(release: str, workspace: str) -> PublicationRecord | None:
    with orm.Session(engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.publications).where(
                schema.publications.c.workspace == workspace,
                schema.publications.c.requested_release == release,
                schema.publications.c.state ==
                models.ImagePublicationState.READY.value,
                schema.publications.c.reservation_active.is_(
                    True))).mappings().first()
    return _publication(row) if row is not None else None


def get_ready_publication_for_artifact(
        image_id: str,
        workspace: str,
        *,
        profile_revision_id: str | None = None) -> PublicationRecord | None:
    statement = sqlalchemy.select(schema.publications).where(
        schema.publications.c.workspace == workspace,
        schema.publications.c.image_id == image_id,
        schema.publications.c.state == models.ImagePublicationState.READY.value,
        schema.publications.c.reservation_active.is_(True))
    if profile_revision_id is not None:
        statement = statement.where(
            schema.publications.c.profile_revision_id == profile_revision_id)
    statement = statement.order_by(schema.publications.c.created_at,
                                   schema.publications.c.id).limit(1)
    with orm.Session(engine()) as session:
        row = session.execute(statement).mappings().first()
    return _publication(row) if row is not None else None


def claim_publication_inspection(
        *,
        worker_id: str,
        lease_seconds: int,
        workspace: str | None = None,
        now: int | None = None) -> PublicationRecord | None:
    """Claims one indexed unbound source inspection with a random fence."""
    token = f'{worker_id}:{uuid.uuid4()}'
    table = schema.publications
    with orm.Session(engine()) as session, session.begin():
        current = database_epoch(session, now=now)
        clock = database_epoch_expression(now=now)
        statement = sqlalchemy.select(table.c.id).where(
            table.c.canonical_location_id.is_(None),
            table.c.state.in_([
                models.ImagePublicationState.PENDING.value,
                models.ImagePublicationState.INSPECTING.value,
            ]), table.c.inspection_claimable_at <= clock)
        if workspace is not None:
            statement = statement.where(table.c.workspace == workspace)
        statement = statement.order_by(
            table.c.inspection_claimable_at,
            table.c.id).limit(1).with_for_update(skip_locked=True)
        publication_id = session.execute(statement).scalar_one_or_none()
        if publication_id is None:
            return None
        row = session.execute(
            table.update().where(table.c.id == publication_id).values(
                state=models.ImagePublicationState.INSPECTING.value,
                inspection_lease_token=token,
                inspection_lease_expires_at=current + lease_seconds,
                attempt_count=table.c.attempt_count + 1,
                next_retry_at=None,
                error_code=None,
                updated_at=current).returning(table)).mappings().one()
        return _publication(row)


def fail_publication_inspection(publication_id: str,
                                lease_token: str,
                                error_code: str,
                                *,
                                retry_at: int | None = None,
                                retry_delay_seconds: int | None = None,
                                terminal: bool,
                                now: int | None = None) -> bool:
    if retry_at is not None and retry_delay_seconds is not None:
        raise ValueError('Specify a retry time or delay, not both.')
    if retry_delay_seconds is not None and retry_delay_seconds < 0:
        raise ValueError('Retry delay must be nonnegative.')
    table = schema.publications
    state = (models.ImagePublicationState.FAILED.value
             if terminal else models.ImagePublicationState.PENDING.value)
    with orm.Session(engine()) as session, session.begin():
        clock = database_epoch_expression(now=now)
        values: dict[str, Any] = {
            'state': state,
            'inspection_lease_token': None,
            'inspection_lease_expires_at': None,
            'next_retry_at': (None if terminal else
                              (clock + retry_delay_seconds if
                               retry_delay_seconds is not None else retry_at)),
            'error_code': error_code,
            'updated_at': clock,
        }
        if terminal:
            values.update(
                reservation_expires_at=clock + _FAILED_RESERVATION_SECONDS,
                record_expires_at=clock + _FAILED_PUBLICATION_RETENTION_SECONDS)
        row = session.execute(table.update().where(
            table.c.id == publication_id,
            table.c.state == models.ImagePublicationState.INSPECTING.value,
            table.c.inspection_lease_token == lease_token,
            table.c.inspection_lease_expires_at.is_not(None),
            table.c.inspection_lease_expires_at
            > clock).values(**values).returning(table)).mappings().first()
        if row is not None and terminal:
            operation_matches = [
                schema.operations.c.result_id == publication_id
            ]
            if row['operation_id'] is not None:
                operation_matches.append(
                    schema.operations.c.id == row['operation_id'])
            session.execute(schema.operations.update().where(
                sqlalchemy.or_(*operation_matches),
                schema.operations.c.state.in_([
                    models.ImageOperationState.PENDING.value,
                    models.ImageOperationState.RUNNING.value,
                ])).values(
                    state=models.ImageOperationState.FAILED.value,
                    result_kind='publication',
                    result_id=publication_id,
                    result_json=json.dumps(
                        {
                            'publication_id': publication_id,
                            'release': row['requested_release'],
                            'state': models.ImagePublicationState.FAILED.value,
                            'image_id': row['image_id'],
                        },
                        sort_keys=True),
                    error_code=error_code,
                    updated_at=clock,
                    terminal_expires_at=(clock + _OPERATION_RETENTION_SECONDS)))
    return row is not None


def heartbeat_publication_inspection(publication_id: str,
                                     lease_token: str,
                                     lease_seconds: int,
                                     *,
                                     now: int | None = None) -> bool:
    table = schema.publications
    with orm.Session(engine()) as session, session.begin():
        clock = database_epoch_expression(now=now)
        changed = session.execute(table.update().where(
            table.c.id == publication_id,
            table.c.state == models.ImagePublicationState.INSPECTING.value,
            table.c.inspection_lease_token == lease_token,
            table.c.inspection_lease_expires_at.is_not(None),
            table.c.inspection_lease_expires_at
            > clock).values(inspection_lease_expires_at=clock + lease_seconds,
                            updated_at=clock)).rowcount
    return changed == 1


def source_for_canonical_location(location_id: str) -> SourceRecord | None:
    """Returns the oldest retained source bound to one canonical location."""
    publications = schema.publications
    sources = schema.sources
    with orm.Session(engine()) as session:
        row = session.execute(
            sqlalchemy.select(sources).join(
                publications, publications.c.source_id == sources.c.id).where(
                    publications.c.canonical_location_id == location_id,
                    publications.c.source_id.is_not(None)).order_by(
                        publications.c.created_at,
                        publications.c.id).limit(1)).mappings().first()
    return _source(row) if row is not None else None


def list_artifacts(
    workspace: str,
    *,
    limit: int = 50,
    after: tuple[int, str] | None = None,
    release: str | None = None,
    runtime_digest: str | None = None,
    source_ref: str | None = None,
) -> list[ArtifactRecord]:
    if not 1 <= limit <= 101:
        raise ValueError('Internal catalog page size must be 1 through 101.')
    table = schema.images
    statement = sqlalchemy.select(table).where(table.c.workspace == workspace)
    if runtime_digest is not None:
        statement = statement.where(table.c.runtime_digest == runtime_digest)
    if source_ref is not None:
        statement = statement.where(sqlalchemy.exists().where(
            schema.sources.c.image_id == table.c.id,
            schema.sources.c.workspace == workspace,
            schema.sources.c.source_ref == source_ref))
    if release is not None:
        publication_filter = [
            schema.publications.c.image_id == table.c.id,
            schema.publications.c.workspace == workspace,
            schema.publications.c.state ==
            models.ImagePublicationState.READY.value,
            schema.publications.c.reservation_active.is_(True),
        ]
        publication_filter.append(
            schema.publications.c.requested_release == release)
        statement = statement.where(
            sqlalchemy.exists().where(*publication_filter))
    if after is not None:
        statement = statement.where(
            sqlalchemy.tuple_(table.c.created_at, table.c.id) < after)
    statement = statement.order_by(table.c.created_at.desc(),
                                   table.c.id.desc()).limit(limit)
    with orm.Session(engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_artifact(row) for row in rows]


def catalog_summaries(image_ids: set[str],
                      workspace: str) -> dict[str, dict[str, Any]]:
    """Loads hard-capped child samples for one bounded artifact page."""
    if not image_ids:
        return {}
    if len(image_ids) > 101:
        raise ValueError('At most 101 catalog summaries may be loaded.')
    summaries: dict[str, dict[str, Any]] = {
        image_id: {
            'releases': set(),
            'distributions': set(),
            'source_refs': set(),
            'targets': set(),
            'location_states': {},
            'publications_truncated': False,
            'sources_truncated': False,
            'locations_truncated': False,
        } for image_id in image_ids
    }
    params = {
        'image_ids': json.dumps(sorted(image_ids)),
        'workspace': workspace,
        'sample_limit': _CATALOG_CHILD_SUMMARY_LIMIT + 1,
    }
    with orm.Session(engine()) as session:
        publication_rows = session.execute(
            sqlalchemy.text("""
                WITH page_images AS (
                    SELECT value AS image_id
                    FROM jsonb_array_elements_text(
                        CAST(:image_ids AS jsonb))
                )
                SELECT page.image_id, child.requested_release, child.profile
                FROM page_images AS page
                CROSS JOIN LATERAL (
                    SELECT publication.requested_release, revision.profile,
                           publication.created_at, publication.id
                    FROM container_image_publications AS publication
                    JOIN container_image_profile_revisions AS revision
                      ON revision.id = publication.profile_revision_id
                    WHERE publication.workspace = :workspace
                      AND publication.image_id = page.image_id
                      AND publication.state = 'READY'
                      AND publication.reservation_active IS TRUE
                    ORDER BY publication.created_at, publication.id
                    LIMIT :sample_limit
                ) AS child
                ORDER BY page.image_id, child.created_at, child.id
            """), params).mappings().all()
        source_rows = session.execute(
            sqlalchemy.text("""
                WITH page_images AS (
                    SELECT value AS image_id
                    FROM jsonb_array_elements_text(
                        CAST(:image_ids AS jsonb))
                )
                SELECT page.image_id, child.source_ref
                FROM page_images AS page
                CROSS JOIN LATERAL (
                    SELECT source.source_ref, source.created_at, source.id
                    FROM container_image_sources AS source
                    WHERE source.workspace = :workspace
                      AND source.image_id = page.image_id
                    ORDER BY source.created_at, source.id
                    LIMIT :sample_limit
                ) AS child
                ORDER BY page.image_id, child.created_at, child.id
            """), params).mappings().all()
        location_rows = session.execute(
            sqlalchemy.text("""
                WITH page_images AS (
                    SELECT value AS image_id
                    FROM jsonb_array_elements_text(
                        CAST(:image_ids AS jsonb))
                )
                SELECT page.image_id, child.state, child.target_id
                FROM page_images AS page
                CROSS JOIN LATERAL (
                    SELECT location.state, shard.target_id,
                           location.created_at, location.id
                    FROM container_image_locations AS location
                    JOIN container_image_registry_shards AS shard
                      ON shard.id = location.shard_id
                    WHERE location.workspace = :workspace
                      AND location.image_id = page.image_id
                    ORDER BY location.created_at, location.id
                    LIMIT :sample_limit
                ) AS child
                ORDER BY page.image_id, child.created_at, child.id
            """), params).mappings().all()
    publication_counts: dict[str, int] = {}
    for row in publication_rows:
        image_id = str(row['image_id'])
        publication_counts[image_id] = publication_counts.get(image_id, 0) + 1
        summary = summaries[image_id]
        if publication_counts[image_id] > _CATALOG_CHILD_SUMMARY_LIMIT:
            summary['publications_truncated'] = True
            continue
        summary['releases'].add(str(row['requested_release']))
        summary['distributions'].add(str(row['profile']))
    source_counts: dict[str, int] = {}
    for row in source_rows:
        image_id = str(row['image_id'])
        source_counts[image_id] = source_counts.get(image_id, 0) + 1
        summary = summaries[image_id]
        if source_counts[image_id] > _CATALOG_CHILD_SUMMARY_LIMIT:
            summary['sources_truncated'] = True
            continue
        summary['source_refs'].add(str(row['source_ref']))
    location_counts: dict[str, int] = {}
    for row in location_rows:
        image_id = str(row['image_id'])
        location_counts[image_id] = location_counts.get(image_id, 0) + 1
        summary = summaries[image_id]
        if location_counts[image_id] > _CATALOG_CHILD_SUMMARY_LIMIT:
            summary['locations_truncated'] = True
            continue
        summary['targets'].add(str(row['target_id']))
        state = str(row['state'])
        summary['location_states'][state] = (
            summary['location_states'].get(state, 0) + 1)
    return {
        image_id: {
            'releases': sorted(summary['releases']),
            'distributions': sorted(summary['distributions']),
            'source_refs': sorted(summary['source_refs']),
            'targets': sorted(summary['targets']),
            'location_states': dict(sorted(summary['location_states'].items())),
            'publications_truncated': summary['publications_truncated'],
            'sources_truncated': summary['sources_truncated'],
            'locations_truncated': summary['locations_truncated'],
        } for image_id, summary in summaries.items()
    }


def get_artifact(image_id: str, workspace: str) -> ArtifactRecord | None:
    with orm.Session(engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.images).where(
                schema.images.c.id == image_id,
                schema.images.c.workspace == workspace)).mappings().first()
    return _artifact(row) if row is not None else None


def get_published_artifact(image_id: str,
                           workspace: str) -> ArtifactRecord | None:
    publications = schema.publications
    images = schema.images
    with orm.Session(engine()) as session:
        row = session.execute(
            sqlalchemy.select(images).where(
                images.c.id == image_id, images.c.workspace == workspace,
                sqlalchemy.exists().where(
                    publications.c.image_id == images.c.id, publications.c.state
                    == models.ImagePublicationState.READY.value,
                    publications.c.reservation_active.is_(
                        True)))).mappings().first()
    return _artifact(row) if row is not None else None


def get_published_artifact_by_source(
        workspace: str, source_ref: str,
        requested_platform: str) -> ArtifactRecord | None:
    images = schema.images
    sources = schema.sources
    publications = schema.publications
    with orm.Session(engine()) as session:
        row = session.execute(
            sqlalchemy.select(images).join(
                sources, sources.c.image_id == images.c.id).where(
                    sources.c.workspace == workspace,
                    sources.c.source_ref == source_ref,
                    sources.c.requested_platform == requested_platform,
                    sqlalchemy.exists().where(
                        publications.c.image_id == images.c.id,
                        publications.c.state ==
                        models.ImagePublicationState.READY.value,
                        publications.c.reservation_active.is_(True))).limit(
                            1)).mappings().first()
    return _artifact(row) if row is not None else None


def get_artifact_by_digest(workspace: str, runtime_digest: str,
                           platform: str) -> ArtifactRecord | None:
    with orm.Session(engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.images).where(
                schema.images.c.workspace == workspace,
                schema.images.c.runtime_digest == runtime_digest,
                schema.images.c.platform == platform)).mappings().first()
    return _artifact(row) if row is not None else None


def list_sources(image_id: str,
                 workspace: str,
                 *,
                 limit: int = 50,
                 after: tuple[int, str] | None = None) -> list[SourceRecord]:
    statement = sqlalchemy.select(schema.sources).where(
        schema.sources.c.image_id == image_id,
        schema.sources.c.workspace == workspace)
    if after is not None:
        statement = statement.where(
            sqlalchemy.tuple_(schema.sources.c.created_at, schema.sources.c.id)
            > after)
    statement = statement.order_by(schema.sources.c.created_at,
                                   schema.sources.c.id).limit(limit)
    with orm.Session(engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_source(row) for row in rows]


def list_publications(
        image_id: str,
        workspace: str,
        *,
        limit: int = 50,
        after: tuple[int, str] | None = None) -> list[PublicationRecord]:
    statement = sqlalchemy.select(schema.publications).where(
        schema.publications.c.image_id == image_id,
        schema.publications.c.workspace == workspace)
    if after is not None:
        statement = statement.where(
            sqlalchemy.tuple_(schema.publications.c.created_at,
                              schema.publications.c.id) < after)
    statement = statement.order_by(schema.publications.c.created_at.desc(),
                                   schema.publications.c.id.desc()).limit(limit)
    with orm.Session(engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_publication(row) for row in rows]


def list_workspace_publications(
    workspace: str,
    *,
    limit: int = 50,
    after: tuple[int, str] | None = None,
    state: models.ImagePublicationState | None = None,
    release: str | None = None,
) -> list[PublicationRecord]:
    """Lists bounded publication reservations, including unbound failures."""
    table = schema.publications
    statement = sqlalchemy.select(table).where(table.c.workspace == workspace)
    if state is not None:
        statement = statement.where(table.c.state == state.value)
    if release is not None:
        statement = statement.where(table.c.requested_release == release)
    if after is not None:
        statement = statement.where(
            sqlalchemy.tuple_(table.c.created_at, table.c.id) < after)
    statement = statement.order_by(table.c.created_at.desc(),
                                   table.c.id.desc()).limit(limit)
    with orm.Session(engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_publication(row) for row in rows]


def list_releases(
        image_id: str,
        workspace: str,
        *,
        limit: int = 50,
        after: tuple[int, str] | None = None) -> list[PublicationRecord]:
    """Lists only public READY release projections for one artifact."""
    table = schema.publications
    statement = sqlalchemy.select(table).where(
        table.c.image_id == image_id, table.c.workspace == workspace,
        table.c.state == models.ImagePublicationState.READY.value,
        table.c.reservation_active.is_(True))
    if after is not None:
        statement = statement.where(
            sqlalchemy.tuple_(table.c.updated_at, table.c.id) < after)
    statement = statement.order_by(table.c.updated_at.desc(),
                                   table.c.id.desc()).limit(limit)
    with orm.Session(engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_publication(row) for row in rows]


def compact_terminal_records(*,
                             now: int | None = None,
                             batch_size: int = 500) -> tuple[int, int]:
    """Expires failed reservations and deletes bounded terminal projections."""
    publications = schema.publications
    operations = schema.operations
    with orm.Session(engine()) as session, session.begin():
        current = database_epoch(session, now=now)
        expiring = session.execute(
            sqlalchemy.select(publications.c.id).where(
                publications.c.state ==
                models.ImagePublicationState.FAILED.value,
                publications.c.reservation_active.is_(True),
                publications.c.reservation_expires_at <= current).order_by(
                    publications.c.reservation_expires_at,
                    publications.c.id).limit(batch_size).with_for_update(
                        skip_locked=True)).scalars().all()
        if expiring:
            session.execute(publications.update().where(
                publications.c.id.in_(expiring)).values(
                    reservation_active=False, updated_at=current))
        deletable_publications = session.execute(
            sqlalchemy.select(publications.c.id).where(
                publications.c.reservation_active.is_(False),
                publications.c.record_expires_at <= current).order_by(
                    publications.c.record_expires_at,
                    publications.c.id).limit(batch_size).with_for_update(
                        skip_locked=True)).scalars().all()
        deleted_publications = 0
        if deletable_publications:
            deleted_publications = session.execute(publications.delete().where(
                publications.c.id.in_(deletable_publications))).rowcount
        deletable_operations = session.execute(
            sqlalchemy.select(operations.c.id).where(
                operations.c.terminal_expires_at <= current).order_by(
                    operations.c.terminal_expires_at,
                    operations.c.id).limit(batch_size).with_for_update(
                        skip_locked=True)).scalars().all()
        deleted_operations = 0
        if deletable_operations:
            deleted_operations = session.execute(operations.delete().where(
                operations.c.id.in_(deletable_operations))).rowcount
    return deleted_publications, deleted_operations
