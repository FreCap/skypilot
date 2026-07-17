"""Durable state for immutable images and registry materializations."""

import dataclasses
import itertools
import json
import re
import threading
import time
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky import global_user_state
from sky.container_images import models
from sky.utils.db import db_utils
from sky.utils.db import retries as db_retries

_FINGERPRINT_PATTERN = re.compile(r'^[0-9a-f]{64}$')
_CATALOG_ROW_ID = 'authority'
_RECONCILIATION_QUEUE_SEQUENCE = itertools.count()
_MAX_CLAIM_CANDIDATE_SEEKS = 64
# A claimant runs several indexed queue probes per profile.  Keep this page
# deliberately small so an empty, high-cardinality catalog has a tight
# constant upper bound while the cursor still advances between calls.
_MAX_PROFILE_PROBES_PER_CALL = 16
_MAX_AUTOMATIC_LOCATION_ATTEMPTS = (
    global_user_state.CONTAINER_IMAGE_MAX_AUTOMATIC_ATTEMPTS)
_PROFILE_CURSOR_LOCK = threading.Lock()
# Each cursor stores the last keyset boundary and a page-local rotation.  The
# rotation keeps small catalogs fair when every call wraps to the same page.
_PROFILE_CURSORS: dict[tuple[str, str | None], tuple[str, str, int]] = {}
_EVICTION_CANDIDATE_CURSORS: dict[tuple[str, str, str, int, int],
                                  tuple[int, str]] = {}


class _ArtifactEvidenceConflict(RuntimeError):
    """Signals that a canonical copy disagrees with immutable OCI evidence."""


def _bounded_profile_page(
    session: orm.Session,
    *,
    workspace: str | None,
    cursor_name: str,
) -> list[sqlalchemy.engine.RowMapping]:
    """Returns one keyset page without materializing all durable profiles."""
    table = global_user_state.container_image_profile_revision_table
    cursor_key = (cursor_name, workspace)
    with _PROFILE_CURSOR_LOCK:
        cursor_state = _PROFILE_CURSORS.get(cursor_key)
    cursor = ((cursor_state[0],
               cursor_state[1]) if cursor_state is not None else None)
    rotation = cursor_state[2] if cursor_state is not None else 0

    def _statement(after: tuple[str, str] | None) -> Any:
        statement = sqlalchemy.select(table.c.workspace, table.c.profile,
                                      table.c.revision)
        if workspace is not None:
            statement = statement.where(table.c.workspace == workspace)
        if after is not None:
            if workspace is None:
                statement = statement.where(
                    sqlalchemy.or_(
                        table.c.workspace > after[0],
                        sqlalchemy.and_(table.c.workspace == after[0],
                                        table.c.profile > after[1])))
            else:
                statement = statement.where(table.c.profile > after[1])
        return statement.order_by(
            table.c.workspace,
            table.c.profile).limit(_MAX_PROFILE_PROBES_PER_CALL)

    rows = list(session.execute(_statement(cursor)).mappings().all())
    if not rows and cursor is not None:
        # Wrap without OFFSET so both the end probe and the next page remain
        # bounded by the profile primary key.
        rows = list(session.execute(_statement(None)).mappings().all())
    if rows:
        page_tail = rows[-1]
        with _PROFILE_CURSOR_LOCK:
            _PROFILE_CURSORS[cursor_key] = (str(page_tail['workspace']),
                                            str(page_tail['profile']),
                                            rotation + 1)
        offset = rotation % len(rows)
        rows = rows[offset:] + rows[:offset]
    return rows


@dataclasses.dataclass(frozen=True)
class ImageRecord:
    """Workspace-scoped immutable OCI artifact."""

    id: str
    workspace: str
    creator_user_hash: str
    source_ref: str | None
    resolved_source_ref: str | None
    source_digest: str
    producer_kind: str
    producer_spec_hash: str | None
    builder_version: str | None
    platforms: tuple[str, ...]
    compressed_size_bytes: int | None
    created_at: int
    updated_at: int

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result['platforms'] = list(self.platforms)
        return result


@dataclasses.dataclass(frozen=True)
class ReleaseRecord:
    """Immutable human-readable binding to an artifact."""

    workspace: str
    name: str
    image_id: str
    created_at: int


@dataclasses.dataclass(frozen=True)
class SourceRecord:
    """One immutable, digest-pinned import source for an artifact."""

    id: str
    workspace: str
    image_id: str
    source_ref: str
    resolved_source_ref: str
    created_at: int


@dataclasses.dataclass(frozen=True)
class LocationRecord:
    """One artifact materialization at one physical registry namespace."""

    id: str
    image_id: str
    profile: str
    target_id: str
    target_fingerprint: str
    policy_fingerprint: str
    profile_revision: int
    canonical: bool
    canonical_location_id: str | None
    canonical_ready: bool
    source_id: str | None
    target_ref: str | None
    expected_digest: str
    state: models.ImageLocationState
    attempt_count: int
    lease_owner: str | None
    lease_expires_at: int | None
    heartbeat_at: int | None
    next_retry_at: int | None
    last_verified_at: int | None
    verification_requested_at: int | None
    last_used_at: int | None
    auto_evict: bool
    last_error: str | None
    updated_at: int

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result['state'] = self.state.value
        return result


@dataclasses.dataclass(frozen=True)
class ReferenceRecord:
    """Durable consumer reference that fences materialization eviction."""

    id: str
    workspace: str
    location_id: str
    consumer_type: str
    consumer_id: str
    expires_at: int | None
    created_at: int
    updated_at: int


@dataclasses.dataclass(frozen=True)
class ProfileRevisionRecord:
    """Authoritative monotonic distribution revision for one workspace."""

    workspace: str
    profile: str
    revision: int
    revision_fingerprint: str
    created_at: int
    updated_at: int


@dataclasses.dataclass(frozen=True)
class ImagePublication:
    """One source-backed artifact publication inside an atomic batch."""

    source_ref: str
    resolved_source_ref: str
    source_digest: str
    workspace: str
    creator_user_hash: str
    release: str | None
    profile: str
    target_id: str
    target_fingerprint: str
    policy_fingerprint: str
    profile_revision: int
    profile_revision_fingerprint: str
    producer_kind: str = 'external_oci'
    producer_spec_hash: str | None = None
    builder_version: str | None = None
    max_artifacts: int = 1_000_000
    max_sources_per_artifact: int = 128
    max_releases_per_artifact: int = 128


@dataclasses.dataclass(frozen=True)
class ImageLocationIntent:
    """One target intent committed with a public prepare operation."""

    target_id: str
    target_fingerprint: str
    policy_fingerprint: str
    canonical: bool
    auto_evict: bool = False


def _engine() -> sqlalchemy.engine.Engine:
    engine = global_user_state.initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'Managed container image state requires the central PostgreSQL '
            'database.')
    return engine


def _validate_catalog_quota(value: int, subject: str, maximum: int) -> int:
    if (not isinstance(value, int) or isinstance(value, bool) or value <= 0 or
            value > maximum):
        raise ValueError(
            f'{subject} must be a positive integer no greater than {maximum}.')
    return value


def _lock_workspace_catalog(
    session: orm.Session,
    workspace: str,
    now: int,
) -> sqlalchemy.engine.RowMapping:
    """Locks the per-workspace quota row and initializes legacy counts."""
    workspace_table = (
        global_user_state.container_image_workspace_catalog_table)
    workspace_insert = session.execute(
        postgresql.insert(workspace_table).values(
            workspace=workspace, artifact_count=0,
            updated_at=now).on_conflict_do_nothing())
    if workspace_insert.rowcount == 1:
        image_table = global_user_state.container_image_table
        existing_count = session.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(image_table).where(
                image_table.c.workspace == workspace)).scalar_one()
        session.execute(workspace_table.update().where(
            workspace_table.c.workspace == workspace).values(
                artifact_count=existing_count, updated_at=now))
    return session.execute(workspace_table.select().where(
        workspace_table.c.workspace ==
        workspace).with_for_update()).mappings().one()


def _insert_do_nothing(table: sqlalchemy.Table, values: dict[str, Any]) -> bool:
    """Inserts without turning expected first-use races into exceptions."""
    engine = _engine()
    statement = postgresql.insert(table).values(
        **values).on_conflict_do_nothing()
    with orm.Session(engine) as session:
        result = session.execute(statement)
        session.commit()
    return result.rowcount == 1


def get_catalog_authority_id(*, create: bool = True) -> str | None:
    """Returns the durable UUID identifying this exact catalog database."""
    table = global_user_state.container_image_catalog_table
    statement = table.select().where(table.c.id == _CATALOG_ROW_ID)
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().first()
    if row is not None:
        return str(row['authority_id'])
    if not create:
        return None
    authority_id = str(uuid.uuid4())
    _insert_do_nothing(
        table, {
            'id': _CATALOG_ROW_ID,
            'authority_id': authority_id,
            'created_at': int(time.time()),
        })
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().one()
    return str(row['authority_id'])


def catalog_authority_matches(expected: str) -> bool:
    """Checks that this process opened the exact expected catalog database."""
    actual = get_catalog_authority_id(create=False)
    return actual is not None and actual == expected


def _error_code(error: models.ImageLocationErrorCode) -> str:
    """Returns a closed diagnostic code; arbitrary provider text is rejected."""
    if not isinstance(error, models.ImageLocationErrorCode):
        raise TypeError('Image location failures require a closed error code.')
    return error.value


def _new_lease_token(owner: str) -> str:
    owner = owner.strip()
    if not owner:
        raise ValueError('lease owner must be non-empty.')
    return f'{owner}:{uuid.uuid4()}'


def _validate_release(release: str) -> str:
    return models.validate_release_label(release, 'release')


def _image_from_row(row: sqlalchemy.engine.RowMapping) -> ImageRecord:
    try:
        workspace = models.validate_workspace_name(
            row['workspace'], 'Stored container image workspace')
        producer_kind = models.validate_image_producer_kind(
            row['producer_kind'], 'Stored container image producer kind')
        producer_spec_hash = models.validate_producer_spec_hash(
            row['producer_spec_hash'],
            'Stored container image producer specification hash')
        builder_version = models.validate_builder_version(
            row['builder_version'], 'Stored container image builder version')
        platforms = models.validate_oci_platforms(
            json.loads(row['platforms_json'] or '[]'),
            'Stored container image platforms')
        compressed_size_bytes = models.validate_compressed_size_bytes(
            row['compressed_size_bytes'], 'Stored container image size')
    except (TypeError, ValueError):
        raise ValueError(
            'Stored container image metadata failed validation.') from None
    return ImageRecord(
        id=row['id'],
        workspace=workspace,
        creator_user_hash=row['creator_user_hash'],
        source_ref=row['source_ref'],
        resolved_source_ref=row['resolved_source_ref'],
        source_digest=row['source_digest'],
        producer_kind=producer_kind,
        producer_spec_hash=producer_spec_hash,
        builder_version=builder_version,
        platforms=platforms,
        compressed_size_bytes=compressed_size_bytes,
        created_at=row['created_at'],
        updated_at=row['updated_at'],
    )


def _release_from_row(row: sqlalchemy.engine.RowMapping) -> ReleaseRecord:
    return ReleaseRecord(workspace=row['workspace'],
                         name=row['name'],
                         image_id=row['image_id'],
                         created_at=row['created_at'])


def _source_from_row(row: sqlalchemy.engine.RowMapping) -> SourceRecord:
    return SourceRecord(id=row['id'],
                        workspace=row['workspace'],
                        image_id=row['image_id'],
                        source_ref=row['source_ref'],
                        resolved_source_ref=row['resolved_source_ref'],
                        created_at=row['created_at'])


def _location_from_row(row: sqlalchemy.engine.RowMapping) -> LocationRecord:
    try:
        location_id = models.validate_catalog_id(
            row['id'], 'Stored container image location ID')
        image_id = models.validate_catalog_id(
            row['image_id'], 'Stored container image artifact ID')
        profile = models.validate_control_plane_identifier(
            row['profile'], 'Stored container image distribution')
        target_id = models.validate_control_plane_identifier(
            row['target_id'], 'Stored container image target')
        target_fingerprint = models.validate_fingerprint(
            row['target_fingerprint'],
            'Stored container image target fingerprint')
        policy_fingerprint = models.validate_fingerprint(
            row['policy_fingerprint'],
            'Stored container image policy fingerprint')
        canonical_location_id = row['canonical_location_id']
        if canonical_location_id is not None:
            canonical_location_id = models.validate_catalog_id(
                canonical_location_id,
                'Stored canonical container image location ID')
        source_id = row['source_id']
        if source_id is not None:
            source_id = models.validate_catalog_id(
                source_id, 'Stored container image source ID')
        target_ref = row['target_ref']
        if target_ref is not None:
            target_ref = models.validate_oci_reference(
                target_ref, 'Stored container image target reference')
        expected_digest = models.validate_sha256_digest(
            row['expected_digest'], 'Stored container image expected digest')
        location_state = models.ImageLocationState(row['state'])
        last_error = row['last_error']
        if last_error is not None:
            last_error = models.ImageLocationErrorCode(last_error).value
    except (TypeError, ValueError):
        raise ValueError('Stored container image location metadata failed '
                         'validation.') from None
    return LocationRecord(
        id=location_id,
        image_id=image_id,
        profile=profile,
        target_id=target_id,
        target_fingerprint=target_fingerprint,
        policy_fingerprint=policy_fingerprint,
        profile_revision=row['profile_revision'],
        canonical=bool(row['canonical']),
        canonical_location_id=canonical_location_id,
        canonical_ready=bool(row['canonical_ready']),
        source_id=source_id,
        target_ref=target_ref,
        expected_digest=expected_digest,
        state=location_state,
        attempt_count=row['attempt_count'],
        lease_owner=row['lease_owner'],
        lease_expires_at=row['lease_expires_at'],
        heartbeat_at=row['heartbeat_at'],
        next_retry_at=row['next_retry_at'],
        last_verified_at=row['last_verified_at'],
        verification_requested_at=row['verification_requested_at'],
        last_used_at=row['last_used_at'],
        auto_evict=bool(row['auto_evict']),
        last_error=last_error,
        updated_at=row['updated_at'],
    )


def _reference_from_row(row: sqlalchemy.engine.RowMapping) -> ReferenceRecord:
    return ReferenceRecord(id=row['id'],
                           workspace=row['workspace'],
                           location_id=row['location_id'],
                           consumer_type=row['consumer_type'],
                           consumer_id=row['consumer_id'],
                           expires_at=row['expires_at'],
                           created_at=row['created_at'],
                           updated_at=row['updated_at'])


def _profile_revision_from_row(
        row: sqlalchemy.engine.RowMapping) -> ProfileRevisionRecord:
    return ProfileRevisionRecord(
        workspace=row['workspace'],
        profile=row['profile'],
        revision=row['revision'],
        revision_fingerprint=row['revision_fingerprint'],
        created_at=row['created_at'],
        updated_at=row['updated_at'],
    )


def _validate_image_publication(
        publication: ImagePublication) -> ImagePublication:
    """Validates and normalizes a publication before any transaction writes."""
    source_digest = publication.source_digest.lower()
    workspace = models.validate_workspace_name(publication.workspace,
                                               'Container image workspace')
    profile = models.validate_control_plane_identifier(
        publication.profile, 'Container image distribution')
    target_id = models.validate_control_plane_identifier(
        publication.target_id, 'Container image target')
    source_ref = models.validate_oci_reference(publication.source_ref,
                                               'source_ref')
    resolved_source_ref = models.validate_oci_reference(
        publication.resolved_source_ref, 'resolved_source_ref')
    _, resolved_digest = models.split_digest(resolved_source_ref)
    if resolved_digest != source_digest:
        raise ValueError('resolved_source_ref must be pinned to source_digest.')
    producer_kind = models.validate_image_producer_kind(
        publication.producer_kind, 'Container image producer kind')
    producer_spec_hash = models.validate_producer_spec_hash(
        publication.producer_spec_hash,
        'Container image producer specification hash')
    builder_version = models.validate_builder_version(
        publication.builder_version, 'Container image builder version')
    release = publication.release
    if release is not None:
        release = _validate_release(release)
    _validate_location_arguments(publication.target_fingerprint,
                                 publication.policy_fingerprint,
                                 canonical=True,
                                 canonical_location_id=None,
                                 auto_evict=False)
    for value, subject, maximum in (
        (publication.max_artifacts, 'max_artifacts', 100_000_000),
        (publication.max_sources_per_artifact, 'max_sources_per_artifact',
         4096),
        (publication.max_releases_per_artifact, 'max_releases_per_artifact',
         4096),
    ):
        _validate_catalog_quota(value, subject, maximum)
    return dataclasses.replace(publication,
                               source_ref=source_ref,
                               resolved_source_ref=resolved_source_ref,
                               source_digest=source_digest,
                               workspace=workspace,
                               release=release,
                               profile=profile,
                               target_id=target_id,
                               producer_kind=producer_kind,
                               producer_spec_hash=producer_spec_hash,
                               builder_version=builder_version)


_IndexedPublication = tuple[int, ImagePublication]


def _artifact_publication_sort_key(
        item: _IndexedPublication) -> tuple[str, str, str, str, str, int]:
    index, publication = item
    return (publication.workspace, publication.source_digest,
            publication.source_ref, publication.profile, publication.target_id,
            index)


def _source_publication_sort_key(
        item: _IndexedPublication) -> tuple[str, str, str, str, str, str, int]:
    index, publication = item
    return (publication.workspace, publication.source_ref,
            publication.source_digest, publication.resolved_source_ref,
            publication.profile, publication.target_id, index)


def _release_publication_sort_key(
        item: _IndexedPublication) -> tuple[str, str, str, str, str, str, int]:
    index, publication = item
    assert publication.release is not None
    return (publication.workspace, publication.release,
            publication.source_digest, publication.source_ref,
            publication.profile, publication.target_id, index)


def _location_publication_sort_key(
        item: _IndexedPublication) -> tuple[str, str, str, str, str, str, int]:
    index, publication = item
    return (publication.workspace, publication.source_digest,
            publication.target_fingerprint, publication.profile,
            publication.target_id, publication.source_ref, index)


def _ensure_publication_artifact_in_session(
    session: orm.Session,
    publication: ImagePublication,
    now: int,
) -> ImageRecord:
    """Ensures and locks one publication artifact without committing."""
    image_table = global_user_state.container_image_table
    workspace_table = (
        global_user_state.container_image_workspace_catalog_table)
    workspace_row = _lock_workspace_catalog(session, publication.workspace, now)
    image_statement = image_table.select().where(
        image_table.c.workspace == publication.workspace,
        image_table.c.source_digest == publication.source_digest)
    image_statement = image_statement.with_for_update()
    image_row = session.execute(image_statement).mappings().first()
    if image_row is None:
        if int(workspace_row['artifact_count']) >= publication.max_artifacts:
            raise ValueError(
                f'Workspace {publication.workspace!r} reached its managed '
                f'container image artifact quota of '
                f'{publication.max_artifacts}.')
        session.execute(
            postgresql.insert(image_table).values(
                id=str(uuid.uuid4()),
                workspace=publication.workspace,
                creator_user_hash=publication.creator_user_hash,
                source_ref=publication.source_ref,
                resolved_source_ref=publication.resolved_source_ref,
                source_digest=publication.source_digest,
                producer_kind=publication.producer_kind,
                producer_spec_hash=publication.producer_spec_hash,
                builder_version=publication.builder_version,
                platforms_json='[]',
                created_at=now,
                updated_at=now,
            ))
        session.execute(workspace_table.update().where(
            workspace_table.c.workspace == publication.workspace).values(
                artifact_count=workspace_table.c.artifact_count + 1,
                updated_at=now))
        image_row = session.execute(image_statement).mappings().one()
    return _image_from_row(image_row)


def _ensure_publication_source_in_session(
    session: orm.Session,
    publication: ImagePublication,
    image: ImageRecord,
    now: int,
) -> str:
    """Ensures and locks one immutable source alias without committing."""
    source_table = global_user_state.container_image_source_table

    source_statement = source_table.select().where(
        source_table.c.workspace == publication.workspace,
        source_table.c.source_ref == publication.source_ref)
    source_statement = source_statement.with_for_update()
    source_row = session.execute(source_statement).mappings().first()
    if source_row is None:
        source_count = session.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(source_table).where(
                source_table.c.image_id == image.id)).scalar_one()
        if source_count >= publication.max_sources_per_artifact:
            raise ValueError(
                f'Container image artifact {image.id!r} reached its source '
                f'alias quota of {publication.max_sources_per_artifact}.')
        session.execute(
            postgresql.insert(source_table).values(
                id=str(uuid.uuid4()),
                workspace=publication.workspace,
                image_id=image.id,
                source_ref=publication.source_ref,
                resolved_source_ref=publication.resolved_source_ref,
                created_at=now,
            ).on_conflict_do_nothing())
        source_row = session.execute(source_statement).mappings().one()
    if (source_row['image_id'] != image.id or source_row['resolved_source_ref']
            != publication.resolved_source_ref):
        raise ValueError(
            f'Container image source {publication.source_ref!r} is already '
            'bound to different immutable content.')
    return str(source_row['id'])


def _ensure_publication_release_in_session(
    session: orm.Session,
    publication: ImagePublication,
    image: ImageRecord,
    now: int,
) -> None:
    """Ensures and locks one immutable release alias without committing."""
    if publication.release is None:
        return
    image_table = global_user_state.container_image_table
    release_table = global_user_state.container_image_release_table
    release_statement = release_table.select().where(
        release_table.c.workspace == publication.workspace,
        release_table.c.name == publication.release)
    release_statement = release_statement.with_for_update()
    release_row = session.execute(release_statement).mappings().first()
    if release_row is None:
        release_count = session.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(release_table).where(
                release_table.c.image_id == image.id)).scalar_one()
        if release_count >= publication.max_releases_per_artifact:
            raise ValueError(
                f'Container image artifact {image.id!r} reached its release '
                f'alias quota of {publication.max_releases_per_artifact}.')
        session.execute(
            postgresql.insert(release_table).values(
                workspace=publication.workspace,
                name=publication.release,
                image_id=image.id,
                created_at=now,
            ).on_conflict_do_nothing())
        release_row = session.execute(release_statement).mappings().one()
    if release_row['image_id'] != image.id:
        bound_digest = session.execute(
            sqlalchemy.select(image_table.c.source_digest).where(
                image_table.c.id == release_row['image_id'])).scalar()
        raise ValueError(
            f'Container image release {publication.release!r} is already '
            f'bound to digest {bound_digest!r}. Release names are '
            'immutable within a workspace.')


def _ensure_publication_location_in_session(
    session: orm.Session,
    publication: ImagePublication,
    image: ImageRecord,
    source_id: str,
    now: int,
) -> None:
    """Ensures one canonical intent after every alias lock is held."""
    _ensure_location_in_session(
        session,
        image.id,
        publication.profile,
        publication.target_id,
        publication.target_fingerprint,
        publication.source_digest,
        policy_fingerprint=publication.policy_fingerprint,
        profile_revision=publication.profile_revision,
        profile_revision_fingerprint=(publication.profile_revision_fingerprint),
        canonical=True,
        canonical_location_id=None,
        source_id=source_id,
        auto_evict=False,
        now=now)


@db_retries.retry
def publish_images_atomically(
        publications: list[ImagePublication]) -> list[ImageRecord]:
    """Publishes a source set in one all-or-nothing catalog transaction."""
    if not publications:
        return []
    publications = [
        _validate_image_publication(publication) for publication in publications
    ]
    revisions: dict[tuple[str, str], tuple[int, str]] = {}
    workspace_quotas: dict[str, tuple[int, int, int]] = {}
    for publication in publications:
        key = (publication.workspace, publication.profile)
        revision_config = (publication.profile_revision,
                           publication.profile_revision_fingerprint)
        previous = revisions.setdefault(key, revision_config)
        if previous != revision_config:
            raise ValueError('One atomic image publication cannot use '
                             'multiple revisions of the same distribution.')
        quota = (publication.max_artifacts,
                 publication.max_sources_per_artifact,
                 publication.max_releases_per_artifact)
        previous_quota = workspace_quotas.setdefault(publication.workspace,
                                                     quota)
        if previous_quota != quota:
            raise ValueError('One atomic image publication must use one '
                             'catalog quota snapshot per workspace.')

    now = int(time.time())
    engine = _engine()
    records: dict[int, ImageRecord] = {}
    source_ids: dict[int, str] = {}
    with orm.Session(engine) as session:
        # Every batch completes globally ordered phases: profiles, artifacts,
        # sources, releases, then locations. No transaction can hold a later
        # kind while waiting on an earlier one, and every key within a phase is
        # stable across callers. A conflict still rolls the whole batch back.
        for workspace, profile in sorted(revisions):
            revision_number, revision_fingerprint = revisions[(workspace,
                                                               profile)]
            _lock_or_activate_profile_revision(session, workspace, profile,
                                               revision_number,
                                               revision_fingerprint, now)

        indexed_publications: list[_IndexedPublication] = list(
            enumerate(publications))
        artifacts: dict[tuple[str, str], ImageRecord] = {}
        for index, publication in sorted(indexed_publications,
                                         key=_artifact_publication_sort_key):
            artifact_key = (publication.workspace, publication.source_digest)
            image = artifacts.get(artifact_key)
            if image is None:
                image = _ensure_publication_artifact_in_session(
                    session, publication, now)
                artifacts[artifact_key] = image
            records[index] = image

        for index, publication in sorted(indexed_publications,
                                         key=_source_publication_sort_key):
            source_ids[index] = _ensure_publication_source_in_session(
                session, publication, records[index], now)

        release_publications = [
            item for item in indexed_publications if item[1].release is not None
        ]
        for index, publication in sorted(release_publications,
                                         key=_release_publication_sort_key):
            _ensure_publication_release_in_session(session, publication,
                                                   records[index], now)

        for index, publication in sorted(indexed_publications,
                                         key=_location_publication_sort_key):
            _ensure_publication_location_in_session(session, publication,
                                                    records[index],
                                                    source_ids[index], now)
        session.commit()
    return [records[index] for index in range(len(publications))]


def publish_image(
    source_ref: str,
    resolved_source_ref: str,
    source_digest: str,
    workspace: str,
    creator_user_hash: str,
    *,
    release: str | None,
    profile: str,
    target_id: str,
    target_fingerprint: str,
    policy_fingerprint: str,
    profile_revision: int,
    profile_revision_fingerprint: str,
    producer_kind: str = 'external_oci',
    producer_spec_hash: str | None = None,
    builder_version: str | None = None,
    max_artifacts: int = 1_000_000,
    max_sources_per_artifact: int = 128,
    max_releases_per_artifact: int = 128,
) -> ImageRecord:
    """Atomically publishes identity, aliases, and canonical intent.

    A rejected publication leaves no losing artifact, source, release, profile
    generation, or location row. Identical concurrent publications converge on
    the same records through database uniqueness constraints.
    """
    return publish_images_atomically([
        ImagePublication(
            source_ref=source_ref,
            resolved_source_ref=resolved_source_ref,
            source_digest=source_digest,
            workspace=workspace,
            creator_user_hash=creator_user_hash,
            release=release,
            profile=profile,
            target_id=target_id,
            target_fingerprint=target_fingerprint,
            policy_fingerprint=policy_fingerprint,
            profile_revision=profile_revision,
            profile_revision_fingerprint=profile_revision_fingerprint,
            producer_kind=producer_kind,
            producer_spec_hash=producer_spec_hash,
            builder_version=builder_version,
            max_artifacts=max_artifacts,
            max_sources_per_artifact=max_sources_per_artifact,
            max_releases_per_artifact=max_releases_per_artifact,
        )
    ])[0]


@db_retries.retry
def prepare_image_atomically(
    *,
    existing_image_id: str | None,
    publication: ImagePublication | None,
    workspace: str,
    profile: str,
    profile_revision: int,
    profile_revision_fingerprint: str,
    expected_digest: str,
    intents: list[ImageLocationIntent],
) -> ImageRecord:
    """Publishes/binds one artifact and all requested targets atomically."""
    if (existing_image_id is None) == (publication is None):
        raise ValueError('Image preparation requires exactly one existing '
                         'artifact or source publication.')
    workspace = models.validate_workspace_name(workspace,
                                               'Container image workspace')
    profile = models.validate_control_plane_identifier(
        profile, 'Container image distribution')
    expected_digest = models.validate_sha256_digest(expected_digest,
                                                    'Preparation digest')
    models.validate_fingerprint(profile_revision_fingerprint,
                                'Profile revision fingerprint')
    if (not isinstance(profile_revision, int) or
            isinstance(profile_revision, bool) or profile_revision <= 0):
        raise ValueError('Profile revision must be a positive integer.')
    if not intents:
        raise ValueError('Image preparation requires target intents.')
    validated_intents: list[ImageLocationIntent] = []
    for intent in intents:
        target_id = models.validate_control_plane_identifier(
            intent.target_id, 'Container image target')
        _validate_location_arguments(
            intent.target_fingerprint,
            intent.policy_fingerprint,
            canonical=intent.canonical,
            canonical_location_id=None if intent.canonical else 'deferred',
            auto_evict=intent.auto_evict)
        validated_intents.append(
            dataclasses.replace(intent, target_id=target_id))
    canonical_intents = [item for item in validated_intents if item.canonical]
    if len(canonical_intents) != 1:
        raise ValueError('Image preparation requires exactly one canonical '
                         'target intent.')
    fingerprints = [item.target_fingerprint for item in validated_intents]
    if len(fingerprints) != len(set(fingerprints)):
        raise ValueError('Image preparation targets must name distinct '
                         'physical destinations.')

    normalized_publication = (_validate_image_publication(publication)
                              if publication is not None else None)
    if normalized_publication is not None:
        if (normalized_publication.workspace != workspace or
                normalized_publication.profile != profile or
                normalized_publication.profile_revision != profile_revision or
                normalized_publication.profile_revision_fingerprint
                != profile_revision_fingerprint or
                normalized_publication.source_digest != expected_digest):
            raise ValueError('Image publication and preparation snapshots do '
                             'not match.')
    if existing_image_id is not None:
        existing_image_id = models.validate_catalog_id(existing_image_id,
                                                       'Image artifact ID')

    now = int(time.time())
    engine = _engine()
    with orm.Session(engine) as session:
        _lock_or_activate_profile_revision(session, workspace, profile,
                                           profile_revision,
                                           profile_revision_fingerprint, now)
        source_id = None
        if normalized_publication is not None:
            record = _ensure_publication_artifact_in_session(
                session, normalized_publication, now)
            source_id = _ensure_publication_source_in_session(
                session, normalized_publication, record, now)
            _ensure_publication_release_in_session(session,
                                                   normalized_publication,
                                                   record, now)
        else:
            assert existing_image_id is not None
            image_table = global_user_state.container_image_table
            statement = image_table.select().where(
                image_table.c.id == existing_image_id,
                image_table.c.workspace == workspace)
            statement = statement.with_for_update()
            row = session.execute(statement).mappings().first()
            if row is None:
                raise ValueError('Container image artifact was not found in '
                                 'the requested workspace.')
            record = _image_from_row(row)
        if record.source_digest != expected_digest:
            raise ValueError('Prepared artifact digest changed before commit.')

        canonical_intent = canonical_intents[0]
        canonical_location = _ensure_location_in_session(
            session,
            record.id,
            profile,
            canonical_intent.target_id,
            canonical_intent.target_fingerprint,
            expected_digest,
            policy_fingerprint=canonical_intent.policy_fingerprint,
            profile_revision=profile_revision,
            profile_revision_fingerprint=profile_revision_fingerprint,
            canonical=True,
            canonical_location_id=None,
            source_id=source_id,
            auto_evict=False,
            now=now)
        for intent in sorted(
            (item for item in validated_intents if not item.canonical),
                key=lambda item: (item.target_fingerprint, item.target_id)):
            _ensure_location_in_session(
                session,
                record.id,
                profile,
                intent.target_id,
                intent.target_fingerprint,
                expected_digest,
                policy_fingerprint=intent.policy_fingerprint,
                profile_revision=profile_revision,
                profile_revision_fingerprint=profile_revision_fingerprint,
                canonical=False,
                canonical_location_id=canonical_location.id,
                source_id=None,
                auto_evict=intent.auto_evict,
                now=now)
        session.commit()
    return record


def register_image(
    source_ref: str | None,
    resolved_source_ref: str | None,
    source_digest: str,
    workspace: str,
    creator_user_hash: str,
    *,
    release: str | None = None,
    producer_kind: str = 'external_oci',
    producer_spec_hash: str | None = None,
    builder_version: str | None = None,
    max_artifacts: int = 1_000_000,
    max_sources_per_artifact: int = 128,
    max_releases_per_artifact: int = 128,
) -> ImageRecord:
    """Idempotently registers content identity, independent of distribution."""
    source_digest = source_digest.lower()
    workspace = models.validate_workspace_name(workspace,
                                               'Container image workspace')
    if source_ref is not None:
        source_ref = models.validate_oci_reference(source_ref, 'source_ref')
    if resolved_source_ref is not None:
        resolved_source_ref = models.validate_oci_reference(
            resolved_source_ref, 'resolved_source_ref')
        _, resolved_digest = models.split_digest(resolved_source_ref)
        if resolved_digest != source_digest:
            raise ValueError('resolved_source_ref must be pinned to '
                             'source_digest.')
    producer_kind = models.validate_image_producer_kind(
        producer_kind, 'Container image producer kind')
    producer_spec_hash = models.validate_producer_spec_hash(
        producer_spec_hash, 'Container image producer specification hash')
    builder_version = models.validate_builder_version(
        builder_version, 'Container image builder version')
    if release is not None:
        release = _validate_release(release)
    _validate_catalog_quota(max_artifacts, 'max_artifacts', 100_000_000)
    _validate_catalog_quota(max_sources_per_artifact,
                            'max_sources_per_artifact', 4096)
    _validate_catalog_quota(max_releases_per_artifact,
                            'max_releases_per_artifact', 4096)
    now = int(time.time())
    values = {
        'id': str(uuid.uuid4()),
        'workspace': workspace,
        'creator_user_hash': creator_user_hash,
        'source_ref': source_ref,
        'resolved_source_ref': resolved_source_ref,
        'source_digest': source_digest,
        'producer_kind': producer_kind,
        'producer_spec_hash': producer_spec_hash,
        'builder_version': builder_version,
        'platforms_json': '[]',
        'created_at': now,
        'updated_at': now,
    }
    engine = _engine()
    image_table = global_user_state.container_image_table
    workspace_table = (
        global_user_state.container_image_workspace_catalog_table)
    with orm.Session(engine) as session:
        workspace_row = _lock_workspace_catalog(session, workspace, now)
        statement = image_table.select().where(
            image_table.c.workspace == workspace,
            image_table.c.source_digest == source_digest)
        statement = statement.with_for_update()
        image_row = session.execute(statement).mappings().first()
        if image_row is None:
            if int(workspace_row['artifact_count']) >= max_artifacts:
                raise ValueError(
                    f'Workspace {workspace!r} reached its managed container '
                    f'image artifact quota of {max_artifacts}.')
            session.execute(image_table.insert().values(**values))
            session.execute(workspace_table.update().where(
                workspace_table.c.workspace == workspace).values(
                    artifact_count=workspace_table.c.artifact_count + 1,
                    updated_at=now))
        session.commit()
    record = get_image_by_digest(source_digest, workspace)
    if record is None:
        raise RuntimeError('Container image registration committed without a '
                           'readable artifact record.')
    if source_ref is not None and resolved_source_ref is not None:
        bind_source(record.id,
                    workspace,
                    source_ref,
                    resolved_source_ref,
                    max_sources_per_artifact=max_sources_per_artifact)
    if release is not None:
        bind_release(record.id,
                     workspace,
                     release,
                     max_releases_per_artifact=max_releases_per_artifact)
    return record


def bind_source(image_id: str,
                workspace: str,
                source_ref: str,
                resolved_source_ref: str,
                *,
                max_sources_per_artifact: int = 128) -> SourceRecord:
    """Binds an immutable source alias without changing artifact identity."""
    source_ref = models.validate_oci_reference(source_ref, 'source_ref')
    resolved_source_ref = models.validate_oci_reference(resolved_source_ref,
                                                        'resolved_source_ref')
    _validate_catalog_quota(max_sources_per_artifact,
                            'max_sources_per_artifact', 4096)
    _, digest = models.split_digest(resolved_source_ref)
    values = {
        'id': str(uuid.uuid4()),
        'workspace': workspace,
        'image_id': image_id,
        'source_ref': source_ref,
        'resolved_source_ref': resolved_source_ref,
        'created_at': int(time.time()),
    }
    table = global_user_state.container_image_source_table
    image_table = global_user_state.container_image_table
    engine = _engine()
    with orm.Session(engine) as session:
        image_statement = image_table.select().where(
            image_table.c.id == image_id, image_table.c.workspace == workspace)
        image_statement = image_statement.with_for_update()
        image_row = session.execute(image_statement).mappings().first()
        if image_row is None:
            raise ValueError(f'Container image {image_id!r} does not exist in '
                             f'workspace {workspace!r}.')
        if digest != image_row['source_digest']:
            raise ValueError(
                'Source alias must resolve to the artifact digest.')
        existing_statement = table.select().where(
            table.c.workspace == workspace, table.c.source_ref == source_ref)
        existing_statement = existing_statement.with_for_update()
        existing_row = session.execute(existing_statement).mappings().first()
        if existing_row is None:
            count = session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(table).where(
                    table.c.image_id == image_id)).scalar_one()
            if count >= max_sources_per_artifact:
                raise ValueError(
                    f'Container image artifact {image_id!r} reached its '
                    f'source alias quota of {max_sources_per_artifact}.')
            session.execute(
                postgresql.insert(table).values(
                    **values).on_conflict_do_nothing())
            existing_row = session.execute(existing_statement).mappings().one()
        if (existing_row['image_id'] != image_id or
                existing_row['resolved_source_ref'] != resolved_source_ref):
            raise ValueError(
                f'Container image source {source_ref!r} is already bound to '
                'different immutable content.')
        session.commit()
    result = get_source(source_ref, workspace)
    assert result is not None
    return result


def get_source(source_ref: str, workspace: str) -> SourceRecord | None:
    table = global_user_state.container_image_source_table
    statement = table.select().where(table.c.workspace == workspace,
                                     table.c.source_ref == source_ref)
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().first()
    return _source_from_row(row) if row is not None else None


def get_source_by_id(source_id: str) -> SourceRecord | None:
    """Returns one immutable source binding by its catalog UUID."""
    source_id = models.validate_catalog_id(source_id, 'source_id')
    table = global_user_state.container_image_source_table
    statement = table.select().where(table.c.id == source_id)
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().first()
    return _source_from_row(row) if row is not None else None


def list_sources(image_id: str,
                 workspace: str | None = None,
                 *,
                 limit: int | None = None) -> list[SourceRecord]:
    table = global_user_state.container_image_source_table
    statement = table.select().where(table.c.image_id == image_id)
    if workspace is not None:
        statement = statement.where(table.c.workspace == workspace)
    statement = statement.order_by(table.c.created_at, table.c.id)
    if limit is not None:
        statement = statement.limit(limit)
    with orm.Session(_engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_source_from_row(row) for row in rows]


def bind_release(image_id: str,
                 workspace: str,
                 release: str,
                 *,
                 max_releases_per_artifact: int = 128) -> ReleaseRecord:
    """Creates an immutable release alias; multiple aliases may share content."""
    release = _validate_release(release)
    _validate_catalog_quota(max_releases_per_artifact,
                            'max_releases_per_artifact', 4096)
    values = {
        'workspace': workspace,
        'name': release,
        'image_id': image_id,
        'created_at': int(time.time()),
    }
    table = global_user_state.container_image_release_table
    image_table = global_user_state.container_image_table
    engine = _engine()
    with orm.Session(engine) as session:
        image_statement = image_table.select().where(
            image_table.c.id == image_id, image_table.c.workspace == workspace)
        image_statement = image_statement.with_for_update()
        image_row = session.execute(image_statement).mappings().first()
        if image_row is None:
            raise ValueError(f'Container image {image_id!r} does not exist in '
                             f'workspace {workspace!r}.')
        release_statement = table.select().where(table.c.workspace == workspace,
                                                 table.c.name == release)
        release_statement = release_statement.with_for_update()
        release_row = session.execute(release_statement).mappings().first()
        if release_row is None:
            count = session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(table).where(
                    table.c.image_id == image_id)).scalar_one()
            if count >= max_releases_per_artifact:
                raise ValueError(
                    f'Container image artifact {image_id!r} reached its '
                    f'release alias quota of {max_releases_per_artifact}.')
            session.execute(
                postgresql.insert(table).values(
                    **values).on_conflict_do_nothing())
            release_row = session.execute(release_statement).mappings().one()
        if release_row['image_id'] != image_id:
            bound_digest = session.execute(
                sqlalchemy.select(image_table.c.source_digest).where(
                    image_table.c.id == release_row['image_id'])).scalar()
            raise ValueError(
                f'Container image release {release!r} is already bound to '
                f'digest {bound_digest!r}. Release names are immutable within '
                'a workspace.')
        session.commit()
    result = get_release(release, workspace)
    assert result is not None
    return result


def get_release(release: str, workspace: str) -> ReleaseRecord | None:
    table = global_user_state.container_image_release_table
    statement = table.select().where(table.c.workspace == workspace,
                                     table.c.name == release)
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().first()
    return _release_from_row(row) if row is not None else None


def list_releases(image_id: str,
                  workspace: str | None = None,
                  *,
                  limit: int | None = None) -> list[ReleaseRecord]:
    table = global_user_state.container_image_release_table
    statement = table.select().where(table.c.image_id == image_id)
    if workspace is not None:
        statement = statement.where(table.c.workspace == workspace)
    statement = statement.order_by(table.c.name)
    if limit is not None:
        statement = statement.limit(limit)
    with orm.Session(_engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_release_from_row(row) for row in rows]


def get_image(image_id: str,
              workspace: str | None = None) -> ImageRecord | None:
    table = global_user_state.container_image_table
    statement = table.select().where(table.c.id == image_id)
    if workspace is not None:
        statement = statement.where(table.c.workspace == workspace)
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().first()
    return _image_from_row(row) if row is not None else None


def get_image_by_digest(source_digest: str,
                        workspace: str) -> ImageRecord | None:
    table = global_user_state.container_image_table
    statement = table.select().where(
        table.c.workspace == workspace,
        table.c.source_digest == source_digest.lower())
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().first()
    return _image_from_row(row) if row is not None else None


def get_image_by_source_ref(source_ref: str,
                            workspace: str) -> ImageRecord | None:
    source_table = global_user_state.container_image_source_table
    image_table = global_user_state.container_image_table
    statement = (image_table.select().join(
        source_table, source_table.c.image_id == image_table.c.id).where(
            source_table.c.workspace == workspace,
            sqlalchemy.or_(source_table.c.source_ref == source_ref,
                           source_table.c.resolved_source_ref == source_ref),
        ).order_by(source_table.c.created_at.desc(),
                   source_table.c.id.desc()).limit(1))
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().first()
    return _image_from_row(row) if row is not None else None


def get_image_by_release(release: str, workspace: str) -> ImageRecord | None:
    binding = get_release(release, workspace)
    if binding is None:
        return None
    return get_image(binding.image_id, workspace)


def get_image_by_version(version: str, workspace: str) -> ImageRecord | None:
    """Compatibility alias for the pre-release API name."""
    return get_image_by_release(version, workspace)


def list_images(workspace: str, limit: int | None = None) -> list[ImageRecord]:
    table = global_user_state.container_image_table
    statement = (table.select().where(table.c.workspace == workspace).order_by(
        table.c.created_at.desc(), table.c.id.desc()))
    if limit is not None:
        if limit <= 0:
            raise ValueError('limit must be positive.')
        statement = statement.limit(limit)
    with orm.Session(_engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_image_from_row(row) for row in rows]


def list_image_associations(
    image_ids: list[str],
    workspace: str,
    *,
    max_rows_per_kind: int | None = None,
) -> tuple[dict[str, list[SourceRecord]], dict[str, list[ReleaseRecord]], dict[
        str, list[LocationRecord]]]:
    """Batch-loads status associations without per-artifact queries."""
    sources: dict[str, list[SourceRecord]] = {
        image_id: [] for image_id in image_ids
    }
    releases: dict[str, list[ReleaseRecord]] = {
        image_id: [] for image_id in image_ids
    }
    locations: dict[str, list[LocationRecord]] = {
        image_id: [] for image_id in image_ids
    }
    if not image_ids:
        return sources, releases, locations
    source_table = global_user_state.container_image_source_table
    release_table = global_user_state.container_image_release_table
    location_table = global_user_state.container_image_location_table
    with orm.Session(_engine()) as session:
        source_statement = source_table.select().where(
            source_table.c.workspace == workspace,
            source_table.c.image_id.in_(image_ids)).order_by(
                source_table.c.image_id, source_table.c.created_at,
                source_table.c.id)
        release_statement = release_table.select().where(
            release_table.c.workspace == workspace,
            release_table.c.image_id.in_(image_ids)).order_by(
                release_table.c.image_id, release_table.c.name)
        location_statement = location_table.select().where(
            location_table.c.image_id.in_(image_ids)).order_by(
                location_table.c.image_id, location_table.c.profile,
                location_table.c.target_id, location_table.c.updated_at.desc())
        if max_rows_per_kind is not None:
            fetch_limit = max_rows_per_kind + 1
            source_statement = source_statement.limit(fetch_limit)
            release_statement = release_statement.limit(fetch_limit)
            location_statement = location_statement.limit(fetch_limit)
        source_rows = session.execute(source_statement).mappings().all()
        release_rows = session.execute(release_statement).mappings().all()
        location_rows = session.execute(location_statement).mappings().all()
    if (max_rows_per_kind is not None and any(
            len(rows) > max_rows_per_kind
            for rows in (source_rows, release_rows, location_rows))):
        raise ValueError(
            'Container image status has too many associations to return '
            'without pagination. Filter by one artifact or reduce aliases.')
    for row in source_rows:
        source = _source_from_row(row)
        sources[source.image_id].append(source)
    for row in release_rows:
        release = _release_from_row(row)
        releases[release.image_id].append(release)
    for row in location_rows:
        location = _location_from_row(row)
        locations[location.image_id].append(location)
    return sources, releases, locations


class StaleProfileRevisionError(ValueError):
    """Raised when a stale process attempts to restore an older profile."""


class ProfileRevisionBusyError(ValueError):
    """Raised when a profile revision changes during active data-plane work."""


def get_profile_revision(workspace: str,
                         profile: str) -> ProfileRevisionRecord | None:
    table = global_user_state.container_image_profile_revision_table
    statement = table.select().where(table.c.workspace == workspace,
                                     table.c.profile == profile)
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().first()
    return _profile_revision_from_row(row) if row is not None else None


def profile_revision_matches(workspace: str, profile: str, revision: int,
                             revision_fingerprint: str) -> bool:
    current = get_profile_revision(workspace, profile)
    return (current is not None and current.revision == revision and
            current.revision_fingerprint == revision_fingerprint)


def _lock_profile_revision(
    session: orm.Session,
    workspace: str,
    profile: str,
    revision: int,
    revision_fingerprint: str,
    now: int,
) -> None:
    """Locks and monotonically activates one complete profile revision."""
    if (not isinstance(revision, int) or isinstance(revision, bool) or
            revision <= 0):
        raise ValueError('profile_revision must be a positive integer.')
    if not _FINGERPRINT_PATTERN.fullmatch(revision_fingerprint):
        raise ValueError('profile_revision_fingerprint must be a lowercase '
                         'SHA-256 hex digest.')
    engine = session.get_bind()
    assert engine is not None
    table = global_user_state.container_image_profile_revision_table
    session.execute(
        postgresql.insert(table).values(
            workspace=workspace,
            profile=profile,
            revision=revision,
            revision_fingerprint=revision_fingerprint,
            created_at=now,
            updated_at=now,
        ).on_conflict_do_nothing())
    row = session.execute(table.select().where(
        table.c.workspace == workspace,
        table.c.profile == profile).with_for_update()).mappings().one()
    current_revision = int(row['revision'])
    current_fingerprint = str(row['revision_fingerprint'])
    if revision < current_revision:
        raise StaleProfileRevisionError(
            f'Registry profile {profile!r} revision {revision} is stale; '
            f'the catalog has revision {current_revision}.')
    if revision == current_revision:
        if revision_fingerprint != current_fingerprint:
            raise ValueError(
                f'Registry profile {profile!r} changed without incrementing '
                f'its revision from {revision}.')
        return

    location = global_user_state.container_image_location_table
    structurally_complete_lease = sqlalchemy.and_(
        location.c.lease_owner.isnot(None),
        location.c.lease_owner != '',
        location.c.lease_expires_at.isnot(None),
        location.c.lease_expires_at > now,
        location.c.heartbeat_at.isnot(None),
    )
    # Activation is O(1) in the dominant PENDING/READY population.  Old rows
    # are generation-fenced by the profile authority and transferred lazily,
    # one locked physical location at a time, when the new policy touches it.
    # Keep each state in a separate LIMIT 1 probe so PostgreSQL can use the
    # matching state-specific partial index without evaluating an OR across
    # the full profile population.
    blocking_work = None
    for lease_state, verification in (
        (models.ImageLocationState.COPYING.value, False),
        (models.ImageLocationState.EVICTING.value, False),
        (models.ImageLocationState.READY.value, True),
    ):
        conditions = [
            location.c.workspace == workspace,
            location.c.profile == profile,
            location.c.state == lease_state,
            structurally_complete_lease,
        ]
        if verification:
            conditions.append(location.c.verification_requested_at.isnot(None))
        blocking_work = session.execute(
            sqlalchemy.select(
                location.c.id).where(*conditions).limit(1)).first()
        if blocking_work is not None:
            break
    if blocking_work is not None:
        raise ProfileRevisionBusyError(
            f'Registry profile {profile!r} cannot advance to revision '
            f'{revision} while data-plane work holds an active lease.')
    activated = session.execute(table.update().where(
        table.c.workspace == workspace,
        table.c.profile == profile,
        table.c.revision == current_revision,
        table.c.revision_fingerprint == current_fingerprint,
    ).values(revision=revision,
             revision_fingerprint=revision_fingerprint,
             updated_at=now))
    if activated.rowcount != 1:
        raise ProfileRevisionBusyError(
            f'Registry profile {profile!r} changed concurrently; retry '
            'profile activation.')


def _lock_or_activate_profile_revision(
    session: orm.Session,
    workspace: str,
    profile: str,
    revision: int,
    revision_fingerprint: str,
    now: int,
) -> None:
    """Shares an unchanged generation or exclusively activates a new one."""
    table = global_user_state.container_image_profile_revision_table
    current = session.execute(
        sqlalchemy.select(table.c.revision, table.c.revision_fingerprint).where(
            table.c.workspace == workspace,
            table.c.profile == profile)).mappings().first()
    if (current is not None and int(current['revision']) == revision and
            str(current['revision_fingerprint']) == revision_fingerprint and
            global_user_state.lock_container_image_profile_revision_for_work(
                session, workspace, profile, revision)):
        return
    _lock_profile_revision(session, workspace, profile, revision,
                           revision_fingerprint, now)


def _validate_location_arguments(
    target_fingerprint: str,
    policy_fingerprint: str,
    *,
    canonical: bool,
    canonical_location_id: str | None,
    auto_evict: bool,
) -> None:
    """Validates location arguments before any catalog transaction writes."""
    models.validate_fingerprint(target_fingerprint, 'target_fingerprint')
    models.validate_fingerprint(policy_fingerprint, 'policy_fingerprint')
    if canonical and auto_evict:
        raise ValueError('Canonical materializations cannot be auto-evicted.')
    if canonical and canonical_location_id is not None:
        raise ValueError('Canonical materializations cannot name a canonical '
                         'source location.')


def _repair_locked_location(
    session: orm.Session,
    location: LocationRecord,
    now: int,
) -> LocationRecord:
    """Repairs one locked row without a profile-wide activation rewrite."""
    lease_metadata_present = any(
        (location.lease_owner is not None, location.lease_expires_at
         is not None, location.heartbeat_at is not None))
    active_lease = (location.lease_owner not in (None, '') and
                    location.lease_expires_at is not None and
                    location.lease_expires_at > now and
                    location.heartbeat_at is not None)
    values: dict[str, Any] = {}
    if (location.state == models.ImageLocationState.COPYING and
            not active_lease):
        values = {
            'state': models.ImageLocationState.FAILED.value,
            'target_ref': None,
            'lease_owner': None,
            'lease_expires_at': None,
            'heartbeat_at': None,
            'next_retry_at': (now if location.attempt_count
                              < _MAX_AUTOMATIC_LOCATION_ATTEMPTS else None),
            'verification_requested_at': None,
            'last_error':
                models.ImageLocationErrorCode.COPY_LEASE_EXPIRED.value,
            'updated_at': now,
        }
    elif (location.state == models.ImageLocationState.EVICTING and
          not active_lease):
        # Deletion may have completed before the worker died.  MISSING is the
        # only safe state; an operator retry rematerializes the exact digest.
        values = {
            'state': models.ImageLocationState.MISSING.value,
            'target_ref': None,
            'lease_owner': None,
            'lease_expires_at': None,
            'heartbeat_at': None,
            'next_retry_at': (now if location.attempt_count
                              < _MAX_AUTOMATIC_LOCATION_ATTEMPTS else None),
            'verification_requested_at': None,
            'last_error':
                models.ImageLocationErrorCode.EVICTION_LEASE_EXPIRED.value,
            'updated_at': now,
        }
    elif (location.state == models.ImageLocationState.READY and
          lease_metadata_present and
          not (active_lease and
               location.verification_requested_at is not None)):
        values = {
            'lease_owner': None,
            'lease_expires_at': None,
            'heartbeat_at': None,
            'updated_at': now,
        }
    elif (location.state not in (
            models.ImageLocationState.COPYING,
            models.ImageLocationState.EVICTING,
            models.ImageLocationState.READY,
    ) and lease_metadata_present):
        values = {
            'lease_owner': None,
            'lease_expires_at': None,
            'heartbeat_at': None,
            'updated_at': now,
        }
    if not values:
        return location

    table = global_user_state.container_image_location_table
    session.execute(
        table.update().where(table.c.id == location.id).values(**values))
    if (location.canonical and
            location.state == models.ImageLocationState.COPYING):
        _set_regional_canonical_ready(session, location.id, False, now)
    row = session.execute(
        table.select().where(table.c.id == location.id)).mappings().one()
    return _location_from_row(row)


def _ensure_location_in_session(
    session: orm.Session,
    image_id: str,
    profile: str,
    target_id: str,
    target_fingerprint: str,
    expected_digest: str,
    *,
    policy_fingerprint: str,
    profile_revision: int,
    profile_revision_fingerprint: str,
    canonical: bool,
    canonical_location_id: str | None,
    source_id: str | None,
    auto_evict: bool,
    now: int,
) -> LocationRecord:
    """Ensures one location without committing the caller's transaction."""
    location_table = global_user_state.container_image_location_table
    image_table = global_user_state.container_image_table
    source_table = global_user_state.container_image_source_table
    image_row = session.execute(image_table.select().where(
        image_table.c.id == image_id)).mappings().first()
    if image_row is None:
        raise ValueError(f'Container image {image_id!r} does not exist.')
    if expected_digest.lower() != image_row['source_digest']:
        raise ValueError('Location digest must match the artifact digest.')
    workspace = str(image_row['workspace'])
    _lock_or_activate_profile_revision(session, workspace, profile,
                                       profile_revision,
                                       profile_revision_fingerprint, now)

    requested_source_id = source_id
    if canonical:
        if source_id is not None:
            source_id = models.validate_catalog_id(source_id, 'source_id')
            source_statement = source_table.select().where(
                source_table.c.id == source_id,
                source_table.c.image_id == image_id,
                source_table.c.workspace == workspace,
            )
        else:
            source_statement = (source_table.select().where(
                source_table.c.image_id == image_id,
                source_table.c.workspace == workspace,
            ).order_by(source_table.c.created_at, source_table.c.id).limit(1))
        source_row = session.execute(source_statement).mappings().first()
        if source_row is None:
            raise ValueError('Canonical materialization requires an immutable '
                             'source binding for the artifact.')
        source_id = str(source_row['id'])
    elif source_id is not None:
        raise ValueError('Regional materializations inherit their source from '
                         'the canonical location.')

    canonical_ready = False
    if canonical_location_id is not None:
        canonical_statement = location_table.select().where(
            location_table.c.id == canonical_location_id,
            location_table.c.image_id == image_id,
            location_table.c.profile == profile,
            location_table.c.canonical.is_(True),
            location_table.c.profile_revision == profile_revision,
        )
        canonical_statement = canonical_statement.with_for_update(read=True)
        canonical_row = session.execute(canonical_statement).mappings().first()
        if canonical_row is None:
            raise ValueError('Regional materialization must bind to the '
                             'current canonical location revision.')
        canonical_ready = (canonical_row['state']
                           == models.ImageLocationState.READY.value and
                           canonical_row['target_ref'] is not None)

    values = {
        'id': str(uuid.uuid4()),
        'workspace': workspace,
        'image_id': image_id,
        'profile': profile,
        'target_id': target_id,
        'target_fingerprint': target_fingerprint,
        'policy_fingerprint': policy_fingerprint,
        'profile_revision': profile_revision,
        'canonical': canonical,
        'canonical_location_id': canonical_location_id,
        'canonical_ready': canonical_ready,
        'source_id': source_id,
        'expected_digest': expected_digest.lower(),
        'auto_evict': auto_evict,
        'state': models.ImageLocationState.PENDING.value,
        'attempt_count': 0,
        'updated_at': now,
    }
    insert_result = session.execute(
        postgresql.insert(location_table).values(
            **values).on_conflict_do_nothing())
    inserted = insert_result.rowcount == 1
    row = session.execute(location_table.select().where(
        location_table.c.image_id == image_id,
        location_table.c.target_fingerprint == target_fingerprint,
    ).with_for_update()).mappings().one()
    location = _location_from_row(row)
    if location.profile != profile:
        raise ValueError(
            'A physical registry destination cannot be assigned to '
            'multiple distribution profiles. Use one profile identity '
            f'for this namespace; it is already {location.profile!r}.')
    if location.canonical != canonical:
        raise ValueError('A physical registry destination cannot change '
                         'between canonical and regional cache roles.')
    location = _repair_locked_location(session, location, now)

    if (canonical and requested_source_id is None and not inserted and
            location.source_id is not None):
        # A selector without an explicit source means "use this artifact's
        # established provenance", not "reselect its oldest alias".  Only a
        # brand-new canonical row defaults to the earliest immutable source.
        source_id = location.source_id

    source_changed = (canonical and location.source_id != source_id)
    if (source_changed and location.state == models.ImageLocationState.READY and
            location.source_id is not None):
        # A READY destination already names verified bytes. New aliases for
        # the same digest remain catalog sources without rewriting provenance
        # for the materialization that produced those bytes.
        source_changed = False
    if source_changed:
        active_lease = (location.lease_owner not in (None, '') and
                        location.lease_expires_at is not None and
                        location.lease_expires_at > now and
                        location.heartbeat_at is not None)
        if active_lease:
            raise ProfileRevisionBusyError(
                'Canonical source binding cannot change during an active '
                'materialization lease.')
        source_update = session.execute(location_table.update().where(
            location_table.c.id == location.id).values(
                source_id=source_id,
                state=models.ImageLocationState.PENDING.value,
                attempt_count=0,
                target_ref=None,
                lease_owner=None,
                lease_expires_at=None,
                heartbeat_at=None,
                next_retry_at=None,
                last_verified_at=None,
                verification_requested_at=None,
                last_error=None,
                updated_at=now))
        if source_update.rowcount == 1 and canonical:
            _set_regional_canonical_ready(session, location.id, False, now)

    policy_changed = (location.target_id != target_id or
                      location.policy_fingerprint != policy_fingerprint or
                      location.profile_revision != profile_revision or
                      location.canonical_location_id != canonical_location_id or
                      location.canonical_ready != canonical_ready or
                      location.auto_evict != auto_evict)
    if policy_changed:
        available = not (location.lease_owner not in (None, '') and
                         location.lease_expires_at is not None and
                         location.lease_expires_at > now and
                         location.heartbeat_at is not None)
        if (location.state in (models.ImageLocationState.COPYING,
                               models.ImageLocationState.EVICTING) or
                not available):
            raise ProfileRevisionBusyError(
                'Registry policy revision cannot be transferred while '
                'the physical location has an active lease.')
        session.execute(location_table.update().where(
            location_table.c.id == location.id).values(
                target_id=target_id,
                policy_fingerprint=policy_fingerprint,
                profile_revision=profile_revision,
                canonical_location_id=canonical_location_id,
                canonical_ready=canonical_ready,
                auto_evict=auto_evict,
                lease_owner=None,
                lease_expires_at=None,
                heartbeat_at=None,
                updated_at=now))
    if location.state == models.ImageLocationState.EVICTED:
        session.execute(location_table.update().where(
            location_table.c.id == location.id).values(
                state=models.ImageLocationState.PENDING.value,
                target_ref=None,
                lease_owner=None,
                lease_expires_at=None,
                heartbeat_at=None,
                next_retry_at=None,
                verification_requested_at=None,
                last_error=None,
                updated_at=now))
    refreshed_row = session.execute(location_table.select().where(
        location_table.c.id == location.id)).mappings().one()
    return _location_from_row(refreshed_row)


@db_retries.retry
def ensure_location(
    image_id: str,
    profile: str,
    target_id: str,
    target_fingerprint: str,
    expected_digest: str,
    *,
    policy_fingerprint: str,
    profile_revision: int,
    profile_revision_fingerprint: str,
    canonical: bool = False,
    canonical_location_id: str | None = None,
    source_id: str | None = None,
    auto_evict: bool = False,
) -> LocationRecord:
    """Creates a physical location under a monotonic profile revision."""
    profile = models.validate_control_plane_identifier(
        profile, 'Container image distribution')
    target_id = models.validate_control_plane_identifier(
        target_id, 'Container image target')
    _validate_location_arguments(target_fingerprint,
                                 policy_fingerprint,
                                 canonical=canonical,
                                 canonical_location_id=canonical_location_id,
                                 auto_evict=auto_evict)
    engine = _engine()
    with orm.Session(engine) as session:
        location = _ensure_location_in_session(
            session,
            image_id,
            profile,
            target_id,
            target_fingerprint,
            expected_digest,
            policy_fingerprint=policy_fingerprint,
            profile_revision=profile_revision,
            profile_revision_fingerprint=profile_revision_fingerprint,
            canonical=canonical,
            canonical_location_id=canonical_location_id,
            source_id=source_id,
            auto_evict=auto_evict,
            now=int(time.time()))
        session.commit()
    return location


def get_location_by_id(location_id: str) -> LocationRecord | None:
    table = global_user_state.container_image_location_table
    statement = table.select().where(table.c.id == location_id)
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().first()
    return _location_from_row(row) if row is not None else None


def get_location_by_fingerprint(
        image_id: str, target_fingerprint: str) -> LocationRecord | None:
    table = global_user_state.container_image_location_table
    statement = table.select().where(
        table.c.image_id == image_id,
        table.c.target_fingerprint == target_fingerprint)
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().first()
    return _location_from_row(row) if row is not None else None


def get_location(
    image_id: str,
    profile: str,
    target_id: str,
    target_fingerprint: str | None = None,
) -> LocationRecord | None:
    table = global_user_state.container_image_location_table
    statement = table.select().where(table.c.image_id == image_id,
                                     table.c.profile == profile,
                                     table.c.target_id == target_id)
    if target_fingerprint is not None:
        statement = statement.where(
            table.c.target_fingerprint == target_fingerprint)
    statement = statement.order_by(table.c.updated_at.desc(), table.c.id.desc())
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().first()
    return _location_from_row(row) if row is not None else None


def list_locations(image_id: str,
                   profile: str | None = None,
                   *,
                   limit: int | None = None) -> list[LocationRecord]:
    table = global_user_state.container_image_location_table
    statement = table.select().where(table.c.image_id == image_id)
    if profile is not None:
        statement = statement.where(table.c.profile == profile)
    statement = statement.order_by(table.c.profile, table.c.target_id,
                                   table.c.updated_at.desc())
    if limit is not None:
        statement = statement.limit(limit)
    with orm.Session(_engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_location_from_row(row) for row in rows]


def _current_profile_revision_exists(location: sqlalchemy.Table) -> Any:
    revision = global_user_state.container_image_profile_revision_table.alias(
        f'current_profile_revision_{location.name}')
    return sqlalchemy.exists().where(
        revision.c.workspace == location.c.workspace,
        revision.c.profile == location.c.profile,
        revision.c.revision == location.c.profile_revision,
    )


def _active_lease(table: sqlalchemy.Table, now: int) -> Any:
    """Returns the single structural definition of a live location lease."""
    return sqlalchemy.and_(_complete_lease_metadata(table),
                           table.c.lease_expires_at > now)


def _complete_lease_metadata(table: sqlalchemy.Table) -> Any:
    """Returns whether every persisted lease component is structurally valid."""
    return sqlalchemy.and_(table.c.lease_owner.isnot(None), table.c.lease_owner
                           != '', table.c.lease_expires_at.isnot(None),
                           table.c.heartbeat_at.isnot(None))


def _incomplete_lease_metadata(table: sqlalchemy.Table) -> Any:
    """Matches absent and partially written historical lease triples."""
    return sqlalchemy.or_(table.c.lease_owner.is_(None),
                          table.c.lease_owner == '',
                          table.c.lease_expires_at.is_(None),
                          table.c.heartbeat_at.is_(None))


def _lease_available(table: sqlalchemy.Table, now: int) -> Any:
    """Treats expired or structurally incomplete historical leases as free."""
    return sqlalchemy.not_(_active_lease(table, now))


def _set_regional_canonical_ready(session: orm.Session,
                                  canonical_location_id: str, ready: bool,
                                  now: int) -> None:
    """Publishes a dependency transition only to the canonical's generation."""
    table = global_user_state.container_image_location_table
    canonical = table.alias('dependency_transition_canonical')
    canonical_revision = sqlalchemy.select(canonical.c.profile_revision).where(
        canonical.c.id == canonical_location_id,
        canonical.c.canonical.is_(True)).scalar_subquery()
    session.execute(table.update().where(
        table.c.canonical_location_id == canonical_location_id,
        table.c.canonical.is_(False),
        table.c.profile_revision == canonical_revision).values(
            canonical_ready=ready, updated_at=now))


def _lock_location_profile_revision(session: orm.Session,
                                    location_id: str) -> bool:
    """Locks the active profile generation before a location transaction."""
    location = global_user_state.container_image_location_table
    row = session.execute(
        sqlalchemy.select(
            location.c.workspace,
            location.c.profile,
            location.c.profile_revision,
        ).where(location.c.id == location_id)).mappings().first()
    if row is None:
        return False
    return global_user_state.lock_container_image_profile_revision_for_work(
        session, str(row['workspace']), str(row['profile']),
        int(row['profile_revision']))


def _lock_image_for_update(
    session: orm.Session,
    image_id: str,
) -> sqlalchemy.engine.RowMapping | None:
    """Locks artifact metadata before a canonical location transition."""
    table = global_user_state.container_image_table
    statement = table.select().where(table.c.id == image_id)
    statement = statement.with_for_update()
    return session.execute(statement).mappings().first()


def _lock_location_for_update(session: orm.Session, location_id: str) -> bool:
    """Locks one physical location before reading the lease clock."""
    table = global_user_state.container_image_location_table
    statement = sqlalchemy.select(table.c.id).where(table.c.id == location_id)
    statement = statement.with_for_update()
    return session.execute(statement).first() is not None


def _exact_canonical_ready(table: sqlalchemy.Table, alias_name: str) -> Any:
    """Requires regional state to retain its exact verified source."""
    return global_user_state.container_image_exact_canonical_ready(
        table, alias_name)


def _exact_canonical_ready_join(
    table: sqlalchemy.Table,
    alias_name: str,
    *,
    distinct_target: bool = False,
) -> tuple[Any, Any]:
    """Returns an indexed join that excludes dependency-blocked regionals."""
    canonical = table.alias(alias_name)
    conditions = [
        canonical.c.id == table.c.canonical_location_id,
        canonical.c.image_id == table.c.image_id,
        canonical.c.profile == table.c.profile,
        canonical.c.profile_revision == table.c.profile_revision,
        canonical.c.canonical.is_(True),
        canonical.c.state == models.ImageLocationState.READY.value,
        canonical.c.target_ref.isnot(None),
    ]
    if distinct_target:
        conditions.append(canonical.c.target_ref != table.c.target_ref)
    return canonical, sqlalchemy.and_(*conditions)


def _regional_candidate_select(
    table: sqlalchemy.Table,
    *,
    workspace: str,
    profile: str,
    profile_revision: int,
    candidate_conditions: tuple[Any, ...] | list[Any],
    queue_eligible: Any,
    ordering_column: Any | None,
    after: tuple[int, str] | None,
    alias_prefix: str,
    distinct_target: bool = False,
    limit: int = 1,
) -> Any:
    """Builds an indexed regional queue probe for either catalog cardinality.

    ``canonical_ready`` is maintained transactionally by canonical state
    transitions. It lets an idle queue start from due regional rows without
    scanning either one million blocked regionals or one million canonicals.
    The claimant still locks and rechecks the exact canonical row before
    mutation, so this denormalized bit is an index key, never the final fence.
    """
    after_condition = None
    if after is not None:
        if ordering_column is None:
            after_condition = table.c.id > after[1]
        else:
            after_condition = sqlalchemy.or_(
                ordering_column > after[0],
                sqlalchemy.and_(ordering_column == after[0], table.c.id
                                > after[1]),
            )
    regional_conditions = [
        table.c.workspace == workspace,
        table.c.profile == profile,
        table.c.profile_revision == profile_revision,
        table.c.canonical.is_(False),
        table.c.canonical_ready.is_(True),
        *candidate_conditions,
        queue_eligible,
    ]
    if distinct_target:
        canonical = table.alias(f'{alias_prefix}_canonical')
        regional_conditions.append(sqlalchemy.exists().where(
            canonical.c.id == table.c.canonical_location_id,
            canonical.c.image_id == table.c.image_id,
            canonical.c.profile == table.c.profile,
            canonical.c.profile_revision == table.c.profile_revision,
            canonical.c.canonical.is_(True),
            canonical.c.state == models.ImageLocationState.READY.value,
            canonical.c.target_ref.isnot(None), canonical.c.target_ref
            != table.c.target_ref))
    if after_condition is not None:
        regional_conditions.append(after_condition)
    queue_position = (sqlalchemy.literal(0)
                      if ordering_column is None else ordering_column)
    statement = sqlalchemy.select(
        table.c.id,
        queue_position.label('queue_position'),
    ).where(*regional_conditions)
    if ordering_column is None:
        statement = statement.order_by(table.c.id)
    else:
        statement = statement.order_by(ordering_column, table.c.id)
    return statement.limit(limit)


def _eviction_candidate_page(
    session: orm.Session,
    table: sqlalchemy.Table,
    *,
    cursor_name: str,
    workspace: str,
    profile: str,
    profile_revision: int,
    queue_index: int,
    candidate_conditions: tuple[Any, ...] | list[Any],
    queue_eligible: Any,
    ordering_column: Any | None,
    alias_prefix: str,
    limit: int,
) -> list[sqlalchemy.engine.RowMapping]:
    """Reads one bounded keyset page and advances past referenced rows."""
    cursor_key = (cursor_name, workspace, profile, profile_revision,
                  queue_index)
    with _PROFILE_CURSOR_LOCK:
        after = _EVICTION_CANDIDATE_CURSORS.get(cursor_key)

    def _query(boundary: tuple[int, str] | None) -> list[Any]:
        statement = _regional_candidate_select(
            table,
            workspace=workspace,
            profile=profile,
            profile_revision=profile_revision,
            candidate_conditions=candidate_conditions,
            queue_eligible=queue_eligible,
            ordering_column=ordering_column,
            after=boundary,
            alias_prefix=alias_prefix,
            distinct_target=True,
            limit=limit,
        )
        return list(session.execute(statement).mappings().all())

    rows = _query(after)
    if after is not None and len(rows) < limit:
        # Complete one cyclic page in the same call.  Merely wrapping when the
        # suffix is empty can report a false-empty queue when the suffix has a
        # candidate that loses its final fence while an eligible prefix row
        # still exists.  Both probes remain bounded by ``limit``; de-duplicate
        # the suffix if the queue contains fewer than one full page.
        seen_ids = {str(row['id']) for row in rows}
        for row in _query(None):
            location_id = str(row['id'])
            if location_id in seen_ids:
                continue
            rows.append(row)
            seen_ids.add(location_id)
            if len(rows) == limit:
                break
    if rows:
        tail = rows[-1]
        with _PROFILE_CURSOR_LOCK:
            _EVICTION_CANDIDATE_CURSORS[cursor_key] = (int(
                tail['queue_position']), str(tail['id']))
    return rows


def _lock_exact_canonical_ready(session: orm.Session, location_id: str) -> bool:
    """Serializes a regional transition against exact-canonical loss."""
    return global_user_state.lock_container_image_exact_canonical_for_work(
        session, location_id)


def _reconciliation_queue_specs(
    table: sqlalchemy.Table,
    now: int,
) -> list[tuple[Any, Any | None, bool]]:
    """Builds disjoint, due-first queues for listing and atomic claiming."""
    attempt_available = (table.c.attempt_count
                         < _MAX_AUTOMATIC_LOCATION_ATTEMPTS)
    pending_fresh = sqlalchemy.and_(
        attempt_available,
        table.c.state == models.ImageLocationState.PENDING.value,
        table.c.next_retry_at.is_(None),
    )
    pending_retry = sqlalchemy.and_(
        attempt_available,
        table.c.state == models.ImageLocationState.PENDING.value,
        table.c.next_retry_at.isnot(None),
        table.c.next_retry_at <= now,
    )
    incomplete_copy = sqlalchemy.and_(
        attempt_available,
        table.c.state == models.ImageLocationState.COPYING.value,
        _incomplete_lease_metadata(table),
    )
    expired_copy = sqlalchemy.and_(
        attempt_available,
        table.c.state == models.ImageLocationState.COPYING.value,
        _complete_lease_metadata(table),
        table.c.lease_expires_at <= now,
    )
    failed_retry = sqlalchemy.and_(
        attempt_available,
        table.c.state.in_([
            # These fixed enum literals must compile as literals, rather than
            # bind parameters, so PostgreSQL can prove the partial-index
            # predicate for FAILED/MISSING due probes.
            sqlalchemy.literal_column("'FAILED'"),
            sqlalchemy.literal_column("'MISSING'"),
        ]),
        table.c.next_retry_at.isnot(None),
        table.c.next_retry_at <= now,
    )
    verification_base = sqlalchemy.and_(
        attempt_available,
        table.c.state == models.ImageLocationState.READY.value,
        table.c.target_ref.isnot(None),
        table.c.verification_requested_at.isnot(None),
        _lease_available(table, now),
    )
    verification_fresh = sqlalchemy.and_(verification_base,
                                         table.c.next_retry_at.is_(None))
    verification_retry = sqlalchemy.and_(
        verification_base,
        table.c.next_retry_at.isnot(None),
        table.c.next_retry_at <= now,
    )
    return [
        (incomplete_copy, None, False),
        (expired_copy, table.c.lease_expires_at, False),
        (pending_fresh, table.c.updated_at, False),
        (pending_retry, table.c.next_retry_at, False),
        (failed_retry, table.c.next_retry_at, False),
        (verification_fresh, table.c.verification_requested_at, True),
        (verification_retry, table.c.next_retry_at, True),
    ]


def list_reconciliation_candidates(
    workspace: str | None = None,
    *,
    now: int | None = None,
    limit: int = 100,
) -> list[LocationRecord]:
    """Returns bounded materialization and verification work ready to claim."""
    if limit <= 0:
        raise ValueError('limit must be positive.')
    if now is None:
        now = int(time.time())
    table = global_user_state.container_image_location_table
    queue_specs = _reconciliation_queue_specs(table, now)
    location_ids: list[str] = []
    engine = _engine()
    with orm.Session(engine) as session:
        profiles = _bounded_profile_page(session,
                                         workspace=workspace,
                                         cursor_name='reconciliation-list')
        for canonical in (True, False):
            for profile_row in profiles:
                for queue_eligible, ordering_column, _ in queue_specs:
                    remaining = limit - len(location_ids)
                    if remaining == 0:
                        break
                    conditions = [
                        table.c.workspace == str(profile_row['workspace']),
                        table.c.profile == str(profile_row['profile']),
                        table.c.profile_revision == int(
                            profile_row['revision']),
                        table.c.canonical.is_(canonical),
                        queue_eligible,
                    ]
                    if not canonical:
                        conditions.append(table.c.canonical_ready.is_(True))
                    statement = sqlalchemy.select(table.c.id).where(*conditions)
                    if ordering_column is None:
                        statement = statement.order_by(table.c.id)
                    else:
                        statement = statement.order_by(ordering_column,
                                                       table.c.id)
                    rows = session.execute(
                        statement.limit(remaining)).scalars().all()
                    location_ids.extend(
                        str(location_id) for location_id in rows)
                if len(location_ids) == limit:
                    break
            if len(location_ids) == limit:
                break
        if not location_ids:
            return []
        rows = session.execute(table.select().where(
            table.c.id.in_(location_ids))).mappings().all()
    rows_by_id = {str(row['id']): row for row in rows}
    return [
        _location_from_row(rows_by_id[location_id])
        for location_id in location_ids
    ]


def claim_next_reconciliation_candidate(
    workspace: str,
    owner: str,
    materialization_lease_seconds: int,
    verification_lease_seconds: int,
    *,
    now: int | None = None,
) -> LocationRecord | None:
    """Claims one due row through state-specific indexed queue probes.

    Queue kinds rotate in process so a continuous import stream cannot starve
    retries or verification.  Discovery stays on partial indexes; the second
    phase locks profile, canonical, and regional rows in that order and
    rechecks the selected primary-key row.  A worker that collides with a peer
    seeks forward instead of sorting or rereading the full queue.
    """
    if materialization_lease_seconds <= 0 or verification_lease_seconds <= 0:
        raise ValueError('reconciliation lease durations must be positive.')
    use_wall_clock = now is None
    if now is None:
        now = int(time.time())
    table = global_user_state.container_image_location_table
    # Expired leases are considered before fresh work in the initial order;
    # the process-local rotation moves every queue kind to the front over
    # successive claims.
    queue_specs = _reconciliation_queue_specs(table, now)
    queue_offset = next(_RECONCILIATION_QUEUE_SEQUENCE) % len(queue_specs)
    queue_specs = queue_specs[queue_offset:] + queue_specs[:queue_offset]
    engine = _engine()
    lease_token = _new_lease_token(owner)
    with orm.Session(engine) as session:
        profiles = _bounded_profile_page(session,
                                         workspace=workspace,
                                         cursor_name='reconciliation-claim')
        session.rollback()
        for profile_row in profiles:
            candidate_profile = str(profile_row['profile'])
            candidate_revision = int(profile_row['revision'])
            if not global_user_state.lock_container_image_profile_revision_for_work(
                    session,
                    workspace,
                    candidate_profile,
                    candidate_revision,
                    skip_locked=True):
                session.rollback()
                continue
            profile_conditions = (
                table.c.workspace == workspace,
                table.c.profile == candidate_profile,
                table.c.profile_revision == candidate_revision,
            )
            candidate_seek_budget = _MAX_CLAIM_CANDIDATE_SEEKS
            # Canonical work remains ahead of regional copies so a missing
            # source is repaired before replicas repeatedly route to it.
            for canonical in (True, False):
                if candidate_seek_budget == 0:
                    break
                for queue_eligible, ordering_column, verification in queue_specs:
                    if candidate_seek_budget == 0:
                        break
                    after: tuple[int, str] | None = None
                    while candidate_seek_budget > 0:
                        candidate_conditions = (
                            *profile_conditions,
                            table.c.canonical.is_(canonical),
                        )
                        if canonical:
                            queue_position = (sqlalchemy.literal(0)
                                              if ordering_column is None else
                                              ordering_column)
                            claim_select = sqlalchemy.select(
                                table.c.id,
                                queue_position.label('queue_position'),
                            ).where(*candidate_conditions, queue_eligible)
                            if after is not None:
                                if ordering_column is None:
                                    claim_select = claim_select.where(
                                        table.c.id > after[1])
                                else:
                                    claim_select = claim_select.where(
                                        sqlalchemy.or_(
                                            ordering_column > after[0],
                                            sqlalchemy.and_(
                                                ordering_column == after[0],
                                                table.c.id > after[1],
                                            )))
                            if ordering_column is None:
                                claim_select = claim_select.order_by(table.c.id)
                            else:
                                claim_select = claim_select.order_by(
                                    ordering_column, table.c.id)
                            claim_select = claim_select.limit(1)
                        else:
                            claim_select = _regional_candidate_select(
                                table,
                                workspace=workspace,
                                profile=candidate_profile,
                                profile_revision=candidate_revision,
                                candidate_conditions=candidate_conditions,
                                queue_eligible=queue_eligible,
                                ordering_column=ordering_column,
                                after=after,
                                alias_prefix='reconciliation_candidate',
                            )
                        row = session.execute(claim_select).mappings().first()
                        if row is None:
                            break
                        candidate_seek_budget -= 1
                        location_id = str(row['id'])
                        after = (int(row['queue_position']), location_id)
                        attempt = session.begin_nested()
                        if not _lock_exact_canonical_ready(
                                session, location_id):
                            attempt.rollback()
                            continue
                        lock_select = sqlalchemy.select(
                            table.c.id).where(table.c.id == location_id)
                        lock_select = lock_select.with_for_update(
                            of=table, skip_locked=True)
                        if session.execute(lock_select).first() is None:
                            attempt.rollback()
                            continue
                        claim_now = (int(time.time())
                                     if use_wall_clock else now)
                        claim_conditions = [
                            table.c.id == location_id,
                            queue_eligible,
                            _exact_canonical_ready(
                                table, 'reconciliation_claim_canonical'),
                            _current_profile_revision_exists(table),
                        ]
                        claim_values: dict[Any, Any]
                        if verification:
                            claim_values = {
                                'lease_owner': lease_token,
                                'lease_expires_at': claim_now +
                                                    verification_lease_seconds,
                                'heartbeat_at': claim_now,
                                'attempt_count': table.c.attempt_count + 1,
                                'updated_at': claim_now,
                            }
                        else:
                            claim_values = {
                                'state':
                                    models.ImageLocationState.COPYING.value,
                                'lease_owner': lease_token,
                                'lease_expires_at':
                                    claim_now + materialization_lease_seconds,
                                'heartbeat_at': claim_now,
                                'attempt_count': table.c.attempt_count + 1,
                                'updated_at': claim_now,
                                'target_ref': None,
                                'verification_requested_at': None,
                                'last_error': None,
                            }
                        result = session.execute(table.update().where(
                            *claim_conditions).values(**claim_values))
                        if result.rowcount != 1:
                            attempt.rollback()
                            continue
                        attempt.commit()
                        session.commit()
                        return get_location_by_id(location_id)
            session.rollback()
        return None


def claim_location(location_id: str, owner: str,
                   lease_seconds: int) -> LocationRecord | None:
    """Claims pending work or reclaims an expired materialization lease."""
    if lease_seconds <= 0:
        raise ValueError('lease_seconds must be positive.')
    location = get_location_by_id(location_id)
    if location is None:
        raise ValueError(f'Unknown image materialization {location_id!r}.')
    table = global_user_state.container_image_location_table
    lease_token = _new_lease_token(owner)
    with orm.Session(_engine()) as session:
        if not _lock_location_profile_revision(session, location_id):
            session.rollback()
            return None
        if not _lock_exact_canonical_ready(session, location_id):
            session.rollback()
            return None
        if not _lock_location_for_update(session, location_id):
            session.rollback()
            return None
        now = int(time.time())
        retry_due = sqlalchemy.and_(table.c.next_retry_at.isnot(None),
                                    table.c.next_retry_at <= now)
        eligible = sqlalchemy.or_(
            sqlalchemy.and_(
                table.c.attempt_count < _MAX_AUTOMATIC_LOCATION_ATTEMPTS,
                table.c.state == models.ImageLocationState.PENDING.value,
                sqlalchemy.or_(table.c.next_retry_at.is_(None),
                               table.c.next_retry_at <= now),
            ),
            sqlalchemy.and_(
                table.c.attempt_count < _MAX_AUTOMATIC_LOCATION_ATTEMPTS,
                table.c.state == models.ImageLocationState.COPYING.value,
                _lease_available(table, now),
            ),
            sqlalchemy.and_(
                table.c.attempt_count < _MAX_AUTOMATIC_LOCATION_ATTEMPTS,
                table.c.state.in_([
                    models.ImageLocationState.FAILED.value,
                    models.ImageLocationState.MISSING.value,
                ]), retry_due),
        )
        conditions = [table.c.id == location_id, eligible]
        if not location.canonical:
            canonical = table.alias('canonical_materialization')
            conditions.append(sqlalchemy.exists().where(
                canonical.c.id == location.canonical_location_id,
                canonical.c.image_id == location.image_id,
                canonical.c.profile_revision == location.profile_revision,
                canonical.c.canonical.is_(True),
                canonical.c.state == models.ImageLocationState.READY.value,
                canonical.c.target_ref.isnot(None),
            ))
        conditions.append(_current_profile_revision_exists(table))
        statement = table.update().where(*conditions).values(
            state=models.ImageLocationState.COPYING.value,
            lease_owner=lease_token,
            lease_expires_at=now + lease_seconds,
            heartbeat_at=now,
            attempt_count=table.c.attempt_count + 1,
            updated_at=now,
            target_ref=None,
            verification_requested_at=None,
            last_error=None,
        )
        result = session.execute(statement)
        session.commit()
    if result.rowcount != 1:
        return None
    return get_location_by_id(location_id)


def heartbeat_location(location_id: str, lease_token: str,
                       lease_seconds: int) -> bool:
    if lease_seconds <= 0:
        raise ValueError('lease_seconds must be positive.')
    table = global_user_state.container_image_location_table
    with orm.Session(_engine()) as session:
        if not _lock_location_profile_revision(session, location_id):
            session.rollback()
            return False
        if not _lock_exact_canonical_ready(session, location_id):
            session.rollback()
            return False
        if not _lock_location_for_update(session, location_id):
            session.rollback()
            return False
        now = int(time.time())
        statement = table.update().where(
            table.c.id == location_id,
            table.c.state.in_([
                models.ImageLocationState.COPYING.value,
                models.ImageLocationState.EVICTING.value,
                models.ImageLocationState.READY.value,
            ]),
            table.c.lease_owner == lease_token,
            table.c.lease_expires_at > now,
        ).values(lease_expires_at=now + lease_seconds,
                 heartbeat_at=now,
                 updated_at=now)
        result = session.execute(statement)
        session.commit()
        return result.rowcount == 1


def complete_location(location_id: str,
                      lease_token: str,
                      target_ref: str,
                      verified_digest: str,
                      platforms: tuple[str, ...],
                      compressed_size_bytes: int | None = None) -> bool:
    """Publishes READY only after digest and platform verification."""
    location = get_location_by_id(location_id)
    if location is None:
        raise ValueError(f'Unknown image materialization {location_id!r}.')
    try:
        target_ref = models.validate_oci_reference(
            target_ref, 'Destination image reference')
    except ValueError:
        fail_location(
            location_id, lease_token,
            models.ImageLocationErrorCode.DESTINATION_REFERENCE_INVALID)
        return False
    _, ref_digest = models.split_digest(target_ref)
    verified_digest = verified_digest.lower()
    if (verified_digest != location.expected_digest or
            ref_digest != verified_digest):
        fail_location(location_id, lease_token,
                      models.ImageLocationErrorCode.DESTINATION_DIGEST_MISMATCH)
        return False
    try:
        platforms = models.validate_materialization_platforms(
            platforms, 'Materialization platforms')
        compressed_size_bytes = models.validate_compressed_size_bytes(
            compressed_size_bytes, 'Materialization compressed size')
    except (TypeError, ValueError):
        fail_location(location_id, lease_token,
                      models.ImageLocationErrorCode.MATERIALIZATION_FAILED)
        return False
    if not location.canonical:
        canonical = (get_location_by_id(location.canonical_location_id)
                     if location.canonical_location_id is not None else None)
        if (canonical is None or not canonical.canonical or
                canonical.image_id != location.image_id or
                canonical.profile_revision != location.profile_revision or
                canonical.state != models.ImageLocationState.READY or
                canonical.target_ref is None or
                target_ref == canonical.target_ref):
            fail_location(
                location_id, lease_token,
                models.ImageLocationErrorCode.REGIONAL_IDENTITY_MISMATCH)
            return False
    table = global_user_state.container_image_location_table
    try:
        with orm.Session(_engine()) as session:
            if not _lock_location_profile_revision(session, location_id):
                session.rollback()
                return False
            image_row = None
            artifact_updates: dict[str, Any] = {}
            if location.canonical:
                image_row = _lock_image_for_update(session, location.image_id)
                if image_row is None:
                    session.rollback()
                    return False
                try:
                    current_platforms = models.validate_oci_platforms(
                        json.loads(image_row['platforms_json'] or '[]'),
                        'Stored container image platforms')
                    current_size = models.validate_compressed_size_bytes(
                        image_row['compressed_size_bytes'],
                        'Stored container image size')
                except (TypeError, ValueError, json.JSONDecodeError):
                    raise _ArtifactEvidenceConflict() from None
                if (current_platforms and
                        set(current_platforms) != set(platforms)):
                    raise _ArtifactEvidenceConflict()
                if (current_size is not None and
                        compressed_size_bytes is not None and
                        current_size != compressed_size_bytes):
                    raise _ArtifactEvidenceConflict()
                if not current_platforms:
                    artifact_updates['platforms_json'] = json.dumps(
                        list(platforms))
                if (current_size is None and compressed_size_bytes is not None):
                    artifact_updates[
                        'compressed_size_bytes'] = compressed_size_bytes
            if not _lock_exact_canonical_ready(session, location_id):
                session.rollback()
                return False
            if not _lock_location_for_update(session, location_id):
                session.rollback()
                return False
            # Read the wall clock only after every potentially blocking row
            # lock. A lease that expired while this worker waited cannot be
            # completed using a stale pre-lock timestamp.
            now = int(time.time())
            conditions = [
                table.c.id == location_id,
                table.c.state == models.ImageLocationState.COPYING.value,
                table.c.lease_owner == lease_token,
                table.c.lease_expires_at > now,
                _current_profile_revision_exists(table),
            ]
            if not location.canonical:
                canonical_table = table.alias('complete_canonical_location')
                conditions.append(sqlalchemy.exists().where(
                    canonical_table.c.id == table.c.canonical_location_id,
                    canonical_table.c.image_id == table.c.image_id,
                    canonical_table.c.profile_revision ==
                    table.c.profile_revision,
                    canonical_table.c.canonical.is_(True),
                    canonical_table.c.state ==
                    models.ImageLocationState.READY.value,
                    canonical_table.c.target_ref.isnot(None),
                ))
            statement = table.update().where(*conditions).values(
                state=models.ImageLocationState.READY.value,
                target_ref=target_ref,
                lease_owner=None,
                lease_expires_at=None,
                heartbeat_at=None,
                next_retry_at=None,
                last_verified_at=now,
                verification_requested_at=None,
                last_used_at=now,
                last_error=None,
                attempt_count=0,
                updated_at=now,
            )
            result = session.execute(statement)
            if result.rowcount == 1 and location.canonical:
                if artifact_updates:
                    artifact_updates['updated_at'] = now
                    session.execute(
                        global_user_state.container_image_table.update().where(
                            global_user_state.container_image_table.c.id ==
                            location.image_id).values(**artifact_updates))
                _set_regional_canonical_ready(session, location_id, True, now)
            session.commit()
            return result.rowcount == 1
    except _ArtifactEvidenceConflict:
        fail_location(location_id, lease_token,
                      models.ImageLocationErrorCode.MATERIALIZATION_FAILED)
        return False
    except sqlalchemy_exc.IntegrityError:
        fail_location(location_id, lease_token,
                      models.ImageLocationErrorCode.DESTINATION_ALIAS_CONFLICT)
        return False


def fail_location(location_id: str,
                  lease_token: str,
                  error: models.ImageLocationErrorCode,
                  retry_at: int | None = None) -> bool:
    table = global_user_state.container_image_location_table
    with orm.Session(_engine()) as session:
        if not _lock_location_profile_revision(session, location_id):
            session.rollback()
            return False
        if not _lock_location_for_update(session, location_id):
            session.rollback()
            return False
        now = int(time.time())
        bounded_retry_at = (None if retry_at is None else sqlalchemy.case(
            (table.c.attempt_count >= _MAX_AUTOMATIC_LOCATION_ATTEMPTS, None),
            else_=retry_at))
        statement = table.update().where(
            table.c.id == location_id,
            table.c.state == models.ImageLocationState.COPYING.value,
            table.c.lease_owner == lease_token,
            table.c.lease_expires_at > now,
        ).values(state=models.ImageLocationState.FAILED.value,
                 lease_owner=None,
                 lease_expires_at=None,
                 heartbeat_at=None,
                 next_retry_at=bounded_retry_at,
                 last_error=_error_code(error),
                 updated_at=now)
        result = session.execute(statement)
        if result.rowcount == 1:
            _set_regional_canonical_ready(session, location_id, False, now)
        session.commit()
        return result.rowcount == 1


def retry_location(location_id: str) -> bool:
    """Retries missing content or requests non-destructive READY verification."""
    table = global_user_state.container_image_location_table
    with orm.Session(_engine()) as session:
        if not _lock_location_profile_revision(session, location_id):
            session.rollback()
            return False
        row = session.execute(table.select().where(
            table.c.id == location_id).with_for_update()).mappings().first()
        if row is None:
            session.rollback()
            return False
        now = int(time.time())
        location = _repair_locked_location(session, _location_from_row(row),
                                           now)
        if (location.state == models.ImageLocationState.READY and
                location.lease_owner not in (None, '') and
                location.lease_expires_at is not None and
                location.lease_expires_at > now and
                location.heartbeat_at is not None and
                location.verification_requested_at is not None):
            session.commit()
            return True
        requested = session.execute(table.update().where(
            table.c.id == location_id,
            table.c.state == models.ImageLocationState.READY.value,
            _current_profile_revision_exists(table),
        ).values(verification_requested_at=now,
                 lease_owner=None,
                 lease_expires_at=None,
                 heartbeat_at=None,
                 next_retry_at=None,
                 attempt_count=0,
                 last_error=None,
                 updated_at=now))
        if requested.rowcount == 1:
            session.commit()
            return True
        result = session.execute(table.update().where(
            table.c.id == location_id,
            table.c.state.in_([
                models.ImageLocationState.FAILED.value,
                models.ImageLocationState.MISSING.value,
                models.ImageLocationState.EVICTED.value,
            ]),
            _current_profile_revision_exists(table),
        ).values(state=models.ImageLocationState.PENDING.value,
                 target_ref=None,
                 lease_owner=None,
                 lease_expires_at=None,
                 heartbeat_at=None,
                 next_retry_at=None,
                 attempt_count=0,
                 verification_requested_at=None,
                 last_error=None,
                 updated_at=now))
        session.commit()
        return result.rowcount == 1


def claim_location_verification(location_id: str, owner: str,
                                lease_seconds: int) -> LocationRecord | None:
    """Claims a requested verification without making READY unavailable."""
    if lease_seconds <= 0:
        raise ValueError('lease_seconds must be positive.')
    table = global_user_state.container_image_location_table
    lease_token = _new_lease_token(owner)
    with orm.Session(_engine()) as session:
        if not _lock_location_profile_revision(session, location_id):
            session.rollback()
            return None
        if not _lock_exact_canonical_ready(session, location_id):
            session.rollback()
            return None
        if not _lock_location_for_update(session, location_id):
            session.rollback()
            return None
        now = int(time.time())
        statement = table.update().where(
            table.c.id == location_id,
            table.c.attempt_count < _MAX_AUTOMATIC_LOCATION_ATTEMPTS,
            table.c.state == models.ImageLocationState.READY.value,
            table.c.target_ref.isnot(None),
            _lease_available(table, now),
            table.c.verification_requested_at.isnot(None),
            sqlalchemy.or_(table.c.next_retry_at.is_(None),
                           table.c.next_retry_at <= now),
            _exact_canonical_ready(table, 'verification_claim_canonical'),
            _current_profile_revision_exists(table),
        ).values(lease_owner=lease_token,
                 lease_expires_at=now + lease_seconds,
                 heartbeat_at=now,
                 attempt_count=table.c.attempt_count + 1,
                 updated_at=now)
        result = session.execute(statement)
        session.commit()
    if result.rowcount != 1:
        return None
    return get_location_by_id(location_id)


def complete_location_verification(location_id: str,
                                   lease_token: str,
                                   verified_digest: str,
                                   retry_at: int | None = None) -> bool:
    """Completes READY verification, marking confirmed drift as MISSING."""
    table = global_user_state.container_image_location_table
    location = get_location_by_id(location_id)
    if location is None or location.target_ref is None:
        return False
    _, reference_digest = models.split_digest(location.target_ref)
    matches = (verified_digest.lower() == location.expected_digest and
               reference_digest == location.expected_digest)
    with orm.Session(_engine()) as session:
        if not _lock_location_profile_revision(session, location_id):
            session.rollback()
            return False
        if not _lock_exact_canonical_ready(session, location_id):
            session.rollback()
            return False
        if not _lock_location_for_update(session, location_id):
            session.rollback()
            return False
        now = int(time.time())
        values: dict[Any, Any] = {
            'lease_owner': None,
            'lease_expires_at': None,
            'heartbeat_at': None,
            'verification_requested_at': None,
            'next_retry_at': None,
            'updated_at': now,
        }
        if matches:
            values.update(last_verified_at=now,
                          last_error=None,
                          attempt_count=0)
        else:
            values.update(state=models.ImageLocationState.MISSING.value,
                          next_retry_at=sqlalchemy.case(
                              (table.c.attempt_count
                               >= _MAX_AUTOMATIC_LOCATION_ATTEMPTS, None),
                              else_=retry_at if retry_at is not None else now),
                          last_error=(models.ImageLocationErrorCode.
                                      MANIFEST_DIGEST_MISMATCH.value))
        statement = table.update().where(
            table.c.id == location_id,
            table.c.state == models.ImageLocationState.READY.value,
            table.c.target_ref == location.target_ref,
            table.c.lease_owner == lease_token,
            table.c.lease_expires_at > now,
            _exact_canonical_ready(table, 'verification_complete_canonical'),
            _current_profile_revision_exists(table),
        ).values(**values)
        result = session.execute(statement)
        if result.rowcount == 1 and not matches and location.canonical:
            _set_regional_canonical_ready(session, location_id, False, now)
        session.commit()
        return result.rowcount == 1 and matches


def fail_location_verification(location_id: str,
                               lease_token: str,
                               error: models.ImageLocationErrorCode,
                               retry_at: int | None = None) -> bool:
    """Releases a transient verification failure without invalidating READY."""
    table = global_user_state.container_image_location_table
    with orm.Session(_engine()) as session:
        if not _lock_location_profile_revision(session, location_id):
            session.rollback()
            return False
        if not _lock_location_for_update(session, location_id):
            session.rollback()
            return False
        now = int(time.time())
        bounded_retry_at = (None if retry_at is None else sqlalchemy.case(
            (table.c.attempt_count >= _MAX_AUTOMATIC_LOCATION_ATTEMPTS, None),
            else_=retry_at))
        bounded_verification_request = sqlalchemy.case(
            (table.c.attempt_count >= _MAX_AUTOMATIC_LOCATION_ATTEMPTS, None),
            else_=table.c.verification_requested_at)
        statement = table.update().where(
            table.c.id == location_id,
            table.c.state == models.ImageLocationState.READY.value,
            table.c.lease_owner == lease_token,
            table.c.lease_expires_at > now,
        ).values(lease_owner=None,
                 lease_expires_at=None,
                 heartbeat_at=None,
                 next_retry_at=bounded_retry_at,
                 verification_requested_at=bounded_verification_request,
                 last_error=_error_code(error),
                 updated_at=now)
        result = session.execute(statement)
        session.commit()
        return result.rowcount == 1


def mark_location_missing(location_id: str) -> bool:
    table = global_user_state.container_image_location_table
    with orm.Session(_engine()) as session:
        if not _lock_location_profile_revision(session, location_id):
            session.rollback()
            return False
        if not _lock_location_for_update(session, location_id):
            session.rollback()
            return False
        now = int(time.time())
        statement = table.update().where(
            table.c.id == location_id,
            table.c.state == models.ImageLocationState.READY.value,
        ).values(
            state=models.ImageLocationState.MISSING.value,
            lease_owner=None,
            lease_expires_at=None,
            heartbeat_at=None,
            verification_requested_at=None,
            last_error=(models.ImageLocationErrorCode.MANIFEST_MISSING.value),
            updated_at=now)
        result = session.execute(statement)
        if result.rowcount == 1:
            _set_regional_canonical_ready(session, location_id, False, now)
        session.commit()
        return result.rowcount == 1


def acquire_reference(location_id: str,
                      workspace: str,
                      consumer_type: str,
                      consumer_id: str,
                      *,
                      expected_ref: str | None = None,
                      expires_at: int | None = None) -> ReferenceRecord:
    """Atomically pins a READY route for a durable workload consumer."""
    consumer_type = consumer_type.strip()
    consumer_id = consumer_id.strip()
    if not consumer_type or not consumer_id:
        raise ValueError('Reference consumer type and ID must be non-empty.')
    if consumer_type == 'cluster':
        raise ValueError(
            'Cluster image references must be committed atomically with the '
            'cluster handle; use global_user_state.add_or_update_cluster().')
    now = int(time.time())
    location_table = global_user_state.container_image_location_table
    image_table = global_user_state.container_image_table
    conditions = [
        location_table.c.id == location_id,
        location_table.c.state == models.ImageLocationState.READY.value,
        _lease_available(location_table, now),
        sqlalchemy.exists().where(image_table.c.id == location_table.c.image_id,
                                  image_table.c.workspace == workspace),
        _exact_canonical_ready(location_table, 'reference_canonical'),
        _current_profile_revision_exists(location_table),
    ]
    if expected_ref is not None:
        conditions.append(location_table.c.target_ref == expected_ref)
    reference_table = global_user_state.container_image_reference_table
    with orm.Session(_engine()) as session:
        if not _lock_location_profile_revision(session, location_id):
            session.rollback()
            raise ValueError('Image materialization belongs to a stale '
                             'registry profile revision.')
        if not _lock_exact_canonical_ready(session, location_id):
            session.rollback()
            raise ValueError('Image materialization is no longer READY '
                             'because its exact canonical source is '
                             'unavailable.')
        touched = session.execute(location_table.update().where(
            *conditions).values(last_used_at=now,
                                next_retry_at=None,
                                updated_at=now))
        if touched.rowcount != 1:
            session.rollback()
            raise ValueError('Image materialization is no longer READY, is '
                             'being verified, or does not match the pinned '
                             'reference.')
        reference_id = str(uuid.uuid4())
        reference_insert = postgresql.insert(reference_table).values(
            id=reference_id,
            workspace=workspace,
            location_id=location_id,
            consumer_type=consumer_type,
            consumer_id=consumer_id,
            expires_at=expires_at,
            created_at=now,
            updated_at=now)
        session.execute(
            reference_insert.on_conflict_do_update(
                index_elements=[
                    reference_table.c.workspace,
                    reference_table.c.consumer_type,
                    reference_table.c.consumer_id,
                ],
                set_={
                    reference_table.c.location_id: location_id,
                    reference_table.c.expires_at: expires_at,
                    reference_table.c.updated_at: now,
                }))
        reference_row = session.execute(reference_table.select().where(
            reference_table.c.workspace == workspace,
            reference_table.c.consumer_type == consumer_type,
            reference_table.c.consumer_id ==
            consumer_id).with_for_update()).mappings().one()
        reference = _reference_from_row(reference_row)
        session.commit()
    return reference


def get_reference(reference_id: str) -> ReferenceRecord | None:
    table = global_user_state.container_image_reference_table
    statement = table.select().where(table.c.id == reference_id)
    with orm.Session(_engine()) as session:
        row = session.execute(statement).mappings().first()
    return _reference_from_row(row) if row is not None else None


def release_reference(workspace: str, consumer_type: str,
                      consumer_id: str) -> bool:
    consumer_type = consumer_type.strip()
    consumer_id = consumer_id.strip()
    if not consumer_type or not consumer_id:
        raise ValueError('Reference consumer type and ID must be non-empty.')
    if consumer_type == 'cluster':
        raise ValueError(
            'Cluster image references must be released atomically with the '
            'cluster handle transition or termination.')
    table = global_user_state.container_image_reference_table
    statement = table.delete().where(table.c.workspace == workspace,
                                     table.c.consumer_type == consumer_type,
                                     table.c.consumer_id == consumer_id)
    with orm.Session(_engine()) as session:
        result = session.execute(statement)
        session.commit()
        return result.rowcount == 1


def _active_reference_exists(location_id: Any, now: int) -> Any:
    table = global_user_state.container_image_reference_table
    return sqlalchemy.exists().where(
        table.c.location_id == location_id,
        sqlalchemy.or_(table.c.expires_at.is_(None), table.c.expires_at > now))


def _eviction_queue_specs(table: sqlalchemy.Table, now: int,
                          unused_before: int) -> list[tuple[Any, Any | None]]:
    """Builds disjoint fresh, retry, and recovery eviction queues."""
    attempt_available = (table.c.attempt_count
                         < _MAX_AUTOMATIC_LOCATION_ATTEMPTS)
    ready_base = sqlalchemy.and_(
        attempt_available,
        table.c.state == models.ImageLocationState.READY.value,
        table.c.last_used_at.isnot(None),
        table.c.last_used_at <= unused_before,
        _lease_available(table, now),
    )
    fresh_ready = sqlalchemy.and_(ready_base, table.c.next_retry_at.is_(None))
    retry_ready = sqlalchemy.and_(
        ready_base,
        table.c.next_retry_at.isnot(None),
        table.c.next_retry_at <= now,
    )
    incomplete_eviction = sqlalchemy.and_(
        attempt_available,
        table.c.state == models.ImageLocationState.EVICTING.value,
        _incomplete_lease_metadata(table),
    )
    expired_eviction = sqlalchemy.and_(
        attempt_available,
        table.c.state == models.ImageLocationState.EVICTING.value,
        _complete_lease_metadata(table),
        table.c.lease_expires_at <= now,
    )
    return [
        (incomplete_eviction, None),
        (expired_eviction, table.c.lease_expires_at),
        (fresh_ready, table.c.last_used_at),
        (retry_ready, table.c.next_retry_at),
    ]


def list_eviction_candidates(workspace: str, unused_before: int,
                             limit: int) -> list[LocationRecord]:
    """Lists unused regional copies without an active durable reference."""
    if limit <= 0:
        raise ValueError('limit must be positive.')
    table = global_user_state.container_image_location_table
    now = int(time.time())
    selection_conditions = (
        table.c.auto_evict.is_(True),
        table.c.target_ref.isnot(None),
    )
    queue_specs = _eviction_queue_specs(table, now, unused_before)
    location_ids: list[str] = []
    engine = _engine()
    with orm.Session(engine) as session:
        profiles = _bounded_profile_page(session,
                                         workspace=workspace,
                                         cursor_name='eviction-list')
        candidate_seek_budget = _MAX_CLAIM_CANDIDATE_SEEKS
        for profile_row in profiles:
            for queue_index, (queue_eligible,
                              ordering_column) in enumerate(queue_specs):
                remaining = limit - len(location_ids)
                if remaining == 0 or candidate_seek_budget == 0:
                    break
                rows = _eviction_candidate_page(
                    session,
                    table,
                    cursor_name='eviction-list-candidate',
                    workspace=workspace,
                    profile=str(profile_row['profile']),
                    profile_revision=int(profile_row['revision']),
                    queue_index=queue_index,
                    candidate_conditions=selection_conditions,
                    queue_eligible=queue_eligible,
                    ordering_column=ordering_column,
                    alias_prefix='eviction_list_candidate',
                    limit=candidate_seek_budget,
                )
                candidate_seek_budget -= len(rows)
                candidate_ids = [str(row['id']) for row in rows]
                if not candidate_ids:
                    continue
                reference_table = (
                    global_user_state.container_image_reference_table)
                referenced_ids = set(
                    str(location_id) for location_id in session.execute(
                        sqlalchemy.select(reference_table.c.location_id).where(
                            reference_table.c.location_id.in_(candidate_ids),
                            sqlalchemy.or_(
                                reference_table.c.expires_at.is_(None),
                                reference_table.c.expires_at > now,
                            ))).scalars().all())
                location_ids.extend(location_id for location_id in candidate_ids
                                    if location_id not in referenced_ids)
                del location_ids[limit:]
            if len(location_ids) == limit or candidate_seek_budget == 0:
                break
        if not location_ids:
            return []
        rows = session.execute(table.select().where(
            table.c.id.in_(location_ids))).mappings().all()
    rows_by_id = {str(row['id']): row for row in rows}
    return [
        _location_from_row(rows_by_id[location_id])
        for location_id in location_ids
    ]


def claim_next_eviction_candidate(
    workspace: str,
    owner: str,
    lease_seconds: int,
    unused_before: int,
    *,
    now: int | None = None,
) -> LocationRecord | None:
    """Atomically claims one due eviction from an indexed fair queue.

    Candidate discovery is deliberately separate from the serialization
    fence.  The first probe can stay on the partial queue indexes at millions
    of rows.  The second phase takes locks in profile, canonical, regional
    order and rechecks every safety predicate on the primary-key row.  If a
    candidate loses that race, the worker seeks forward instead of reporting
    an empty queue or sorting the whole profile again.
    """
    if lease_seconds <= 0:
        raise ValueError('lease_seconds must be positive.')
    use_wall_clock = now is None
    if now is None:
        now = int(time.time())
    table = global_user_state.container_image_location_table
    canonical = table.alias('next_eviction_canonical')
    selection_conditions = [
        table.c.workspace == workspace,
        table.c.canonical.is_(False),
        table.c.auto_evict.is_(True),
        table.c.attempt_count < _MAX_AUTOMATIC_LOCATION_ATTEMPTS,
        table.c.target_ref.isnot(None),
        _lease_available(table, now),
    ]
    # Reference exclusion is intentionally absent from discovery. A correlated
    # anti-join can scan every referenced due row before LIMIT. Candidate pages
    # instead advance a bounded keyset cursor; the locked UPDATE remains the
    # authoritative reference fence.
    candidate_conditions = selection_conditions
    fence_conditions = [
        *selection_conditions,
        sqlalchemy.exists().where(
            canonical.c.id == table.c.canonical_location_id,
            canonical.c.image_id == table.c.image_id,
            canonical.c.profile_revision == table.c.profile_revision,
            canonical.c.canonical.is_(True),
            canonical.c.state == models.ImageLocationState.READY.value,
            canonical.c.target_ref.isnot(None),
            canonical.c.target_ref != table.c.target_ref,
        ),
        ~_active_reference_exists(table.c.id, now),
    ]
    engine = _engine()
    lease_token = _new_lease_token(owner)
    with orm.Session(engine) as session:
        profiles = _bounded_profile_page(session,
                                         workspace=workspace,
                                         cursor_name='eviction-claim')
        # Profile enumeration is advisory.  Start each profile attempt in a
        # fresh transaction so exhausted profiles do not retain shared locks
        # while a later profile is scanned.
        session.rollback()
        candidate_seek_budget = _MAX_CLAIM_CANDIDATE_SEEKS
        for candidate in profiles:
            if candidate_seek_budget == 0:
                break
            candidate_profile = str(candidate['profile'])
            candidate_revision = int(candidate['revision'])
            if not global_user_state.lock_container_image_profile_revision_for_work(
                    session,
                    workspace,
                    candidate_profile,
                    candidate_revision,
                    skip_locked=True):
                session.rollback()
                continue
            candidate_queues = _eviction_queue_specs(table, now, unused_before)
            for queue_index, (queue_eligible,
                              ordering_column) in enumerate(candidate_queues):
                if candidate_seek_budget == 0:
                    break
                rows = _eviction_candidate_page(
                    session,
                    table,
                    cursor_name='eviction-claim-candidate',
                    workspace=workspace,
                    profile=candidate_profile,
                    profile_revision=candidate_revision,
                    queue_index=queue_index,
                    candidate_conditions=candidate_conditions,
                    queue_eligible=queue_eligible,
                    ordering_column=ordering_column,
                    alias_prefix='next_eviction_candidate',
                    limit=candidate_seek_budget,
                )
                candidate_seek_budget -= len(rows)
                for row in rows:
                    location_id = str(row['id'])
                    attempt = session.begin_nested()
                    if not _lock_exact_canonical_ready(session, location_id):
                        attempt.rollback()
                        continue
                    lock_select = sqlalchemy.select(
                        table.c.id).where(table.c.id == location_id)
                    lock_select = lock_select.with_for_update(of=table,
                                                              skip_locked=True)
                    if session.execute(lock_select).first() is None:
                        attempt.rollback()
                        continue
                    claim_now = int(time.time()) if use_wall_clock else now
                    # This is a new READ COMMITTED statement after taking the
                    # row lock.  A reference that committed while the worker
                    # was routing is therefore visible to the final fence.
                    result = session.execute(table.update().where(
                        table.c.id == location_id,
                        *fence_conditions,
                        queue_eligible,
                        _current_profile_revision_exists(table),
                    ).values(
                        state=models.ImageLocationState.EVICTING.value,
                        lease_owner=lease_token,
                        lease_expires_at=claim_now + lease_seconds,
                        heartbeat_at=claim_now,
                        attempt_count=table.c.attempt_count + 1,
                        updated_at=claim_now,
                        last_error=None,
                    ))
                    if result.rowcount != 1:
                        attempt.rollback()
                        continue
                    attempt.commit()
                    session.commit()
                    return get_location_by_id(location_id)
            session.rollback()
        return None


def claim_location_eviction(location_id: str, owner: str, lease_seconds: int,
                            unused_before: int) -> LocationRecord | None:
    """Claims an unused regional copy with reference and lease fencing."""
    if lease_seconds <= 0:
        raise ValueError('lease_seconds must be positive.')
    table = global_user_state.container_image_location_table
    canonical = table.alias('canonical_materialization')
    lease_token = _new_lease_token(owner)
    with orm.Session(_engine()) as session:
        if not _lock_location_profile_revision(session, location_id):
            session.rollback()
            return None
        if not _lock_exact_canonical_ready(session, location_id):
            session.rollback()
            return None
        # Profile, canonical, then regional is the common row-lock order.  The
        # reference predicate is evaluated by a second PostgreSQL statement
        # with a fresh READ COMMITTED snapshot, so a concurrently committed
        # durable reference cannot be missed after waiting for the row.
        if not _lock_location_for_update(session, location_id):
            session.rollback()
            return None
        now = int(time.time())
        eligible = sqlalchemy.or_(
            sqlalchemy.and_(
                table.c.attempt_count < _MAX_AUTOMATIC_LOCATION_ATTEMPTS,
                table.c.state == models.ImageLocationState.READY.value,
                table.c.last_used_at.isnot(None),
                table.c.last_used_at <= unused_before,
                sqlalchemy.or_(table.c.next_retry_at.is_(None),
                               table.c.next_retry_at <= now),
            ),
            sqlalchemy.and_(
                table.c.attempt_count < _MAX_AUTOMATIC_LOCATION_ATTEMPTS,
                table.c.state == models.ImageLocationState.EVICTING.value,
                _lease_available(table, now),
            ),
        )
        statement = table.update().where(
            table.c.id == location_id,
            table.c.canonical.is_(False),
            table.c.auto_evict.is_(True),
            table.c.attempt_count < _MAX_AUTOMATIC_LOCATION_ATTEMPTS,
            table.c.target_ref.isnot(None),
            _lease_available(table, now),
            eligible,
            _current_profile_revision_exists(table),
            sqlalchemy.exists().where(
                canonical.c.id == table.c.canonical_location_id,
                canonical.c.image_id == table.c.image_id,
                canonical.c.profile_revision == table.c.profile_revision,
                canonical.c.canonical.is_(True),
                canonical.c.state == models.ImageLocationState.READY.value,
                canonical.c.target_ref.isnot(None),
                canonical.c.target_ref != table.c.target_ref,
            ),
            ~_active_reference_exists(table.c.id, now),
        ).values(state=models.ImageLocationState.EVICTING.value,
                 lease_owner=lease_token,
                 lease_expires_at=now + lease_seconds,
                 heartbeat_at=now,
                 attempt_count=table.c.attempt_count + 1,
                 updated_at=now,
                 last_error=None)
        result = session.execute(statement)
        session.commit()
    if result.rowcount != 1:
        return None
    return get_location_by_id(location_id)


def complete_location_eviction(location_id: str, lease_token: str) -> bool:
    table = global_user_state.container_image_location_table
    with orm.Session(_engine()) as session:
        if not _lock_location_profile_revision(session, location_id):
            session.rollback()
            return False
        if not _lock_exact_canonical_ready(session, location_id):
            session.rollback()
            return False
        if not _lock_location_for_update(session, location_id):
            session.rollback()
            return False
        now = int(time.time())
        statement = table.update().where(
            table.c.id == location_id,
            table.c.state == models.ImageLocationState.EVICTING.value,
            table.c.lease_owner == lease_token,
            table.c.lease_expires_at > now,
            _exact_canonical_ready(table, 'eviction_complete_canonical'),
            _current_profile_revision_exists(table),
        ).values(state=models.ImageLocationState.EVICTED.value,
                 target_ref=None,
                 lease_owner=None,
                 lease_expires_at=None,
                 heartbeat_at=None,
                 next_retry_at=None,
                 verification_requested_at=None,
                 updated_at=now,
                 last_error=None)
        result = session.execute(statement)
        session.commit()
        return result.rowcount == 1


def fail_location_eviction(location_id: str,
                           lease_token: str,
                           error: models.ImageLocationErrorCode,
                           retry_at: int | None = None,
                           *,
                           manifest_may_be_missing: bool = False) -> bool:
    table = global_user_state.container_image_location_table
    with orm.Session(_engine()) as session:
        if not _lock_location_profile_revision(session, location_id):
            session.rollback()
            return False
        if not _lock_location_for_update(session, location_id):
            session.rollback()
            return False
        now = int(time.time())
        bounded_retry_at = (None if retry_at is None else sqlalchemy.case(
            (table.c.attempt_count >= _MAX_AUTOMATIC_LOCATION_ATTEMPTS, None),
            else_=retry_at))
        values: dict[str, Any] = {
            'state': (models.ImageLocationState.MISSING.value
                      if manifest_may_be_missing else
                      models.ImageLocationState.READY.value),
            'lease_owner': None,
            'lease_expires_at': None,
            'heartbeat_at': None,
            'next_retry_at': bounded_retry_at,
            'updated_at': now,
            'last_error': _error_code(error),
        }
        if manifest_may_be_missing:
            values['target_ref'] = None
        statement = table.update().where(
            table.c.id == location_id,
            table.c.state == models.ImageLocationState.EVICTING.value,
            table.c.lease_owner == lease_token,
            table.c.lease_expires_at > now,
        ).values(**values)
        result = session.execute(statement)
        session.commit()
        return result.rowcount == 1


# Data-plane workers remain a separately deployed concern. This state module
# deliberately exposes leases and exact-digest transitions without importing a
# cloud SDK or moving image bytes through an API request worker.
