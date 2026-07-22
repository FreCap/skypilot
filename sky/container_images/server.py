"""Direct REST API for managed container image distribution."""

from __future__ import annotations

import base64
from collections.abc import Callable
import dataclasses
import hashlib
import math
import time
from typing import Any, NoReturn, TypeVar
import uuid

import fastapi

from sky import exceptions
from sky.container_images import api_models
from sky.container_images import catalog_state
from sky.container_images import config
from sky.container_images import demand_state
from sky.container_images import models
from sky.container_images import pagination
from sky.container_images import preparation
from sky.container_images import publication
from sky.container_images import qualification
from sky.container_images import topology_state
from sky.users import permission
from sky.users import rbac
from sky.workspaces import core as workspaces_core

router = fastapi.APIRouter()

_Record = TypeVar('_Record')
_CLOSED_ERRORS = frozenset({
    'ARTIFACT_NOT_READY',
    'AUTH_BINDING_UNAVAILABLE',
    'CANARY_DAILY_COST_LIMIT',
    'CANARY_FAILED',
    'IDEMPOTENCY_KEY_REUSED',
    'IMAGE_CATALOG_UNAVAILABLE',
    'IMAGE_LIMIT_EXCEEDED',
    'IMAGE_LOCALITY_UNSUPPORTED',
    'IMAGE_LOCATION_NOT_FOUND',
    'IMAGE_NOT_PUBLISHED',
    'IMAGE_OPERATION_NOT_FOUND',
    'IMAGE_PREPARATION_FAILED',
    'IMAGE_PUBLICATION_NOT_FOUND',
    'PERMISSION_DENIED',
    'PLATFORM_UNSUPPORTED',
    'PROFILE_NOT_ACTIVE',
    'QUALIFICATION_FAILED',
    'REGISTRY_CAPACITY_EXHAUSTED',
    'REGISTRY_LOCATION_QUARANTINED',
    'REGISTRY_SHARD_UNAVAILABLE',
    'RELEASE_CONFLICT',
    'TARGET_READ_ONLY',
})


def _resolve_workspace(request: fastapi.Request, requested: str | None) -> str:
    try:
        if requested is not None:
            requested = models.validate_workspace_name(
                requested, 'Container image workspace')
        resolution = workspaces_core.resolve_workspace_for_user(
            request.state.auth_user, requested)
        return models.validate_workspace_name(
            resolution.workspace, 'Resolved container image workspace')
    except exceptions.PermissionDeniedError:
        _raise_code('PERMISSION_DENIED')
    except (exceptions.WorkspaceAmbiguousError, ValueError):
        raise fastapi.HTTPException(status_code=422,
                                    detail={'code': 'INVALID_WORKSPACE'
                                           }) from None


def _roles(request: fastapi.Request) -> set[str]:
    return set(
        permission.permission_service.get_user_roles(
            request.state.auth_user.id))


def _require_admin(request: fastapi.Request) -> None:
    if rbac.RoleName.ADMIN.value not in _roles(request):
        _raise_code('PERMISSION_DENIED')


def _require_publisher(request: fastapi.Request, workspace: str) -> None:
    if not _can_publish(request, workspace):
        _raise_code('PERMISSION_DENIED')


def _can_publish(request: fastapi.Request, workspace: str) -> bool:
    roles = _roles(request)
    if rbac.RoleName.ADMIN.value in roles:
        return True
    if rbac.RoleName.VIEWER.value in roles:
        return False
    policy = config.get_workspace_policy(workspace)
    return request.state.auth_user.id in policy.publishers


def _actor_hash(request: fastapi.Request) -> str:
    return hashlib.sha256(
        f'container-images:{request.state.auth_user.id}'.encode()).hexdigest()


def _idempotency_key(value: str) -> str:
    if not isinstance(value, str) or not 16 <= len(value.encode()) <= 128:
        raise fastapi.HTTPException(status_code=422,
                                    detail={'code': 'INVALID_IDEMPOTENCY_KEY'})
    return value


def _raise_code(code: str) -> NoReturn:
    if code == 'PERMISSION_DENIED':
        status = 403
    elif code in ('IMAGE_LOCATION_NOT_FOUND', 'IMAGE_NOT_PUBLISHED',
                  'IMAGE_OPERATION_NOT_FOUND', 'IMAGE_PUBLICATION_NOT_FOUND'):
        status = 404
    elif code in ('IDEMPOTENCY_KEY_REUSED', 'REGISTRY_LOCATION_QUARANTINED',
                  'REGISTRY_SHARD_UNAVAILABLE', 'RELEASE_CONFLICT'):
        status = 409
    elif code == 'IMAGE_CATALOG_UNAVAILABLE':
        status = 503
    else:
        status = 422
    raise fastapi.HTTPException(status_code=status, detail={'code': code})


def _api_error(error: BaseException) -> NoReturn:
    if isinstance(error, catalog_state.IdempotencyKeyReusedError):
        _raise_code('IDEMPOTENCY_KEY_REUSED')
    if isinstance(error, catalog_state.ReleaseConflictError):
        _raise_code('RELEASE_CONFLICT')
    if isinstance(error, topology_state.RegistryCapacityExhaustedError):
        _raise_code('REGISTRY_CAPACITY_EXHAUSTED')
    if isinstance(error, topology_state.RegistryLocationQuarantinedError):
        _raise_code('REGISTRY_LOCATION_QUARANTINED')
    if isinstance(error, topology_state.RegistryShardUnavailableError):
        _raise_code('REGISTRY_SHARD_UNAVAILABLE')
    code = error.args[0] if error.args else None
    if isinstance(code, str) and code in _CLOSED_ERRORS:
        _raise_code(code)
    if isinstance(error, RuntimeError):
        _raise_code('IMAGE_CATALOG_UNAVAILABLE')
    raise fastapi.HTTPException(status_code=422,
                                detail={'code': 'INVALID_IMAGE_REQUEST'
                                       }) from None


def _limit(value: int) -> int:
    if not 1 <= value <= 100:
        raise fastapi.HTTPException(status_code=422,
                                    detail={'code': 'INVALID_PAGE_SIZE'})
    return value


def _after(cursor: str | None, *, scope: str, workspace: str,
           filters: dict[str, Any]) -> tuple[int, str] | None:
    if cursor is None:
        return None
    try:
        return pagination.decode(cursor,
                                 scope=scope,
                                 workspace=workspace,
                                 filters=filters)
    except pagination.InvalidCursorError:
        raise fastapi.HTTPException(status_code=409,
                                    detail={'code': 'STALE_IMAGE_CURSOR'
                                           }) from None


def _page(
    records: list[_Record], *, limit: int, scope: str, workspace: str,
    filters: dict[str, Any], key: Callable[[_Record], tuple[int, str]],
    view: Callable[[_Record], api_models._ApiModel]
) -> api_models.Page:  # pylint: disable=protected-access
    has_more = len(records) > limit
    page_records = records[:limit]
    next_cursor = None
    if has_more and page_records:
        next_cursor = pagination.encode(scope=scope,
                                        workspace=workspace,
                                        filters=filters,
                                        key=key(page_records[-1]))
    return api_models.Page(items=[
        item.model_dump(mode='json') for item in map(view, page_records)
    ],
                           next_cursor=next_cursor)


def _artifact(image_id: str, workspace: str) -> catalog_state.ArtifactRecord:
    try:
        image_id = models.validate_catalog_id(image_id, 'Image artifact ID')
    except ValueError:
        _raise_code('IMAGE_NOT_PUBLISHED')
    artifact = catalog_state.get_artifact(image_id, workspace)
    if artifact is None:
        _raise_code('IMAGE_NOT_PUBLISHED')
    return artifact


def _location_view(
        location: topology_state.LocationRecord) -> api_models.LocationView:
    shard = topology_state.get_shard(location.shard_id)
    if shard is None:
        _raise_code('IMAGE_CATALOG_UNAVAILABLE')
    return api_models.LocationView.from_record(location, shard.target_id,
                                               shard.profile)


def _publication_result(
    mutation: publication.PublicationMutation,) -> api_models.MutationResult:
    return api_models.MutationResult(
        kind='publication',
        operation=api_models.OperationView.from_record(mutation.operation),
        publication=api_models.PublicationView.from_record(
            mutation.publication))


def _location_result(
    mutation: preparation.LocationMutation,) -> api_models.MutationResult:
    return api_models.MutationResult(
        kind='location',
        operation=api_models.OperationView.from_record(mutation.operation),
        location=_location_view(mutation.location))


@router.post('/publications', response_model=api_models.MutationResult)
def create_publication(
    request: fastapi.Request,
    body: api_models.PublicationCreate,
    idempotency_key: str = fastapi.Header(alias='Idempotency-Key'),
) -> api_models.MutationResult:
    workspace = _resolve_workspace(request, body.workspace)
    _require_publisher(request, workspace)
    try:
        mutation = publication.publish(
            source_ref=body.source_ref,
            release=body.release,
            distribution=body.distribution,
            workspace=workspace,
            actor_hash=_actor_hash(request),
            idempotency_key=_idempotency_key(idempotency_key),
            requested_platform=body.platform,
            source_auth_binding_id=body.source_auth)
        return _publication_result(mutation)
    except (RuntimeError, TypeError, ValueError) as error:
        _api_error(error)


@router.post('/artifacts/{image_id}/prepare',
             response_model=api_models.MutationResult)
def prepare_artifact(
    image_id: str,
    request: fastapi.Request,
    body: api_models.ArtifactPrepare,
    idempotency_key: str = fastapi.Header(alias='Idempotency-Key'),
) -> api_models.MutationResult:
    workspace = _resolve_workspace(request, body.workspace)
    _require_publisher(request, workspace)
    try:
        mutation = preparation.prepare(
            image_id=models.validate_catalog_id(image_id, 'Image artifact ID'),
            distribution=body.distribution,
            target_id=body.target,
            workspace=workspace,
            actor_hash=_actor_hash(request),
            idempotency_key=_idempotency_key(idempotency_key))
        return _location_result(mutation)
    except (RuntimeError, TypeError, ValueError) as error:
        _api_error(error)


@router.post('/publications/{publication_id}/retry',
             response_model=api_models.MutationResult)
def retry_publication(
    publication_id: str,
    request: fastapi.Request,
    body: api_models.WorkspaceMutation,
    idempotency_key: str = fastapi.Header(alias='Idempotency-Key'),
) -> api_models.MutationResult:
    workspace = _resolve_workspace(request, body.workspace)
    _require_publisher(request, workspace)
    try:
        mutation = publication.retry(
            publication_id=publication_id,
            workspace=workspace,
            actor_hash=_actor_hash(request),
            idempotency_key=_idempotency_key(idempotency_key))
        return _publication_result(mutation)
    except (RuntimeError, TypeError, ValueError) as error:
        _api_error(error)


@router.post('/locations/{location_id}/retry',
             response_model=api_models.MutationResult)
def retry_location(
    location_id: str,
    request: fastapi.Request,
    body: api_models.WorkspaceMutation,
    idempotency_key: str = fastapi.Header(alias='Idempotency-Key'),
) -> api_models.MutationResult:
    workspace = _resolve_workspace(request, body.workspace)
    _require_publisher(request, workspace)
    try:
        mutation = preparation.retry_location(
            location_id=location_id,
            workspace=workspace,
            actor_hash=_actor_hash(request),
            idempotency_key=_idempotency_key(idempotency_key))
        return _location_result(mutation)
    except (RuntimeError, TypeError, ValueError) as error:
        _api_error(error)


@router.post('/profiles/{profile_name}/qualification',
             response_model=api_models.MutationResult)
def qualify_profile(
    profile_name: str,
    request: fastapi.Request,
    body: api_models.QualificationCreate,
    idempotency_key: str = fastapi.Header(alias='Idempotency-Key'),
) -> api_models.MutationResult:
    _require_admin(request)
    try:
        operation, revision = qualification.ingest_manifest(
            profile_name=profile_name,
            manifest=body.manifest,
            actor_hash=_actor_hash(request),
            idempotency_key=_idempotency_key(idempotency_key))
        return api_models.MutationResult(
            kind='profile_qualification',
            operation=api_models.OperationView.from_record(operation),
            profile=api_models.ProfileView.from_record(revision))
    except (RuntimeError, TypeError, ValueError) as error:
        _api_error(error)


@router.post('/profiles/{profile_name}/canaries',
             response_model=api_models.MutationResult)
def create_canary(
    profile_name: str,
    request: fastapi.Request,
    body: api_models.CanaryCreate,
    idempotency_key: str = fastapi.Header(alias='Idempotency-Key'),
) -> api_models.MutationResult:
    _require_admin(request)
    workspace = _resolve_workspace(request, body.workspace)
    try:
        operation, revision = qualification.request_canary(
            workspace=workspace,
            profile_name=profile_name,
            target_id=body.target,
            backend=body.backend,
            runtime_id=body.runtime_id,
            actor_hash=_actor_hash(request),
            idempotency_key=_idempotency_key(idempotency_key))
        return api_models.MutationResult(
            kind='profile_canary',
            operation=api_models.OperationView.from_record(operation),
            profile=api_models.ProfileView.from_record(revision))
    except (RuntimeError, TypeError, ValueError) as error:
        _api_error(error)


@router.get('/capabilities', response_model=api_models.CapabilitiesView)
def capabilities(request: fastapi.Request,
                 workspace: str | None = None) -> api_models.CapabilitiesView:
    resolved = _resolve_workspace(request, workspace)
    roles = _roles(request)
    admin = rbac.RoleName.ADMIN.value in roles
    use = admin or rbac.RoleName.VIEWER.value not in roles
    publish = _can_publish(request, resolved)
    try:
        policy = config.get_workspace_policy(resolved)
        profiles = [
            profile for profile in config.configured_profiles()
            if not policy.allowed_profiles or
            profile.name in policy.allowed_profiles
        ]
        active = {
            revision.profile: revision
            for revision in topology_state.list_active_profile_revisions(
                resolved, tuple(profile.name for profile in profiles))
        }
        selected, _ = config.resolve_profile_name(None, resolved)
        bindings = (sorted(
            name for name, binding in config.access_bindings().items()
            if 'source_read' in binding.purposes) if publish else [])
    except (RuntimeError, TypeError, ValueError) as error:
        _api_error(error)
    distributions: list[api_models.DistributionCapabilityView] = []
    for profile in profiles:
        targets: list[api_models.DistributionTargetView] = []
        for target in (profile.canonical,) + profile.targets:
            targets.append(
                api_models.DistributionTargetView(
                    name=target.name,
                    region=target.region,
                    canonical=target is profile.canonical,
                    runtime_backends=sorted(
                        backend for backend, _ in target.runtime_pull),
                    runtime_ids={
                        backend: list(
                            qualification.runtime_ids(
                                target, backend, profile.bindings[binding_id])
                        ) for backend, binding_id in target.runtime_pull
                    }))
        active_revision = active.get(profile.name)
        distributions.append(
            api_models.DistributionCapabilityView(
                name=profile.name,
                revision=profile.revision,
                active=(active_revision is not None and
                        active_revision.revision == profile.revision and
                        active_revision.config_hash == profile.config_hash),
                targets=targets))
    return api_models.CapabilitiesView(workspace=resolved,
                                       read=True,
                                       use=use,
                                       publish=publish,
                                       admin=admin,
                                       workspace_mode=policy.mode.value,
                                       default_distribution=selected,
                                       source_bindings=bindings,
                                       distributions=distributions)


@router.get('/catalog', response_model=api_models.Page)
def list_catalog(request: fastapi.Request,
                 workspace: str | None = None,
                 limit: int = 50,
                 cursor: str | None = None,
                 release: str | None = None,
                 digest: str | None = None,
                 source_ref: str | None = None,
                 distribution: str | None = None,
                 target: str | None = None,
                 state: str | None = None) -> api_models.Page:
    resolved = _resolve_workspace(request, workspace)
    limit = _limit(limit)
    try:
        release = (models.validate_release_label(release, 'Release filter')
                   if release is not None else None)
        digest = (models.validate_sha256_digest(digest, 'Digest filter')
                  if digest is not None else None)
        source_ref = (models.validate_oci_reference(source_ref, 'Source filter')
                      if source_ref is not None else None)
        distribution = (models.validate_control_plane_identifier(
            distribution, 'Distribution filter')
                        if distribution is not None else None)
        target = (models.validate_control_plane_identifier(
            target, 'Target filter') if target is not None else None)
        location_state = (models.ImageLocationState(state)
                          if state is not None else None)
    except ValueError:
        raise fastapi.HTTPException(status_code=422,
                                    detail={'code': 'INVALID_IMAGE_FILTER'
                                           }) from None
    filters: dict[str, Any] = {
        'release': release,
        'digest': digest,
        'source_ref': source_ref,
        'distribution': distribution,
        'target': target,
        'state': state,
    }
    after = _after(cursor, scope='catalog', workspace=resolved, filters=filters)
    try:
        records = catalog_state.list_artifacts(resolved,
                                               limit=limit + 1,
                                               after=after,
                                               release=release,
                                               runtime_digest=digest,
                                               source_ref=source_ref,
                                               distribution=distribution,
                                               target_id=target,
                                               location_state=location_state)
        summaries = catalog_state.catalog_summaries(
            {record.id for record in records}, resolved)
    except (RuntimeError, ValueError) as error:
        _api_error(error)
    return _page(records,
                 limit=limit,
                 scope='catalog',
                 workspace=resolved,
                 filters=filters,
                 key=lambda item: (item.created_at, item.id),
                 view=lambda item: api_models.CatalogArtifactView.from_summary(
                     item, summaries[item.id]))


@router.get('/publications', response_model=api_models.Page)
def list_workspace_publications(
    request: fastapi.Request,
    workspace: str | None = None,
    state: str | None = None,
    release: str | None = None,
    limit: int = 50,
    cursor: str | None = None,
) -> api_models.Page:
    resolved = _resolve_workspace(request, workspace)
    limit = _limit(limit)
    try:
        publication_state = (models.ImagePublicationState(state)
                             if state is not None else None)
        release = (models.validate_release_label(release, 'Release filter')
                   if release is not None else None)
    except ValueError:
        raise fastapi.HTTPException(status_code=422,
                                    detail={'code': 'INVALID_IMAGE_FILTER'
                                           }) from None
    filters = {'state': state, 'release': release}
    after = _after(cursor,
                   scope='workspace_publications',
                   workspace=resolved,
                   filters=filters)
    records = catalog_state.list_workspace_publications(resolved,
                                                        limit=limit + 1,
                                                        after=after,
                                                        state=publication_state,
                                                        release=release)
    return _page(records,
                 limit=limit,
                 scope='workspace_publications',
                 workspace=resolved,
                 filters=filters,
                 key=lambda item: (item.created_at, item.id),
                 view=api_models.PublicationView.from_record)


@router.get('/artifacts/{image_id}')
def get_artifact(image_id: str,
                 request: fastapi.Request,
                 workspace: str | None = None) -> dict[str, Any]:
    resolved = _resolve_workspace(request, workspace)
    try:
        artifact = _artifact(image_id, resolved)
        return {
            'version': 1,
            'artifact': api_models.ArtifactView.from_record(artifact),
        }
    except (RuntimeError, ValueError) as error:
        _api_error(error)


def _artifact_collection_context(
    request: fastapi.Request,
    image_id: str,
    workspace: str | None,
    limit: int,
    cursor: str | None,
    scope: str,
) -> tuple[str, int, tuple[int, str] | None, dict[str, Any]]:
    resolved = _resolve_workspace(request, workspace)
    _artifact(image_id, resolved)
    limit = _limit(limit)
    filters = {'image_id': image_id}
    return (resolved, limit,
            _after(cursor, scope=scope, workspace=resolved,
                   filters=filters), filters)


@router.get('/artifacts/{image_id}/releases', response_model=api_models.Page)
def list_releases(image_id: str,
                  request: fastapi.Request,
                  workspace: str | None = None,
                  limit: int = 50,
                  cursor: str | None = None) -> api_models.Page:
    resolved, limit, after, filters = _artifact_collection_context(
        request, image_id, workspace, limit, cursor, 'releases')
    records = catalog_state.list_releases(image_id,
                                          resolved,
                                          limit=limit + 1,
                                          after=after)
    return _page(records,
                 limit=limit,
                 scope='releases',
                 workspace=resolved,
                 filters=filters,
                 key=lambda item: (item.updated_at, item.id),
                 view=api_models.ReleaseView.from_record)


@router.get('/artifacts/{image_id}/sources', response_model=api_models.Page)
def list_sources(image_id: str,
                 request: fastapi.Request,
                 workspace: str | None = None,
                 limit: int = 50,
                 cursor: str | None = None) -> api_models.Page:
    resolved, limit, after, filters = _artifact_collection_context(
        request, image_id, workspace, limit, cursor, 'sources')
    records = catalog_state.list_sources(image_id,
                                         resolved,
                                         limit=limit + 1,
                                         after=after)
    return _page(records,
                 limit=limit,
                 scope='sources',
                 workspace=resolved,
                 filters=filters,
                 key=lambda item: (item.created_at, item.id),
                 view=api_models.SourceView.from_record)


@router.get('/artifacts/{image_id}/publications',
            response_model=api_models.Page)
def list_publications(image_id: str,
                      request: fastapi.Request,
                      workspace: str | None = None,
                      limit: int = 50,
                      cursor: str | None = None) -> api_models.Page:
    resolved, limit, after, filters = _artifact_collection_context(
        request, image_id, workspace, limit, cursor, 'publications')
    records = catalog_state.list_publications(image_id,
                                              resolved,
                                              limit=limit + 1,
                                              after=after)
    return _page(records,
                 limit=limit,
                 scope='publications',
                 workspace=resolved,
                 filters=filters,
                 key=lambda item: (item.created_at, item.id),
                 view=api_models.PublicationView.from_record)


@router.get('/artifacts/{image_id}/locations', response_model=api_models.Page)
def list_locations(image_id: str,
                   request: fastapi.Request,
                   workspace: str | None = None,
                   limit: int = 50,
                   cursor: str | None = None) -> api_models.Page:
    resolved, limit, after, filters = _artifact_collection_context(
        request, image_id, workspace, limit, cursor, 'locations')
    records = topology_state.list_locations(image_id,
                                            resolved,
                                            limit=limit + 1,
                                            after=after)
    shards = topology_state.get_shards({record.shard_id for record in records})

    def view(record: topology_state.LocationRecord) -> api_models.LocationView:
        shard = shards.get(record.shard_id)
        if shard is None:
            _raise_code('IMAGE_CATALOG_UNAVAILABLE')
        return api_models.LocationView.from_record(record, shard.target_id,
                                                   shard.profile)

    return _page(records,
                 limit=limit,
                 scope='locations',
                 workspace=resolved,
                 filters=filters,
                 key=lambda item: (item.created_at, item.id),
                 view=view)


@router.get('/artifacts/{image_id}/demands', response_model=api_models.Page)
def list_demands(image_id: str,
                 request: fastapi.Request,
                 workspace: str | None = None,
                 limit: int = 50,
                 cursor: str | None = None) -> api_models.Page:
    resolved, limit, after, filters = _artifact_collection_context(
        request, image_id, workspace, limit, cursor, 'demands')
    records = demand_state.list_demands(image_id,
                                        resolved,
                                        limit=limit + 1,
                                        after=after)
    return _page(records,
                 limit=limit,
                 scope='demands',
                 workspace=resolved,
                 filters=filters,
                 key=lambda item: (item.created_at, item.id),
                 view=api_models.DemandView.from_record)


@router.get('/operations/{operation_id}',
            response_model=api_models.OperationView)
def get_operation(operation_id: str,
                  request: fastapi.Request,
                  workspace: str | None = None) -> api_models.OperationView:
    resolved = _resolve_workspace(request, workspace)
    try:
        operation_id = models.validate_catalog_id(operation_id,
                                                  'Image operation ID')
        operation = catalog_state.get_operation(operation_id, resolved)
    except (RuntimeError, ValueError) as error:
        _api_error(error)
    if operation is None:
        _raise_code('IMAGE_OPERATION_NOT_FOUND')
    return api_models.OperationView.from_record(operation)


@router.get('/profiles', response_model=api_models.Page)
def list_profiles(request: fastapi.Request,
                  workspace: str | None = None,
                  limit: int = 50,
                  cursor: str | None = None) -> api_models.Page:
    resolved = _resolve_workspace(request, workspace)
    limit = _limit(limit)
    filters: dict[str, Any] = {}
    after = _after(cursor,
                   scope='profiles',
                   workspace=resolved,
                   filters=filters)
    try:
        records = topology_state.list_profile_revision_history(resolved,
                                                               limit=limit + 1,
                                                               after=after)
    except RuntimeError as error:
        _api_error(error)
    return _page(records,
                 limit=limit,
                 scope='profiles',
                 workspace=resolved,
                 filters=filters,
                 key=lambda item: (item.created_at, item.id),
                 view=api_models.ProfileView.from_record)


@router.get('/workers', response_model=api_models.Page)
def list_workers(request: fastapi.Request,
                 workspace: str | None = None,
                 limit: int = 50,
                 cursor: str | None = None) -> api_models.Page:
    _require_admin(request)
    resolved = _resolve_workspace(request, workspace)
    limit = _limit(limit)
    filters: dict[str, Any] = {}
    after = _after(cursor, scope='workers', workspace=resolved, filters=filters)
    records = topology_state.list_workers(limit=limit + 1, after=after)
    return _page(records,
                 limit=limit,
                 scope='workers',
                 workspace=resolved,
                 filters=filters,
                 key=lambda item: (item.heartbeat_at, item.id),
                 view=api_models.WorkerView.from_record)


@router.get('/readiness', response_model=api_models.ReadinessView)
def readiness(request: fastapi.Request,
              workspace: str | None = None) -> api_models.ReadinessView:
    _require_admin(request)
    resolved = _resolve_workspace(request, workspace)
    try:
        policy = config.get_workspace_policy(resolved)
        profile_records = topology_state.list_operational_profile_revisions(
            resolved, limit=1001)
        shard_records = topology_state.list_shards(resolved, limit=1001)
        workers = topology_state.list_workers(limit=101)
        provider_budgets = topology_state.list_provider_budgets(limit=1001)
        authority = catalog_state.get_catalog_authority_id()
        assert authority is not None
    except (RuntimeError, ValueError) as error:
        _api_error(error)
    shards_truncated = len(shard_records) > 1000
    shards = shard_records[:1000]
    queue_shards = shard_records
    if shards_truncated:
        boundary = (shard_records[-1].profile, shard_records[-1].target_id,
                    shard_records[-1].account, shard_records[-1].region)
        queue_shards = [
            shard for shard in shard_records
            if (shard.profile, shard.target_id, shard.account,
                shard.region) != boundary
        ]
    try:
        queues, queue_groups_truncated = (
            topology_state.readiness_queue_stats(queue_shards))
    except (RuntimeError, ValueError) as error:
        _api_error(error)
    queues_truncated = shards_truncated or queue_groups_truncated
    profiles_truncated = len(profile_records) > 1000
    profiles = profile_records[:1000]
    if profiles_truncated:
        # A profile can have both an ACTIVE and QUALIFYING revision. Exclude
        # the boundary profile rather than returning half of its current state.
        boundary_profile = profile_records[-1].profile
        profiles = [
            profile for profile in profiles
            if profile.profile != boundary_profile
        ]
    workers_truncated = len(workers) > 100
    workers = workers[:100]
    provider_budgets_truncated = len(provider_budgets) > 1000
    provider_budgets = provider_budgets[:1000]
    budgets_by_target = {
        (budget.account, budget.region): budget
        for budget in provider_budgets
        if budget.provider == 'aws' and budget.api_family == 'ecr'
    }
    for queue in queues:
        budget = budgets_by_target.get((queue['account'], queue['region']))
        rate = (budget.applied_rate_milli /
                1000 if budget is not None else None)
        queue['quota_rate_per_second'] = rate
        queue['quota_blocked_until'] = (budget.blocked_until
                                        if budget is not None else None)
        queue['quota_bound_eta_seconds'] = (math.ceil(
            queue['queue_depth'] /
            rate) if rate is not None and rate > 0 else None)
        queue['quota_bound_eta_at_least'] = queue['queue_depth_at_least']
    authority_base32 = base64.b32encode(
        uuid.UUID(authority).bytes).decode().lower().rstrip('=')
    return api_models.ReadinessView(
        catalog_authority=authority,
        catalog_authority_base32=authority_base32,
        workspace=resolved,
        workspace_policy={
            'mode': policy.mode.value,
            'default_profile': policy.default_profile,
            'allowed_profiles': list(policy.allowed_profiles),
            'locality': policy.locality.value,
            'regional_cache_retention_weeks':
                (policy.regional_cache_retention_weeks),
        },
        profiles=[
            api_models.ProfileView.from_record(item) for item in profiles
        ],
        profiles_truncated=profiles_truncated,
        shards=[{
            key: (value.value if hasattr(value, 'value') else value)
            for key, value in dataclasses.asdict(item).items()
            if key not in ('inventory_cursor', 'inventory_lease_token')
        }
                for item in shards],
        shards_truncated=shards_truncated,
        workers=[api_models.WorkerView.from_record(item) for item in workers],
        workers_truncated=workers_truncated,
        provider_budgets=[
            api_models.ProviderBudgetView.from_record(item)
            for item in provider_budgets
        ],
        provider_budgets_truncated=provider_budgets_truncated,
        queues=queues,
        queues_truncated=queues_truncated,
        generated_at=int(time.time()))
