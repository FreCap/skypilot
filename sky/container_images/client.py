"""Python SDK for the direct managed container image API."""

from __future__ import annotations

from collections.abc import Callable
import time
from typing import Any, TypeVar
import uuid

from sky import sky_logging
from sky.container_images import api_models
from sky.container_images import models
from sky.server import common as server_common
from sky.server import constants as server_constants
from sky.server import versions
from sky.usage import usage_lib
from sky.utils import annotations as annotations_lib
from sky.utils import context

_T = TypeVar('_T', bound=api_models._ApiModel)  # pylint: disable=protected-access
logger = sky_logging.init_logger(__name__)


def _request(method: str,
             path: str,
             response_type: type[_T],
             *,
             json: dict[str, Any] | None = None,
             params: dict[str, Any] | None = None,
             idempotency_key: str | None = None) -> _T:
    headers = ({
        'Idempotency-Key': idempotency_key
    } if idempotency_key is not None else None)
    response = server_common.make_authenticated_request(method,
                                                        path,
                                                        json=json,
                                                        params=params,
                                                        headers=headers)
    if response.status_code >= 400:
        try:
            detail = response.json().get('detail', {})
            code = detail.get('code') if isinstance(detail, dict) else None
        except (AttributeError, ValueError):
            code = None
        if isinstance(code, str):
            raise RuntimeError(code)
    server_common.handle_request_error(response)
    return response_type.model_validate(response.json())


def _key(value: str | None) -> str:
    return value or str(uuid.uuid4())


def _wait(submit: Callable[[], api_models.MutationResult],
          initial: api_models.MutationResult, workspace: str | None,
          wait: bool) -> api_models.MutationResult:
    if not wait or initial.operation.state in ('SUCCEEDED', 'FAILED'):
        return initial
    while True:
        operation = get_operation(initial.operation.id, workspace=workspace)
        if operation.state in ('SUCCEEDED', 'FAILED'):
            return submit()
        time.sleep(1)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def publish(source_ref: str,
            release: str,
            distribution: str,
            *,
            workspace: str | None = None,
            platform: str = 'linux/amd64',
            source_auth: str | None = None,
            idempotency_key: str | None = None,
            wait: bool = True) -> api_models.MutationResult:
    """Publishes one digest-pinned source under an immutable release."""
    body = api_models.PublicationCreate(source_ref=source_ref,
                                        release=release,
                                        distribution=distribution,
                                        workspace=workspace,
                                        platform=platform,
                                        source_auth=source_auth)
    key = _key(idempotency_key)

    def submit() -> api_models.MutationResult:
        return _request('POST',
                        '/images/publications',
                        api_models.MutationResult,
                        json=body.model_dump(mode='json'),
                        idempotency_key=key)

    return _wait(submit, submit(), workspace, wait)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def catalog(*,
            workspace: str | None = None,
            limit: int = 50,
            cursor: str | None = None,
            release: str | None = None,
            digest: str | None = None,
            source_ref: str | None = None,
            distribution: str | None = None,
            target: str | None = None,
            state: str | None = None) -> api_models.Page:
    """Returns one synchronous keyset-paginated catalog page."""
    params = {
        key: value for key, value in {
            'workspace': workspace,
            'limit': limit,
            'cursor': cursor,
            'release': release,
            'digest': digest,
            'source_ref': source_ref,
            'distribution': distribution,
            'target': target,
            'state': state,
        }.items() if value is not None
    }
    return _request('GET', '/images/catalog', api_models.Page, params=params)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def publications(*,
                 workspace: str | None = None,
                 state: str | None = None,
                 release: str | None = None,
                 limit: int = 50,
                 cursor: str | None = None) -> api_models.Page:
    params = {
        key: value for key, value in {
            'workspace': workspace,
            'state': state,
            'release': release,
            'limit': limit,
            'cursor': cursor,
        }.items() if value is not None
    }
    return _request('GET',
                    '/images/publications',
                    api_models.Page,
                    params=params)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def locations(image_id: str,
              *,
              workspace: str | None = None,
              limit: int = 100,
              cursor: str | None = None) -> api_models.Page:
    image_id = models.validate_catalog_id(image_id, 'Image artifact ID')
    params = {
        key: value for key, value in {
            'workspace': workspace,
            'limit': limit,
            'cursor': cursor,
        }.items() if value is not None
    }
    return _request('GET',
                    f'/images/artifacts/{image_id}/locations',
                    api_models.Page,
                    params=params)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def status(selector: str | None = None,
           workspace: str | None = None) -> list[api_models.ArtifactView]:
    """Lists catalog artifacts selected by an explicit identity namespace."""
    filters: dict[str, str] = {}
    if selector is not None:
        parsed = models.parse_explicit_image_selector(selector)
        if parsed is None:
            parsed = models.ContainerImage(ref=selector)
        if parsed.artifact_id is not None:
            return [get_artifact(parsed.artifact_id, workspace=workspace)]
        if parsed.release is not None:
            filters['release'] = parsed.release
        elif parsed.ref is not None:
            filters['source_ref'] = parsed.ref
    page = catalog(workspace=workspace, limit=100, **filters)
    if page.next_cursor is not None:
        logger.warning(
            'Container image status is limited to the first 100 artifacts. '
            'Use sky.image.catalog() with its next_cursor or narrow the '
            'selector to inspect the remaining catalog.')
    result: list[api_models.ArtifactView] = []
    for item in page.items:
        summary = api_models.CatalogArtifactView.model_validate(item)
        result.append(
            api_models.ArtifactView.model_validate(summary,
                                                   from_attributes=True))
    return result


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def get_artifact(image_id: str,
                 *,
                 workspace: str | None = None) -> api_models.ArtifactView:
    image_id = models.validate_catalog_id(image_id, 'Image artifact ID')
    params = {'workspace': workspace} if workspace is not None else None
    response = server_common.make_authenticated_request(
        'GET', f'/images/artifacts/{image_id}', params=params)
    server_common.handle_request_error(response)
    return api_models.ArtifactView.model_validate(response.json()['artifact'])


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def prepare(image_id: str,
            distribution: str,
            target: str,
            *,
            workspace: str | None = None,
            idempotency_key: str | None = None,
            wait: bool = True) -> api_models.MutationResult:
    """Prepares one published artifact at one qualified target."""
    image_id = models.validate_catalog_id(image_id, 'Image artifact ID')
    body = api_models.ArtifactPrepare(distribution=distribution,
                                      target=target,
                                      workspace=workspace)
    key = _key(idempotency_key)

    def submit() -> api_models.MutationResult:
        return _request('POST',
                        f'/images/artifacts/{image_id}/prepare',
                        api_models.MutationResult,
                        json=body.model_dump(mode='json'),
                        idempotency_key=key)

    return _wait(submit, submit(), workspace, wait)


def _retry(path: str, *, workspace: str | None, idempotency_key: str | None,
           wait: bool) -> api_models.MutationResult:
    body = api_models.WorkspaceMutation(workspace=workspace)
    key = _key(idempotency_key)

    def submit() -> api_models.MutationResult:
        return _request('POST',
                        path,
                        api_models.MutationResult,
                        json=body.model_dump(mode='json'),
                        idempotency_key=key)

    return _wait(submit, submit(), workspace, wait)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def retry_publication(publication_id: str,
                      *,
                      workspace: str | None = None,
                      idempotency_key: str | None = None,
                      wait: bool = True) -> api_models.MutationResult:
    publication_id = models.validate_catalog_id(publication_id,
                                                'Publication ID')
    return _retry(f'/images/publications/{publication_id}/retry',
                  workspace=workspace,
                  idempotency_key=idempotency_key,
                  wait=wait)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def retry_location(location_id: str,
                   *,
                   workspace: str | None = None,
                   idempotency_key: str | None = None,
                   wait: bool = True) -> api_models.MutationResult:
    location_id = models.validate_catalog_id(location_id, 'Location ID')
    return _retry(f'/images/locations/{location_id}/retry',
                  workspace=workspace,
                  idempotency_key=idempotency_key,
                  wait=wait)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def qualify(profile: str,
            manifest: dict[str, Any],
            *,
            idempotency_key: str | None = None) -> api_models.MutationResult:
    profile = models.validate_control_plane_identifier(profile,
                                                       'Registry profile')
    return _request('POST',
                    f'/images/profiles/{profile}/qualification',
                    api_models.MutationResult,
                    json={'manifest': manifest},
                    idempotency_key=_key(idempotency_key))


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def canary(profile: str,
           target: str,
           backend: str,
           *,
           workspace: str,
           runtime_id: str | None = None,
           idempotency_key: str | None = None,
           wait: bool = True) -> api_models.MutationResult:
    profile = models.validate_control_plane_identifier(profile,
                                                       'Registry profile')
    body = api_models.CanaryCreate(workspace=workspace,
                                   target=target,
                                   backend=backend,
                                   runtime_id=runtime_id,
                                   confirm_cost=True)
    key = _key(idempotency_key)

    def submit() -> api_models.MutationResult:
        return _request('POST',
                        f'/images/profiles/{profile}/canaries',
                        api_models.MutationResult,
                        json=body.model_dump(mode='json'),
                        idempotency_key=key)

    return _wait(submit, submit(), workspace, wait)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations_lib.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def get_operation(operation_id: str,
                  *,
                  workspace: str | None = None) -> api_models.OperationView:
    operation_id = models.validate_catalog_id(operation_id,
                                              'Image operation ID')
    params = {'workspace': workspace} if workspace is not None else None
    return _request('GET',
                    f'/images/operations/{operation_id}',
                    api_models.OperationView,
                    params=params)
