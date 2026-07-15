"""Python SDK for managed container image operations."""

import json
from typing import Any

from sky.container_images import models
from sky.schemas.api import responses
from sky.server import common as server_common
from sky.server import constants as server_constants
from sky.server import versions
from sky.server.requests import payloads
from sky.usage import usage_lib
from sky.utils import annotations
from sky.utils import context


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def publish(
    image: str | dict[str, str],
    workspace: str | None = None,
) -> server_common.RequestId[responses.ContainerImageRecord]:
    """Publishes a digest-pinned source and optional immutable release."""
    body = payloads.ImagePublishBody(image=image, workspace=workspace)
    response = server_common.make_authenticated_request(
        'POST', '/images/publish', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def register(
    image: str | dict[str, str],
    workspace: str | None = None,
) -> server_common.RequestId[responses.ContainerImageRecord]:
    """Compatibility alias for :func:`publish`."""
    body = payloads.ImagePublishBody(image=image, workspace=workspace)
    response = server_common.make_authenticated_request(
        'POST', '/images/register', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def status(
    image: str | None = None,
    workspace: str | None = None,
) -> server_common.RequestId[list[responses.ContainerImageRecord]]:
    """Lists image catalog and preparation state."""
    params: dict[str, Any] = {}
    if image is not None:
        image = models.validate_operational_image_selector(image)
        params['image'] = image
    if workspace is not None:
        workspace = models.validate_workspace_name(workspace,
                                                   'Container image workspace')
        params['workspace'] = workspace
    response = server_common.make_authenticated_request('GET',
                                                        '/images',
                                                        params=params)
    return server_common.get_request_id(response)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def prepare(
    image: str | dict[str, str],
    targets: list[str],
    workspace: str | None = None,
    distribution: str | None = None,
) -> server_common.RequestId[responses.ContainerImageRecord]:
    """Creates durable preparation intents for explicit registry targets."""
    body = payloads.ImagePrepareBody(image=image,
                                     targets=targets,
                                     distribution=distribution,
                                     workspace=workspace)
    response = server_common.make_authenticated_request(
        'POST', '/images/prepare', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@context.contextual
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
@versions.minimal_api_version(server_constants.MIN_CONTAINER_IMAGES_API_VERSION)
def retry(
    image: str,
    target: str,
    workspace: str | None = None,
    distribution: str | None = None,
) -> server_common.RequestId[responses.ContainerImageRecord]:
    """Retries a failed or missing preparation target."""
    body = payloads.ImageRetryBody(image=image,
                                   target=target,
                                   distribution=distribution,
                                   workspace=workspace)
    response = server_common.make_authenticated_request(
        'POST', '/images/retry', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)
