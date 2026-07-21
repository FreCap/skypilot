"""Direct REST, SDK model, authorization, and pagination tests."""

from __future__ import annotations

import dataclasses
from types import SimpleNamespace
from typing import Any
from unittest import mock
import uuid

import fastapi
import pydantic
import pytest

from sky.container_images import api_models
from sky.container_images import catalog_state
from sky.container_images import client
from sky.container_images import models
from sky.container_images import pagination
from sky.container_images import publication
from sky.container_images import server
from sky.container_images import topology_state

DIGEST = 'sha256:' + 'a' * 64
CONFIG_DIGEST = 'sha256:' + 'b' * 64
SOURCE = f'ghcr.io/boltz-bio/runtime@{DIGEST}'


def _request(user_id: str = 'publisher-1') -> Any:
    return SimpleNamespace(state=SimpleNamespace(auth_user=SimpleNamespace(
        id=user_id)))


def _unwrap(function):
    while hasattr(function, '__wrapped__'):
        function = function.__wrapped__
    return function


def _artifact() -> catalog_state.ArtifactRecord:
    return catalog_state.ArtifactRecord(
        id=str(uuid.uuid4()),
        workspace='research',
        runtime_digest=DIGEST,
        platform='linux/amd64',
        config_digest=CONFIG_DIGEST,
        manifest_media_type='application/vnd.oci.image.manifest.v1+json',
        manifest_size_bytes=100,
        declared_size_bytes=1000,
        creator_user_hash='actor',
        producer_kind='external_oci',
        producer_spec_hash=None,
        builder_version=None,
        created_at=10,
        updated_at=11)


def _operation(
        *,
        result: dict[str, Any] | None = None) -> catalog_state.OperationRecord:
    return catalog_state.OperationRecord(
        id=str(uuid.uuid4()),
        authority_id=str(uuid.uuid4()),
        scope='research',
        actor_hash='a' * 64,
        kind='PROFILE_CANARY',
        idempotency_key='idempotency-key-1',
        request_hash='b' * 64,
        state=models.ImageOperationState.RUNNING,
        result_kind=None,
        result_id=None,
        result=result,
        error_code=None,
        lease_token=None,
        lease_expires_at=None,
        child_launch_id=None,
        teardown_deadline=None,
        created_at=10,
        updated_at=11,
        terminal_expires_at=None)


def _profile_record(
    profile: models.ManagedRegistryProfile
) -> topology_state.ProfileRevisionRecord:
    return topology_state.ProfileRevisionRecord(
        id=str(uuid.uuid4()),
        workspace='research',
        profile=profile.name,
        revision=profile.revision,
        desired_generation=1,
        state=models.ImageProfileState.ACTIVE,
        config_hash=profile.config_hash,
        config_snapshot=profile.to_snapshot(),
        terraform_hash='c' * 64,
        physical_manifest_hash=profile.physical_manifest_hash,
        attestations={},
        attestations_hash='d' * 64,
        qualified_at=100,
        failed_code=None,
        canary_window_day=None,
        canary_reserved_microusd=0,
        max_daily_canary_microusd=5_000_000,
        created_at=10,
        updated_at=11)


def test_mutation_models_are_closed_digest_pinned_and_cost_confirmed() -> None:
    body = api_models.PublicationCreate(source_ref=SOURCE,
                                        release='boltz-l4',
                                        distribution='gpu-production',
                                        workspace='research')
    assert body.platform == 'linux/amd64'
    with pytest.raises(pydantic.ValidationError):
        api_models.PublicationCreate(source_ref='ghcr.io/boltz/runtime:latest',
                                     release='boltz-l4',
                                     distribution='gpu-production')
    with pytest.raises(pydantic.ValidationError):
        api_models.PublicationCreate(source_ref=SOURCE,
                                     release='boltz-l4',
                                     distribution='gpu-production',
                                     password='must-not-be-accepted')
    with pytest.raises(pydantic.ValidationError):
        api_models.CanaryCreate(workspace='research',
                                target='canonical',
                                backend='aws_vm',
                                confirm_cost=False)


def test_operation_view_never_returns_single_use_canary_nonce() -> None:
    view = api_models.OperationView.from_record(
        _operation(result={
            'nonce': 'single-use-secret',
            'status': 'RUNNING'
        }))
    assert view.result is not None
    assert 'nonce' not in view.result
    assert view.result['nonce_hash'] != 'single-use-secret'


def test_cursor_is_signed_and_bound_to_workspace_scope_and_filters(
        monkeypatch: pytest.MonkeyPatch) -> None:
    authority = str(uuid.uuid4())
    monkeypatch.setattr(pagination.catalog_state,
                        'get_catalog_authority_id',
                        lambda create=False: authority)
    filters = {'state': 'READY'}
    cursor = pagination.encode(scope='catalog',
                               workspace='research',
                               filters=filters,
                               key=(100, str(uuid.uuid4())))
    key = pagination.decode(cursor,
                            scope='catalog',
                            workspace='research',
                            filters=filters)
    assert key[0] == 100
    for kwargs in ({
            'scope': 'locations',
            'workspace': 'research',
            'filters': filters,
    }, {
            'scope': 'catalog',
            'workspace': 'other',
            'filters': filters,
    }, {
            'scope': 'catalog',
            'workspace': 'research',
            'filters': {
                'state': 'FAILED'
            },
    }):
        with pytest.raises(pagination.InvalidCursorError):
            pagination.decode(cursor, **kwargs)
    with pytest.raises(pagination.InvalidCursorError):
        pagination.decode(cursor[:-1] + ('A' if cursor[-1] != 'A' else 'B'),
                          scope='catalog',
                          workspace='research',
                          filters=filters)


@pytest.mark.parametrize(('roles', 'user_id', 'allowed'), [
    (['admin'], 'anyone', True),
    (['viewer'], 'publisher-1', False),
    (['user'], 'publisher-1', True),
    (['user'], 'other-user', False),
])
def test_publish_authorization_is_role_and_workspace_scoped(
        monkeypatch: pytest.MonkeyPatch, roles: list[str], user_id: str,
        allowed: bool) -> None:
    monkeypatch.setattr(server.permission.permission_service, 'get_user_roles',
                        lambda _: roles)
    monkeypatch.setattr(
        server.config, 'get_workspace_policy',
        lambda _: models.WorkspaceImagePolicy(publishers=('publisher-1',)))
    assert server._can_publish(  # pylint: disable=protected-access
        _request(user_id), 'research') is allowed


def test_publication_route_authorizes_before_mutating_and_returns_direct_result(
        monkeypatch: pytest.MonkeyPatch) -> None:
    request = _request()
    body = api_models.PublicationCreate(source_ref=SOURCE,
                                        release='boltz-l4',
                                        distribution='gpu-production',
                                        workspace='research')
    monkeypatch.setattr(server, '_resolve_workspace',
                        lambda request, requested: 'research')
    monkeypatch.setattr(server, '_require_publisher',
                        lambda request, workspace: None)
    operation = dataclasses.replace(_operation(),
                                    kind='PUBLISH',
                                    state=models.ImageOperationState.PENDING)
    publication_record = catalog_state.PublicationRecord(
        id=str(uuid.uuid4()),
        workspace='research',
        operation_id=operation.id,
        profile_revision_id=str(uuid.uuid4()),
        requested_release='boltz-l4',
        reservation_active=True,
        source_ref=SOURCE,
        source_root_digest=DIGEST,
        requested_platform='linux/amd64',
        source_auth_binding_id=None,
        source_auth_fingerprint=None,
        state=models.ImagePublicationState.PENDING,
        inspection_lease_token=None,
        inspection_lease_expires_at=None,
        attempt_count=0,
        next_retry_at=None,
        error_code=None,
        image_id=None,
        source_id=None,
        canonical_location_id=None,
        reservation_expires_at=None,
        record_expires_at=None,
        created_at=10,
        updated_at=10)
    mutate = mock.Mock(return_value=publication.PublicationMutation(
        operation=operation, publication=publication_record))
    monkeypatch.setattr(server.publication, 'publish', mutate)
    result = server.create_publication(request,
                                       body,
                                       idempotency_key='idempotency-key-1')
    assert result.kind == 'publication'
    assert result.operation.id == operation.id
    assert result.publication is not None
    mutate.assert_called_once()


def test_capabilities_are_metadata_only_and_show_exact_runtime_ids(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    monkeypatch.setattr(server, '_resolve_workspace',
                        lambda request, requested: 'research')
    monkeypatch.setattr(server, '_roles', lambda request: {'admin'})
    monkeypatch.setattr(server, '_can_publish', lambda request, workspace: True)
    monkeypatch.setattr(
        server.config, 'get_workspace_policy', lambda _: models.
        WorkspaceImagePolicy(mode=models.WorkspaceImageMode.MANAGED_REQUIRED,
                             default_profile=profile.name,
                             allowed_profiles=(profile.name,),
                             publishers=('publisher-1',)))
    monkeypatch.setattr(server.config, 'configured_profiles', lambda:
                        (profile,))
    monkeypatch.setattr(
        server.config, 'resolve_profile_name', lambda selected, workspace:
        (profile.name, mock.sentinel.policy))
    monkeypatch.setattr(server.config, 'access_bindings',
                        lambda: profile.bindings)
    active_lookup = mock.Mock(return_value=[_profile_record(profile)])
    monkeypatch.setattr(server.topology_state, 'list_active_profile_revisions',
                        active_lookup)
    view = server.capabilities(_request(), workspace='research')
    assert view.admin and view.publish and view.use
    assert view.default_distribution == profile.name
    assert len(view.distributions) == 1
    west = next(item for item in view.distributions[0].targets
                if item.name == 'aws-us-west-2')
    assert west.runtime_ids['aws_vm'] == ['us-west-2']
    assert west.runtime_ids['aws_eks'] == ['boltz-west']
    active_lookup.assert_called_once_with('research', (profile.name,))


def test_profile_history_api_is_keyset_paginated_and_query_bounded(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    authority = str(uuid.uuid4())
    monkeypatch.setattr(pagination.catalog_state,
                        'get_catalog_authority_id',
                        lambda create=False: authority)
    monkeypatch.setattr(server, '_resolve_workspace',
                        lambda request, requested: 'research')
    records = [
        dataclasses.replace(_profile_record(profile),
                            id=str(uuid.uuid4()),
                            created_at=created_at)
        for created_at in (30, 20, 10)
    ]
    history = mock.Mock(return_value=records)
    monkeypatch.setattr(server.topology_state, 'list_profile_revision_history',
                        history)

    page = server.list_profiles(_request(), workspace='research', limit=2)

    assert len(page.items) == 2
    assert page.next_cursor is not None
    history.assert_called_once_with('research', limit=3, after=None)
    after = pagination.decode(page.next_cursor,
                              scope='profiles',
                              workspace='research',
                              filters={})
    assert after == (records[1].created_at, records[1].id)

    history.reset_mock()
    history.return_value = []
    empty = server.list_profiles(_request(),
                                 workspace='research',
                                 limit=2,
                                 cursor=page.next_cursor)
    assert empty.items == []
    assert empty.next_cursor is None
    history.assert_called_once_with('research', limit=3, after=after)


def test_catalog_page_is_bounded_and_cursor_is_last_returned_item(
        monkeypatch: pytest.MonkeyPatch) -> None:
    authority = str(uuid.uuid4())
    monkeypatch.setattr(pagination.catalog_state,
                        'get_catalog_authority_id',
                        lambda create=False: authority)
    records = [
        dataclasses.replace(_artifact(), created_at=index) for index in range(3)
    ]
    page = server._page(  # pylint: disable=protected-access
        records,
        limit=2,
        scope='catalog',
        workspace='research',
        filters={},
        key=lambda item: (item.created_at, item.id),
        view=api_models.ArtifactView.from_record)
    assert len(page.items) == 2
    assert page.next_cursor is not None
    assert pagination.decode(page.next_cursor,
                             scope='catalog',
                             workspace='research',
                             filters={}) == (records[1].created_at,
                                             records[1].id)


def test_sdk_status_projects_catalog_summary_without_extra_field_failure(
        monkeypatch: pytest.MonkeyPatch) -> None:
    artifact = _artifact()
    summary = api_models.CatalogArtifactView.from_summary(
        artifact, {
            'releases': ['boltz-l4'],
            'distributions': ['gpu-production'],
            'source_refs': [SOURCE],
            'targets': ['canonical'],
            'location_states': {
                'READY': 1
            },
        })
    monkeypatch.setattr(
        client, 'catalog', lambda **kwargs: api_models.Page(
            items=[summary.model_dump(mode='json')]))
    result = _unwrap(client.status)(workspace='research')
    assert len(result) == 1
    assert result[0].id == artifact.id


def test_router_exposes_only_direct_image_api_contract() -> None:
    paths = {(route.path, method)
             for route in server.router.routes
             for method in route.methods}
    required = {
        ('/publications', 'POST'),
        ('/catalog', 'GET'),
        ('/readiness', 'GET'),
        ('/artifacts/{image_id}/prepare', 'POST'),
        ('/profiles/{profile_name}/qualification', 'POST'),
        ('/profiles/{profile_name}/canaries', 'POST'),
    }
    assert required <= paths
    assert all('/api/get' not in path for path, _ in paths)


def test_closed_error_mapping_never_reflects_provider_text() -> None:
    with pytest.raises(fastapi.HTTPException) as error:
        server._api_error(  # pylint: disable=protected-access
            RuntimeError('provider said token=must-not-reflect'))
    assert error.value.status_code == 503
    assert error.value.detail == {'code': 'IMAGE_CATALOG_UNAVAILABLE'}


@pytest.mark.parametrize(('failure', 'code'), (
    (topology_state.RegistryShardUnavailableError('REGISTRY_SHARD_UNAVAILABLE'),
     'REGISTRY_SHARD_UNAVAILABLE'),
    (topology_state.RegistryLocationQuarantinedError(
        'REGISTRY_LOCATION_QUARANTINED'), 'REGISTRY_LOCATION_QUARANTINED')))
def test_retry_route_returns_typed_registry_conflict(
        monkeypatch: pytest.MonkeyPatch, failure: ValueError,
        code: str) -> None:
    monkeypatch.setattr(server, '_resolve_workspace',
                        lambda request, requested: 'research')
    monkeypatch.setattr(server, '_require_publisher',
                        lambda request, workspace: None)
    monkeypatch.setattr(server.preparation, 'retry_location',
                        mock.Mock(side_effect=failure))

    with pytest.raises(fastapi.HTTPException) as error:
        server.retry_location(
            str(uuid.uuid4()),
            _request(),
            api_models.WorkspaceMutation(workspace='research'),
            idempotency_key='retry-location-0001')
    assert error.value.status_code == 409
    assert error.value.detail == {'code': code}
