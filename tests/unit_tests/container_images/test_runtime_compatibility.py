"""Runtime placement, demand attribution, and wire compatibility tests."""
# pylint: disable=protected-access

from __future__ import annotations

import dataclasses
import pickle
from typing import Any
from unittest import mock

import pytest

import sky
from sky import exceptions
from sky.backends import cloud_vm_ray_backend
from sky.client import sdk
from sky.container_images import catalog_state
from sky.container_images import demand_state
from sky.container_images import models
from sky.container_images import runtime
from sky.container_images import topology_state
from sky.serve import constants as serve_constants
from sky.serve import serve_utils
from sky.server import versions
from sky.server.requests import payloads
from sky.utils import dag_utils

DIGEST = 'sha256:' + 'a' * 64
CONFIG_DIGEST = 'sha256:' + 'b' * 64
SOURCE = f'ghcr.io/boltz-bio/runtime@{DIGEST}'
_ARTIFACT_ID = '00000000-0000-4000-8000-000000000001'
_PUBLICATION_ID = '00000000-0000-4000-8000-000000000002'
_OPERATION_ID = '00000000-0000-4000-8000-000000000003'
_SOURCE_ID = '00000000-0000-4000-8000-000000000004'
_LOCATION_ID = '00000000-0000-4000-8000-000000000005'
_SHARD_ID = '00000000-0000-4000-8000-000000000006'
_REVISION_ID = '00000000-0000-4000-8000-000000000007'
_DEMAND_ID = '00000000-0000-4000-8000-000000000008'
_AUTHORITY_ID = '00000000-0000-4000-8000-000000000009'


class _FakeResources:
    """Small Resources-compatible value used to isolate resolver behavior."""

    def __init__(
        self,
        image: models.ContainerImage,
        *,
        legacy: bool = False,
        image_id: dict[str | None, str] | None = None,
        resolved: models.ResolvedContainerImage | None = None,
        docker_login_config: Any = None,
    ) -> None:
        self.container_image = image
        self.container_image_from_legacy_image_id = legacy
        self.image_id = image_id
        self.resolved_container_image = resolved
        self.docker_login_config = docker_login_config

    def copy(self, **updates: Any) -> '_FakeResources':
        return _FakeResources(
            updates.pop('container_image', self.container_image),
            legacy=updates.pop('_container_image_from_legacy_image_id',
                               self.container_image_from_legacy_image_id),
            image_id=updates.pop('image_id', self.image_id),
            resolved=updates.pop('_resolved_container_image',
                                 self.resolved_container_image),
            docker_login_config=updates.pop('_docker_login_config',
                                            self.docker_login_config),
        )


def _artifact() -> catalog_state.ArtifactRecord:
    return catalog_state.ArtifactRecord(
        id=_ARTIFACT_ID,
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


def _publication() -> catalog_state.PublicationRecord:
    return catalog_state.PublicationRecord(
        id=_PUBLICATION_ID,
        workspace='research',
        operation_id=_OPERATION_ID,
        profile_revision_id=_REVISION_ID,
        requested_release='boltz-l4',
        reservation_active=True,
        source_ref=SOURCE,
        source_root_digest=DIGEST,
        requested_platform='linux/amd64',
        source_auth_binding_id=None,
        source_auth_fingerprint=None,
        state=models.ImagePublicationState.READY,
        inspection_lease_token=None,
        inspection_lease_expires_at=None,
        attempt_count=1,
        next_retry_at=None,
        error_code=None,
        image_id=_ARTIFACT_ID,
        source_id=_SOURCE_ID,
        canonical_location_id='00000000-0000-4000-8000-000000000010',
        reservation_expires_at=None,
        record_expires_at=None,
        created_at=10,
        updated_at=11)


def _active_revision(
    profile: models.ManagedRegistryProfile,
    *,
    observed_at: int | None,
) -> topology_state.ProfileRevisionRecord:
    target = profile.target('aws-us-west-2')
    binding = profile.bindings[target.runtime_binding('aws_vm')]
    attestations: dict[str, Any] = {}
    if observed_at is not None:
        key = models.profile_attestation_key('runtime', target.name, 'aws_vm',
                                             binding.fingerprint, 'us-west-2')
        attestations[key] = {
            'status': 'READY',
            'observed_at': observed_at,
            'target_fingerprint': target.target_fingerprint,
            'binding_fingerprint': binding.fingerprint,
            'backend': 'aws_vm',
            'runtime_id': 'us-west-2',
        }
    return topology_state.ProfileRevisionRecord(
        id=_REVISION_ID,
        workspace='research',
        profile=profile.name,
        revision=profile.revision,
        desired_generation=1,
        state=models.ImageProfileState.ACTIVE,
        config_hash=profile.config_hash,
        config_snapshot=profile.to_snapshot(),
        terraform_hash='c' * 64,
        physical_manifest_hash=profile.physical_manifest_hash,
        attestations=attestations,
        attestations_hash='d' * 64,
        qualified_at=100,
        failed_code=None,
        canary_window_day=None,
        canary_reserved_microusd=0,
        max_daily_canary_microusd=5_000_000,
        created_at=10,
        updated_at=11)


def _location(
        profile: models.ManagedRegistryProfile,
        state: models.ImageLocationState) -> topology_state.LocationRecord:
    target = profile.target('aws-us-west-2')
    return topology_state.LocationRecord(
        id=_LOCATION_ID,
        workspace='research',
        image_id=_ARTIFACT_ID,
        shard_id=_SHARD_ID,
        target_fingerprint=target.target_fingerprint,
        physical_fingerprint='e' * 64,
        runtime_digest=DIGEST,
        canonical=False,
        canonical_location_id='00000000-0000-4000-8000-000000000010',
        target_ref=('123456789012.dkr.ecr.us-west-2.amazonaws.com/'
                    f'skypilot/images/west/000@{DIGEST}'),
        state=state,
        lease_kind=None,
        lease_token=None,
        lease_expires_at=None,
        attempt_count=0,
        next_retry_at=None,
        error_code=None,
        last_verified_at=100
        if state == models.ImageLocationState.READY else None,
        last_used_at=None,
        inventory_epoch_seen=None,
        reserved_declared_bytes=1000,
        created_at=10,
        updated_at=11)


def _demand(
    profile: models.ManagedRegistryProfile,
    state: models.ImageDemandState = models.ImageDemandState.WARMING
) -> demand_state.DemandRecord:
    target = profile.target('aws-us-west-2')
    return demand_state.DemandRecord(
        id=_DEMAND_ID,
        authority_id=_AUTHORITY_ID,
        workspace='research',
        consumer_kind='service_version',
        consumer_owner='boltz:v7',
        consumer_generation=1,
        target_key=f'{_ARTIFACT_ID}:{target.target_fingerprint}',
        owner_epoch=10,
        retry_epoch=0,
        image_id=_ARTIFACT_ID,
        runtime_digest=DIGEST,
        profile_revision_id=_REVISION_ID,
        target_fingerprint=target.target_fingerprint,
        location_id=_LOCATION_ID,
        placement={},
        pull_plan=None,
        state=state,
        error_code=None,
        consumer_attached=False,
        first_terminal_observed_at=None,
        last_terminal_observed_at=None,
        terminal_observation_count=0,
        terminal_at=None,
        expires_at=None,
        created_at=10,
        updated_at=11)


def _wire_metadata(monkeypatch: pytest.MonkeyPatch,
                   profile: models.ManagedRegistryProfile,
                   policy: models.WorkspaceImagePolicy,
                   active: topology_state.ProfileRevisionRecord) -> None:
    monkeypatch.setattr(runtime.config, 'resolve_profile',
                        lambda selected, workspace: (profile, policy))
    monkeypatch.setattr(runtime.topology_state, 'get_active_profile',
                        lambda workspace, name: active)
    monkeypatch.setattr(
        runtime, '_published_identity', lambda image, workspace, platform:
        (_artifact(), _publication()))
    monkeypatch.setattr(runtime.topology_state, 'get_profile_revision',
                        lambda revision_id: active)


def test_ready_resolution_pins_exact_ami_helper_and_one_durable_demand(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    now = 1000
    active = _active_revision(profile, observed_at=now - 1)
    policy = models.WorkspaceImagePolicy(
        mode=models.WorkspaceImageMode.MANAGED_REQUIRED,
        default_profile=profile.name,
        allowed_profiles=(profile.name,),
        locality=models.Locality.PREFER)
    _wire_metadata(monkeypatch, profile, policy, active)
    location = _location(profile, models.ImageLocationState.READY)
    created = _demand(profile)
    create = mock.Mock(return_value=created)
    commit = mock.Mock(return_value=dataclasses.replace(
        created,
        state=models.ImageDemandState.READY,
        pull_plan={'reference': location.target_ref}))
    monkeypatch.setattr(runtime.time, 'time', lambda: now)
    monkeypatch.setattr(runtime.topology_state, 'get_location_for_target',
                        lambda **kwargs: location)
    monkeypatch.setattr(runtime.catalog_state,
                        'get_catalog_authority_id',
                        lambda create=False: _AUTHORITY_ID)
    monkeypatch.setattr(runtime.transactions,
                        'create_warming_demand_for_owner_epoch', create)
    monkeypatch.setattr(runtime.transactions, 'commit_ready_demand', commit)

    resources = _FakeResources(
        models.ContainerImage(release='boltz-l4', distribution=profile.name))
    resolved = runtime.resolve_for_placement(
        resources,
        models.Placement(provider='aws',
                         region='us-west-2',
                         backend='aws_vm',
                         platform='linux/amd64'),
        workspace='research',
        consumer_kind='service_version',
        consumer_owner='boltz:v7',
        owner_epoch_token='service-hash:v7')

    assert resolved.image_id == {'us-west-2': 'ami-0fedcba9876543210'}
    assert resolved.resolved_container_image is not None
    assert resolved.resolved_container_image.reference == location.target_ref
    assert resolved.resolved_container_image.credential_helper == 'ecr-login'
    assert resolved.docker_login_config.credential_helper == 'ecr-login'
    assert resolved.docker_login_config.password == ''
    create.assert_called_once()
    commit.assert_called_once_with(demand_id=_DEMAND_ID,
                                   consumer_generation=1,
                                   pull_plan=mock.ANY)


def test_metadata_filter_is_mutation_free_and_fails_closed_on_stale_binding(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active = _active_revision(profile, observed_at=1)
    policy = models.WorkspaceImagePolicy(
        mode=models.WorkspaceImageMode.MANAGED_REQUIRED,
        default_profile=profile.name,
        allowed_profiles=(profile.name,),
        locality=models.Locality.PREFER)
    _wire_metadata(monkeypatch, profile, policy, active)
    reserve = mock.Mock(side_effect=AssertionError('metadata path mutated'))
    monkeypatch.setattr(runtime.transactions, 'reserve_regional_location',
                        reserve)
    monkeypatch.setattr(runtime.time, 'time', lambda: 100000)
    resources = _FakeResources(
        models.ContainerImage(release='boltz-l4', distribution=profile.name))

    result = runtime.prepare_metadata_only(
        resources,
        models.Placement(provider='aws',
                         region='us-west-2',
                         backend='aws_vm',
                         platform='linux/amd64'), 'research')
    assert result is None
    reserve.assert_not_called()


def test_managed_preferred_stale_route_preserves_direct_digest_path(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active = _active_revision(profile, observed_at=None)
    policy = models.WorkspaceImagePolicy(
        mode=models.WorkspaceImageMode.MANAGED_PREFERRED,
        default_profile=profile.name,
        allowed_profiles=(profile.name,),
        locality=models.Locality.PREFER)
    _wire_metadata(monkeypatch, profile, policy, active)
    resources = _FakeResources(
        models.ContainerImage(ref=SOURCE, distribution=profile.name))
    result = runtime.resolve_for_placement(resources,
                                           models.Placement(
                                               provider='aws',
                                               region='us-west-2',
                                               backend='aws_vm',
                                               platform='linux/amd64'),
                                           workspace='research',
                                           consumer_kind='cluster',
                                           consumer_owner='cluster',
                                           owner_epoch_token='request')
    assert result is resources
    assert result.resolved_container_image is None


def test_exact_host_image_mismatch_is_rejected_before_demand_mutation(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active = _active_revision(profile, observed_at=100)
    policy = models.WorkspaceImagePolicy(
        mode=models.WorkspaceImageMode.MANAGED_REQUIRED,
        default_profile=profile.name,
        allowed_profiles=(profile.name,))
    _wire_metadata(monkeypatch, profile, policy, active)
    mutate = mock.Mock(side_effect=AssertionError('unexpected demand'))
    monkeypatch.setattr(runtime.transactions,
                        'create_warming_demand_for_owner_epoch', mutate)
    monkeypatch.setattr(runtime.time, 'time', lambda: 101)
    resources = _FakeResources(models.ContainerImage(release='boltz-l4',
                                                     distribution=profile.name),
                               image_id={'us-west-2': 'ami-unqualified'})
    assert runtime.prepare_metadata_only(
        resources,
        models.Placement(provider='aws',
                         region='us-west-2',
                         backend='aws_vm',
                         platform='linux/amd64',
                         host_image_id='ami-unqualified'), 'research') is None
    mutate.assert_not_called()


def test_one_thousand_service_replicas_share_one_version_target_owner(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cloud_vm_ray_backend.common_utils,
                        'get_current_request_id', lambda: 'request-id')
    task = sky.Task()
    context = {
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'boltz-l4-fleet',
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'service-hash',
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 7,
    }
    owners = set()
    for replica in range(1000):
        attribution = cloud_vm_ray_backend._get_image_demand_attribution(
            task, f'boltz-l4-fleet-{replica}', 'service', context)
        owners.add((attribution.consumer_kind, attribution.consumer_owner,
                    attribution.owner_epoch_token))
    assert len(owners) == 1
    assert owners == {('service_version', 'boltz-l4-fleet:v7',
                       'service-hash:v7')}


def test_legacy_docker_image_survives_copy_pickle_and_yaml_round_trip() -> None:
    resources = sky.Resources(image_id='docker:ubuntu:22.04')
    copied = pickle.loads(pickle.dumps(resources))
    task = sky.Task()
    task.set_resources(copied)
    dag = dag_utils.convert_entrypoint_to_dag(task)
    assert len(dag.tasks) == 1
    assert copied.container_image_from_legacy_image_id
    assert copied.container_image.ref == 'ubuntu:22.04'
    assert copied.to_yaml_config()['image_id'] == {'docker': 'ubuntu:22.04'}


def test_api_61_rejects_only_new_container_image_syntax(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(versions, 'get_remote_api_version', lambda: 61)
    task = sky.Task()
    task.set_resources(sky.Resources(container_image=SOURCE))
    with pytest.raises(exceptions.APINotSupportedError, match='version 62'):
        sdk._check_container_image_api_support(
            dag_utils.convert_entrypoint_to_dag(task))

    legacy_task = sky.Task()
    legacy_task.set_resources(sky.Resources(image_id='docker:ubuntu:22.04'))
    sdk._check_container_image_api_support(
        dag_utils.convert_entrypoint_to_dag(legacy_task))


def test_new_server_preserves_old_client_task_bytes_for_every_task_route(
) -> None:
    legacy_yaml = ('resources:\n'
                   '  infra: kubernetes\n'
                   '  image_id: ubuntu:22.04\n'
                   'run: echo old-client\n')
    bodies = (
        payloads.LaunchBody(task=legacy_yaml, cluster_name='cluster'),
        payloads.ExecBody(task=legacy_yaml, cluster_name='cluster'),
        payloads.JobsLaunchBody(task=legacy_yaml, name=None),
        payloads.ServeUpBody(task=legacy_yaml, service_name='service'),
        payloads.ServeUpdateBody(task=legacy_yaml,
                                 service_name='service',
                                 mode=serve_utils.UpdateMode.ROLLING),
        payloads.JobsPoolApplyBody(task=legacy_yaml,
                                   workers=1,
                                   pool_name='pool',
                                   mode=serve_utils.UpdateMode.ROLLING),
    )
    assert all(body.task == legacy_yaml for body in bodies)
    assert payloads._serialized_task_uses_container_image(legacy_yaml)


def test_nested_alternatives_are_validated_and_private_pull_plans_rejected(
        profile: models.ManagedRegistryProfile) -> None:
    explicit = ('resources:\n'
                '  container_image:\n'
                f'    release: boltz-l4\n'
                f'    distribution: {profile.name}\n'
                '  any_of:\n'
                '    - infra: aws/us-west-2\n'
                '    - infra: kubernetes/boltz-west\n')
    assert payloads._serialized_task_uses_container_image(explicit)
    forged = ('resources:\n'
              f'  container_image: {SOURCE}\n'
              '  _resolved_container_image:\n'
              f'    reference: attacker.example/model@{DIGEST}\n')
    # Pydantic wraps the domain exception at the API model boundary.
    with pytest.raises(ValueError,
                       match='Invalid managed container image task'):
        payloads.LaunchBody(task=forged, cluster_name='cluster')
