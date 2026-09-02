"""Tests for server-owned SkyServe system-OOM recovery profiles."""

import json

import pytest

import sky
from sky import clouds
from sky import container_images
from sky import skypilot_config
from sky.serve import constants
from sky.serve import system_oom_recovery
from sky.skylet import system_oom_recovery as runtime_recovery

_IMAGE_DIGEST = 'sha256:' + 'a' * 64
_PINNED_IMAGE = f'example.invalid/model@{_IMAGE_DIGEST}'
_SERVICE_NAME = 'boltz-l4-fleet'
_SERVICE_HASH = 'incarnation-123'
_CONTROLLER_INCARNATION = '33333333-3333-4333-8333-333333333333'


def _task(run: str | None = None) -> sky.Task:
    if run is None:
        run = _owned_spec().render()
    task = sky.Task(run=run, envs={'MODEL': 'boltz'})
    task.set_resources(sky.Resources(instance_type='g2-standard-4'))
    return task


def _owned_spec() -> runtime_recovery.OwnedContainerSpec:
    return runtime_recovery.OwnedContainerSpec(
        image=_PINNED_IMAGE,
        create_options=('--gpus', 'all', '--publish', '8080:8080'),
        argv=('serve', '--port', '8080'),
        inherited_environment_names=('MODEL',))


def _v3_task(run: str | None = None) -> sky.Task:
    task = sky.Task(run=run or _owned_spec().render(), envs={'MODEL': 'boltz'})
    task.set_resources(
        sky.Resources(cloud=clouds.AWS(),
                      instance_type='g6.xlarge',
                      region='us-east-1',
                      zone='us-east-1a',
                      memory='16',
                      use_spot=False))
    return task


def _install_profiles(monkeypatch, profile_version: int, *profiles:
                      dict[str, object]) -> None:
    monkeypatch.setenv(
        constants.SYSTEM_OOM_RECOVERY_PROFILES_ENV_VAR,
        json.dumps({
            'version': profile_version,
            'profiles': list(profiles),
        }))


def _v3_authorization(task: sky.Task) -> dict[str, object]:
    spec = _owned_spec()
    return {
        'authorization_version': 3,
        'profile_id': 'boltz-l4-authorization-v3',
        'workspace': 'default',
        'service_name': _SERVICE_NAME,
        'service_hash': _SERVICE_HASH,
        'task_sha256': system_oom_recovery.safety_profile_digest(task),
        'runtime_image_digest': _IMAGE_DIGEST,
        'runtime_profile_version': 2,
        'required_runtime_capability': runtime_recovery.CAPABILITY_V2,
        'owned_container_spec': spec.to_dict(),
        'owned_container_spec_sha256': system_oom_recovery._sha256_json(  # pylint: disable=protected-access
            spec.to_dict()),
        'execution_envelope_sha256': system_oom_recovery._sha256_json(  # pylint: disable=protected-access
            runtime_recovery.RecoveryExecutionEnvelope.standard().to_dict()),
        'resource_envelope': {
            'provider': 'aws',
            'allowed_aws_account_ids': ['123456789012'],
            'allowed_locations': [{
                'region': 'us-east-1',
                'availability_zones': ['us-east-1a'],
            }],
            'allowed_market_types': ['on_demand'],
            'allowed_instance_types': ['g6.xlarge'],
            'max_host_memory_gib': 16,
            'num_nodes': 1,
            'dedicated': True,
            'require_new_create': True,
            'required_identity': [
                'aws_account_id', 'region', 'availability_zone',
                'ec2_instance_id'
            ],
        },
    }


def _install_v3(monkeypatch, task: sky.Task) -> None:
    _install_profiles(monkeypatch, 3, _v3_authorization(task))


def _v3_intent(requested, *, nonce: str = 'b' * 64):
    intent = requested.to_intent_fields()
    intent.update({
        'service_hash': _SERVICE_HASH,
        'replica_id': 7,
        'launch_generation': 3,
        'launch_nonce': nonce,
    })
    return intent


def _legacy_launch_context() -> dict[str, object]:
    """Return a historical context without importing removed constants."""
    return {
        constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: _SERVICE_NAME,
        constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: _SERVICE_HASH,
        constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION_KEY: 1,
        constants.SYSTEM_OOM_RECOVERY_PROFILE_ID_KEY: 'boltz-l4-v1',
        'sky_serve_system_oom_recovery_profile_version': 1,
    }


def _bound_v3_context(monkeypatch, task: sky.Task) -> dict[str, object]:
    _install_v3(monkeypatch, task)
    requested = system_oom_recovery.resolve_requested_authorization_v3(
        task, service_name=_SERVICE_NAME, service_hash=_SERVICE_HASH)
    assert requested is not None
    unbound = system_oom_recovery.create_unbound_launch_context(
        _v3_intent(requested),
        service_name=_SERVICE_NAME,
        service_version=5,
        service_lifecycle_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.2',
        controller_incarnation=_CONTROLLER_INCARNATION,
        controller_owner_epoch=6)
    return system_oom_recovery.bind_launch_context(unbound, 'request-123')


def _resolved_container_task() -> sky.Task:
    resolved = container_images.ResolvedContainerImage(
        image_id='00000000-0000-4000-8000-000000000001',
        reference=_PINNED_IMAGE,
        target_id='source',
        digest=_IMAGE_DIGEST,
        auth_strategy='source_config',
        status='WARMING',
        fallback_reason='managed_route_warming')
    task = _task()
    task.set_resources(
        sky.Resources(instance_type='g2-standard-4',
                      container_image={
                          'release': 'boltz-l4',
                          'distribution': 'production',
                      },
                      _resolved_container_image=resolved))
    return task


def test_safety_profile_digest_ignores_placement_but_binds_command():
    first = _task()
    second = _task()
    second.set_resources(sky.Resources(instance_type='g6.xlarge',
                                       use_spot=True))

    assert (system_oom_recovery.safety_profile_digest(first) ==
            system_oom_recovery.safety_profile_digest(second))
    assert (system_oom_recovery.safety_profile_digest(first)
            != system_oom_recovery.safety_profile_digest(
                _task('systemctl start escaped.service')))


def test_runtime_image_digest_requires_the_actual_operand_to_be_pinned():
    assert system_oom_recovery.runtime_image_digest(_task()) == _IMAGE_DIGEST
    rejected_commands = (
        'docker run --rm example.invalid/model:mutable',
        f'docker run --env DECOY=repo.invalid/decoy@{_IMAGE_DIGEST} '
        'example.invalid/model:mutable',
        f'evil-docker run --rm {_PINNED_IMAGE}',
        f'echo docker run --rm {_PINNED_IMAGE}',
        f"echo '; docker run --rm {_PINNED_IMAGE}'",
        f'echo ";" docker run --rm {_PINNED_IMAGE}',
        f'echo \\; docker run --rm {_PINNED_IMAGE}',
        f'# docker run --rm {_PINNED_IMAGE}\necho ready',
        f'cat <<EOF\ndocker run --rm {_PINNED_IMAGE}\nEOF',
        f'docker run --unknown {_PINNED_IMAGE}',
        f'docker run "{_PINNED_IMAGE}"',
        f'sudo docker run --rm {_PINNED_IMAGE}',
        f'echo ready\ndocker run --rm {_PINNED_IMAGE}',
        f'exec docker run --rm {_PINNED_IMAGE}',
        _owned_spec().render().replace('docker run ', 'docker run  ', 1),
    )
    for command in rejected_commands:
        assert system_oom_recovery.runtime_image_digest(_task(command)) is None


def test_generic_task_container_image_is_ineligible(monkeypatch):
    task = _resolved_container_task()
    _install_v3(monkeypatch, task)

    assert system_oom_recovery.runtime_image_digest(task) is None
    assert system_oom_recovery.resolve_requested_authorization_v3(
        task, service_name=_SERVICE_NAME, service_hash=_SERVICE_HASH) is None


def test_managed_secret_reference_is_ineligible(monkeypatch):
    task = sky.Task.from_yaml_config({
        'run': _owned_spec().render(),
        'envs': {
            'MODEL': 'boltz'
        },
        'secrets': ['secrets:model_token'],
        'resources': {
            'instance_type': 'g2-standard-4'
        },
    })
    _install_v3(monkeypatch, task)

    assert task.managed_secret_refs
    assert system_oom_recovery.resolve_requested_authorization_v3(
        task, service_name=_SERVICE_NAME, service_hash=_SERVICE_HASH) is None


def test_safety_profile_binds_effective_policy_mutations(tmp_path):
    safe_source = tmp_path / 'safe'
    unsafe_source = tmp_path / 'unsafe'
    safe_source.mkdir()
    unsafe_source.mkdir()

    def _loaded_task(source):
        return sky.Task.from_yaml_str(f'''\
resources:
  instance_type: g2-standard-4
envs:
  MODEL: boltz
file_mounts:
  /model: {source}
run: docker run --rm {_PINNED_IMAGE}
''')

    baseline = _loaded_task(safe_source)
    original = system_oom_recovery.safety_profile_digest(baseline)
    env_mutated = _loaded_task(safe_source)
    env_mutated.update_envs({'MODEL': 'unsafe'})
    mount_mutated = _loaded_task(safe_source)
    mount_mutated.update_file_mounts({'/model': str(unsafe_source)})

    assert system_oom_recovery.safety_profile_digest(env_mutated) != original
    assert system_oom_recovery.safety_profile_digest(mount_mutated) != original


def test_safety_profile_normalizes_only_server_replica_id():
    first = _task()
    first.update_envs({constants.REPLICA_ID_ENV_VAR: '1'})
    second = _task()
    second.update_envs({constants.REPLICA_ID_ENV_VAR: '42'})

    original_envs = dict(first.envs)
    first_digest = system_oom_recovery.safety_profile_digest(first)

    assert first.envs == original_envs
    assert first_digest == system_oom_recovery.safety_profile_digest(second)


def test_safety_profile_binds_legacy_resource_volumes():
    without_volume = _task()
    with_volume = _task()
    with_volume.set_resources(
        sky.Resources(cloud=clouds.GCP(),
                      region='us-central1',
                      instance_type='g2-standard-4',
                      volumes=[{
                          'name': 'model-data',
                          'path': '/mnt/data',
                      }]))

    assert (system_oom_recovery.safety_profile_digest(without_volume)
            != system_oom_recovery.safety_profile_digest(with_volume))


@pytest.mark.parametrize('document_version', [1, 2])
def test_pre_v3_authorization_documents_are_ignored(monkeypatch,
                                                    document_version):
    task = _task()
    _install_profiles(monkeypatch, document_version, _v3_authorization(task))

    assert system_oom_recovery.resolve_requested_authorization_v3(
        task, service_name=_SERVICE_NAME, service_hash=_SERVICE_HASH) is None
    assert system_oom_recovery.match_trusted_profile(
        task, _legacy_launch_context()) is None


def test_v3_requires_exact_canonical_owned_container_render(monkeypatch):
    canonical_task = _task()
    noncanonical_task = _task(_owned_spec().render().replace(
        'docker run ', 'docker run  ', 1))
    authorization = _v3_authorization(noncanonical_task)
    _install_profiles(monkeypatch, 3, authorization)

    assert _owned_spec().render() == canonical_task.run
    assert system_oom_recovery.resolve_requested_authorization_v3(
        noncanonical_task,
        service_name=_SERVICE_NAME,
        service_hash=_SERVICE_HASH) is None


def test_exact_incarnation_workspace_and_task_are_bound(monkeypatch):
    task = _v3_task()
    context = _bound_v3_context(monkeypatch, task)

    changed_context = dict(context)
    changed_context[constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY] = 'other'
    assert system_oom_recovery.match_trusted_profile(task,
                                                     changed_context) is None
    assert system_oom_recovery.match_trusted_profile(
        _v3_task('systemctl start escaped.service'), context) is None
    with skypilot_config.local_active_workspace_ctx('another-workspace'):
        assert system_oom_recovery.match_trusted_profile(task, context) is None


@pytest.mark.parametrize('document', [
    '{not-json',
    json.dumps({
        'version': True,
        'profiles': [],
    }),
    json.dumps({
        'version': 3,
        'profiles': [{
            'profile_id': 'duplicate',
        }, {
            'profile_id': 'duplicate',
        }],
    }),
])
def test_invalid_server_profile_document_fails_closed(monkeypatch, document):
    monkeypatch.setenv(constants.SYSTEM_OOM_RECOVERY_PROFILES_ENV_VAR, document)
    assert system_oom_recovery.resolve_requested_authorization_v3(
        _task(), service_name=_SERVICE_NAME, service_hash=_SERVICE_HASH) is None


def test_authorization_v3_maps_explicitly_to_runtime_profile_two(monkeypatch):
    task = _v3_task()
    _install_v3(monkeypatch, task)

    requested = system_oom_recovery.resolve_requested_authorization_v3(
        task, service_name=_SERVICE_NAME, service_hash=_SERVICE_HASH)
    assert requested is not None
    assert requested.authorization_version == 3
    assert requested.runtime_profile_version == 2
    assert requested.expected_runtime_capability == runtime_recovery.CAPABILITY_V2

    unbound = system_oom_recovery.create_unbound_launch_context(
        _v3_intent(requested),
        service_name=_SERVICE_NAME,
        service_version=5,
        service_lifecycle_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.2',
        controller_incarnation=_CONTROLLER_INCARNATION,
        controller_owner_epoch=6)
    bound = system_oom_recovery.bind_launch_context(unbound, 'request-123')
    matched = system_oom_recovery.match_trusted_profile(task, bound)

    assert matched is not None
    assert matched.authorization_version == 3
    assert matched.runtime_profile_version == 2
    assert matched.capability == runtime_recovery.CAPABILITY_V2
    assert matched.launch_plan(
    ) == runtime_recovery.RecoveryLaunchPlan.owned_container(_owned_spec())


def test_v3_context_is_closed_and_nonce_becomes_server_request_id(monkeypatch):
    task = _v3_task()
    _install_v3(monkeypatch, task)
    requested = system_oom_recovery.resolve_requested_authorization_v3(
        task, service_name=_SERVICE_NAME, service_hash=_SERVICE_HASH)
    assert requested is not None
    unbound = system_oom_recovery.create_unbound_launch_context(
        _v3_intent(requested),
        service_name=_SERVICE_NAME,
        service_version=5,
        service_lifecycle_epoch=4,
        controller_pid=None,
        controller_ip=None,
        controller_incarnation=_CONTROLLER_INCARNATION,
        controller_owner_epoch=6)

    assert system_oom_recovery.has_v3_system_oom_recovery_context(unbound)
    assert system_oom_recovery.is_unbound_launch_context(unbound)
    bound = system_oom_recovery.bind_launch_context(unbound, 'request-123')
    assert constants.SYSTEM_OOM_RECOVERY_LAUNCH_NONCE_KEY not in bound
    assert bound[constants.SYSTEM_OOM_RECOVERY_BOUND_REQUEST_ID_KEY] == (
        'request-123')
    assert system_oom_recovery.validate_bound_launch_context(bound) == bound

    with pytest.raises(ValueError, match='invalid fields'):
        system_oom_recovery.validate_unbound_launch_context({
            **unbound,
            'sky_serve_system_oom_recovery_profile_version': 2,
        })
    with pytest.raises(ValueError, match='invalid fields'):
        system_oom_recovery.validate_bound_launch_context({
            **bound,
            'sky_serve_system_oom_recovery_unknown': True,
        })


def test_v3_context_preserves_empty_workspace_text(monkeypatch):
    task = _v3_task()
    authorization = _v3_authorization(task)
    authorization['workspace'] = ''
    _install_profiles(monkeypatch, 3, authorization)

    with skypilot_config.local_active_workspace_ctx(''):
        requested = system_oom_recovery.resolve_requested_authorization_v3(
            task, service_name=_SERVICE_NAME, service_hash=_SERVICE_HASH)
        assert requested is not None
        unbound = system_oom_recovery.create_unbound_launch_context(
            _v3_intent(requested),
            service_name=_SERVICE_NAME,
            service_version=5,
            service_lifecycle_epoch=4,
            controller_pid=None,
            controller_ip=None,
            controller_incarnation=_CONTROLLER_INCARNATION,
            controller_owner_epoch=6)
        bound = system_oom_recovery.bind_launch_context(unbound, 'request-123')

        assert unbound[constants.SYSTEM_OOM_RECOVERY_WORKSPACE_KEY] == ''
        assert system_oom_recovery.validate_bound_launch_context(bound)[
            constants.SYSTEM_OOM_RECOVERY_WORKSPACE_KEY] == ''
        assert system_oom_recovery.match_trusted_profile(task,
                                                         bound) is not None


def test_v3_detector_routes_historical_context_to_closed_validation():
    legacy = _legacy_launch_context()

    assert system_oom_recovery.has_v3_system_oom_recovery_context(legacy)
    assert system_oom_recovery.has_v3_system_oom_recovery_context({
        **legacy,
        constants.SYSTEM_OOM_RECOVERY_LAUNCH_NONCE_KEY: 'b' * 64,
    })
    assert system_oom_recovery.has_v3_system_oom_recovery_context({
        'sky_serve_system_oom_recovery_unknown': True,
    })


def test_v3_candidate_requires_exact_singleton_aws_resource(monkeypatch):
    gcp_task = _v3_task()
    gcp_task.set_resources(
        sky.Resources(cloud=clouds.GCP(), instance_type='g2-standard-4'))
    _install_v3(monkeypatch, gcp_task)

    assert system_oom_recovery.resolve_requested_authorization_v3(
        gcp_task, service_name=_SERVICE_NAME,
        service_hash=_SERVICE_HASH) is None

    mixed_task = _v3_task()
    mixed_task.set_resources({
        sky.Resources(cloud=clouds.GCP(), instance_type='g2-standard-4'),
        sky.Resources(cloud=clouds.AWS(), instance_type='g6.xlarge'),
    })
    monkeypatch.setattr(clouds.AWS, 'get_vcpus_mem_from_instance_type',
                        lambda *_args, **_kwargs: (4, 16))
    _install_v3(monkeypatch, mixed_task)
    assert system_oom_recovery.resolve_requested_authorization_v3(
        mixed_task, service_name=_SERVICE_NAME,
        service_hash=_SERVICE_HASH) is None

    provider_unset_task = _v3_task()
    provider_unset_task.set_resources(
        sky.Resources(instance_type='g6.xlarge',
                      region='us-east-1',
                      zone='us-east-1a',
                      use_spot=False))
    _install_v3(monkeypatch, provider_unset_task)
    assert system_oom_recovery.resolve_requested_authorization_v3(
        provider_unset_task,
        service_name=_SERVICE_NAME,
        service_hash=_SERVICE_HASH) is None

    stale_multivalue_task = _v3_task()
    stale_multivalue_authorization = _v3_authorization(stale_multivalue_task)
    stale_resource_envelope = stale_multivalue_authorization[
        'resource_envelope']
    assert isinstance(stale_resource_envelope, dict)
    stale_resource_envelope['allowed_market_types'] = ['on_demand', 'spot']
    _install_profiles(monkeypatch, 3, stale_multivalue_authorization)
    assert system_oom_recovery.resolve_requested_authorization_v3(
        stale_multivalue_task,
        service_name=_SERVICE_NAME,
        service_hash=_SERVICE_HASH) is None

    for stale_resource in (sky.Resources(cloud=clouds.AWS(),
                                         instance_type='g5.xlarge',
                                         region='us-east-1',
                                         zone='us-east-1a',
                                         use_spot=False),
                           sky.Resources(cloud=clouds.AWS(),
                                         instance_type='g6.xlarge',
                                         region='us-west-2',
                                         zone='us-west-2a',
                                         use_spot=False),
                           sky.Resources(cloud=clouds.AWS(),
                                         instance_type='g6.xlarge',
                                         region='us-east-1',
                                         zone='us-east-1b',
                                         use_spot=False),
                           sky.Resources(cloud=clouds.AWS(),
                                         instance_type='g6.xlarge',
                                         region='us-east-1',
                                         zone='us-east-1a',
                                         use_spot=True)):
        stale_task = _v3_task()
        stale_task.set_resources(stale_resource)
        _install_v3(monkeypatch, stale_task)
        assert system_oom_recovery.resolve_requested_authorization_v3(
            stale_task, service_name=_SERVICE_NAME,
            service_hash=_SERVICE_HASH) is None
