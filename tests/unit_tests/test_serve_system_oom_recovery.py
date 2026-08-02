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


def _task(run: str = f'exec docker run --rm {_PINNED_IMAGE}') -> sky.Task:
    task = sky.Task(run=run, envs={'MODEL': 'boltz'})
    task.set_resources(sky.Resources(instance_type='g2-standard-4'))
    return task


def _owned_spec() -> runtime_recovery.OwnedContainerSpec:
    return runtime_recovery.OwnedContainerSpec(
        image=_PINNED_IMAGE,
        create_options=('--gpus', 'all', '--publish', '8080:8080'),
        argv=('serve', '--port', '8080'),
        inherited_environment_names=('MODEL',))


def _profile(task: sky.Task,
             profile_version: int = 1,
             profile_id: str | None = None) -> dict[str, object]:
    value: dict[str, object] = {
        'profile_id': profile_id or f'boltz-l4-v{profile_version}',
        'workspace': 'default',
        'service_name': _SERVICE_NAME,
        'service_hash': _SERVICE_HASH,
        'task_digest': system_oom_recovery.safety_profile_digest(task),
        'runtime_image_digest': _IMAGE_DIGEST,
    }
    if profile_version == 2:
        value['owned_container_spec'] = _owned_spec().to_dict()
    return value


def _install_profiles(monkeypatch, profile_version: int, *profiles:
                      dict[str, object]) -> None:
    monkeypatch.setenv(
        constants.SYSTEM_OOM_RECOVERY_PROFILES_ENV_VAR,
        json.dumps({
            'version': profile_version,
            'profiles': list(profiles),
        }))


def _launch_context(*,
                    profile_id: str = 'boltz-l4-v1',
                    profile_version: int = 1,
                    contract_version: object = 1) -> dict[str, object]:
    return {
        constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: _SERVICE_NAME,
        constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: _SERVICE_HASH,
        constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION_KEY: contract_version,
        constants.SYSTEM_OOM_RECOVERY_PROFILE_ID_KEY: profile_id,
        constants.SYSTEM_OOM_RECOVERY_PROFILE_VERSION_KEY: profile_version,
    }


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
    assert system_oom_recovery.runtime_image_digest(
        _task('docker run --rm example.invalid/model:mutable')) is None
    decoy = (f'docker run --env DECOY=repo.invalid/decoy@{_IMAGE_DIGEST} '
             'example.invalid/model:mutable')
    assert system_oom_recovery.runtime_image_digest(_task(decoy)) is None
    assert system_oom_recovery.runtime_image_digest(
        _task(f'evil-docker run --rm {_PINNED_IMAGE}')) is None
    assert system_oom_recovery.runtime_image_digest(
        _task(f'echo docker run --rm {_PINNED_IMAGE}')) is None
    assert system_oom_recovery.runtime_image_digest(
        _task(f"echo '; docker run --rm {_PINNED_IMAGE}'")) is None
    assert system_oom_recovery.runtime_image_digest(
        _task(f'echo ";" docker run --rm {_PINNED_IMAGE}')) is None
    assert system_oom_recovery.runtime_image_digest(
        _task(f'echo \\; docker run --rm {_PINNED_IMAGE}')) is None
    assert system_oom_recovery.runtime_image_digest(
        _task(f'# docker run --rm {_PINNED_IMAGE}\necho ready')) is None
    assert system_oom_recovery.runtime_image_digest(
        _task(f'cat <<EOF\ndocker run --rm {_PINNED_IMAGE}\nEOF')) is None
    assert system_oom_recovery.runtime_image_digest(
        _task(f'docker run --unknown {_PINNED_IMAGE}')) is None
    escaped_image = _PINNED_IMAGE.replace('aaaa', r'\aaaa', 1)
    assert system_oom_recovery.runtime_image_digest(
        _task(f'docker run "{escaped_image}"')) is None
    assert (system_oom_recovery.runtime_image_digest(
        _task(f'sudo docker run --rm {_PINNED_IMAGE}')) == _IMAGE_DIGEST)
    assert (system_oom_recovery.runtime_image_digest(
        _task(f'echo ready\ndocker run --rm {_PINNED_IMAGE}')) == _IMAGE_DIGEST)


def test_generic_task_container_image_is_ineligible(monkeypatch):
    task = _resolved_container_task()
    _install_profiles(monkeypatch, 1, _profile(task))

    assert system_oom_recovery.runtime_image_digest(task) is None
    assert system_oom_recovery.match_trusted_profile(task,
                                                     _launch_context()) is None


def test_managed_secret_reference_is_ineligible(monkeypatch):
    task = sky.Task.from_yaml_config({
        'run': f'exec docker run --rm {_PINNED_IMAGE}',
        'envs': {
            'MODEL': 'boltz'
        },
        'secrets': ['secrets:model_token'],
        'resources': {
            'instance_type': 'g2-standard-4'
        },
    })
    _install_profiles(monkeypatch, 1, _profile(task))

    assert task.managed_secret_refs
    assert system_oom_recovery.match_trusted_profile(task,
                                                     _launch_context()) is None


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


@pytest.mark.parametrize('profile_version', [1, 2])
def test_match_trusted_profile_maps_exact_capability(monkeypatch,
                                                     profile_version):
    task = (_task() if profile_version == 1 else _task(_owned_spec().render()))
    profile_id = f'boltz-l4-v{profile_version}'
    _install_profiles(monkeypatch, profile_version,
                      _profile(task, profile_version))

    profile = system_oom_recovery.match_trusted_profile(
        task,
        _launch_context(profile_id=profile_id, profile_version=profile_version))

    assert profile is not None
    assert profile.profile_version == profile_version
    assert profile.capability == (
        runtime_recovery.CAPABILITY_BY_PROFILE_VERSION[profile_version])
    assert profile.launch_plan().capability == profile.capability
    if profile_version == 2:
        assert profile.owned_container_spec == _owned_spec()


@pytest.mark.parametrize('contract_version', [None, 0, 2, True, '1'])
def test_match_requires_exact_controller_contract(monkeypatch,
                                                  contract_version):
    task = _task()
    _install_profiles(monkeypatch, 1, _profile(task))

    assert system_oom_recovery.match_trusted_profile(
        task, _launch_context(contract_version=contract_version)) is None


def test_match_requires_exact_persisted_profile_identity(monkeypatch):
    task = _task()
    _install_profiles(monkeypatch, 1, _profile(task))

    assert system_oom_recovery.match_trusted_profile(
        task, _launch_context(profile_id='other')) is None
    assert system_oom_recovery.match_trusted_profile(
        task, _launch_context(profile_version=2)) is None


def test_resolve_requested_profile_does_not_require_launch_contract(
        monkeypatch):
    task = _task()
    _install_profiles(monkeypatch, 1, _profile(task))

    requested = system_oom_recovery.resolve_requested_profile(
        task, service_name=_SERVICE_NAME, service_hash=_SERVICE_HASH)

    assert requested == system_oom_recovery.RequestedRecoveryProfile(
        profile_id='boltz-l4-v1', profile_version=1)


def test_v2_requires_exact_canonical_owned_container_render(monkeypatch):
    canonical_task = _task(_owned_spec().render())
    noncanonical_task = _task(f'docker run  --gpus all --publish 8080:8080 '
                              f'--env MODEL {_PINNED_IMAGE} serve --port 8080')
    # Bind the profile to the noncanonical effective task so only the canonical
    # renderer check, not the broad safety digest, can reject it.
    _install_profiles(monkeypatch, 2, _profile(noncanonical_task, 2))

    assert _owned_spec().render() == canonical_task.run
    assert system_oom_recovery.match_trusted_profile(
        noncanonical_task,
        _launch_context(profile_id='boltz-l4-v2', profile_version=2)) is None


def test_exact_incarnation_workspace_and_task_are_bound(monkeypatch):
    task = _task()
    _install_profiles(monkeypatch, 1, _profile(task))
    context = _launch_context()

    changed_context = dict(context)
    changed_context[constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY] = 'other'
    assert system_oom_recovery.match_trusted_profile(task,
                                                     changed_context) is None
    assert system_oom_recovery.match_trusted_profile(
        _task('systemctl start escaped.service'), context) is None
    with skypilot_config.local_active_workspace_ctx('another-workspace'):
        assert system_oom_recovery.match_trusted_profile(task, context) is None


@pytest.mark.parametrize('document', [
    '{not-json',
    json.dumps({
        'version': True,
        'profiles': [],
    }),
    json.dumps({
        'version': 1,
        'profiles': [{
            'profile_id': 'duplicate',
        }, {
            'profile_id': 'duplicate',
        }],
    }),
])
def test_invalid_server_profile_document_fails_closed(monkeypatch, document):
    monkeypatch.setenv(constants.SYSTEM_OOM_RECOVERY_PROFILES_ENV_VAR, document)
    assert system_oom_recovery.match_trusted_profile(_task(),
                                                     _launch_context()) is None
