"""Tests for fail-closed system-OOM authorization-v3 bootstrap."""

import dataclasses
import json
import logging
import os
import subprocess
import sys
import textwrap
import types

import pytest

import sky
from sky import clouds
from sky import skypilot_config
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import service_spec as service_spec_lib
from sky.serve import system_oom_recovery
from sky.serve import system_oom_recovery_authorization as authorization
from sky.skylet import constants as skylet_constants
from sky.skylet import system_oom_recovery as runtime_recovery
from sky.utils import yaml_utils

_IMAGE_DIGEST = 'sha256:' + 'a' * 64
_IMAGE = f'example.invalid/canary@{_IMAGE_DIGEST}'
_SERVICE_NAME = 'oom-canary'
_SERVICE_HASH = 'exact-incarnation'
_PROFILE_ID = 'oom-canary-on-demand-v3'
_TASK_ENV_VALUE = 'bootstrap-task-environment-value'


def _owned_spec(*, argv: tuple[str, ...] = ('serve', '--port', '8080')):
    return runtime_recovery.OwnedContainerSpec(
        image=_IMAGE,
        create_options=('--publish', '8080:8080'),
        argv=argv,
        inherited_environment_names=('MODEL',))


def _task(*,
          argv: tuple[str, ...] = ('serve', '--port', '8080'),
          instance_type: str = 'g6.xlarge',
          region: str = 'us-east-1',
          zone: str = 'us-east-1a',
          use_spot: bool = False) -> sky.Task:
    task = sky.Task(run=_owned_spec(argv=argv).render(),
                    envs={'MODEL': _TASK_ENV_VALUE})
    task.set_resources(
        sky.Resources(cloud=clouds.AWS(),
                      instance_type=instance_type,
                      region=region,
                      zone=zone,
                      use_spot=use_spot))
    task.update_envs({'SKYPILOT_SERVE_REPLICA_ID': '1'})
    return task


def _target(*, task: sky.Task | None = None, workspace: str = 'default'):
    return authorization.AuthorizationBootstrapTarget(
        service_name=_SERVICE_NAME,
        service_hash=_SERVICE_HASH,
        workspace=workspace,
        version=7,
        task=task or _task())


def _mock_aws_catalog(monkeypatch,
                      *,
                      memory_gib: float = 16,
                      offered: bool = True,
                      account_id: str = '123456789012') -> None:
    monkeypatch.setattr(clouds.AWS, 'get_active_user_identity',
                        classmethod(lambda _cls: ['aws-user', account_id]))
    monkeypatch.setattr(
        clouds.AWS, 'get_vcpus_mem_from_instance_type',
        classmethod(lambda _cls, _instance_type: (4, memory_gib)))

    def _offerings(_cls,
                   _instance_type,
                   accelerators,
                   use_spot,
                   region,
                   zone,
                   resources=None):
        del accelerators, use_spot, resources
        if not offered:
            return []
        return [clouds.Region(region).set_zones([clouds.Zone(zone)])]

    monkeypatch.setattr(clouds.AWS, 'regions_with_offering',
                        classmethod(_offerings))


def _envelope(monkeypatch,
              *,
              target: authorization.AuthorizationBootstrapTarget | None = None,
              market: str = 'on_demand'):
    _mock_aws_catalog(monkeypatch)
    return authorization.build_aws_resource_envelope(
        target or _target(),
        aws_account_ids=('123456789012',),
        aws_locations=('us-east-1=us-east-1a',),
        market_types=(market,),
        instance_types=('g6.xlarge',))


def _mock_central_postgres(monkeypatch,
                           *,
                           uri: str = 'postgresql://user:password@db/db'
                          ) -> None:
    monkeypatch.setenv(skylet_constants.ENV_VAR_DB_CONNECTION_URI, uri)
    monkeypatch.delenv(skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER,
                       raising=False)
    engine = types.SimpleNamespace(dialect=types.SimpleNamespace(
        name='postgresql'))
    monkeypatch.setattr(serve_state, 'get_database_engine', lambda: engine)


def test_central_selection_forces_verify_and_restores_environment(monkeypatch):
    monkeypatch.setenv(skylet_constants.ENV_VAR_DB_CONNECTION_URI,
                       'postgresql://configured-central.invalid/serve')
    monkeypatch.delenv(skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER,
                       raising=False)
    monkeypatch.setenv(skylet_constants.ENV_VAR_STATE_DB_MIGRATION_MODE,
                       'upgrade')

    def _engine():
        assert os.environ[skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER] == 'true'
        assert os.environ[
            skylet_constants.ENV_VAR_STATE_DB_MIGRATION_MODE] == 'verify'
        return types.SimpleNamespace(dialect=types.SimpleNamespace(
            name='postgresql'))

    monkeypatch.setattr(serve_state, 'get_database_engine', _engine)

    with authorization._central_postgres_selection():  # pylint: disable=protected-access
        assert os.environ[
            skylet_constants.ENV_VAR_STATE_DB_MIGRATION_MODE] == 'verify'

    assert skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER not in os.environ
    assert os.environ[
        skylet_constants.ENV_VAR_STATE_DB_MIGRATION_MODE] == 'upgrade'


def _service_spec(*, min_replicas: int = 0):
    return service_spec_lib.SkyServiceSpec(readiness_path='/health',
                                           initial_delay_seconds=0,
                                           readiness_timeout_seconds=10,
                                           endpoint_probe_interval_seconds=5,
                                           lb_stream_timeout_seconds=600,
                                           min_replicas=min_replicas,
                                           max_replicas=max(1, min_replicas),
                                           target_qps_per_replica=1)


def _snapshot(*,
              min_replicas: int = 0,
              replica_count: int = 0,
              status=serve_state.ServiceStatus.NO_REPLICA):
    return {
        'service_name': _SERVICE_NAME,
        'service_hash': _SERVICE_HASH,
        'workspace': 'default',
        'version': 7,
        'status': status,
        'pool': False,
        'resource_action_mode': 'legacy',
        'spec': _service_spec(min_replicas=min_replicas),
        'yaml_content': 'run: echo placeholder\n',
        'quarantined_at': None,
        'replica_count': replica_count,
    }


def test_generate_round_trips_production_parser_and_matcher(monkeypatch):
    envelope = _envelope(monkeypatch)
    previous = 'preexisting-document'
    monkeypatch.setenv('SKYPILOT_INTERNAL_SERVE_SYSTEM_OOM_RECOVERY_PROFILES',
                       previous)

    document = authorization.generate_authorization_document(
        _target(), profile_id=_PROFILE_ID, resource_envelope=envelope)

    assert document == json.dumps(json.loads(document),
                                  sort_keys=True,
                                  separators=(',', ':'),
                                  ensure_ascii=True)
    parsed = system_oom_recovery.parse_authorization_document_v3(document)
    assert len(parsed) == 1
    assert parsed[0].task_sha256 == system_oom_recovery.safety_profile_digest(
        _task())
    assert parsed[0].owned_container_spec == _owned_spec()
    assert parsed[0].resource_envelope.allowed_market_types == ('on_demand',)
    assert _TASK_ENV_VALUE not in document
    assert os.environ[
        'SKYPILOT_INTERNAL_SERVE_SYSTEM_OOM_RECOVERY_PROFILES'] == previous


def test_load_target_uses_exact_durable_snapshot_and_shared_task_builder(
        monkeypatch):
    snapshot = _snapshot()
    built_task = _task()
    monkeypatch.setattr(serve_state, 'system_recovery_persistence_available',
                        lambda: True)
    monkeypatch.setattr(serve_state,
                        'get_system_recovery_authorization_snapshot',
                        lambda _service_name: snapshot)

    def _build(yaml_content, replica_id, resources_override, *,
               exact_resources_override, authoritative_service_spec,
               service_name):
        assert yaml_content == snapshot['yaml_content']
        assert replica_id == 1
        assert resources_override is None
        assert not exact_resources_override
        assert authoritative_service_spec is snapshot['spec']
        assert service_name == _SERVICE_NAME
        return built_task

    monkeypatch.setattr(replica_managers, '_build_replica_launch_task', _build)

    target = authorization.load_bootstrap_target(_SERVICE_NAME)

    assert target == authorization.AuthorizationBootstrapTarget(
        service_name=_SERVICE_NAME,
        service_hash=_SERVICE_HASH,
        workspace='default',
        version=7,
        task=built_task)


@pytest.mark.parametrize(('snapshot_override', 'message'), [
    ({
        'replica_count': 1
    }, 'durably min=0'),
    ({
        'status': serve_state.ServiceStatus.READY
    }, 'durably min=0'),
    ({
        'spec': _service_spec(min_replicas=1)
    }, 'durably min=0'),
    ({
        'resource_action_mode': 'action'
    }, 'lifecycle'),
    ({
        'pool': True
    }, 'lifecycle'),
    ({
        'quarantined_at': 1.0
    }, 'committed bootstrap target'),
])
def test_load_target_rejects_nonzero_or_nonlegacy_service(
        monkeypatch, snapshot_override, message):
    snapshot = _snapshot()
    snapshot.update(snapshot_override)
    monkeypatch.setattr(serve_state, 'system_recovery_persistence_available',
                        lambda: True)
    monkeypatch.setattr(serve_state,
                        'get_system_recovery_authorization_snapshot',
                        lambda _service_name: snapshot)

    with pytest.raises(authorization.AuthorizationBootstrapError,
                       match=message):
        authorization.load_bootstrap_target(_SERVICE_NAME)


@pytest.mark.parametrize(('memory_gib', 'offered', 'message'), [
    (16.01, True, 'within 16 GiB'),
    (None, True, 'unknown memory'),
    (16, False, 'absent from the catalog'),
])
def test_envelope_rejects_unsafe_or_unavailable_aws_shape(
        monkeypatch, memory_gib, offered, message):
    _mock_aws_catalog(monkeypatch, memory_gib=memory_gib, offered=offered)

    with pytest.raises(authorization.AuthorizationBootstrapError,
                       match=message):
        authorization.build_aws_resource_envelope(
            _target(),
            aws_account_ids=('123456789012',),
            aws_locations=('us-east-1=us-east-1a',),
            market_types=('on_demand',),
            instance_types=('g6.xlarge',))


@pytest.mark.parametrize(('field', 'values'), [
    ('aws_account_ids', ('123456789012', '210987654321')),
    ('aws_locations', ('us-east-1=us-east-1a', 'us-west-2=us-west-2a')),
    ('aws_locations', ('us-east-1=us-east-1a,us-east-1b',)),
    ('market_types', ('on_demand', 'spot')),
    ('instance_types', ('g6.xlarge', 'g5.xlarge')),
])
def test_envelope_requires_one_exact_aws_allowance(monkeypatch, field, values):
    _mock_aws_catalog(monkeypatch)
    arguments = {
        'aws_account_ids': ('123456789012',),
        'aws_locations': ('us-east-1=us-east-1a',),
        'market_types': ('on_demand',),
        'instance_types': ('g6.xlarge',),
    }
    arguments[field] = values

    with pytest.raises(authorization.AuthorizationBootstrapError):
        authorization.build_aws_resource_envelope(_target(), **arguments)


def test_envelope_binds_active_account_inside_target_workspace(monkeypatch):
    target = _target(workspace='research')
    observed_workspaces = []

    def _identity(_cls):
        observed_workspaces.append(skypilot_config.get_active_workspace())
        return ['aws-user', '123456789012']

    def _offerings(_cls,
                   _instance_type,
                   accelerators,
                   use_spot,
                   region,
                   zone,
                   resources=None):
        del accelerators, use_spot, resources
        observed_workspaces.append(skypilot_config.get_active_workspace())
        return [clouds.Region(region).set_zones([clouds.Zone(zone)])]

    monkeypatch.setattr(clouds.AWS, 'get_active_user_identity',
                        classmethod(_identity))
    monkeypatch.setattr(clouds.AWS, 'get_vcpus_mem_from_instance_type',
                        classmethod(lambda _cls, _instance_type: (4, 16)))
    monkeypatch.setattr(clouds.AWS, 'regions_with_offering',
                        classmethod(_offerings))

    authorization.build_aws_resource_envelope(
        target,
        aws_account_ids=('123456789012',),
        aws_locations=('us-east-1=us-east-1a',),
        market_types=('on_demand',),
        instance_types=('g6.xlarge',))

    assert observed_workspaces == ['research', 'research']


def test_envelope_rejects_account_other_than_active_identity(monkeypatch):
    _mock_aws_catalog(monkeypatch, account_id='210987654321')

    with pytest.raises(authorization.AuthorizationBootstrapError,
                       match='does not match the active identity'):
        authorization.build_aws_resource_envelope(
            _target(),
            aws_account_ids=('123456789012',),
            aws_locations=('us-east-1=us-east-1a',),
            market_types=('on_demand',),
            instance_types=('g6.xlarge',))


def test_spot_task_requires_spot_singleton_envelope(monkeypatch):
    target = _target(task=_task(use_spot=True))
    envelope = _envelope(monkeypatch, target=target, market='spot')

    document = authorization.generate_authorization_document(
        target, profile_id=_PROFILE_ID, resource_envelope=envelope)

    profile, = system_oom_recovery.parse_authorization_document_v3(document)
    assert profile.resource_envelope.allowed_market_types == ('spot',)


@pytest.mark.parametrize('stale_task', [
    _task(instance_type='g5.xlarge'),
    _task(region='us-west-2'),
    _task(zone='us-east-1b'),
    _task(use_spot=True),
])
def test_validator_rejects_stale_aws_task_placement(monkeypatch, stale_task):
    target = _target()
    document = authorization.generate_authorization_document(
        target,
        profile_id=_PROFILE_ID,
        resource_envelope=_envelope(monkeypatch, target=target))

    with pytest.raises(authorization.AuthorizationBootstrapError,
                       match='does not match the AWS envelope'):
        authorization.validate_authorization_document(
            dataclasses.replace(target, task=stale_task),
            document,
            expected_profile_id=_PROFILE_ID)


def test_validator_rejects_noncanonical_and_stale_task(monkeypatch):
    envelope = _envelope(monkeypatch)
    target = _target()
    document = authorization.generate_authorization_document(
        target, profile_id=_PROFILE_ID, resource_envelope=envelope)

    with pytest.raises(authorization.AuthorizationBootstrapError,
                       match='not canonical'):
        authorization.validate_authorization_document(
            target,
            json.dumps(json.loads(document), indent=2),
            expected_profile_id=_PROFILE_ID)

    replaced_incarnation = dataclasses.replace(
        target, service_hash='replacement-incarnation')
    with pytest.raises(authorization.AuthorizationBootstrapError,
                       match='does not match the durable target'):
        authorization.validate_authorization_document(
            replaced_incarnation, document, expected_profile_id=_PROFILE_ID)

    wrong_workspace = dataclasses.replace(target, workspace='research')
    with pytest.raises(authorization.AuthorizationBootstrapError,
                       match='does not match the durable target'):
        authorization.validate_authorization_document(
            wrong_workspace, document, expected_profile_id=_PROFILE_ID)

    stale_target = dataclasses.replace(target,
                                       task=_task(argv=('serve', '--port',
                                                        '8081')))
    with pytest.raises(authorization.AuthorizationBootstrapError,
                       match='production v3 matcher rejected the durable'):
        authorization.validate_authorization_document(
            stale_target, document, expected_profile_id=_PROFILE_ID)


@pytest.mark.parametrize(
    'resources',
    [
        {
            sky.Resources(cloud=clouds.AWS(), instance_type='g6.xlarge'),
            sky.Resources(cloud=clouds.GCP(), instance_type='n1-standard-4'),
        },
        {sky.Resources(instance_type='g6.xlarge')},
        {
            sky.Resources(cloud=clouds.AWS(),
                          instance_type='g6.xlarge',
                          region='us-east-1',
                          zone='us-east-1a')
        },
    ],
)
def test_generator_requires_explicit_aws_only_task(monkeypatch, resources):
    task = _task()
    task.set_resources(resources)

    with pytest.raises(authorization.AuthorizationBootstrapError,
                       match='failed authorization-v3 construction'):
        authorization.generate_authorization_document(
            _target(task=task),
            profile_id=_PROFILE_ID,
            resource_envelope=_envelope(monkeypatch))


def test_generator_rejects_provider_conditional_shell(monkeypatch):
    task = _task()
    task.run = f'if true; then {_owned_spec().render()}; fi'

    with pytest.raises(authorization.AuthorizationBootstrapError,
                       match='failed authorization-v3 construction'):
        authorization.generate_authorization_document(
            _target(task=task),
            profile_id=_PROFILE_ID,
            resource_envelope=_envelope(monkeypatch))


def test_generator_rejects_task_wide_outer_container(monkeypatch):
    task = _task()
    task.set_resources(
        sky.Resources(cloud=clouds.AWS(),
                      instance_type='g6.xlarge',
                      region='us-east-1',
                      zone='us-east-1a',
                      use_spot=False,
                      container_image=_IMAGE))

    with pytest.raises(authorization.AuthorizationBootstrapError,
                       match='failed authorization-v3 construction'):
        authorization.generate_authorization_document(
            _target(task=task),
            profile_id=_PROFILE_ID,
            resource_envelope=_envelope(monkeypatch))


@pytest.mark.parametrize('secret', [
    'top-secret-with-"-quote',
    'top-secret-with-\\-backslash',
    'top-secret-with-\n-newline',
    'top-secret-with-雪-nonascii',
])
def test_cli_never_prints_an_escaped_typed_task_secret(monkeypatch, capsys,
                                                       secret):
    task = _task(argv=('serve', '--token', secret))
    task.update_secrets({'MODEL_TOKEN': secret})
    monkeypatch.setattr(authorization, 'load_bootstrap_target',
                        lambda _service_name: _target(task=task))
    _mock_aws_catalog(monkeypatch)
    _mock_central_postgres(monkeypatch)

    exit_code = authorization.main([
        'generate', '--service-name', _SERVICE_NAME, '--profile-id',
        _PROFILE_ID, '--aws-account-id', '123456789012', '--aws-location',
        'us-east-1=us-east-1a', '--market-type', 'on_demand', '--instance-type',
        'g6.xlarge'
    ])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert secret not in captured.out
    assert secret not in captured.err
    assert captured.out == ''
    assert 'would expose a task secret' in captured.err


def test_validator_checks_typed_secret_against_json_keys(monkeypatch):
    target = _target()
    document = authorization.generate_authorization_document(
        target,
        profile_id=_PROFILE_ID,
        resource_envelope=_envelope(monkeypatch, target=target))
    secret_task = _task()
    secret_task.update_secrets({'MODEL_TOKEN': 'authorization_version'})

    with pytest.raises(authorization.AuthorizationBootstrapError,
                       match='would expose a task secret'):
        authorization.validate_authorization_document(
            dataclasses.replace(target, task=secret_task),
            document,
            expected_profile_id=_PROFILE_ID)


def test_cli_never_prints_database_uri_from_untyped_argv(monkeypatch, capsys):
    database_uri = (
        'postgresql://bootstrap-user:super-secret@db.internal/serve')
    task = _task(argv=('serve', '--database-uri', database_uri))
    monkeypatch.setattr(authorization, 'load_bootstrap_target',
                        lambda _service_name: _target(task=task))
    _mock_aws_catalog(monkeypatch)
    _mock_central_postgres(monkeypatch, uri=database_uri)

    exit_code = authorization.main([
        'generate', '--service-name', _SERVICE_NAME, '--profile-id',
        _PROFILE_ID, '--aws-account-id', '123456789012', '--aws-location',
        'us-east-1=us-east-1a', '--market-type', 'on_demand', '--instance-type',
        'g6.xlarge'
    ])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ''
    assert database_uri not in captured.err
    assert captured.err == (
        'authorization-v3 bootstrap failed: The authorization document would '
        'expose bootstrap configuration.\n')


def test_cli_masks_unexpected_secret_bearing_internal_error(
        monkeypatch, capsys):
    secret = 'secret-from-an-unexpected-exception'

    def _fail(_service_name):
        raise RuntimeError(secret)

    monkeypatch.setattr(authorization, 'load_bootstrap_target', _fail)
    _mock_central_postgres(monkeypatch)

    exit_code = authorization.main([
        'generate', '--service-name', _SERVICE_NAME, '--profile-id',
        _PROFILE_ID, '--aws-account-id', '123456789012', '--aws-location',
        'us-east-1=us-east-1a', '--market-type', 'on_demand', '--instance-type',
        'g6.xlarge'
    ])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ''
    assert secret not in captured.err
    assert 'internal validation failed' in captured.err


@pytest.mark.parametrize('argv', [
    [],
    ['--help'],
    [
        'generate', '--service-name', _SERVICE_NAME, '--profile-id',
        _PROFILE_ID, '--aws-account-id', '123456789012', '--aws-location',
        'us-east-1=us-east-1a', '--market-type', 'secret-market-value',
        '--instance-type', 'g6.xlarge'
    ],
])
def test_cli_argument_errors_are_value_free(argv, capsys):
    exit_code = authorization.main(argv)

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ''
    assert 'secret-market-value' not in captured.err
    assert captured.err == (
        'authorization-v3 bootstrap failed: Command arguments are invalid.\n')


def test_cli_rejects_missing_central_uri_without_output_values(
        monkeypatch, capsys):
    monkeypatch.delenv(skylet_constants.ENV_VAR_DB_CONNECTION_URI,
                       raising=False)

    exit_code = authorization.main([
        'generate', '--service-name', _SERVICE_NAME, '--profile-id',
        _PROFILE_ID, '--aws-account-id', '123456789012', '--aws-location',
        'us-east-1=us-east-1a', '--market-type', 'on_demand', '--instance-type',
        'g6.xlarge'
    ])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ''
    assert captured.err == (
        'authorization-v3 bootstrap failed: Central PostgreSQL configuration '
        'is unavailable.\n')


def test_cli_generate_and_validate_emit_canonical_success(
        monkeypatch, capsys, tmp_path):
    target = _target()
    monkeypatch.setattr(authorization, 'load_bootstrap_target',
                        lambda _service_name: target)
    _mock_aws_catalog(monkeypatch)
    _mock_central_postgres(monkeypatch)
    generate_args = [
        'generate', '--service-name', _SERVICE_NAME, '--profile-id',
        _PROFILE_ID, '--aws-account-id', '123456789012', '--aws-location',
        'us-east-1=us-east-1a', '--market-type', 'on_demand', '--instance-type',
        'g6.xlarge'
    ]

    assert authorization.main(generate_args) == 0
    generated = capsys.readouterr()
    assert generated.err == ''
    document = generated.out.removesuffix('\n')
    profiles = system_oom_recovery.parse_authorization_document_v3(document)
    assert document == system_oom_recovery.canonical_authorization_document_v3(
        profiles)
    assert profiles[0].owned_container_spec.argv == ('serve', '--port', '8080')

    document_path = tmp_path / 'authorization-v3.json'
    document_path.write_text(f'{document}\n', encoding='utf-8')
    assert authorization.main([
        'validate', '--service-name', _SERVICE_NAME, '--profile-id',
        _PROFILE_ID, '--document-file',
        str(document_path)
    ]) == 0
    validated = capsys.readouterr()
    assert validated.err == ''
    assert json.loads(validated.out) == {
        'authorization_sha256': profiles[0].authorization_sha256,
        'valid': True,
    }


def test_fresh_preimport_entrypoint_selects_server_verify_and_is_closed(
        tmp_path):
    selection_probe = tmp_path / 'central-config-selection-probe'
    fake_sky = tmp_path / 'sky'
    fake_serve = fake_sky / 'serve'
    fake_serve.mkdir(parents=True)
    import_secret = 'secret-from-first-sky-import'
    operation_secret = 'secret-from-bootstrap-operation'
    fake_sky.joinpath('__init__.py').write_text(textwrap.dedent("""
            import logging
            import os
            import pathlib
            import sys

            assert os.environ.get('IS_SKYPILOT_SERVER') == 'true'
            assert os.environ.get('SKYPILOT_STATE_DB_MIGRATION_MODE') == 'verify'
            assert os.environ.get('SKYPILOT_DEBUG') == '1'
            assert (os.environ.get('SKYPILOT_DB_CONNECTION_URI') ==
                    os.environ.get('AUTH_V3_EXPECTED_DB_URI'))
            pathlib.Path(os.environ['AUTH_V3_DB_SELECTION_PROBE']).write_text(
                'central-server-verify-selected', encoding='utf-8')
            secret = os.environ['AUTH_V3_IMPORT_SECRET']
            print(secret)
            print(secret, file=sys.stderr)
            os.write(1, secret.encode('utf-8'))
            os.write(2, secret.encode('utf-8'))
            logging.basicConfig(level=logging.DEBUG)
            logging.critical(secret)
        """),
                                                encoding='utf-8')
    fake_serve.joinpath('__init__.py').write_text('', encoding='utf-8')
    fake_serve.joinpath('system_oom_recovery_authorization.py').write_text(
        textwrap.dedent("""
            import logging
            import os
            import sys


            def run_cli(_argv):
                secret = os.environ['AUTH_V3_OPERATION_SECRET']
                print(secret)
                print(secret, file=sys.stderr)
                os.write(1, secret.encode('utf-8'))
                os.write(2, secret.encode('utf-8'))
                logging.critical(secret)
                return (
                    1,
                    'authorization-v3 bootstrap failed: Central PostgreSQL '
                    'state is unavailable.',
                    True,
                )
        """),
        encoding='utf-8')
    configured_uri = (
        'postgresql://bootstrap-user:uri-secret@central.invalid/serve')
    environment = os.environ.copy()
    environment[skylet_constants.ENV_VAR_DB_CONNECTION_URI] = configured_uri
    environment.pop(skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER, None)
    environment['AUTH_V3_EXPECTED_DB_URI'] = configured_uri
    environment['AUTH_V3_DB_SELECTION_PROBE'] = str(selection_probe)
    environment['AUTH_V3_IMPORT_SECRET'] = import_secret
    environment['AUTH_V3_OPERATION_SECRET'] = operation_secret
    environment[skylet_constants.ENV_VAR_STATE_DB_MIGRATION_MODE] = 'upgrade'
    environment['SKYPILOT_DEBUG'] = '1'
    environment['PYTHONPATH'] = os.pathsep.join((str(tmp_path), os.getcwd()))

    result = subprocess.run([
        sys.executable, '-m',
        'skypilot_serve_system_oom_recovery_authorization', 'generate',
        '--service-name', _SERVICE_NAME, '--profile-id', _PROFILE_ID,
        '--aws-account-id', '123456789012', '--aws-location',
        'us-east-1=us-east-1a', '--market-type', 'on_demand', '--instance-type',
        'g6.xlarge'
    ],
                            cwd=tmp_path,
                            env=environment,
                            capture_output=True,
                            text=True,
                            check=False)

    assert result.returncode == 1
    assert selection_probe.read_text(
        encoding='utf-8') == 'central-server-verify-selected'
    assert result.stdout == ''
    assert import_secret not in result.stderr
    assert operation_secret not in result.stderr
    assert configured_uri not in result.stderr
    assert result.stderr == (
        'authorization-v3 bootstrap failed: Central PostgreSQL state is '
        'unavailable.\n')


def test_fresh_preimport_entrypoint_rejects_missing_uri_before_sky_import():
    environment = os.environ.copy()
    environment.pop(skylet_constants.ENV_VAR_DB_CONNECTION_URI, None)
    environment.pop(skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER, None)
    environment['SKYPILOT_DEBUG'] = '1'
    environment['PYTHONPATH'] = os.getcwd()

    result = subprocess.run([
        sys.executable, '-m',
        'skypilot_serve_system_oom_recovery_authorization', 'generate',
        '--service-name', _SERVICE_NAME, '--profile-id', _PROFILE_ID,
        '--aws-account-id', '123456789012', '--aws-location',
        'us-east-1=us-east-1a', '--market-type', 'on_demand', '--instance-type',
        'g6.xlarge'
    ],
                            cwd=os.getcwd(),
                            env=environment,
                            capture_output=True,
                            text=True,
                            check=False)

    assert result.returncode == 1
    assert result.stdout == ''
    assert result.stderr == (
        'authorization-v3 bootstrap failed: Central PostgreSQL configuration '
        'is unavailable.\n')


def test_config_schema_initialization_honors_verify_mode(monkeypatch):
    observed = []
    engine = object()
    monkeypatch.setenv(skylet_constants.ENV_VAR_STATE_DB_MIGRATION_MODE,
                       'verify')
    monkeypatch.setattr(
        skypilot_config.migration_utils, 'safe_alembic_upgrade',
        lambda actual_engine, section, revision, *, mode: observed.append(
            (actual_engine, section, revision, mode)))

    skypilot_config._create_table(engine)  # pylint: disable=protected-access

    assert observed == [
        (engine, skypilot_config.migration_utils.SKYPILOT_CONFIG_DB_NAME,
         skypilot_config.migration_utils.SKYPILOT_CONFIG_VERSION, 'verify')
    ]


def test_real_task_builder_catalog_failure_has_value_free_cli_output(
        monkeypatch, capsys, caplog):
    secret = 'secret-from-noisy-catalog-layer'
    monkeypatch.setattr(clouds.AWS, 'get_active_user_identity',
                        classmethod(lambda _cls: ['aws-user', '123456789012']))
    monkeypatch.setattr(clouds.AWS, 'get_vcpus_mem_from_instance_type',
                        classmethod(lambda _cls, _instance_type: (4, 16)))
    snapshot = _snapshot()
    snapshot['yaml_content'] = yaml_utils.dump_yaml_str(
        _task().to_yaml_config())
    monkeypatch.setattr(serve_state, 'system_recovery_persistence_available',
                        lambda: True)
    monkeypatch.setattr(serve_state,
                        'get_system_recovery_authorization_snapshot',
                        lambda _service_name: snapshot)
    _mock_central_postgres(monkeypatch)

    def _noisy_catalog(*_args, **_kwargs):
        print(secret)
        print(secret, file=sys.stderr)
        logging.error(secret)
        logging.log(logging.CRITICAL + 1, secret)
        raise RuntimeError(secret)

    monkeypatch.setattr(clouds.AWS, 'regions_with_offering',
                        classmethod(_noisy_catalog))

    exit_code = authorization.main([
        'generate', '--service-name', _SERVICE_NAME, '--profile-id',
        _PROFILE_ID, '--aws-account-id', '123456789012', '--aws-location',
        'us-east-1=us-east-1a', '--market-type', 'on_demand', '--instance-type',
        'g6.xlarge'
    ])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ''
    assert secret not in captured.err
    assert secret not in caplog.text
    assert captured.err == (
        'authorization-v3 bootstrap failed: The AWS offering catalog could '
        'not be evaluated.\n')
