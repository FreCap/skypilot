"""Strict immutable SkyServe platform projections."""
# pylint: disable=protected-access
import json
from unittest import mock

import pytest

from sky import exceptions
from sky import execution
from sky import resources as resources_lib
from sky import skypilot_config
from sky import task as task_lib
from sky.backends import backend_utils
from sky.provision.kubernetes import config as kubernetes_config
from sky.provision.kubernetes import instance as kubernetes_instance
from sky.serve import constants
from sky.serve import kubernetes_identity
from sky.serve import serve_state
from sky.serve.server import server
from sky.skylet import constants as skylet_constants
from sky.utils import common_utils
from sky.utils import schemas
from sky.utils import yaml_utils


def _storage_broker():
    return {
        'endpoint': 'https://storage-broker.int.boltz.bio/v3/grants',
        'audience': 'boltz-skyserve-worker',
        'api_version': 3,
        'grant_uri_prefix': 's3://boltz-skyserve-grants/prod',
        'authenticated_worker_role_arns': [
            'arn:aws:iam::123456789012:role/skyserve-worker-east',
            'arn:aws:iam::123456789012:role/skyserve-worker-phx',
        ],
        'kms_key_id': 'alias/skyserve-grants',
    }


def _controller_auth():
    return {
        'secret_name': 'skypilot-serve-lb-data-plane-auth',
        'secret_key': 'tokens',
        'mount_path': ('/etc/skypilot/serve-auth/lb-data-plane/tokens'),
    }


def _worker_role(context='east'):
    return f'arn:aws:iam::123456789012:role/skyserve-worker-{context}'


def _accelerator_scheduling(accelerator='H200'):
    return {
        'label_key': 'nvidia.com/gpu.product',
        'label_values': [f'NVIDIA-{accelerator}'],
        'resource_key': 'nvidia.com/gpu',
    }


def _kubernetes_api_error(status):
    return kubernetes_instance.kubernetes.api_exception()(status=status)


def test_storage_broker_schema_accepts_only_non_secret_descriptor():
    config = {'serve': {'storage_broker': _storage_broker()}}
    common_utils.validate_schema(config, schemas.get_config_schema(),
                                 'Invalid config')
    common_utils.validate_schema(
        {
            'workspaces': {
                'research': {
                    'serve': {
                        'storage_broker': _storage_broker()
                    }
                }
            }
        }, schemas.get_config_schema(), 'Invalid config')

    with pytest.raises(ValueError):
        common_utils.validate_schema(
            {
                'serve': {
                    'storage_broker': {
                        **_storage_broker(), 'bearer_token': 'secret'
                    }
                }
            }, schemas.get_config_schema(), 'Invalid config')


@pytest.mark.parametrize('overrides', [{
    'endpoint': 'http://broker.example/v2'
}, {
    'endpoint': 'https://user:password@broker.example/v2'
}, {
    'endpoint': 'https://broker.example:bad/v2'
}, {
    'grant_uri_prefix': 'https://bucket.example/prefix'
}, {
    'grant_uri_prefix': 's3://user:password@bucket/prefix'
}, {
    'grant_uri_prefix': 's3://bucket:443/prefix'
}, {
    'grant_uri_prefix': 's3://bucket/foo/../bar'
}, {
    'grant_uri_prefix': 's3://bucket/%2e%2e/bar'
}, {
    'grant_uri_prefix': 's3://bucket/pro%64'
}, {
    'grant_uri_prefix': 's3://bucket/%41'
}, {
    'grant_uri_prefix': 's3://bucket/foo%20bar'
}, {
    'grant_uri_prefix': 's3://bucket/foo%00bar'
}, {
    'grant_uri_prefix': 's3://bucket/foo%23bar'
}, {
    'grant_uri_prefix': 's3://bucket//prefix'
}, {
    'grant_uri_prefix': r's3://bucket/foo\bar'
}, {
    'grant_uri_prefix': 's3://bucket.-name/prefix'
}, {
    'grant_uri_prefix': 's3://bucket-.name/prefix'
}, {
    'grant_uri_prefix': 's3://xn--bucket/prefix'
}, {
    'grant_uri_prefix': 's3://sthree-bucket/prefix'
}, {
    'grant_uri_prefix': 's3://amzn-s3-demo-bucket/prefix'
}, {
    'grant_uri_prefix': 's3://bucket-s3alias/prefix'
}, {
    'grant_uri_prefix': 's3://bucket--ol-s3/prefix'
}, {
    'grant_uri_prefix': 's3://bucket.mrap/prefix'
}, {
    'grant_uri_prefix': 's3://bucket--x-s3/prefix'
}, {
    'grant_uri_prefix': 's3://bucket--table-s3/prefix'
}, {
    'grant_uri_prefix': 's3://bucket/prefix?X-Amz-Signature=secret'
}, {
    'api_version': 1
}, {
    'api_version': 2
}, {
    'api_version': 4
}, {
    'authenticated_worker_role_arns': ['not-an-iam-role']
}, {
    'kms_key_id': ''
}])
def test_storage_broker_validator_fails_closed(overrides):
    value = {**_storage_broker(), **overrides}
    with pytest.raises(ValueError):
        kubernetes_identity.validate_storage_broker_projection(value,
                                                               allow_none=False)


def test_storage_broker_worker_roles_are_ordered_unique():
    broker = _storage_broker()
    projected = kubernetes_identity.validate_storage_broker_projection(
        broker, allow_none=False)
    assert projected is not None
    assert projected['authenticated_worker_role_arns'] == broker[
        'authenticated_worker_role_arns']
    assert projected['authenticated_worker_role_arns'] is not broker[
        'authenticated_worker_role_arns']

    duplicate = broker['authenticated_worker_role_arns'][0]
    with pytest.raises(ValueError, match='duplicates'):
        kubernetes_identity.validate_storage_broker_projection(
            {
                **broker,
                'authenticated_worker_role_arns': [duplicate, duplicate],
            },
            allow_none=False)
    with pytest.raises(ValueError, match='between 1 and 16'):
        kubernetes_identity.validate_storage_broker_projection(
            {
                **broker,
                'authenticated_worker_role_arns': [
                    f'arn:aws:iam::123456789012:role/worker-{index}'
                    for index in range(17)
                ],
            },
            allow_none=False)


def test_storage_broker_projection_prefers_workspace_and_copies(monkeypatch):
    workspace_broker = {**_storage_broker(), 'audience': 'workspace'}
    global_broker = {**_storage_broker(), 'audience': 'global'}

    def _get_nested(*, keys, default_value):
        del default_value
        if keys == ('workspaces', 'research', 'serve', 'storage_broker'):
            return workspace_broker
        if keys == ('serve', 'storage_broker'):
            return global_broker
        return None

    monkeypatch.setattr(skypilot_config, 'get_nested', _get_nested)
    projected = kubernetes_identity.build_storage_broker_projection(
        workspace='research')
    assert projected == workspace_broker
    assert projected is not workspace_broker


def test_version_history_exposes_descriptor_without_secrets():
    broker = _storage_broker()
    record = {
        'pool': False,
        'elected_version': 1,
        'active_versions': [1],
    }
    version = {
        'version': 1,
        'spec': mock.Mock(autoscaling_policy_str=mock.Mock(return_value='p')),
        'yaml_content': 'envs:\n  TOKEN: secret\n',
        'submitted_yaml_content': 'envs:\n  TOKEN: secret\n',
        'created_at': 1.0,
        'created_by': 'user',
        'quarantined_at': None,
        'quarantine_reason': None,
        'controller_job_projection': None,
        'controller_work_cache': None,
        'worker_placement_projections': None,
        'storage_broker': broker,
    }
    with mock.patch.object(server.serve_state,
                           'get_service_from_name',
                           return_value=record), \
         mock.patch.object(server.serve_state,
                           'get_version_records',
                           return_value=[version]), \
         mock.patch.object(server.debug_dump_helpers,
                           'redact_task_yaml',
                           return_value='envs:\n  TOKEN: <redacted>\n'):
        result = server._service_version_history('svc')

    serialized = json.dumps(result)
    assert result['placement_projection_protocol_version'] == 1
    assert result['versions'][0]['storage_broker'] == broker
    assert 'secret' not in serialized
    assert 'X-Amz-Signature' not in serialized


def test_historical_version_has_null_projections():
    assert kubernetes_identity.validate_controller_job_projection(None) is None
    assert kubernetes_identity.validate_controller_work_cache_projection(
        None) is None
    assert kubernetes_identity.validate_worker_placement_projections(
        None) is None
    assert kubernetes_identity.validate_storage_broker_projection(None) is None


def test_controller_projection_requires_explicit_workspace():
    projection = {
        'workspace': 'controller',
        'kubernetes_context': 'east',
        'namespace': 'controller-system',
        'service_account_name': 'controller-sa',
        'priority_class_name': None,
        'lb_data_plane_auth': _controller_auth(),
    }
    assert kubernetes_identity.validate_controller_job_projection(
        projection, allow_none=False) == projection
    without_workspace = dict(projection)
    without_workspace.pop('workspace')
    with pytest.raises(ValueError, match='exactly'):
        kubernetes_identity.validate_controller_job_projection(
            without_workspace, allow_none=False)


def test_controller_auth_projection_is_reference_only_and_copied():
    auth = _controller_auth()
    projection = {
        'workspace': 'controller',
        'kubernetes_context': 'east',
        'namespace': 'controller-system',
        'service_account_name': 'controller-sa',
        'priority_class_name': None,
        'lb_data_plane_auth': auth,
    }
    projected = kubernetes_identity.validate_controller_job_projection(
        projection, allow_none=False)
    assert projected is not None
    assert projected['lb_data_plane_auth'] == auth
    assert projected['lb_data_plane_auth'] is not auth
    assert 'token' not in projected['lb_data_plane_auth']

    with pytest.raises(ValueError, match='exactly'):
        kubernetes_identity.validate_controller_job_projection(
            {
                **projection,
                'lb_data_plane_auth': {
                    **auth, 'token': 'must-never-be-persisted'
                },
            },
            allow_none=False)


def test_controller_projection_resolves_only_in_controller_workspace(
        monkeypatch):
    calls = []
    controller_cache = {
        'kind': 'empty_dir',
        'mount_path': '/mnt/controller-work',
        'required_bytes': 100,
        'required_inodes': 10,
        'size_limit_bytes': 120,
    }

    def _effective_config(*,
                          cloud,
                          keys,
                          region=None,
                          workspace=None,
                          default_value=None,
                          override_configs=None):
        del cloud, default_value, override_configs
        calls.append((keys, region, workspace))
        if keys == ('serve_controller_workspace',):
            assert workspace == 'inference'
            return 'controller'
        if keys == ('serve_controller_context',):
            return ('inference-context'
                    if workspace == 'inference' else 'east-context')
        if keys == ('serve_controller_work_cache',):
            assert region == 'east-context'
            assert workspace == 'controller'
            return controller_cache
        if keys == ('serve_controller_lb_data_plane_auth',):
            assert region == 'east-context'
            assert workspace == 'controller'
            auth = _controller_auth()
            return {
                'secret_name': auth['secret_name'],
                'secret_key': auth['secret_key'],
            }
        raise AssertionError((keys, region, workspace))

    location_calls = []

    def _project_location(context, overrides, workspace):
        location_calls.append((context, overrides, workspace))
        return {
            'kubernetes_context': context,
            'namespace': 'controller-system',
            'service_account_name': 'controller-sa',
            'priority_class_name': 'controller-priority',
        }

    monkeypatch.setattr(skypilot_config,
                        'get_effective_workspace_region_config',
                        _effective_config)
    monkeypatch.setattr(
        skypilot_config, 'get_nested', lambda *, keys, default_value: ({
            'kubernetes': {}
        } if keys == ('workspaces', 'controller') else default_value))
    monkeypatch.setattr(kubernetes_identity, '_project_location',
                        _project_location)

    identity = kubernetes_identity.build_controller_job_projection(
        mock.Mock(), workspace='inference')
    cache = kubernetes_identity.build_controller_work_cache_projection(
        mock.Mock(), workspace='inference')

    assert identity == {
        'workspace': 'controller',
        'kubernetes_context': 'east-context',
        'namespace': 'controller-system',
        'service_account_name': 'controller-sa',
        'priority_class_name': 'controller-priority',
        'lb_data_plane_auth': _controller_auth(),
    }
    assert cache == controller_cache
    assert location_calls == [('east-context', {}, 'controller')]
    assert (('serve_controller_work_cache',), 'east-context',
            'controller') in calls


def test_controller_context_without_workspace_fails_closed(monkeypatch):

    def _effective_config(*, keys, **_kwargs):
        if keys == ('serve_controller_workspace',):
            return None
        if keys == ('serve_controller_context',):
            return 'inference-context'
        raise AssertionError(keys)

    monkeypatch.setattr(skypilot_config,
                        'get_effective_workspace_region_config',
                        _effective_config)
    with pytest.raises(ValueError, match='explicit'):
        kubernetes_identity.build_controller_job_projection(
            mock.Mock(), workspace='inference')


def test_controller_workspace_must_be_distinct_and_configured(monkeypatch):

    def _effective_config(*, keys, workspace=None, **_kwargs):
        if keys == ('serve_controller_workspace',):
            return 'inference' if workspace == 'inference' else None
        if keys == ('serve_controller_context',):
            return None
        raise AssertionError(keys)

    monkeypatch.setattr(skypilot_config,
                        'get_effective_workspace_region_config',
                        _effective_config)
    with pytest.raises(ValueError, match='separate'):
        kubernetes_identity.build_controller_job_projection(
            mock.Mock(), workspace='inference')

    monkeypatch.setattr(
        skypilot_config, 'get_effective_workspace_region_config',
        lambda *, keys, **_kwargs: ('controller' if keys ==
                                    ('serve_controller_workspace',) else None))
    monkeypatch.setattr(skypilot_config, 'get_nested',
                        lambda *, keys, default_value: default_value)
    with pytest.raises(ValueError, match='not configured'):
        kubernetes_identity.build_controller_job_projection(
            mock.Mock(), workspace='inference')


def test_controller_workspace_is_server_owned_config():
    common_utils.validate_schema(
        {
            'kubernetes': {
                'serve_controller_workspace': 'controller',
            },
            'workspaces': {
                'controller': {
                    'kubernetes': {
                        'serve_controller_context': 'east',
                        'serve_controller_lb_data_plane_auth': {
                            'secret_name': 'skypilot-serve-lb-data-plane-auth',
                            'secret_key': 'tokens',
                        },
                    },
                },
            },
        }, schemas.get_config_schema(), 'Invalid config')
    assert ('kubernetes', 'serve_controller_workspace') in (
        skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    assert ('serve',
            'storage_broker') in (skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    assert ('kubernetes', 'serve_controller_lb_data_plane_auth') in (
        skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)


def test_controller_projection_requires_server_auth_reference(monkeypatch):

    def _effective_config(*, keys, workspace=None, **_kwargs):
        if keys == ('serve_controller_workspace',):
            return 'controller'
        if keys == ('serve_controller_context',):
            return 'east'
        if keys == ('serve_controller_lb_data_plane_auth',):
            return None
        raise AssertionError((keys, workspace))

    monkeypatch.setattr(skypilot_config,
                        'get_effective_workspace_region_config',
                        _effective_config)
    monkeypatch.setattr(
        skypilot_config, 'get_nested', lambda *, keys, default_value: ({
            'kubernetes': {}
        } if keys == ('workspaces', 'controller') else default_value))
    with pytest.raises(ValueError, match='serve_controller_lb_data_plane_auth'):
        kubernetes_identity.build_controller_job_projection(
            mock.Mock(), workspace='inference')


@pytest.mark.parametrize('priority', [None, 'preemptible-inference-low'])
def test_worker_priority_class_is_narrow_nullable_server_config(priority):
    priority_value = None if priority is None else -1000
    preemption_policy = None if priority is None else 'Never'
    common_utils.validate_schema(
        {
            'kubernetes': {
                'serve_worker_priority_class_name': priority,
                'serve_worker_priority_value': priority_value,
                'serve_worker_preemption_policy': preemption_policy,
                'serve_worker_accelerator_scheduling': {
                    'H200': _accelerator_scheduling(),
                },
            }
        }, schemas.get_config_schema(), 'Invalid config')
    assert ('kubernetes', 'serve_worker_priority_class_name') in (
        skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    assert ('kubernetes', 'serve_worker_priority_value') in (
        skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    assert ('kubernetes', 'serve_worker_preemption_policy') in (
        skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    assert ('kubernetes', 'serve_worker_accelerator_scheduling') in (
        skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)


def test_worker_accelerator_scheduling_freezes_verified_east_and_phx_labels(
        monkeypatch):
    scheduling_by_context = {
        'east': {
            'A100': _accelerator_scheduling('A100-SXM4-40GB'),
            'A100-80GB': _accelerator_scheduling('A100-SXM4-80GB'),
        },
        'phx': {
            'H200': _accelerator_scheduling(),
        },
    }
    monkeypatch.setattr(
        skypilot_config, 'get_effective_workspace_region_config',
        lambda *, region, keys, **_kwargs: scheduling_by_context[region]
        if keys == ('serve_worker_accelerator_scheduling',) else None)

    assert kubernetes_identity._project_accelerator_scheduling(
        'east', 'A100', 'inference') == {
            'label_key': 'nvidia.com/gpu.product',
            'label_values': ['NVIDIA-A100-SXM4-40GB'],
            'resource_key': 'nvidia.com/gpu',
        }
    assert kubernetes_identity._project_accelerator_scheduling(
        'east', 'a100-80gb',
        'inference')['label_values'] == ['NVIDIA-A100-SXM4-80GB']
    assert kubernetes_identity._project_accelerator_scheduling(
        'phx', 'H200', 'inference')['label_values'] == ['NVIDIA-H200']


@pytest.mark.parametrize('scheduling,match', [
    ({
        'H200': _accelerator_scheduling(),
        'h200': _accelerator_scheduling(),
    }, 'case-insensitively unique'),
    ({
        'H200': _accelerator_scheduling(),
        'H100': _accelerator_scheduling(),
    }, 'ambiguous label value'),
    ({
        'H200': {
            **_accelerator_scheduling(),
            'resource_key': 'gpu',
        },
    }, 'extended resource'),
    ({
        'H200': {
            **_accelerator_scheduling(),
            'label_values': ['NVIDIA-H200', 'NVIDIA-H200'],
        },
    }, 'must be unique'),
])
def test_worker_accelerator_scheduling_rejects_ambiguous_or_invalid_maps(
        scheduling, match):
    with pytest.raises(ValueError, match=match):
        kubernetes_identity._validate_accelerator_scheduling_map(scheduling)


def test_worker_priority_projection_ignores_pod_config(monkeypatch):
    monkeypatch.setattr(
        kubernetes_identity, '_project_location', lambda *_args, **_kwargs: {
            'kubernetes_context': 'east',
            'namespace': 'inference',
            'service_account_name': 'worker',
            'priority_class_name': 'raw-pod-config-priority',
        })
    monkeypatch.setattr(skypilot_config,
                        'get_effective_workspace_region_config',
                        lambda **_kwargs: None)

    projected = kubernetes_identity._project_worker_location(
        'east', {}, 'inference')

    assert projected['priority_class_name'] is None
    assert projected['pod_identity_role_arn'] is None


def test_worker_priority_projection_freezes_configured_expectation(monkeypatch):
    monkeypatch.setattr(
        kubernetes_identity, '_project_location', lambda *_args, **_kwargs: {
            'kubernetes_context': 'east',
            'namespace': 'inference',
            'service_account_name': 'worker',
            'priority_class_name': None,
        })
    monkeypatch.setattr(
        skypilot_config, 'get_effective_workspace_region_config',
        lambda *, keys, **_kwargs: {
            ('serve_worker_priority_class_name',): 'preemptible-inference-low',
            ('serve_worker_priority_value',): -1000,
            ('serve_worker_preemption_policy',): 'Never',
            ('serve_worker_pod_identity_role_arn',): _worker_role(),
        }[keys])

    projected = kubernetes_identity._project_worker_location(
        'east', {}, 'inference')

    assert projected['priority_class_name'] == 'preemptible-inference-low'
    assert projected['priority_value'] == -1000
    assert projected['preemption_policy'] == 'Never'
    assert projected['pod_identity_role_arn'] == _worker_role()


def test_worker_role_is_server_owned_and_strict():
    common_utils.validate_schema(
        {'kubernetes': {
            'serve_worker_pod_identity_role_arn': _worker_role(),
        }}, schemas.get_config_schema(), 'Invalid config')
    assert ('kubernetes', 'serve_worker_pod_identity_role_arn') in (
        skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    with pytest.raises(ValueError, match='AWS IAM role ARN'):
        kubernetes_identity.validate_worker_placement_projections(
            [{
                'candidate_id': 'kubernetes-0000',
                'kubernetes_context': 'east',
                'namespace': 'inference',
                'service_account_name': 'worker',
                'priority_class_name': None,
                'priority_value': None,
                'preemption_policy': None,
                'pod_identity_role_arn': 'not-an-arn',
                'accelerator_name': 'A100-80GB',
                'accelerator_count': 1,
                'accelerator_scheduling':
                    _accelerator_scheduling('A100-SXM4-80GB'),
                'cache': {
                    'kind': 'none'
                },
            }],
            allow_none=False)


def test_worker_projection_rejects_ambiguous_launch_selection_tuple():
    projection = {
        'candidate_id': 'kubernetes-0000',
        'kubernetes_context': 'east',
        'namespace': 'inference',
        'service_account_name': 'worker',
        'priority_class_name': None,
        'priority_value': None,
        'preemption_policy': None,
        'pod_identity_role_arn': _worker_role(),
        'accelerator_name': 'A100-80GB',
        'accelerator_count': 1,
        'accelerator_scheduling': _accelerator_scheduling('A100-SXM4-80GB'),
        'cache': {
            'kind': 'none'
        },
    }
    duplicate = {
        **projection,
        'candidate_id': 'kubernetes-0001',
        # Selection is case-insensitive, matching runtime lookup.
        'accelerator_name': 'a100-80gb',
    }

    with pytest.raises(ValueError, match='unique by Kubernetes context'):
        kubernetes_identity.validate_worker_placement_projections(
            [projection, duplicate], allow_none=False)


def test_worker_catalog_includes_h200_with_deterministic_candidate_id(
        monkeypatch):
    task = task_lib.Task.from_yaml_str('''
resources:
  infra: k8s/phx
  accelerators: H200:1
run: echo hi
''')
    candidates = [
        (0, kubernetes_identity.clouds.Kubernetes(), 'east', {
            'A100-80GB': 1
        }, {}),
        (2, kubernetes_identity.clouds.Kubernetes(), 'phx', {
            'H200': 1
        }, {}),
    ]
    monkeypatch.setattr(kubernetes_identity, '_catalog_candidates',
                        lambda *_args, **_kwargs: candidates)
    monkeypatch.setattr(
        kubernetes_identity, '_project_worker_location',
        lambda context, *_args: {
            'kubernetes_context': context,
            'namespace': 'rescluster-k8s-prod-east1-preemptible-inference',
            'service_account_name': f'{context}-worker',
            'priority_class_name': 'preemptible-inference-low',
            'priority_value': -1000,
            'preemption_policy': 'Never',
            'pod_identity_role_arn': _worker_role(context),
        })
    monkeypatch.setattr(kubernetes_identity, '_project_cache',
                        lambda context, _workspace: {'kind': 'none'})
    monkeypatch.setattr(
        kubernetes_identity, '_project_accelerator_scheduling',
        lambda _context, accelerator, _workspace: _accelerator_scheduling(
            'A100-SXM4-80GB' if accelerator == 'A100-80GB' else accelerator))

    projected = kubernetes_identity.build_worker_placement_projections(
        task, workspace='research', placement_catalog={})

    assert projected is not None
    assert [
        (item['candidate_id'], item['accelerator_name']) for item in projected
    ] == [('kubernetes-0000', 'A100-80GB'), ('kubernetes-0002', 'H200')]
    assert [item['pod_identity_role_arn'] for item in projected] == [
        _worker_role('east'),
        _worker_role('phx'),
    ]
    assert projected[1]['accelerator_scheduling'] == (_accelerator_scheduling())


@pytest.mark.parametrize('resource_config', [
    'accelerators: H200:1', '''any_of:
    - accelerators: H200:1
    - infra: k8s/phx
      accelerators: H200:1'''
])
def test_worker_projection_does_not_resolve_unconstrained_cloud_from_catalog(
        monkeypatch, resource_config):
    task = task_lib.Task.from_yaml_str(f'''
resources:
  {resource_config}
run: echo hi
''')
    catalog_location = kubernetes_identity.spot_placer.Location(
        cloud=kubernetes_identity.clouds.Kubernetes(),
        region='phx',
        zone=None,
        accelerators={'H200': 1},
        use_spot=False)
    placement_catalog = kubernetes_identity.spot_placer.PlacementCatalog(
        ((catalog_location, 0.0),)).to_dict()
    project_location = mock.Mock(side_effect=AssertionError(
        'catalog expansion must not create a trusted worker projection'))
    monkeypatch.setattr(kubernetes_identity, '_project_worker_location',
                        project_location)

    assert kubernetes_identity.build_worker_placement_projections(
        task, workspace='research', placement_catalog=placement_catalog) is None
    project_location.assert_not_called()


def test_catalog_shape_coverage_requires_exact_accelerator_and_count():
    task = task_lib.Task.from_yaml_str('''
resources:
  infra: k8s/phx
  accelerators: H200:1
run: echo hi
''')
    wrong_name = {
        'entries': [{
            'location': {
                'cloud': 'Kubernetes',
                'region': 'phx',
                'accelerators': {
                    'H100': 1
                },
            },
        }],
    }
    wrong_count = {
        'entries': [{
            'location': {
                'cloud': 'Kubernetes',
                'region': 'phx',
                'accelerators': {
                    'H200': 8
                },
            },
        }],
    }

    assert kubernetes_identity.catalog_missing_task_shapes(task,
                                                           wrong_name) == {
                                                               ('phx', 'H200',
                                                                1)
                                                           }
    assert kubernetes_identity.catalog_missing_task_shapes(task,
                                                           wrong_count) == {
                                                               ('phx', 'H200',
                                                                1)
                                                           }


def test_projected_worker_rejects_raw_task_volume_before_resolution():
    task = task_lib.Task.from_yaml_str('''
resources:
  infra: k8s/phx
  accelerators: H200:1
volumes:
  /mnt/caller: caller-rwx-pvc
run: echo hi
''')
    dag = execution.dag_utils.convert_entrypoint_to_dag(task)
    launch_context = {
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [{}],
    }

    with pytest.raises(exceptions.RequestCancelled,
                       match='volumes or volume_mounts'):
        execution._validate_projected_service_task_inputs(dag, launch_context)


def test_projected_worker_preserves_trusted_version_runtime_inputs():
    # These fields came from the already-committed operator-owned service
    # version, not a mutable campaign handoff.
    task = task_lib.Task()
    task.set_resources(
        resources_lib.Resources(cloud=kubernetes_identity.clouds.Kubernetes(),
                                region='phx',
                                accelerators={'H200': 1}))
    task.set_file_mounts({'/opt/model/release': 's3://models/release-v2'})
    task.update_secrets(
        {'MODEL_RUNTIME_TOKEN': 'operator-owned-version-secret'})
    dag = execution.dag_utils.convert_entrypoint_to_dag(task)
    launch_context = {
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [{}],
    }

    execution._validate_projected_service_task_inputs(dag, launch_context)

    assert task.file_mounts == {'/opt/model/release': 's3://models/release-v2'}
    assert 'MODEL_RUNTIME_TOKEN' in task.secrets


@pytest.mark.parametrize('task_kubernetes_config', [{
    'context_configs': {
        'phx': {
            'pod_config': {
                'spec': {
                    'containers': [{
                        'name': 'ray-node',
                        'securityContext': {
                            'privileged': True,
                        },
                    }],
                },
            },
        },
    },
}, {
    'remote_identity': 'LOCAL_CREDENTIALS',
}, {
    'context_configs': {
        'phx': {
            'namespace': 'caller-namespace',
        },
    },
}])
def test_projected_worker_rejects_task_kubernetes_identity_overrides(
        task_kubernetes_config):
    task = task_lib.Task.from_yaml_str('''
resources:
  infra: k8s/phx
  accelerators: H200:1
run: echo hi
''')
    resource = next(iter(task.resources))
    resource._cluster_config_overrides = {  # pylint: disable=protected-access
        'kubernetes': task_kubernetes_config,
    }
    dag = execution.dag_utils.convert_entrypoint_to_dag(task)
    launch_context = {
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [{}],
    }

    with pytest.raises(exceptions.RequestCancelled,
                       match='pod_config, namespace, or remote_identity'):
        execution._validate_projected_service_task_inputs(dag, launch_context)


def test_projected_worker_rejects_malformed_context_config():
    task = task_lib.Task.from_yaml_str('''
resources:
  infra: k8s/phx
  accelerators: H200:1
run: echo hi
''')
    resource = next(iter(task.resources))
    resource._cluster_config_overrides = {  # pylint: disable=protected-access
        'kubernetes': {
            'context_configs': {
                'phx': 'not-a-mapping',
            },
        },
    }
    dag = execution.dag_utils.convert_entrypoint_to_dag(task)
    launch_context = {
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [{}],
    }

    with pytest.raises(exceptions.RequestCancelled,
                       match='context config to be a mapping'):
        execution._validate_projected_service_task_inputs(dag, launch_context)


def test_runtime_cache_uses_final_h200_choice_from_heterogeneous_task():
    task = task_lib.Task()
    east = kubernetes_identity.clouds.Kubernetes()
    phx = kubernetes_identity.clouds.Kubernetes()
    task.set_resources([
        resources_lib.Resources(cloud=east,
                                region='east',
                                accelerators={'A100-80GB': 1}),
        resources_lib.Resources(cloud=phx,
                                region='phx',
                                accelerators={'H200': 1}),
    ])
    task.best_resources = next(
        resource for resource in task.resources if resource.region == 'phx')
    task.update_envs({'SKYPILOT_SERVE_CACHE_KIND': 'caller-env'})
    task.update_secrets({
        'SKYPILOT_SERVE_CACHE_KIND': 'caller-secret',
        'SKYPILOT_SERVE_CACHE_EVIL': 'caller-secret',
    })
    projections = [{
        'candidate_id': 'kubernetes-0000',
        'kubernetes_context': 'east',
        'namespace': 'inference',
        'service_account_name': 'east-worker',
        'priority_class_name': 'preemptible-inference-low',
        'priority_value': -1000,
        'preemption_policy': 'Never',
        'pod_identity_role_arn': _worker_role('east'),
        'accelerator_name': 'A100-80GB',
        'accelerator_count': 1,
        'accelerator_scheduling': _accelerator_scheduling('A100-SXM4-80GB'),
        'cache': {
            'kind': 'none'
        },
    }, {
        'candidate_id': 'kubernetes-0001',
        'kubernetes_context': 'phx',
        'namespace': 'inference',
        'service_account_name': 'phx-worker',
        'priority_class_name': 'preemptible-inference-low',
        'priority_value': -1000,
        'preemption_policy': 'Never',
        'pod_identity_role_arn': _worker_role('phx'),
        'accelerator_name': 'H200',
        'accelerator_count': 1,
        'accelerator_scheduling': _accelerator_scheduling(),
        'cache': {
            'kind': 'node_local',
            'mount_path': '/mnt/sky-cache',
            'volume_name': 'phx-cache',
            'host_path': '/mnt/local-nvme/sky-cache',
            'attestation': {
                'attestation_id': 'phx-cache-v1',
                'device_source_pattern': '^/dev/nvme[0-9]+n[0-9]+$',
                'filesystem_type': 'xfs',
                'required_bytes_per_replica': 100,
                'required_inodes_per_replica': 10,
                'max_replicas_per_node': 1,
                'reserved_bytes_per_node': 0,
                'reserved_inodes_per_node': 0,
                'usable_bytes_per_node': 100,
                'usable_inodes_per_node': 10,
            },
        },
    }]
    launch_context = {
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: projections,
    }

    execution._apply_service_worker_cache_to_task(task, launch_context,
                                                  task.best_resources)

    assert task.envs['SKYPILOT_SERVE_CACHE_KIND'] == 'node_local'
    assert task.envs['SKYPILOT_SERVE_CACHE_MOUNT_PATH'] == '/mnt/sky-cache'
    assert task.envs['SKYPILOT_SERVE_CACHE_ATTESTATION_ID'] == 'phx-cache-v1'
    assert not any(
        key.startswith(kubernetes_identity.CACHE_ENV_PREFIX)
        for key in task.secrets)
    assert task.envs_and_secrets['SKYPILOT_SERVE_CACHE_KIND'] == 'node_local'


def test_storage_broker_must_list_every_projected_worker_role():
    worker = {
        'candidate_id': 'kubernetes-0000',
        'kubernetes_context': 'east',
        'namespace': 'inference',
        'service_account_name': 'worker',
        'priority_class_name': None,
        'priority_value': None,
        'preemption_policy': None,
        'pod_identity_role_arn': 'arn:aws:iam::123456789012:role/unrelated-worker',
        'accelerator_name': 'A100-80GB',
        'accelerator_count': 1,
        'accelerator_scheduling': _accelerator_scheduling('A100-SXM4-80GB'),
        'cache': {
            'kind': 'none'
        },
    }
    with pytest.raises(ValueError, match='listed by the immutable storage'):
        serve_state._validated_placement_projections(None, None, [worker],
                                                     _storage_broker())


def test_final_kubernetes_yaml_enforces_platform_identity_and_cache():
    projection = {
        'candidate_id': 'kubernetes-0002',
        'kubernetes_context': 'phx',
        'namespace': 'rescluster-k8s-prod-east1-preemptible-inference',
        'service_account_name': 'phx-worker',
        'priority_class_name': 'preemptible-inference-low',
        'priority_value': -1000,
        'preemption_policy': 'Never',
        'pod_identity_role_arn': _worker_role('phx'),
        'accelerator_name': 'H200',
        'accelerator_count': 1,
        'accelerator_scheduling': _accelerator_scheduling(),
        'cache': {
            'kind': 'none'
        },
    }
    cluster_yaml = {
        'provider': {
            'context': 'caller-context',
            'namespace': 'caller-namespace',
        },
        'available_node_types': {
            'ray.head.default': {
                'node_config': {
                    'metadata': {
                        'annotations': {
                            'fresh-platform-annotation': 'trusted',
                        },
                    },
                    'spec': {
                        'serviceAccountName': 'caller-sa',
                        'priorityClassName': 'caller-priority',
                        'containers': [{
                            'name': 'ray-node',
                            'env': [{
                                'name': 'SKYPILOT_SERVE_CACHE_EVIL',
                                'value': 'caller-value',
                            }],
                        }, {
                            'name': 'sidecar',
                            'env': [{
                                'name': 'SKYPILOT_SERVE_CACHE_EVIL',
                                'value': 'caller-value',
                            }],
                        }],
                    }
                }
            }
        },
    }

    backend_utils._enforce_worker_projection_on_kubernetes_yaml(
        cluster_yaml, projection)

    pod_spec = cluster_yaml['available_node_types']['ray.head.default'][
        'node_config']['spec']
    assert cluster_yaml['provider'] == {
        'context': 'phx',
        'namespace': 'rescluster-k8s-prod-east1-preemptible-inference',
        'serve_worker_expected_priority_class_name': 'preemptible-inference-low',
        'serve_worker_expected_priority_value': -1000,
        'serve_worker_expected_preemption_policy': 'Never',
        'serve_worker_expected_service_account_name': 'phx-worker',
        'serve_worker_expected_accelerator_label_key': 'nvidia.com/gpu.product',
        'serve_worker_expected_accelerator_label_values': ['NVIDIA-H200'],
        'serve_worker_expected_accelerator_resource_key': 'nvidia.com/gpu',
        'serve_worker_expected_accelerator_count': 1,
    }
    assert pod_spec['serviceAccountName'] == 'phx-worker'
    assert pod_spec['priorityClassName'] == 'preemptible-inference-low'
    assert pod_spec['affinity']['nodeAffinity'][
        'requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][
            0]['matchExpressions'][-1] == {
                'key': 'nvidia.com/gpu.product',
                'operator': 'In',
                'values': ['NVIDIA-H200'],
            }
    assert pod_spec['containers'][0]['resources'] == {
        'requests': {
            'nvidia.com/gpu': 1,
        },
        'limits': {
            'nvidia.com/gpu': 1,
        },
    }
    assert pod_spec['containers'][0]['env'] == [{
        'name': 'SKYPILOT_SERVE_CACHE_KIND',
        'value': 'none',
    }]
    assert not pod_spec['containers'][1]['env']


def test_legacy_yaml_restore_cannot_replace_projected_identity_or_cache():
    projection = {
        'candidate_id': 'kubernetes-0002',
        'kubernetes_context': 'phx',
        'namespace': 'inference',
        'service_account_name': 'phx-worker',
        'priority_class_name': 'preemptible-inference-low',
        'priority_value': -1000,
        'preemption_policy': 'Never',
        'pod_identity_role_arn': _worker_role('phx'),
        'accelerator_name': 'H200',
        'accelerator_count': 1,
        'accelerator_scheduling': _accelerator_scheduling(),
        'cache': {
            'kind': 'node_local',
            'mount_path': '/mnt/sky-cache',
            'volume_name': 'phx-cache',
            'host_path': '/mnt/local-nvme/sky-cache',
            'attestation': {
                'attestation_id': 'phx-cache-v1',
                'device_source_pattern': '^/dev/nvme[0-9]+n[0-9]+$',
                'filesystem_type': 'xfs',
                'required_bytes_per_replica': 100,
                'required_inodes_per_replica': 10,
                'max_replicas_per_node': 1,
                'reserved_bytes_per_node': 0,
                'reserved_inodes_per_node': 0,
                'usable_bytes_per_node': 100,
                'usable_inodes_per_node': 10,
            },
        },
    }
    new_config = {
        'provider': {
            'type': 'kubernetes',
            'context': 'phx',
            'namespace': 'inference',
        },
        'available_node_types': {
            'ray_head_default': {
                'node_config': {
                    'metadata': {
                        'annotations': {
                            'fresh-platform-annotation': 'trusted',
                        },
                    },
                    'spec': {
                        'containers': [{
                            'name': 'ray-node',
                            'env': [{
                                'name': 'FRESH',
                                'value': 'yes',
                            }],
                        }],
                    },
                },
            },
        },
    }
    backend_utils._enforce_worker_projection_on_kubernetes_yaml(
        new_config, projection)
    old_config = {
        'provider': {
            'type': 'kubernetes',
            'context': 'old-context',
            'namespace': 'old-namespace',
        },
        'available_node_types': {
            'ray_head_default': {
                'node_config': {
                    'metadata': {
                        'annotations': {
                            'caller.example/inject-sidecar': 'true',
                        },
                    },
                    'spec': {
                        'serviceAccountName': 'old-sa',
                        'priorityClassName': 'old-priority',
                        'containers': [{
                            'name': 'ray-node',
                            'env': [{
                                'name': 'SKYPILOT_SERVE_CACHE_KIND',
                                'value': 'old-cache',
                            }],
                            'volumeMounts': [{
                                'name': 'old-cache',
                                'mountPath': '/mnt/old-cache',
                            }],
                        }],
                        'volumes': [{
                            'name': 'old-cache',
                            'hostPath': {
                                'path': '/mnt/old-cache',
                            },
                        }],
                    },
                },
            },
        },
    }
    restored = backend_utils._replace_yaml_dicts(
        yaml_utils.dump_yaml_str(new_config),
        yaml_utils.dump_yaml_str(old_config),
        backend_utils._RAY_YAML_KEYS_TO_RESTORE_FOR_BACK_COMPATIBILITY,
        backend_utils._RAY_YAML_KEYS_TO_RESTORE_EXCEPTIONS)
    assert yaml_utils.safe_load(restored)['provider']['context'] == (
        'old-context')

    enforced = backend_utils._restore_projected_worker_kubernetes_fields(
        yaml_utils.dump_yaml_str(new_config), restored, projection)
    final_config = yaml_utils.safe_load(enforced)
    final_spec = final_config['available_node_types']['ray_head_default'][
        'node_config']['spec']

    assert final_config['provider']['context'] == 'phx'
    assert final_config['provider']['namespace'] == 'inference'
    assert final_config['provider'][
        'serve_worker_expected_service_account_name'] == 'phx-worker'
    assert final_config['provider'][
        'serve_worker_expected_priority_class_name'] == (
            'preemptible-inference-low')
    assert final_config['provider'][
        'serve_worker_expected_priority_value'] == -1000
    assert final_config['provider'][
        'serve_worker_expected_preemption_policy'] == 'Never'
    assert final_spec['serviceAccountName'] == 'phx-worker'
    assert final_spec['priorityClassName'] == 'preemptible-inference-low'
    assert final_spec['affinity']['nodeAffinity'][
        'requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][
            0]['matchExpressions'][-1] == {
                'key': 'nvidia.com/gpu.product',
                'operator': 'In',
                'values': ['NVIDIA-H200'],
            }
    assert final_spec['containers'][0]['resources']['requests'][
        'nvidia.com/gpu'] == 1
    assert final_config['available_node_types']['ray_head_default'][
        'node_config']['metadata'] == {
            'annotations': {
                'fresh-platform-annotation': 'trusted',
            },
        }
    assert final_spec['volumes'] == [{
        'name': 'phx-cache',
        'hostPath': {
            'path': '/mnt/local-nvme/sky-cache',
            'type': 'Directory',
        },
    }]
    ray_node = final_spec['containers'][0]
    assert ray_node['volumeMounts'] == [{
        'name': 'phx-cache',
        'mountPath': '/mnt/sky-cache',
    }]
    assert any(entry == {
        'name': 'SKYPILOT_SERVE_CACHE_KIND',
        'value': 'node_local',
    } for entry in ray_node['env'])
    serialized = yaml_utils.dump_yaml_str(final_config)
    assert 'old-context' not in serialized
    assert 'old-cache' not in serialized
    assert 'old-sa' not in serialized
    assert 'inject-sidecar' not in serialized


def test_legacy_yaml_restore_rejects_changed_node_type_set():
    projection = {
        'candidate_id': 'kubernetes-0000',
        'kubernetes_context': 'phx',
        'namespace': 'inference',
        'service_account_name': 'phx-worker',
        'priority_class_name': None,
        'priority_value': None,
        'preemption_policy': None,
        'pod_identity_role_arn': _worker_role('phx'),
        'accelerator_name': 'H200',
        'accelerator_count': 1,
        'accelerator_scheduling': _accelerator_scheduling(),
        'cache': {
            'kind': 'none'
        },
    }
    node = {
        'node_config': {
            'spec': {
                'containers': [{
                    'name': 'ray-node'
                }],
            },
        },
    }
    new_config = {
        'provider': {},
        'available_node_types': {
            'ray_head_default': node,
            'fresh_worker': node,
        },
    }
    restored_config = {
        'provider': {},
        'available_node_types': {
            'ray_head_default': node,
        },
    }

    with pytest.raises(exceptions.InvalidCloudConfigs,
                       match='changed node types'):
        backend_utils._restore_projected_worker_kubernetes_fields(
            yaml_utils.dump_yaml_str(new_config),
            yaml_utils.dump_yaml_str(restored_config), projection)


def test_admitted_worker_priority_mismatch_is_deleted(monkeypatch):
    pod = mock.MagicMock()
    pod.metadata.name = 'replica-head'
    pod.spec.priority_class_name = 'unexpected-priority'
    core_api = mock.MagicMock()
    core_api.read_namespaced_pod.side_effect = _kubernetes_api_error(404)
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)

    with pytest.raises(kubernetes_config.KubernetesError,
                       match='immutable platform placement contract'):
        kubernetes_instance._attest_serve_worker_priority_class(
            pod, 'inference', 'east', 'preemptible-inference-low')

    core_api.delete_namespaced_pod.assert_called_once_with(
        'replica-head',
        'inference',
        grace_period_seconds=0,
        _request_timeout=kubernetes_config.DELETION_TIMEOUT)


def test_admitted_worker_priority_added_to_projected_null_is_deleted(
        monkeypatch):
    pod = mock.MagicMock()
    pod.metadata.name = 'replica-head'
    pod.spec.priority_class_name = 'webhook-added-priority'
    core_api = mock.MagicMock()
    core_api.read_namespaced_pod.side_effect = _kubernetes_api_error(404)
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)

    with pytest.raises(kubernetes_config.KubernetesError,
                       match=r"has priority class 'webhook-added-priority'; "
                       r'expected None'):
        kubernetes_instance._attest_serve_worker_priority_class(
            pod, 'inference', 'east', None)

    core_api.delete_namespaced_pod.assert_called_once_with(
        'replica-head',
        'inference',
        grace_period_seconds=0,
        _request_timeout=kubernetes_config.DELETION_TIMEOUT)


def test_admitted_worker_without_priority_class_accepts_kubernetes_defaults():
    pod = mock.MagicMock()
    pod.spec.priority_class_name = None
    pod.spec.priority = 0
    pod.spec.preemption_policy = 'PreemptLowerPriority'

    kubernetes_instance._attest_serve_worker_priority_class(
        pod, 'inference', 'east', None, None, None)


def _admitted_accelerator_pod(*,
                              label_values=None,
                              resource_count=1,
                              include_affinity=True,
                              alternate_term=False):
    expression = mock.Mock(key='nvidia.com/gpu.product',
                           operator='In',
                           values=(label_values or ['NVIDIA-H200']))
    terms = [mock.Mock(match_expressions=[expression])]
    if alternate_term:
        terms.append(mock.Mock(match_expressions=[]))
    required = mock.Mock(node_selector_terms=terms)
    node_affinity = mock.Mock(
        required_during_scheduling_ignored_during_execution=required)
    affinity = mock.Mock(node_affinity=node_affinity)
    resources = mock.Mock(requests={'nvidia.com/gpu': resource_count},
                          limits={'nvidia.com/gpu': resource_count})
    container = mock.Mock()
    container.name = 'ray-node'
    container.resources = resources
    pod = mock.Mock()
    pod.metadata.name = 'replica-head'
    pod.spec.containers = [container]
    pod.spec.affinity = affinity if include_affinity else None
    return pod


def test_admitted_worker_accelerator_scheduling_contract_is_accepted():
    kubernetes_instance._attest_serve_worker_accelerator_scheduling(
        _admitted_accelerator_pod(), 'inference', 'phx',
        'nvidia.com/gpu.product', ['NVIDIA-H200'], 'nvidia.com/gpu', 1)


@pytest.mark.parametrize('pod', [
    _admitted_accelerator_pod(include_affinity=False),
    _admitted_accelerator_pod(alternate_term=True),
    _admitted_accelerator_pod(label_values=['NVIDIA-H100']),
    _admitted_accelerator_pod(resource_count=2),
])
def test_admitted_worker_accelerator_mutation_is_deleted(monkeypatch, pod):
    core_api = mock.MagicMock()
    core_api.read_namespaced_pod.side_effect = _kubernetes_api_error(404)
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)

    with pytest.raises(kubernetes_config.KubernetesError,
                       match='accelerator scheduling contract'):
        kubernetes_instance._attest_serve_worker_accelerator_scheduling(
            pod, 'inference', 'phx', 'nvidia.com/gpu.product', ['NVIDIA-H200'],
            'nvidia.com/gpu', 1)

    core_api.delete_namespaced_pod.assert_called_once()


@pytest.mark.parametrize(('field', 'actual', 'message'), [
    ('priority', 0, 'numeric priority'),
    ('preemption_policy', 'PreemptLowerPriority', 'preemption policy'),
])
def test_admitted_worker_priority_semantics_mismatch_is_deleted(
        monkeypatch, field, actual, message):
    pod = mock.MagicMock()
    pod.metadata.name = 'replica-head'
    pod.spec.priority_class_name = 'preemptible-inference-low'
    pod.spec.priority = -1000
    pod.spec.preemption_policy = 'Never'
    setattr(pod.spec, field, actual)
    core_api = mock.MagicMock()
    core_api.read_namespaced_pod.side_effect = _kubernetes_api_error(404)
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)

    with pytest.raises(kubernetes_config.KubernetesError, match=message):
        kubernetes_instance._attest_serve_worker_priority_class(
            pod, 'inference', 'phx', 'preemptible-inference-low', -1000,
            'Never')

    core_api.delete_namespaced_pod.assert_called_once_with(
        'replica-head',
        'inference',
        grace_period_seconds=0,
        _request_timeout=kubernetes_config.DELETION_TIMEOUT)


def test_admitted_worker_service_account_mismatch_is_deleted(monkeypatch):
    pod = mock.MagicMock()
    pod.metadata.name = 'replica-head'
    pod.spec.service_account_name = 'webhook-mutated-sa'
    core_api = mock.MagicMock()
    core_api.read_namespaced_pod.side_effect = _kubernetes_api_error(404)
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)

    with pytest.raises(kubernetes_config.KubernetesError,
                       match=r"has service account 'webhook-mutated-sa'; "
                       r"expected 'phx-worker'"):
        kubernetes_instance._attest_serve_worker_service_account(
            pod, 'inference', 'phx', 'phx-worker')

    core_api.delete_namespaced_pod.assert_called_once_with(
        'replica-head',
        'inference',
        grace_period_seconds=0,
        _request_timeout=kubernetes_config.DELETION_TIMEOUT)


def test_admitted_worker_mismatch_retries_delete_and_confirms_absence(
        monkeypatch):
    pod = mock.MagicMock()
    pod.metadata.name = 'replica-head'
    pod.spec.service_account_name = 'webhook-mutated-sa'
    core_api = mock.MagicMock()
    core_api.delete_namespaced_pod.side_effect = [
        _kubernetes_api_error(500), None
    ]
    core_api.read_namespaced_pod.side_effect = [pod, _kubernetes_api_error(404)]
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)
    monkeypatch.setattr(kubernetes_instance.kubernetes_utils.time, 'sleep',
                        lambda *_args, **_kwargs: None)

    with pytest.raises(kubernetes_config.KubernetesError,
                       match='Its absence was confirmed'):
        kubernetes_instance._attest_serve_worker_service_account(
            pod, 'inference', 'phx', 'phx-worker')

    assert core_api.delete_namespaced_pod.call_count == 2
    assert core_api.read_namespaced_pod.call_count == 2


def test_admitted_worker_mismatch_accepts_lost_delete_ack_when_absent(
        monkeypatch):
    pod = mock.MagicMock()
    pod.metadata.name = 'replica-head'
    pod.spec.service_account_name = 'webhook-mutated-sa'
    core_api = mock.MagicMock()
    core_api.delete_namespaced_pod.side_effect = _kubernetes_api_error(500)
    core_api.read_namespaced_pod.side_effect = _kubernetes_api_error(404)
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)

    with pytest.raises(kubernetes_config.KubernetesError,
                       match='Its absence was confirmed'):
        kubernetes_instance._attest_serve_worker_service_account(
            pod, 'inference', 'phx', 'phx-worker')

    core_api.delete_namespaced_pod.assert_called_once()
    core_api.read_namespaced_pod.assert_called_once()


def test_admitted_worker_mismatch_treats_already_gone_as_confirmed(monkeypatch):
    pod = mock.MagicMock()
    pod.metadata.name = 'replica-head'
    pod.spec.service_account_name = 'webhook-mutated-sa'
    core_api = mock.MagicMock()
    core_api.delete_namespaced_pod.side_effect = _kubernetes_api_error(404)
    core_api.read_namespaced_pod.side_effect = _kubernetes_api_error(404)
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)

    with pytest.raises(kubernetes_config.KubernetesError,
                       match='Its absence was confirmed'):
        kubernetes_instance._attest_serve_worker_service_account(
            pod, 'inference', 'phx', 'phx-worker')

    core_api.delete_namespaced_pod.assert_called_once()
    core_api.read_namespaced_pod.assert_called_once()


def test_admitted_worker_mismatch_reports_non_api_cleanup_failure(monkeypatch):
    pod = mock.MagicMock()
    pod.metadata.name = 'replica-head'
    pod.spec.service_account_name = 'webhook-mutated-sa'
    core_api = mock.MagicMock()
    core_api.delete_namespaced_pod.side_effect = RuntimeError('transport broke')
    core_api.read_namespaced_pod.return_value = pod
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)
    monkeypatch.setattr(kubernetes_instance.time, 'sleep',
                        lambda *_args, **_kwargs: None)

    with pytest.raises(
            kubernetes_config.KubernetesError,
            match=r'Cleanup could not confirm Pod absence.*transport broke'):
        kubernetes_instance._attest_serve_worker_service_account(
            pod, 'inference', 'phx', 'phx-worker')

    assert core_api.delete_namespaced_pod.call_count == 3
    assert core_api.read_namespaced_pod.call_count == 3


def test_final_kubernetes_yaml_requires_one_runtime_container():
    projection = {
        'candidate_id': 'kubernetes-0002',
        'kubernetes_context': 'phx',
        'namespace': 'inference',
        'service_account_name': 'phx-worker',
        'priority_class_name': None,
        'priority_value': None,
        'preemption_policy': None,
        'pod_identity_role_arn': _worker_role('phx'),
        'accelerator_name': 'H200',
        'accelerator_count': 1,
        'accelerator_scheduling': _accelerator_scheduling(),
        'cache': {
            'kind': 'none'
        },
    }
    cluster_yaml = {
        'provider': {},
        'available_node_types': {
            'ray.head.default': {
                'node_config': {
                    'spec': {
                        'containers': [{
                            'name': 'not-the-runtime',
                        }],
                    },
                },
            },
        },
    }
    with pytest.raises(backend_utils.exceptions.InvalidCloudConfigs,
                       match='exactly one'):
        backend_utils._enforce_worker_projection_on_kubernetes_yaml(
            cluster_yaml, projection)
