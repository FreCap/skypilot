"""Strict immutable SkyServe platform projections."""
# pylint: disable=protected-access
import copy
import hashlib
import json
from unittest import mock

import pytest

from sky import exceptions
from sky import execution
from sky import resources as resources_lib
from sky import skypilot_config
from sky import task as task_lib
from sky.adaptors import kubernetes as kubernetes_adaptor
from sky.backends import backend_utils
from sky.data import storage as storage_lib
from sky.provision.kubernetes import config as kubernetes_config
from sky.provision.kubernetes import instance as kubernetes_instance
from sky.provision.kubernetes import pod_spec as kubernetes_pod_spec
from sky.serve import constants
from sky.serve import kubernetes_identity
from sky.skylet import constants as skylet_constants
from sky.utils import common_utils
from sky.utils import resources_utils
from sky.utils import schemas
from sky.utils import yaml_utils


def _controller_auth():
    return {
        'secret_name': 'skypilot-serve-lb-data-plane-auth',
        'secret_key': 'tokens',
        'mount_path': ('/etc/skypilot/serve-auth/lb-data-plane/tokens'),
    }


def _worker_role(context='east'):
    return f'arn:aws:iam::123456789012:role/skyserve-worker-{context}'


_USE_DEFAULT_WORKER_ROLE = object()


def _accelerator_scheduling(accelerator='H200'):
    return {
        'label_key': 'nvidia.com/gpu.product',
        'label_values': [f'NVIDIA-{accelerator}'],
        'resource_key': 'nvidia.com/gpu',
    }


def _node_local_cache():
    return {
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
    }


def _worker_projection(*,
                       context='phx',
                       accelerator='H200',
                       protocol_version=1,
                       kueue_admission=None,
                       scheduler_name='default-scheduler',
                       provision_timeout=-1,
                       scratch=None,
                       pod_identity_role_arn=_USE_DEFAULT_WORKER_ROLE):
    if pod_identity_role_arn is _USE_DEFAULT_WORKER_ROLE:
        pod_identity_role_arn = _worker_role(context)
    projection = {
        'candidate_id': 'kubernetes-0000',
        'kubernetes_context': context,
        'namespace': 'inference',
        'service_account_name': f'{context}-worker',
        'priority_class_name': 'preemptible-inference-low',
        'priority_value': -1000,
        'preemption_policy': 'Never',
        'pod_identity_role_arn': pod_identity_role_arn,
        'accelerator_name': accelerator,
        'accelerator_count': 1,
        'accelerator_scheduling': _accelerator_scheduling(accelerator),
        'cache': {
            'kind': 'none'
        },
    }
    if protocol_version in (2, 3, 4, 5, 6, 7):
        projection = {
            'projection_version': protocol_version,
            **projection,
            'scheduler_name': scheduler_name,
            'kueue_admission': kueue_admission,
        }
    if protocol_version in (3, 4, 5, 6, 7):
        projection['provision_timeout'] = provision_timeout
        projection['scratch'] = ({
            'kind': 'none'
        } if scratch is None else scratch)
    return projection


def _current_bootstrap_script() -> str:
    lines = [
        'canonical bootstrap',
        f'# {kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENV_MARKER}',
    ]
    lines.extend(f'export {key}={json.dumps(value)}' for key, value in sorted(
        kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT.items()))
    return '\n'.join(lines)


def _current_bootstrap_pod_environment() -> list[dict[str, str]]:
    return [{
        'name': key,
        'value': value,
    } for key, value in sorted(
        kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT.items())]


def _kubernetes_api_error(status):
    return kubernetes_instance.kubernetes.api_exception()(status=status)


def test_historical_version_has_null_projections():
    assert kubernetes_identity.validate_controller_job_projection(None) is None
    assert kubernetes_identity.validate_controller_work_cache_projection(
        None) is None
    assert kubernetes_identity.validate_worker_placement_projections(
        None) is None


def test_worker_projection_protocol_v2_is_explicit_and_v1_is_isolated():
    v1 = _worker_projection()
    admission = {
        'local_queue_name': 'inference',
        'workload_priority_class_name': 'inference-low',
    }
    v2 = _worker_projection(protocol_version=2, kueue_admission=admission)

    assert kubernetes_identity.worker_projection_protocol_version(v1) == 1
    assert kubernetes_identity.worker_projection_protocol_version(v2) == 2
    assert kubernetes_identity.validate_worker_placement_projections(
        [v1], require_protocol_version=1) == [v1]
    assert kubernetes_identity.validate_worker_placement_projections(
        [v2], require_protocol_version=2) == [v2]
    with pytest.raises(ValueError, match='does not satisfy required version'):
        kubernetes_identity.validate_worker_placement_projections(
            [v1], require_protocol_version=2)
    with pytest.raises(ValueError, match='must not mix protocol versions'):
        kubernetes_identity.validate_worker_placement_projections([v1, v2])
    with pytest.raises(ValueError, match='protocol-v1 keys'):
        kubernetes_identity.worker_projection_protocol_version({
            **v2, 'unknown': True
        })
    with pytest.raises(ValueError, match='protocol-v2 keys'):
        kubernetes_identity.worker_projection_protocol_version({
            **v2, 'provision_timeout': -1
        })


def test_worker_projection_protocol_v7_is_canonical_and_older_are_isolated():
    admission = {
        'local_queue_name': 'inference',
        'workload_priority_class_name': 'inference-low',
    }
    v2 = _worker_projection(protocol_version=2, kueue_admission=admission)
    v3 = _worker_projection(protocol_version=3,
                            kueue_admission=admission,
                            scratch={
                                'kind': 'memory',
                                'mount_path': '/tmp',
                                'volume_name': 'skypilot-serve-worker-tmp',
                                'size_limit_bytes': 20 * 1024**3,
                            })
    v4 = {
        **v3,
        'projection_version': 4,
    }
    v5 = {
        **v3,
        'projection_version': 5,
    }
    v6 = {
        **v3,
        'projection_version': 6,
    }
    v7 = {
        **v3,
        'projection_version': 7,
    }

    assert kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENV_MARKER == (
        'SKYPILOT_SERVE_WORKER_BOOTSTRAP_ENV_V7')
    assert kubernetes_pod_spec.SERVE_WORKER_LEGACY_BOOTSTRAP_ENV_MARKERS == {
        'SKYPILOT_SERVE_WORKER_BOOTSTRAP_ENV_V5',
        'SKYPILOT_SERVE_WORKER_BOOTSTRAP_ENV_V6',
    }
    assert kubernetes_identity.worker_projection_protocol_version(v2) == 2
    assert kubernetes_identity.worker_projection_protocol_version(v3) == 3
    assert kubernetes_identity.worker_projection_protocol_version(v4) == 4
    assert kubernetes_identity.worker_projection_protocol_version(v5) == 5
    assert kubernetes_identity.worker_projection_protocol_version(v6) == 6
    assert kubernetes_identity.worker_projection_protocol_version(v7) == 7
    assert kubernetes_identity.worker_projection_has_strict_admission(v2)
    assert kubernetes_identity.worker_projection_has_strict_admission(v3)
    assert kubernetes_identity.worker_projection_has_strict_admission(v4)
    assert kubernetes_identity.worker_projection_has_strict_admission(v5)
    assert kubernetes_identity.worker_projection_has_strict_admission(v6)
    assert kubernetes_identity.worker_projection_has_strict_admission(v7)
    assert kubernetes_identity.worker_projection_has_scratch(v3)
    assert kubernetes_identity.worker_projection_has_scratch(v4)
    assert kubernetes_identity.worker_projection_has_scratch(v5)
    assert kubernetes_identity.worker_projection_has_scratch(v6)
    assert kubernetes_identity.worker_projection_has_scratch(v7)
    assert not (kubernetes_pod_spec.
                serve_worker_projection_protocol_has_runtime_readiness(3))
    assert (kubernetes_pod_spec.
            serve_worker_projection_protocol_has_runtime_readiness(4))
    assert (kubernetes_pod_spec.
            serve_worker_projection_protocol_has_runtime_readiness(5))
    assert (kubernetes_pod_spec.
            serve_worker_projection_protocol_has_runtime_readiness(6))
    assert (kubernetes_pod_spec.
            serve_worker_projection_protocol_has_runtime_readiness(7))
    assert not (kubernetes_pod_spec.
                serve_worker_projection_protocol_has_scratch_bootstrap(5))
    assert (kubernetes_pod_spec.
            serve_worker_projection_protocol_has_scratch_bootstrap(6))
    assert (kubernetes_pod_spec.
            serve_worker_projection_protocol_has_scratch_bootstrap(7))
    assert kubernetes_identity.validate_worker_placement_projections(
        [v2], require_protocol_version=2) == [v2]
    assert kubernetes_identity.validate_worker_placement_projections(
        [v3], require_protocol_version=3) == [v3]
    assert kubernetes_identity.validate_worker_placement_projections(
        [v4], require_protocol_version=4) == [v4]
    assert kubernetes_identity.validate_worker_placement_projections(
        [v5], require_protocol_version=5) == [v5]
    assert kubernetes_identity.validate_worker_placement_projections(
        [v6], require_protocol_version=6) == [v6]
    assert kubernetes_identity.validate_worker_placement_projections(
        [v7], require_protocol_version=7) == [v7]
    with pytest.raises(ValueError, match='must not mix protocol versions'):
        kubernetes_identity.validate_worker_placement_projections([v6, v7])
    with pytest.raises(ValueError, match='protocol-v3/v4/v5/v6/v7 keys'):
        kubernetes_identity.worker_projection_protocol_version({
            **v7, 'unknown': True
        })
    missing_timeout = copy.deepcopy(v4)
    missing_timeout.pop('provision_timeout')
    with pytest.raises(ValueError, match='protocol-v3/v4/v5/v6/v7 keys'):
        kubernetes_identity.worker_projection_protocol_version(missing_timeout)
    assert (kubernetes_identity.worker_projection_sha256(v3)
            != kubernetes_identity.worker_projection_sha256(v4))
    assert (kubernetes_identity.worker_projection_sha256(v4)
            != kubernetes_identity.worker_projection_sha256(v5))
    assert (kubernetes_identity.worker_projection_sha256(v5)
            != kubernetes_identity.worker_projection_sha256(v6))
    assert (kubernetes_identity.worker_projection_sha256(v6)
            != kubernetes_identity.worker_projection_sha256(v7))


def test_worker_projection_v3_digest_covers_scratch():
    projection = _worker_projection(
        protocol_version=3,
        scratch={
            'kind': 'memory',
            'mount_path': '/tmp',
            'volume_name': 'skypilot-serve-worker-tmp',
            'size_limit_bytes': 20 * 1024**3,
        })
    changed = copy.deepcopy(projection)
    changed['scratch']['size_limit_bytes'] += 1

    assert (kubernetes_identity.worker_projection_sha256(projection)
            != kubernetes_identity.worker_projection_sha256(changed))


def test_worker_projection_v3_digest_covers_provision_timeout():
    projection = _worker_projection(protocol_version=3, provision_timeout=-1)
    changed = copy.deepcopy(projection)
    changed['provision_timeout'] = 30

    assert (kubernetes_identity.worker_projection_sha256(projection)
            != kubernetes_identity.worker_projection_sha256(changed))


@pytest.mark.parametrize('provision_timeout', [-1, 0, 30])
def test_worker_projection_v3_accepts_closed_provision_timeout(
        provision_timeout):
    projection = _worker_projection(protocol_version=3,
                                    provision_timeout=provision_timeout)

    assert kubernetes_identity.validate_worker_placement_projections(
        [projection], require_protocol_version=3) == [projection]


@pytest.mark.parametrize('provision_timeout', [True, -2, 1.5, '30', None])
def test_worker_projection_v3_rejects_malformed_provision_timeout(
        provision_timeout):
    with pytest.raises(ValueError, match='provision_timeout'):
        kubernetes_identity.validate_worker_placement_projections(
            [
                _worker_projection(protocol_version=3,
                                   provision_timeout=provision_timeout)
            ],
            require_protocol_version=3)


@pytest.mark.parametrize('scratch', [
    {
        'kind': 'memory',
        'mount_path': '/var/tmp',
        'volume_name': 'skypilot-serve-worker-tmp',
        'size_limit_bytes': 1,
    },
    {
        'kind': 'memory',
        'mount_path': '/tmp',
        'volume_name': 'caller-name',
        'size_limit_bytes': 1,
    },
    {
        'kind': 'memory',
        'mount_path': '/tmp',
        'volume_name': 'skypilot-serve-worker-tmp',
        'size_limit_bytes': True,
    },
    {
        'kind': 'memory',
        'mount_path': '/tmp',
        'volume_name': 'skypilot-serve-worker-tmp',
        'size_limit_bytes': 1,
        'extra': True,
    },
])
def test_worker_projection_v3_rejects_malformed_scratch(scratch):
    with pytest.raises(ValueError):
        kubernetes_identity.validate_worker_placement_projections(
            [_worker_projection(protocol_version=3, scratch=scratch)],
            require_protocol_version=3)


def test_worker_projection_v2_hashes_explicit_identity_free_partition():
    admission = {
        'local_queue_name': 'inference',
        'workload_priority_class_name': 'inference-low',
    }
    identity_free = _worker_projection(protocol_version=2,
                                       kueue_admission=admission,
                                       pod_identity_role_arn=None)
    identity_bearing = _worker_projection(protocol_version=2,
                                          kueue_admission=admission,
                                          pod_identity_role_arn=_worker_role())

    assert identity_free['pod_identity_role_arn'] is None
    assert kubernetes_identity.validate_worker_placement_projections(
        [identity_free], require_protocol_version=2) == [identity_free]
    assert (kubernetes_identity.worker_projection_sha256(identity_free)
            != kubernetes_identity.worker_projection_sha256(identity_bearing))


def test_worker_projection_v1_still_requires_pod_identity_role():
    projection = _worker_projection()
    projection['pod_identity_role_arn'] = None

    with pytest.raises(ValueError,
                       match='pod_identity_role_arn must be a non-empty'):
        kubernetes_identity.validate_worker_placement_projections(
            [projection], require_protocol_version=1)


@pytest.mark.parametrize('kueue_admission', [{
    'local_queue_name': 'inference',
}, {
    'local_queue_name': '',
    'workload_priority_class_name': 'inference-low',
}, {
    'local_queue_name': 'inference',
    'workload_priority_class_name': '',
}, {
    'local_queue_name': 'inference',
    'workload_priority_class_name': 'inference-low',
    'require_managed': True,
}])
def test_worker_projection_v2_rejects_partial_kueue_admission(kueue_admission):
    projection = _worker_projection(protocol_version=2,
                                    kueue_admission=kueue_admission)
    with pytest.raises(ValueError):
        kubernetes_identity.validate_worker_placement_projections(
            [projection], require_protocol_version=2)


def test_worker_projection_v2_digest_covers_complete_validated_candidate():
    projection = _worker_projection(
        protocol_version=2,
        kueue_admission={
            'local_queue_name': 'inference',
            'workload_priority_class_name': 'inference-low',
        })
    validated = kubernetes_identity.validate_worker_placement_projections(
        [projection], allow_none=False, require_protocol_version=2)
    assert validated is not None
    canonical = json.dumps(validated[0],
                           sort_keys=True,
                           separators=(',', ':'),
                           ensure_ascii=False,
                           allow_nan=False)
    expected = hashlib.sha256(canonical.encode('utf-8')).hexdigest()
    assert expected == (
        '465fa566ddb0198fa42a1b36b01a56e5cad9e3335e0b2a95dc0c50c0e160eee2')
    assert kubernetes_identity.worker_projection_sha256(projection) == expected
    assert kubernetes_identity.worker_projection_sha256(
        dict(reversed(list(projection.items())))) == expected

    mutated = json.loads(json.dumps(projection))
    mutated['kueue_admission'][
        'workload_priority_class_name'] = 'inference-lower'
    assert kubernetes_identity.worker_projection_sha256(mutated) != expected
    mutated_scheduler = json.loads(json.dumps(projection))
    mutated_scheduler['scheduler_name'] = 'trusted-batch-scheduler'
    assert (kubernetes_identity.worker_projection_sha256(mutated_scheduler)
            != expected)
    with pytest.raises(ValueError, match='requires protocol 2, 3, 4, 5, or 6'):
        kubernetes_identity.worker_projection_sha256(_worker_projection())


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
        if keys == ('serve_controller_priority_class_name',):
            assert region == 'east-context'
            assert workspace == 'controller'
            return 'typed-controller-priority'
        raise AssertionError((keys, region, workspace))

    location_calls = []

    def _project_location(context, overrides, workspace):
        location_calls.append((context, overrides, workspace))
        return {
            'kubernetes_context': context,
            'namespace': 'controller-system',
            'service_account_name': 'controller-sa',
            'priority_class_name': 'pod-config-priority-must-not-win',
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
        'priority_class_name': 'typed-controller-priority',
        'lb_data_plane_auth': _controller_auth(),
    }
    assert cache == controller_cache
    assert location_calls == [('east-context', {}, 'controller')]
    assert (('serve_controller_work_cache',), 'east-context',
            'controller') in calls
    assert (('serve_controller_priority_class_name',), 'east-context',
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
    config = {
        'kubernetes': {
            'serve_controller_workspace': 'controller',
        },
        'workspaces': {
            'controller': {
                'kubernetes': {
                    'serve_controller_context': 'east',
                    'serve_controller_priority_class_name': 'workspace-controller-priority',
                    'serve_controller_lb_data_plane_auth': {
                        'secret_name': 'skypilot-serve-lb-data-plane-auth',
                        'secret_key': 'tokens',
                    },
                    'context_configs': {
                        'east': {
                            'serve_controller_priority_class_name': 'context-controller-priority',
                        },
                    }
                },
            },
        },
    }
    common_utils.validate_schema(config, schemas.get_config_schema(),
                                 'Invalid config')
    assert skypilot_config.get_effective_workspace_region_config_from_snapshot(
        config_snapshot=config,
        cloud='kubernetes',
        region='east',
        keys=('serve_controller_priority_class_name',),
        workspace='controller',
        default_value=None) == 'context-controller-priority'
    config['workspaces']['controller']['kubernetes']['context_configs']['east'][
        'serve_controller_priority_class_name'] = ''
    with pytest.raises(exceptions.InvalidSkyPilotConfigError):
        common_utils.validate_schema(config, schemas.get_config_schema(),
                                     'Invalid config')
    assert ('kubernetes', 'serve_controller_workspace') in (
        skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    assert ('kubernetes', 'serve_controller_lb_data_plane_auth') in (
        skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    assert ('kubernetes', 'serve_controller_priority_class_name') in (
        skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    assert ('kubernetes', 'context_configs', '*',
            'serve_controller_priority_class_name') in (
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


def test_worker_scratch_is_context_owned_closed_server_config():
    config = {
        'kubernetes': {
            'serve_worker_scratch': {
                'kind': 'none',
            },
        },
        'workspaces': {
            'research': {
                'kubernetes': {
                    'serve_worker_scratch': {
                        'kind': 'memory',
                        'size_limit_bytes': 10,
                    },
                    'context_configs': {
                        'phx': {
                            'serve_worker_scratch': {
                                'kind': 'memory',
                                'size_limit_bytes': 20,
                            },
                        },
                    },
                },
            },
        },
    }
    common_utils.validate_schema(config, schemas.get_config_schema(),
                                 'Invalid config')
    assert skypilot_config.get_effective_workspace_region_config_from_snapshot(
        config_snapshot=config,
        cloud='kubernetes',
        region='phx',
        keys=('serve_worker_scratch',),
        workspace='research',
        default_value=None) == {
            'kind': 'memory',
            'size_limit_bytes': 20,
        }
    assert ('kubernetes', 'serve_worker_scratch') in (
        skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    assert ('kubernetes', 'context_configs', '*', 'serve_worker_scratch') in (
        skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)

    for malformed in ({
            'kind': 'memory',
            'size_limit_bytes': True,
    }, {
            'kind': 'memory',
            'size_limit_bytes': 1,
            'mount_path': '/tmp',
    }):
        with pytest.raises(exceptions.InvalidSkyPilotConfigError):
            common_utils.validate_schema(
                {'kubernetes': {
                    'serve_worker_scratch': malformed,
                }}, schemas.get_config_schema(), 'Invalid config')


def test_worker_provision_timeout_uses_context_over_workspace_config():
    config = {
        'kubernetes': {
            'provision_timeout': 30,
        },
        'workspaces': {
            'research': {
                'kubernetes': {
                    'provision_timeout': 60,
                    'context_configs': {
                        'phx': {
                            'provision_timeout': -1,
                        },
                    },
                },
            },
        },
    }
    common_utils.validate_schema(config, schemas.get_config_schema(),
                                 'Invalid config')

    assert skypilot_config.get_effective_workspace_region_config_from_snapshot(
        config_snapshot=config,
        cloud='kubernetes',
        region='phx',
        keys=('provision_timeout',),
        workspace='research',
        default_value=10) == -1


def test_worker_scratch_projection_uses_exact_context(monkeypatch):
    resolver = mock.Mock(return_value={
        'kind': 'memory',
        'size_limit_bytes': 20 * 1024**3,
    })
    monkeypatch.setattr(skypilot_config,
                        'get_effective_workspace_region_config', resolver)

    assert kubernetes_identity._project_worker_scratch('phx', 'research') == {
        'kind': 'memory',
        'mount_path': '/tmp',
        'volume_name': 'skypilot-serve-worker-tmp',
        'size_limit_bytes': 20 * 1024**3,
    }
    resolver.assert_called_once_with(cloud='kubernetes',
                                     region='phx',
                                     keys=('serve_worker_scratch',),
                                     workspace='research',
                                     default_value={'kind': 'none'})


def test_worker_provision_timeout_projection_freezes_context_value(monkeypatch):
    resolver = mock.Mock(side_effect=lambda *, keys, **_kwargs: ({
        'enabled': False
    } if keys == ('dws',) else -1))
    monkeypatch.setattr(skypilot_config,
                        'get_effective_workspace_region_config', resolver)

    assert kubernetes_identity._project_worker_provision_timeout(
        'phx', 'research', num_nodes=1, volume_mounts=None) == -1
    assert resolver.call_args_list == [
        mock.call(cloud='kubernetes',
                  region='phx',
                  keys=('dws',),
                  workspace='research',
                  default_value={}),
        mock.call(cloud='kubernetes',
                  region='phx',
                  keys=('provision_timeout',),
                  workspace='research',
                  default_value=10),
    ]


@pytest.mark.parametrize(('dws_config', 'expected_default'), [
    ({}, 10),
    ({
        'enabled': True
    }, 1200),
])
def test_worker_provision_timeout_projection_preserves_existing_default(
        monkeypatch, dws_config, expected_default):

    def resolver(*, keys, default_value, **_kwargs):
        if keys == ('dws',):
            return dws_config
        assert keys == ('provision_timeout',)
        return default_value

    monkeypatch.setattr(skypilot_config,
                        'get_effective_workspace_region_config', resolver)

    assert kubernetes_identity._project_worker_provision_timeout(
        'phx', 'research', num_nodes=1, volume_mounts=None) == expected_default


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
    assert projected['scheduler_name'] == 'default-scheduler'
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
            ('pod_config',): {
                'spec': {
                    'schedulerName': 'trusted-batch-scheduler'
                }
            },
            ('serve_worker_priority_class_name',): 'preemptible-inference-low',
            ('serve_worker_priority_value',): -1000,
            ('serve_worker_preemption_policy',): 'Never',
            ('serve_worker_pod_identity_role_arn',): _worker_role(),
        }[keys])

    projected = kubernetes_identity._project_worker_location(
        'east', {}, 'inference')

    assert projected['priority_class_name'] == 'preemptible-inference-low'
    assert projected['scheduler_name'] == 'trusted-batch-scheduler'
    assert projected['priority_value'] == -1000
    assert projected['preemption_policy'] == 'Never'
    assert projected['pod_identity_role_arn'] == _worker_role()


def test_worker_scheduler_projection_uses_trusted_workspace_not_task_override(
        monkeypatch):
    monkeypatch.setattr(
        kubernetes_identity, '_project_location', lambda *_args, **_kwargs: {
            'kubernetes_context': 'east',
            'namespace': 'inference',
            'service_account_name': 'worker',
            'priority_class_name': None,
        })

    def effective_config(*, keys, **_kwargs):
        if keys == ('pod_config',):
            return {'spec': {'schedulerName': 'trusted-batch-scheduler'}}
        return None

    monkeypatch.setattr(skypilot_config,
                        'get_effective_workspace_region_config',
                        effective_config)

    projected = kubernetes_identity._project_worker_location(
        'east', {
            'kubernetes': {
                'pod_config': {
                    'spec': {
                        'schedulerName': 'caller-scheduler'
                    }
                }
            }
        }, 'inference')

    assert projected['scheduler_name'] == 'trusted-batch-scheduler'


def test_worker_kueue_projection_is_workspace_owned_and_all_or_none(
        monkeypatch):
    queue_resolver = mock.Mock(return_value='inference')
    managed_resolver = mock.Mock(return_value=True)
    class_resolver = mock.Mock(return_value='inference-low')
    monkeypatch.setattr(skypilot_config, 'get_effective_queue_name',
                        queue_resolver)
    monkeypatch.setattr(skypilot_config, 'get_effective_kueue_require_managed',
                        managed_resolver)
    monkeypatch.setattr(skypilot_config,
                        'get_effective_workspace_region_config', class_resolver)

    assert kubernetes_identity._project_worker_kueue_admission(
        'phx', 'research') == {
            'local_queue_name': 'inference',
            'workload_priority_class_name': 'inference-low',
        }
    queue_resolver.assert_called_once_with(cloud='kubernetes',
                                           region='phx',
                                           workspace='research',
                                           override_configs=None)
    managed_resolver.assert_called_once_with(cloud='kubernetes',
                                             region='phx',
                                             workspace='research',
                                             override_configs=None)
    class_resolver.assert_called_once_with(
        cloud='kubernetes',
        region='phx',
        keys=('serve_worker_kueue_workload_priority_class_name',),
        workspace='research',
        default_value=None)

    class_resolver.return_value = None
    with pytest.raises(ValueError, match='both configured or both absent'):
        kubernetes_identity._project_worker_kueue_admission('phx', 'research')


def test_worker_kueue_workload_priority_class_is_server_owned():
    common_utils.validate_schema(
        {
            'kubernetes': {
                'serve_worker_kueue_workload_priority_class_name': 'inference-low',
                'context_configs': {
                    'phx': {
                        'serve_worker_kueue_workload_priority_class_name': 'inference-phx-low',
                    },
                },
            },
            'workspaces': {
                'research': {
                    'kubernetes': {
                        'serve_worker_kueue_workload_priority_class_name': 'workspace-low',
                    },
                },
            },
        }, schemas.get_config_schema(), 'Invalid config')
    assert ('kubernetes',
            'serve_worker_kueue_workload_priority_class_name') in (
                skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    assert ('kubernetes', 'context_configs', '*',
            'serve_worker_kueue_workload_priority_class_name') in (
                skylet_constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    with pytest.raises(ValueError):
        common_utils.validate_schema(
            {
                'kubernetes': {
                    'serve_worker_kueue_workload_priority_class_name': '',
                },
            }, schemas.get_config_schema(), 'Invalid config')


@pytest.mark.parametrize('resource_mutator', [
    lambda resource: resource._set_priority_class('caller-priority'),
    lambda resource: setattr(resource, '_cluster_config_overrides', {
        'kubernetes': {
            'kueue': {
                'local_queue_name': 'caller-queue'
            }
        }
    }),
    lambda resource: setattr(
        resource, '_cluster_config_overrides', {
            'kubernetes': {
                'context_configs': {
                    'phx': {
                        'quota': {
                            'queue': 'caller-queue'
                        }
                    }
                }
            }
        }),
])
def test_projected_worker_rejects_task_owned_admission(resource_mutator):
    task = task_lib.Task.from_yaml_str('''
resources:
  infra: k8s/phx
  accelerators: H200:1
run: echo hi
''')
    resource = next(iter(task.resources))
    resource_mutator(resource)

    with pytest.raises(ValueError, match='Projected SkyServe Kubernetes'):
        kubernetes_identity.validate_no_task_worker_projection_overrides(task)


def test_projected_worker_rejects_task_resource_labels_at_commit_and_launch():
    task = task_lib.Task.from_yaml_str('''
resources:
  infra: k8s/phx
  accelerators: H200:1
  labels:
    task.example/inject: "true"
run: echo hi
''')

    with pytest.raises(ValueError, match='task resource labels'):
        kubernetes_identity.build_worker_placement_projections(
            task, workspace='research')

    dag = execution.dag_utils.convert_entrypoint_to_dag(task)
    launch_context = {
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [
            _worker_projection(protocol_version=3)
        ],
    }
    with pytest.raises(exceptions.RequestCancelled,
                       match='task resource labels'):
        execution._validate_projected_service_task_inputs(dag, launch_context)


_TASK_POD_MUTATION_CONFIGS = {
    'provision_timeout': 30,
    'auto_mounts': [{
        'volume_name': 'caller-volume',
    }],
    'enable_docker': {
        'mode': 'ALL',
        'cache_volume': 'caller-pvc',
    },
    'custom_metadata': {
        'finalizers': ['blocked.example/finalizer'],
        'annotations': {
            'task.example/inject': 'true',
        },
    },
}


def _task_override_shape(key, value, shape):
    if shape == 'root':
        return {'kubernetes': {key: value}}
    if shape == 'cloud_relative':
        return {key: value}
    if shape == 'context':
        return {
            'kubernetes': {
                'context_configs': {
                    'phx': {
                        key: value,
                    },
                },
            },
        }
    assert shape == 'nested_list'
    return {
        'kubernetes': {
            'context_configs': {
                'phx': {
                    'future_scopes': [{
                        key: value,
                    }],
                },
            },
        },
    }


@pytest.mark.parametrize('key', _TASK_POD_MUTATION_CONFIGS)
@pytest.mark.parametrize('shape',
                         ['root', 'cloud_relative', 'context', 'nested_list'])
def test_projected_worker_rejects_task_pod_mutation_config_at_commit_and_launch(
        key, shape):
    task = task_lib.Task.from_yaml_str('''
resources:
  infra: k8s/phx
  accelerators: H200:1
run: echo hi
''')
    resource = next(iter(task.resources))
    resource._cluster_config_overrides = _task_override_shape(  # pylint: disable=protected-access
        key, _TASK_POD_MUTATION_CONFIGS[key], shape)

    with pytest.raises(ValueError, match=key):
        kubernetes_identity.build_worker_placement_projections(
            task, workspace='research')

    dag = execution.dag_utils.convert_entrypoint_to_dag(task)
    launch_context = {
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [
            _worker_projection(protocol_version=3)
        ],
    }
    with pytest.raises(exceptions.RequestCancelled, match=key):
        execution._validate_projected_service_task_inputs(dag, launch_context)


def test_projected_worker_rejects_parsed_task_yaml_provision_timeout():
    task = task_lib.Task.from_yaml_str('''
config:
  kubernetes:
    provision_timeout: 30
resources:
  infra: k8s/phx
  accelerators: H200:1
run: echo hi
''')
    resource = next(iter(task.resources))
    assert resource.cluster_config_overrides == {
        'kubernetes': {
            'provision_timeout': 30,
        },
    }

    with pytest.raises(ValueError, match='provision_timeout'):
        kubernetes_identity.build_worker_placement_projections(
            task, workspace='research')

    dag = execution.dag_utils.convert_entrypoint_to_dag(task)
    launch_context = {
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [
            _worker_projection(protocol_version=3)
        ],
    }
    with pytest.raises(exceptions.RequestCancelled, match='provision_timeout'):
        execution._validate_projected_service_task_inputs(dag, launch_context)

    # Historical v2 did not own timeout and retains its launch-time override.
    launch_context[constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY] = [
        _worker_projection(protocol_version=2)
    ]
    execution._validate_projected_service_task_inputs(dag, launch_context)


def test_projected_worker_rejects_direct_fuse_activation_without_mount():
    task = task_lib.Task.from_yaml_str('''
resources:
  infra: k8s/phx
  accelerators: H200:1
  _requires_fuse: true
run: echo hi
''')

    with pytest.raises(ValueError, match='direct FUSE activation'):
        kubernetes_identity.build_worker_placement_projections(
            task, workspace='research')

    dag = execution.dag_utils.convert_entrypoint_to_dag(task)
    launch_context = {
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [
            _worker_projection(protocol_version=3)
        ],
    }
    with pytest.raises(exceptions.RequestCancelled,
                       match='direct FUSE activation'):
        execution._validate_projected_service_task_inputs(dag, launch_context)


def test_projected_worker_accepts_derived_fuse_for_committed_mount_storage():
    task = task_lib.Task.from_yaml_str('''
resources:
  infra: k8s/phx
  accelerators: H200:1
run: echo hi
''')
    task.storage_mounts = {
        '/mnt/dataset': mock.Mock(mode=storage_lib.StorageMode.MOUNT),
    }
    next(iter(task.resources)).set_requires_fuse(True)

    kubernetes_identity.validate_no_task_worker_projection_overrides(task)


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
            'scheduler_name': 'default-scheduler',
            'priority_class_name': 'preemptible-inference-low',
            'priority_value': -1000,
            'preemption_policy': 'Never',
            'pod_identity_role_arn': _worker_role(context),
        })
    monkeypatch.setattr(kubernetes_identity, '_project_cache',
                        lambda context, _workspace: {'kind': 'none'})
    monkeypatch.setattr(
        kubernetes_identity, '_project_worker_kueue_admission',
        lambda _context, _workspace: {
            'local_queue_name': 'inference',
            'workload_priority_class_name': 'inference-low',
        })
    monkeypatch.setattr(
        kubernetes_identity, '_project_accelerator_scheduling',
        lambda _context, accelerator, _workspace: _accelerator_scheduling(
            'A100-SXM4-80GB' if accelerator == 'A100-80GB' else accelerator))
    monkeypatch.setattr(kubernetes_identity,
                        '_project_worker_provision_timeout',
                        lambda *_args, **_kwargs: -1)

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
    assert all(item['projection_version'] == 7 for item in projected)
    assert all(item['provision_timeout'] == -1 for item in projected)
    assert all(item['scratch'] == {'kind': 'none'} for item in projected)
    assert all(
        item['kueue_admission'] == {
            'local_queue_name': 'inference',
            'workload_priority_class_name': 'inference-low',
        } for item in projected)
    assert projected[1]['accelerator_scheduling'] == (_accelerator_scheduling())


def test_worker_catalog_preserves_identity_free_v7_candidates(monkeypatch):
    task = task_lib.Task.from_yaml_str('''
resources:
  infra: k8s/phx
  accelerators: H200:1
run: echo hi
''')
    monkeypatch.setattr(
        kubernetes_identity, '_catalog_candidates', lambda *_args, **_kwargs: [(
            0, kubernetes_identity.clouds.Kubernetes(), 'phx', {
                'H200': 1
            }, {})])
    monkeypatch.setattr(
        kubernetes_identity, '_project_worker_location',
        lambda context, *_args: {
            'kubernetes_context': context,
            'namespace': 'identity-free-inference',
            'service_account_name': 'identity-free-worker',
            'scheduler_name': 'gpu-binpack',
            'priority_class_name': 'preemptible-inference-low',
            'priority_value': -1000,
            'preemption_policy': 'Never',
            'pod_identity_role_arn': None,
        })
    monkeypatch.setattr(kubernetes_identity, '_project_cache',
                        lambda _context, _workspace: {'kind': 'none'})
    monkeypatch.setattr(
        kubernetes_identity, '_project_worker_kueue_admission',
        lambda _context, _workspace: {
            'local_queue_name': 'inference',
            'workload_priority_class_name': 'inference-low',
        })
    monkeypatch.setattr(
        kubernetes_identity, '_project_accelerator_scheduling', lambda _context,
        accelerator, _workspace: _accelerator_scheduling(accelerator))
    monkeypatch.setattr(kubernetes_identity,
                        '_project_worker_provision_timeout',
                        lambda *_args, **_kwargs: -1)

    projected = kubernetes_identity.build_worker_placement_projections(
        task, workspace='inference', placement_catalog={})

    assert projected is not None
    assert projected[0]['projection_version'] == 7
    assert projected[0]['provision_timeout'] == -1
    assert projected[0]['scratch'] == {'kind': 'none'}
    assert projected[0]['pod_identity_role_arn'] is None


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


@pytest.mark.parametrize('protocol_version', [2, 3])
def test_projected_worker_rejects_raw_task_volume_before_resolution(
        protocol_version):
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
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [
            _worker_projection(protocol_version=protocol_version)
        ],
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
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [
            _worker_projection(protocol_version=2)
        ],
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
    'serve_worker_scratch': {
        'kind': 'memory',
        'size_limit_bytes': 1,
    },
}, {
    'context_configs': {
        'phx': {
            'namespace': 'caller-namespace',
        },
    },
}])
@pytest.mark.parametrize('protocol_version', [2, 3])
def test_projected_worker_rejects_task_kubernetes_identity_overrides(
        task_kubernetes_config, protocol_version):
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
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [
            _worker_projection(protocol_version=protocol_version)
        ],
    }

    with pytest.raises(exceptions.RequestCancelled,
                       match=('pod_config, namespace, '
                              '(?:provision_timeout, )?or remote_identity')):
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
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [
            _worker_projection(protocol_version=2)
        ],
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
        'projection_version': 7,
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
        'scheduler_name': 'default-scheduler',
        'kueue_admission': None,
        'provision_timeout': -1,
        'scratch': {
            'kind': 'none'
        },
    }, {
        'projection_version': 7,
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
        'scheduler_name': 'default-scheduler',
        'kueue_admission': None,
        'provision_timeout': -1,
        'scratch': {
            'kind': 'none'
        },
    }]
    launch_context = {
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: projections,
    }

    execution._apply_service_worker_runtime_projection_to_task(
        task, launch_context, task.best_resources)

    assert task.envs['SKYPILOT_SERVE_CACHE_KIND'] == 'node_local'
    assert task.envs['SKYPILOT_SERVE_CACHE_MOUNT_PATH'] == '/mnt/sky-cache'
    assert task.envs['SKYPILOT_SERVE_CACHE_ATTESTATION_ID'] == 'phx-cache-v1'
    assert not any(
        key.startswith(kubernetes_identity.CACHE_ENV_PREFIX)
        for key in task.secrets)
    assert task.envs_and_secrets['SKYPILOT_SERVE_CACHE_KIND'] == 'node_local'


def test_runtime_scratch_v7_owns_bootstrap_paths_and_overrides_caller():
    task = task_lib.Task()
    task.set_resources(
        resources_lib.Resources(cloud=kubernetes_identity.clouds.Kubernetes(),
                                region='phx',
                                accelerators={'H200': 1}))
    task.update_envs({
        'SKYPILOT_SERVE_SCRATCH_KIND': 'caller',
        'SKYPILOT_SERVE_SCRATCH_SIZE_LIMIT_BYTES': '1',
        'SKY_RUNTIME_DIR': '/root',
        'UV_CACHE_DIR': '/root/.cache/uv',
    })
    task.update_secrets({
        'SKYPILOT_SERVE_SCRATCH_EVIL': 'secret',
        'UV_PYTHON_INSTALL_DIR': '/root/.local/share/uv/python',
    })
    launch_context = {
        constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY: [
            _worker_projection(protocol_version=7,
                               scratch={
                                   'kind': 'memory',
                                   'mount_path': '/tmp',
                                   'volume_name': 'skypilot-serve-worker-tmp',
                                   'size_limit_bytes': 20 * 1024**3,
                               })
        ],
    }

    execution._apply_service_worker_runtime_projection_to_task(
        task, launch_context, next(iter(task.resources)))

    assert task.envs['SKYPILOT_SERVE_SCRATCH_KIND'] == 'memory'
    assert task.envs['SKYPILOT_SERVE_SCRATCH_MOUNT_PATH'] == '/tmp'
    assert task.envs['SKYPILOT_SERVE_SCRATCH_SIZE_LIMIT_BYTES'] == str(20 *
                                                                       1024**3)
    assert not any(
        key.startswith('SKYPILOT_SERVE_SCRATCH_') for key in task.secrets)
    assert {
        key: task.envs[key]
        for key in kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT
    } == kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT
    assert not any(key in kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT
                   for key in task.secrets)


def test_runtime_bootstrap_paths_are_owned_by_v6_v7_memory_scratch():
    memory_scratch = {
        'kind': 'memory',
        'mount_path': '/tmp',
        'volume_name': 'skypilot-serve-worker-tmp',
        'size_limit_bytes': 20 * 1024**3,
    }

    assert kubernetes_identity.bootstrap_environment(
        _worker_projection(protocol_version=4, scratch=memory_scratch)) == {}
    assert kubernetes_identity.bootstrap_environment(
        _worker_projection(protocol_version=5, scratch=memory_scratch)) == {}
    assert kubernetes_identity.bootstrap_environment(
        _worker_projection(protocol_version=6, scratch=memory_scratch)) == (
            kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT)
    assert kubernetes_identity.bootstrap_environment(
        _worker_projection(protocol_version=6)) == {}
    assert kubernetes_identity.bootstrap_environment(
        _worker_projection(protocol_version=7, scratch=memory_scratch)) == (
            kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT)
    assert kubernetes_identity.bootstrap_environment(
        _worker_projection(protocol_version=7)) == {}


def test_final_kubernetes_yaml_enforces_platform_identity_and_cache():
    projection = {
        'projection_version': 7,
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
        'scheduler_name': 'default-scheduler',
        'kueue_admission': None,
        'provision_timeout': -1,
        'scratch': {
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
                            'command': ['/bin/bash', '-c', '--'],
                            'args': ['canonical bootstrap'],
                            'env': [{
                                'name': 'SKYPILOT_SERVE_CACHE_EVIL',
                                'value': 'caller-value',
                            }],
                        }, {
                            'name': 'sidecar',
                            'resources': {
                                'requests': {
                                    'nvidia.com/gpu': 4,
                                },
                                'limits': {
                                    'nvidia.com/gpu': 4,
                                },
                            },
                            'env': [{
                                'name': 'SKYPILOT_SERVE_CACHE_EVIL',
                                'value': 'caller-value',
                            }],
                        }],
                        'initContainers': [{
                            'name': 'gpu-init',
                            'resources': {
                                'requests': {
                                    'nvidia.com/gpu': 8,
                                },
                                'limits': {
                                    'nvidia.com/gpu': 8,
                                },
                            },
                        }],
                        'overhead': {
                            'nvidia.com/gpu': 2,
                            'cpu': '100m',
                        },
                        'resources': {
                            'requests': {
                                'nvidia.com/gpu': 16,
                                'cpu': '2',
                            },
                            'limits': {
                                'nvidia.com/gpu': 16,
                                'memory': '4Gi',
                            },
                        },
                    }
                }
            }
        },
    }

    pod_spec = cluster_yaml['available_node_types']['ray.head.default'][
        'node_config']['spec']
    bootstrap_sha256 = (
        kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(pod_spec))
    backend_utils._enforce_worker_projection_on_kubernetes_yaml(
        cluster_yaml,
        projection,
        expected_runtime_bootstrap_sha256=bootstrap_sha256)

    assert cluster_yaml['provider'] == {
        'context': 'phx',
        'namespace': 'rescluster-k8s-prod-east1-preemptible-inference',
        'timeout': -1,
        'serve_worker_projection_protocol_version': 7,
        'serve_worker_expected_runtime_bootstrap_sha256': bootstrap_sha256,
        'serve_worker_expected_scratch': {
            'kind': 'none'
        },
        'serve_worker_expected_scheduler_name': 'default-scheduler',
        'kueue_local_queue_name': None,
        'kueue_require_managed': False,
        'kueue_workload_priority_class_name': None,
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
    assert {
        'name': 'SKYPILOT_SERVE_CACHE_KIND',
        'value': 'none',
    } in pod_spec['containers'][0]['env']
    assert not pod_spec['containers'][1]['env']
    assert pod_spec['containers'][1]['resources'] == {
        'requests': {},
        'limits': {},
    }
    assert pod_spec['initContainers'][0]['resources'] == {
        'requests': {},
        'limits': {},
    }
    assert pod_spec['overhead'] == {'cpu': '100m'}
    assert pod_spec['resources'] == {
        'requests': {
            'cpu': '2',
        },
        'limits': {
            'memory': '4Gi',
        },
    }


@pytest.mark.parametrize('claim_surface', ['pod', 'runtime_container'])
def test_final_kubernetes_yaml_rejects_dynamic_resource_claims(claim_surface):
    projection = _worker_projection(protocol_version=7)
    pod_spec = {
        'containers': [{
            'name': 'ray-node',
            'command': ['/bin/bash', '-c', '--'],
            'args': ['canonical bootstrap'],
        }],
    }
    if claim_surface == 'pod':
        pod_spec['resourceClaims'] = [{'name': 'opaque-gpu'}]
    else:
        pod_spec['containers'][0]['resources'] = {
            'claims': [{
                'name': 'opaque-gpu'
            }]
        }
    cluster_yaml = {
        'provider': {},
        'available_node_types': {
            'ray_head_default': {
                'node_config': {
                    'spec': pod_spec,
                },
            },
        },
    }
    bootstrap_sha256 = (
        kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(pod_spec))

    with pytest.raises(exceptions.InvalidCloudConfigs,
                       match='Dynamic Resource Allocation'):
        backend_utils._enforce_worker_projection_on_kubernetes_yaml(
            cluster_yaml,
            projection,
            expected_runtime_bootstrap_sha256=bootstrap_sha256)


def test_final_kubernetes_yaml_reasserts_v7_kueue_admission():
    projection = _worker_projection(
        protocol_version=7,
        scheduler_name='trusted-batch-scheduler',
        kueue_admission={
            'local_queue_name': 'inference',
            'workload_priority_class_name': 'inference-low',
        })
    cluster_yaml = {
        'provider': {
            'timeout': 30,
            'kueue_local_queue_name': 'caller-queue',
            'kueue_require_managed': False,
            'kueue_workload_priority_class_name': 'caller-workload-priority',
        },
        'available_node_types': {
            'ray_head_default': {
                'node_config': {
                    'metadata': {
                        'labels': {
                            'kueue.x-k8s.io/managed': 'true',
                            'kueue.x-k8s.io/queue-name': 'caller-queue',
                            'kueue.x-k8s.io/priority-class': 'caller-workload-priority',
                        },
                    },
                    'spec': {
                        'nodeName': 'caller-selected-node',
                        'schedulerName': 'caller-scheduler',
                        'priorityClassName': 'caller-pod-priority',
                        'containers': [{
                            'name': 'ray-node',
                            'command': ['/bin/bash', '-c', '--'],
                            'args': ['canonical bootstrap'],
                        }],
                    },
                },
            },
        },
    }

    pod_spec = cluster_yaml['available_node_types']['ray_head_default'][
        'node_config']['spec']
    bootstrap_sha256 = (
        kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(pod_spec))
    backend_utils._enforce_worker_projection_on_kubernetes_yaml(
        cluster_yaml,
        projection,
        expected_runtime_bootstrap_sha256=bootstrap_sha256)

    provider = cluster_yaml['provider']
    assert provider['timeout'] == -1
    assert provider['kueue_local_queue_name'] == 'inference'
    assert provider['kueue_require_managed'] is True
    assert provider['kueue_workload_priority_class_name'] == 'inference-low'
    assert provider['serve_worker_expected_scheduler_name'] == (
        'trusted-batch-scheduler')
    node_config = cluster_yaml['available_node_types']['ray_head_default'][
        'node_config']
    assert node_config['metadata']['labels'] == {
        'kueue.x-k8s.io/queue-name': 'inference',
        'kueue.x-k8s.io/priority-class': 'inference-low',
    }
    assert node_config['spec'][
        'priorityClassName'] == 'preemptible-inference-low'
    assert 'nodeName' not in node_config['spec']
    assert node_config['spec']['schedulerName'] == 'trusted-batch-scheduler'

    projection_without_kueue = _worker_projection(protocol_version=7,
                                                  kueue_admission=None)
    backend_utils._enforce_worker_projection_on_kubernetes_yaml(
        cluster_yaml,
        projection_without_kueue,
        expected_runtime_bootstrap_sha256=bootstrap_sha256)
    assert provider['kueue_local_queue_name'] is None
    assert provider['kueue_require_managed'] is False
    assert provider['kueue_workload_priority_class_name'] is None
    assert not any(
        key in node_config['metadata']['labels']
        for key in ('kueue.x-k8s.io/queue-name',
                    'kueue.x-k8s.io/priority-class', 'kueue.x-k8s.io/managed'))


def test_final_v7_yaml_composes_kueue_cache_scratch_and_readiness():
    projection = _worker_projection(
        protocol_version=7,
        scheduler_name='trusted-batch-scheduler',
        kueue_admission={
            'local_queue_name': 'inference',
            'workload_priority_class_name': 'inference-low',
        },
        scratch={
            'kind': 'memory',
            'mount_path': '/tmp',
            'volume_name': 'skypilot-serve-worker-tmp',
            'size_limit_bytes': 20 * 1024**3,
        })
    projection['cache'] = _node_local_cache()
    cluster_yaml = {
        'provider': {
            'timeout': 30,
        },
        'available_node_types': {
            'ray_head_default': {
                'node_config': {
                    'metadata': {},
                    'spec': {
                        'containers': [{
                            'name': 'ray-node',
                            'command': ['/bin/bash', '-c', '--'],
                            'args': [_current_bootstrap_script()],
                            'env': [{
                                'name': 'SKYPILOT_SERVE_SCRATCH_KIND',
                                'value': 'caller',
                            }, *_current_bootstrap_pod_environment()],
                        }],
                    },
                },
            },
        },
    }
    pod_spec = cluster_yaml['available_node_types']['ray_head_default'][
        'node_config']['spec']
    bootstrap_sha256 = (
        kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(pod_spec))

    backend_utils._enforce_worker_projection_on_kubernetes_yaml(
        cluster_yaml,
        projection,
        expected_runtime_bootstrap_sha256=bootstrap_sha256)
    first = copy.deepcopy(cluster_yaml)
    backend_utils._enforce_worker_projection_on_kubernetes_yaml(
        cluster_yaml,
        projection,
        expected_runtime_bootstrap_sha256=bootstrap_sha256)

    assert cluster_yaml == first
    assert cluster_yaml['provider'][
        'serve_worker_projection_protocol_version'] == 7
    assert cluster_yaml['provider']['timeout'] == -1
    assert cluster_yaml['provider'][
        'serve_worker_expected_runtime_bootstrap_sha256'] == bootstrap_sha256
    assert cluster_yaml['provider']['serve_worker_expected_scratch'] == {
        'kind': 'memory',
        'mount_path': '/tmp',
        'volume_name': 'skypilot-serve-worker-tmp',
        'size_limit_bytes': 20 * 1024**3,
    }
    node = cluster_yaml['available_node_types']['ray_head_default'][
        'node_config']
    assert node['metadata']['labels'] == {
        'kueue.x-k8s.io/queue-name': 'inference',
        'kueue.x-k8s.io/priority-class': 'inference-low',
    }
    assert node['spec']['volumes'] == [{
        'name': 'phx-cache',
        'hostPath': {
            'path': '/mnt/local-nvme/sky-cache',
            'type': 'Directory',
        },
    }, {
        'name': 'skypilot-serve-worker-tmp',
        'emptyDir': {
            'medium': 'Memory',
            'sizeLimit': str(20 * 1024**3),
        },
    }]
    runtime = node['spec']['containers'][0]
    assert runtime['volumeMounts'] == [{
        'name': 'phx-cache',
        'mountPath': '/mnt/sky-cache',
    }, {
        'name': 'skypilot-serve-worker-tmp',
        'mountPath': '/tmp',
    }]
    environment = {
        entry['name']: entry['value']
        for entry in runtime['env']
        if 'value' in entry
    }
    assert {
        key: value
        for key, value in environment.items()
        if key.startswith('SKYPILOT_SERVE_SCRATCH_')
    } == {
        'SKYPILOT_SERVE_SCRATCH_KIND': 'memory',
        'SKYPILOT_SERVE_SCRATCH_MOUNT_PATH': '/tmp',
        'SKYPILOT_SERVE_SCRATCH_SIZE_LIMIT_BYTES': str(20 * 1024**3),
    }
    assert {
        key: environment[key]
        for key in kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT
    } == kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT
    assert node['spec']['restartPolicy'] == 'Never'
    assert [
        entry for entry in runtime['env'] if entry['name'] == 'SKYPILOT_POD_UID'
    ] == [{
        'name': 'SKYPILOT_POD_UID',
        'valueFrom': {
            'fieldRef': {
                'apiVersion': 'v1',
                'fieldPath': 'metadata.uid',
            },
        },
    }]
    expected_probe_command = [
        '/bin/sh', '-c',
        ('test -n "$SKYPILOT_POD_UID" && '
         'test "$(cat /tmp/skypilot-serve-worker-runtime-ready '
         '2>/dev/null)" = "$SKYPILOT_POD_UID"')
    ]
    assert runtime['startupProbe'] == {
        'exec': {
            'command': expected_probe_command,
        },
        'initialDelaySeconds': 0,
        'periodSeconds': 2,
        'timeoutSeconds': 1,
        'successThreshold': 1,
        'failureThreshold': 900,
    }
    assert runtime['readinessProbe'] == {
        'exec': {
            'command': expected_probe_command,
        },
        'initialDelaySeconds': 0,
        'periodSeconds': 2,
        'timeoutSeconds': 1,
        'successThreshold': 1,
        'failureThreshold': 1,
    }


@pytest.mark.parametrize('protocol_version', [1, 2, 3, 4, 5, 6])
def test_final_yaml_rejects_historical_projection_before_mutation(
        protocol_version):
    cluster_yaml = _projected_worker_cluster_yaml()
    original = copy.deepcopy(cluster_yaml)

    with pytest.raises(exceptions.InvalidCloudConfigs,
                       match='does not satisfy required version 7'):
        backend_utils._enforce_worker_projection_on_kubernetes_yaml(
            cluster_yaml, _worker_projection(protocol_version=protocol_version))

    assert cluster_yaml == original


@pytest.mark.parametrize('collision', [
    'restart_policy',
    'pod_uid_env',
    'startup_probe',
    'readiness_probe',
])
def test_projected_runtime_readiness_rejects_owned_surface_collision(collision):
    pod_spec = {
        'containers': [{
            'name': 'ray-node',
            'command': ['/bin/bash', '-c', '--'],
            'args': ['canonical bootstrap'],
        }]
    }
    bootstrap_sha256 = (
        kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(pod_spec))
    runtime = pod_spec['containers'][0]
    if collision == 'restart_policy':
        pod_spec['restartPolicy'] = 'Always'
    elif collision == 'pod_uid_env':
        runtime['env'] = [{
            'name': 'SKYPILOT_POD_UID',
            'value': 'caller',
        }]
    elif collision == 'startup_probe':
        runtime['startupProbe'] = {'exec': {'command': ['true']}}
    else:
        runtime['readinessProbe'] = {'exec': {'command': ['true']}}

    with pytest.raises(
            kubernetes_pod_spec.ProjectedRuntimeReadinessContractError,
            match='runtime|UID|restartPolicy'):
        kubernetes_pod_spec.enforce_projected_worker_runtime_readiness_contract(
            pod_spec, rewrite=True, expected_bootstrap_sha256=bootstrap_sha256)


@pytest.mark.parametrize('mutation', [
    lambda runtime: runtime.__setitem__('command', ['/bin/sh', '-c']),
    lambda runtime: runtime.__setitem__('args', ['forged bootstrap']),
    lambda runtime: runtime.__setitem__(
        'lifecycle', {
            'postStart': {
                'exec': {
                    'command':
                        ['touch', '/tmp/skypilot-serve-worker-runtime-ready']
                }
            },
        }),
    lambda runtime: runtime.__setitem__('lifecycle', {
        'preStop': {
            'exec': {
                'command': ['/bin/sh', '-c', 'true']
            }
        },
    }),
])
def test_final_v7_yaml_rejects_bootstrap_producer_mutation(mutation):
    cluster_yaml = _projected_worker_cluster_yaml()
    pod_spec = cluster_yaml['available_node_types']['ray_head_default'][
        'node_config']['spec']
    bootstrap_sha256 = (
        kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(pod_spec))
    mutation(pod_spec['containers'][0])

    with pytest.raises(exceptions.InvalidCloudConfigs,
                       match='bootstrap command|bootstrap producer'):
        backend_utils._enforce_worker_projection_on_kubernetes_yaml(
            cluster_yaml,
            _worker_projection(protocol_version=7),
            expected_runtime_bootstrap_sha256=bootstrap_sha256)


@pytest.mark.parametrize('mutation', [
    lambda runtime: runtime.__setitem__('command', ['/bin/sh', '-c']),
    lambda runtime: runtime.__setitem__('args', ['forged bootstrap']),
    lambda runtime: runtime.__setitem__(
        'lifecycle', {
            **runtime['lifecycle'],
            'postStart': {
                'exec': {
                    'command':
                        ['touch', '/tmp/skypilot-serve-worker-runtime-ready']
                }
            },
        }),
    lambda runtime: runtime['lifecycle']['preStop']['exec'].__setitem__(
        'command', ['/bin/sh', '-c', 'true']),
])
def test_admitted_runtime_readiness_rejects_bootstrap_producer_mutation(
        mutation):
    pod_spec = {
        'containers': [{
            'name': 'ray-node',
            'command': ['/bin/bash', '-c', '--'],
            'args': ['canonical bootstrap'],
            'lifecycle': {
                'preStop': {
                    'exec': {
                        'command': ['/bin/sh', '-c', 'echo stopping'],
                    },
                },
            },
        }]
    }
    bootstrap_sha256 = (
        kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(pod_spec))
    canonical = (
        kubernetes_pod_spec.enforce_projected_worker_runtime_readiness_contract(
            pod_spec, rewrite=True, expected_bootstrap_sha256=bootstrap_sha256))
    assert canonical.matches

    mutation(pod_spec['containers'][0])
    admitted = (
        kubernetes_pod_spec.enforce_projected_worker_runtime_readiness_contract(
            pod_spec, rewrite=False,
            expected_bootstrap_sha256=bootstrap_sha256))

    assert not admitted.matches
    assert admitted.actual['bootstrap_sha256'] != bootstrap_sha256
    pod = mock.Mock()
    pod.metadata.name = 'mutated-worker'
    pod.spec = pod_spec
    with pytest.raises(kubernetes_instance._ServeWorkerIdentityRejection):
        kubernetes_instance._attest_serve_worker_runtime_readiness(
            pod, 'inference', 'phx', True, bootstrap_sha256, defer_cleanup=True)


def test_admitted_v7_runtime_readiness_rejects_bootstrap_environment_drift():
    pod_spec = {
        'containers': [{
            'name': 'ray-node',
            'command': ['/bin/bash', '-c', '--'],
            'args': [_current_bootstrap_script()],
            'env': _current_bootstrap_pod_environment(),
        }]
    }
    bootstrap_sha256 = (
        kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(pod_spec))
    canonical = (
        kubernetes_pod_spec.enforce_projected_worker_runtime_readiness_contract(
            pod_spec, rewrite=True, expected_bootstrap_sha256=bootstrap_sha256))
    assert canonical.matches
    next(entry for entry in pod_spec['containers'][0]['env']
         if entry['name'] == 'UV_CACHE_DIR')['value'] = '/root/.cache/uv'

    admitted = (
        kubernetes_pod_spec.enforce_projected_worker_runtime_readiness_contract(
            pod_spec, rewrite=False,
            expected_bootstrap_sha256=bootstrap_sha256))

    assert not admitted.matches
    assert admitted.actual['bootstrap_sha256'] != bootstrap_sha256
    pod = mock.Mock()
    pod.metadata.name = 'mutated-worker'
    pod.spec = pod_spec
    with pytest.raises(kubernetes_instance._ServeWorkerIdentityRejection):
        kubernetes_instance._attest_serve_worker_runtime_readiness(
            pod, 'inference', 'phx', True, bootstrap_sha256, defer_cleanup=True)


def test_v7_bootstrap_contract_rejects_missing_post_runcmd_export():
    pod_spec = {
        'containers': [{
            'name': 'ray-node',
            'command': ['/bin/bash', '-c', '--'],
            'args': [
                _current_bootstrap_script().replace(
                    'export UV_CACHE_DIR="/tmp/.skypilot-runtime/uv-cache"', '')
            ],
            'env': _current_bootstrap_pod_environment(),
        }]
    }

    with pytest.raises(
            kubernetes_pod_spec.ProjectedRuntimeReadinessContractError,
            match='re-export'):
        kubernetes_pod_spec.validate_projected_worker_bootstrap_environment(
            pod_spec, kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT)


@pytest.mark.parametrize('line_kind', ['marker', 'export'])
def test_v7_bootstrap_contract_requires_exact_full_lines(line_kind):
    script = _current_bootstrap_script()
    if line_kind == 'marker':
        exact_line = (
            f'# {kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENV_MARKER}')
    else:
        exact_line = ('export UV_CACHE_DIR="/tmp/.skypilot-runtime/uv-cache"')
    pod_spec = {
        'containers': [{
            'name': 'ray-node',
            'command': ['/bin/bash', '-c', '--'],
            'args': [script.replace(exact_line, f'{exact_line} trailing')],
            'env': _current_bootstrap_pod_environment(),
        }]
    }

    with pytest.raises(
            kubernetes_pod_spec.ProjectedRuntimeReadinessContractError,
            match='exact marker|re-export'):
        kubernetes_pod_spec.validate_projected_worker_bootstrap_environment(
            pod_spec, kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT)


def test_real_kubernetes_client_runtime_readiness_contract_is_accepted():
    pod_spec = {
        'containers': [{
            'name': 'ray-node',
            'command': ['/bin/bash', '-c', '--'],
            'args': ['canonical bootstrap'],
            'lifecycle': {
                'preStop': {
                    'exec': {
                        'command': ['/bin/sh', '-c', 'echo stopping'],
                    },
                },
            },
        }]
    }
    bootstrap_sha256 = (
        kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(pod_spec))
    expected = (
        kubernetes_pod_spec.enforce_projected_worker_runtime_readiness_contract(
            pod_spec, rewrite=True, expected_bootstrap_sha256=bootstrap_sha256))
    client = kubernetes_adaptor.kubernetes.client
    api_pod_spec = client.ApiClient()._ApiClient__deserialize(  # pylint: disable=protected-access
        pod_spec, 'V1PodSpec')

    observed = (
        kubernetes_pod_spec.enforce_projected_worker_runtime_readiness_contract(
            api_pod_spec,
            rewrite=False,
            expected_bootstrap_sha256=bootstrap_sha256))

    assert expected.matches
    assert observed.matches
    assert observed.actual == expected.expected


def _projected_worker_cluster_yaml(*, bootstrap_environment=False):
    runtime = {
        'name': 'ray-node',
        'command': ['/bin/bash', '-c', '--'],
        'args': [
            _current_bootstrap_script()
            if bootstrap_environment else 'canonical bootstrap'
        ],
    }
    if bootstrap_environment:
        runtime['env'] = _current_bootstrap_pod_environment()
    return {
        'provider': {},
        'available_node_types': {
            'ray_head_default': {
                'node_config': {
                    'spec': {
                        'containers': [runtime],
                    },
                },
            },
        },
    }


def _worker_bootstrap_sha256(cluster_yaml):
    node_type = next(iter(cluster_yaml['available_node_types'].values()))
    return kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(
        node_type['node_config']['spec'])


@pytest.mark.parametrize('mutate', [
    lambda spec: spec.setdefault('volumes', []).append({
        'name': 'skypilot-serve-worker-tmp',
        'emptyDir': {
            'medium': '',
            'sizeLimit': '1',
        },
    }),
    lambda spec: spec['containers'][0].setdefault('volumeMounts', []).append({
        'name': 'caller-volume',
        'mountPath': '/tmp',
    }),
    lambda spec: spec['containers'][0].setdefault('volumeMounts', []).append({
        'name': 'caller-nested-volume',
        'mountPath': '/tmp/private',
    }),
    lambda spec: spec.setdefault('initContainers', []).append({
        'name': 'init',
        'volumeMounts': [{
            'name': 'skypilot-serve-worker-tmp',
            'mountPath': '/work',
        }],
    }),
    lambda spec: spec['containers'][0].setdefault('volumeDevices', []).append({
        'name': 'skypilot-serve-worker-tmp',
        'devicePath': '/dev/scratch',
    }),
])
def test_final_v7_yaml_rejects_existing_scratch_identity_collisions(mutate):
    projection = _worker_projection(
        protocol_version=7,
        scratch={
            'kind': 'memory',
            'mount_path': '/tmp',
            'volume_name': 'skypilot-serve-worker-tmp',
            'size_limit_bytes': 20 * 1024**3,
        })
    cluster_yaml = _projected_worker_cluster_yaml(bootstrap_environment=True)
    spec = cluster_yaml['available_node_types']['ray_head_default'][
        'node_config']['spec']
    mutate(spec)
    bootstrap_sha256 = _worker_bootstrap_sha256(cluster_yaml)

    with pytest.raises(exceptions.InvalidCloudConfigs, match='collides'):
        backend_utils._enforce_worker_projection_on_kubernetes_yaml(
            cluster_yaml,
            projection,
            expected_runtime_bootstrap_sha256=bootstrap_sha256)


@pytest.mark.parametrize('duplicate', ['volume', 'mount'])
def test_final_v7_yaml_rejects_duplicate_exact_scratch_identity(duplicate):
    projection = _worker_projection(
        protocol_version=7,
        scratch={
            'kind': 'memory',
            'mount_path': '/tmp',
            'volume_name': 'skypilot-serve-worker-tmp',
            'size_limit_bytes': 20 * 1024**3,
        })
    cluster_yaml = _projected_worker_cluster_yaml(bootstrap_environment=True)
    spec = cluster_yaml['available_node_types']['ray_head_default'][
        'node_config']['spec']
    volume = {
        'name': 'skypilot-serve-worker-tmp',
        'emptyDir': {
            'medium': 'Memory',
            'sizeLimit': str(20 * 1024**3),
        },
    }
    mount = {
        'name': 'skypilot-serve-worker-tmp',
        'mountPath': '/tmp',
    }
    if duplicate == 'volume':
        spec['volumes'] = [copy.deepcopy(volume), copy.deepcopy(volume)]
    else:
        spec['volumes'] = [volume]
        spec['containers'][0]['volumeMounts'] = [
            copy.deepcopy(mount), copy.deepcopy(mount)
        ]
    bootstrap_sha256 = _worker_bootstrap_sha256(cluster_yaml)

    with pytest.raises(exceptions.InvalidCloudConfigs, match='scratch'):
        backend_utils._enforce_worker_projection_on_kubernetes_yaml(
            cluster_yaml,
            projection,
            expected_runtime_bootstrap_sha256=bootstrap_sha256)


def test_final_v7_yaml_rejects_cache_scratch_collision():
    projection = _worker_projection(
        protocol_version=7,
        scratch={
            'kind': 'memory',
            'mount_path': '/tmp',
            'volume_name': 'skypilot-serve-worker-tmp',
            'size_limit_bytes': 20 * 1024**3,
        })
    projection['cache'] = {
        **_node_local_cache(),
        'mount_path': '/tmp',
    }
    cluster_yaml = _projected_worker_cluster_yaml(bootstrap_environment=True)

    with pytest.raises(exceptions.InvalidCloudConfigs,
                       match='cache and scratch'):
        backend_utils._enforce_worker_projection_on_kubernetes_yaml(
            cluster_yaml,
            projection,
            expected_runtime_bootstrap_sha256=(
                _worker_bootstrap_sha256(cluster_yaml)))


def test_final_v7_none_rejects_cache_tmp_alias():
    projection = _worker_projection(protocol_version=7)
    projection['cache'] = {
        **_node_local_cache(),
        'mount_path': '/var/../tmp/',
    }
    cluster_yaml = _projected_worker_cluster_yaml()

    with pytest.raises(exceptions.InvalidCloudConfigs,
                       match='cache and scratch'):
        backend_utils._enforce_worker_projection_on_kubernetes_yaml(
            cluster_yaml,
            projection,
            expected_runtime_bootstrap_sha256=(
                _worker_bootstrap_sha256(cluster_yaml)))


@pytest.mark.parametrize('collision', ['volume', 'mount', 'nested_mount'])
def test_final_v7_none_rejects_inherited_scratch_owner(collision):
    cluster_yaml = _projected_worker_cluster_yaml()
    spec = cluster_yaml['available_node_types']['ray_head_default'][
        'node_config']['spec']
    if collision == 'volume':
        spec['volumes'] = [{
            'name': 'skypilot-serve-worker-tmp',
            'emptyDir': {},
        }]
    else:
        spec['containers'][0]['volumeMounts'] = [{
            'name': 'caller-tmp',
            'mountPath':
                ('/tmp/private' if collision == 'nested_mount' else '/tmp'),
        }]

    with pytest.raises(exceptions.InvalidCloudConfigs, match='collides'):
        backend_utils._enforce_worker_projection_on_kubernetes_yaml(
            cluster_yaml,
            _worker_projection(protocol_version=7),
            expected_runtime_bootstrap_sha256=(
                _worker_bootstrap_sha256(cluster_yaml)))


@pytest.mark.parametrize('protocol_version', [1, 2, 3, 4, 5, 6, 7])
def test_kubernetes_deploy_vars_require_current_projected_admission(
        monkeypatch, protocol_version):
    resources = mock.MagicMock()
    resources.instance_type = '8CPU--32GB--H200:1'
    resources.accelerators = {'H200': 1}
    resources.use_spot = False
    resources.cluster_config_overrides = {}
    resources.network_tier = resources_utils.NetworkTier.STANDARD
    resources.requires_fuse = False
    resources.priority_class = 'caller-priority'
    resources.ephemeral_storage = None
    resources.hooks = None
    resources.extract_docker_image.return_value = None
    setattr(resources, 'assert_launchable', lambda: resources)
    projection = _worker_projection(
        protocol_version=protocol_version,
        kueue_admission={
            'local_queue_name': 'inference',
            'workload_priority_class_name': 'inference-low',
        },
        scratch=({
            'kind': 'memory',
            'mount_path': '/tmp',
            'volume_name': 'skypilot-serve-worker-tmp',
            'size_limit_bytes': 20 * 1024**3,
        } if protocol_version == 7 else None))
    cloud = kubernetes_identity.clouds.Kubernetes()
    region = mock.MagicMock()
    region.name = 'phx'

    queue_resolver = mock.Mock(
        side_effect=AssertionError('projection must not resolve a live queue'))
    managed_resolver = mock.Mock(side_effect=AssertionError(
        'projection must not resolve live management'))
    service_account_resolver = mock.Mock(
        side_effect=AssertionError('projection must not resolve live identity'))
    accelerator_resolver = mock.Mock(
        side_effect=AssertionError('projection must not discover live labels'))
    resource_key_resolver = mock.Mock(side_effect=AssertionError(
        'projection must not discover a resource key'))
    monkeypatch.setattr(skypilot_config, 'get_effective_queue_name',
                        queue_resolver)
    monkeypatch.setattr(skypilot_config, 'get_effective_kueue_require_managed',
                        managed_resolver)
    monkeypatch.setattr(kubernetes_identity.kubernetes_cloud,
                        'get_service_account_name', service_account_resolver)
    monkeypatch.setattr(kubernetes_identity.kubernetes_utils,
                        'get_accelerator_label_key_values',
                        accelerator_resolver)
    monkeypatch.setattr(kubernetes_identity.kubernetes_utils,
                        'get_gpu_resource_key', resource_key_resolver)
    monkeypatch.setattr(
        kubernetes_identity.kubernetes_utils, 'adjust_resources_to_allocatable',
        mock.Mock(
            side_effect=AssertionError('projection must not inspect nodes')))
    monkeypatch.setattr(kubernetes_identity.kubernetes_utils,
                        'resolve_effective_pod_config',
                        lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        kubernetes_identity.kubernetes_cloud.network_utils, 'get_port_mode',
        mock.Mock(return_value=mock.MagicMock(value='portforward')))
    monkeypatch.setattr(kubernetes_identity.kubernetes_cloud.gcp_utils,
                        'get_dws_config',
                        mock.Mock(return_value=(False, False, None)))
    config_resolver = mock.Mock(
        side_effect=lambda *, keys, default_value=None, **_kwargs: 30
        if keys == ('provision_timeout',) else default_value)
    monkeypatch.setattr(skypilot_config, 'get_effective_region_config',
                        config_resolver)
    monkeypatch.setattr(kubernetes_identity.kubernetes_cloud.catalog,
                        'get_image_id_from_tag',
                        mock.Mock(return_value='worker-image'))
    monkeypatch.setattr(
        cloud, '_detect_network_type',
        mock.Mock(return_value=(kubernetes_identity.kubernetes_utils.
                                KubernetesHighPerformanceNetworkType.NONE,
                                None)))

    if protocol_version != 7:
        with pytest.raises(
                ValueError,
                match='exact current worker placement projection protocol'):
            cloud.make_deploy_resources_variables(
                resources,
                resources_utils.ClusterName('replica', 'replica'),
                region,
                None,
                1,
                worker_placement_projection=projection)
        queue_resolver.assert_not_called()
        managed_resolver.assert_not_called()
        service_account_resolver.assert_not_called()
        accelerator_resolver.assert_not_called()
        resource_key_resolver.assert_not_called()
        config_resolver.assert_not_called()
        return

    deploy_vars = cloud.make_deploy_resources_variables(
        resources,
        resources_utils.ClusterName('replica', 'replica'),
        region,
        None,
        1,
        worker_placement_projection=projection)

    assert deploy_vars['k8s_namespace'] == 'inference'
    assert deploy_vars['k8s_service_account_name'] == 'phx-worker'
    assert deploy_vars['k8s_kueue_require_managed'] is True
    assert deploy_vars['k8s_kueue_local_queue_name'] == 'inference'
    assert deploy_vars[
        'k8s_kueue_workload_priority_class_name'] == 'inference-low'
    assert deploy_vars['k8s_acc_label_key'] == 'nvidia.com/gpu.product'
    assert deploy_vars['k8s_resource_key'] == 'nvidia.com/gpu'
    assert deploy_vars['k8s_projected_serve_worker_runtime_readiness'] is True
    assert deploy_vars['k8s_projected_worker_runtime_ready_marker'] == (
        '/tmp/skypilot-serve-worker-runtime-ready')
    expected_bootstrap_environment = (
        kubernetes_pod_spec.SERVE_WORKER_BOOTSTRAP_ENVIRONMENT)
    assert deploy_vars['k8s_projected_worker_bootstrap_environment'] == (
        expected_bootstrap_environment)
    assert {
        key: deploy_vars['k8s_env_vars'][key]
        for key in expected_bootstrap_environment
    } == expected_bootstrap_environment
    timeout_calls = [
        call for call in config_resolver.call_args_list
        if call.kwargs['keys'] == ('provision_timeout',)
    ]
    assert deploy_vars['timeout'] == '-1'
    assert timeout_calls == []


def test_legacy_yaml_restore_cannot_replace_projected_identity_or_cache():
    projection = {
        'projection_version': 7,
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
        'scheduler_name': 'default-scheduler',
        'kueue_admission': None,
        'provision_timeout': -1,
        'scratch': {
            'kind': 'none'
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
                            'command': ['/bin/bash', '-c', '--'],
                            'args': ['canonical bootstrap'],
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
        new_config,
        projection,
        expected_runtime_bootstrap_sha256=(
            _worker_bootstrap_sha256(new_config)))
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
            'labels': {},
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


def test_legacy_yaml_restore_reasserts_v7_kueue_admission():
    projection = _worker_projection(
        protocol_version=7,
        kueue_admission={
            'local_queue_name': 'inference',
            'workload_priority_class_name': 'inference-low',
        })
    fresh_node = {
        'node_config': {
            'metadata': {
                'labels': {
                    'kueue.x-k8s.io/queue-name': 'inference',
                    'kueue.x-k8s.io/priority-class': 'inference-low',
                },
            },
            'spec': {
                'containers': [{
                    'name': 'ray-node',
                    'command': ['/bin/bash', '-c', '--'],
                    'args': ['canonical bootstrap'],
                }],
            },
        },
    }
    stale_node = copy.deepcopy(fresh_node)
    stale_node['node_config']['metadata']['labels'] = {
        'kueue.x-k8s.io/queue-name': 'caller-queue',
        'kueue.x-k8s.io/priority-class': 'caller-priority',
    }
    fresh = {
        'provider': {
            'context': 'phx',
            'namespace': 'inference',
            'timeout': 60,
            'serve_worker_expected_runtime_bootstrap_sha256':
                (kubernetes_pod_spec.projected_worker_runtime_bootstrap_sha256(
                    fresh_node['node_config']['spec'])),
            'kueue_local_queue_name': 'inference',
            'kueue_require_managed': True,
            'kueue_workload_priority_class_name': 'inference-low',
        },
        'available_node_types': {
            'ray_head_default': fresh_node,
        },
    }
    stale = {
        'provider': {
            'context': 'old',
            'namespace': 'old',
            'timeout': 30,
            'kueue_local_queue_name': 'caller-queue',
            'kueue_require_managed': False,
            'kueue_workload_priority_class_name': 'caller-priority',
        },
        'available_node_types': {
            'ray_head_default': stale_node,
        },
    }

    restored = backend_utils._restore_projected_worker_kubernetes_fields(
        yaml_utils.dump_yaml_str(fresh), yaml_utils.dump_yaml_str(stale),
        projection)
    restored_config = yaml_utils.safe_load(restored)
    assert restored_config['provider']['timeout'] == -1
    assert restored_config['provider']['kueue_local_queue_name'] == 'inference'
    assert restored_config['provider']['kueue_require_managed'] is True
    assert restored_config['provider'][
        'kueue_workload_priority_class_name'] == 'inference-low'
    labels = restored_config['available_node_types']['ray_head_default'][
        'node_config']['metadata']['labels']
    assert labels['kueue.x-k8s.io/queue-name'] == 'inference'
    assert labels['kueue.x-k8s.io/priority-class'] == 'inference-low'


def test_legacy_yaml_restore_reasserts_v7_memory_scratch():
    projection = _worker_projection(
        protocol_version=7,
        scratch={
            'kind': 'memory',
            'mount_path': '/tmp',
            'volume_name': 'skypilot-serve-worker-tmp',
            'size_limit_bytes': 20 * 1024**3,
        })
    fresh = _projected_worker_cluster_yaml(bootstrap_environment=True)
    backend_utils._enforce_worker_projection_on_kubernetes_yaml(
        fresh,
        projection,
        expected_runtime_bootstrap_sha256=_worker_bootstrap_sha256(fresh))
    stale = copy.deepcopy(fresh)
    stale['provider']['timeout'] = 30
    stale_spec = stale['available_node_types']['ray_head_default'][
        'node_config']['spec']
    stale_spec['volumes'] = [{
        'name': 'caller-tmp',
        'hostPath': {
            'path': '/tmp',
        },
    }]
    stale_spec['containers'][0]['volumeMounts'] = [{
        'name': 'caller-tmp',
        'mountPath': '/tmp',
    }]

    restored = backend_utils._restore_projected_worker_kubernetes_fields(
        yaml_utils.dump_yaml_str(fresh), yaml_utils.dump_yaml_str(stale),
        projection)
    final_config = yaml_utils.safe_load(restored)
    final_spec = final_config['available_node_types']['ray_head_default'][
        'node_config']['spec']

    assert final_config['provider']['timeout'] == -1
    assert final_spec['volumes'][-1] == {
        'name': 'skypilot-serve-worker-tmp',
        'emptyDir': {
            'medium': 'Memory',
            'sizeLimit': str(20 * 1024**3),
        },
    }
    assert final_spec['containers'][0]['volumeMounts'][-1] == {
        'name': 'skypilot-serve-worker-tmp',
        'mountPath': '/tmp',
    }
    assert 'caller-tmp' not in yaml_utils.dump_yaml_str(final_spec)


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


def _memory_scratch_contract():
    return {
        'kind': 'memory',
        'mount_path': '/tmp',
        'volume_name': 'skypilot-serve-worker-tmp',
        'size_limit_bytes': 20 * 1024**3,
    }


def _admitted_memory_scratch_pod():
    pod = mock.Mock()
    pod.metadata.name = 'replica-head'
    pod.spec = {
        'containers': [{
            'name': 'ray-node',
            'volumeMounts': [{
                'name': 'skypilot-serve-worker-tmp',
                'mountPath': '/tmp',
            }],
        }],
        'volumes': [{
            'name': 'skypilot-serve-worker-tmp',
            'emptyDir': {
                'medium': 'Memory',
                'sizeLimit': str(20 * 1024**3),
            },
        }],
    }
    return pod


def test_admitted_worker_memory_scratch_contract_is_accepted():
    kubernetes_instance._attest_serve_worker_scratch(
        _admitted_memory_scratch_pod(), 'inference', 'phx',
        _memory_scratch_contract())


def test_real_kubernetes_client_pod_memory_scratch_contract_is_accepted():
    client = kubernetes_adaptor.kubernetes.client
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name='replica-head'),
        spec=client.V1PodSpec(containers=[
            client.V1Container(name='ray-node',
                               volume_mounts=[
                                   client.V1VolumeMount(
                                       name='skypilot-serve-worker-tmp',
                                       mount_path='/tmp')
                               ])
        ],
                              volumes=[
                                  client.V1Volume(
                                      name='skypilot-serve-worker-tmp',
                                      empty_dir=client.V1EmptyDirVolumeSource(
                                          medium='Memory', size_limit='20Gi'))
                              ]))

    kubernetes_instance._attest_serve_worker_scratch(pod, 'inference', 'phx',
                                                     _memory_scratch_contract())


@pytest.mark.parametrize('mutation',
                         ['size', 'alternate_source', 'recursive_read_only'])
def test_admitted_worker_scratch_mutation_is_deleted(monkeypatch, mutation):
    pod = _admitted_memory_scratch_pod()
    if mutation == 'size':
        pod.spec['volumes'][0]['emptyDir']['sizeLimit'] = str(10 * 1024**3)
    elif mutation == 'alternate_source':
        pod.spec['volumes'][0]['secret'] = {}
    else:
        pod.spec['containers'][0]['volumeMounts'][0][
            'recursiveReadOnly'] = 'Enabled'
    core_api = mock.MagicMock()
    core_api.read_namespaced_pod.side_effect = _kubernetes_api_error(404)
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)

    with pytest.raises(kubernetes_config.KubernetesError,
                       match='worker scratch contract'):
        kubernetes_instance._attest_serve_worker_scratch(
            pod, 'inference', 'phx', _memory_scratch_contract())

    core_api.delete_namespaced_pod.assert_called_once_with(
        'replica-head',
        'inference',
        grace_period_seconds=0,
        _request_timeout=kubernetes_config.DELETION_TIMEOUT)


@pytest.mark.parametrize('mount_path', ['/var/../tmp/', '/tmp/private'])
def test_admitted_worker_none_scratch_rejects_tmp_path_alias(
        monkeypatch, mount_path):
    pod = mock.Mock()
    pod.metadata.name = 'replica-head'
    pod.spec = {
        'containers': [{
            'name': 'ray-node',
            'volumeMounts': [{
                'name': 'caller-volume',
                'mountPath': mount_path,
            }],
        }],
        'volumes': [{
            'name': 'caller-volume',
            'emptyDir': {},
        }],
    }
    core_api = mock.MagicMock()
    core_api.read_namespaced_pod.side_effect = _kubernetes_api_error(404)
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)

    with pytest.raises(kubernetes_config.KubernetesError,
                       match='worker scratch contract'):
        kubernetes_instance._attest_serve_worker_scratch(
            pod, 'inference', 'phx', {'kind': 'none'})

    core_api.delete_namespaced_pod.assert_called_once()


def test_admitted_worker_memory_scratch_rejects_nested_tmp_mount(monkeypatch):
    pod = _admitted_memory_scratch_pod()
    pod.spec['containers'][0]['volumeMounts'].append({
        'name': 'webhook-volume',
        'mountPath': '/tmp/webhook',
    })
    pod.spec['volumes'].append({
        'name': 'webhook-volume',
        'emptyDir': {},
    })
    core_api = mock.MagicMock()
    core_api.read_namespaced_pod.side_effect = _kubernetes_api_error(404)
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)

    with pytest.raises(kubernetes_config.KubernetesError,
                       match='worker scratch contract'):
        kubernetes_instance._attest_serve_worker_scratch(
            pod, 'inference', 'phx', _memory_scratch_contract())

    core_api.delete_namespaced_pod.assert_called_once()


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
    pod.spec.init_containers = []
    pod.spec.overhead = {}
    pod.spec.affinity = affinity if include_affinity else None
    return pod


def test_admitted_worker_accelerator_scheduling_contract_is_accepted():
    kubernetes_instance._attest_serve_worker_accelerator_scheduling(
        _admitted_accelerator_pod(), 'inference', 'phx',
        'nvidia.com/gpu.product', ['NVIDIA-H200'], 'nvidia.com/gpu', 1)


def test_real_kubernetes_client_pod_accelerator_contract_is_accepted():
    client = kubernetes_adaptor.kubernetes.client
    expression = client.V1NodeSelectorRequirement(key='nvidia.com/gpu.product',
                                                  operator='In',
                                                  values=['NVIDIA-H200'])
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name='replica-head'),
        spec=client.V1PodSpec(
            containers=[
                client.V1Container(name='ray-node',
                                   resources=client.V1ResourceRequirements(
                                       requests={'nvidia.com/gpu': 1},
                                       limits={'nvidia.com/gpu': 1})),
            ],
            init_containers=[],
            overhead={},
            affinity=client.V1Affinity(node_affinity=client.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=(
                    client.V1NodeSelector(node_selector_terms=[
                        client.V1NodeSelectorTerm(
                            match_expressions=[expression])
                    ]))))))

    kubernetes_instance._attest_serve_worker_accelerator_scheduling(
        pod, 'inference', 'phx', 'nvidia.com/gpu.product', ['NVIDIA-H200'],
        'nvidia.com/gpu', 1)


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


@pytest.mark.parametrize('injection', [
    'sidecar',
    'init_container',
    'overhead',
    'pod_resources',
    'pod_resource_claim',
    'container_resource_claim',
])
def test_admitted_worker_rejects_noncanonical_accelerator_surface(
        monkeypatch, injection):
    pod = _admitted_accelerator_pod()
    injected_resources = mock.Mock(requests={'nvidia.com/gpu': 8}, limits={})
    injected_container = mock.Mock(name='admission-injected',
                                   resources=injected_resources)
    if injection == 'sidecar':
        pod.spec.containers.append(injected_container)
    elif injection == 'init_container':
        pod.spec.init_containers = [injected_container]
    elif injection == 'overhead':
        pod.spec.overhead = {'nvidia.com/gpu': 8}
    elif injection == 'pod_resources':
        pod.spec.resources = mock.Mock(requests={'nvidia.com/gpu': 8},
                                       limits={})
    elif injection == 'pod_resource_claim':
        pod.spec.resource_claims = [mock.Mock(name='opaque-gpu')]
    else:
        pod.spec.containers[0].resources.claims = [mock.Mock(name='opaque-gpu')]
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
    pod.metadata.namespace = 'inference'
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
    pod.metadata.namespace = 'inference'
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
    pod.metadata.namespace = 'inference'
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
    pod.metadata.namespace = 'inference'
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
    pod.metadata.namespace = 'inference'
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


def test_admitted_worker_namespace_mismatch_is_deleted(monkeypatch):
    pod = mock.MagicMock()
    pod.metadata.name = 'replica-head'
    pod.metadata.namespace = 'webhook-mutated-namespace'
    pod.spec.service_account_name = 'phx-worker'
    core_api = mock.MagicMock()
    core_api.read_namespaced_pod.side_effect = _kubernetes_api_error(404)
    monkeypatch.setattr(kubernetes_instance.kubernetes, 'core_api',
                        lambda *_args, **_kwargs: core_api)

    with pytest.raises(kubernetes_config.KubernetesError,
                       match=r"has namespace 'webhook-mutated-namespace'; "
                       r"expected 'inference'"):
        kubernetes_instance._attest_serve_worker_service_account(
            pod, 'inference', 'phx', 'phx-worker')

    core_api.delete_namespaced_pod.assert_called_once()


def test_final_kubernetes_yaml_requires_one_runtime_container():
    projection = _worker_projection(
        protocol_version=7,
        scratch={
            'kind': 'memory',
            'mount_path': '/tmp',
            'volume_name': 'skypilot-serve-worker-tmp',
            'size_limit_bytes': 20 * 1024**3,
        })
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
            cluster_yaml,
            projection,
            expected_runtime_bootstrap_sha256='0' * 64)
