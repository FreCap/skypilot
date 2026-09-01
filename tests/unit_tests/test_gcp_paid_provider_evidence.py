"""Tests for exact GCP VM and launch-owned boot-disk evidence."""

from unittest import mock

import pytest

from sky.provision import common
from sky.provision.gcp import instance
from sky.provision.gcp import instance_utils
from sky.server.requests import postgres as request_postgres

_PROJECT_ID = 'boltz-spot-project'
_ZONE = 'us-central1-a'
_INSTANCE_NAME = 'sky-svc-head-12345678-compute'
_INSTANCE_TYPE = 'g2-standard-4'


def _running_spot_instance(**updates):
    prefix = ('https://www.googleapis.com/compute/v1/projects/'
              f'{_PROJECT_ID}')
    value = {
        'name': _INSTANCE_NAME,
        'selfLink': f'{prefix}/zones/{_ZONE}/instances/{_INSTANCE_NAME}',
        'zone': f'{prefix}/zones/{_ZONE}',
        'machineType': f'{prefix}/zones/{_ZONE}/machineTypes/{_INSTANCE_TYPE}',
        'status': 'RUNNING',
        'scheduling': {
            'provisioningModel': 'SPOT',
        },
    }
    value.update(updates)
    return value


def test_capture_fresh_gcp_identity_requires_exact_running_spot() -> None:
    identity = instance._capture_fresh_compute_instance_identity(  # pylint: disable=protected-access
        project_id=_PROJECT_ID,
        zone=_ZONE,
        expected_instance_type=_INSTANCE_TYPE,
        created_instance_ids=[_INSTANCE_NAME],
        resumed_instance_ids=[],
        running_instances={_INSTANCE_NAME: _running_spot_instance()})

    assert identity == common.GCPInstanceIdentity(project_id=_PROJECT_ID,
                                                  zone=_ZONE,
                                                  instance_name=_INSTANCE_NAME,
                                                  instance_type=_INSTANCE_TYPE,
                                                  market_type='spot')


@pytest.mark.parametrize('updates', [{
    'name': 'other-instance'
}, {
    'status': 'PROVISIONING'
}, {
    'selfLink': ('https://www.googleapis.com/compute/v1/projects/'
                 f'other-project/zones/{_ZONE}/instances/{_INSTANCE_NAME}')
}, {
    'zone': ('https://www.googleapis.com/compute/v1/projects/'
             f'{_PROJECT_ID}/zones/us-central1-b')
}, {
    'machineType': ('https://www.googleapis.com/compute/v1/projects/'
                    f'{_PROJECT_ID}/zones/{_ZONE}/machineTypes/'
                    'g2-standard-8')
}, {
    'scheduling': {
        'provisioningModel': 'STANDARD'
    }
}, {
    'scheduling': {}
}],
                         ids=('wrong-name', 'not-running', 'wrong-project',
                              'wrong-zone', 'wrong-machine', 'not-spot',
                              'missing-market'))
def test_capture_fresh_gcp_identity_rejects_observation_mismatch(
        updates) -> None:
    with pytest.raises(ValueError, match='exact RUNNING Spot identity'):
        instance._capture_fresh_compute_instance_identity(  # pylint: disable=protected-access
            project_id=_PROJECT_ID,
            zone=_ZONE,
            expected_instance_type=_INSTANCE_TYPE,
            created_instance_ids=[_INSTANCE_NAME],
            resumed_instance_ids=[],
            running_instances={
                _INSTANCE_NAME: _running_spot_instance(**updates)
            })


@pytest.mark.parametrize(
    ('expected_instance_type', 'created', 'resumed', 'running'),
    [(None, [_INSTANCE_NAME], [], {
        _INSTANCE_NAME: _running_spot_instance()
    }), (_INSTANCE_TYPE, [], [], {}),
     (_INSTANCE_TYPE, [_INSTANCE_NAME, 'worker'], [], {
         _INSTANCE_NAME: _running_spot_instance(),
         'worker': _running_spot_instance(name='worker'),
     }),
     (_INSTANCE_TYPE, [_INSTANCE_NAME], ['resumed'], {
         _INSTANCE_NAME: _running_spot_instance(),
     }), (_INSTANCE_TYPE, [_INSTANCE_NAME], [], {}),
     (_INSTANCE_TYPE, [_INSTANCE_NAME], [], {
         _INSTANCE_NAME: _running_spot_instance(),
         'other': _running_spot_instance(name='other'),
     })],
    ids=('missing-machine', 'no-create', 'multi-create', 'resumed',
         'not-observed', 'extra-running'))
def test_capture_fresh_gcp_identity_omits_inexact_fresh_set(
        expected_instance_type, created, resumed, running) -> None:
    assert instance._capture_fresh_compute_instance_identity(  # pylint: disable=protected-access
        project_id=_PROJECT_ID,
        zone=_ZONE,
        expected_instance_type=expected_instance_type,
        created_instance_ids=created,
        resumed_instance_ids=resumed,
        running_instances=running) is None


@pytest.mark.parametrize('provisioning_model', ['SPOT', 'STANDARD'])
def test_run_instances_populates_only_matching_gcp_spot_identity(
        monkeypatch, provisioning_model) -> None:
    config = common.ProvisionConfig(provider_config={
        'project_id': _PROJECT_ID,
        'availability_zone': _ZONE,
    },
                                    authentication_config={},
                                    docker_config={},
                                    node_config={'machineType': _INSTANCE_TYPE},
                                    count=1,
                                    tags={},
                                    resume_stopped_nodes=False,
                                    ports_to_open_on_launch=None)
    observed = _running_spot_instance(scheduling={
        'provisioningModel': provisioning_model,
    })
    monkeypatch.setattr(
        instance_utils.GCPComputeInstance, 'filter',
        mock.Mock(side_effect=[{}, {}, {}, {
            _INSTANCE_NAME: observed
        }]))
    monkeypatch.setattr(instance_utils.GCPComputeInstance, 'create_instances',
                        mock.Mock(return_value=(None, [_INSTANCE_NAME])))

    record = instance._run_instances(  # pylint: disable=protected-access
        'us-central1', 'sky-svc', config)

    if provisioning_model == 'SPOT':
        assert record.fresh_gcp_instance_identity is not None
        assert record.fresh_gcp_instance_identity.project_id == _PROJECT_ID
    else:
        assert record.fresh_gcp_instance_identity is None


def test_query_managed_boot_disks_requires_marker_and_exact_name(
        monkeypatch) -> None:
    compute = mock.Mock()
    disks = compute.disks.return_value
    request = mock.Mock()
    disks.list.return_value = request
    request.execute.return_value = {
        'items': [{
            'name': 'svc-abc-head-1234abcd-compute',
            'labels': {
                'skypilot-managed': 'true',
            },
        }, {
            'name': 'svc-abc-worker-8765dcba-compute',
            'labels': {
                'skypilot-managed': 'true',
            },
        }, {
            'name': 'svc-abc-head-aaaaaaaa-compute',
            'labels': {},
        }, {
            'name': 'svc-abcd-head-aaaaaaaa-compute',
            'labels': {
                'skypilot-managed': 'true',
            },
        }, {
            'name': 'user-volume',
            'labels': {
                'skypilot-managed': 'true',
                'ray-cluster-name': 'svc-abc',
            },
        }, {
            'name': 'other-volume',
            'labels': {
                'skypilot-managed': 'true',
                'ray-cluster-name': 'other-cluster',
            },
        }]
    }
    disks.list_next.return_value = None
    monkeypatch.setattr(instance_utils.GCPComputeInstance, 'load_resource',
                        lambda: compute)

    names = instance.query_managed_boot_disks('svc-abc', {
        'project_id': 'boltz-498512',
        'availability_zone': 'us-east4-a',
    })

    assert names == [
        'svc-abc-head-1234abcd-compute',
        'svc-abc-worker-8765dcba-compute',
    ]
    disks.list.assert_called_once_with(project='boltz-498512',
                                       zone='us-east4-a',
                                       filter='labels.skypilot-managed = true')


def test_query_create_operations_classifies_exact_generated_targets(
        monkeypatch) -> None:
    compute = mock.Mock()
    operations = compute.zoneOperations.return_value
    request = mock.Mock()
    operations.list.return_value = request

    def _operation(name, status, *, operation_type='insert', error=None):
        value = {
            'name': f'operation-{name}',
            'operationType': operation_type,
            'status': status,
            'targetLink': ('https://compute.googleapis.com/compute/v1/projects/'
                           f'p/zones/z/instances/{name}'),
        }
        if error is not None:
            value['error'] = error
        return value

    request.execute.return_value = {
        'items': [
            _operation('svc-abc-head-11111111-compute', 'RUNNING'),
            _operation(
                'svc-abc-worker-22222222-compute',
                'DONE',
                error={'errors': [{
                    'code': 'ZONE_RESOURCE_POOL_EXHAUSTED'
                }]}),
            _operation('svc-abc-worker-33333333-compute', 'DONE'),
            _operation('svc-abc-worker-44444444-compute',
                       'RUNNING',
                       operation_type='delete'),
            _operation('other-head-55555555-compute', 'RUNNING'),
        ]
    }
    operations.list_next.return_value = None
    monkeypatch.setattr(instance_utils.GCPComputeInstance, 'load_resource',
                        lambda: compute)

    targets = instance.query_instance_create_operation_targets(
        'svc-abc', {
            'project_id': 'boltz-498512',
            'availability_zone': 'us-east4-a',
        })

    assert targets == {
        'failed': ['svc-abc-worker-22222222-compute'],
        'inflight': ['svc-abc-head-11111111-compute'],
        'succeeded': ['svc-abc-worker-33333333-compute'],
    }
    operations.list.assert_called_once_with(project='boltz-498512',
                                            zone='us-east4-a',
                                            filter='targetLink eq .*svc-abc.*')


def test_compute_timeout_retains_operation_for_reconciliation(
        monkeypatch) -> None:
    compute = mock.Mock()
    monkeypatch.setattr(instance_utils.GCPComputeInstance, 'load_resource',
                        lambda: compute)
    monkeypatch.setattr(instance_utils, '_format_and_log_message_from_errors',
                        mock.Mock())

    with pytest.raises(common.ProvisionerError, match='Operation timed out'):
        instance_utils.GCPComputeInstance.wait_for_operation(
            {'name': 'operation-create'},
            'boltz-498512',
            zone='us-east4-a',
            timeout=0)

    compute.zoneOperations.return_value.delete.assert_not_called()


def test_custom_disk_name_disables_legacy_absence_contract() -> None:
    task_yaml = """
resources:
  cloud: gcp
  instance_type: g2-standard-4
  accelerators: L4:1
  use_spot: true
volumes:
  /models:
    name: custom-model-disk
    store: gcp
run: echo ready
"""

    assert not (
        request_postgres.
        _gcp_launch_task_supports_plain_compute_disk_reconciliation(task_yaml))


def test_plain_gcp_compute_task_supports_generated_boot_disk_identity() -> None:
    task_yaml = """
resources:
  cloud: gcp
  instance_type: g2-standard-4
  accelerators: L4:1
  use_spot: true
run: echo ready
"""

    assert (
        request_postgres.
        _gcp_launch_task_supports_plain_compute_disk_reconciliation(task_yaml))


def test_mig_override_disables_plain_compute_evidence() -> None:
    task_yaml = """
resources:
  cloud: gcp
  instance_type: g2-standard-4
  accelerators: L4:1
  use_spot: true
  _cluster_config_overrides:
    gcp:
      managed_instance_group:
        run_duration: 3600
run: echo ready
"""

    assert not (
        request_postgres.
        _gcp_launch_task_supports_plain_compute_disk_reconciliation(task_yaml))


def test_tpu_pool_is_not_plain_compute() -> None:
    assert not request_postgres._gcp_paid_pool_is_plain_compute({
        'accelerators': [['tpu-v5litepod-4', 1]],
        'instance_type': 'tpu-vm',
    })
    assert not request_postgres._gcp_paid_pool_is_plain_compute({
        'accelerators': [['custom-accelerator', 1]],
        'instance_type': 'TPU-VM',
    })
    assert request_postgres._gcp_paid_pool_is_plain_compute({
        'accelerators': [['l4', 1]],
        'instance_type': 'g2-standard-4',
    })


def test_terminate_managed_boot_disks_waits_for_every_delete(
        monkeypatch) -> None:
    names = [
        'svc-abc-head-1234abcd-compute',
        'svc-abc-worker-8765dcba-compute',
    ]
    compute = mock.Mock()
    disks = compute.disks.return_value
    operations = [{'name': 'delete-head'}, {'name': 'delete-worker'}]
    disks.delete.return_value.execute.side_effect = operations
    monkeypatch.setattr(instance, 'query_managed_boot_disks',
                        lambda *_args: names)
    monkeypatch.setattr(instance_utils.GCPComputeInstance, 'load_resource',
                        lambda: compute)
    wait = mock.Mock()
    monkeypatch.setattr(instance_utils.GCPComputeInstance, 'wait_for_operation',
                        wait)
    provider_config = {
        'project_id': 'boltz-498512',
        'availability_zone': 'us-east4-a',
    }

    instance.terminate_managed_boot_disks('svc-abc', provider_config)

    assert disks.delete.call_args_list == [
        mock.call(project='boltz-498512', zone='us-east4-a', disk=names[0]),
        mock.call(project='boltz-498512', zone='us-east4-a', disk=names[1]),
    ]
    assert wait.call_args_list == [
        mock.call(operations[0], 'boltz-498512', zone='us-east4-a'),
        mock.call(operations[1], 'boltz-498512', zone='us-east4-a'),
    ]
