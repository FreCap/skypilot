import inspect
import pathlib
import pickle
import subprocess
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from sky import logs
from sky import resources
from sky import skypilot_config
from sky.backends import backend_utils
from sky.catalog import gcp_catalog
from sky.clouds import Region
from sky.clouds import Zone
from sky.clouds.gcp import GCP
from sky.clouds.utils import gcp_utils
from sky.provision import common
from sky.provision import constants as provision_constants
from sky.provision.gcp import api as gcp_api
from sky.provision.gcp import config as gcp_config
from sky.provision.gcp import constants as gcp_constants
from sky.provision.gcp import instance as gcp_instance
from sky.provision.gcp import instance_utils
from sky.provision.gcp import tpu_node
from sky.provision.gcp import volume_utils as gcp_volume_utils
from sky.utils import common_utils
from sky.utils import config_utils
from sky.utils import resources_utils


def test_gcp_rtxpro6000_instance_type_mapping():
    # RTXPRO6000 (GCP G4) maps to g4-standard-{48,96,192,384} for 1/2/4/8 GPUs.
    assert gcp_catalog._ACC_INSTANCE_TYPE_DICTS['RTXPRO6000'] == {
        1: ['g4-standard-48'],
        2: ['g4-standard-96'],
        4: ['g4-standard-192'],
        8: ['g4-standard-384'],
    }
    expected = {
        'g4-standard-48': 1,
        'g4-standard-96': 2,
        'g4-standard-192': 4,
        'g4-standard-384': 8,
    }
    for instance_type, count in expected.items():
        assert gcp_catalog._INSTANCE_TYPE_TO_ACC[instance_type] == {
            'RTXPRO6000': count,
        }


@pytest.mark.parametrize(
    'instance_type',
    ['g4-standard-48', 'g4-standard-96', 'g4-standard-192', 'g4-standard-384'])
def test_gcp_g4_uses_hyperdisk_balanced(instance_type):
    # G4 only supports hyperdisk-balanced (no pd-* support), like n4/a4.
    tier2name = gcp_volume_utils.get_data_disk_tier_mapping(instance_type)
    for tier in resources_utils.DiskTier:
        if tier == resources_utils.DiskTier.BEST:
            continue
        assert tier2name[tier] == 'hyperdisk-balanced', (
            f'{instance_type} tier {tier.value} should be hyperdisk-balanced, '
            f'got {tier2name[tier]}')


@pytest.mark.parametrize((
    'mock_return', 'expected'
), [([
    gcp_utils.GCPReservation(
        self_link=
        'https://www.googleapis.com/compute/v1/projects/<project>/zones/<zone>/reservations/<reservation>',
        specific_reservation=gcp_utils.SpecificReservation(count=1,
                                                           in_use_count=0),
        specific_reservation_required=True,
        zone='zone')
], {
    'projects/<project>/reservations/<reservation>': 1
}),
    ([
        gcp_utils.GCPReservation(
            self_link=
            'https://www.googleapis.com/compute/v1/projects/<project>/zones/<zone>/reservations/<reservation>',
            specific_reservation=gcp_utils.SpecificReservation(count=2,
                                                               in_use_count=1),
            specific_reservation_required=False,
            zone='zone')
    ], {
        'projects/<project>/reservations/<reservation>': 1
    }),
    ([
        gcp_utils.GCPReservation(
            self_link=
            'https://www.googleapis.com/compute/v1/projects/<project2>/zones/<zone>/reservations/<reservation>',
            specific_reservation=gcp_utils.SpecificReservation(count=1,
                                                               in_use_count=0),
            specific_reservation_required=True,
            zone='zone')
    ], {})])
def test_gcp_get_reservations_available_resources(mock_return, expected):
    gcp = GCP()
    with patch.object(gcp_utils,
                      'list_reservations_for_instance_type_in_zone',
                      return_value=mock_return):
        reservations = gcp.get_reservations_available_resources(
            'instance_type', 'region', 'zone',
            {'projects/<project>/reservations/<reservation>'})
        assert reservations == expected


def test_gcp_reservation_from_dict():
    r = gcp_utils.GCPReservation.from_dict({
        'selfLink': 'test',
        'specificReservation': {
            'count': '1',
            'inUseCount': '0'
        },
        'specificReservationRequired': True,
        'zone': 'zone'
    })

    assert r.self_link == 'test'
    assert r.specific_reservation.count == 1
    assert r.specific_reservation.in_use_count == 0
    assert r.specific_reservation_required == True
    assert r.zone == 'zone'


@pytest.mark.parametrize(('count', 'in_use_count', 'expected'), [(1, 0, 1),
                                                                 (1, 1, 0)])
def test_gcp_reservation_available_resources(count, in_use_count, expected):
    r = gcp_utils.GCPReservation(
        self_link='test',
        specific_reservation=gcp_utils.SpecificReservation(
            count=count, in_use_count=in_use_count),
        specific_reservation_required=True,
        zone='zone')

    assert r.available_resources == expected


def test_gcp_reservation_name():
    r = gcp_utils.GCPReservation(
        self_link=
        'https://www.googleapis.com/compute/v1/projects/<project>/zones/<zone>/reservations/<reservation-name>',
        specific_reservation=gcp_utils.SpecificReservation(count=1,
                                                           in_use_count=1),
        specific_reservation_required=True,
        zone='zone')
    assert r.name == 'projects/<project>/reservations/<reservation-name>'


@pytest.mark.parametrize(
    ('specific_reservations', 'specific_reservation_required', 'expected'), [
        ([], False, True),
        ([], True, False),
        (['projects/<project>/reservations/<reservation>'], True, True),
        (['projects/<project>/reservations/<invalid>'], True, False),
    ])
def test_gcp_reservation_is_consumable(specific_reservations,
                                       specific_reservation_required, expected):
    r = gcp_utils.GCPReservation(
        self_link=
        'https://www.googleapis.com/compute/v1/projects/<project>/zones/<zone>/reservations/<reservation>',
        specific_reservation=gcp_utils.SpecificReservation(count=1,
                                                           in_use_count=1),
        specific_reservation_required=specific_reservation_required,
        zone='zone')
    assert r.is_consumable(
        specific_reservations=specific_reservations) is expected


def test_gcp_get_user_identities_workspace_cache_bypass():
    """Test that get_user_identities bypasses cache when workspace changes."""
    # Mock the external dependencies
    with patch('sky.clouds.gcp._run_output') as mock_run_output, \
         patch.object(GCP, 'get_project_id') as mock_get_project_id, \
         patch.object(skypilot_config, 'get_workspace_cloud') as mock_get_workspace_cloud:

        # Set up different project IDs for different workspaces
        def workspace_cloud_side_effect(cloud_name):
            current_workspace = skypilot_config.get_active_workspace()
            if current_workspace == 'default':
                return {'project_id': 'default-project'}
            elif current_workspace == 'other':
                return {'project_id': 'other-project'}
            return {}

        def project_id_side_effect():
            current_workspace = skypilot_config.get_active_workspace()
            if current_workspace == 'default':
                return 'default-project'
            elif current_workspace == 'other':
                return 'other-project'
            return 'fallback-project'

        mock_get_workspace_cloud.side_effect = workspace_cloud_side_effect
        mock_get_project_id.side_effect = project_id_side_effect
        mock_run_output.return_value = 'test@example.com'

        # First call in default workspace
        result1 = GCP.get_user_identities()
        expected1 = [['test@example.com [project_id=default-project]']]
        assert result1 == expected1

        # Switch to another workspace and call again
        with skypilot_config.local_active_workspace_ctx('other'):
            result2 = GCP.get_user_identities()
            expected2 = [['test@example.com [project_id=other-project]']]
            assert result2 == expected2

        # Back to default workspace - should get the original result
        result3 = GCP.get_user_identities()
        assert result3 == expected1

        # Verify that the underlying method was called for each different workspace config
        # Should be called 3 times total: once for default, once for other, once for default again
        assert mock_run_output.call_count == 2
        assert mock_get_project_id.call_count == 2

        # Verify workspace cloud was queried for each call
        assert mock_get_workspace_cloud.call_count == 3
        mock_get_workspace_cloud.assert_any_call('gcp')


def _make_subnet(name: str, vpc_name: str, project_id: str = 'test-project'):
    return {
        'name': name,
        'network': f'projects/{project_id}/global/networks/{vpc_name}',
        'selfLink': f'https://example.com/{name}',
    }


def _make_provision_config(provider_config):
    return common.ProvisionConfig(
        provider_config=provider_config,
        authentication_config={},
        docker_config={},
        node_config={},
        count=1,
        tags={},
        resume_stopped_nodes=False,
        ports_to_open_on_launch=None,
    )


def test_gcp_config_gateway_polling_shapes():
    crm = MagicMock()
    crm_request = MagicMock()
    crm_request.execute.return_value = {'done': True}
    crm.operations().get.return_value = crm_request
    crm_operation = {'name': 'create-project'}

    assert gcp_config.wait_for_crm_operation(crm_operation, crm) == {
        'done': True
    }
    crm.operations().get.assert_called_once_with(name='create-project')

    compute = MagicMock()
    global_request = MagicMock()
    global_request.execute.return_value = {'status': 'DONE'}
    compute.globalOperations().get.return_value = global_request
    global_operation = {'name': 'create-network'}
    assert gcp_config.wait_for_compute_global_operation('project',
                                                        global_operation,
                                                        compute) == {
                                                            'status': 'DONE'
                                                        }
    compute.globalOperations().get.assert_called_once_with(
        project='project', operation='create-network')

    region_request = MagicMock()
    region_request.execute.return_value = {'status': 'DONE'}
    compute.regionOperations().get.return_value = region_request
    region_operation = {'name': 'create-subnet'}
    assert gcp_config.wait_for_compute_region_operation('project',
                                                        'us-central1',
                                                        region_operation,
                                                        compute) == {
                                                            'status': 'DONE'
                                                        }
    compute.regionOperations().get.assert_called_once_with(
        project='project', region='us-central1', operation='create-subnet')


def test_gcp_config_gateway_polling_propagates_operation_error():
    compute = MagicMock()
    request = MagicMock()
    request.execute.return_value = {'error': 'permission denied'}
    compute.globalOperations().get.return_value = request

    with pytest.raises(Exception, match='permission denied'):
        gcp_config.wait_for_compute_global_operation('project',
                                                     {'name': 'create-network'},
                                                     compute)


def test_gcp_config_gateway_rejects_empty_poll_budget(monkeypatch):
    monkeypatch.setattr(gcp_constants, 'MAX_POLLS', 0)

    with pytest.raises(RuntimeError, match='polling did not run'):
        gcp_config.wait_for_crm_operation({'name': 'create-project'},
                                          MagicMock())


def test_gcp_http_retry_rejects_empty_retry_budget():
    wrapped = instance_utils._retry_on_gcp_http_exception(  # pylint: disable=protected-access
        max_retries=0)(lambda: 'ok')

    with pytest.raises(ValueError, match='max_retries must be at least 1'):
        wrapped()


@pytest.mark.parametrize(
    ('node_type', 'handler'),
    [(instance_utils.GCPNodeType.COMPUTE, instance_utils.GCPComputeInstance),
     (instance_utils.GCPNodeType.MIG, instance_utils.GCPManagedInstanceGroup),
     (instance_utils.GCPNodeType.TPU, instance_utils.GCPTPUVMInstance)])
def test_run_instances_enforces_managed_label(monkeypatch, node_type, handler):
    config = common.ProvisionConfig(
        provider_config={
            'project_id': 'project',
            'availability_zone': 'us-central1-a',
        },
        authentication_config={},
        docker_config={},
        node_config={},
        count=1,
        tags={
            'team': 'research',
            provision_constants.TAG_SKYPILOT_MANAGED: 'false',
        },
        resume_stopped_nodes=False,
        ports_to_open_on_launch=None,
    )
    monkeypatch.setattr(instance_utils, 'get_node_type',
                        MagicMock(return_value=node_type))
    filter_mock = MagicMock(side_effect=[
        {},
        {},
        {},
        {
            'node-1': {
                handler.STATUS_FIELD: handler.RUNNING_STATE,
            }
        },
    ])
    create_mock = MagicMock(return_value=(None, ['node-1']))
    monkeypatch.setattr(handler, 'filter', filter_mock)
    monkeypatch.setattr(handler, 'create_instances', create_mock)

    result = gcp_instance._run_instances(  # pylint: disable=protected-access
        'us-central1', 'cluster', config)

    labels = create_mock.call_args.args[4]
    assert labels == {
        provision_constants.TAG_SKYPILOT_MANAGED:
            provision_constants.SKYPILOT_MANAGED_TAG_VALUE,
        'team': 'research',
    }
    assert config.tags[provision_constants.TAG_SKYPILOT_MANAGED] == 'false'
    assert result.created_instance_ids == ['node-1']


def test_compute_instance_labels_new_persistent_disks(monkeypatch):
    node_config = {
        'machineType': 'n2-standard-4',
        'disks': [
            {
                'type': 'PERSISTENT',
                'initializeParams': {
                    'sourceImage': 'projects/image-project/global/images/base',
                    'labels': {
                        'owner': 'research',
                        provision_constants.TAG_SKYPILOT_MANAGED: 'false',
                    },
                },
            },
            {
                # An omitted type defaults to a persistent disk.
                'initializeParams': {},
            },
            {
                'type': 'PERSISTENT',
                'source': 'projects/project/zones/us-central1-a/disks/existing',
            },
            {
                'type': 'PERSISTENT',
                'source': 'projects/project/zones/us-central1-a/disks/existing-raw',
                'initializeParams': {
                    'labels': {
                        'owner': 'research',
                    },
                },
            },
            {
                'type': 'SCRATCH',
                'initializeParams': {
                    'diskType': 'local-ssd',
                    'labels': {
                        'owner': 'research',
                    },
                },
            },
        ],
    }
    captured_config = {}

    def _capture_create(cls, names, project_id, zone, config, head_tag_needed):
        del cls, names, project_id, zone, head_tag_needed
        captured_config.update(config)
        return None

    monkeypatch.setattr(instance_utils.GCPComputeInstance, '_create_instances',
                        classmethod(_capture_create))

    errors, _ = instance_utils.GCPComputeInstance.create_instances(
        'cluster',
        'project',
        'us-central1-a',
        node_config,
        labels={},
        count=1,
        total_count=1,
        include_head_node=True)

    assert errors is None
    disks = captured_config['disks']
    assert disks[0]['initializeParams']['labels'] == {
        'owner': 'research',
        provision_constants.TAG_SKYPILOT_MANAGED:
            provision_constants.SKYPILOT_MANAGED_TAG_VALUE,
        provision_constants.TAG_RAY_CLUSTER_NAME: 'cluster',
    }
    assert disks[1]['initializeParams']['labels'] == {
        provision_constants.TAG_SKYPILOT_MANAGED:
            provision_constants.SKYPILOT_MANAGED_TAG_VALUE,
        provision_constants.TAG_RAY_CLUSTER_NAME: 'cluster',
    }
    assert 'initializeParams' not in disks[2]
    assert disks[3]['initializeParams']['labels'] == {'owner': 'research'}
    assert disks[4]['initializeParams']['labels'] == {'owner': 'research'}
    assert node_config['disks'][0]['initializeParams']['labels'][
        provision_constants.TAG_SKYPILOT_MANAGED] == 'false'


def test_mig_instance_template_labels_new_persistent_disks(monkeypatch):
    template_config = {}
    monkeypatch.setattr(instance_utils.mig_utils,
                        'check_instance_template_exits',
                        MagicMock(return_value=False))
    monkeypatch.setattr(instance_utils.mig_utils,
                        'check_managed_instance_group_exists',
                        MagicMock(return_value=False))

    def _capture_template(cluster_name, project_id, region, template_name,
                          config):
        del cluster_name, project_id, region, template_name
        template_config.update(config)
        return {'name': 'create-template'}

    monkeypatch.setattr(instance_utils.mig_utils,
                        'create_region_instance_template', _capture_template)
    monkeypatch.setattr(instance_utils.mig_utils,
                        'create_managed_instance_group',
                        MagicMock(return_value={'name': 'create-mig'}))
    monkeypatch.setattr(instance_utils.mig_utils,
                        'resize_managed_instance_group',
                        MagicMock(return_value={'name': 'resize-mig'}))
    monkeypatch.setattr(instance_utils.mig_utils,
                        'wait_for_managed_group_to_be_stable', MagicMock())
    monkeypatch.setattr(instance_utils.GCPManagedInstanceGroup,
                        'wait_for_operation', MagicMock())
    monkeypatch.setattr(instance_utils.GCPManagedInstanceGroup,
                        '_add_labels_and_find_head',
                        MagicMock(return_value=['node-1']))
    monkeypatch.setattr(instance_utils.GCPManagedInstanceGroup,
                        'create_node_tag', MagicMock(return_value='node-1'))

    errors, _ = instance_utils.GCPManagedInstanceGroup.create_instances(
        'cluster',
        'project',
        'us-central1-a', {
            'machineType': 'a3-highgpu-8g',
            'disks': [{
                'type': 'PERSISTENT',
                'initializeParams': {
                    'labels': {
                        provision_constants.TAG_SKYPILOT_MANAGED: 'false',
                    },
                },
            }],
            gcp_constants.MANAGED_INSTANCE_GROUP_CONFIG: {
                'run_duration': 3600,
            },
        },
        labels={},
        count=1,
        total_count=1,
        include_head_node=True)

    assert errors is None
    assert template_config['disks'][0]['initializeParams']['labels'] == {
        provision_constants.TAG_SKYPILOT_MANAGED:
            provision_constants.SKYPILOT_MANAGED_TAG_VALUE,
        provision_constants.TAG_RAY_CLUSTER_NAME: 'cluster',
    }


def test_tpu_timeout_cancels_every_unfinished_operation(monkeypatch):
    resource = MagicMock()
    operations_api = resource.projects().locations().operations()
    nodes_api = resource.projects().locations().nodes()

    create_requests = [MagicMock(), MagicMock()]
    create_requests[0].execute.return_value = {'name': 'operation-1'}
    create_requests[1].execute.return_value = {'name': 'operation-2'}
    nodes_api.create.side_effect = create_requests

    cancel_requests = [MagicMock(), MagicMock()]
    operations_api.cancel.side_effect = cancel_requests
    monkeypatch.setattr(instance_utils.GCPTPUVMInstance, 'load_resource',
                        MagicMock(return_value=resource))
    monkeypatch.setattr(instance_utils, 'GCP_TIMEOUT', 0)
    monkeypatch.setattr(instance_utils, '_format_and_log_message_from_errors',
                        MagicMock())

    errors, names = instance_utils.GCPTPUVMInstance._create_standard_instances(  # pylint: disable=protected-access
        ['node-1', 'node-2'], 'project', 'zone', {'labels': {}})

    assert names == ['node-1', 'node-2']
    assert errors == [{
        'code': 'TIMEOUT',
        'message': 'Timeout waiting for creation operation',
        'domain': 'create_instances'
    }]
    assert operations_api.cancel.call_count == 2
    assert cancel_requests[0].http.timeout == 1
    assert cancel_requests[1].http.timeout == 1
    cancel_requests[0].execute.assert_called_once_with(
        num_retries=instance_utils.GCP_CREATE_MAX_RETRIES)
    cancel_requests[1].execute.assert_called_once_with(
        num_retries=instance_utils.GCP_CREATE_MAX_RETRIES)


def test_gcp_config_gateway_callables_keep_pickle_identity():
    gateway_names = (
        'wait_for_crm_operation',
        'wait_for_compute_global_operation',
        'wait_for_compute_region_operation',
        '_create_crm',
        '_create_iam',
        '_create_compute',
        '_create_tpu',
        '_delete_firewall_rule',
        '_list_firewall_rules',
        '_create_vpcnet',
        '_list_vpcnets',
        '_delete_vpcnet',
        '_list_subnets',
        '_network_interface_to_vpc_name',
        '_get_project',
        '_create_project',
        '_get_service_account',
        '_create_service_account',
        '_add_iam_policy_binding',
        '_create_subnet',
        '_delete_subnet',
        '_create_placement_policy',
        '_get_placement_policy',
    )

    for gateway_name in gateway_names:
        gateway_callable = getattr(gcp_config, gateway_name)
        assert gateway_callable.__module__ == gcp_config.__name__
        assert getattr(gcp_api, gateway_name) is gateway_callable
        assert pickle.loads(pickle.dumps(gateway_callable)) is gateway_callable


def test_gcp_get_usable_vpc_and_subnet_uses_specified_subnet(monkeypatch):
    provider_config = {
        'project_id': 'test-project',
        'vpc_name': 'train-vpc',
        'subnet_names': ['train-subnet-b', 'train-subnet-a'],
    }
    provision_config = _make_provision_config(provider_config)
    monkeypatch.setattr(gcp_config, '_list_vpcnets', lambda *args, **kwargs: [{
        'name': 'train-vpc'
    }])
    monkeypatch.setattr(
        gcp_config, '_list_subnets', lambda *args, **kwargs: [
            _make_subnet('train-subnet-a', 'train-vpc'),
            _make_subnet('train-subnet-b', 'train-vpc'),
        ])

    vpc_name, subnet = gcp_config.get_usable_vpc_and_subnet(
        'cluster', 'us-central1', provision_config, MagicMock())

    assert vpc_name == 'train-vpc'
    assert subnet['name'] == 'train-subnet-b'


def test_gcp_get_usable_vpc_and_subnet_infers_vpc_from_subnet(monkeypatch):
    provider_config = {
        'project_id': 'test-project',
        'subnet_names': 'train-subnet',
    }
    provision_config = _make_provision_config(provider_config)
    monkeypatch.setattr(
        gcp_config, '_list_subnets', lambda *args, **kwargs: [
            _make_subnet('train-subnet', 'train-vpc'),
        ])

    vpc_name, subnet = gcp_config.get_usable_vpc_and_subnet(
        'cluster', 'us-central1', provision_config, MagicMock())

    assert vpc_name == 'train-vpc'
    assert subnet['name'] == 'train-subnet'


def test_gcp_get_usable_vpc_and_subnet_rejects_multiple_vpcs(monkeypatch):
    provider_config = {
        'project_id': 'test-project',
        'subnet_names': ['train-subnet-a', 'train-subnet-b'],
    }
    provision_config = _make_provision_config(provider_config)
    monkeypatch.setattr(
        gcp_config, '_list_subnets', lambda *args, **kwargs: [
            _make_subnet('train-subnet-a', 'train-vpc-a'),
            _make_subnet('train-subnet-b', 'train-vpc-b'),
        ])

    with pytest.raises(RuntimeError) as exc_info:
        gcp_config.get_usable_vpc_and_subnet('cluster', 'us-central1',
                                             provision_config, MagicMock())

    assert 'multiple VPCs' in str(exc_info.value)


def test_gcp_get_usable_vpc_and_subnet_partial_name_match(monkeypatch):
    provider_config = {
        'project_id': 'test-project',
        'vpc_name': 'train-vpc',
        'subnet_names': ['missing-subnet', 'train-subnet-b'],
    }
    provision_config = _make_provision_config(provider_config)
    monkeypatch.setattr(gcp_config, '_list_vpcnets', lambda *args, **kwargs: [{
        'name': 'train-vpc'
    }])
    monkeypatch.setattr(
        gcp_config, '_list_subnets', lambda *args, **kwargs: [
            _make_subnet('train-subnet-a', 'train-vpc'),
            _make_subnet('train-subnet-b', 'train-vpc'),
        ])

    vpc_name, subnet = gcp_config.get_usable_vpc_and_subnet(
        'cluster', 'us-central1', provision_config, MagicMock())

    assert vpc_name == 'train-vpc'
    assert subnet['name'] == 'train-subnet-b'


def test_gcp_get_usable_vpc_and_subnet_empty_subnet_names(monkeypatch):
    provider_config = {
        'project_id': 'test-project',
        'vpc_name': 'train-vpc',
        'subnet_names': [],
    }
    provision_config = _make_provision_config(provider_config)
    monkeypatch.setattr(gcp_config, '_list_vpcnets', lambda *args, **kwargs: [{
        'name': 'train-vpc'
    }])
    monkeypatch.setattr(
        gcp_config, '_list_subnets', lambda *args, **kwargs: [
            _make_subnet('train-subnet-a', 'train-vpc'),
            _make_subnet('train-subnet-b', 'train-vpc'),
        ])

    vpc_name, subnet = gcp_config.get_usable_vpc_and_subnet(
        'cluster', 'us-central1', provision_config, MagicMock())

    assert vpc_name == 'train-vpc'
    assert subnet['name'] == 'train-subnet-a'


def test_gcp_get_usable_vpc_and_subnet_shared_vpc_with_subnet_names(
        monkeypatch):
    provider_config = {
        'project_id': 'service-project',
        'vpc_name': 'host-project/train-vpc',
        'subnet_names': ['train-subnet-b'],
    }
    provision_config = _make_provision_config(provider_config)
    seen_projects = []

    def list_vpcnets(project_id, *args, **kwargs):
        seen_projects.append(project_id)
        return [{'name': 'train-vpc'}]

    def list_subnets(project_id, *args, **kwargs):
        seen_projects.append(project_id)
        return [
            _make_subnet('train-subnet-a',
                         'train-vpc',
                         project_id='host-project'),
            _make_subnet('train-subnet-b',
                         'train-vpc',
                         project_id='host-project'),
        ]

    monkeypatch.setattr(gcp_config, '_list_vpcnets', list_vpcnets)
    monkeypatch.setattr(gcp_config, '_list_subnets', list_subnets)

    vpc_name, subnet = gcp_config.get_usable_vpc_and_subnet(
        'cluster', 'us-central1', provision_config, MagicMock())

    assert seen_projects == ['host-project', 'host-project']
    assert vpc_name == 'train-vpc'
    assert subnet['name'] == 'train-subnet-b'
    assert subnet['network'] == (
        'projects/host-project/global/networks/train-vpc')


def test_gcp_minimal_compute_permissions_skip_firewall_for_custom_subnet():

    def get_effective_region_config_side_effect(cloud,
                                                region,
                                                keys,
                                                default_value=None,
                                                **kwargs):
        del cloud, region, kwargs
        if keys == ('subnet_names',):
            return ['train-subnet']
        return default_value

    with patch.object(skypilot_config,
                      'get_effective_region_config',
                      side_effect=get_effective_region_config_side_effect):
        permissions = gcp_utils.get_minimal_compute_permissions()

    for permission in gcp_constants.FIREWALL_PERMISSIONS:
        assert permission not in permissions


def test_gcp_minimal_compute_permissions_include_disk_labeling():
    assert 'compute.disks.setLabels' in (
        gcp_utils.get_minimal_compute_permissions())


def test_gcp_minimal_compute_permissions_include_firewall_for_empty_subnets():

    def get_effective_region_config_side_effect(cloud,
                                                region,
                                                keys,
                                                default_value=None,
                                                **kwargs):
        del cloud, region, kwargs
        if keys == ('subnet_names',):
            return []
        return default_value

    with patch.object(skypilot_config,
                      'get_effective_region_config',
                      side_effect=get_effective_region_config_side_effect):
        permissions = gcp_utils.get_minimal_compute_permissions()

    for permission in gcp_constants.FIREWALL_PERMISSIONS:
        assert permission in permissions


def test_gcp_network_config_override_in_cluster_config(monkeypatch):
    """Test that GCP network overrides are passed through to the template."""
    monkeypatch.setattr(common_utils, 'make_cluster_name_on_cloud',
                        lambda *args, **kwargs: args[0])
    monkeypatch.setattr(backend_utils, '_get_yaml_path_from_cluster_name',
                        lambda *args, **kwargs: '/tmp/fake-gcp-yaml-path')
    monkeypatch.setattr(resources.Resources, 'make_deploy_variables',
                        lambda *args, **kwargs: {'region': 'us-central1'})
    monkeypatch.setattr(logs, 'get_logging_agent', lambda *args, **kwargs: None)
    monkeypatch.setattr(
        backend_utils.auth_utils, 'get_or_generate_keys',
        lambda *args, **kwargs:
        ('/tmp/fake-private-key', '/tmp/fake-public-key'))

    config_dict = config_utils.Config.from_dict({})
    monkeypatch.setattr(skypilot_config, '_get_loaded_config',
                        lambda *args, **kwargs: config_dict)

    override_configs = {
        'gcp': {
            'vpc_name': 'override-vpc',
            'subnet_names': ['override-subnet'],
        },
    }

    def fill_template_side_effect(*args, **kwargs):
        del kwargs
        template_vars = args[1]
        assert template_vars['vpc_name'] == 'override-vpc'
        assert template_vars['subnet_names'] == ['override-subnet']
        raise RuntimeError('fake-error')

    monkeypatch.setattr(common_utils, 'fill_template',
                        fill_template_side_effect)

    with pytest.raises(RuntimeError):
        backend_utils.write_cluster_config(
            to_provision=resources.Resources(
                cloud=GCP(),
                instance_type='n1-standard-4',
                _cluster_config_overrides=override_configs),
            num_nodes=1,
            cluster_config_template='gcp-ray.yml.j2',
            cluster_name='fake-gcp-cluster',
            local_wheel_path=pathlib.Path('fake-wheel-path'),
            wheel_hash='fake-wheel-hash',
            region=Region(name='us-central1'),
            zones=[Zone(name='us-central1-a')])


# --- Tests for _is_reservation_bound ---


class TestIsReservationBound:
    """Tests for DENSE/CALENDAR reservation detection."""

    @patch('sky.provision.gcp.instance_utils'
           '.GCPComputeInstance.load_resource')
    def test_dense_reservation_returns_true(self, mock_load):
        """DENSE reservation should trigger RESERVATION_BOUND."""
        from sky.provision.gcp.instance_utils import _is_reservation_bound
        mock_compute = MagicMock()
        mock_load.return_value = mock_compute
        mock_compute.reservations.return_value.list.return_value \
            .execute.return_value = {
                'items': [{
                    'name': 'my-dense-reservation',
                    'deploymentType': 'DENSE',
                }]
            }

        result = _is_reservation_bound('my-project', 'us-central1-a',
                                       'my-dense-reservation')
        assert result is True

        mock_compute.reservations.return_value.list.assert_called_once_with(
            project='my-project',
            zone='us-central1-a',
            filter='name=my-dense-reservation',
        )

    @patch('sky.provision.gcp.instance_utils'
           '.GCPComputeInstance.load_resource')
    def test_standard_reservation_returns_false(self, mock_load):
        """Standard (SPECIFIC) reservation should not trigger override."""
        from sky.provision.gcp.instance_utils import _is_reservation_bound
        mock_compute = MagicMock()
        mock_load.return_value = mock_compute
        mock_compute.reservations.return_value.list.return_value \
            .execute.return_value = {
                'items': [{
                    'name': 'my-standard-reservation',
                    'deploymentType': 'DEPLOYMENT_TYPE_UNSPECIFIED',
                }]
            }

        result = _is_reservation_bound('my-project', 'us-central1-a',
                                       'my-standard-reservation')
        assert result is False

    @patch('sky.provision.gcp.instance_utils'
           '.GCPComputeInstance.load_resource')
    def test_no_deployment_type_returns_false(self, mock_load):
        """Reservation without deploymentType field should not override."""
        from sky.provision.gcp.instance_utils import _is_reservation_bound
        mock_compute = MagicMock()
        mock_load.return_value = mock_compute
        mock_compute.reservations.return_value.list.return_value \
            .execute.return_value = {
                'items': [{
                    'name': 'my-reservation',
                }]
            }

        result = _is_reservation_bound('my-project', 'us-central1-a',
                                       'my-reservation')
        assert result is False

    @patch('sky.provision.gcp.instance_utils'
           '.GCPComputeInstance.load_resource')
    def test_reservation_not_found_returns_false(self, mock_load):
        """Empty list result should return False."""
        from sky.provision.gcp.instance_utils import _is_reservation_bound
        mock_compute = MagicMock()
        mock_load.return_value = mock_compute
        mock_compute.reservations.return_value.list.return_value \
            .execute.return_value = {
                'items': []
            }

        result = _is_reservation_bound('my-project', 'us-central1-a',
                                       'nonexistent-reservation')
        assert result is False

    @patch('sky.provision.gcp.instance_utils'
           '.GCPComputeInstance.load_resource')
    def test_api_failure_returns_false(self, mock_load):
        """API errors should gracefully fall back to False."""
        from sky.provision.gcp.instance_utils import _is_reservation_bound
        mock_compute = MagicMock()
        mock_load.return_value = mock_compute
        mock_compute.reservations.return_value.list.return_value \
            .execute.side_effect = Exception('Permission denied')

        result = _is_reservation_bound('my-project', 'us-central1-a',
                                       'my-reservation')
        assert result is False

    @patch('sky.provision.gcp.instance_utils'
           '.GCPComputeInstance.load_resource')
    def test_full_uri_parses_short_name(self, mock_load):
        """Full reservation URI should be parsed to short name for API call."""
        from sky.provision.gcp.instance_utils import _is_reservation_bound
        mock_compute = MagicMock()
        mock_load.return_value = mock_compute
        mock_compute.reservations.return_value.list.return_value \
            .execute.return_value = {
                'items': [{
                    'name': 'my-dense-res',
                    'deploymentType': 'DENSE',
                }]
            }

        full_uri = ('projects/my-project/zones/us-central1-a'
                    '/reservations/my-dense-res')
        result = _is_reservation_bound('my-project', 'us-central1-a', full_uri)
        assert result is True

        mock_compute.reservations.return_value.list.assert_called_once_with(
            project='my-project',
            zone='us-central1-a',
            filter='name=my-dense-res',
        )


class TestTPUNodeGateway:
    """Characterizes the legacy gcloud TPU node lifecycle gateway."""

    def test_instance_utils_facade_identity(self):
        assert instance_utils.delete_tpu_node is tpu_node.delete_tpu_node
        assert instance_utils.create_tpu_node.__module__ == (
            'sky.provision.gcp.instance_utils')
        assert instance_utils.delete_tpu_node.__module__ == (
            'sky.provision.gcp.instance_utils')
        assert pickle.loads(pickle.dumps(
            instance_utils.create_tpu_node)) is (instance_utils.create_tpu_node)
        assert pickle.loads(pickle.dumps(
            instance_utils.delete_tpu_node)) is (instance_utils.delete_tpu_node)
        create_signature = inspect.signature(instance_utils.create_tpu_node)
        assert list(create_signature.parameters) == [
            'project_id', 'zone', 'tpu_node_config', 'vpc_name'
        ]
        assert create_signature.return_annotation is inspect.Signature.empty
        delete_signature = inspect.signature(instance_utils.delete_tpu_node)
        assert list(delete_signature.parameters) == [
            'project_id', 'zone', 'tpu_node_config'
        ]
        assert delete_signature.return_annotation is inspect.Signature.empty

    @patch('subprocess.run')
    def test_create_tpu_node_command(self, mock_run):
        mock_run.return_value.stdout = b'created\n'

        instance_utils.create_tpu_node(
            'project-1', 'us-central1-b', {
                'name': 'tpu-1',
                'acceleratorType': 'v3-8',
                'runtimeVersion': 'tpu-vm-base',
            }, 'default')

        mock_run.assert_called_once_with(
            'yes | gcloud compute tpus create tpu-1 '
            '--project=project-1 --zone=us-central1-b '
            '--version=tpu-vm-base --accelerator-type=v3-8 '
            '--labels=skypilot-managed=true '
            '--network=default',
            capture_output=True,
            shell=True,
            check=True,
        )

    @patch('subprocess.run')
    def test_create_tpu_node_maps_quota_error(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd='gcloud compute tpus create',
            stderr=b'RESOURCE_EXHAUSTED',
        )

        with pytest.raises(common.ProvisionerError) as exc_info:
            instance_utils.create_tpu_node(
                'project-1', 'us-central1-b', {
                    'name': 'tpu-1',
                    'acceleratorType': 'v3-8',
                    'runtimeVersion': 'tpu-vm-base',
                }, 'default')

        assert exc_info.value.errors == [{
            'code': 'RESOURCE_EXHAUSTED',
            'domain': 'tpu',
            'message': 'TPU tpu-1 creation failed due to quota exhaustion. '
                       'Please visit '
                       'https://console.cloud.google.com/iam-admin/quotas '
                       'for more information.'
        }]

    @patch('subprocess.run')
    def test_delete_tpu_node_command(self, mock_run):
        mock_run.return_value.stdout = b'deleted\n'

        instance_utils.delete_tpu_node('project-1', 'us-central1-b',
                                       {'name': 'tpu-1'})

        mock_run.assert_called_once_with(
            'yes | gcloud compute tpus delete tpu-1 '
            '--project=project-1 --zone=us-central1-b',
            capture_output=True,
            shell=True,
            check=True,
        )

    @patch('subprocess.run')
    def test_delete_tpu_node_ignores_not_found(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd='gcloud compute tpus delete',
            output=b'',
            stderr=b'ERROR: (gcloud.compute.tpus.delete) NOT_FOUND',
        )

        instance_utils.delete_tpu_node('project-1', 'us-central1-b',
                                       {'name': 'tpu-1'})
