"""Characterization tests for DigitalOcean deploy variables."""

import copy
import unittest.mock as mock

import pytest

from sky import clouds
from sky import resources as resources_lib
from sky.clouds import do as do_cloud
from sky.utils import resources_utils

_INSTANCE_TYPE = 's-2vcpu-4gb'
_REGION = clouds.Region(name='nyc3')
_CLUSTER_NAME = resources_utils.ClusterName(display_name='display',
                                            name_on_cloud='display-on-cloud')


def _make_resources(cloud_image_ids: dict[str | None, str] | None) -> mock.Mock:
    resources = mock.Mock(spec=resources_lib.Resources)
    resources.instance_type = _INSTANCE_TYPE
    resources.assert_launchable.return_value = resources
    resources.get_cloud_image_id.return_value = cloud_image_ids
    return resources


def _make_deploy_variables(
    resources: mock.Mock,
    *,
    cluster_name: resources_utils.ClusterName = _CLUSTER_NAME,
    zones: list[clouds.Zone] | None = None,
    num_nodes: int = 1,
    dryrun: bool = False,
    volume_mounts: list[object] | None = None,
) -> dict[str, str | None]:
    return do_cloud.DO().make_deploy_resources_variables(
        resources=resources,
        cluster_name=cluster_name,
        region=_REGION,
        zones=zones,
        num_nodes=num_nodes,
        dryrun=dryrun,
        volume_mounts=volume_mounts)


@pytest.mark.parametrize(('accelerators', 'cloud_image_ids', 'expected'), [
    pytest.param(None,
                 None, {
                     'instance_type': _INSTANCE_TYPE,
                     'custom_resources': None,
                     'region': 'nyc3',
                 },
                 id='no-accelerator-absent-image'),
    pytest.param({
        'H100': 1,
        'L4': 2,
    },
                 None, {
                     'instance_type': _INSTANCE_TYPE,
                     'custom_resources': '{"H100":1,"L4":2}',
                     'region': 'nyc3',
                 },
                 id='accelerator-compact-json'),
    pytest.param(None, {
        None: 'global-image',
    }, {
        'instance_type': _INSTANCE_TYPE,
        'custom_resources': None,
        'region': 'nyc3',
        'image_id': 'global-image',
    },
                 id='global-image'),
    pytest.param(None, {
        'nyc3': 'regional-image',
        'sfo3': 'other-image',
    }, {
        'instance_type': _INSTANCE_TYPE,
        'custom_resources': None,
        'region': 'nyc3',
        'image_id': 'regional-image',
    },
                 id='regional-image'),
])
def test_make_deploy_resources_variables_exact_mapping(accelerators,
                                                       cloud_image_ids,
                                                       expected):
    """The legacy callback projects the exact scalar DigitalOcean mapping."""
    resources = _make_resources(cloud_image_ids)
    accelerator_before = copy.deepcopy(accelerators)
    image_ids_before = copy.deepcopy(cloud_image_ids)

    with mock.patch.object(do_cloud.DO,
                           'get_accelerators_from_instance_type',
                           return_value=accelerators) as get_accelerators:
        result = _make_deploy_variables(resources)

    assert result == expected
    get_accelerators.assert_called_once_with(_INSTANCE_TYPE)
    resources.assert_launchable.assert_called_once_with()
    resources.get_cloud_image_id.assert_called_once_with()
    assert accelerators == accelerator_before
    assert cloud_image_ids == image_ids_before
    assert resources.instance_type == _INSTANCE_TYPE


def test_make_deploy_resources_variables_returns_fresh_mapping_without_mutation(
):
    """Repeated calls return independent mappings and leave inputs untouched."""
    accelerators = {'H100': 1}
    cloud_image_ids = {
        'nyc3': 'regional-image',
        'sfo3': 'other-image',
    }
    resources = _make_resources(cloud_image_ids)
    accelerator_before = copy.deepcopy(accelerators)
    image_ids_before = copy.deepcopy(cloud_image_ids)

    with mock.patch.object(do_cloud.DO,
                           'get_accelerators_from_instance_type',
                           return_value=accelerators):
        first = _make_deploy_variables(resources)
        second = _make_deploy_variables(resources)

    assert first == second
    assert first is not second
    first['region'] = 'mutated'
    first['custom_resources'] = None
    assert second == {
        'instance_type': _INSTANCE_TYPE,
        'custom_resources': '{"H100":1}',
        'region': 'nyc3',
        'image_id': 'regional-image',
    }
    assert accelerators == accelerator_before
    assert cloud_image_ids == image_ids_before
    assert resources.instance_type == _INSTANCE_TYPE


def test_make_deploy_resources_variables_ignores_non_projected_arguments():
    """Cluster, zone, count, dry-run, and mount inputs do not affect output."""
    resources = _make_resources({'nyc3': 'regional-image'})

    with mock.patch.object(do_cloud.DO,
                           'get_accelerators_from_instance_type',
                           return_value={'H100': 1}):
        baseline = _make_deploy_variables(resources)
        variant = _make_deploy_variables(
            resources,
            cluster_name=resources_utils.ClusterName(
                display_name='other-display', name_on_cloud='other-on-cloud'),
            zones=[clouds.Zone(name='unused-zone')],
            num_nodes=99,
            dryrun=True,
            volume_mounts=[object()])

    assert variant == baseline
