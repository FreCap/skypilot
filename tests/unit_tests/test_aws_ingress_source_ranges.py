"""Logic tests for `aws.ingress_source_ranges`.

SkyPilot opens every port in `resources.ports` to the whole internet. For a
workload with no authentication of its own -- a SkyServe replica, whose bearer
token the load balancer strips before proxying -- that means anyone can reach
it. These tests pin the configurable source ranges AND, just as importantly,
that the default is unchanged.
"""
# pylint: disable=protected-access
import importlib
import os
import tempfile
from unittest import mock

import jinja2
import yaml

import sky
from sky import clouds
from sky import skypilot_config
from sky.provision.aws import config as aws_config
from sky.provision.aws import instance as aws_instance
from sky.resources import Resources
from sky.utils import resources_utils


def test_default_is_the_whole_internet():
    """The historical behaviour must be byte-identical when unconfigured."""
    assert aws_instance._ingress_source_ranges(None) == ['0.0.0.0/0']
    assert aws_instance._ingress_source_ranges({}) == ['0.0.0.0/0']
    assert (aws_instance._ingress_source_ranges({'ingress_source_ranges': None
                                                }) == ['0.0.0.0/0'])
    assert (aws_instance._ingress_source_ranges({'ingress_source_ranges': []
                                                }) == ['0.0.0.0/0'])


def test_configured_ranges_are_used_verbatim():
    assert aws_instance._ingress_source_ranges(
        {'ingress_source_ranges': ['52.54.64.159/32']}) == ['52.54.64.159/32']
    assert aws_instance._ingress_source_ranges({
        'ingress_source_ranges': ['10.0.0.0/8', '52.54.64.159/32']
    }) == ['10.0.0.0/8', '52.54.64.159/32']


def test_both_modules_agree_on_the_default():
    """A split default would silently reopen SSH while ports stay narrowed."""
    assert (aws_instance._DEFAULT_INGRESS_SOURCE_RANGE ==
            aws_config._DEFAULT_INGRESS_SOURCE_RANGE == '0.0.0.0/0')


def test_schema_accepts_and_rejects_appropriately():
    import jsonschema  # pylint: disable=import-outside-toplevel

    from sky.utils import schemas  # pylint: disable=import-outside-toplevel
    schema = schemas.get_config_schema()

    jsonschema.validate({'aws': {
        'ingress_source_ranges': ['52.54.64.159/32']
    }}, schema)
    # An empty list would silently mean "no rules", which is a footgun; require
    # at least one entry so the intent is explicit.
    for bad in ([], '52.54.64.159/32', [''], [None]):
        try:
            jsonschema.validate({'aws': {'ingress_source_ranges': bad}}, schema)
            raise AssertionError(f'schema accepted {bad!r}')
        except jsonschema.ValidationError:
            pass


def _render_provider_config(deploy_vars: dict) -> dict:
    """Render the AWS ray template's ``provider:`` block from deploy vars.

    ``sky.provision.aws`` reads ``ingress_source_ranges`` off the provider
    config, which is the rendered ``provider:`` block of the cluster YAML.
    ``make_deploy_variables`` places the value into the Jinja *context*, but the
    value only reaches the provider config if the template actually references
    it -- so this renders exactly that block, the hop the knob depends on.
    """
    template_path = os.path.join(os.path.dirname(sky.__file__), 'templates',
                                 'aws-ray.yml.j2')
    with open(template_path, encoding='utf-8') as fin:
        lines = fin.read().splitlines()
    start = next(
        i for i, line in enumerate(lines) if line.strip() == 'provider:')
    end = next(i for i, line in enumerate(lines) if line.strip() == 'auth:')
    block = '\n'.join(lines[start:end])
    # Provider-block vars that write_cluster_config adds outside of
    # make_deploy_variables(); default them the way an unset config would.
    context = {
        'vpc_name': None,
        'subnet_names': None,
        'use_internal_ips': False,
        **deploy_vars,
    }
    rendered = jinja2.Template(block).render(**context)
    return yaml.safe_load(rendered)['provider']


def _make_deploy_vars_with_config(config_body: str) -> dict:
    """Run AWS make_deploy_variables under a given ~/.sky/config.yaml body."""
    with tempfile.NamedTemporaryFile('w', suffix='.yaml',
                                     delete=False) as config_file:
        config_file.write(config_body)
        config_path = config_file.name
    try:
        os.environ[skypilot_config.ENV_VAR_SKYPILOT_CONFIG] = config_path
        importlib.reload(skypilot_config)
        cluster_name = resources_utils.ClusterName(display_name='display',
                                                   name_on_cloud='cloud')
        resource = Resources(cloud=clouds.AWS(), instance_type='fake-type: 3')
        return resource.make_deploy_variables(cluster_name,
                                              clouds.Region(name='fake-region'),
                                              [clouds.Zone(name='fake-zone')],
                                              num_nodes=1,
                                              dryrun=True)
    finally:
        os.environ.pop(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, None)
        os.unlink(config_path)
        importlib.reload(skypilot_config)


@mock.patch('sky.catalog.instance_type_exists', return_value=True)
@mock.patch('sky.catalog.get_accelerators_from_instance_type',
            return_value={'fake-acc': 2})
@mock.patch('sky.clouds.aws.AWS.get_image_root_device_name',
            return_value='/dev/sda1')
@mock.patch('sky.catalog.get_image_id_from_tag', return_value='fake-image')
@mock.patch('sky.catalog.get_arch_from_instance_type', return_value='fake-arch')
@mock.patch.object(clouds.aws, 'DEFAULT_SECURITY_GROUP_NAME', 'fake-default-sg')
def test_configured_ranges_reach_the_provider_config(*_mocks):
    """End-to-end: a configured CIDR must survive the full config -> deploy
    vars -> template -> provider_config -> consumer chain.

    This is the hop the knob actually depends on. make_deploy_variables placing
    the value in the Jinja context is necessary but not sufficient: the template
    must emit it into the ``provider:`` block or every consumer reads ``None``
    and silently falls back to 0.0.0.0/0. Fails on the pre-fix template.
    """
    deploy_vars = _make_deploy_vars_with_config(
        'aws:\n  ingress_source_ranges:\n    - 203.0.113.7/32\n')
    # config -> deploy vars
    assert deploy_vars['ingress_source_ranges'] == ['203.0.113.7/32']
    # deploy vars -> provider_config (the hop that was broken)
    provider_config = _render_provider_config(deploy_vars)
    assert provider_config.get('ingress_source_ranges') == ['203.0.113.7/32']
    # provider_config -> consumer that builds the SG rules
    assert aws_instance._ingress_source_ranges(provider_config) == [
        '203.0.113.7/32'
    ]


@mock.patch('sky.catalog.instance_type_exists', return_value=True)
@mock.patch('sky.catalog.get_accelerators_from_instance_type',
            return_value={'fake-acc': 2})
@mock.patch('sky.clouds.aws.AWS.get_image_root_device_name',
            return_value='/dev/sda1')
@mock.patch('sky.catalog.get_image_id_from_tag', return_value='fake-image')
@mock.patch('sky.catalog.get_arch_from_instance_type', return_value='fake-arch')
@mock.patch.object(clouds.aws, 'DEFAULT_SECURITY_GROUP_NAME', 'fake-default-sg')
def test_unconfigured_provider_config_stays_open(*_mocks):
    """Unconfigured, the provider config must carry the historical default so
    behaviour is byte-identical for anyone who does not set the knob."""
    deploy_vars = _make_deploy_vars_with_config('aws: {}\n')
    assert deploy_vars['ingress_source_ranges'] == ['0.0.0.0/0']
    provider_config = _render_provider_config(deploy_vars)
    assert provider_config.get('ingress_source_ranges') == ['0.0.0.0/0']
    assert aws_instance._ingress_source_ranges(provider_config) == ['0.0.0.0/0']
