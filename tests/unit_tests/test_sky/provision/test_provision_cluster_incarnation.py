"""Compatibility tests for internal cluster-incarnation propagation."""
# pylint: disable=protected-access

import contextlib
import dataclasses
import importlib
import inspect
import pickle

import pytest

from sky import clouds
from sky import global_user_state
from sky import provision
from sky.provision import common
from sky.provision import provisioner
from sky.utils import resources_utils


@dataclasses.dataclass
class _RequiredProvisionConfigExtension(common.ProvisionConfig):
    required_extension: str


def _old_positional_values():
    return (
        {
            'provider': 'value'
        },
        {
            'auth': 'value'
        },
        {
            'docker': 'value'
        },
        {
            'node': 'value'
        },
        2,
        {
            'tag': 'value'
        },
        True,
        [8080],
    )


def _provision_record() -> common.ProvisionRecord:
    return common.ProvisionRecord(
        provider_name='kubernetes',
        region='test-region',
        zone=None,
        cluster_name='cloud-name',
        head_instance_id='head',
        resumed_instance_ids=[],
        created_instance_ids=['head'],
    )


def test_provision_config_retains_old_constructor_and_subclass_contract():
    old_names = [
        'provider_config',
        'authentication_config',
        'docker_config',
        'node_config',
        'count',
        'tags',
        'resume_stopped_nodes',
        'ports_to_open_on_launch',
    ]
    parameters = list(
        inspect.signature(common.ProvisionConfig).parameters.values())

    assert [parameter.name for parameter in parameters
           ] == [*old_names, 'cluster_incarnation',
                 'provider_effect_guard_factory']
    assert all(parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
               for parameter in parameters[:-2])
    assert all(parameter.kind is inspect.Parameter.KEYWORD_ONLY
               for parameter in parameters[-2:])
    assert all(parameter.default is None for parameter in parameters[-2:])

    config = common.ProvisionConfig(*_old_positional_values())
    assert [getattr(config, name) for name in old_names
           ] == list(_old_positional_values())
    assert config.cluster_incarnation is None
    assert config.provider_effect_guard_factory is None

    extension = _RequiredProvisionConfigExtension(*_old_positional_values(),
                                                  required_extension='required')
    assert extension.required_extension == 'required'
    assert extension.cluster_incarnation is None
    assert extension.provider_effect_guard_factory is None
    assert [field.name for field in dataclasses.fields(config)
           ] == [*old_names, 'cluster_incarnation',
                 'provider_effect_guard_factory']


def test_provision_config_equality_repr_pickle_and_redaction_contract():
    legacy = common.ProvisionConfig(*_old_positional_values())
    marked = dataclasses.replace(legacy, cluster_incarnation='raw-generation')
    other = dataclasses.replace(legacy, cluster_incarnation='other-generation')

    assert marked != legacy
    assert marked != other
    assert 'cluster_incarnation' not in repr(marked)
    assert 'raw-generation' not in repr(marked)
    expected_redacted = {
        'provider_config': {
            'provider': 'value'
        },
        'authentication_config': {
            'auth': 'value'
        },
        'docker_config': {
            'docker': 'value'
        },
        'node_config': {
            'node': 'value'
        },
        'count': 2,
        'tags': {
            'tag': 'value'
        },
        'resume_stopped_nodes': True,
        'ports_to_open_on_launch': [8080],
    }
    assert marked.get_redacted_config() == expected_redacted

    legacy.__dict__.pop('cluster_incarnation')
    legacy.__dict__.pop('provider_effect_guard_factory')
    assert 'cluster_incarnation' not in legacy.__dict__
    assert 'provider_effect_guard_factory' not in legacy.__dict__
    restored = pickle.loads(pickle.dumps(legacy))
    assert restored.cluster_incarnation is None
    assert restored.provider_effect_guard_factory is None
    assert dataclasses.asdict(restored)['cluster_incarnation'] is None
    assert dataclasses.asdict(restored)['provider_effect_guard_factory'] is None
    assert restored.get_redacted_config() == expected_redacted


@pytest.mark.parametrize('marker', [None, 'raw-generation'])
def test_bulk_provision_propagates_exact_optional_incarnation(
        monkeypatch, tmp_path, marker):
    original_config = {
        'head_node_type': 'ray.head.default',
        'provider': {},
        'auth': {},
        'docker': {},
        'available_node_types': {
            'ray.head.default': {
                'node_config': {}
            }
        },
    }
    monkeypatch.setattr(global_user_state, 'get_cluster_yaml_dict',
                        lambda *_: original_config)
    monkeypatch.setattr(provisioner.provision_logging,
                        'setup_provision_logging',
                        lambda *_: contextlib.nullcontext())
    monkeypatch.setattr(provisioner.provision_volume,
                        'provision_ephemeral_volumes', lambda *_: None)
    monkeypatch.setattr(global_user_state, 'add_cluster_event', lambda *_: None)

    calls = []
    guard_depth = 0

    @contextlib.contextmanager
    def provider_effect_guard():
        nonlocal guard_depth
        guard_depth += 1
        try:
            yield
        finally:
            guard_depth -= 1

    def bootstrap_instances(provider_name, region, cluster_name_on_cloud,
                            config):
        # Auxiliary setup is not accelerator-bearing; the in-tree Kubernetes
        # provider enters this factory only at its exact Pod mutation seams.
        assert guard_depth == 0
        calls.append(
            ('bootstrap', provider_name, region, cluster_name_on_cloud, config))
        return config

    record = _provision_record()

    def run_instances(provider_name, region, cluster_name,
                      cluster_name_on_cloud, config):
        # The Kubernetes implementation re-enters the injected guard only at
        # its mutation seams; the opaque run call and passive waits are free.
        assert guard_depth == 0
        calls.append(('run', provider_name, region, cluster_name,
                      cluster_name_on_cloud, config))
        return record

    monkeypatch.setattr(provision, 'bootstrap_instances', bootstrap_instances)
    monkeypatch.setattr(provision, 'run_instances', run_instances)

    kwargs = {'provider_effect_guard_factory': provider_effect_guard}
    if marker is not None:
        kwargs['cluster_incarnation'] = marker
    result = provisioner.bulk_provision(
        cloud=clouds.Kubernetes(),
        region=clouds.Region('test-region'),
        zones=None,
        cluster_name=resources_utils.ClusterName('display-name', 'cloud-name'),
        num_nodes=1,
        cluster_yaml='/unused/cluster.yaml',
        prev_cluster_ever_up=False,
        log_dir=str(tmp_path),
        **kwargs,
    )

    assert result is record
    assert [call[0] for call in calls] == ['bootstrap', 'run']
    bootstrap_config = calls[0][-1]
    assert calls[1][-1] is bootstrap_config
    assert bootstrap_config.cluster_incarnation is marker
    assert (bootstrap_config.provider_effect_guard_factory is
            provider_effect_guard)


def test_builtin_bulk_identity_refreshes_with_module_reload():
    old_function = provisioner.bulk_provision
    assert provisioner._BUILTIN_BULK_PROVISION is old_function

    reloaded = importlib.reload(provisioner)

    assert reloaded is provisioner
    assert provisioner.bulk_provision is provisioner._BUILTIN_BULK_PROVISION
    assert provisioner.bulk_provision is not old_function
    assert inspect.isfunction(provisioner._BUILTIN_BULK_PROVISION)
