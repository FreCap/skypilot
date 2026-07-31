"""Characterization tests for Kubernetes context selection policy."""

import copy
from unittest import mock

import pytest

from sky import skypilot_config
from sky.clouds import kubernetes
from sky.clouds import kubernetes_context_policy
from sky.utils import config_utils

_AVAILABLE_CONTEXTS = ('ctx-global', 'ctx-workspace', 'ctx-resource',
                       'ctx-current', 'ctx-a', 'ctx-b', 'in-cluster',
                       'ssh-node-pool')


@pytest.mark.parametrize(
    ('workspace', 'region', 'override_configs', 'expected'), [
        ('default', None, None, 'global'),
        ('default', 'ctx-a', None, 'global-context'),
        ('team', None, None, 'workspace'),
        ('team', 'ctx-a', None, 'workspace-context'),
        ('team', 'ctx-b', None, 'workspace'),
        ('missing', 'ctx-a', None, 'global-context'),
        ('team', 'ctx-a', {
            'kubernetes': {
                'context_configs': {
                    'ctx-a': {
                        'policy_key': 'resource'
                    }
                }
            }
        }, 'resource'),
    ])
def test_effective_workspace_region_config_from_snapshot_precedence(
        workspace, region, override_configs, expected):
    snapshot = {
        'kubernetes': {
            'policy_key': 'global',
            'context_configs': {
                'ctx-a': {
                    'policy_key': 'global-context'
                }
            },
        },
        'workspaces': {
            'team': {
                'kubernetes': {
                    'policy_key': 'workspace',
                    'context_configs': {
                        'ctx-a': {
                            'policy_key': 'workspace-context'
                        }
                    },
                }
            }
        },
    }
    original = copy.deepcopy(snapshot)

    actual = skypilot_config.get_effective_workspace_region_config_from_snapshot(
        config_snapshot=snapshot,
        cloud='kubernetes',
        keys=('policy_key',),
        region=region,
        default_value='default',
        workspace=workspace,
        override_configs=override_configs)

    assert actual == expected
    assert snapshot == original


def test_snapshot_getter_does_not_read_ambient_workspace(monkeypatch):

    def _fail_ambient_read(*_args, **_kwargs):
        raise AssertionError('ambient workspace must not be read')

    monkeypatch.setattr(skypilot_config, 'get_active_workspace',
                        _fail_ambient_read)
    snapshot = {
        'kubernetes': {
            'policy_key': 'global'
        },
        'workspaces': {
            'team': {
                'kubernetes': {
                    'policy_key': 'workspace'
                }
            }
        },
    }

    assert skypilot_config.get_effective_workspace_region_config_from_snapshot(
        config_snapshot=snapshot,
        cloud='kubernetes',
        keys=('policy_key',),
        workspace='team') == 'workspace'
    assert skypilot_config.get_effective_workspace_region_config_from_snapshot(
        config_snapshot=snapshot,
        cloud='kubernetes',
        keys=('policy_key',),
        workspace=None) == 'global'


@pytest.mark.parametrize(('workspace', 'region', 'override_configs'), [
    ('default', None, None),
    ('default', 'ctx-a', None),
    ('team', None, None),
    ('team', 'ctx-a', None),
    ('team', 'ctx-b', None),
    ('missing', 'ctx-a', None),
    ('team', 'ctx-a', {
        'kubernetes': {
            'context_configs': {
                'ctx-a': {
                    'policy_key': 'resource'
                }
            }
        }
    }),
])
def test_snapshot_getter_matches_live_getter(monkeypatch, workspace, region,
                                             override_configs):
    snapshot = {
        'kubernetes': {
            'policy_key': 'global',
            'context_configs': {
                'ctx-a': {
                    'policy_key': 'global-context'
                }
            },
        },
        'workspaces': {
            'team': {
                'kubernetes': {
                    'policy_key': 'workspace',
                    'context_configs': {
                        'ctx-a': {
                            'policy_key': 'workspace-context'
                        }
                    },
                }
            }
        },
    }
    monkeypatch.setattr(skypilot_config, '_get_loaded_config',
                        lambda: config_utils.Config(copy.deepcopy(snapshot)))

    live = skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        keys=('policy_key',),
        region=region,
        default_value='default',
        workspace=workspace,
        override_configs=override_configs)
    snapshotted = (
        skypilot_config.get_effective_workspace_region_config_from_snapshot(
            config_snapshot=snapshot,
            cloud='kubernetes',
            keys=('policy_key',),
            region=region,
            default_value='default',
            workspace=workspace,
            override_configs=override_configs))

    assert snapshotted == live


@pytest.mark.parametrize(('workspace_argument', 'expected_workspace'), [
    (None, 'active-workspace'),
    ('explicit-workspace', 'explicit-workspace'),
])
def test_live_getter_delegates_with_one_config_and_workspace_capture(
        monkeypatch, workspace_argument, expected_workspace):
    snapshot = {'kubernetes': {'policy_key': 'global'}}
    get_loaded_config = mock.Mock(return_value=snapshot)
    get_active_workspace = mock.Mock(return_value='active-workspace')
    snapshot_getter = mock.Mock(return_value='resolved')
    monkeypatch.setattr(skypilot_config, '_get_loaded_config',
                        get_loaded_config)
    monkeypatch.setattr(skypilot_config, 'get_active_workspace',
                        get_active_workspace)
    monkeypatch.setattr(skypilot_config,
                        'get_effective_workspace_region_config_from_snapshot',
                        snapshot_getter)

    actual = skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        keys=('policy_key',),
        region='ctx-a',
        default_value='default',
        workspace=workspace_argument,
        override_configs={'kubernetes': {
            'policy_key': 'resource'
        }})

    assert actual == 'resolved'
    get_loaded_config.assert_called_once_with()
    if workspace_argument is None:
        get_active_workspace.assert_called_once_with()
    else:
        get_active_workspace.assert_not_called()
    snapshot_getter.assert_called_once_with(
        config_snapshot=snapshot,
        cloud='kubernetes',
        keys=('policy_key',),
        region='ctx-a',
        default_value='default',
        workspace=expected_workspace,
        override_configs={'kubernetes': {
            'policy_key': 'resource'
        }})


def _resolve(
    *,
    effective_allowed_contexts=None,
    available_contexts: tuple[str, ...] = _AVAILABLE_CONTEXTS,
    current_context: str | None = 'ctx-current',
    in_cluster_available: bool = True,
    allow_all_contexts: bool = False,
    include_in_cluster: bool = True,
) -> kubernetes_context_policy.KubernetesAllowedContextsResolution:
    return kubernetes_context_policy.resolve_kubernetes_allowed_contexts(
        effective_allowed_contexts=effective_allowed_contexts,
        available_contexts=available_contexts,
        current_context=current_context,
        in_cluster_available=in_cluster_available,
        in_cluster_context='in-cluster',
        allow_all_contexts=allow_all_contexts,
        include_in_cluster=include_in_cluster)


@pytest.mark.parametrize(
    ('config_snapshot', 'workspace', 'override_configs', 'expected'), [
        ({
            'kubernetes': {
                'allowed_contexts': ['ctx-global']
            },
            'workspaces': {
                'team': {
                    'kubernetes': {
                        'allowed_contexts': ['ctx-workspace']
                    }
                }
            },
        }, 'team', None, ('ctx-workspace',)),
        ({
            'kubernetes': {
                'allowed_contexts': ['ctx-global']
            },
            'workspaces': {
                'team': {
                    'kubernetes': {
                        'allowed_contexts': ['ctx-workspace']
                    }
                }
            },
        }, 'team', {
            'kubernetes': {
                'allowed_contexts': ['ctx-resource']
            }
        }, ('ctx-resource',)),
        ({
            'kubernetes': {
                'allowed_contexts': ['ctx-global']
            }
        }, 'missing', None, ('ctx-global',)),
    ])
def test_config_precedence_matches_legacy_policy(config_snapshot, workspace,
                                                 override_configs, expected):
    effective_allowed_contexts = (
        skypilot_config.get_effective_workspace_region_config_from_snapshot(
            config_snapshot=config_snapshot,
            cloud='kubernetes',
            keys=('allowed_contexts',),
            workspace=workspace,
            override_configs=override_configs))
    resolution = _resolve(effective_allowed_contexts=effective_allowed_contexts)

    assert resolution.existing_contexts == expected
    assert not resolution.skipped_contexts


def test_explicit_contexts_preserve_order_duplicates_and_warning_order():
    resolution = _resolve(effective_allowed_contexts=[
        'ctx-b', 'missing', 'ctx-a', 'ctx-b', 'ssh-node-pool', 'missing'
    ])

    assert resolution.existing_contexts == ('ctx-b', 'ctx-a', 'ctx-b')
    assert resolution.skipped_contexts == ('missing', 'missing')


def test_all_matches_legacy_set_order_and_filters_ssh():
    available = ('ctx-b', 'ssh-node-pool', 'ctx-a', 'ctx-b', 'in-cluster')
    resolution = _resolve(effective_allowed_contexts='all',
                          available_contexts=available)

    expected = tuple(
        context for context in set(available) if not context.startswith('ssh-'))
    assert resolution.existing_contexts == expected
    assert not resolution.skipped_contexts


def test_allow_all_environment_only_applies_when_config_is_absent():
    env_resolution = _resolve(allow_all_contexts=True, include_in_cluster=False)
    explicit_empty_resolution = _resolve(effective_allowed_contexts=[],
                                         allow_all_contexts=True,
                                         include_in_cluster=False)

    expected = tuple(
        context for context in set(_AVAILABLE_CONTEXTS)
        if not context.startswith('ssh-') and context != 'in-cluster')
    assert env_resolution.existing_contexts == expected
    assert not explicit_empty_resolution.existing_contexts


@pytest.mark.parametrize(('current_context', 'in_cluster_available',
                          'include_in_cluster', 'expected'), [
                              ('ctx-current', False, True, ('ctx-current',)),
                              ('ssh-node-pool', True, True, ('in-cluster',)),
                              (None, True, True, ('in-cluster',)),
                              (None, True, False, ()),
                              (None, False, True, ()),
                          ])
def test_derived_current_and_in_cluster_fallbacks(current_context,
                                                  in_cluster_available,
                                                  include_in_cluster, expected):
    resolution = _resolve(current_context=current_context,
                          in_cluster_available=in_cluster_available,
                          include_in_cluster=include_in_cluster)

    assert resolution.existing_contexts == expected
    assert not resolution.skipped_contexts


def test_explicit_in_cluster_context_bypasses_derived_filter():
    resolution = _resolve(effective_allowed_contexts=['in-cluster'],
                          include_in_cluster=False)

    assert resolution.existing_contexts == ('in-cluster',)


def test_empty_inventory_returns_empty_before_policy_evaluation():
    resolution = _resolve(effective_allowed_contexts=object(),
                          available_contexts=())

    assert not resolution.existing_contexts
    assert not resolution.skipped_contexts


def test_legacy_cloud_uses_one_snapshot_and_preserves_warning_output(
        monkeypatch):
    snapshot = {
        'kubernetes': {
            'allowed_contexts': [
                'ctx-b', 'missing', 'ctx-a', 'ctx-b', 'ssh-node-pool', 'missing'
            ]
        }
    }
    to_dict = mock.Mock(return_value=snapshot)
    get_active_workspace = mock.Mock(return_value='default')
    current_context = mock.Mock(side_effect=AssertionError(
        'explicit contexts must not read current kubeconfig context'))
    in_cluster_available = mock.Mock(side_effect=AssertionError(
        'explicit contexts must not probe in-cluster availability'))
    in_cluster_context_name = mock.Mock(side_effect=AssertionError(
        'explicit contexts must not read the in-cluster context name'))
    warning = mock.Mock()
    monkeypatch.setattr(skypilot_config, 'to_dict', to_dict)
    monkeypatch.setattr(skypilot_config, 'get_active_workspace',
                        get_active_workspace)
    monkeypatch.setattr(
        'sky.provision.kubernetes.utils.get_all_kube_context_names',
        lambda: ['ctx-a', 'ctx-b', 'ssh-node-pool'])
    monkeypatch.setattr(
        'sky.provision.kubernetes.utils.'
        'get_current_kube_config_context_name', current_context)
    monkeypatch.setattr(
        'sky.provision.kubernetes.utils.is_incluster_config_available',
        in_cluster_available)
    monkeypatch.setattr(kubernetes.kubernetes, 'in_cluster_context_name',
                        in_cluster_context_name)
    monkeypatch.setattr(kubernetes.Kubernetes, '_log_skipped_contexts_once',
                        warning)

    result = kubernetes.Kubernetes.existing_allowed_contexts(silent=False)

    assert result == ['ctx-b', 'ctx-a', 'ctx-b']
    to_dict.assert_called_once_with()
    get_active_workspace.assert_called_once_with()
    current_context.assert_not_called()
    in_cluster_available.assert_not_called()
    in_cluster_context_name.assert_not_called()
    warning.assert_called_once_with(('missing', 'missing'))
