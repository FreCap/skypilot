"""A service must use at most ONE security group, never one per replica.

SkyPilot names a port-declaring cluster's group after the cluster, which for a
service is one group per replica. This fleet replaces spot replicas
continuously across 20 AWS regions, so the count grows without bound: ~3000
groups against a 2500-per-VPC quota, ~99% orphaned because teardown cannot wait
long enough for the network interface to detach.
"""
# pylint: disable=protected-access
import sky
from sky.clouds import cloud as cloud_lib
from sky.serve import replica_managers
from sky.skylet import constants
from sky.utils import resources_utils


def _task_with_ports() -> sky.Task:
    task = sky.Task(run='echo hi')
    task.set_resources(sky.Resources(cloud=sky.AWS(), ports=['8080']))
    return task


def _deploy_variables(task: sky.Task, cluster: str) -> dict:
    resource = list(task.resources)[0].copy(region='us-east-1',
                                            zone='us-east-1a',
                                            instance_type='m5.large')
    return sky.AWS().make_deploy_resources_variables(
        resource,
        resources_utils.ClusterName(cluster, cluster),
        region=cloud_lib.Region('us-east-1'),
        zones=[cloud_lib.Zone('us-east-1a')],
        num_nodes=1)


def test_key_is_task_overrideable():
    """Without this the override is silently dropped by Resources.copy."""
    assert ('aws',
            'security_group_name') in constants.OVERRIDEABLE_CONFIG_KEYS_IN_TASK


def test_replica_group_is_named_for_the_service_not_the_replica():
    task = _task_with_ports()
    replica_managers._scope_security_group_to_service(task, 'boltz-l4-fleet')
    variables = _deploy_variables(task, 'boltz-l4-fleet-123')
    assert variables['security_group'] == 'sky-sg-boltz-l4-fleet'
    # The replica id must not appear, or it is still one group per replica.
    assert '123' not in variables['security_group']


def test_two_replicas_of_one_service_share_a_group():
    names = set()
    for replica_id in (1, 2, 3):
        task = _task_with_ports()
        replica_managers._scope_security_group_to_service(task, 'svc')
        names.add(
            _deploy_variables(task, f'svc-{replica_id}')['security_group'])
    assert names == {'sky-sg-svc'}


def test_two_different_services_do_not_share_a_group():
    """Sharing across services would be a real widening: the group's
    self-referencing rule grants ALL protocols and ports between members."""
    groups = set()
    for service in ('svc-a', 'svc-b'):
        task = _task_with_ports()
        replica_managers._scope_security_group_to_service(task, service)
        groups.add(_deploy_variables(task, f'{service}-1')['security_group'])
    assert len(groups) == 2, groups


def test_scoped_group_is_not_deleted_on_a_single_replica_teardown():
    """One replica going away must not remove its siblings' group."""
    task = _task_with_ports()
    replica_managers._scope_security_group_to_service(task, 'svc')
    variables = _deploy_variables(task, 'svc-1')
    assert variables['security_group_managed_by_skypilot'] == 'false'


def test_unscoped_cluster_keeps_todays_per_cluster_behaviour():
    task = _task_with_ports()
    variables = _deploy_variables(task, 'my-cluster')
    assert variables['security_group'] == 'sky-sg-my-cluster'
    assert variables['security_group_managed_by_skypilot'] == 'true'


def test_no_service_name_is_a_no_op():
    task = _task_with_ports()
    replica_managers._scope_security_group_to_service(task, None)
    assert all(not r.cluster_config_overrides for r in task.resources)


def test_an_explicit_operator_pin_wins():
    task = _task_with_ports()
    replica_managers._scope_security_group_to_service(task, 'first')
    replica_managers._scope_security_group_to_service(task, 'second')
    assert _deploy_variables(task,
                             'first-1')['security_group'] == ('sky-sg-first')
