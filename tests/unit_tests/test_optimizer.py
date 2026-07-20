"""Unit tests for sky.optimizer."""
import pickle
import types
from unittest import mock

import pytest

from sky import clouds
from sky import dag as dag_lib
from sky import exceptions
from sky import optimizer
from sky import optimizer_candidate_generation
from sky import resources as resources_lib
from sky import task as task_lib


def test_filter_out_blocked_launchable_resources_preserves_order():
    """Blocked candidates are removed without reordering the survivors."""
    blocked = object()
    first = mock.Mock()
    first.should_be_blocked_by.return_value = False
    second = mock.Mock()
    second.should_be_blocked_by.return_value = True
    third = mock.Mock()
    third.should_be_blocked_by.return_value = False

    result = optimizer._filter_out_blocked_launchable_resources(  # pylint: disable=protected-access
        [first, second, third], [blocked])

    assert result == [first, third]
    first.should_be_blocked_by.assert_called_once_with(blocked)
    second.should_be_blocked_by.assert_called_once_with(blocked)
    third.should_be_blocked_by.assert_called_once_with(blocked)


def test_check_specified_regions_rejects_missing_kubernetes_context():
    """A requested Kubernetes context must be enabled."""
    resource = types.SimpleNamespace(cloud=clouds.Kubernetes(),
                                     region='missing-context')
    task = types.SimpleNamespace(resources=[resource],
                                 name='training',
                                 volume_mounts=[])

    with mock.patch.object(clouds.Kubernetes,
                           'existing_allowed_contexts',
                           return_value=[]), pytest.raises(
                               exceptions.ResourcesUnavailableError,
                               match='Kubernetes/missing-context'):
        optimizer._check_specified_regions(  # pylint: disable=protected-access
            task)


def test_candidate_generation_compatibility_aliases():
    """The original optimizer import paths remain stable after extraction."""
    assert getattr(optimizer, '_filter_out_blocked_launchable_resources') is (
        optimizer_candidate_generation.filter_out_blocked_launchable_resources)
    assert getattr(optimizer, '_check_specified_clouds') is (
        optimizer_candidate_generation.check_specified_clouds)
    assert getattr(optimizer, '_check_specified_regions') is (
        optimizer_candidate_generation.check_specified_regions)
    assert getattr(optimizer, '_fill_in_launchable_resources') is (
        optimizer_candidate_generation.fill_in_launchable_resources)


def test_print_optimized_plan_preserves_facade_logging_and_egress_hook():
    """The optimizer facade keeps its reporting output and egress hook."""
    task = mock.Mock(spec=task_lib.Task)
    task.name = 'training'
    task.num_nodes = 1
    task.time_estimator_func = None
    task.resources = set()
    task.get_inputs.return_value = None
    task.get_outputs.return_value = None

    cloud = mock.Mock(spec=clouds.AWS)
    cloud.__str__ = mock.Mock(return_value='AWS')
    cloud.get_vcpus_mem_from_instance_type.return_value = (4.0, 16.0)

    resource = mock.Mock(spec=resources_lib.Resources)
    resource.cloud = cloud
    resource.instance_type = 'g5.xlarge'
    resource.accelerators = {'A10G': 1}
    resource.use_spot = False
    resource.disk_tier = None
    resource.get_accelerators_str.return_value = 'A10G:1'
    resource.get_spot_str.return_value = ''
    resource.assert_launchable.return_value = resource
    resource.to_yaml_config.return_value = {'instance_type': 'g5.xlarge'}
    resource.infra.formatted_str.return_value = 'AWS (us-east-1)'

    graph = mock.Mock()
    node_to_cost_map = {task: {resource: 1.25}}
    plan = {task: resource}

    with mock.patch.object(optimizer.Optimizer,
                           '_print_egress_plan') as mock_print_egress, \
            mock.patch.object(optimizer, 'logger') as mock_logger:
        optimizer.Optimizer.print_optimized_plan(graph, [task], plan, 3600,
                                                 1.25, node_to_cost_map, True)

    mock_print_egress.assert_called_once_with(graph, plan, True)
    messages = '\n'.join(
        str(call.args[0]) for call in mock_logger.info.call_args_list)
    assert 'Considered resources' in messages
    assert 'g5.xlarge' in messages
    assert 'A10G:1' in messages


def test_reporting_facade_preserves_callable_identity():
    """Historical optimizer reporting methods remain pickle-compatible."""
    for name in ('_print_egress_plan', 'print_optimized_plan',
                 '_print_candidates', '_print_job_group_plan'):
        method = getattr(optimizer.Optimizer, name)
        assert method.__module__ == optimizer.__name__
        assert pickle.loads(pickle.dumps(method)) is method


def test_print_egress_plan_preserves_facade_helpers_and_logging():
    """Egress reporting keeps optimizer-owned calculation hooks."""
    parent = mock.Mock(spec=task_lib.Task)
    parent.name = 'producer'
    parent.__str__ = mock.Mock(return_value='producer')
    child = mock.Mock(spec=task_lib.Task)
    child.__str__ = mock.Mock(return_value='consumer')
    graph = mock.Mock()
    graph.edges.return_value = [(parent, child)]
    parent_resource = mock.Mock(spec=resources_lib.Resources)
    child_resource = mock.Mock(spec=resources_lib.Resources)
    plan = {parent: parent_resource, child: child_resource}

    with mock.patch.object(optimizer.Optimizer,
                           '_get_egress_info',
                           return_value=(clouds.AWS(), clouds.GCP(), 2.5)), \
            mock.patch.object(optimizer.Optimizer,
                              '_egress_cost',
                              return_value=0.25) as mock_egress_cost, \
            mock.patch.object(optimizer, 'logger') as mock_logger:
        optimizer.Optimizer._print_egress_plan(  # pylint: disable=protected-access
            graph, plan, True)

    mock_egress_cost.assert_called_once()
    message = str(mock_logger.info.call_args.args[0])
    assert 'Egress plan' in message
    assert 'producer' in message
    assert 'consumer' in message
    assert '2.5' in message
    assert '0.25' in message


def test_print_egress_plan_preserves_zero_byte_return_value():
    """The historical zero-byte short-circuit still returns zero."""
    parent = mock.Mock(spec=task_lib.Task)
    child = mock.Mock(spec=task_lib.Task)
    graph = mock.Mock()
    graph.edges.return_value = [(parent, child)]
    plan = {
        parent: mock.Mock(spec=resources_lib.Resources),
        child: mock.Mock(spec=resources_lib.Resources),
    }

    with mock.patch.object(optimizer.Optimizer,
                           '_get_egress_info',
                           return_value=(None, None, 0)):
        result = optimizer.Optimizer._print_egress_plan(  # pylint: disable=protected-access
            graph, plan, True)

    assert result == 0


def test_print_candidates_preserves_facade_logging():
    """Candidate reporting keeps the optimizer logger compatibility seam."""
    node = mock.Mock(spec=task_lib.Task)
    best_resource = mock.Mock(spec=resources_lib.Resources)
    best_resource.accelerators = {'L4': 1}
    best_resource.get_accelerators_str.return_value = 'L4:1'
    node.best_resources = best_resource

    first = mock.Mock(spec=resources_lib.Resources)
    first.instance_type = 'g2-standard-4'
    first.get_accelerators_str.return_value = 'L4:1'
    second = mock.Mock(spec=resources_lib.Resources)
    second.instance_type = 'g2-standard-8'
    second.get_accelerators_str.return_value = 'L4:1'

    with mock.patch.object(optimizer.optimizer_reporting.resources_utils,
                           'format_resource',
                           return_value=('L4:1', 'L4:1')), mock.patch.object(
                               optimizer, 'logger') as mock_logger:
        optimizer.Optimizer._print_candidates(  # pylint: disable=protected-access
            {node: {
                clouds.GCP(): [first, second]
            }})

    messages = '\n'.join(
        str(call.args[0]) for call in mock_logger.info.call_args_list)
    assert 'Multiple GCP instances satisfy L4:1' in messages
    assert 'g2-standard-4' in messages
    assert 'g2-standard-8' in messages
    assert 'sky gpus list L4' in messages


def test_check_specified_clouds_keeps_enabled_clouds():
    """Enabled resource clouds proceed to region validation unchanged."""
    cloud = clouds.AWS()
    resource = types.SimpleNamespace(cloud=cloud)
    task = types.SimpleNamespace(resources=[resource], name='training')
    dag = types.SimpleNamespace(tasks=[task])

    with mock.patch.object(optimizer_candidate_generation.sky_check,
                           'get_cached_enabled_clouds_or_refresh',
                           return_value=[cloud]), mock.patch.object(
                               optimizer_candidate_generation,
                               'check_specified_regions') as mock_check_regions:
        optimizer._check_specified_clouds(  # pylint: disable=protected-access
            dag)

    mock_check_regions.assert_called_once_with(task)


def test_fill_in_launchable_resources_preserves_candidate_metadata():
    """Candidate generation preserves ordering, hints, and launchables."""
    cloud = clouds.AWS()
    requested = mock.Mock()
    requested.cloud = None
    requested.container_image = None
    requested.validate = mock.Mock()
    requested.no_missing_accel_warnings = False
    cheapest = mock.Mock()
    alternative = mock.Mock()
    launchable = mock.Mock()
    launchable.should_be_blocked_by.return_value = False
    feasible = types.SimpleNamespace(resources_list=[cheapest, alternative],
                                     fuzzy_candidate_list=[],
                                     hint='capacity hint')
    task = types.SimpleNamespace(resources=[requested], num_nodes=2)

    with mock.patch.object(
            optimizer_candidate_generation.sky_check,
            'get_cached_enabled_clouds_or_refresh',
            return_value=[cloud]), mock.patch.object(
                optimizer_candidate_generation.subprocess_utils,
                'run_in_parallel',
                return_value=[(cloud, feasible)]), mock.patch.object(
                    optimizer_candidate_generation.resources_utils,
                    'make_launchables_for_valid_region_zones',
                    return_value=[launchable]):
        launchable_map, candidates, fuzzy, hints = (
            optimizer._fill_in_launchable_resources(  # pylint: disable=protected-access
                task,
                blocked_resources=[mock.sentinel.blocked]))

    requested.validate.assert_called_once_with()
    assert launchable_map[requested] == [launchable]
    assert candidates[cloud] == [cheapest, alternative]
    assert not fuzzy
    assert hints[requested] == ['capacity hint']


def _optimize_ordered_task_with_mock_launchable(dag, launchable_call_indexes):
    fill_calls = []

    def fake_fill_in_launchable_resources(task, blocked_resources, quiet):
        del blocked_resources, quiet
        requested_resources = next(iter(task.resources))
        call_index = len(fill_calls)
        fill_calls.append(requested_resources)
        launchable_resources = ([requested_resources] if call_index
                                in launchable_call_indexes else [])
        return {requested_resources: launchable_resources}, {}, [], {}

    def fake_optimize_by_dp(topo_order, node_to_cost_map, minimize_cost):
        del node_to_cost_map, minimize_cost
        task_node = topo_order[0]
        return {task_node: next(iter(task_node.resources))}, 0

    with mock.patch('sky.optimizer._fill_in_launchable_resources',
                    side_effect=fake_fill_in_launchable_resources), \
            mock.patch.object(optimizer.Optimizer,
                              '_estimate_nodes_cost_or_time',
                              return_value=({}, {})), \
            mock.patch.object(optimizer.Optimizer,
                              '_optimize_by_dp',
                              side_effect=fake_optimize_by_dp), \
            mock.patch.object(optimizer.Optimizer,
                              '_compute_total_time',
                              return_value=0):
        optimizer.Optimizer._optimize_dag(  # pylint: disable=protected-access
            dag, quiet=True)

    return fill_calls


def test_ordered_resources_without_docker_login_stops_at_first_launchable():
    """Ensure ordered resources stop at the first launchable candidate."""
    with dag_lib.Dag() as dag:
        task = task_lib.Task(name='ordered-no-docker')
        task.set_resources([
            resources_lib.Resources(infra='aws/us-east-2',
                                    accelerators='A100:8',
                                    use_spot=True),
            resources_lib.Resources(infra='aws/us-east-2',
                                    accelerators='L4:4',
                                    use_spot=True),
        ])

    fill_calls = _optimize_ordered_task_with_mock_launchable(
        dag, launchable_call_indexes={0})

    assert len(fill_calls) == 1
    assert task.best_resources.accelerators == {'A100': 8}


def test_ordered_resources_with_docker_login_stops_at_first_launchable():
    """Ensure ordered resources use the post-set_resources launchable key."""
    with dag_lib.Dag() as dag:
        task = task_lib.Task(
            name='ordered-docker',
            secrets={
                'SKYPILOT_DOCKER_SERVER': 'registry.example.com',
                'SKYPILOT_DOCKER_USERNAME': 'user',
                'SKYPILOT_DOCKER_PASSWORD': 'password',
            })
        task.set_resources([
            resources_lib.Resources(infra='aws/us-east-2',
                                    accelerators='A100:8',
                                    image_id='docker:repo/image:tag',
                                    use_spot=True),
            resources_lib.Resources(infra='aws/us-east-2',
                                    accelerators='L4:4',
                                    image_id='docker:repo/image:tag',
                                    use_spot=True),
        ])

    fill_calls = _optimize_ordered_task_with_mock_launchable(
        dag, launchable_call_indexes={0})

    assert len(fill_calls) == 1
    assert task.best_resources.accelerators == {'A100': 8}


def test_ordered_resources_with_docker_login_uses_first_launchable():
    """Ensure ordered resources continue to the first launchable candidate."""
    with dag_lib.Dag() as dag:
        task = task_lib.Task(
            name='ordered-docker-first-unavailable',
            secrets={
                'SKYPILOT_DOCKER_SERVER': 'registry.example.com',
                'SKYPILOT_DOCKER_USERNAME': 'user',
                'SKYPILOT_DOCKER_PASSWORD': 'password',
            })
        task.set_resources([
            resources_lib.Resources(infra='aws/us-east-2',
                                    accelerators='A100:8',
                                    image_id='docker:repo/image:tag',
                                    use_spot=True),
            resources_lib.Resources(infra='aws/us-east-2',
                                    accelerators='H100:8',
                                    image_id='docker:repo/image:tag',
                                    use_spot=True),
            resources_lib.Resources(infra='aws/us-east-2',
                                    accelerators='L4:4',
                                    image_id='docker:repo/image:tag',
                                    use_spot=True),
        ])

    fill_calls = _optimize_ordered_task_with_mock_launchable(
        dag, launchable_call_indexes={1})

    assert len(fill_calls) == 2
    assert task.best_resources.accelerators == {'H100': 8}
