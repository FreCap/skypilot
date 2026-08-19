"""Heterogeneous (hybrid) spot placement: shapes per location + zero-cost tier.

Phase-2 of the research-cluster scavenger design: one any_of set mixes
zero-cost reserved capacity (Kubernetes pool, non-spot, A100) with paid
cloud spot (L4). The placer must fill the zero-cost tier COMPLETELY
before spending on spot, pin each launch's accelerators/use_spot via its
location, and pull load back when reserved capacity frees (TTL retry).
"""
# pylint: disable=import-outside-toplevel,redefined-outer-name,protected-access,unused-variable
import json
from unittest import mock

import pytest
from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer

from sky.container_images import models as container_image_models
from sky.serve import placement_policy
from sky.serve import service_spec as service_spec_lib
from sky.serve import spot_placer


def _physical_contract():
    return placement_policy.resolve_fresh_contract(
        placement_policy.SPOT_HEDGE_PLACER, pool=False)


def _logical_contract():
    return placement_policy.resolve_fresh_contract(
        placement_policy.CAPACITY_AWARE_SPOT_PLACER, pool=False)


class TestCentralPlacementCatalog:
    """One immutable complete catalog drives every runtime lookup."""

    def test_explicit_instance_types_remain_launchable_for_feasibility(
            self, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.clouds import aws as aws_cloud
        from sky.utils import resources_utils

        task = sky.Task.from_yaml_str("""
resources:
  any_of:
    - infra: aws/us-east-1
      instance_type: r6a.xlarge
      use_spot: true
    - infra: aws/us-east-1
      instance_type: r6a.xlarge
      use_spot: false
run: echo hi
""")
        captured = []

        def _feasible(self, resources, num_nodes=1):
            del num_nodes
            assert resources.cloud == self
            assert resources.is_launchable()
            captured.append(resources)
            return resources_utils.FeasibleResources([resources], [], None)

        monkeypatch.setattr(aws_cloud.AWS, 'get_feasible_launchable_resources',
                            _feasible)
        monkeypatch.setattr(
            spot_placer.resources_utils,
            'make_launchables_for_valid_region_zones', lambda resources, **_:
            [resources.copy(region='us-east-1', zone=None)])

        locations = spot_placer._get_possible_location_from_task(task)

        assert {(location.instance_type, location.use_spot)
                for location in locations} == {
                    ('r6a.xlarge', True),
                    ('r6a.xlarge', False),
                }
        assert len(captured) == 2

    def test_round_trip_preserves_exact_locations_and_unavailable_price(self):
        reserved = make_location('research-ctx',
                                 accelerators={'A100': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        paid = make_location('us-east-1',
                             accelerators={'L4': 1},
                             cloud_name='AWS',
                             instance_type='g6.xlarge')
        paid.image_id = {None: 'docker:registry.example/model:v1'}
        entries = tuple(
            sorted(((reserved, 0.0), (paid, float('inf'))),
                   key=lambda item: item[0].sort_key()))
        catalog = spot_placer.PlacementCatalog(entries)

        serialized = catalog.to_dict()
        assert any(
            entry['hourly_cost'] is None for entry in serialized['entries'])
        paid_entry = next(entry for entry in serialized['entries']
                          if entry['location']['cloud'] == 'AWS')
        assert paid_entry['location']['image_id'] == [{
            'region': None,
            'image': 'docker:registry.example/model:v1',
        }]
        restored = spot_placer.PlacementCatalog.from_dict(serialized)
        assert restored.to_dict() == serialized

    def test_new_catalog_round_trip_persists_strict_node_count(self):
        paid = make_location('us-east-1',
                             accelerators={'L4': 1},
                             cloud_name='AWS',
                             instance_type='g6.xlarge')
        catalog = spot_placer.PlacementCatalog(((paid, 0.2),), num_nodes=3)

        serialized = catalog.to_dict()

        assert serialized['num_nodes'] == 3
        assert spot_placer.PlacementCatalog.from_dict(serialized).num_nodes == 3

    @pytest.mark.parametrize('num_nodes', [True, False, 0, -1, 1.5, '2'])
    def test_catalog_rejects_invalid_node_count(self, num_nodes):
        serialized = self._single_catalog_dict()
        serialized['num_nodes'] = num_nodes

        with pytest.raises(ValueError, match='num_nodes'):
            spot_placer.PlacementCatalog.from_dict(serialized)

    def test_catalog_rejects_overflowing_hourly_cost(self):
        serialized = self._single_catalog_dict()
        serialized['entries'][0]['hourly_cost'] = 10**1000

        with pytest.raises(ValueError, match='hourly cost'):
            spot_placer.PlacementCatalog.from_dict(serialized)

    @staticmethod
    def _single_catalog_dict():
        paid = make_location('us-east-1',
                             accelerators={'L4': 1},
                             cloud_name='AWS',
                             instance_type='g6.xlarge')
        return spot_placer.PlacementCatalog(((paid, 0.2),)).to_dict()

    def test_catalog_rejects_boolean_schema_version(self):
        serialized = self._single_catalog_dict()
        serialized['schema_version'] = True

        with pytest.raises(ValueError, match='schema version'):
            spot_placer.PlacementCatalog.from_dict(serialized)

    def test_runtime_lookup_never_resolves_a_complete_catalog(self):
        reserved = make_location('research-ctx',
                                 accelerators={'A100': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        paid = make_location('us-east-1',
                             accelerators={'L4': 1},
                             cloud_name='AWS',
                             instance_type='g6.xlarge')
        catalog = spot_placer.PlacementCatalog(((reserved, 0.0), (paid, 0.2)))
        resources = mock.MagicMock()
        task = mock.MagicMock(resources=[resources], num_nodes=1)
        with mock.patch.object(
                spot_placer,
                '_get_possible_location_from_task',
                side_effect=AssertionError(
                    'persisted catalog load must not enumerate providers')):
            placer = spot_placer.SpotPlacer(task,
                                            _physical_contract(),
                                            placement_catalog=catalog)

        assert placer.cost_per_hour(paid) == 0.2
        assert placer.zero_cost_locations() == [reserved]
        assert placer._min_cost_location([paid, reserved]) == reserved
        resources.copy.assert_not_called()

    def test_bulk_cost_view_is_immutable_and_evaluates_policy_once(self):
        reserved = make_location('research-ctx',
                                 accelerators={'A100': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        paid = make_location('us-east-1',
                             accelerators={'L4': 1},
                             cloud_name='AWS',
                             instance_type='g6.xlarge')
        placer = make_placer({reserved: 0.0, paid: 0.2})

        with mock.patch.object(
                placer,
                '_workspace_eligible_locations',
                wraps=placer._workspace_eligible_locations) as eligibility:
            costs = placer.known_location_costs()

        assert dict(costs) == {reserved: 0.0, paid: 0.2}
        eligibility.assert_called_once_with()
        with pytest.raises(TypeError):
            costs[paid] = 9.0
        placer.location2cost[paid] = 9.0
        assert costs[paid] == 0.2

    def test_explicit_workspace_disallowed_cloud_is_not_enumerated(
            self, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.clouds import aws as aws_cloud
        from sky.clouds import kubernetes as kubernetes_cloud
        from sky.utils import resources_utils

        task = sky.Task.from_yaml_str("""
resources:
  any_of:
    - infra: k8s/research-ctx
      accelerators: A100:1
      use_spot: false
    - infra: aws/us-east-1
      accelerators: L4:1
      use_spot: true
run: echo hi
""")
        get_allowed_clouds = mock.MagicMock(return_value=['AWS'])
        monkeypatch.setattr(spot_placer.sky_check,
                            'get_workspace_allowed_clouds', get_allowed_clouds)
        kubernetes_feasibility = mock.MagicMock(side_effect=AssertionError(
            'workspace-disallowed Kubernetes must not be enumerated'))
        monkeypatch.setattr(kubernetes_cloud.Kubernetes,
                            'get_feasible_launchable_resources',
                            kubernetes_feasibility)

        def _aws_feasible(self, resources, num_nodes=1):
            del num_nodes
            launchable = resources.copy(cloud=self, instance_type='g6.xlarge')
            return resources_utils.FeasibleResources([launchable], [], None)

        monkeypatch.setattr(aws_cloud.AWS, 'get_feasible_launchable_resources',
                            _aws_feasible)
        monkeypatch.setattr(
            spot_placer.resources_utils,
            'make_launchables_for_valid_region_zones', lambda resources, **_:
            [resources.copy(region='us-east-1', zone='us-east-1a')])

        locations = spot_placer._get_possible_location_from_task(
            task, workspace='research')

        assert locations
        assert {str(location.cloud) for location in locations} == {'AWS'}
        get_allowed_clouds.assert_called_once_with(
            'research',
            capability=spot_placer.sky_cloud.CloudCapability.COMPUTE)
        kubernetes_feasibility.assert_not_called()

    def test_implicit_candidates_intersect_stale_enabled_cloud_cache(
            self, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.clouds import aws as aws_cloud
        from sky.clouds import kubernetes as kubernetes_cloud
        from sky.utils import resources_utils

        task = sky.Task.from_yaml_str("""
resources:
  accelerators: L4:1
  use_spot: true
run: echo hi
""")
        monkeypatch.setattr(spot_placer.sky_check,
                            'get_workspace_allowed_clouds',
                            lambda workspace, **_: ['AWS'])

        def _cached_enabled_clouds(**_):
            assert (spot_placer.skypilot_config.get_active_workspace() ==
                    'research')
            # Simulate the non-atomic window after policy changed but before
            # the enabled-cloud cache was refreshed.
            return [kubernetes_cloud.Kubernetes(), aws_cloud.AWS()]

        monkeypatch.setattr(spot_placer.sky_check,
                            'get_cached_enabled_clouds_or_refresh',
                            _cached_enabled_clouds)
        kubernetes_feasibility = mock.MagicMock(side_effect=AssertionError(
            'a stale enabled-cloud cache must not reintroduce Kubernetes'))
        monkeypatch.setattr(kubernetes_cloud.Kubernetes,
                            'get_feasible_launchable_resources',
                            kubernetes_feasibility)

        def _aws_feasible(self, resources, num_nodes=1):
            del num_nodes
            launchable = resources.copy(cloud=self, instance_type='g6.xlarge')
            return resources_utils.FeasibleResources([launchable], [], None)

        monkeypatch.setattr(aws_cloud.AWS, 'get_feasible_launchable_resources',
                            _aws_feasible)
        monkeypatch.setattr(
            spot_placer.resources_utils,
            'make_launchables_for_valid_region_zones', lambda resources, **_:
            [resources.copy(region='us-east-1', zone='us-east-1a')])

        locations = spot_placer._get_possible_location_from_task(
            task, workspace='research')

        assert locations
        assert {str(location.cloud) for location in locations} == {'AWS'}
        kubernetes_feasibility.assert_not_called()

    def test_explicit_provider_enumeration_uses_service_workspace(
            self, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.clouds import kubernetes as kubernetes_cloud

        task = sky.Task.from_yaml_str("""
resources:
  infra: k8s/research-ctx
  accelerators: A100-80GB:1
  use_spot: false
run: echo hi
""")
        monkeypatch.setattr(spot_placer.sky_check,
                            'get_workspace_allowed_clouds',
                            lambda workspace, **_: ['Kubernetes'])

        def _allowed_contexts(_cls, silent=False):
            del silent
            assert (spot_placer.skypilot_config.get_active_workspace() ==
                    'research')
            return ['research-ctx']

        monkeypatch.setattr(kubernetes_cloud.Kubernetes,
                            'existing_allowed_contexts',
                            classmethod(_allowed_contexts))
        live_feasibility = mock.MagicMock(side_effect=AssertionError(
            'declarative Kubernetes catalog must not query live capacity'))
        monkeypatch.setattr(kubernetes_cloud.Kubernetes,
                            'get_feasible_launchable_resources',
                            live_feasibility)

        locations = spot_placer._get_possible_location_from_task(
            task, workspace='research')

        assert len(locations) == 1
        assert str(locations[0].cloud) == 'Kubernetes'
        assert locations[0].region == 'research-ctx'
        assert locations[0].accelerators == {'A100-80GB': 1}
        live_feasibility.assert_not_called()

    def test_scale_to_zero_exact_gpu_shapes_remain_in_catalog(
            self, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.clouds import kubernetes as kubernetes_cloud

        task = sky.Task.from_yaml_str("""
resources:
  any_of:
    - infra: k8s/east
      accelerators: A100:1
      use_spot: false
    - infra: k8s/east
      accelerators: A100-80GB:1
      use_spot: false
    - infra: k8s/phx
      accelerators: H200:1
      use_spot: false
run: echo hi
""")
        monkeypatch.setattr(spot_placer.sky_check,
                            'get_workspace_allowed_clouds',
                            lambda *_args, **_kwargs: ['Kubernetes'])
        monkeypatch.setattr(
            kubernetes_cloud.Kubernetes, 'existing_allowed_contexts',
            classmethod(lambda _cls, silent=False: ['east', 'phx']))
        monkeypatch.setattr(
            kubernetes_cloud.Kubernetes, 'get_feasible_launchable_resources',
            mock.MagicMock(side_effect=AssertionError(
                'live node capacity is not catalog authority')))

        catalog = spot_placer.PlacementCatalog.from_task(task,
                                                         workspace='research')

        assert {(location.region,
                 tuple(sorted((location.accelerators or {}).items())))
                for location, _ in catalog.entries} == {
                    ('east', (('A100', 1),)),
                    ('east', (('A100-80GB', 1),)),
                    ('phx', (('H200', 1),)),
                }

    @pytest.mark.parametrize('cpus', ['4+', None])
    def test_declarative_gpu_shape_normalizes_ratio_memory(self, cpus):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.clouds import kubernetes as kubernetes_cloud

        resources = sky.Resources(cloud=kubernetes_cloud.Kubernetes(),
                                  accelerators={'H200': 1},
                                  cpus=cpus,
                                  memory='4x')

        instance_type = (kubernetes_cloud.Kubernetes.
                         get_declarative_instance_type(resources))

        assert instance_type == '4CPU--16GB--H200:1'

    def test_persisted_catalog_is_projected_onto_workspace_policy(
            self, monkeypatch):
        reserved = make_location('research-ctx',
                                 accelerators={'A100': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        paid = make_location('us-east-1',
                             accelerators={'L4': 1},
                             cloud_name='AWS',
                             instance_type='g6.xlarge')
        catalog = spot_placer.PlacementCatalog(((reserved, 0.0), (paid, 0.2)))
        resources = mock.MagicMock()
        task = mock.MagicMock(resources=[resources], num_nodes=1)
        allowed_clouds = ['AWS']
        monkeypatch.setattr(spot_placer.sky_check,
                            'get_workspace_allowed_clouds',
                            lambda workspace, **_: list(allowed_clouds))
        monkeypatch.setattr(spot_placer.skypilot_config, 'get_workspace_cloud',
                            lambda cloud, workspace: {})

        placer = spot_placer.SpotPlacer(task,
                                        _physical_contract(),
                                        placement_catalog=catalog,
                                        workspace='default')

        # The immutable version catalog remains intact, but every runtime
        # placement/reserved-fill view contains eligible locations only.
        assert placer.placement_catalog == catalog
        assert set(placer.location2status) == {reserved, paid}
        assert placer.location2cost[reserved] == 0.0
        assert placer.known_locations() == [paid]
        assert placer.zero_cost_locations() == []
        assert placer.select_next_zero_cost_location() is None
        assert placer.select_next_location() == paid
        assert placer.cost_per_hour(reserved) == float('inf')
        assert placer.cost_per_hour(paid) == 0.2

        # A later broadening can restore the durable entry without rebuilding
        # the catalog or restarting the controller.
        allowed_clouds.append('Kubernetes')
        assert set(placer.known_locations()) == {reserved, paid}
        assert placer.zero_cost_locations() == [reserved]
        assert placer.select_next_location() == reserved

    def test_live_placer_rechecks_narrowed_workspace_policy(self, monkeypatch):
        reserved_a = make_location('research-a',
                                   accelerators={'A100': 1},
                                   cloud_name='Kubernetes',
                                   use_spot=False)
        reserved_b = make_location('research-b',
                                   accelerators={'A100': 1},
                                   cloud_name='Kubernetes',
                                   use_spot=False)
        paid = make_location('us-east-1',
                             accelerators={'L4': 1},
                             cloud_name='AWS',
                             instance_type='g6.xlarge')
        catalog = spot_placer.PlacementCatalog(
            ((reserved_a, 0.0), (reserved_b, 0.0), (paid, 0.2)))
        resources = mock.MagicMock()
        task = mock.MagicMock(resources=[resources], num_nodes=1)
        policy = {
            'clouds': ['Kubernetes', 'AWS'],
            'contexts': ['research-a', 'research-b'],
        }
        monkeypatch.setattr(spot_placer.sky_check,
                            'get_workspace_allowed_clouds',
                            lambda workspace, **_: list(policy['clouds']))

        def _workspace_cloud(cloud, workspace):
            assert workspace == 'research'
            if cloud == 'kubernetes':
                return {'allowed_contexts': list(policy['contexts'])}
            return {}

        monkeypatch.setattr(spot_placer.skypilot_config, 'get_workspace_cloud',
                            _workspace_cloud)
        placer = spot_placer.SpotPlacer(task,
                                        _physical_contract(),
                                        placement_catalog=catalog,
                                        workspace='research')

        with mock.patch.object(spot_placer.skypilot_config,
                               'safe_reload_config') as reload_config:
            placer.refresh_workspace_policy()
        reload_config.assert_called_once_with()
        assert set(placer.known_locations()) == {reserved_a, reserved_b, paid}

        # Simulate a scale-to-zero service whose workspace drops one context
        # after the controller has already loaded the durable catalog.
        policy['contexts'] = ['research-b']
        assert set(placer.known_locations()) == {reserved_b, paid}
        assert placer.zero_cost_locations() == [reserved_b]
        assert placer.cost_per_hour(reserved_a) == float('inf')
        assert not placer.is_active_location(reserved_a)
        assert not placer.is_launch_admissible(reserved_a, selected_at=100.0)
        snapshot_locations = placer.placement_snapshot()['locations']
        assert {entry['region'] for entry in snapshot_locations
               } == {'research-b', 'us-east-1'}
        assert placer.select_next_location() == reserved_b

        # Disabling Kubernetes must take effect in the same long-lived placer,
        # without waiting for a controller restart or rebuilding the catalog.
        policy['clouds'] = ['AWS']
        assert placer.zero_cost_locations() == []
        assert placer.select_next_location() == paid

        # A later policy broadening makes the immutable candidates eligible
        # again; their retry state and catalog prices were retained.
        policy['clouds'] = ['Kubernetes', 'AWS']
        policy['contexts'] = ['research-a', 'research-b']
        assert set(placer.zero_cost_locations()) == {reserved_a, reserved_b}
        assert placer.cost_per_hour(reserved_a) == 0.0

    @pytest.mark.parametrize(
        ('cloud_name', 'region', 'config'),
        [
            ('Kubernetes', 'research-a', {
                'allowed_contexts': ['research-b']
            }),
            ('SSH', 'ssh-pool-a', {
                'allowed_node_pools': ['pool-b']
            }),
            ('Slurm', 'cluster-a', {
                'allowed_clusters': ['cluster-b']
            }),
        ],
    )
    def test_runtime_context_policy_excludes_stale_candidate(
            self, monkeypatch, cloud_name, region, config):
        location = make_location(region,
                                 accelerators={'A100': 1},
                                 cloud_name=cloud_name,
                                 use_spot=False)
        catalog = spot_placer.PlacementCatalog(((location, 0.0),))
        task = mock.MagicMock(resources=[mock.MagicMock()], num_nodes=1)
        monkeypatch.setattr(spot_placer.sky_check,
                            'get_workspace_allowed_clouds',
                            lambda workspace, **_: [cloud_name])
        monkeypatch.setattr(
            spot_placer.skypilot_config, 'get_workspace_cloud',
            lambda cloud, workspace: config
            if cloud == cloud_name.lower() else {})

        placer = spot_placer.SpotPlacer(task,
                                        _physical_contract(),
                                        placement_catalog=catalog,
                                        workspace='research')

        assert placer.known_locations() == []
        assert placer.active_locations() == []
        assert placer.zero_cost_locations() == []
        assert placer.cost_per_hour(location) == float('inf')
        assert placer.select_next_location() is None


@pytest.fixture
def hybrid_placer():
    k8s = make_location('research-ctx',
                        accelerators={'A100': 1},
                        use_spot=False)
    cheap_spot = make_location('us-east-1',
                               accelerators={'L4': 1},
                               use_spot=True)
    pricey_spot = make_location('eu-west-3',
                                accelerators={'L4': 1},
                                use_spot=True)
    placer = make_placer({
        k8s: 0.0,
        cheap_spot: 0.2,
        pricey_spot: 0.3,
    })
    return placer, k8s, cheap_spot, pricey_spot


def _make_per_gpu_placer(costs):
    return make_placer(costs, placement_contract=_logical_contract())


class TestInstanceTypeLocationIdentity:
    """Exact provider shapes stay distinct across current and legacy rows."""

    def test_same_card_and_region_with_different_instance_types_are_distinct(
            self):
        first = make_location('us-east-1', {'L4': 1},
                              cloud_name='AWS',
                              instance_type='g6.xlarge')
        second = make_location('us-east-1', {'L4': 1},
                               cloud_name='AWS',
                               instance_type='g6.2xlarge')

        assert first != second
        assert len({first, second}) == 2

    def test_legacy_missing_instance_type_resolves_when_unambiguous(self):
        current = make_location('us-east-1', {'L4': 1},
                                cloud_name='AWS',
                                instance_type='g6.xlarge')
        legacy = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')

        assert make_placer({current: 1.0}).resolve_location(legacy) == current

    def test_legacy_missing_instance_type_is_skipped_when_ambiguous(self):
        first = make_location('us-east-1', {'L4': 1},
                              cloud_name='AWS',
                              instance_type='g6.xlarge')
        second = make_location('us-east-1', {'L4': 1},
                               cloud_name='AWS',
                               instance_type='g6.2xlarge')
        legacy = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')

        placer = make_placer({
            first: 1.0,
            second: 2.0,
        })

        assert placer.resolve_location(legacy) is None
        assert placer.resolve_location(
            legacy, allow_ambiguous_legacy_shape=True) == first
        assert placer.is_launch_admissible(legacy, selected_at=100)

    def test_strict_catalog_match_reports_legacy_ambiguity(self):
        first = make_location('us-east-1', {'L4': 1},
                              cloud_name='AWS',
                              instance_type='g6.xlarge')
        second = make_location('us-east-1', {'L4': 1},
                               cloud_name='AWS',
                               instance_type='g6.2xlarge')
        legacy = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')

        assert spot_placer.match_catalog_location_strict(
            legacy, [first, second]) == (None, True)
        assert spot_placer.match_catalog_location_strict(legacy,
                                                         [first]) == (first,
                                                                      False)
        assert spot_placer.match_catalog_location_strict(
            second, [first, second]) == (second, False)

    def test_ambiguous_legacy_failure_benches_cheapest_matching_shape(self):
        first = make_location('us-east-1', {'L4': 1},
                              cloud_name='AWS',
                              instance_type='g6.xlarge')
        second = make_location('us-east-1', {'L4': 1},
                               cloud_name='AWS',
                               instance_type='g6.2xlarge')
        legacy = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        placer = make_placer({first: 1.0, second: 2.0})

        placer.set_preemptive(legacy)

        assert first in placer.preemptive_locations()
        assert second not in placer.preemptive_locations()

    def test_exact_current_instance_type_resolves(self):
        current = make_location('us-east-1', {'L4': 1},
                                cloud_name='AWS',
                                instance_type='g6.xlarge')
        equivalent = make_location('us-east-1', {'L4': 1},
                                   cloud_name='AWS',
                                   instance_type='g6.xlarge')

        assert make_placer({
            current: 1.0
        }).resolve_location(equivalent) == equivalent

    def test_exact_mapping_match_does_not_scan_catalog(self):
        current = make_location('us-east-1', {'L4': 1},
                                cloud_name='AWS',
                                instance_type='g6.xlarge')
        equivalent = make_location('us-east-1', {'L4': 1},
                                   cloud_name='AWS',
                                   instance_type='g6.xlarge')

        class _ExactOnlyMapping(dict):

            def __iter__(self):
                raise AssertionError('exact mapping lookup must not iterate')

        candidates = _ExactOnlyMapping(
            {current: spot_placer.LocationStatus.ACTIVE})

        assert spot_placer.match_catalog_location_strict(
            equivalent, candidates) == (equivalent, False)

    def test_failed_type_can_fall_back_to_sibling_type_in_same_region(self):
        cheapest = make_location('us-east-1', {'L4': 1},
                                 cloud_name='AWS',
                                 instance_type='g6.xlarge')
        sibling = make_location('us-east-1', {'L4': 1},
                                cloud_name='AWS',
                                instance_type='g6.2xlarge')
        other_region = make_location('us-west-2', {'L4': 1},
                                     cloud_name='AWS',
                                     instance_type='g6.xlarge')
        placer = make_placer({
            cheapest: 1.0,
            sibling: 1.1,
            other_region: 2.0,
        })

        assert placer.select_next_location() == cheapest
        placer.set_preemptive(cheapest)
        assert placer.select_next_location() == sibling


class TestZeroCostTierFirst:
    """Free capacity fills completely before any paid launch."""

    def test_zero_cost_wins(self, hybrid_placer):
        placer, k8s, cheap_spot, pricey_spot = hybrid_placer
        assert placer.select_next_location() == k8s

    def test_benched_zero_cost_falls_back_to_cheapest_spot(
            self, hybrid_placer, monkeypatch):
        placer, k8s, cheap_spot, pricey_spot = hybrid_placer
        monkeypatch.setattr(spot_placer.time, 'time', lambda: 1000.0)
        placer.set_preemptive(k8s)  # cluster full -> launch failed
        assert placer.select_next_location() == cheap_spot

    def test_ttl_retry_pulls_back_to_zero_cost(self, hybrid_placer,
                                               monkeypatch):
        placer, k8s, cheap_spot, pricey_spot = hybrid_placer
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(k8s)
        assert placer.select_next_location() == cheap_spot
        # Capacity frees; after the TTL the zero-cost tier is probed
        # again and immediately preferred.
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert placer.select_next_location() == k8s


class TestCapacityAwareCost:
    """Opt-in placement discovers and prices paid shapes per GPU."""

    def test_catalog_counts_expand_and_region_feasibility_filters(
            self, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.clouds import aws as aws_cloud
        from sky.utils import resources_utils

        task = sky.Task.from_yaml_str("""
resources:
  infra: aws/us-east-1
  accelerators: L4:1
  use_spot: true
run: echo hi
""")
        monkeypatch.setattr(spot_placer.catalog, 'list_accelerator_counts',
                            lambda **_: {'L4': [1.0, 4.0, 8.0, 0.5]})

        def _feasible(self, resources, num_nodes=1):
            del self, num_nodes
            count = next(iter(resources.accelerators.values()))
            launchable = resources.copy(cloud=aws_cloud.AWS(),
                                        instance_type=f'fake-l4-{count}')
            return resources_utils.FeasibleResources([launchable], [], None)

        def _launchables(resources, override_optimize_by_zone=False):
            del override_optimize_by_zone
            count = next(iter(resources.accelerators.values()))
            # Simulate the 8-GPU shape being catalog-supported globally but
            # unavailable in the one region allowed by this task.
            region = 'us-west-2' if count == 8 else 'us-east-1'
            return [resources.copy(region=region, zone=None)]

        monkeypatch.setattr(aws_cloud.AWS, 'get_feasible_launchable_resources',
                            _feasible)
        monkeypatch.setattr(spot_placer.resources_utils,
                            'make_launchables_for_valid_region_zones',
                            _launchables)

        locations = spot_placer._get_possible_location_from_task(
            task, expand_accelerator_counts=True)

        assert {next(iter(loc.accelerators.values())) for loc in locations
               } == {1, 4}
        assert {loc.region for loc in locations} == {'us-east-1'}
        assert {loc.instance_type for loc in locations
               } == {'fake-l4-1', 'fake-l4-4'}

    def test_live_cluster_catalog_is_never_queried(self, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.clouds import kubernetes as kubernetes_cloud
        from sky.utils import resources_utils

        task = sky.Task.from_yaml_str("""
resources:
  infra: k8s/research-ctx
  accelerators: A100:1
  use_spot: true
run: echo hi
""")
        catalog_call = mock.MagicMock(
            side_effect=AssertionError('must not query Kubernetes catalog'))
        monkeypatch.setattr(spot_placer.catalog, 'list_accelerator_counts',
                            catalog_call)
        monkeypatch.setattr(kubernetes_cloud.Kubernetes,
                            'get_feasible_launchable_resources',
                            lambda self, resources, num_nodes=1: resources_utils
                            .FeasibleResources([], [], None))

        spot_placer._get_possible_location_from_task(
            task, expand_accelerator_counts=True)

        catalog_call.assert_not_called()

    def test_each_any_of_accelerator_model_expands_independently(
            self, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.clouds import aws as aws_cloud
        from sky.utils import resources_utils

        task = sky.Task.from_yaml_str("""
resources:
  any_of:
    - infra: aws/us-east-1
      accelerators: L4:1
      use_spot: true
    - infra: aws/us-east-1
      accelerators: A10G:1
      use_spot: true
run: echo hi
""")

        def _counts(*, name_filter, **_kwargs):
            if 'L4' in name_filter:
                return {'L4': [1.0, 4.0]}
            return {'A10G': [1.0, 2.0]}

        catalog_call = mock.MagicMock(side_effect=_counts)
        monkeypatch.setattr(spot_placer.catalog, 'list_accelerator_counts',
                            catalog_call)

        def _feasible(self, resources, num_nodes=1):
            del self, num_nodes
            accelerator, count = next(iter(resources.accelerators.items()))
            launchable = resources.copy(cloud=aws_cloud.AWS(),
                                        instance_type=f'fake-{accelerator}-'
                                        f'{count}')
            return resources_utils.FeasibleResources([launchable], [], None)

        monkeypatch.setattr(aws_cloud.AWS, 'get_feasible_launchable_resources',
                            _feasible)
        monkeypatch.setattr(
            spot_placer.resources_utils,
            'make_launchables_for_valid_region_zones', lambda resources, **_:
            [resources.copy(region='us-east-1', zone=None)])

        locations = spot_placer._get_possible_location_from_task(
            task, expand_accelerator_counts=True)

        assert {(accelerator, count)
                for location in locations
                for accelerator, count in (location.accelerators or {}).items()
               } == {('L4', 1), ('L4', 4), ('A10G', 1), ('A10G', 2)}
        assert {location.instance_type for location in locations
               } == {'fake-L4-1', 'fake-L4-4', 'fake-A10G-1', 'fake-A10G-2'}
        assert catalog_call.call_count == 2

    def test_explicit_instance_type_remains_exact(self, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import sky

        resources = sky.Resources(cloud=sky.clouds.AWS(),
                                  instance_type='g6.12xlarge',
                                  accelerators={'L4': 4},
                                  use_spot=True)
        catalog_call = mock.MagicMock()
        monkeypatch.setattr(spot_placer.catalog, 'list_accelerator_counts',
                            catalog_call)

        assert spot_placer._expand_accelerator_counts_for_cloud(
            resources, sky.clouds.AWS()) == [resources]
        catalog_call.assert_not_called()

    def test_multi_gpu_shape_wins_when_it_is_cheaper_per_gpu(self):
        one_gpu = make_location('same-zone',
                                accelerators={'L4': 1},
                                cloud_name='AWS')
        four_gpu = make_location('same-zone',
                                 accelerators={'L4': 4},
                                 cloud_name='AWS')
        costs = {one_gpu: 0.2, four_gpu: 0.6}

        assert make_placer(costs).select_next_location() == one_gpu
        per_gpu = _make_per_gpu_placer(costs)
        assert per_gpu.select_next_location() == four_gpu
        assert per_gpu.ranked_active_locations() == [four_gpu, one_gpu]

    def test_ranked_active_locations_matches_selection_and_stabilizes_ties(
            self, monkeypatch):
        first_tie = make_location('first-tie',
                                  accelerators={'L4': 1},
                                  cloud_name='AWS')
        cheapest = make_location('cheapest',
                                 accelerators={'L4': 2},
                                 cloud_name='AWS')
        second_tie = make_location('second-tie',
                                   accelerators={'L4': 4},
                                   cloud_name='AWS')
        benched = make_location('benched',
                                accelerators={'L4': 1},
                                cloud_name='AWS')
        workspace_excluded = make_location('workspace-excluded',
                                           accelerators={'L4': 1},
                                           cloud_name='AWS')
        placer = _make_per_gpu_placer({
            first_tie: 0.2,
            cheapest: 0.2,
            second_tie: 0.8,
            benched: 0.01,
            workspace_excluded: 0.001,
        })
        monkeypatch.setattr(spot_placer.time, 'time', lambda: 1000.0)
        monkeypatch.setattr(spot_placer, '_preemption_retry_seconds',
                            lambda: 600.0)
        placer.set_preemptive(benched)
        workspace_eligible = {first_tie, cheapest, second_tie, benched}

        with mock.patch.object(placer,
                               '_workspace_eligible_locations',
                               return_value=workspace_eligible):
            remaining = placer.active_locations()
            repeated_selection_order = []
            while remaining:
                selected = placer._min_cost_location(remaining)
                repeated_selection_order.append(selected)
                remaining.remove(selected)
            ranked = placer.ranked_active_locations()

        assert ranked == repeated_selection_order
        assert ranked == [cheapest, first_tie, second_tie]

    def test_ranked_active_locations_evaluates_each_cost_key_once(self):
        locations = [
            make_location(f'region-{index}',
                          accelerators={'L4': 1},
                          cloud_name='AWS') for index in range(64)
        ]
        placer = make_placer({
            location: float(len(locations) - index)
            for index, location in enumerate(locations)
        })

        with mock.patch.object(
                placer,
                '_normalized_location_cost',
                wraps=placer._normalized_location_cost) as normalized_cost:
            with mock.patch.object(
                    placer,
                    '_min_cost_location',
                    side_effect=AssertionError(
                        'ranking must not repeat minimum scans')):
                ranked = placer.ranked_active_locations()

        assert ranked == list(reversed(locations))
        assert normalized_cost.call_count == len(locations)

    def test_fractional_gpu_shape_uses_exact_configured_count(self):
        half_gpu = make_location('half', accelerators={'L4': 0.5})
        one_gpu = make_location('one', accelerators={'L4': 1})
        placer = _make_per_gpu_placer({half_gpu: 0.4, one_gpu: 0.6})

        assert placer.select_next_location() == one_gpu

    def test_cheapest_per_gpu_shape_is_reused(self):
        one_gpu = make_location('one', accelerators={'L4': 1}, cloud_name='AWS')
        four_gpu = make_location('four',
                                 accelerators={'L4': 4},
                                 cloud_name='AWS')
        placer = _make_per_gpu_placer({one_gpu: 0.2, four_gpu: 0.6})

        assert placer.select_next_location() == four_gpu
        assert placer.select_next_location() == four_gpu

    def test_cheapest_per_gpu_wins_across_regions(self):
        one_gpu = make_location('east',
                                accelerators={'L4': 1},
                                cloud_name='AWS')
        four_gpu = make_location('east',
                                 accelerators={'L4': 4},
                                 cloud_name='AWS')
        other_region = make_location('west',
                                     accelerators={'L4': 1},
                                     cloud_name='AWS')
        placer = _make_per_gpu_placer({
            one_gpu: 0.2,
            four_gpu: 0.6,
            other_region: 0.25,
        })

        assert placer.select_next_location() == four_gpu
        assert placer.select_next_location() == four_gpu

    def test_multiple_accelerator_models_share_per_gpu_optimization(self):
        l4 = make_location('same-zone',
                           accelerators={'L4': 4},
                           cloud_name='AWS')
        a10g = make_location('same-zone',
                             accelerators={'A10G': 1},
                             cloud_name='AWS')
        placer = _make_per_gpu_placer({l4: 0.8, a10g: 0.25})

        assert placer.select_next_location() == l4

    def test_zero_cost_tier_still_wins(self):
        reserved = make_location('reserved', accelerators={'A100': 1})
        paid = make_location('paid', accelerators={'L4': 8})
        placer = _make_per_gpu_placer({reserved: 0.0, paid: 0.4})

        assert placer.select_next_location() == reserved


class TestProvisionTimeoutWarning:
    """The safety warning must use the task's effective timeout."""

    def test_task_override_suppresses_false_global_warning(self, monkeypatch):
        task_override = {'kubernetes': {'provision_timeout': 90}}
        resource = mock.MagicMock(cluster_config_overrides=task_override)
        task = mock.MagicMock(resources=[resource], num_nodes=1)
        k8s = make_location('research-ctx', cloud_name='Kubernetes')
        monkeypatch.setattr(spot_placer, '_get_possible_location_from_task',
                            lambda _: [k8s])

        get_timeout = mock.MagicMock(
            side_effect=lambda *args, override_configs=None, **kwargs:
            (override_configs['kubernetes']['provision_timeout']
             if override_configs else 600))
        monkeypatch.setattr(spot_placer.skypilot_config,
                            'get_effective_region_config', get_timeout)
        warning = mock.MagicMock()
        monkeypatch.setattr(spot_placer.logger, 'warning', warning)

        spot_placer.SpotPlacer(task, _physical_contract())

        warning.assert_not_called()
        assert get_timeout.call_args.kwargs['override_configs'] == task_override


class TestHeterogeneousLocations:
    """Locations are distinct per shape and carry launch overrides."""

    def test_same_region_different_shape_are_distinct(self):
        a = make_location('us-east-1', accelerators={'L4': 1})
        b = make_location('us-east-1', accelerators={'L4': 4})
        assert a != b
        assert len({a, b}) == 2

    def test_container_image_selector_namespaces_are_distinct(self):
        base = make_location('us-east-1', accelerators={'L4': 1})

        def with_image(image):
            return spot_placer.Location(cloud=base.cloud,
                                        region=base.region,
                                        zone=base.zone,
                                        accelerators=base.accelerators,
                                        use_spot=base.use_spot,
                                        container_image=image)

        source_ref = ('registry.example/model@sha256:' + 'a' * 64)
        by_ref = with_image(
            container_image_models.ContainerImage(ref=source_ref))
        by_release = with_image(
            container_image_models.ContainerImage(release='same'))
        by_artifact = with_image(
            container_image_models.ContainerImage(
                artifact_id='11111111-1111-4111-8111-111111111111'))
        by_ref_and_release = with_image(
            container_image_models.ContainerImage(ref=source_ref,
                                                  release='same'))

        assert len({by_ref, by_release, by_artifact, by_ref_and_release}) == 4
        assert by_ref != by_release
        assert by_release != by_artifact
        assert by_ref != by_ref_and_release

    def test_to_dict_carries_shape_and_spotness(self):
        k8s = make_location('ctx', accelerators={'A100': 1}, use_spot=False)
        d = k8s.to_dict()
        assert d['accelerators'] == {'A100': 1}
        assert d['use_spot'] is False
        # image_id/disk_tier are always present (None clears): a location
        # without them must strip them from every copied entry — e.g. a
        # VM-selected launch clearing the k8s entry's docker image.
        assert 'image_id' in d and d['image_id'] is None
        assert 'disk_tier' in d and d['disk_tier'] is None
        assert 'ephemeral_storage' in d and d['ephemeral_storage'] is None
        # accelerators likewise unconditional: a CPU-only location must
        # strip GPU entries' accelerators from its copies.
        cpu_only = make_location('cpu-region', accelerators=None)
        d_cpu = cpu_only.to_dict()
        assert 'accelerators' in d_cpu and d_cpu['accelerators'] is None

    def test_pickleable_roundtrip_and_backcompat(self):
        with mock.patch.object(spot_placer.registry.CLOUD_REGISTRY,
                               'from_str',
                               return_value=mock.MagicMock()):
            loc = spot_placer.Location.from_pickleable({
                'cloud': 'AWS',
                'region': 'us-east-1',
                'zone': 'us-east-1a',
                'accelerators': {
                    'L4': 1
                },
                'use_spot': False,
            })
            assert loc.accelerators == {'L4': 1}
            assert loc.use_spot is False
            # Rows pickled before this change carry neither key.
            old = spot_placer.Location.from_pickleable({
                'cloud': 'AWS',
                'region': 'us-east-1',
                'zone': None,
            })
            assert old.accelerators is None
            assert old.use_spot is True


class TestEphemeralStoragePerLocation:
    """Kubernetes entries carry storage requests without poisoning VMs."""

    def test_mixed_storage_request_reaches_each_cloud_shape(self, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.clouds import aws as aws_cloud
        from sky.clouds import kubernetes as kubernetes_cloud
        from sky.utils import resources_utils

        captured = []

        def _capture(self, resources, num_nodes=1):
            del self, num_nodes
            captured.append(resources)
            return resources_utils.FeasibleResources([], [], None)

        monkeypatch.setattr(aws_cloud.AWS, 'get_feasible_launchable_resources',
                            _capture)
        monkeypatch.setattr(
            kubernetes_cloud.Kubernetes, 'existing_allowed_contexts',
            classmethod(lambda _cls, silent=False: ['research-ctx']))

        def _capture_declarative(cls, resources):
            del cls
            captured.append(resources)
            return '4CPU--16GB--A100:1'

        monkeypatch.setattr(kubernetes_cloud.Kubernetes,
                            'get_declarative_instance_type',
                            classmethod(_capture_declarative))
        task = sky.Task.from_yaml_str("""
resources:
  cpus: 4+
  ports: 8080
  any_of:
    - infra: k8s/research-ctx
      accelerators: A100:1
      use_spot: false
      ephemeral_storage: 20
    - infra: aws/us-east-1
      accelerators: L4:1
      use_spot: true
run: echo hi
""")

        spot_placer._get_possible_location_from_task(task)

        by_accelerator = {next(iter(r.accelerators or {})): r for r in captured}
        assert by_accelerator['A100'].ephemeral_storage == 20
        assert by_accelerator['L4'].ephemeral_storage is None

    def test_location_override_pins_or_clears_ephemeral_storage(self):
        k8s = make_location('research-ctx',
                            cloud_name='Kubernetes',
                            ephemeral_storage=20)
        cloud = make_location('us-east-1', cloud_name='AWS')

        assert k8s.to_dict()['ephemeral_storage'] == 20
        assert cloud.to_dict()['ephemeral_storage'] is None

    def test_pickle_roundtrip_and_backcompat(self):
        with mock.patch.object(spot_placer.registry.CLOUD_REGISTRY,
                               'from_str',
                               return_value=mock.MagicMock()):
            loc = spot_placer.Location.from_pickleable({
                'cloud': 'Kubernetes',
                'region': 'research-ctx',
                'zone': None,
                'ephemeral_storage': 20,
            })
            assert loc.ephemeral_storage == 20
            old = spot_placer.Location.from_pickleable({
                'cloud': 'Kubernetes',
                'region': 'research-ctx',
                'zone': None,
            })
            assert old.ephemeral_storage is None


class TestShouldUseSpotMixed:
    """Mixed spot/non-spot any_of engages the placer (ANY semantics)."""

    def test_mixed_any_of(self):
        # pylint: disable=import-outside-toplevel
        # Imported here: replica_managers pulls the full backend graph,
        # which the pure-placer tests above should not pay for.
        from sky.serve import replica_managers
        yaml_mixed = """
resources:
  cpus: 2+
  any_of:
    - infra: aws/us-east-1
      accelerators: L4:1
      use_spot: true
    - infra: aws/us-east-2
      accelerators: A10G:1
      use_spot: false
run: echo hi
"""
        assert replica_managers._should_use_spot(yaml_mixed, None) is True
        yaml_none = yaml_mixed.replace('use_spot: true', 'use_spot: false')
        assert replica_managers._should_use_spot(yaml_none, None) is False
        # Override still wins.
        assert replica_managers._should_use_spot(yaml_none,
                                                 {'use_spot': True}) is True


class TestLegacyLocationResolution:
    """Pre-upgrade shape-less locations resolve onto shape-bearing keys."""

    def test_set_preemptive_resolves_by_region(self, monkeypatch):
        monkeypatch.setattr(spot_placer.time, 'time', lambda: 1000.0)
        keyed = make_location('us-east-1', accelerators={'L4': 1})
        other = make_location('us-east-2', accelerators={'L4': 1})
        placer = make_placer({keyed: 0.2, other: 0.2})
        # Shape-less location, as deserialized from a pre-upgrade row.
        placer.set_preemptive(
            spot_placer.Location(cloud=keyed.cloud,
                                 region='us-east-1',
                                 zone=None))
        assert keyed in placer.preemptive_locations()

    def test_unknown_location_is_ignored_not_asserted(self):
        keyed = make_location('us-east-1', accelerators={'L4': 1})
        placer = make_placer({keyed: 0.2})
        stranger = make_location('mars-central-1')
        # Must not raise.
        placer.set_preemptive(stranger)
        placer.set_active(stranger)
        assert keyed in placer.active_locations()


class TestMixedValidation:
    """validate_service_task accepts placer-managed mixed sets only."""

    def _task(self, spot_placer_value, k8s_spot):
        import textwrap

        import sky
        per_gpu = spot_placer_value == 'dynamic_fallback_per_gpu'
        logical_replica_policy = (
            '                target_concurrency_per_replica: 1'
            if per_gpu else '                target_qps_per_replica: 0.1')
        logical_service_policy = (
            '              graceful_drain_async_occupancy: true'
            if per_gpu else '')
        yaml_str = textwrap.dedent(f"""
            resources:
              cpus: 2+
              ports: 8080
              any_of:
                - infra: aws/us-east-1
                  accelerators: L4:1
                  use_spot: true
                - infra: aws/us-east-2
                  accelerators: A10G:1
                  use_spot: {str(k8s_spot).lower()}
            service:
              readiness_probe: /health
{logical_service_policy}
              replica_policy:
                min_replicas: 1
                max_replicas: 2
{logical_replica_policy}
                {'spot_placer: ' + spot_placer_value if spot_placer_value else ''}
            run: echo hi
            """)
        return sky.Task.from_yaml_str(yaml_str)

    def test_mixed_with_placer_accepted(self):
        from sky.serve import serve_utils
        serve_utils.validate_service_task(self._task('dynamic_fallback',
                                                     k8s_spot=False),
                                          pool=False)

    def test_mixed_with_per_gpu_placer_accepted(self):
        from sky.serve import serve_utils
        serve_utils.validate_service_task(self._task('dynamic_fallback_per_gpu',
                                                     k8s_spot=False),
                                          pool=False)

    def test_validation_persists_inferred_resource_port(self):
        from sky.serve import serve_utils
        task = self._task('dynamic_fallback_per_gpu', k8s_spot=False)
        assert task.service is not None
        assert task.service.ports is None

        serve_utils.validate_service_task(task, pool=False)

        assert task.service.ports == '8080'

    def test_per_gpu_placer_rejects_multi_node_before_submission(self):
        from sky.serve import serve_utils
        task = self._task('dynamic_fallback_per_gpu', k8s_spot=False)
        task.num_nodes = 2

        with pytest.raises(ValueError, match='only single-node services'):
            serve_utils.validate_service_task(task, pool=False)

    def test_mixed_without_placer_rejected(self):
        from sky.serve import serve_utils
        with pytest.raises(ValueError, match='all use spot'):
            serve_utils.validate_service_task(self._task(None, k8s_spot=False),
                                              pool=False)

    def test_placer_with_no_spot_at_all_rejected(self):
        import textwrap

        import sky
        from sky.serve import serve_utils
        yaml_str = textwrap.dedent("""
            resources:
              cpus: 2+
              ports: 8080
              accelerators: L4:1
              use_spot: false
              infra: aws/us-east-1
            service:
              readiness_probe: /health
              replica_policy:
                min_replicas: 1
                max_replicas: 2
                target_qps_per_replica: 0.1
                spot_placer: dynamic_fallback
            run: echo hi
            """)
        with pytest.raises(ValueError, match='at least one spot'):
            serve_utils.validate_service_task(sky.Task.from_yaml_str(yaml_str),
                                              pool=False)

    def _kubernetes_only_task(self,
                              provision_timeout=None,
                              first_shape='accelerators: A100-80GB:1',
                              second_shape='accelerators: H200:1',
                              placer='dynamic_fallback_per_gpu'):
        import textwrap

        import sky

        def _shape_block(shape):
            if shape is None:
                return ''
            return '\n' + textwrap.indent(shape, ' ' * 18)

        first_shape_block = _shape_block(first_shape)
        second_shape_block = _shape_block(second_shape)
        timeout_override = ''
        if provision_timeout is not None:
            timeout_override = f"""
                  _cluster_config_overrides:
                    kubernetes:
                      provision_timeout: {provision_timeout}"""
        yaml_str = textwrap.dedent(f"""
            resources:
              ports: 8080
              any_of:
                - infra: k8s/prod_research_cluster_eks
                  {first_shape_block}
                  use_spot: false{timeout_override}
                - infra: k8s/prod_research_cluster_eks
                  {second_shape_block}
                  use_spot: false{timeout_override}
            service:
              readiness_probe: /health
              graceful_drain_async_occupancy: true
              replica_policy:
                min_replicas: 0
                max_replicas: 2
                target_concurrency_per_replica: 1
                spot_placer: {placer}
            run: echo hi
            """)
        return sky.Task.from_yaml_str(yaml_str)

    def test_kubernetes_only_non_spot_placer_accepted(self):
        from sky.serve import serve_utils

        serve_utils.validate_service_task(self._kubernetes_only_task(),
                                          pool=False)

    def test_kubernetes_only_placer_accepts_finite_provisioning(self):
        from sky.serve import serve_utils

        serve_utils.validate_service_task(
            self._kubernetes_only_task(provision_timeout=30), pool=False)

    def test_kubernetes_only_placer_accepts_indefinite_provisioning(self):
        from sky.serve import serve_utils

        serve_utils.validate_service_task(
            self._kubernetes_only_task(provision_timeout=-1), pool=False)

    @pytest.mark.parametrize('invalid_shape', [None, 'accelerators: A100:0.5'])
    def test_kubernetes_only_placer_rejects_unbudgetable_shape(
            self, invalid_shape):
        from sky.serve import serve_utils

        task = self._kubernetes_only_task(first_shape=invalid_shape,
                                          placer='dynamic_fallback')
        with pytest.raises(ValueError,
                           match='positive whole-number accelerator'):
            serve_utils.validate_service_task(task, pool=False)

    def test_kubernetes_only_placer_rejects_compound_accelerators_in_schema(
            self):
        from sky import exceptions

        with pytest.raises(exceptions.InvalidSkyPilotConfigError,
                           match='too many properties'):
            self._kubernetes_only_task(
                first_shape='accelerators:\n  A100: 1\n  H200: 1',
                placer='dynamic_fallback')

    def test_kubernetes_only_non_spot_pool_remains_unsupported(self):
        import sky
        from sky.serve import serve_utils

        task = sky.Task.from_yaml_str("""
            resources:
              any_of:
                - infra: k8s/prod_research_cluster_eks
                  accelerators: A100-80GB:1
                  use_spot: false
                - infra: k8s/phx_research_cluster_eks
                  accelerators: H200:1
                  use_spot: false
            pool:
              workers: 2
              spot_placer: dynamic_fallback
            run: echo hi
            """)

        with pytest.raises(ValueError, match='requires at least one spot'):
            serve_utils.validate_service_task(task, pool=True)


class TestReservedFillPoolValidation:
    """validate_service_task groups accelerators per k8s context.

    All Kubernetes entries are treated as candidate pool shapes because
    zero-cost-ness is not knowable client-side.
    """

    def _task(self, k8s_entries, *, logical=False):
        # pylint: disable=import-outside-toplevel
        import sky
        normalized_entries = [(ctx, gpu, count)
                              for ctx, gpu, *counts in k8s_entries
                              for count in [counts[0] if counts else 1]]
        entries = '\n'.join(f'    - infra: k8s/{ctx}\n'
                            f'      accelerators: {gpu}:{count}'
                            for ctx, gpu, count in normalized_entries)
        if logical:
            entries += ('\n    - infra: aws/us-east-1\n'
                        '      accelerators: L4:1\n'
                        '      use_spot: true')
        target = ('target_concurrency_per_replica: 1'
                  if logical else 'target_qps_per_replica: 0.1')
        logical_policy = ('''\n  graceful_drain_async_occupancy: true'''
                          if logical else '')
        placer = ('\n    spot_placer: dynamic_fallback_per_gpu'
                  if logical else '')
        yaml_str = f"""
resources:
  cpus: 2+
  ports: 8080
  any_of:
{entries}
service:
  readiness_probe: /health{logical_policy}
  replica_policy:
    min_replicas: 1
    max_replicas: 4
    {target}
    reserved_capacity_fill: true{placer}
run: echo hi
"""
        return sky.Task.from_yaml_str(yaml_str)

    def test_multiple_physical_contexts_with_independent_widths_accepted(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_utils
        entries = [('ctx-a', 'A100', 1), ('ctx-b', 'H200', 8)]
        serve_utils.validate_service_task(self._task(entries), pool=False)

    def test_same_context_mixed_widths_rejected(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_utils
        entries = [('ctx-a', 'A100', 1), ('ctx-a', 'H100', 2)]
        with pytest.raises(ValueError,
                           match='one GPU count within each Kubernetes'):
            serve_utils.validate_service_task(self._task(entries), pool=False)

    def test_single_pool_accepted(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_utils
        serve_utils.validate_service_task(self._task([('ctx-a', 'A100')]),
                                          pool=False)
        serve_utils.validate_service_task(self._task([('ctx-a', 'A100'),
                                                      ('ctx-a', 'H100')]),
                                          pool=False)
        # Same pool enumerated case-insensitively still counts once.
        serve_utils.validate_service_task(self._task([('ctx-a', 'A100'),
                                                      ('ctx-a', 'a100')]),
                                          pool=False)

    def test_logical_single_pool_exact_one_gpu_accepted(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_utils
        task = self._task([('ctx-a', 'A100', 1), ('ctx-a', 'H100', 1.0)],
                          logical=True)
        serve_utils.validate_service_task(task, pool=False)

    def test_logical_multiple_contexts_exact_one_gpu_accepted(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_utils
        task = self._task([('ctx-a', 'A100', 1), ('ctx-b', 'H200', 1)],
                          logical=True)
        serve_utils.validate_service_task(task, pool=False)

    def test_logical_aws_only_accepts_broker_supplied_reserved_fill(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_utils
        serve_utils.validate_service_task(self._task([], logical=True),
                                          pool=False)

    @pytest.mark.parametrize(
        'gpu_count',
        [0.5, 1.5, 2, float('nan'), float('inf')])
    def test_logical_pool_rejects_every_non_exact_one_gpu_count(
            self, gpu_count):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_utils
        task = self._task([('ctx-a', 'A100', 1)], logical=True)
        k8s_resource = next(resource for resource in task.resources
                            if str(resource.cloud).lower() == 'kubernetes')
        # Resources currently permits non-finite positive-looking floats.
        # Mutating the parsed object exercises validation at the Serve trust
        # boundary without depending on a YAML parser's NaN/Inf spelling.
        k8s_resource._accelerators = {  # pylint: disable=protected-access
            'A100': gpu_count
        }

        with pytest.raises(ValueError, match='one-GPU Kubernetes fill shapes'):
            serve_utils.validate_service_task(task, pool=False)

    def test_historical_physical_per_gpu_contract_still_requires_one_gpu(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_utils
        task = self._task([('ctx-a', 'A100', 2)], logical=True)
        assert task.service is not None
        legacy_state = dict(task.service.__dict__)
        for field in placement_policy.CONTRACT_FIELDS:
            legacy_state.pop(field)
        legacy_state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)
        legacy = service_spec_lib.SkyServiceSpec.__new__(
            service_spec_lib.SkyServiceSpec)
        legacy.__setstate__(legacy_state)
        assert legacy.placement_contract.is_legacy_physical_per_gpu
        task.set_service(legacy)

        with pytest.raises(ValueError, match='one-GPU Kubernetes fill shapes'):
            serve_utils.validate_service_task(task, pool=False)


class TestContainerImageNormalizationEndToEnd:
    """Enumerated locations preserve native and legacy image provenance."""

    def test_enumeration_keeps_native_container_image(self):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.serve import replica_managers
        image_ref = 'registry.example/model@sha256:' + 'a' * 64
        task = sky.Task.from_yaml_str(f"""
resources:
  infra: aws/us-east-1
  accelerators: L4:1
  use_spot: true
  container_image:
    ref: {image_ref}
    distribution: direct
run: echo hi
""")

        locations = spot_placer._get_possible_location_from_task(task)

        assert locations
        resource = next(iter(task.resources))
        for location in locations:
            assert location.image_id is None
            assert location.container_image is not None
            assert location.container_image.ref == image_ref
            assert location.container_image.distribution == 'direct'
            override = location.to_dict()
            replica = replica_managers.ReplicaInfo(
                replica_id=1,
                cluster_name='replica-1',
                replica_port='8080',
                is_spot=True,
                location=location,
                version=1,
                resources_override=override,
            )
            stored_override = json.loads(json.dumps(
                replica.to_storage_dict()))['resources_override']
            assert stored_override['container_image'] == {
                'ref': image_ref,
                'distribution': 'direct',
            }
            copied = resource.copy(**override)
            assert not copied.container_image_from_legacy_image_id
            assert copied.container_image == location.container_image

    def test_enumeration_preserves_legacy_image_id_copy_path(self):
        # pylint: disable=import-outside-toplevel
        import sky
        t = sky.Task.from_yaml_str("""
resources:
  cpus: 4+
  ports: 8080
  any_of:
    - infra: aws/us-east-1
      accelerators: L4:1
      use_spot: true
      image_id: docker:myrepo/model:v1
run: echo hi
""")
        locs = spot_placer._get_possible_location_from_task(t)
        assert locs, 'expected at least one location'
        resource = next(iter(t.resources))
        for loc in locs:
            assert loc.image_id == {'docker': 'myrepo/model:v1'}, loc
            assert loc.container_image is None, loc
            copied = resource.copy(**loc.to_dict())
            assert copied.container_image_from_legacy_image_id
            assert copied.extract_docker_image() == 'myrepo/model:v1'


class TestDiskTierPerLocation:
    """disk_tier is a per-location attribute: VM entries keep 'high'
    without breaking uniformity against k8s entries that reject it."""

    def test_mixed_disk_tier_enumerates(self):
        # pylint: disable=import-outside-toplevel
        import sky
        t = sky.Task.from_yaml_str("""
resources:
  cpus: 4+
  ports: 8080
  any_of:
    - infra: aws/us-east-1
      accelerators: L4:1
      use_spot: true
      disk_tier: high
    - infra: aws/us-east-2
      accelerators: A10G:1
      use_spot: false
run: echo hi
""")
        locs = spot_placer._get_possible_location_from_task(t)
        l4 = [l for l in locs if 'L4' in (l.accelerators or {})]
        a10g = [l for l in locs if 'A10G' in (l.accelerators or {})]
        assert l4 and all(l.disk_tier == 'high' for l in l4)
        assert a10g and all(l.disk_tier is None for l in a10g)
        # The launch override pins OR CLEARS the tier: a tier-less
        # location must strip disk_tier from VM-originated entries when
        # the override is applied across the whole any_of set (a
        # Kubernetes copy with disk_tier=high fails validation).
        assert l4[0].to_dict()['disk_tier'] == 'high'
        assert a10g[0].to_dict()['disk_tier'] is None

    def test_shape_passed_to_feasibility_has_no_leaked_attrs(self, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import sky
        from sky.clouds import aws as aws_cloud
        from sky.utils import resources_utils

        captured = []

        def _capture(self, resources, num_nodes=1):
            del self, num_nodes
            captured.append(resources)
            return resources_utils.FeasibleResources([], [], None)

        monkeypatch.setattr(aws_cloud.AWS, 'get_feasible_launchable_resources',
                            _capture)
        t = sky.Task.from_yaml_str("""
resources:
  cpus: 4+
  ports: 8080
  any_of:
    - infra: aws/us-east-1
      accelerators: L4:1
      use_spot: true
      disk_tier: high
      image_id: docker:myrepo/model:v1
    - infra: aws/us-east-2
      accelerators: A10G:1
      use_spot: false
run: echo hi
""")
        spot_placer._get_possible_location_from_task(t)
        by_acc = {list(r.accelerators)[0]: r for r in captured}
        assert by_acc['L4'].disk_tier is not None
        assert by_acc['L4'].image_id is None
        assert by_acc['L4'].container_image is not None
        # The tier-less/image-less entry must not inherit the other
        # entry's disk_tier or container_image in the feasibility shape:
        # clouds that reject those attributes would silently drop the
        # location.
        assert by_acc['A10G'].disk_tier is None
        assert by_acc['A10G'].image_id is None
        assert by_acc['A10G'].container_image is None

    def test_pickle_roundtrip_and_backcompat(self):
        with mock.patch.object(spot_placer.registry.CLOUD_REGISTRY,
                               'from_str',
                               return_value=mock.MagicMock()):
            loc = spot_placer.Location.from_pickleable({
                'cloud': 'AWS',
                'region': 'us-east-1',
                'zone': None,
                'disk_tier': 'high',
            })
            assert loc.disk_tier == 'high'
            old = spot_placer.Location.from_pickleable({
                'cloud': 'AWS',
                'region': 'us-east-1',
                'zone': None,
            })
            assert old.disk_tier is None
            assert old.container_image is None

            with_image = spot_placer.Location.from_pickleable({
                'cloud': 'AWS',
                'region': 'us-east-1',
                'zone': None,
                'container_image': {
                    'ref': 'registry.example/model@sha256:' + 'a' * 64,
                    'distribution': 'managed',
                },
            })
            assert with_image.container_image is not None
            assert with_image.container_image.distribution == 'managed'
            assert (spot_placer.Location.from_pickleable(
                with_image.to_pickleable()) == with_image)
