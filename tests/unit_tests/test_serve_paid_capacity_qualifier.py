"""Hermetic contracts for the paid-provider qualification harness."""
# pylint: disable=protected-access

import asyncio
import dataclasses
import datetime
import hashlib
import importlib.util
import json
import os
import pathlib
import shlex
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

import pytest
from smoke_tests import smoke_tests_utils
import yaml

import sky
from sky.serve import replica_managers
from sky.serve import serve_utils


def _load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_FIXTURE_DIR = pathlib.Path(__file__).parents[1] / 'skyserve' / 'paid_capacity'
qualifier = _load_module('paid_capacity_qualifier', _FIXTURE_DIR / 'qualify.py')


def _instance(*,
              provisioning_model: str = 'SPOT',
              cluster_name: str = 'paid-e2e-1',
              suffix: str = '1234abcd') -> dict:
    return {
        'name': f'{cluster_name}-head-{suffix}-compute',
        'labels': {
            'ray-cluster-name': cluster_name,
        },
        'status': 'RUNNING',
        'zone': 'zones/us-central1-a',
        'machineType': 'machineTypes/g2-standard-4',
        'scheduling': {
            'provisioningModel': provisioning_model,
        },
        'guestAccelerators': [{
            'acceleratorType': 'acceleratorTypes/nvidia-l4',
            'acceleratorCount': 1,
        }],
    }


def _database_state(**overrides):
    values = {
        'service_hash': 'incarnation',
        'controller': qualifier.ControllerIdentity(
            pid=123,
            ip='10.0.0.1',
            owner_epoch=7,
            incarnation='12345678-1234-5678-9234-567812345678'),
        'paid_debit_units': 0,
        'claimed_units': 0,
        'claim_priorities': (),
        'waiter_count': 0,
        'demand_units': 0,
        'bound_cluster_zones': (),
    }
    values.update(overrides)
    return qualifier.DatabaseState(**values)


def _provider_state(**overrides):
    values = {
        'instance_count': 0,
        'running_count': 0,
        'disk_count': 0,
        'inflight_operation_count': 0,
        'cluster_names': frozenset(),
    }
    values.update(overrides)
    return qualifier.ProviderState(**values)


def _load_balancer_state(**overrides):
    values = {
        'service_hash': 'incarnation',
        'demand_units': 0,
        'ready_replicas': 0,
        'pod_uid': 'active-pod',
        'slot': 'a',
        'role_generation': 3,
    }
    values.update(overrides)
    return qualifier.LoadBalancerState(**values)


def _observation(observed_at: float = 1000,
                 observed_monotonic: float | None = None,
                 **overrides):
    values = {
        'database': _database_state(),
        'provider': _provider_state(),
        'load_balancer': _load_balancer_state(),
    }
    values.update(overrides)
    return qualifier.Observation(
        observed_at=observed_at,
        observed_monotonic=(observed_at if observed_monotonic is None else
                            observed_monotonic),
        **values)


def _request_telemetry(*,
                       queue_depth=0,
                       in_flight=0,
                       processing=0,
                       state_counts=None):
    summary = {
        'request_telemetry_observed_at': 1000.0,
        'request_telemetry_state': 'fresh',
        'request_telemetry_reason': 'complete',
        'request_telemetry_compatibility_complete': True,
        'request_queue_depth': queue_depth,
        'in_flight_requests': in_flight,
        'processing_requests': processing,
        'confirmed_in_flight_requests': in_flight,
        'confirmed_processing_requests': processing,
    }
    return qualifier.request_telemetry_from_summary(summary, state_counts or {})


def test_render_profiles_share_one_spot_only_service(tmp_path):
    source = _FIXTURE_DIR / 'service.yaml'
    for name, expected_units, expected_first_wave, expected_period in (
        ('small', 2, 2, 10),
        ('scale', 120, 120, 60),
    ):
        output = tmp_path / f'{name}.yaml'
        args = type('Args', (), {
            'profile': name,
            'source': str(source),
            'output': str(output),
        })()
        qualifier.render_service(args)
        config = yaml.safe_load(output.read_text(encoding='utf-8'))
        assert (config['service']['load_balancing_policy'] ==
                'instance_aware_least_load')
        policy = config['service']['replica_policy']
        resources = config['resources']
        assert policy['min_replicas'] == 0
        assert policy['max_replicas'] == expected_units
        assert policy['max_live_paid_gpu_units'] == expected_units
        assert policy['scale_up_rate_min_replicas'] == expected_first_wave
        assert policy['scale_up_rate_period_seconds'] == expected_period
        assert policy['spot_placer'] == 'dynamic_fallback_per_gpu'
        assert policy['cost_rebalance'] is False
        queue = config['service']['load_balancer']['request_queue']
        profile = qualifier.PROFILES[name]
        assert queue['min_size'] == profile.pressure_concurrency
        assert queue['max_size'] == max(32, profile.pressure_concurrency * 2)
        assert queue['max_concurrency'] == min(128,
                                               profile.pressure_concurrency)
        assert resources['use_spot'] is True
        assert resources['infra'] == 'gcp/us-central1'
        assert resources['instance_type'] == 'g2-standard-4'
        assert resources['accelerators'] == 'L4:1'
        assert 'workdir' not in config
        assert 'file_mounts' not in config
        assert 'server.py' not in config['run']
        assert "exec python3 - <<'PY'" in config['run']
        task = sky.Task.from_yaml_str(output.read_text(encoding='utf-8'))
        assert task.workdir is None
        serve_utils.validate_service_task(task, pool=False)


def test_render_rejects_paid_fixture_without_exact_accelerator_routing(
        tmp_path):
    config = yaml.safe_load(
        (_FIXTURE_DIR / 'service.yaml').read_text(encoding='utf-8'))
    del config['service']['load_balancing_policy']
    source = tmp_path / 'source.yaml'
    source.write_text(yaml.safe_dump(config, sort_keys=False), encoding='utf-8')
    args = type(
        'Args', (), {
            'profile': 'scale',
            'source': str(source),
            'output': str(tmp_path / 'rendered.yaml'),
        })()

    with pytest.raises(qualifier.QualificationError,
                       match='requires exact accelerator routing'):
        qualifier.render_service(args)


def test_provider_scope_comes_from_durable_version_not_ambient(
        monkeypatch, tmp_path):
    config_bytes = yaml.safe_dump({
        'active_workspace': 'workspace-a',
        'workspaces': {
            'workspace-a': {
                'gcp': {
                    'project_id': 'durable-project',
                },
            },
        },
    }).encode()
    monkeypatch.setattr(
        qualifier.serve_utils, 'parse_and_validate_version_controller_config',
        lambda contents, workspace, _source: yaml.safe_load(contents))
    monkeypatch.setenv('GOOGLE_CLOUD_PROJECT', 'wrong-ambient-project')
    authority = {
        'service_hash': 'incarnation',
        'service_lifecycle_epoch': 7,
        'current_version': 11,
        'workspace': 'workspace-a',
        'controller_config': config_bytes,
        'controller_config_digest': hashlib.sha256(config_bytes).hexdigest(),
        'controller_config_snapshot_id': 'a' * 64,
    }
    scope = qualifier.provider_scope_from_controller_config(
        authority, expected_region='us-central1')
    assert scope.project_id == 'durable-project'
    receipt = tmp_path / 'scope.json'
    qualifier.write_provider_scope(receipt, 'paid-e2e', scope)
    assert qualifier.read_provider_scope(receipt, 'paid-e2e') == scope


def test_gcp_observer_uses_compute_api_adc_and_paginates(monkeypatch):

    class Request:
        """One deterministic discovery API request."""

        def __init__(self, response):
            self._response = response

        def execute(self, *, num_retries):
            assert num_retries == qualifier._GCP_API_RETRIES
            return self._response

    class Collection:
        """Record explicit page tokens for one aggregated-list endpoint."""

        def __init__(self, pages):
            self._pages = pages
            self.calls = []

        def aggregatedList(self, **kwargs):  # pylint: disable=invalid-name
            self.calls.append(kwargs)
            return Request(self._pages[kwargs.get('pageToken')])

    instances = Collection({
        None: {
            'items': {
                'zones/us-central1-a': {
                    'instances': [{
                        'name': 'first'
                    }],
                },
            },
            'nextPageToken': 'next-instances',
        },
        'next-instances': {
            'items': {
                'zones/us-central1-b': {
                    'instances': [{
                        'name': 'second'
                    }],
                },
            },
        },
    })
    disks = Collection({
        None: {
            'items': {
                'zones/us-central1-a': {
                    'disks': [{
                        'name': 'disk'
                    }],
                },
            },
        },
    })
    operations = Collection({
        None: {
            'items': {
                'zones/us-central1-a': {
                    'operations': [{
                        'name': 'operation'
                    }],
                },
            },
        },
    })

    class Compute:

        def instances(self):
            return instances

        def disks(self):
            return disks

        def globalOperations(self):  # pylint: disable=invalid-name
            return operations

    builds = []

    def build(*args, **kwargs):
        builds.append((args, kwargs))
        return Compute()

    monkeypatch.setattr(qualifier.gcp_adaptor, 'build', build)
    scope = qualifier.ProviderScope(service_hash='incarnation',
                                    lifecycle_epoch=7,
                                    service_version=11,
                                    project_id='durable-project',
                                    workspace='workspace-a',
                                    region='us-central1',
                                    controller_config_digest='a' * 64,
                                    controller_config_snapshot_id='b' * 64)
    observer = qualifier.GcpObserver(service_name='paid-e2e',
                                     scope=scope,
                                     profile=qualifier.PROFILES['small'])
    census = observer.census()

    assert builds == [(('compute', 'v1'), {
        'credentials': None,
        'cache_discovery': False,
    })]
    assert census.instances == [{'name': 'first'}, {'name': 'second'}]
    assert census.disks == [{'name': 'disk'}]
    assert census.operations == [{'name': 'operation'}]
    assert instances.calls == [{
        'project': 'durable-project',
        'maxResults': qualifier._GCP_LIST_PAGE_SIZE,
        'returnPartialSuccess': True,
    }, {
        'project': 'durable-project',
        'maxResults': qualifier._GCP_LIST_PAGE_SIZE,
        'returnPartialSuccess': True,
        'pageToken': 'next-instances',
    }]


def test_gcp_observer_sanitizes_api_failures():

    class Request:

        def execute(self, **_kwargs):
            raise RuntimeError('credential-bearing-provider-error')

    class Collection:

        def aggregatedList(self, **_kwargs):  # pylint: disable=invalid-name
            return Request()

    class Compute:

        def instances(self):
            return Collection()

    scope = qualifier.ProviderScope(service_hash='incarnation',
                                    lifecycle_epoch=7,
                                    service_version=11,
                                    project_id='durable-project',
                                    workspace='workspace-a',
                                    region='us-central1',
                                    controller_config_digest='a' * 64,
                                    controller_config_snapshot_id='b' * 64)
    observer = qualifier.GcpObserver(service_name='paid-e2e',
                                     scope=scope,
                                     profile=qualifier.PROFILES['small'],
                                     compute=Compute())
    with pytest.raises(qualifier.QualificationError) as error:
        observer.census()
    assert str(error.value) == 'GCP Compute API instances census failed.'
    assert 'credential-bearing' not in str(error.value)


def test_provider_guard_rejects_on_demand_wrong_shape_and_overshoot():
    profile = qualifier.PROFILES['small']
    valid = _instance()
    state = qualifier.parse_gcp_state(
        service_name='paid-e2e',
        expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
        profile=profile,
        instances=[valid],
        disks=[],
        expected_region='us-central1')
    assert state.running_count == 1
    assert state.cluster_names == frozenset({'paid-e2e-1'})

    on_demand = {**valid, 'scheduling': {'provisioningModel': 'STANDARD'}}
    with pytest.raises(qualifier.QualificationError, match='not Spot'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
            profile=profile,
            instances=[on_demand],
            disks=[],
            expected_region='us-central1')

    wrong_shape = {**valid, 'machineType': 'machineTypes/g2-standard-8'}
    with pytest.raises(qualifier.QualificationError, match='wrong shape'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
            profile=profile,
            instances=[wrong_shape],
            disks=[],
            expected_region='us-central1')

    instances = [
        _instance(cluster_name=f'paid-e2e-{index}') for index in range(1, 4)
    ]
    with pytest.raises(qualifier.QualificationError, match='armed cap'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={
                f'paid-e2e-{index}': 'us-central1-a' for index in range(1, 4)
            },
            profile=profile,
            instances=instances,
            disks=[],
            expected_region='us-central1')


def test_provider_guard_ignores_preexisting_unrelated_resources():
    state = qualifier.parse_gcp_state(
        service_name='paid-e2e',
        expected_cluster_zones={'paid-e2e-1-tenanthash': 'us-central1-a'},
        profile=qualifier.PROFILES['small'],
        instances=[
            _instance(provisioning_model='STANDARD', cluster_name='unrelated-1')
        ],
        disks=[{
            'name': 'unrelated-1-head'
        }],
        expected_region='us-central1')
    assert state == qualifier.ProviderState(instance_count=0,
                                            running_count=0,
                                            disk_count=0,
                                            inflight_operation_count=0,
                                            cluster_names=frozenset())


def test_provider_guard_rejects_unbound_service_effects():
    with pytest.raises(qualifier.GuardViolation,
                       match='without a durable launch binding'):
        qualifier.parse_gcp_state(service_name='paid-e2e',
                                  expected_cluster_zones={},
                                  profile=qualifier.PROFILES['small'],
                                  instances=[_instance()],
                                  disks=[],
                                  expected_region='us-central1')


@pytest.mark.parametrize('labels', ({}, {'ray-cluster-name': 'paid-e2e-2'}))
def test_provider_guard_discovers_instance_with_missing_or_corrupt_label(
        labels):
    instance = _instance()
    instance['labels'] = labels
    with pytest.raises(qualifier.GuardViolation,
                       match='cluster metadata.*disagrees'):
        qualifier.parse_gcp_state(service_name='paid-e2e',
                                  expected_cluster_zones={
                                      'paid-e2e-1': 'us-central1-a',
                                      'paid-e2e-2': 'us-central1-a',
                                  },
                                  profile=qualifier.PROFILES['small'],
                                  instances=[instance],
                                  disks=[],
                                  expected_region='us-central1')


def test_provider_guard_rejects_duplicate_and_multiple_instance_effects():
    instance = _instance()
    with pytest.raises(qualifier.GuardViolation,
                       match='duplicate provider identity'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
            profile=qualifier.PROFILES['small'],
            instances=[instance, dict(instance)],
            disks=[],
            expected_region='us-central1')

    with pytest.raises(qualifier.GuardViolation,
                       match='multiple GCP instance effects'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
            profile=qualifier.PROFILES['small'],
            instances=[instance, _instance(suffix='87654321')],
            disks=[],
            expected_region='us-central1')


def test_provider_guard_discovers_orphan_disk_without_metadata():
    orphan = {
        'name': 'paid-e2e-1-head-deadbeef-compute',
        'zone': 'zones/us-central1-a',
        'labels': {},
    }
    with pytest.raises(qualifier.GuardViolation, match='metadata.*disagrees'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
            profile=qualifier.PROFILES['small'],
            instances=[],
            disks=[orphan],
            expected_region='us-central1')

    cleanup = qualifier.parse_gcp_cleanup_state(service_name='paid-e2e',
                                                instances=[],
                                                disks=[orphan],
                                                operations=[])
    assert cleanup.disk_count == 1

    labelled_orphan = {
        **orphan, 'labels': {
            'skypilot-managed': 'true',
            'ray-cluster-name': 'paid-e2e-1',
        }
    }
    with pytest.raises(qualifier.GuardViolation,
                       match='different GCP instance and disk identities'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
            profile=qualifier.PROFILES['small'],
            instances=[_instance()],
            disks=[labelled_orphan],
            expected_region='us-central1')


def test_provider_guard_rejects_multiple_disks_for_one_binding():

    def disk(suffix):
        return {
            'name': f'paid-e2e-1-head-{suffix}-compute',
            'zone': 'zones/us-central1-a',
            'labels': {
                'skypilot-managed': 'true',
                'ray-cluster-name': 'paid-e2e-1',
            },
        }

    with pytest.raises(qualifier.GuardViolation,
                       match='multiple GCP disk effects'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
            profile=qualifier.PROFILES['small'],
            instances=[],
            disks=[disk('1234abcd'), disk('87654321')],
            expected_region='us-central1')


def test_provider_guard_accepts_region_but_enforces_each_binding_zone():
    zone_b = {
        **_instance(cluster_name='paid-e2e-2'),
        'zone': 'zones/us-central1-b',
    }
    state = qualifier.parse_gcp_state(service_name='paid-e2e',
                                      expected_cluster_zones={
                                          'paid-e2e-1': 'us-central1-a',
                                          'paid-e2e-2': 'us-central1-b',
                                      },
                                      profile=qualifier.PROFILES['small'],
                                      instances=[_instance(), zone_b],
                                      disks=[],
                                      expected_region='us-central1')
    assert state.instance_count == 2

    with pytest.raises(qualifier.GuardViolation, match='binding zone'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-2': 'us-central1-a'},
            profile=qualifier.PROFILES['small'],
            instances=[zone_b],
            disks=[],
            expected_region='us-central1')


def test_cleanup_census_counts_unbound_scoped_provider_effects():
    state = qualifier.parse_gcp_cleanup_state(service_name='paid-e2e',
                                              instances=[_instance()],
                                              disks=[],
                                              operations=[])
    assert state.instance_count == 1
    assert state.running_count == 1


def test_cleanup_census_counts_instance_after_cluster_label_loss():
    instance = _instance()
    instance['labels'] = {}
    state = qualifier.parse_gcp_cleanup_state(service_name='paid-e2e',
                                              instances=[instance],
                                              disks=[],
                                              operations=[])
    assert state.instance_count == 1
    assert state.running_count == 1


def test_provider_census_uses_binding_derived_tenant_hashed_name():
    cloud_name = 'paid-e2e-1-tenanthash'
    generated_name = f'{cloud_name}-head-1234abcd-compute'
    state = qualifier.parse_gcp_state(
        service_name='paid-e2e',
        expected_cluster_zones={cloud_name: 'us-central1-a'},
        profile=qualifier.PROFILES['small'],
        instances=[
            _instance(cluster_name=cloud_name),
        ],
        disks=[{
            'name': generated_name,
            'zone': 'zones/us-central1-a',
            'labels': {
                'skypilot-managed': 'true',
                'ray-cluster-name': cloud_name,
            },
        }],
        operations=[{
            'operationType': 'insert',
            'status': 'RUNNING',
            'targetLink': ('https://compute.googleapis.com/compute/v1/projects/'
                           'project/zones/us-central1-a/instances/'
                           f'{generated_name}'),
        }],
        expected_region='us-central1')
    assert state.instance_count == 1
    assert state.disk_count == 1
    assert state.inflight_operation_count == 1
    assert state.cluster_names == frozenset({cloud_name})


def test_paid_debit_includes_failed_until_cleanup_is_proven():
    pool_payload = {
        'accelerators': [['l4', 1]],
        'cloud': 'gcp',
        'instance_type': 'g2-standard-4',
        'num_nodes': 1,
        'region': 'us-central1',
        'use_spot': True,
        'version': 1,
        'workspace': 'default',
        'zone': 'us-central1-a',
    }
    pool_key = json.dumps(pool_payload, sort_keys=True, separators=(',', ':'))
    failed = {
        'replica_id': 1,
        'paid_capacity_pool_key': pool_key,
        'sky_down_status': 'SCHEDULED',
        'replica_state': {
            'status_property': {
                'sky_down_status': 'SCHEDULED',
            },
            'paid_capacity_pool_key': pool_key,
            'is_zero_cost': False,
        },
    }
    census = qualifier.paid_debit_census([failed])
    assert census.gpu_units == 1
    assert census.replicas == (failed,)

    cleaned = {
        'replica_id': 1,
        'paid_capacity_pool_key': pool_key,
        'sky_down_status': 'SUCCEEDED',
        'replica_state': {
            'status_property': {
                'sky_down_status': 'SUCCEEDED',
            },
            'paid_capacity_pool_key': pool_key,
            'is_zero_cost': False,
        },
    }
    assert qualifier.paid_debit_census([cleaned]).gpu_units == 0


def test_paid_claim_requires_exact_offered_priority_and_plan():
    claim = {
        'priority': 50,
        'capacity_plan_generation': 9,
        'capacity_plan_sha256': 'a' * 64,
        'persisted_plan_sha256': 'a' * 64,
        'capacity_plan_accelerator': 'L4',
        'capacity_plan_units': 1,
    }
    census = qualifier.paid_claim_census([claim, dict(claim)])
    assert census.gpu_units == 2
    assert census.priorities == (50, 50)

    for invalid_priority in (49, 51, True, None):
        with pytest.raises(qualifier.GuardViolation, match='not priority 50'):
            qualifier.paid_claim_census([{
                **claim, 'priority': invalid_priority
            }])


def test_receipt_sample_records_exact_controller_owner_and_claim_priority(
        tmp_path):
    authority = {
        'controller_pid': 321,
        'controller_ip': '10.0.0.9',
        'controller_owner_epoch': 12,
        'controller_incarnation': 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee',
    }
    controller = qualifier.controller_identity_from_authority(authority)
    observation = _observation(database=_database_state(
        controller=controller, claimed_units=1, claim_priorities=(50,)))
    receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                service_name='paid-e2e',
                                profile=qualifier.PROFILES['small'])
    receipt.sample('scale', observation)

    assert receipt._payload['schema_version'] == 2
    assert receipt._payload['request_priority'] == 50
    assert receipt._payload['samples'] == [{
        'phase': 'scale',
        'observed_at': 1000,
        'controller_pid': 321,
        'controller_ip': '10.0.0.9',
        'controller_owner_epoch': 12,
        'controller_incarnation': 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee',
        'paid_debit_units': 0,
        'claimed_units': 1,
        'paid_claim_priorities': [50],
        'waiters': 0,
        'postgres_demand_units': 0,
        'provider_instances': 0,
        'provider_running': 0,
        'provider_disks': 0,
        'provider_inflight_operations': 0,
        'lb_demand_units': 0,
        'lb_ready_replicas': 0,
    }]

    with pytest.raises(qualifier.GuardViolation,
                       match='controller owner fence'):
        qualifier.controller_identity_from_authority({
            **authority, 'controller_owner_epoch': 0
        })


def test_route_report_uses_only_the_routed_active_lb():
    now = datetime.datetime.now(datetime.timezone.utc)
    active = {
        'reporter_session_id': 'active-process',
        'lb_session_id': 'active-pod',
        'lb_slot': 'a',
        'received_at': now,
        'complete': True,
        'payload': {
            'applied_role': 'ACTIVE',
            'applied_generation': 3,
            'queue_depth': 7,
        },
    }
    standby = {
        **active,
        'reporter_session_id': 'standby-process',
        'lb_session_id': 'standby-pod',
        'lb_slot': 'b',
        'payload': {
            'applied_role': 'ARMED',
            'applied_generation': 3,
            'queue_depth': 99,
        },
    }
    authority = {
        'lb_ha_enabled': 1,
        'lb_active_slot': 'a',
        'lb_cutover_generation': 3,
        'lb_cutover_phase': 'STABLE',
    }
    selected = qualifier.select_route_authoritative_report(
        authority, [standby, active], _load_balancer_state())
    assert selected is active
    assert qualifier.demand_units(selected['payload']) == 7

    incomplete = {**active, 'complete': False}
    with pytest.raises(qualifier.QualificationError,
                       match='ambiguous or incomplete'):
        qualifier.select_route_authoritative_report(authority, [incomplete],
                                                    _load_balancer_state())
    assert qualifier.select_route_authoritative_report(
        authority, [incomplete], _load_balancer_state(),
        require_complete=False) is incomplete

    with pytest.raises(qualifier.QualificationError,
                       match='does not match stable'):
        qualifier.select_route_authoritative_report(
            {
                **authority, 'lb_active_slot': 'b'
            }, [active], _load_balancer_state())


def test_data_plane_token_uses_projected_ring_without_exposing_it(monkeypatch):
    monkeypatch.delenv('SKYPILOT_SERVE_E2E_AUTH_TOKEN', raising=False)
    monkeypatch.setattr(qualifier.auth_tokens, 'get_lb_auth_tokens',
                        lambda required: ('projected-secret',))
    assert qualifier.resolve_data_plane_token(
        'SKYPILOT_SERVE_E2E_AUTH_TOKEN') == 'projected-secret'


def test_request_telemetry_uses_observer_postgres_engine(monkeypatch):
    observer = object.__new__(qualifier.PostgresObserver)
    observer._engine = object()
    observer._service_name = 'paid-e2e'
    observer._provider_scope = qualifier.ProviderScope(
        service_hash='service-hash',
        lifecycle_epoch=1,
        service_version=1,
        workspace='workspace-a',
        project_id='project-a',
        region='us-central1',
        controller_config_digest='a' * 64,
        controller_config_snapshot_id='b' * 64)
    seen = {}

    def get_request_summary(service_name, service_hash, *, engine):
        seen.update(service_name=service_name,
                    service_hash=service_hash,
                    engine=engine)
        return {
            'request_telemetry_observed_at': 1000.0,
            'request_telemetry_state': 'fresh',
            'request_telemetry_reason': 'complete',
            'request_telemetry_compatibility_complete': True,
            'request_queue_depth': 0,
            'in_flight_requests': 0,
            'processing_requests': 0,
            'confirmed_in_flight_requests': 0,
            'confirmed_processing_requests': 0,
        }

    monkeypatch.setattr(qualifier.demand_state, 'get_request_summary',
                        get_request_summary)

    def get_ledger_summary(*_args, **kwargs):
        seen['ledger_engine'] = kwargs['engine']
        return {
            'available': True,
            'service_hash': 'service-hash',
            'state_counts': {},
        }

    monkeypatch.setattr(qualifier.async_request_ledger, 'get_summary',
                        get_ledger_summary)

    telemetry = observer.request_telemetry()

    assert seen == {
        'service_name': 'paid-e2e',
        'service_hash': 'service-hash',
        'engine': observer._engine,
        'ledger_engine': observer._engine,
    }
    assert telemetry.is_exact_zero()


def test_cleanup_command_preserves_primary_failure_and_still_cleans(tmp_path):
    marker = tmp_path / 'cleanup-ran'
    command = smoke_tests_utils.command_with_cleanup(
        'exit 23', f'touch {shlex.quote(str(marker))}')
    result = subprocess.run(command,
                            shell=True,
                            executable='/bin/bash',
                            check=False)
    assert result.returncode == 23
    assert marker.exists()

    cleanup_failure = smoke_tests_utils.command_with_cleanup(
        'exit 0', 'exit 29')
    result = subprocess.run(cleanup_failure,
                            shell=True,
                            executable='/bin/bash',
                            check=False)
    assert result.returncode == 29


def test_replica_binding_selection_accepts_retry_history():
    replica = {
        'replica_id': 7,
        'replica_record_id': 'record-1',
        'ordinary_launch_association_id': None,
    }
    terminal_retry = {
        'association_id': 'association-1',
        'replica_id': 7,
        'replica_record_id': 'record-1',
        'launch_generation': 1,
        'resolution': 'PRE_EFFECT_TERMINAL',
        'reconciliation_outcome': 'PRE_EFFECT_TERMINAL',
        'projected_at': object(),
        'service_job_id': None,
    }
    successful_retry = {
        'association_id': 'association-2',
        'replica_id': 7,
        'replica_record_id': 'record-1',
        'launch_generation': 2,
        'resolution': 'PROJECTED',
        'reconciliation_outcome': 'PROJECTED',
        'projected_at': object(),
        'service_job_id': 42,
    }
    assert qualifier.select_replica_binding(
        replica, [terminal_retry, successful_retry]) is successful_retry

    unresolved = {
        **terminal_retry,
        'association_id': 'association-3',
        'launch_generation': 3,
        'resolution': 'BOUND',
        'reconciliation_outcome': None,
        'projected_at': None,
    }
    pointed_replica = {
        **replica,
        'ordinary_launch_association_id': 'association-3',
    }
    assert qualifier.select_replica_binding(
        pointed_replica,
        [terminal_retry, successful_retry, unresolved]) is unresolved


def test_replica_binding_selection_accepts_settled_failed_provider_absence():
    """A rejected Spot create retains exact evidence but has no service job."""
    association_id = uuid.UUID('11111111-1111-4111-8111-111111111111')
    replica_record_id = uuid.UUID('22222222-2222-4222-8222-222222222222')
    profile = qualifier.ordinary_launch_binding.NonPoolLaunchProfile.create(
        qualifier.ordinary_launch_binding.NonPoolLaunchProfileKind.
        ORDINARY_PAID,
        authorization_reference='paid-capacity:incarnation:record-1:pool',
        authorization_generation=1,
        authorization_payload={'capacity_plan_generation': 1})
    pool_key = json.dumps(
        {
            'accelerators': [['l4', 1]],
            'cloud': 'gcp',
            'instance_type': 'g2-standard-4',
            'num_nodes': 1,
            'region': 'us-central1',
            'use_spot': True,
            'version': 1,
            'workspace': 'workspace-a',
            'zone': 'us-central1-a',
        },
        sort_keys=True,
        separators=(',', ':'))
    quiesced_at = datetime.datetime(2026,
                                    9,
                                    1,
                                    1,
                                    0,
                                    tzinfo=datetime.timezone.utc)
    binding = {
        'association_id': association_id,
        'request_id': 'request-1',
        'service_name': 'paid-e2e',
        'replica_id': 7,
        'replica_record_id': replica_record_id,
        'launch_generation': 1,
        'input_digest': 'a' * 64,
        'cluster_name': 'paid-e2e-7',
        'tenant_scope': 'tenant-a',
        'paid_capacity_pool_key': pool_key,
        'effect_phase': 'PROVIDER_IO',
        'resolution': 'PROJECTED',
        'terminal_status': 'FAILED',
        'terminal_cause': 'handler_failed',
        'terminal_execution_generation': 1,
        'execution_quiescence_required': True,
        'execution_quiesced_generation': 1,
        'execution_quiesced_at': quiesced_at,
        'service_job_id': None,
        'result_recorded_at': None,
        'ambiguity_code': None,
        'projected_at': quiesced_at,
        'pin_released_at': quiesced_at,
        'tombstone_not_before': quiesced_at + datetime.timedelta(days=60),
        'binding_protocol_version': 2,
        'profile_kind': profile.kind.value,
        'profile_version': profile.version,
        'profile_digest': profile.digest,
        'capability_cohort_epoch':
            (qualifier.ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH
            ),
        'capability_profile_set_digest':
            (qualifier.ordinary_launch_binding.
             supported_non_pool_profile_set_digest()),
        'receipt_protocol_version': 1,
        'authorization_kind': profile.authorization_kind.value,
        'authorization_reference': profile.authorization_reference,
        'authorization_generation': profile.authorization_generation,
        'authorization_digest': profile.authorization_digest,
        'reconciliation_outcome': 'PROJECTED',
        'provider_evidence': 'ABSENT',
        'provider_evidence_observed_at':
            (quiesced_at + datetime.timedelta(seconds=1)),
    }
    provider_identity = (
        qualifier.ordinary_launch_binding.ordinary_paid_gcp_provider_identity(
            binding, project_id='test-project'))
    binding['provider_evidence_payload'] = {
        'association_id': str(association_id),
        'cluster_name': 'paid-e2e-7',
        'create_operation_targets': {
            'failed': [],
            'inflight': [],
            'succeeded': [],
        },
        'disk_ids': [],
        'instance_ids': [],
        'probe_contract': 'gcp-vm-disk-operation-presence-v1',
        'profile_kind': 'ORDINARY_PAID',
        'provider_identity': provider_identity,
        'replica_record_id': str(replica_record_id),
        'result': 'ABSENT',
    }
    _, binding['provider_evidence_digest'] = (
        qualifier.ordinary_launch_binding._ordinary_paid_provider_evidence(
            binding, binding['cluster_name'],
            qualifier.ordinary_launch_binding.ProviderEvidence.ABSENT))
    replica = {
        'replica_id': 7,
        'replica_record_id': str(replica_record_id),
        'ordinary_launch_association_id': None,
    }

    assert qualifier.select_replica_binding(replica, [binding]) is binding

    malformed = {
        **binding,
        'resolution': 'AMBIGUOUS',
        'reconciliation_outcome': 'POST_EFFECT_AMBIGUOUS',
        'provider_evidence': 'UNKNOWN',
        'ambiguity_code': 'provider_state_unknown',
    }
    assert not qualifier._is_selectable_settled_paid_binding(malformed)
    with pytest.raises(qualifier.GuardViolation,
                       match='unique current or latest settled'):
        qualifier.select_replica_binding(replica, [malformed])


def test_exact_provider_free_unbound_paid_debit_is_an_observation_miss(
        tmp_path):
    record_id = '22222222-2222-4222-8222-222222222222'
    pool_key = 'exact-gcp-spot-pool'
    info = replica_managers.ReplicaInfo(replica_id=7,
                                        cluster_name='paid-e2e-7',
                                        replica_port='8000',
                                        is_spot=True,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    info.replica_record_id = record_id
    info.paid_capacity_pool_key = pool_key
    replica = {
        'replica_id': 7,
        'replica_state_version': 1,
        'replica_record_id': record_id,
        'ordinary_launch_association_id': None,
        'status': info.status.value,
        'is_spot': True,
        'paid_capacity_pool_key': pool_key,
        'version': 1,
        'cluster_name': 'paid-e2e-7',
        'replica_state': info.to_storage_dict(),
    }
    claim = {
        'replica_id': 7,
        'pool_key': pool_key,
    }
    assert qualifier._is_exact_provider_free_unbound_paid_debit(
        replica, claim, [])
    assert not qualifier._is_exact_provider_free_unbound_paid_debit(
        replica, {
            **claim, 'pool_key': 'another-pool'
        }, [])
    profile = qualifier.ordinary_launch_binding.NonPoolLaunchProfile.create(
        qualifier.ordinary_launch_binding.NonPoolLaunchProfileKind.
        ORDINARY_PAID,
        authorization_reference=(
            f'paid-capacity:incarnation:{record_id}:{pool_key}'),
        authorization_generation=1,
        authorization_payload={'capacity_plan_generation': 1})
    quiesced_at = datetime.datetime(2026,
                                    9,
                                    1,
                                    1,
                                    0,
                                    tzinfo=datetime.timezone.utc)
    predecessor = {
        'association_id': uuid.UUID('11111111-1111-4111-8111-111111111111'),
        'request_id': 'request-1',
        'service_name': 'paid-e2e',
        'service_hash': 'incarnation',
        'service_workspace': 'mt-hybrid',
        'service_lifecycle_epoch': 1,
        'service_binding_epoch': 1,
        'service_version': 1,
        'replica_id': 7,
        'replica_record_id': record_id,
        'launch_generation': 1,
        'input_digest': 'a' * 64,
        'cluster_name': 'paid-e2e-7',
        'tenant_scope': 'tenant-a',
        'paid_capacity_pool_key': pool_key,
        'effect_phase': 'NOT_STARTED',
        'resolution': 'PRE_EFFECT_TERMINAL',
        'terminal_status': 'FAILED',
        'terminal_cause': 'request_never_executed',
        'terminal_execution_generation': 1,
        'execution_quiescence_required': True,
        'execution_quiesced_generation': 1,
        'execution_quiesced_at': quiesced_at,
        'service_job_id': None,
        'result_recorded_at': None,
        'ambiguity_code': None,
        'projected_at': quiesced_at,
        'pin_released_at': quiesced_at,
        'tombstone_not_before': quiesced_at + datetime.timedelta(days=60),
        'cancel_reason': None,
        'cancel_requested_at': None,
        'binding_protocol_version': 2,
        'profile_kind': profile.kind.value,
        'profile_version': profile.version,
        'profile_digest': profile.digest,
        'capability_cohort_epoch':
            (qualifier.ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH
            ),
        'capability_profile_set_digest':
            (qualifier.ordinary_launch_binding.
             supported_non_pool_profile_set_digest()),
        'receipt_protocol_version': 1,
        'authorization_kind': profile.authorization_kind.value,
        'authorization_reference': profile.authorization_reference,
        'authorization_generation': profile.authorization_generation,
        'authorization_digest': profile.authorization_digest,
        'reconciliation_outcome': 'PRE_EFFECT_TERMINAL',
        'provider_evidence': 'NOT_QUERIED',
        'provider_evidence_observed_at': None,
        'provider_evidence_payload': None,
        'provider_evidence_digest': None,
    }
    assert (qualifier.ordinary_launch_binding.
            settled_association_proves_execution_quiescence(predecessor))
    assert qualifier._is_exact_provider_free_unbound_paid_debit(
        replica, None, [predecessor])
    assert not qualifier._is_exact_provider_free_unbound_paid_debit(
        replica, claim, [predecessor])
    assert qualifier._is_exact_provider_free_unbound_paid_debit(
        {
            **replica,
            'status': 'FAILED_CLEANUP',
            'replica_state': {
                **replica['replica_state'],
                'status_property': {
                    **replica['replica_state']['status_property'],
                    'sky_launch_status': 'RUNNING',
                    'sky_down_status': 'FAILED',
                },
            },
        }, None, [predecessor])
    assert not qualifier._is_exact_provider_free_unbound_paid_debit(
        replica, None, [])
    assert not qualifier._is_exact_provider_free_unbound_paid_debit(
        replica, None, [{
            **predecessor,
            'paid_capacity_pool_key': 'another-pool',
        }])
    assert not qualifier._is_exact_provider_free_unbound_paid_debit(
        replica, None, [{
            **predecessor,
            'resolution': 'AMBIGUOUS',
            'reconciliation_outcome': 'POST_EFFECT_AMBIGUOUS',
        }])
    assert not qualifier._is_exact_provider_free_unbound_paid_debit(
        replica, None, [{
            **predecessor,
            'execution_quiesced_at': None,
        }])
    assert not qualifier._is_exact_provider_free_unbound_paid_debit(
        replica, None, [{
            **predecessor,
            'launch_generation': 2,
        }])
    assert not qualifier._is_exact_provider_free_unbound_paid_debit(
        replica, None, [{
            **predecessor,
            'cancel_reason': 'explicit_cancel',
            'cancel_requested_at': quiesced_at,
        }])
    assert not qualifier._is_exact_provider_free_unbound_paid_debit(
        replica, None,
        [predecessor, {
            **predecessor,
            'association_id': 'association-2',
        }])

    phase_a = _observation(database=_database_state(
        paid_debit_units=1,
        claimed_units=1,
        claim_priorities=(50,),
        demand_units=4,
        provider_free_unbound_replica_ids=(7,)),
                           load_balancer=_load_balancer_state(demand_units=4))

    class Observer:

        async def snapshot(self, *, require_complete_demand_report=True):
            assert not require_complete_demand_report
            return phase_a

    receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                service_name='paid-e2e',
                                profile=qualifier.PROFILES['scale'])
    observed = asyncio.run(
        qualifier._validated_sample(observer=Observer(),
                                    profile=qualifier.PROFILES['scale'],
                                    progress=qualifier.Progress(),
                                    receipt=receipt,
                                    phase='scale'))
    assert observed is None
    assert receipt._payload['samples'][-1]['observation_error_type'] == (
        'QualificationError')


def test_phase_a_observation_cannot_hide_a_provider_effect(tmp_path):
    phase_a_with_effect = _observation(
        database=_database_state(paid_debit_units=1,
                                 claimed_units=1,
                                 claim_priorities=(50,),
                                 demand_units=4,
                                 provider_free_unbound_replica_ids=(7,)),
        provider=_provider_state(instance_count=1,
                                 cluster_names=frozenset({'paid-e2e-7'})),
        load_balancer=_load_balancer_state(demand_units=4))

    class Observer:

        async def snapshot(self, *, require_complete_demand_report=True):
            assert not require_complete_demand_report
            return phase_a_with_effect

    receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                service_name='paid-e2e',
                                profile=qualifier.PROFILES['scale'])
    with pytest.raises(qualifier.GuardViolation,
                       match='durable launch binding'):
        asyncio.run(
            qualifier._validated_sample(observer=Observer(),
                                        profile=qualifier.PROFILES['scale'],
                                        progress=qualifier.Progress(),
                                        receipt=receipt,
                                        phase='scale'))


@pytest.mark.parametrize('phase', ['baseline', 'drain'])
def test_zero_gates_require_complete_demand_reports(tmp_path, phase):
    observation = _observation()

    class Observer:

        async def snapshot(self, *, require_complete_demand_report=True):
            assert require_complete_demand_report
            return observation

    receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                service_name='paid-e2e',
                                profile=qualifier.PROFILES['scale'])
    observed = asyncio.run(
        qualifier._validated_sample(observer=Observer(),
                                    profile=qualifier.PROFILES['scale'],
                                    progress=qualifier.Progress(),
                                    receipt=receipt,
                                    phase=phase))
    assert observed is observation
    assert receipt._payload['samples'][-1]['phase'] == phase


def test_demand_projection_is_zero_sensitive():
    assert qualifier.demand_units({
        'queue_depth': 0,
        'http_in_flight': {},
        'async_occupancy': {
            'http://replica': 0,
        },
        'unique_job_arrivals_300s': 0,
    }) == 0
    assert qualifier.demand_units({
        'queue_depth': 2,
        'http_in_flight': {
            'http://replica': 1,
        },
    }) == 3


def test_retained_binding_allows_provider_effect_after_claim_release():
    profile = qualifier.PROFILES['small']
    unbound = _observation(database=_database_state(paid_debit_units=1),
                           provider=_provider_state(instance_count=1,
                                                    running_count=1,
                                                    cluster_names=frozenset(
                                                        {'paid-e2e-1'})))
    with pytest.raises(qualifier.QualificationError,
                       match='durable launch bindings'):
        qualifier.validate_observation(unbound, profile)

    bound = dataclasses.replace(
        unbound,
        database=dataclasses.replace(
            unbound.database,
            # A successful provider request releases its admission claim.  A
            # retained immutable binding, not a current claim or plan head,
            # remains the proof for the live provider effect.
            claimed_units=0,
            bound_cluster_zones=(('paid-e2e-1', 'us-central1-a'),)))
    qualifier.validate_observation(bound, profile)


def test_route_authority_must_be_fresh_and_match_lifecycle():
    authority = {
        'service_hash': 'incarnation',
        'service_lifecycle_epoch': 7,
        'route_generation': 11,
        'route_fresh': True,
        'route_service_hash': 'incarnation',
        'route_lifecycle_epoch': 7,
    }
    assert qualifier.validate_route_authority(authority) == ('incarnation', 7)
    with pytest.raises(qualifier.QualificationError,
                       match='fresh route/lifecycle'):
        qualifier.validate_route_authority({**authority, 'route_fresh': False})
    with pytest.raises(qualifier.QualificationError,
                       match='fresh route/lifecycle'):
        qualifier.validate_route_authority({
            **authority, 'route_lifecycle_epoch': 6
        })


def test_progress_requires_scale_slo_and_sustained_exact_zero():
    profile = qualifier.PROFILES['small']
    scaled = _observation(
        observed_at=1000,
        database=_database_state(paid_debit_units=2,
                                 bound_cluster_zones=(('paid-e2e-1',
                                                       'us-central1-a'),
                                                      ('paid-e2e-2',
                                                       'us-central1-a'))),
        provider=_provider_state(instance_count=2,
                                 running_count=2,
                                 disk_count=2,
                                 cluster_names=frozenset(
                                     {'paid-e2e-1', 'paid-e2e-2'})),
        load_balancer=_load_balancer_state(demand_units=4, ready_replicas=2))
    qualifier.validate_observation(scaled, profile)
    progress = qualifier.Progress(scale_started_monotonic=900)
    progress.observe(scaled, profile)
    assert progress.scale_reached_monotonic == 1000

    zero = _observation(observed_at=1100)
    for observed_at in (1100, 1105, 1461):
        zero = _observation(observed_at=observed_at)
        progress.observe(zero, profile)
        progress.observe_zero(zero)
    assert progress.drain_complete(zero, profile)

    # Wall-clock movement cannot make the elapsed-time gate pass or fail.
    too_slow = dataclasses.replace(scaled,
                                   observed_at=800,
                                   observed_monotonic=2000)
    with pytest.raises(qualifier.QualificationError, match='Scale-out took'):
        qualifier.Progress(scale_started_monotonic=1000).observe(
            too_slow, profile)


def test_scale_survives_transient_observer_blackout(tmp_path):
    """Model pressure surviving observer loss from 20 to 64 VMs."""
    cloud_names = frozenset(f'paid-e2e-{index:03d}' for index in range(64))
    profile = dataclasses.replace(qualifier.PROFILES['scale'],
                                  minimum_running=64,
                                  poll_seconds=0,
                                  scale_timeout_seconds=2)
    database = _database_state(
        paid_debit_units=64,
        claimed_units=0,
        demand_units=64,
        bound_cluster_zones=tuple(
            (name, 'us-central1-a') for name in cloud_names))
    now = qualifier.time.time()
    observations = [
        _observation(observed_at=now + 1,
                     database=database,
                     provider=_provider_state(instance_count=20,
                                              running_count=20,
                                              disk_count=20,
                                              cluster_names=frozenset(
                                                  sorted(cloud_names)[:20])),
                     load_balancer=_load_balancer_state(demand_units=64)),
        qualifier.QualificationError('transient observer blackout'),
        _observation(observed_at=now + 2,
                     database=database,
                     provider=_provider_state(instance_count=64,
                                              running_count=64,
                                              disk_count=64,
                                              cluster_names=cloud_names),
                     load_balancer=_load_balancer_state(demand_units=64,
                                                        ready_replicas=20)),
    ]

    class Observer:

        async def snapshot(self, *, require_complete_demand_report=True):
            assert not require_complete_demand_report
            result = observations.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

    async def exercise():
        progress = qualifier.Progress(scale_started_monotonic=now)
        receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                    service_name='paid-e2e',
                                    profile=profile)
        keep_alive = asyncio.Event()
        pressure = asyncio.create_task(keep_alive.wait())
        try:
            await qualifier._wait_for_scale(observer=Observer(),
                                            profile=profile,
                                            progress=progress,
                                            receipt=receipt,
                                            pressure=pressure)
            return progress, receipt
        finally:
            pressure.cancel()
            await asyncio.gather(pressure, return_exceptions=True)

    progress, receipt = asyncio.run(exercise())
    assert progress.peak_running == 64
    assert progress.scale_reached_monotonic == now + 2
    assert [
        sample.get('observation_error_type')
        for sample in receipt._payload['samples']
    ] == [None, 'QualificationError', None]


def test_pressure_remains_continuous_until_scale_converges(monkeypatch):
    calls = []

    async def exercise():
        stop = asyncio.Event()

        async def fake_request(_session, **kwargs):
            calls.append(kwargs['request_id'])
            if len(calls) >= 8:
                stop.set()
            await asyncio.sleep(0)

        monkeypatch.setattr(qualifier, '_one_request', fake_request)
        return await qualifier.send_continuous_pressure(
            endpoint='http://unused.test',
            token='secret',
            prefix='pressure',
            concurrency=2,
            duration_seconds=30,
            timeout_seconds=5,
            stop=stop)

    successes = asyncio.run(exercise())
    assert successes >= 8
    assert len(calls) == len(set(calls))
    assert any(request_id.endswith('000002') for request_id in calls)


def test_one_request_retries_capacity_failure_with_stable_identity():

    class Response:
        """Minimal asynchronous HTTP response double."""

        def __init__(self, status, body, headers=None):
            self.status = status
            self._body = body
            self.headers = headers or {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def read(self):
            return self._body

    class Session:
        """Return a capacity failure followed by a successful retry."""

        def __init__(self):
            self.requests = []
            self._responses = [
                Response(503, b'', {'Retry-After': '0.1'}),
                Response(
                    200,
                    json.dumps({
                        'request_id': 'stable-id',
                        'status': 'ok',
                    }).encode()),
            ]

        def post(self, _url, *, headers, json):  # pylint: disable=redefined-outer-name
            self.requests.append((headers, json))
            return self._responses.pop(0)

    session = Session()
    asyncio.run(
        qualifier._one_request(session,
                               url='http://unused.test/predict',
                               token='secret',
                               request_id='stable-id',
                               duration_seconds=30,
                               deadline=qualifier.time.monotonic() + 2))
    assert len(session.requests) == 2
    assert {body['request_id'] for _, body in session.requests} == {'stable-id'}


def test_one_request_rejects_identity_only_acknowledgement():

    class Response:
        """Return an identity-bearing acknowledgement, not a completion."""

        status = 200
        headers = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def read(self):
            return json.dumps({
                'request_id': 'stable-id',
                'status': 'accepted',
            }).encode()

    class Session:
        """Minimal session returning the incomplete acknowledgement."""

        def post(self, _url, **_kwargs):
            return Response()

    with pytest.raises(qualifier.QualificationError,
                       match='did not report completed processing'):
        asyncio.run(
            qualifier._one_request(Session(),
                                   url='http://unused.test/predict',
                                   token='secret',
                                   request_id='stable-id',
                                   duration_seconds=30,
                                   deadline=qualifier.time.monotonic() + 2))


def test_exact_async_request_uses_canonical_acceptance_and_completion():
    attempt_id = '11111111-1111-4111-8111-111111111111'

    class Response:
        """Minimal exact protocol response."""

        def __init__(self, status, body, state, revision):
            self.status = status
            self._body = body
            self.headers = {
                'X-SkyServe-Async-Ledger-Protocol': '1',
                'X-SkyServe-Service-Incarnation': 'incarnation-a',
                'X-SkyServe-Async-Attempt-Id': attempt_id,
                'X-SkyServe-Async-Attempt-No': '1',
                'X-SkyServe-Async-Ledger-Revision': str(revision),
                'X-SkyServe-Async-Ledger-State': state,
            }

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def read(self):
            return self._body

    class Session:
        """Return one durable ACCEPTED receipt and its terminal callback."""

        def __init__(self):
            self.calls = []
            self.responses = [
                Response(
                    202,
                    json.dumps({
                        'request_id': 'execution-1',
                        'status': 'accepted',
                    }).encode(), 'ACCEPTED', 2),
                Response(204, b'', 'SUCCEEDED', 3),
            ]

        def post(self, url, **kwargs):
            self.calls.append((url, kwargs))
            return self.responses.pop(0)

    session = Session()
    asyncio.run(
        qualifier._one_exact_async_request(session,
                                           endpoint='https://service.test',
                                           token='secret',
                                           service_hash='incarnation-a',
                                           request_id='execution-1',
                                           stable_job_id='job-1',
                                           duration_seconds=0,
                                           deadline=qualifier.time.monotonic() +
                                           2))

    assert len(session.calls) == 2
    submit_url, submit = session.calls[0]
    assert submit_url.endswith('/v1/models/model:predict')
    assert submit['data'] == qualifier.rfc8785.dumps({
        'action': 'async_predict',
        'payload': {
            'duration_seconds': 0,
        },
        'request_id': 'execution-1',
    })
    assert submit['headers']['X-SkyServe-Job-Id'] == 'job-1'
    callback_url, callback = session.calls[1]
    assert callback_url.endswith('/_lb/prediction-completed')
    assert callback['json']['attempt_id'] == attempt_id
    assert callback['json']['expected_revision'] == 2
    assert callback['json']['status'] == 'SUCCEEDED'


def test_exact_async_request_never_replays_ambiguous_submission():

    class Session:

        def __init__(self):
            self.calls = 0

        def post(self, _url, **_kwargs):
            self.calls += 1
            raise qualifier.aiohttp.ClientConnectionError('lost response')

    session = Session()
    with pytest.raises(qualifier.QualificationError,
                       match='lost its exact admission response'):
        asyncio.run(
            qualifier._submit_exact_async_request(
                session,
                endpoint='https://service.test',
                token='secret',
                service_hash='incarnation-a',
                request_id='execution-1',
                stable_job_id='job-1',
                duration_seconds=0,
                deadline=qualifier.time.monotonic() + 2))
    assert session.calls == 1


def test_request_telemetry_requires_exact_positive_and_terminal_delta(tmp_path):
    baseline = _request_telemetry()
    positive = _request_telemetry(queue_depth=7,
                                  in_flight=5,
                                  processing=3,
                                  state_counts={'ACCEPTED': 3})
    final = _request_telemetry(state_counts={'SUCCEEDED': 16})
    assert baseline.is_exact_zero()
    assert positive.is_fresh_complete()
    assert not positive.is_exact_zero()
    assert final.is_exact_zero()
    assert final.ledger_succeeded - baseline.ledger_succeeded == 16

    class Observer:

        def __init__(self, values):
            self.values = list(values)

        async def request_telemetry(self):
            return self.values.pop(0)

    async def exercise():
        profile = dataclasses.replace(qualifier.PROFILES['small'],
                                      poll_seconds=0)
        receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                    service_name='paid-e2e',
                                    profile=profile)
        held = asyncio.create_task(asyncio.Event().wait())
        try:
            observed_positive = await (
                qualifier._wait_for_positive_request_telemetry(
                    observer=Observer([positive]),
                    profile=profile,
                    receipt=receipt,
                    traffic=held,
                    baseline=baseline))
            observed_final = await qualifier._wait_for_final_request_telemetry(
                observer=Observer([final]),
                profile=profile,
                receipt=receipt,
                baseline=baseline,
                expected_succeeded_delta=16)
            return observed_positive, observed_final, receipt
        finally:
            held.cancel()
            await asyncio.gather(held, return_exceptions=True)

    observed_positive, observed_final, receipt = asyncio.run(exercise())
    assert observed_positive == positive
    assert observed_final == final
    assert [
        sample['phase']
        for sample in receipt._payload['request_telemetry_samples']
    ] == ['positive', 'final']


def test_worker_exposes_health_occupancy_and_stable_identity():
    config = yaml.safe_load(
        (_FIXTURE_DIR / 'service.yaml').read_text(encoding='utf-8'))
    with socket.socket() as port_socket:
        port_socket.bind(('127.0.0.1', 0))
        port = port_socket.getsockname()[1]
    process = subprocess.Popen(['bash', '-c', config['run']],
                               env={
                                   **os.environ, 'PORT': str(port)
                               },
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE,
                               text=True)
    endpoint = f'http://127.0.0.1:{port}'
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                with urllib.request.urlopen(f'{endpoint}/health',
                                            timeout=1) as response:
                    assert json.load(response) == {'status': 'ok'}
                break
            except urllib.error.URLError:
                if process.poll() is not None:
                    _, stderr = process.communicate(timeout=1)
                    pytest.fail(f'Inline worker exited early: {stderr}')
                if time.monotonic() >= deadline:
                    pytest.fail('Inline worker did not become healthy.')
                time.sleep(0.05)
        capacity_request = urllib.request.Request(
            f'{endpoint}/v1/models/model:predict',
            data=json.dumps({
                'action': 'async_capacity'
            }).encode(),
            headers={'Content-Type': 'application/json'},
            method='POST')
        with urllib.request.urlopen(capacity_request, timeout=2) as response:
            capacity = json.load(response)
        assert capacity['running_count'] == 0
        assert capacity['predict_concurrency'] == 1
        work_request = urllib.request.Request(
            f'{endpoint}/v1/models/model:predict',
            data=json.dumps({
                'request_id': 'stable-1',
                'duration_seconds': 0,
            }).encode(),
            headers={'Content-Type': 'application/json'},
            method='POST')
        with urllib.request.urlopen(work_request, timeout=2) as response:
            assert json.load(response) == {
                'request_id': 'stable-1',
                'status': 'ok',
            }
        exact_body, exact_intent = qualifier._canonical_exact_request(
            'exact-execution-1', 0.5)
        exact_request = urllib.request.Request(
            f'{endpoint}/v1/models/model:predict',
            data=exact_body,
            headers={
                'Content-Type': 'application/json',
                'X-SkyServe-Async-Ledger-Protocol': '1',
                'X-SkyServe-Service-Incarnation': 'incarnation-a',
                'X-SkyServe-Async-Intent-Sha256': exact_intent,
                'X-SkyServe-Execution-Request-Id': 'exact-execution-1',
                'X-SkyServe-Async-Attempt-Id': '11111111-1111-4111-8111-111111111111',
                'X-SkyServe-Async-Attempt-No': '1',
                'X-SkyServe-Async-Ledger-Revision': '1',
            },
            method='POST')
        with urllib.request.urlopen(exact_request, timeout=2) as response:
            assert response.status == 202
            assert json.load(response) == {
                'request_id': 'exact-execution-1',
                'status': 'accepted',
            }
        with urllib.request.urlopen(capacity_request, timeout=2) as response:
            assert json.load(response)['running_count'] == 1
        time.sleep(0.6)
        with urllib.request.urlopen(capacity_request, timeout=2) as response:
            assert json.load(response)['running_count'] == 0
    finally:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
