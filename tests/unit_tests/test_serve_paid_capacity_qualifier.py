"""Hermetic contracts for the paid-provider qualification harness."""
# pylint: disable=missing-class-docstring,protected-access

import asyncio
import copy
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
import threading
import time
import types
import typing
from typing import Any
import urllib.error
import urllib.request
import uuid

import aiohttp.web
import pytest
from smoke_tests import smoke_tests_utils
import yaml

import sky
from sky.serve import load_balancer
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


class _HttpResponseContext:
    """Minimal aiohttp request context for endpoint-readiness tests."""

    def __init__(self, outcome):
        self._outcome = outcome

    async def __aenter__(self):
        if isinstance(self._outcome, BaseException):
            raise self._outcome
        return self

    async def __aexit__(self, *_args):
        return False

    @property
    def status(self):
        return self._outcome

    async def read(self):
        return b''


class _HttpSession:
    """Scripted aiohttp session for endpoint-readiness tests."""

    def __init__(self, outcomes, calls, timeout):
        self._outcomes = outcomes
        self._calls = calls
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def get(self, url, **kwargs):
        self._calls.append((url, kwargs))
        outcome = self._outcomes.pop(0)
        if callable(outcome):
            outcome = outcome()
        return _HttpResponseContext(outcome)


def _provider_scope(**overrides):
    values = {
        'service_hash': 'incarnation',
        'resource_scope': 'resource-scope',
        'lifecycle_epoch': 7,
        'service_version': 11,
        'max_live_paid_gpu_units': 2,
        'providers': ('aws', 'gcp'),
        'project_id': 'durable-project',
        'workspace': 'workspace-a',
        'location_scope': qualifier.GcpLocationScope.PROJECT_WIDE,
        'aws_location_scope':
            (qualifier.AwsLocationScope.FROZEN_CATALOG_REGIONS),
        'aws_regions':
            (qualifier.AwsRegionScope(aws_account_id='123456789012',
                                      credential_profile='durable-profile',
                                      region='us-east-2'),),
        'catalog_shapes': (
            qualifier.CatalogShape(cloud='aws',
                                   region='us-east-2',
                                   zone='us-east-2a',
                                   instance_type='g6.xlarge',
                                   gpu_units_per_instance=1),
            qualifier.CatalogShape(cloud='aws',
                                   region='us-east-2',
                                   zone='us-east-2b',
                                   instance_type='g6.12xlarge',
                                   gpu_units_per_instance=4),
            qualifier.CatalogShape(cloud='aws',
                                   region='us-east-2',
                                   zone='us-east-2c',
                                   instance_type='g6.48xlarge',
                                   gpu_units_per_instance=8),
            qualifier.CatalogShape(cloud='gcp',
                                   region='us-central1',
                                   zone='us-central1-a',
                                   instance_type='g2-standard-4',
                                   gpu_units_per_instance=1),
            qualifier.CatalogShape(cloud='gcp',
                                   region='us-east1',
                                   zone='us-east1-b',
                                   instance_type='g2-standard-4',
                                   gpu_units_per_instance=1),
        ),
        'placement_catalog_sha256': 'c' * 64,
        'service_yaml_sha256': 'd' * 64,
        'qualification_profile': 'small',
        'qualification_source_sha256': 'e' * 64,
        'qualification_projection_sha256':
            qualifier._qualification_projection_sha256(
                source_sha256='e' * 64,
                profile=qualifier.PROFILES['small'],
                providers=('aws', 'gcp')),
        'controller_config_digest': 'a' * 64,
        'controller_config_snapshot_id': 'b' * 64,
    }
    values.update(overrides)
    return qualifier.ProviderScope(**values)


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


def _aws_identity(*,
                  client_token='token-new',
                  cluster_name='paid-e2e-1-1234567890-tenant',
                  instance_type='g6.xlarge',
                  width=1,
                  zone='us-east-2a'):
    return qualifier.AwsProviderIdentity(aws_account_id='123456789012',
                                         client_token=client_token,
                                         cluster_name_on_cloud=cluster_name,
                                         gpu_units_per_instance=width,
                                         instance_type=instance_type,
                                         num_nodes=1,
                                         region='us-east-2',
                                         use_spot=True,
                                         workspace='workspace-a',
                                         zone=zone)


def _aws_instance(*,
                  client_token='token-new',
                  cluster_name='paid-e2e-1-1234567890-tenant',
                  instance_id='i-new',
                  instance_type='g6.xlarge',
                  width=1,
                  zone='us-east-2a',
                  state='running',
                  volume_id='vol-new'):
    return {
        'availability_zone': zone,
        'client_token': client_token,
        'cluster_name_on_cloud': cluster_name,
        'instance_id': instance_id,
        'instance_type': instance_type,
        'market': 'spot',
        'provider_gpu_units': width,
        'region': 'us-east-2',
        'state': state,
        'volume_ids': (volume_id,),
    }


def _aws_volume(*,
                cluster_name='paid-e2e-1-1234567890-tenant',
                volume_id='vol-new'):
    return {
        'cluster_name_on_cloud': cluster_name,
        'region': 'us-east-2',
        'state': 'in-use',
        'volume_id': volume_id,
    }


def _database_state(**overrides):
    bound_cluster_zones = overrides.pop('bound_cluster_zones', ())
    values = {
        'service_hash': 'incarnation',
        'controller': qualifier.ControllerIdentity(
            pid=123,
            ip='10.0.0.1',
            owner_epoch=7,
            incarnation='12345678-1234-5678-9234-567812345678'),
        'paid_debit_units': 0,
        'claimed_units': 0,
        'claim_priority_units': (),
        'waiter_count': 0,
        'demand_units': 0,
        'gcp_provider_identities': tuple(
            qualifier.GcpProviderIdentity(
                cluster_name_on_cloud=cluster_name,
                gpu_units_per_instance=1,
                instance_type='g2-standard-4',
                project_id='durable-project',
                region=qualifier._gcp_region_from_zone(zone),
                workspace='workspace-a',
                zone=zone) for cluster_name, zone in bound_cluster_zones),
        'aws_provider_identities': (),
        'provider_free_unbound_replica_ids': (),
    }
    values.update(overrides)
    return qualifier.DatabaseState(**values)


def _provider_state(**overrides):
    instance_count = overrides.get('instance_count', 0)
    running_count = overrides.get('running_count', 0)
    gpu_units = overrides.get('gpu_units', instance_count)
    running_gpu_units = overrides.get('running_gpu_units', running_count)
    values = {
        'instance_count': 0,
        'running_count': 0,
        'gpu_units': gpu_units,
        'running_gpu_units': running_gpu_units,
        'disk_count': 0,
        'inflight_operation_count': 0,
        'cluster_names': frozenset(),
        'clouds': (
            qualifier.ProviderCloudState(cloud='gcp',
                                         instance_count=instance_count,
                                         running_count=running_count,
                                         gpu_units=gpu_units,
                                         running_gpu_units=running_gpu_units,
                                         disk_count=overrides.get(
                                             'disk_count', 0),
                                         inflight_operation_count=overrides.get(
                                             'inflight_operation_count', 0)),
            qualifier.ProviderCloudState(cloud='aws',
                                         instance_count=0,
                                         running_count=0,
                                         gpu_units=0,
                                         running_gpu_units=0,
                                         disk_count=0,
                                         inflight_operation_count=0),
        ),
    }
    values.update(overrides)
    return qualifier.ProviderState(**values)


def _provider_cluster_names(cloud, count):
    return tuple(f'paid-e2e-{cloud}-{index:03d}' for index in range(count))


def _cross_cloud_provider_state(*,
                                gcp_running_count,
                                aws_running_count,
                                gpu_units_per_instance=1):
    gcp_names = _provider_cluster_names('gcp', gcp_running_count)
    aws_names = _provider_cluster_names('aws', aws_running_count)

    def _cloud_state(cloud, running_count):
        gpu_units = running_count * gpu_units_per_instance
        return qualifier.ProviderCloudState(cloud=cloud,
                                            instance_count=running_count,
                                            running_count=running_count,
                                            gpu_units=gpu_units,
                                            running_gpu_units=gpu_units,
                                            disk_count=running_count,
                                            inflight_operation_count=0)

    gcp = _cloud_state('gcp', gcp_running_count)
    aws = _cloud_state('aws', aws_running_count)
    return qualifier.ProviderState(
        instance_count=gcp.instance_count + aws.instance_count,
        running_count=gcp.running_count + aws.running_count,
        gpu_units=gcp.gpu_units + aws.gpu_units,
        running_gpu_units=gcp.running_gpu_units + aws.running_gpu_units,
        disk_count=gcp.disk_count + aws.disk_count,
        inflight_operation_count=0,
        cluster_names=frozenset(gcp_names + aws_names),
        clouds=(gcp, aws))


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
    finished_monotonic = (observed_at
                          if observed_monotonic is None else observed_monotonic)
    return qualifier.Observation(observed_started_at=observed_at - 0.5,
                                 observed_started_monotonic=finished_monotonic -
                                 0.5,
                                 observed_at=observed_at,
                                 observed_monotonic=finished_monotonic,
                                 **values)


def _request_telemetry(*,
                       queue_depth=0,
                       in_flight=0,
                       processing=0,
                       state_counts=None,
                       observed_at=1000.0):
    summary = {
        'request_telemetry_observed_at': observed_at,
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


async def _campaign_progress(profile, *, turnover=0):
    window = qualifier.scale_stimulus_count(profile)
    progress = qualifier.ExactRequestCampaignProgress(
        total_count=profile.exact_requests, window_size=window)
    for _ in range(window):
        await progress.mark_offered()
    for _ in range(turnover):
        await progress.mark_succeeded()
        await progress.mark_offered()
    return progress


def _qualification_config(*, persisted: bool, reverse: bool = False):
    config = yaml.safe_load(
        (_FIXTURE_DIR / 'service.yaml').read_text(encoding='utf-8'))
    branches = list(config['resources']['any_of'])
    if reverse:
        branches.reverse()
    if persisted:
        branches = [{
            **branch,
            'accelerators': {
                'L4': 1,
            },
            'use_spot': True,
        } for branch in branches]
        config['resources'] = {'any_of': branches}
    else:
        config['resources']['any_of'] = branches
    return config


@pytest.mark.parametrize('persisted', [False, True])
@pytest.mark.parametrize('reverse', [False, True])
def test_cross_cloud_service_contract_normalizes_one_canonical_shape(
        persisted, reverse):
    config = _qualification_config(persisted=persisted, reverse=reverse)
    assert qualifier._validate_qualification_service_config(config) == (
        qualifier.QualificationServiceContract(
            providers=('aws', 'gcp'),
            min_replicas=0,
            max_replicas=2,
            max_live_paid_gpu_units=2,
            scale_up_rate_min_replicas=2,
            scale_up_rate_period_seconds=10,
            request_queue_min_size=4,
            request_queue_max_size=32,
            request_queue_max_concurrency=4,
            request_queue_timeout_seconds=600,
            max_concurrency_per_replica=8))


@pytest.mark.parametrize('mutation', [
    'top_region',
    'branch_region',
    'branch_zone',
    'branch_instance_type',
    'duplicate_cloud',
    'extra_cloud',
    'wrong_width',
    'wrong_card',
    'on_demand',
    'accelerators_string',
    'boolean_width',
    'spot_string',
    'malformed_branch',
])
def test_cross_cloud_service_contract_rejects_scope_drift(mutation):
    config = _qualification_config(persisted=True)
    resources = config['resources']
    branches = resources['any_of']
    if mutation == 'top_region':
        resources['region'] = 'us-east-1'
    elif mutation in ('branch_region', 'branch_zone', 'branch_instance_type'):
        branches[0][mutation.removeprefix('branch_')] = 'pinned'
    elif mutation == 'duplicate_cloud':
        branches[1]['infra'] = branches[0]['infra']
    elif mutation == 'extra_cloud':
        branches[1]['infra'] = 'azure'
    elif mutation == 'wrong_width':
        branches[0]['accelerators']['L4'] = 2
    elif mutation == 'wrong_card':
        branches[0]['accelerators'] = {'A100': 1}
    elif mutation == 'on_demand':
        branches[0]['use_spot'] = False
    elif mutation == 'accelerators_string':
        branches[0]['accelerators'] = 'L4:1'
    elif mutation == 'boolean_width':
        branches[0]['accelerators']['L4'] = True
    elif mutation == 'spot_string':
        branches[0]['use_spot'] = 'true'
    elif mutation == 'malformed_branch':
        branches[0] = 'aws'
    else:
        raise AssertionError(mutation)
    with pytest.raises(ValueError, match='not generic whole-L4 Spot'):
        qualifier._validate_qualification_service_config(config)


@pytest.mark.parametrize('provider', ['aws', 'gcp'])
def test_service_contract_accepts_provider_canary_projection(provider):
    config = _qualification_config(persisted=True)
    config['resources']['any_of'] = [
        branch for branch in config['resources']['any_of']
        if branch['infra'] == provider
    ]

    contract = qualifier._validate_qualification_service_config(config)

    assert contract.providers == (provider,)


def test_render_profiles_share_one_spot_only_service(tmp_path):
    source = _FIXTURE_DIR / 'service.yaml'
    for name, expected_units, expected_first_wave, expected_period in (
        ('small', 2, 2, 10),
        ('scale', 800, 800, 10),
    ):
        output = tmp_path / f'{name}.yaml'
        args = type(
            'Args', (), {
                'profile': name,
                'provider': None,
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
        projection = qualifier._profile_projection(profile)
        assert queue['min_size'] == projection['request_queue_min_size']
        assert queue['max_size'] == projection['request_queue_max_size']
        assert queue['max_concurrency'] == (
            projection['request_queue_max_concurrency'])
        assert queue['timeout_seconds'] == (
            projection['request_queue_timeout_seconds'])
        assert resources['use_spot'] is True
        assert resources['accelerators'] == 'L4:1'
        assert resources['any_of'] == [
            {
                'infra': 'aws',
            },
            {
                'infra': 'gcp',
            },
        ]
        assert 'infra' not in resources
        assert 'instance_type' not in resources
        assert 'workdir' not in config
        assert 'file_mounts' not in config
        assert 'server.py' not in config['run']
        assert "exec python3 - <<'PY'" in config['run']
        task = sky.Task.from_yaml_str(output.read_text(encoding='utf-8'))
        assert task.workdir is None
        serve_utils.validate_service_task(task, pool=False)


def test_source_identity_rejects_nested_reserved_provenance(tmp_path):
    canonical = _qualification_config(persisted=False)
    altered = yaml.safe_load(yaml.safe_dump(canonical, sort_keys=False))
    altered['run'] = 'echo attacker-controlled-outer-task'
    altered['_user_specified_yaml'] = yaml.safe_dump(canonical, sort_keys=False)
    persisted = _qualification_config(persisted=True)
    persisted['_user_specified_yaml'] = yaml.safe_dump(altered, sort_keys=False)

    with pytest.raises(ValueError, match='reserved provenance'):
        qualifier._persisted_qualification_user_config(persisted)
    with pytest.raises(ValueError, match='reserved provenance'):
        qualifier._qualification_source_sha256(altered)

    source = tmp_path / 'nested-provenance.yaml'
    output = tmp_path / 'rendered.yaml'
    source.write_text(yaml.safe_dump(altered, sort_keys=False),
                      encoding='utf-8')
    args = type(
        'Args', (), {
            'profile': 'scale',
            'provider': None,
            'economic_receipt': None,
            'source': str(source),
            'output': str(output),
        })()
    with pytest.raises(qualifier.QualificationError,
                       match='invalid reserved provenance'):
        qualifier.render_service(args)
    assert not output.exists()


def test_scale_profile_exceeds_physical_gate_for_rendered_shape():
    profile = qualifier.PROFILES['scale']

    assert profile.max_replicas == 800
    config = _qualification_config(persisted=False)
    assert config['resources']['accelerators'] == 'L4:1'
    assert profile.max_units == 800
    assert profile.max_units // 1 >= 100
    assert profile.exact_requests == 10_000
    assert profile.request_concurrency == 128
    assert profile.request_concurrency < profile.exact_requests
    assert qualifier.scale_stimulus_count(profile) == profile.max_units
    assert profile.scale_up_min_replicas == profile.max_units


@pytest.mark.parametrize(
    ('arrivals_60s', 'arrivals_300s', 'campaign_offered', 'campaign_succeeded',
     'require_stimulus_commit', 'expected'),
    [(800, 800, 800, 0, True, True), (0, 799, 800, 0, True, True),
     (0, 0, 800, 0, False, True), (801, 801, 801, 1, False, True),
     (10_001, 10_001, 10_000, 9_200, False, False),
     (1, 0, 800, 0, False, False)],
)
def test_scale_arrival_attribution_has_commit_and_sliding_modes(
        arrivals_60s, arrivals_300s, campaign_offered, campaign_succeeded,
        require_stimulus_commit, expected):
    previous = None
    if not require_stimulus_commit:
        previous = qualifier._ScaleArrivalAttributionState(
            unique_job_arrivals_60s=800,
            unique_job_arrivals_300s=800,
            campaign_offered=800,
            campaign_succeeded=0)
    state = qualifier._next_scale_arrival_attribution_state(
        previous=previous,
        unique_job_arrivals_60s=arrivals_60s,
        unique_job_arrivals_300s=arrivals_300s,
        headerless_arrivals_60s=0,
        headerless_arrivals_300s=0,
        offered_arrival_tracking_saturated=False,
        initial_arrivals=800,
        maximum_arrivals=10_000,
        campaign_offered=campaign_offered,
        campaign_succeeded=campaign_succeeded)
    assert (state is not None) is expected


def test_scale_arrival_attribution_allows_bounded_turnover_and_rejects_bools():
    committed = qualifier._next_scale_arrival_attribution_state(
        previous=None,
        unique_job_arrivals_60s=800,
        unique_job_arrivals_300s=800,
        headerless_arrivals_60s=0,
        headerless_arrivals_300s=0,
        offered_arrival_tracking_saturated=False,
        initial_arrivals=800,
        maximum_arrivals=10_000,
        campaign_offered=800,
        campaign_succeeded=0)
    assert committed is not None
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(committed, 'unique_job_arrivals_60s', 1)
    aged_out = qualifier._next_scale_arrival_attribution_state(
        previous=committed,
        unique_job_arrivals_60s=0,
        unique_job_arrivals_300s=0,
        headerless_arrivals_60s=0,
        headerless_arrivals_300s=0,
        offered_arrival_tracking_saturated=False,
        initial_arrivals=800,
        maximum_arrivals=10_000,
        campaign_offered=800,
        campaign_succeeded=0)
    assert aged_out is not None
    assert qualifier._next_scale_arrival_attribution_state(
        previous=aged_out,
        unique_job_arrivals_60s=1,
        unique_job_arrivals_300s=1,
        headerless_arrivals_60s=0,
        headerless_arrivals_300s=0,
        offered_arrival_tracking_saturated=False,
        initial_arrivals=800,
        maximum_arrivals=10_000,
        campaign_offered=801,
        campaign_succeeded=1) is not None
    for field, invalid in (('headerless_arrivals_60s',
                            False), ('headerless_arrivals_300s',
                                     False), ('headerless_arrivals_60s', 0.0),
                           ('headerless_arrivals_300s', '0')):
        fields = {
            'headerless_arrivals_60s': 0,
            'headerless_arrivals_300s': 0,
        }
        fields[field] = invalid
        assert qualifier._next_scale_arrival_attribution_state(
            previous=None,
            unique_job_arrivals_60s=800,
            unique_job_arrivals_300s=800,
            offered_arrival_tracking_saturated=False,
            initial_arrivals=800,
            maximum_arrivals=10_000,
            campaign_offered=800,
            campaign_succeeded=0,
            **fields) is None
    assert qualifier._next_scale_arrival_attribution_state(
        previous=None,
        unique_job_arrivals_60s=True,
        unique_job_arrivals_300s=800,
        headerless_arrivals_60s=0,
        headerless_arrivals_300s=0,
        offered_arrival_tracking_saturated=False,
        initial_arrivals=800,
        maximum_arrivals=10_000,
        campaign_offered=800,
        campaign_succeeded=0) is None


def test_scale_arrivals_cannot_run_ahead_of_terminal_success_frontier():
    common = {
        'previous': qualifier._ScaleArrivalAttributionState(
            unique_job_arrivals_60s=800,
            unique_job_arrivals_300s=800,
            campaign_offered=800,
            campaign_succeeded=0),
        'unique_job_arrivals_60s': 801,
        'unique_job_arrivals_300s': 801,
        'headerless_arrivals_60s': 0,
        'headerless_arrivals_300s': 0,
        'offered_arrival_tracking_saturated': False,
        'initial_arrivals': 800,
        'maximum_arrivals': 10_000,
    }

    assert qualifier._next_scale_arrival_attribution_state(campaign_offered=801,
                                                           campaign_succeeded=0,
                                                           **common) is None
    assert qualifier._next_scale_arrival_attribution_state(campaign_offered=801,
                                                           campaign_succeeded=1,
                                                           **common) is not None
    assert qualifier._next_scale_arrival_attribution_state(campaign_offered=802,
                                                           campaign_succeeded=1,
                                                           **common) is None
    assert qualifier._next_scale_arrival_attribution_state(
        previous=None,
        unique_job_arrivals_60s=799,
        unique_job_arrivals_300s=799,
        headerless_arrivals_60s=0,
        headerless_arrivals_300s=0,
        offered_arrival_tracking_saturated=False,
        initial_arrivals=800,
        maximum_arrivals=10_000,
        campaign_offered=799,
        campaign_succeeded=0) is None
    advanced = qualifier._next_scale_arrival_attribution_state(
        campaign_offered=801, campaign_succeeded=1, **common)
    assert advanced is not None
    assert qualifier._next_scale_arrival_attribution_state(
        previous=advanced,
        unique_job_arrivals_60s=800,
        unique_job_arrivals_300s=800,
        headerless_arrivals_60s=0,
        headerless_arrivals_300s=0,
        offered_arrival_tracking_saturated=False,
        initial_arrivals=800,
        maximum_arrivals=10_000,
        campaign_offered=800,
        campaign_succeeded=1) is None


def test_campaign_progress_serializes_window_and_terminal_order():

    async def exercise():
        progress = qualifier.ExactRequestCampaignProgress(total_count=3,
                                                          window_size=2)
        await asyncio.gather(progress.mark_offered(), progress.mark_offered())
        assert await progress.snapshot(
        ) == qualifier.ExactRequestCampaignCounters(offered=2, succeeded=0)
        with pytest.raises(qualifier.QualificationError, match='active window'):
            await progress.mark_offered()
        await progress.mark_succeeded()
        await progress.mark_offered()
        await asyncio.gather(progress.mark_succeeded(),
                             progress.mark_succeeded())
        return await progress.snapshot()

    assert asyncio.run(exercise()) == qualifier.ExactRequestCampaignCounters(
        offered=3, succeeded=3)


def test_positive_telemetry_deadline_allows_one_retry_after_scale_timeout():
    profile = qualifier.PROFILES['scale']
    small = qualifier.PROFILES['small']

    assert profile.request_queue_timeout_seconds == 600
    assert qualifier.positive_telemetry_deadline_monotonic(
        profile, scale_started_monotonic=100.0) == 1590.0

    with pytest.raises(ValueError, match='polling margin'):
        qualifier.positive_telemetry_window_seconds(
            dataclasses.replace(profile, request_queue_timeout_seconds=10))
    assert qualifier.positive_telemetry_window_seconds(small) == 120
    assert qualifier.positive_telemetry_deadline_monotonic(
        small, scale_started_monotonic=100.0) == 220.0


def test_scale_queue_is_bounded_to_one_sliding_window():
    profile = qualifier.PROFILES['scale']
    projection = qualifier._profile_projection(profile)
    instance = object.__new__(load_balancer.SkyServeLoadBalancer)
    instance._request_queue_config = {  # pylint: disable=protected-access
        'max_size': projection['request_queue_max_size'],
        'max_concurrency': projection['request_queue_max_concurrency'],
    }

    assert projection['request_queue_min_size'] == 800
    assert projection['request_queue_max_size'] == 800
    assert projection['request_queue_max_concurrency'] == 128
    assert projection['request_queue_timeout_seconds'] == 600
    assert instance._request_queue_submission_limit() == 928
    assert instance._request_queue_submission_limit() < profile.exact_requests


def test_fixture_processing_fits_worker_bound_below_queue_timeout():
    config = yaml.safe_load(
        (_FIXTURE_DIR / 'service.yaml').read_text(encoding='utf-8'))
    duration_limit = qualifier._fixture_duration_limit(config)
    queue_timeout = config['service']['load_balancer']['request_queue'][
        'timeout_seconds']

    assert duration_limit == 360
    assert qualifier.request_processing_seconds(
        qualifier.PROFILES['scale']) == 20
    assert all(
        qualifier.request_processing_seconds(profile) <= duration_limit
        for profile in qualifier.PROFILES.values())
    assert duration_limit < queue_timeout


@pytest.mark.parametrize('provider', ['aws', 'gcp'])
def test_provider_canary_is_rendered_from_the_canonical_fixture(
        tmp_path, provider):
    source_config = yaml.safe_load(
        (_FIXTURE_DIR / 'service.yaml').read_text(encoding='utf-8'))
    economic_args = _aggregate_args(
        tmp_path,
        with_canary=False,
        source_sha256=qualifier._qualification_source_sha256(source_config),
        economic_peaks={
            'aws' if provider == 'gcp' else 'gcp': 100,
            provider: 0,
        })
    output = tmp_path / f'{provider}.yaml'
    qualifier.render_service(
        type(
            'Args', (), {
                'profile': 'provider-canary',
                'provider': provider,
                'economic_receipt': economic_args.economic_receipt,
                'source': str(_FIXTURE_DIR / 'service.yaml'),
                'output': str(output),
            })())

    config = yaml.safe_load(output.read_text(encoding='utf-8'))
    policy = config['service']['replica_policy']
    assert policy['max_replicas'] == 1
    assert policy['max_live_paid_gpu_units'] == 1
    assert config['resources']['any_of'] == [{'infra': provider}]
    assert qualifier._validate_qualification_service_config(
        config).providers == (provider,)


@pytest.mark.parametrize('authorization', ['missing', 'wrong', 'unnecessary'])
def test_provider_canary_is_rejected_before_render_without_exact_economic_gap(
        tmp_path, authorization):
    source = _FIXTURE_DIR / 'service.yaml'
    source_config = yaml.safe_load(source.read_text(encoding='utf-8'))
    provider = 'gcp'
    economic_peaks = ({
        'aws': 100,
        'gcp': 0,
    } if authorization != 'unnecessary' else {
        'aws': 50,
        'gcp': 50,
    })
    economic = _aggregate_args(
        tmp_path,
        with_canary=False,
        economic_peaks=economic_peaks,
        source_sha256=qualifier._qualification_source_sha256(source_config))
    economic_receipt = (None if authorization == 'missing' else
                        economic.economic_receipt)
    if authorization == 'wrong':
        provider = 'aws'
    args = type(
        'Args', (), {
            'profile': 'provider-canary',
            'provider': provider,
            'economic_receipt': economic_receipt,
            'source': str(source),
            'output': str(tmp_path / 'canary.yaml'),
        })()

    with pytest.raises(qualifier.QualificationError, match='Provider canary'):
        qualifier.render_service(args)
    assert not pathlib.Path(args.output).exists()


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
            'provider': None,
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
                'aws': {
                    'profile': 'durable-profile',
                },
            },
        },
    }).encode()
    monkeypatch.setattr(
        qualifier.serve_utils, 'parse_and_validate_version_controller_config',
        lambda contents, workspace, _source: yaml.safe_load(contents))
    monkeypatch.setenv('GOOGLE_CLOUD_PROJECT', 'wrong-ambient-project')

    class Sts:

        @staticmethod
        def get_caller_identity():
            return {'Account': '123456789012'}

    class Session:

        @staticmethod
        def client(name, *, region_name):
            assert name == 'sts'
            assert region_name == 'us-east-2'
            return Sts()

    monkeypatch.setattr(qualifier.aws_adaptor, 'session',
                        lambda profile: Session())
    catalog = qualifier.spot_placer.PlacementCatalog(
        entries=((qualifier.spot_placer.Location(sky.AWS(),
                                                 'us-east-2',
                                                 'us-east-2a',
                                                 accelerators={'L4': 1},
                                                 use_spot=True,
                                                 instance_type='g6.xlarge'),
                  0.1),
                 (qualifier.spot_placer.Location(sky.GCP(),
                                                 'us-central1',
                                                 'us-central1-a',
                                                 accelerators={'L4': 1},
                                                 use_spot=True,
                                                 instance_type='g2-standard-4'),
                  0.2)),
        num_nodes=1).to_dict()
    user_service = yaml.safe_load(
        (_FIXTURE_DIR / 'service.yaml').read_text(encoding='utf-8'))
    user_service['service']['load_balancer']['request_queue'][
        'max_concurrency'] = 8
    user_service_yaml = yaml.safe_dump(user_service, sort_keys=False)
    persisted_service = yaml.safe_load(user_service_yaml)
    user_resources = persisted_service['resources']
    # The API stores effective resources inside each ``any_of`` branch and may
    # canonicalize their order.  Provider scope must validate that durable
    # form, not the pre-submission shorthand.
    persisted_service['resources'] = {
        'any_of': [{
            **branch,
            'accelerators': {
                'L4': 1,
            },
            'use_spot': True,
            'disk_size': 256,
            'ports': ['8080'],
        } for branch in reversed(user_resources['any_of'])],
    }
    persisted_service['_user_specified_yaml'] = user_service_yaml
    service_yaml = yaml.safe_dump(persisted_service, sort_keys=False)
    authority = {
        'service_hash': 'incarnation',
        'resource_scope': 'resource-scope',
        'service_lifecycle_epoch': 7,
        'current_version': 11,
        'workspace': 'workspace-a',
        'controller_config': config_bytes,
        'controller_config_digest': hashlib.sha256(config_bytes).hexdigest(),
        'controller_config_snapshot_id': 'a' * 64,
        'placement_catalog': catalog,
        'yaml_content': service_yaml,
    }
    scope = qualifier.provider_scope_from_controller_config(authority)
    assert scope.project_id == 'durable-project'
    assert scope.resource_scope == 'resource-scope'
    assert (scope.location_scope is qualifier.GcpLocationScope.PROJECT_WIDE)
    receipt = tmp_path / 'scope.json'
    qualifier.write_provider_scope(receipt, 'paid-e2e', scope)
    assert qualifier.read_provider_scope(receipt, 'paid-e2e') == scope


@pytest.mark.parametrize('provider', ['aws', 'gcp'])
def test_provider_scope_receipt_accepts_one_provider_canary(tmp_path, provider):
    base = _provider_scope()
    scope = dataclasses.replace(
        base,
        max_live_paid_gpu_units=1,
        providers=(provider,),
        project_id=(base.project_id if provider == 'gcp' else None),
        location_scope=(base.location_scope if provider == 'gcp' else None),
        aws_location_scope=(base.aws_location_scope
                            if provider == 'aws' else None),
        aws_regions=(base.aws_regions if provider == 'aws' else ()),
        qualification_profile='provider-canary',
        qualification_projection_sha256=(
            qualifier._qualification_projection_sha256(
                source_sha256=base.qualification_source_sha256,
                profile=qualifier.PROFILES['provider-canary'],
                providers=(provider,))),
        catalog_shapes=tuple(
            shape for shape in base.catalog_shapes if shape.cloud == provider))
    receipt = tmp_path / f'{provider}-scope.json'

    qualifier.write_provider_scope(receipt, 'paid-e2e', scope)

    assert qualifier.read_provider_scope(receipt, 'paid-e2e') == scope


def test_provider_scope_commands_are_regionless_by_default():
    freeze = qualifier._parser().parse_args([
        'freeze-scope', '--service-name', 'paid-e2e', '--output',
        '/tmp/scope.json'
    ])
    run = qualifier._parser().parse_args([
        'run', '--profile', 'small', '--service-name', 'paid-e2e', '--endpoint',
        'https://example.test', '--receipt', '/tmp/receipt.json', '--scope',
        '/tmp/scope.json'
    ])
    assert not hasattr(freeze, 'region')
    assert not hasattr(run, 'region')


def test_http_authentication_waits_for_dns_and_endpoint_readiness(monkeypatch):
    now = [100.0]
    sleeps = []
    calls = []
    outcomes = [
        qualifier.aiohttp.ClientConnectorDNSError(None,
                                                  OSError('not resolved')),
        503,
        401,
        200,
    ]

    def client_session(*, timeout):
        assert timeout.total == 15
        return _HttpSession(outcomes, calls, timeout)

    async def sleep(delay):
        sleeps.append(delay)
        now[0] += delay

    monkeypatch.setattr(qualifier.aiohttp, 'ClientSession', client_session)
    monkeypatch.setattr(qualifier.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(qualifier.asyncio, 'sleep', sleep)

    asyncio.run(
        qualifier.HttpObserver('https://new-nlb.example.test',
                               'token').prove_authentication())

    assert sleeps == [2, 2]
    assert len(calls) == 4
    assert calls[-2][1] == {}
    assert calls[-1][1] == {
        'headers': {
            qualifier._AUTH_HEADER: 'Bearer token',
        }
    }


def test_http_authentication_fails_immediately_when_not_enforced(monkeypatch):
    calls = []
    outcomes = [200]

    monkeypatch.setattr(
        qualifier.aiohttp, 'ClientSession',
        lambda *, timeout: _HttpSession(outcomes, calls, timeout))

    with pytest.raises(qualifier.QualificationError,
                       match='authentication is not enforced'):
        asyncio.run(
            qualifier.HttpObserver('https://service.example.test',
                                   'token').prove_authentication())

    assert len(calls) == 1


def test_http_authentication_fails_immediately_for_rejected_token(monkeypatch):
    calls = []
    outcomes = [401, 403]

    monkeypatch.setattr(
        qualifier.aiohttp, 'ClientSession',
        lambda *, timeout: _HttpSession(outcomes, calls, timeout))

    with pytest.raises(qualifier.QualificationError,
                       match='Authenticated capacity probe returned 403'):
        asyncio.run(
            qualifier.HttpObserver('https://service.example.test',
                                   'bad-token').prove_authentication())

    assert len(calls) == 2


def test_http_authentication_dns_retry_has_monotonic_deadline(monkeypatch):
    now = [100.0]
    sleeps = []
    calls = []
    outcomes = [
        lambda: qualifier.aiohttp.ClientConnectorDNSError(
            None, OSError('not resolved')),
        lambda: qualifier.aiohttp.ClientConnectorDNSError(
            None, OSError('not resolved')),
    ]

    async def sleep(delay):
        sleeps.append(delay)
        now[0] += delay

    monkeypatch.setattr(
        qualifier.aiohttp, 'ClientSession',
        lambda *, timeout: _HttpSession(outcomes, calls, timeout))
    monkeypatch.setattr(qualifier.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(qualifier.asyncio, 'sleep', sleep)
    monkeypatch.setattr(qualifier, '_ENDPOINT_AUTHENTICATION_TIMEOUT_SECONDS',
                        4)

    with pytest.raises(qualifier.QualificationError,
                       match='authentication deadline'):
        asyncio.run(
            qualifier.HttpObserver('https://new-nlb.example.test',
                                   'token').prove_authentication())

    assert sleeps == [2, 2]
    assert len(calls) == 2


def test_retained_request_accepts_cross_region_and_rejects_scope_drift(
        monkeypatch):
    scope = _provider_scope()
    association_id = uuid.UUID('11111111-1111-4111-8111-111111111111')
    binding = {
        'association_id': association_id,
        'request_id': 'request-1',
        'tenant_scope': 'tenant-a',
        'cluster_name': 'paid-e2e-1',
        'service_workspace': 'workspace-a',
    }
    for index, field in enumerate(qualifier._BOUND_REQUEST_PROFILE_FIELDS):
        binding[field] = f'profile-{index}'
    request_row = {
        **{
            field: binding[field] for field in qualifier._BOUND_REQUEST_PROFILE_FIELDS
        },
        'ordinary_launch_association_id': association_id,
        'request_id': 'request-1',
        'handler_name': qualifier.non_pool_launch.NON_POOL_LAUNCH_HANDLER_NAME,
        'user_id': 'tenant-a',
        'cluster_name': 'paid-e2e-1',
    }
    retained_request: list[object | None] = [None]
    monkeypatch.setattr(qualifier.request_postgres, 'request_from_mapping',
                        lambda _row: retained_request[0])
    context = object()
    monkeypatch.setattr(qualifier.ordinary_launch_binding,
                        'bound_context_from_association', lambda _row: context)
    monkeypatch.setattr(qualifier.ordinary_launch_binding,
                        'parse_bound_non_pool_launch_context',
                        lambda _row: context)

    def configure(*,
                  project_id='durable-project',
                  region='us-east1',
                  zone='us-east1-b',
                  accelerator='l4',
                  instance_type='g2-standard-4'):
        pool_payload = {
            'accelerators': [[accelerator, 1]],
            'cloud': 'gcp',
            'instance_type': instance_type,
            'num_nodes': 1,
            'region': region,
            'use_spot': True,
            'version': 1,
            'workspace': 'workspace-a',
            'zone': zone,
        }
        binding['paid_capacity_pool_key'] = json.dumps(pool_payload,
                                                       sort_keys=True,
                                                       separators=(',', ':'))
        retained_request[0] = types.SimpleNamespace(
            request_body=types.SimpleNamespace(
                extra_launch_context={},
                override_skypilot_config={
                    'active_workspace': 'workspace-a',
                    'workspaces': {
                        'workspace-a': {
                            'gcp': {
                                'project_id': project_id,
                            },
                        },
                    },
                }))

    configure()
    identity = qualifier.gcp_identity_from_retained_request(
        binding, request_row, scope)
    assert identity.region == 'us-east1'
    assert identity.zone == 'us-east1-b'

    for overrides in ({
            'project_id': 'different-project'
    }, {
            'region': 'us-east1',
            'zone': 'us-west1-a'
    }, {
            'instance_type': 'g2-standard-8'
    }, {
            'accelerator': 'a100'
    }):
        configure(**overrides)
        with pytest.raises(qualifier.GuardViolation,
                           match='retained-request GCP identity'):
            qualifier.gcp_identity_from_retained_request(
                binding, request_row, scope)


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
                'zones/us-east1-b': {
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
    scope = _provider_scope()
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

    scope = _provider_scope()
    observer = qualifier.GcpObserver(service_name='paid-e2e',
                                     scope=scope,
                                     profile=qualifier.PROFILES['small'],
                                     compute=Compute())
    with pytest.raises(qualifier.QualificationError) as error:
        observer.census()
    assert str(error.value) == 'GCP Compute API instances census failed.'
    assert 'credential-bearing' not in str(error.value)


def test_aws_retained_identity_is_bound_to_frozen_catalog(monkeypatch):
    scope = _provider_scope(max_live_paid_gpu_units=8)
    pool = {
        'accelerators': [['L4', 8]],
        'cloud': 'aws',
        'instance_type': 'g6.48xlarge',
        'num_nodes': 1,
        'provider_identity': {
            'aws_account_id': '123456789012',
        },
        'region': 'us-east-2',
        'use_spot': True,
        'version': 2,
        'workspace': 'workspace-a',
        'zone': 'us-east-2c',
    }
    # Server-controller launch requests deliberately omit credentials and
    # retain only the workspace selector.
    config = {'active_workspace': 'workspace-a'}
    monkeypatch.setattr(qualifier, '_retained_launch_request', lambda *_args:
                        (config, pool, 'workspace-a'))

    def provider_identity(_binding, *, credential_profile):
        assert credential_profile is None
        return {
            'aws_account_id': pool['provider_identity']['aws_account_id'],
            'client_token': 'token-wide',
            'cluster_name_on_cloud': 'paid-e2e-1-1234567890-tenant',
            'instance_type': pool['instance_type'],
            'num_nodes': 1,
            'region': pool['region'],
            'use_spot': True,
            'workspace': 'workspace-a',
            'zone': pool['zone'],
        }

    monkeypatch.setattr(qualifier.ordinary_launch_binding,
                        'ordinary_paid_aws_provider_identity',
                        provider_identity)
    identity = qualifier.aws_identity_from_retained_request({}, {}, scope)
    assert identity.instance_type == 'g6.48xlarge'
    assert identity.gpu_units_per_instance == 8
    assert not hasattr(identity, 'credential_profile')

    pool['accelerators'] = [['L4', 4]]
    with pytest.raises(qualifier.GuardViolation,
                       match='retained-request AWS identity'):
        qualifier.aws_identity_from_retained_request({}, {}, scope)
    pool['accelerators'] = [['L4', 8]]
    pool['instance_type'] = 'g6.xlarge'
    with pytest.raises(qualifier.GuardViolation,
                       match='retained-request AWS identity'):
        qualifier.aws_identity_from_retained_request({}, {}, scope)
    pool['instance_type'] = 'g6.48xlarge'
    pool['provider_identity']['aws_account_id'] = '210987654321'
    with pytest.raises(qualifier.GuardViolation,
                       match='retained-request AWS identity'):
        qualifier.aws_identity_from_retained_request({}, {}, scope)


def test_aws_provider_reduction_counts_logical_width_and_allows_retry_history():
    profile = dataclasses.replace(qualifier.PROFILES['small'], max_units=16)
    cluster = 'paid-e2e-1-1234567890-tenant'
    old = _aws_identity(client_token='token-old',
                        cluster_name=cluster,
                        instance_type='g6.48xlarge',
                        width=8,
                        zone='us-east-2c')
    new = _aws_identity(client_token='token-new',
                        cluster_name=cluster,
                        instance_type='g6.48xlarge',
                        width=8,
                        zone='us-east-2c')
    instance = _aws_instance(client_token='token-new',
                             cluster_name=cluster,
                             instance_type='g6.48xlarge',
                             width=8,
                             zone='us-east-2c')
    state = qualifier.parse_aws_state(
        identities=(old, new),
        profile=profile,
        service_instances=(instance,),
        service_volumes=(_aws_volume(cluster_name=cluster),))
    assert state.instance_count == 1
    assert state.running_count == 1
    assert state.gpu_units == 8
    assert state.running_gpu_units == 8
    assert state.cloud('aws').shapes == (qualifier.ProviderShapeState(
        gpu_units_per_instance=8,
        instance_count=1,
        instance_type='g6.48xlarge',
        running_count=1,
        running_gpu_units=8),)

    simultaneous_old = _aws_instance(client_token='token-old',
                                     cluster_name=cluster,
                                     instance_id='i-old',
                                     instance_type='g6.48xlarge',
                                     width=8,
                                     zone='us-east-2c',
                                     volume_id='vol-old')
    with pytest.raises(qualifier.GuardViolation,
                       match='multiple live provider effects'):
        qualifier.parse_aws_state(
            identities=(old, new),
            profile=profile,
            service_instances=(simultaneous_old, instance),
            service_volumes=(_aws_volume(cluster_name=cluster),
                             _aws_volume(cluster_name=cluster,
                                         volume_id='vol-old')))


def test_aws_empty_root_attachment_retries_then_reduces_and_records(
        monkeypatch, tmp_path):
    """A bound EC2 instance may precede its root-EBS attachment snapshot."""
    profile = qualifier.PROFILES['small']
    identity = _aws_identity()
    cluster = identity.cluster_name_on_cloud
    tags = [{
        'Key': qualifier.provision_constants.TAG_RAY_CLUSTER_NAME,
        'Value': cluster,
    }, {
        'Key': qualifier.provision_constants.TAG_SKYPILOT_CLUSTER_NAME,
        'Value': cluster,
    }, {
        'Key': qualifier.provision_constants.TAG_SKYPILOT_MANAGED,
        'Value': qualifier.provision_constants.SKYPILOT_MANAGED_TAG_VALUE,
    }]
    raw_pending = {
        'BlockDeviceMappings': [],
        'ClientToken': identity.client_token,
        'InstanceId': 'i-new',
        'InstanceLifecycle': 'spot',
        'InstanceType': identity.instance_type,
        'Placement': {
            'AvailabilityZone': identity.zone,
        },
        'State': {
            'Name': 'pending',
        },
        'Tags': tags,
    }
    raw_attached = {
        **raw_pending,
        'BlockDeviceMappings': [{
            'Ebs': {
                'DeleteOnTermination': True,
                'VolumeId': 'vol-new',
            },
        }],
    }
    raw_volume = {
        'VolumeId': 'vol-new',
        'State': 'in-use',
        'Tags': tags,
    }
    instance_reads = 0

    class Paginator:
        """Expose an attachment only in the second complete census."""

        def __init__(self, name):
            self._name = name

        def paginate(self, *, Filters):  # pylint: disable=invalid-name
            del Filters
            nonlocal instance_reads
            if self._name == 'describe_instances':
                instance_reads += 1
                instance = (raw_pending
                            if instance_reads <= 2 else raw_attached)
                return ({'Reservations': [{'Instances': [instance]}]},)
            return ({'Volumes': ([] if instance_reads <= 2 else [raw_volume])},)

    class Ec2:

        @staticmethod
        def get_paginator(name):
            return Paginator(name)

        @staticmethod
        def describe_instance_types(*, InstanceTypes):
            assert InstanceTypes == [identity.instance_type]
            return {
                'InstanceTypes': [{
                    'InstanceType': identity.instance_type,
                    'GpuInfo': {
                        'Gpus': [{
                            'Name': 'L4',
                            'Manufacturer': 'NVIDIA',
                            'Count': identity.gpu_units_per_instance,
                        }],
                    },
                }],
            }

    class Sts:

        @staticmethod
        def get_caller_identity():
            return {'Account': identity.aws_account_id}

    class Session:

        @staticmethod
        def client(name, *, region_name):
            assert region_name == identity.region
            return Sts() if name == 'sts' else Ec2()

    monkeypatch.setattr(qualifier.aws_adaptor, 'session',
                        lambda profile: Session())
    aws = qualifier.AwsObserver(profile=profile,
                                service_name='paid-e2e',
                                scope=_provider_scope())
    first_census = aws.census()
    assert first_census.service_instances[0]['volume_ids'] == ()
    with pytest.raises(qualifier.QualificationError,
                       match='AWS instance EBS attachment is not yet visible'):
        aws.reduce(first_census, (identity,))
    second_census = aws.census()
    settled_aws = aws.reduce(second_census, (identity,))
    assert second_census.service_instances[0]['volume_ids'] == ('vol-new',)
    assert settled_aws.instance_count == 1
    assert settled_aws.disk_count == 1

    database = _database_state(aws_provider_identities=(identity,))
    settled = _observation(observed_at=time.time(),
                           database=database,
                           provider=qualifier.combine_provider_states(
                               qualifier.empty_provider_state('gcp'),
                               settled_aws))
    outcomes = iter((
        qualifier.QualificationError(
            'AWS instance EBS attachment is not yet visible.'),
        settled,
    ))

    class Observer:
        """Narrow provider-observation interface with one transient miss."""

        async def snapshot(self, *, require_complete_demand_report=True):
            assert not require_complete_demand_report
            result = next(outcomes)
            if isinstance(result, Exception):
                raise result
            return result

    receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                service_name='paid-e2e',
                                profile=profile)
    observer = Observer()
    progress = qualifier.Progress()
    first = asyncio.run(
        qualifier._validated_sample(observer=observer,
                                    profile=profile,
                                    progress=progress,
                                    receipt=receipt,
                                    phase='scale'))
    second = asyncio.run(
        qualifier._validated_sample(observer=observer,
                                    profile=profile,
                                    progress=progress,
                                    receipt=receipt,
                                    phase='scale'))

    assert first is None
    assert second is not None
    assert second.provider.cloud('aws').instance_count == 1
    assert second.provider.cloud('aws').disk_count == 1
    assert [
        sample.get('observation_error_type')
        for sample in receipt._payload['samples']
    ] == ['QualificationError', None]


@pytest.mark.parametrize(('field', 'replacement'), [
    ('availability_zone', 'us-east-2b'),
    ('client_token', 'wrong-token'),
    ('cluster_name_on_cloud', 'wrong-cluster'),
    ('instance_id', ''),
    ('instance_type', 'g6.12xlarge'),
    ('market', 'on_demand'),
    ('provider_gpu_units', 4),
    ('region', 'us-west-2'),
    ('state', 'unknown'),
    ('volume_ids', []),
    ('volume_ids', ('',)),
])
def test_aws_invalid_binding_remains_fatal_before_empty_ebs_retry(
        field, replacement):
    instance = {
        **_aws_instance(state='pending'),
        'volume_ids': (),
        field: replacement,
    }

    with pytest.raises(qualifier.GuardViolation,
                       match='escaped its retained launch binding'):
        qualifier.parse_aws_state(identities=(_aws_identity(),),
                                  profile=qualifier.PROFILES['small'],
                                  service_instances=(instance,),
                                  service_volumes=())


@pytest.mark.parametrize(('field', 'replacement'), [
    ('market', 'on_demand'),
    ('client_token', 'wrong-token'),
    ('instance_type', 'g6.12xlarge'),
    ('provider_gpu_units', 4),
])
def test_aws_empty_ebs_does_not_mask_later_invalid_instance(field, replacement):
    first = {
        **_aws_instance(state='pending'),
        'volume_ids': (),
    }
    invalid = {
        **_aws_instance(instance_id='i-later'),
        field: replacement,
    }

    with pytest.raises(qualifier.GuardViolation,
                       match='escaped its retained launch binding'):
        qualifier.parse_aws_state(identities=(_aws_identity(),),
                                  profile=qualifier.PROFILES['small'],
                                  service_instances=(first, invalid),
                                  service_volumes=(_aws_volume(),))


def test_aws_empty_ebs_does_not_mask_later_unbound_volume():
    pending = {
        **_aws_instance(state='pending'),
        'volume_ids': (),
    }
    unbound = _aws_volume(cluster_name='unbound-cluster')

    with pytest.raises(qualifier.GuardViolation,
                       match='EBS effect has no retained launch binding'):
        qualifier.parse_aws_state(identities=(_aws_identity(),),
                                  profile=qualifier.PROFILES['small'],
                                  service_instances=(pending,),
                                  service_volumes=(unbound,))


def test_aws_empty_ebs_does_not_mask_later_duplicate_instance():
    pending = {
        **_aws_instance(state='pending'),
        'volume_ids': (),
    }

    with pytest.raises(qualifier.GuardViolation,
                       match='escaped its retained launch binding'):
        qualifier.parse_aws_state(identities=(_aws_identity(),),
                                  profile=qualifier.PROFILES['small'],
                                  service_instances=(pending, dict(pending)),
                                  service_volumes=())


def test_aws_empty_ebs_does_not_mask_later_gpu_cap_violation():
    first_identity = _aws_identity()
    second_identity = _aws_identity(client_token='token-second',
                                    cluster_name='paid-e2e-2-tenant')
    pending = {
        **_aws_instance(state='pending'),
        'volume_ids': (),
    }
    second = _aws_instance(client_token=second_identity.client_token,
                           cluster_name=second_identity.cluster_name_on_cloud,
                           instance_id='i-second',
                           volume_id='vol-second')

    with pytest.raises(qualifier.GuardViolation,
                       match='GPU units exceeded the armed cap'):
        qualifier.parse_aws_state(
            identities=(first_identity, second_identity),
            profile=qualifier.PROFILES['provider-canary'],
            service_instances=(pending, second),
            service_volumes=(_aws_volume(
                cluster_name=second_identity.cluster_name_on_cloud,
                volume_id='vol-second'),))


@pytest.mark.parametrize(('field', 'replacement'), [
    ('availability_zone', 'us-east-2b'),
    ('client_token', 'different-token'),
    ('cluster_name_on_cloud', 'paid-e2e-2-1234567890-tenant'),
    ('instance_id', 'i-different'),
    ('instance_type', 'g6.12xlarge'),
    ('market', 'on_demand'),
    ('region', 'us-west-2'),
])
def test_aws_duplicate_tag_snapshots_reject_immutable_identity_drift(
        field, replacement):
    previous = _aws_instance()
    current = {
        **previous,
        field: replacement,
    }

    with pytest.raises(qualifier.GuardViolation,
                       match='instance identity is contradictory'):
        qualifier._merge_aws_instance_observations(previous, current)


def test_aws_duplicate_tag_snapshots_do_not_hide_repeated_ebs_identity():
    previous = {
        **_aws_instance(),
        'volume_ids': ('vol-new', 'vol-new'),
    }

    with pytest.raises(qualifier.GuardViolation,
                       match='repeats one EBS volume identity'):
        qualifier._merge_aws_instance_observations(previous, _aws_instance())


def test_aws_duplicate_tag_snapshots_reject_ebs_identity_drift():
    previous = _aws_volume()
    current = {
        **previous,
        'region': 'us-west-2',
    }

    with pytest.raises(qualifier.GuardViolation,
                       match='volume identity is contradictory'):
        qualifier._merge_aws_volume_observations(previous, current)


def test_aws_observer_scans_both_tags_and_attests_provider_width(monkeypatch):
    cluster = 'paid-e2e-1-1234567890-tenant'
    tags = [{
        'Key': qualifier.provision_constants.TAG_RAY_CLUSTER_NAME,
        'Value': cluster,
    }, {
        'Key': qualifier.provision_constants.TAG_SKYPILOT_CLUSTER_NAME,
        'Value': cluster,
    }, {
        'Key': qualifier.provision_constants.TAG_SKYPILOT_MANAGED,
        'Value': qualifier.provision_constants.SKYPILOT_MANAGED_TAG_VALUE,
    }]
    raw_instance = {
        'BlockDeviceMappings': [{
            'Ebs': {
                'DeleteOnTermination': True,
                'VolumeId': 'vol-new',
            },
        }],
        'ClientToken': 'token-new',
        'InstanceId': 'i-new',
        'InstanceLifecycle': 'spot',
        'InstanceType': 'g6.xlarge',
        'Placement': {
            'AvailabilityZone': 'us-east-2a',
        },
        'State': {
            'Name': 'running',
        },
        'Tags': tags,
    }
    pending_instance = {
        **raw_instance,
        'BlockDeviceMappings': [],
        'State': {
            'Name': 'pending',
        },
    }
    raw_volume = {
        'VolumeId': 'vol-new',
        'State': 'in-use',
        'Tags': tags,
    }
    creating_volume = {
        **raw_volume,
        'State': 'creating',
    }
    paginator_calls = []
    instance_observations: Any = iter((pending_instance, raw_instance))
    volume_observations: Any = iter((creating_volume, raw_volume))

    class Paginator:
        """Return one frozen provider observation per tag query."""

        def __init__(self, name):
            self.name = name

        def paginate(self, *, Filters):  # pylint: disable=invalid-name
            paginator_calls.append((self.name, Filters))
            if self.name == 'describe_instances':
                observation = next(instance_observations, raw_instance)
                return ({'Reservations': [{'Instances': [observation]}]},)
            observation = next(volume_observations, raw_volume)
            return ({'Volumes': [observation]},)

    class Ec2:
        """Minimal sequential-snapshot EC2 client."""

        type_calls = 0

        @staticmethod
        def get_paginator(name):
            return Paginator(name)

        @classmethod
        def describe_instance_types(cls, *, InstanceTypes):
            cls.type_calls += 1
            assert InstanceTypes == ['g6.xlarge']
            return {
                'InstanceTypes': [{
                    'InstanceType': 'g6.xlarge',
                    'GpuInfo': {
                        'Gpus': [{
                            'Name': 'L4',
                            'Manufacturer': 'NVIDIA',
                            'Count': 1,
                        }],
                    },
                }],
            }

    class Sts:

        @staticmethod
        def get_caller_identity():
            return {'Account': '123456789012'}

    class Session:

        @staticmethod
        def client(name, *, region_name):
            assert region_name == 'us-east-2'
            return Sts() if name == 'sts' else Ec2()

    monkeypatch.setattr(qualifier.aws_adaptor, 'session',
                        lambda profile: Session())
    observer = qualifier.AwsObserver(profile=qualifier.PROFILES['small'],
                                     service_name='paid-e2e',
                                     scope=_provider_scope())
    census = observer.census()
    state = observer.reduce(census, (_aws_identity(),))
    # The two required cluster-tag queries are separate provider snapshots.
    # A pending -> running / creating -> in-use transition is not contradictory,
    # but the mixed census must not claim RUNNING capacity.
    assert state.running_gpu_units == 0
    assert census.service_instances[0]['state'] == 'pending'
    assert census.service_instances[0]['volume_ids'] == ('vol-new',)
    assert census.service_volumes[0]['state'] == 'creating'
    assert census.service_instances[0]['provider_gpu_units'] == 1
    assert Ec2.type_calls == 1
    settled_census = observer.census()
    assert observer.reduce(settled_census,
                           (_aws_identity(),)).running_gpu_units == 1
    assert Ec2.type_calls == 1
    instance_tag_queries = [
        filters[1]['Name']
        for name, filters in paginator_calls
        if name == 'describe_instances'
    ]
    assert set(instance_tag_queries) == {
        'tag:ray-cluster-name', 'tag:skypilot-cluster-name'
    }
    volume_tag_queries = [
        filters[0]['Name']
        for name, filters in paginator_calls
        if name == 'describe_volumes' and filters[0]['Name'].startswith('tag:')
    ]
    assert set(volume_tag_queries) == {
        'tag:ray-cluster-name', 'tag:skypilot-cluster-name'
    }

    raw_volume['Tags'] = [tags[0]]
    with pytest.raises(qualifier.GuardViolation,
                       match='volume escaped exact scope'):
        qualifier.AwsObserver(profile=qualifier.PROFILES['small'],
                              service_name='paid-e2e',
                              scope=_provider_scope()).census()
    raw_volume['Tags'] = tags

    original = Ec2.describe_instance_types

    def wrong_width(*, InstanceTypes):
        response = original(InstanceTypes=InstanceTypes)
        response['InstanceTypes'][0]['GpuInfo']['Gpus'][0]['Count'] = 4
        return response

    monkeypatch.setattr(Ec2, 'describe_instance_types',
                        staticmethod(wrong_width))
    with pytest.raises(qualifier.GuardViolation,
                       match='disagrees with frozen catalog'):
        qualifier.AwsObserver(profile=qualifier.PROFILES['small'],
                              service_name='paid-e2e',
                              scope=_provider_scope()).census()

    class WrongSts:

        @staticmethod
        def get_caller_identity():
            return {'Account': '999999999999'}

    class WrongSession:

        @staticmethod
        def client(name, *, region_name):
            assert region_name == 'us-east-2'
            return WrongSts() if name == 'sts' else Ec2()

    monkeypatch.setattr(qualifier.aws_adaptor, 'session',
                        lambda profile: WrongSession())
    with pytest.raises(qualifier.GuardViolation,
                       match='resolved to another account'):
        qualifier.AwsObserver(profile=qualifier.PROFILES['small'],
                              service_name='paid-e2e',
                              scope=_provider_scope()).census()


def test_aws_cleanup_counts_orphan_instance_and_ebs_without_database():
    instance = _aws_instance(instance_type='g6.48xlarge',
                             width=8,
                             zone='us-east-2c')
    state = qualifier.parse_aws_cleanup_state(service_instances=(instance,),
                                              service_volumes=({
                                                  'cluster_name_on_cloud': None,
                                                  'region': 'us-east-2',
                                                  'state': 'available',
                                                  'volume_id': 'vol-orphan',
                                              },))
    assert state.instance_count == 1
    assert state.gpu_units == 8
    assert state.disk_count == 1
    assert state.cloud('aws').shapes[0].gpu_units_per_instance == 8


def test_aws_observer_scans_every_frozen_catalog_region(monkeypatch):
    calls = []

    class Paginator:

        def __init__(self, region, name):
            self.region = region
            self.name = name

        def paginate(self, *, Filters):  # pylint: disable=invalid-name
            calls.append((self.region, self.name, Filters))
            if self.name == 'describe_instances':
                return ({'Reservations': []},)
            return ({'Volumes': []},)

    class Ec2:

        def __init__(self, region):
            self.region = region

        def get_paginator(self, name):
            return Paginator(self.region, name)

    class Sts:

        @staticmethod
        def get_caller_identity():
            return {'Account': '123456789012'}

    profiles = {
        'east-profile': 'us-east-2',
        'west-profile': 'us-west-2',
    }

    class Session:

        def __init__(self, region):
            self.region = region

        def client(self, name, *, region_name):
            assert region_name == self.region
            return Sts() if name == 'sts' else Ec2(self.region)

    monkeypatch.setattr(qualifier.aws_adaptor, 'session',
                        lambda profile: Session(profiles[profile]))
    shapes = (*_provider_scope().catalog_shapes,
              qualifier.CatalogShape(cloud='aws',
                                     region='us-west-2',
                                     zone='us-west-2a',
                                     instance_type='g6.xlarge',
                                     gpu_units_per_instance=1))
    scope = _provider_scope(
        aws_regions=(qualifier.AwsRegionScope(aws_account_id='123456789012',
                                              credential_profile='east-profile',
                                              region='us-east-2'),
                     qualifier.AwsRegionScope(aws_account_id='123456789012',
                                              credential_profile='west-profile',
                                              region='us-west-2')),
        catalog_shapes=tuple(sorted(shapes, key=qualifier._catalog_shape_key)))
    observer = qualifier.AwsObserver(profile=qualifier.PROFILES['small'],
                                     service_name='paid-e2e',
                                     scope=scope)
    census = observer.census()
    assert census.service_instances == ()
    assert census.service_volumes == ()
    for region in profiles.values():
        regional = [call for call in calls if call[0] == region]
        assert sum(call[1] == 'describe_instances' for call in regional) == 2
        assert sum(call[1] == 'describe_volumes' for call in regional) == 2


def test_aws_observer_bounds_parallel_regions_and_aggregates_in_order(
        monkeypatch):
    regions = ('ap-south-1', 'eu-west-1', 'us-east-2', 'us-west-2')
    scopes = tuple(
        qualifier.AwsRegionScope(aws_account_id='123456789012',
                                 credential_profile=f'{region}-profile',
                                 region=region) for region in regions)
    shapes = tuple(
        qualifier.CatalogShape(cloud='aws',
                               region=region,
                               zone=f'{region}a',
                               instance_type='g6.xlarge',
                               gpu_units_per_instance=1) for region in regions)
    scope = _provider_scope(aws_regions=scopes,
                            catalog_shapes=tuple(
                                sorted(shapes,
                                       key=qualifier._catalog_shape_key)))
    observer = qualifier.AwsObserver(profile=qualifier.PROFILES['small'],
                                     service_name='paid-e2e',
                                     scope=scope)
    monkeypatch.setattr(qualifier, '_AWS_CENSUS_MAX_WORKERS', 2)
    lock = threading.Lock()
    active = 0
    peak = 0

    def read_region(region_scope):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        # Reverse completion order to prove aggregation is not completion-ordered.
        time.sleep(0.01 * (len(regions) - regions.index(region_scope.region)))
        with lock:
            active -= 1
        suffix = len(regions) - regions.index(region_scope.region)
        return qualifier._AwsRegionCensus(
            region=region_scope.region,
            service_instances=({
                'instance_id': f'i-{suffix}'
            },),
            service_volumes=({
                'volume_id': f'vol-{suffix}'
            },),
            retained_volume_ids=(f'vol-{suffix}',),
            instance_type_widths=(('g6.xlarge', 1),))

    monkeypatch.setattr(observer, '_read_region', read_region)
    instances, volumes = observer._service_census()

    assert peak == 2
    assert [item['instance_id'] for item in instances
           ] == ['i-1', 'i-2', 'i-3', 'i-4']
    assert [item['volume_id'] for item in volumes
           ] == ['vol-1', 'vol-2', 'vol-3', 'vol-4']
    assert observer.retained_volume_ids() == {
        region: [f'vol-{len(regions) - regions.index(region)}']
        for region in regions
    }


def test_aws_cleanup_census_retains_exact_legacy_ebs_identity(monkeypatch):

    class Paginator:

        def __init__(self, name):
            self.name = name

        def paginate(self, *, Filters):  # pylint: disable=invalid-name
            if self.name == 'describe_instances':
                return ({'Reservations': []},)
            if Filters[0]['Name'] == 'volume-id':
                assert Filters[0]['Values'] == ['vol-legacy']
                return ({
                    'Volumes': [{
                        'VolumeId': 'vol-legacy',
                        'State': 'available',
                        'Tags': [],
                    }],
                },)
            return ({'Volumes': []},)

    class Ec2:

        @staticmethod
        def get_paginator(name):
            return Paginator(name)

    class Sts:

        @staticmethod
        def get_caller_identity():
            return {'Account': '123456789012'}

    class Session:

        @staticmethod
        def client(name, *, region_name):
            assert region_name == 'us-east-2'
            return Sts() if name == 'sts' else Ec2()

    monkeypatch.setattr(qualifier.aws_adaptor, 'session',
                        lambda profile: Session())
    observer = qualifier.AwsObserver(
        profile=qualifier.PROFILES['small'],
        service_name='paid-e2e',
        scope=_provider_scope(),
        retained_volume_ids_by_region={'us-east-2': ['vol-legacy']})
    census = observer.census()
    assert census.service_instances == ()
    assert census.service_volumes == ({
        'cluster_name_on_cloud': None,
        'region': 'us-east-2',
        'state': 'available',
        'volume_id': 'vol-legacy',
    },)
    assert qualifier.parse_aws_cleanup_state(
        service_instances=census.service_instances,
        service_volumes=census.service_volumes).disk_count == 1


def test_aws_cleanup_census_batches_retained_ebs_identity(monkeypatch):
    retained_volume_ids = [f'vol-{index:03d}' for index in range(201)]
    exact_lookups = []

    class Paginator:

        def __init__(self, name):
            self.name = name

        def paginate(self, *, Filters):  # pylint: disable=invalid-name
            if self.name == 'describe_instances':
                return ({'Reservations': []},)
            if Filters[0]['Name'] == 'volume-id':
                values = Filters[0]['Values']
                if len(values) > 200:
                    raise RuntimeError(
                        'AWS rejects more than 200 filter values')
                exact_lookups.append(values)
            return ({'Volumes': []},)

    class Ec2:

        @staticmethod
        def get_paginator(name):
            return Paginator(name)

    class Sts:

        @staticmethod
        def get_caller_identity():
            return {'Account': '123456789012'}

    class Session:

        @staticmethod
        def client(name, *, region_name):
            assert region_name == 'us-east-2'
            return Sts() if name == 'sts' else Ec2()

    monkeypatch.setattr(qualifier.aws_adaptor, 'session',
                        lambda profile: Session())
    observer = qualifier.AwsObserver(profile=qualifier.PROFILES['small'],
                                     service_name='paid-e2e',
                                     scope=_provider_scope(),
                                     retained_volume_ids_by_region={
                                         'us-east-2': retained_volume_ids,
                                     })

    census = observer.census()

    assert census.service_instances == ()
    assert census.service_volumes == ()
    assert [len(batch) for batch in exact_lookups] == [200, 1]
    assert [volume_id for batch in exact_lookups for volume_id in batch
           ] == retained_volume_ids


def test_optional_aws_receipt_never_blocks_tag_scoped_cleanup(tmp_path):
    missing = tmp_path / 'missing.json'
    assert qualifier.read_optional_aws_volume_ids_receipt(missing,
                                                          'paid-e2e') == {}
    missing.write_text('{partial', encoding='utf-8')
    assert qualifier.read_optional_aws_volume_ids_receipt(missing,
                                                          'paid-e2e') == {}


def test_schema_ten_receipt_is_cleanup_compatible_but_cannot_qualify(tmp_path):
    """Cleanup may span versions; qualification evidence may not."""
    receipt = tmp_path / 'schema-10.json'
    receipt.write_text(json.dumps({
        'schema_version': 10,
        'service_name': 'paid-e2e',
        'aws_retained_volume_ids': {
            'us-east-2': ['vol-0001'],
        },
    }),
                       encoding='utf-8')

    assert qualifier.read_aws_volume_ids_receipt(receipt, 'paid-e2e') == {
        'us-east-2': ['vol-0001'],
    }
    with pytest.raises(qualifier.QualificationError,
                       match='malformed|unavailable'):
        qualifier._read_qualification_evidence(
            receipt, qualifier.ExpectationKind.ECONOMIC)


def test_provider_guard_rejects_on_demand_wrong_shape_and_overshoot():
    profile = qualifier.PROFILES['small']
    valid = _instance()
    state = qualifier.parse_gcp_state(
        service_name='paid-e2e',
        expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
        profile=profile,
        instances=[valid],
        disks=[])
    assert state.running_count == 1
    assert state.cluster_names == frozenset({'paid-e2e-1'})

    on_demand = {**valid, 'scheduling': {'provisioningModel': 'STANDARD'}}
    with pytest.raises(qualifier.QualificationError, match='not Spot'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
            profile=profile,
            instances=[on_demand],
            disks=[])

    wrong_shape = {**valid, 'machineType': 'machineTypes/g2-standard-8'}
    with pytest.raises(qualifier.QualificationError, match='wrong shape'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
            profile=profile,
            instances=[wrong_shape],
            disks=[])

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
            disks=[])


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
        }])
    assert state.instance_count == 0
    assert state.gpu_units == 0
    assert state.cluster_names == frozenset()
    assert state.cloud('gcp').instance_count == 0


def test_provider_guard_rejects_unbound_service_effects():
    with pytest.raises(qualifier.GuardViolation,
                       match='without a durable launch binding'):
        qualifier.parse_gcp_state(service_name='paid-e2e',
                                  expected_cluster_zones={},
                                  profile=qualifier.PROFILES['small'],
                                  instances=[_instance()],
                                  disks=[])


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
                                  disks=[])


def test_provider_guard_rejects_duplicate_and_multiple_instance_effects():
    instance = _instance()
    with pytest.raises(qualifier.GuardViolation,
                       match='duplicate provider identity'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
            profile=qualifier.PROFILES['small'],
            instances=[instance, dict(instance)],
            disks=[])

    with pytest.raises(qualifier.GuardViolation,
                       match='multiple GCP instance effects'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
            profile=qualifier.PROFILES['small'],
            instances=[instance, _instance(suffix='87654321')],
            disks=[])


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
            disks=[orphan])

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
            disks=[labelled_orphan])


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
            disks=[disk('1234abcd'), disk('87654321')])


def test_provider_guard_accepts_cross_region_but_enforces_binding_zone():
    east_zone_b = {
        **_instance(cluster_name='paid-e2e-2'),
        'zone': 'zones/us-east1-b',
    }
    state = qualifier.parse_gcp_state(service_name='paid-e2e',
                                      expected_cluster_zones={
                                          'paid-e2e-1': 'us-central1-a',
                                          'paid-e2e-2': 'us-east1-b',
                                      },
                                      profile=qualifier.PROFILES['small'],
                                      instances=[_instance(), east_zone_b],
                                      disks=[])
    assert state.instance_count == 2

    with pytest.raises(qualifier.GuardViolation, match='binding zone'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-2': 'us-central1-a'},
            profile=qualifier.PROFILES['small'],
            instances=[east_zone_b],
            disks=[])


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


def _cleanup_wait_fixture(monkeypatch,
                          tmp_path,
                          census,
                          *,
                          timeout_seconds=2,
                          poll_seconds=0):
    base = _provider_scope()
    scope = dataclasses.replace(
        base,
        max_live_paid_gpu_units=1,
        providers=('gcp',),
        aws_location_scope=None,
        aws_regions=(),
        qualification_profile='provider-canary',
        qualification_projection_sha256=(
            qualifier._qualification_projection_sha256(
                source_sha256=base.qualification_source_sha256,
                profile=qualifier.PROFILES['provider-canary'],
                providers=('gcp',))),
        catalog_shapes=tuple(
            shape for shape in base.catalog_shapes if shape.cloud == 'gcp'))
    scope_path = tmp_path / 'scope.json'
    qualification_path = tmp_path / 'qualification.json'
    output_path = tmp_path / 'cleanup.json'
    qualifier.write_provider_scope(scope_path, 'paid-e2e', scope)
    qualification_path.write_text('{"receipt": "test"}\n', encoding='utf-8')

    class Postgres:
        """Exact-zero database observer double."""

        def __init__(self, *_args):
            pass

        def bind_provider_scope(self, observed_scope):
            assert observed_scope == scope

        @staticmethod
        def cleanup_debits():
            return (0, 0, 0)

        @staticmethod
        def close():
            pass

    census_fn = census

    class Gcp:

        @staticmethod
        def census():
            return census_fn()

    monkeypatch.setenv('TEST_DATABASE_URL', 'postgresql://unused')
    monkeypatch.setattr(qualifier, 'PostgresObserver', Postgres)
    monkeypatch.setattr(qualifier, '_provider_observers', lambda **_kwargs:
                        (Gcp(), None))
    args = types.SimpleNamespace(
        postgres_url_env='TEST_DATABASE_URL',
        service_name='paid-e2e',
        scope=str(scope_path),
        receipt=str(qualification_path),
        output=str(output_path),
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )
    return args, output_path, qualification_path, scope


def _empty_gcp_census():
    return qualifier.ProviderCensus(instances=[], disks=[], operations=[])


def test_wait_cleanup_writes_identity_bound_sustained_zero_receipt(
        monkeypatch, tmp_path):
    args, output_path, qualification_path, scope = _cleanup_wait_fixture(
        monkeypatch, tmp_path, _empty_gcp_census)

    asyncio.run(qualifier.wait_for_cleanup(args))

    payload = json.loads(output_path.read_text(encoding='utf-8'))
    assert payload['outcome'] == 'passed'
    assert payload['service_hash'] == scope.service_hash
    assert payload['expected_providers'] == ['gcp']
    assert payload['zero_samples'] == 3
    assert [sample['exact_zero'] for sample in payload['samples']
           ] == [True, True, True]
    assert payload['qualification_receipt_sha256'] == hashlib.sha256(
        qualification_path.read_bytes()).hexdigest()


def test_wait_cleanup_retries_provider_miss_and_restarts_zero_streak(
        monkeypatch, tmp_path):
    outcomes = [
        None, None,
        RuntimeError('credential material'), None, None, None
    ]

    def census():
        outcome = outcomes.pop(0)
        if outcome is not None:
            raise outcome
        return _empty_gcp_census()

    args, output_path, _, _ = _cleanup_wait_fixture(monkeypatch, tmp_path,
                                                    census)

    asyncio.run(qualifier.wait_for_cleanup(args))

    payload_text = output_path.read_text(encoding='utf-8')
    payload = json.loads(payload_text)
    assert payload['outcome'] == 'passed'
    assert payload['zero_samples'] == 3
    assert len(payload['samples']) == 6
    assert [sample['exact_zero'] for sample in payload['samples']
           ] == [True, True, False, True, True, True]
    miss = payload['samples'][2]
    assert miss['observation_error_type'] == 'RuntimeError'
    assert miss['zero_samples'] == 0
    assert 'credential material' not in payload_text


def test_wait_cleanup_persistent_provider_miss_fails_at_deadline(
        monkeypatch, tmp_path):
    attempts = []

    def census():
        attempts.append(None)
        raise RuntimeError('credential material')

    args, output_path, _, _ = _cleanup_wait_fixture(monkeypatch,
                                                    tmp_path,
                                                    census,
                                                    timeout_seconds=0.2,
                                                    poll_seconds=0.01)

    with pytest.raises(
            qualifier.QualificationError,
            match='Teardown left paid database debits or scoped provider'):
        asyncio.run(qualifier.wait_for_cleanup(args))

    payload_text = output_path.read_text(encoding='utf-8')
    payload = json.loads(payload_text)
    assert payload['outcome'] == 'failed'
    assert payload['error_type'] == 'QualificationError'
    assert payload['zero_samples'] == 0
    assert len(attempts) >= 2
    assert len(payload['samples']) == len(attempts)
    assert all(sample['observation_error_type'] == 'RuntimeError'
               for sample in payload['samples'])
    assert all(sample['exact_zero'] is False for sample in payload['samples'])
    assert 'credential material' not in payload_text


@pytest.mark.parametrize('child_status', ('RUNNING', 'DONE'))
def test_cleanup_census_attributes_bulk_insert_parent_by_operation_lineage(
        child_status):
    operation_group_id = '3fd64c92-0559-4aac-85ea-abd455118d1d'
    state = qualifier.parse_gcp_cleanup_state(
        service_name='paid-e2e',
        instances=[],
        disks=[],
        operations=[{
            'name': 'provider-generated-bulk-operation',
            'operationType': 'bulkInsert',
            'operationGroupId': operation_group_id,
            'status': 'RUNNING',
            'targetLink': ('https://compute.googleapis.com/compute/v1/'
                           'projects/project'),
        }, {
            'name': 'provider-generated-child-operation',
            'operationType': 'insert',
            'operationGroupId': operation_group_id,
            'status': child_status,
            'targetLink': ('https://compute.googleapis.com/compute/v1/'
                           'projects/project/zones/us-central1-a/instances/'
                           'paid-e2e-1-head-1234abcd-compute'),
        }])
    # An active child remains one target, while a terminal child leaves its
    # still-running bulk parent as one in-flight lineage.
    assert state.inflight_operation_count == 1


def test_cleanup_census_does_not_guess_bulk_insert_parent_from_name():
    operation_group_id = '97d626fe-7df7-4c0d-81de-3e17fbefa589'
    state = qualifier.parse_gcp_cleanup_state(
        service_name='paid-e2e',
        instances=[],
        disks=[],
        operations=[
            {
                # Neither an operation-name prefix nor a shared group with
                # another service is ownership evidence for this service.
                'name': 'paid-e2e-bulk-insert',
                'operationType': 'bulkInsert',
                'operationGroupId': operation_group_id,
                'status': 'RUNNING',
                'targetLink': ('https://compute.googleapis.com/compute/v1/'
                               'projects/project'),
            },
            {
                'name': 'unrelated-child-operation',
                'operationType': 'insert',
                'operationGroupId': operation_group_id,
                'status': 'DONE',
                'targetLink': ('https://compute.googleapis.com/compute/v1/'
                               'projects/project/zones/us-central1-a/instances/'
                               'unrelated-1-head-1234abcd-compute'),
            }
        ])
    assert state.inflight_operation_count == 0


def test_cleanup_census_ignores_terminal_bulk_insert_lineage():
    operation_group_id = '3fd64c92-0559-4aac-85ea-abd455d7b607'
    state = qualifier.parse_gcp_cleanup_state(
        service_name='paid-e2e',
        instances=[],
        disks=[],
        operations=[{
            'name': 'provider-generated-bulk-operation',
            'operationType': 'bulkInsert',
            'operationGroupId': operation_group_id,
            'status': 'DONE',
            'targetLink': ('https://compute.googleapis.com/compute/v1/'
                           'projects/project'),
        }, {
            'name': 'provider-generated-child-operation',
            'operationType': 'insert',
            'operationGroupId': operation_group_id,
            'status': 'DONE',
            'targetLink': ('https://compute.googleapis.com/compute/v1/'
                           'projects/project/zones/us-central1-a/instances/'
                           'paid-e2e-1-head-1234abcd-compute'),
        }])
    assert state.inflight_operation_count == 0


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
        }])
    assert state.instance_count == 1
    assert state.disk_count == 1
    assert state.inflight_operation_count == 1
    assert state.cluster_names == frozenset({cloud_name})


@pytest.mark.parametrize('child_status', ('RUNNING', 'DONE'))
def test_provider_reducer_attributes_bound_bulk_insert_parent_once(
        child_status):
    operation_group_id = 'c935584d-d1a3-4db7-a825-f799c34cc454'
    child = {
        'operationType': 'insert',
        'operationGroupId': operation_group_id,
        'status': child_status,
        'targetLink': ('https://compute.googleapis.com/compute/v1/projects/'
                       'project/zones/us-central1-a/instances/'
                       'paid-e2e-1-head-1234abcd-compute'),
    }
    parent = {
        'name': 'provider-generated-bulk-operation',
        'operationType': 'bulkInsert',
        'operationGroupId': operation_group_id,
        'status': 'RUNNING',
        'targetLink': ('https://compute.googleapis.com/compute/v1/projects/'
                       'project'),
    }
    state = qualifier.parse_gcp_state(
        service_name='paid-e2e',
        expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
        profile=qualifier.PROFILES['small'],
        instances=[],
        disks=[],
        operations=[parent, child])
    assert state.inflight_operation_count == 1

    wrong_zone_child = {
        **child,
        'targetLink': child['targetLink'].replace('us-central1-a',
                                                  'us-east1-b'),
    }
    with pytest.raises(qualifier.GuardViolation,
                       match='outside its binding zone'):
        qualifier.parse_gcp_state(
            service_name='paid-e2e',
            expected_cluster_zones={'paid-e2e-1': 'us-central1-a'},
            profile=qualifier.PROFILES['small'],
            instances=[],
            disks=[],
            operations=[parent, wrong_zone_child])

    with pytest.raises(qualifier.GuardViolation,
                       match='without a durable launch binding'):
        qualifier.parse_gcp_state(service_name='paid-e2e',
                                  expected_cluster_zones={},
                                  profile=qualifier.PROFILES['small'],
                                  instances=[],
                                  disks=[],
                                  operations=[parent, child])


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
    assert census.priority_units == (qualifier.PaidClaimPriorityUnits(
        priority=50, gpu_units=2),)

    for invalid_priority in (49, 51, True, None):
        with pytest.raises(qualifier.GuardViolation, match='not priority 50'):
            qualifier.paid_claim_census([{
                **claim, 'priority': invalid_priority
            }])


def test_paid_claim_census_aggregates_multi_gpu_claim_units_by_priority():
    claims = [{
        'priority': 50,
        'capacity_plan_generation': 9,
        'capacity_plan_sha256': 'a' * 64,
        'persisted_plan_sha256': 'a' * 64,
        'capacity_plan_accelerator': 'L4',
        'capacity_plan_units': width,
    } for width in (4, 8)]

    census = qualifier.paid_claim_census(claims)

    assert census.gpu_units == 12
    assert census.priority_units == (qualifier.PaidClaimPriorityUnits(
        priority=50, gpu_units=12),)


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
        controller=controller,
        claimed_units=1,
        claim_priority_units=(
            qualifier.PaidClaimPriorityUnits(priority=50, gpu_units=1),)))
    receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                service_name='paid-e2e',
                                profile=qualifier.PROFILES['small'])
    receipt.sample('scale', observation)

    assert receipt._payload['schema_version'] == 13
    assert receipt._payload['request_priority'] == 50
    assert receipt._payload['scale_slo_seconds'] == 300
    assert receipt._payload['scale_timeout_seconds'] == 900
    sample = receipt._payload['samples'][0]
    assert sample['phase'] == 'scale'
    assert sample['observation_started_at'] == 999.5
    assert sample['observation_finished_at'] == 1000
    assert sample['observation_duration_seconds'] == 0.5
    assert sample['controller_pid'] == 321
    assert sample['controller_owner_epoch'] == 12
    assert sample['claimed_units'] == 1
    assert sample['paid_claim_priority_units'] == [{
        'priority': 50,
        'gpu_units': 1,
    }]
    assert sample['provider_instances'] == 0
    assert sample['provider_gpu_units'] == 0
    assert sample['provider_running_gpu_units'] == 0
    assert set(sample['provider_by_cloud']) == {'aws', 'gcp'}
    assert all(
        cloud['shapes'] == [] for cloud in sample['provider_by_cloud'].values())
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
    observer._provider_scope = _provider_scope(service_hash='service-hash',
                                               lifecycle_epoch=1,
                                               service_version=1,
                                               project_id='project-a')
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


def test_campaign_membership_reads_current_attempts_from_observer_postgres():
    prefix = 'postgres-membership'
    request_keys = qualifier._campaign_request_key_sha256s(prefix, 3)
    rows = [{
        'request_key_sha256': request_key,
        'state': 'SUCCEEDED',
    } for request_key in request_keys]
    seen = {}

    class Result:

        def mappings(self):
            return self

        def all(self):
            return rows

    class Connection:

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def execute(statement, params):
            seen['statement'] = str(statement)
            seen['params'] = params
            return Result()

    class Engine:

        @staticmethod
        def connect():
            return Connection()

    observer = object.__new__(qualifier.PostgresObserver)
    observer._engine = Engine()
    observer._service_name = 'paid-e2e'
    observer._provider_scope = _provider_scope(service_hash='service-hash',
                                               lifecycle_epoch=1,
                                               service_version=1,
                                               project_id='project-a')

    assert observer.campaign_terminal_membership(
        prefix, 3) == qualifier._campaign_manifest_sha256(prefix, 3)
    assert seen['params'] == {
        'service_name': 'paid-e2e',
        'service_hash': 'service-hash',
        'request_keys': request_keys,
    }
    assert 'attempt.attempt_id = request.current_attempt_id' in seen[
        'statement']


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


def test_exact_provider_free_unbound_paid_debit_remains_visible_during_scale(
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
        claim_priority_units=(qualifier.PaidClaimPriorityUnits(priority=50,
                                                               gpu_units=1),),
        demand_units=4,
        provider_free_unbound_replica_ids=(7,)),
                           load_balancer=_load_balancer_state(demand_units=4))
    exact_zero_without_marker = _observation()
    assert exact_zero_without_marker.is_exact_zero()
    assert not dataclasses.replace(
        exact_zero_without_marker,
        database=dataclasses.replace(
            exact_zero_without_marker.database,
            provider_free_unbound_replica_ids=(7,))).is_exact_zero()

    class Observer:
        """Return one provider-free phase-A observation."""

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
    assert observed is phase_a
    sample = receipt._payload['samples'][-1]
    assert sample['phase'] == 'scale'
    assert sample['provider_free_unbound_replicas'] == 1
    assert 'observation_error_type' not in sample


def test_phase_a_observation_cannot_hide_a_provider_effect(tmp_path):
    phase_a_with_effect = _observation(
        database=_database_state(
            paid_debit_units=1,
            claimed_units=1,
            claim_priority_units=(qualifier.PaidClaimPriorityUnits(
                priority=50, gpu_units=1),),
            demand_units=4,
            provider_free_unbound_replica_ids=(7,)),
        provider=_provider_state(instance_count=1,
                                 cluster_names=frozenset({'paid-e2e-7'})),
        load_balancer=_load_balancer_state(demand_units=4))

    class Observer:
        """Return a phase-A observation that already has a provider effect."""

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

        @staticmethod
        async def request_telemetry():
            return _request_telemetry()

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


def test_provider_baseline_fails_on_first_valid_nonzero_sample(tmp_path):
    profile = dataclasses.replace(qualifier.PROFILES['small'], poll_seconds=0)
    observations = [
        _observation(load_balancer=_load_balancer_state(demand_units=1)),
        _observation(),
    ]

    class Observer:

        @staticmethod
        async def request_telemetry():
            return _request_telemetry()

        async def snapshot(self, *, require_complete_demand_report=True):
            assert require_complete_demand_report
            return observations.pop(0)

    with pytest.raises(qualifier.QualificationError,
                       match='pre-demand provider observation is nonzero'):
        asyncio.run(
            qualifier._wait_for_joined_baseline(
                observer=Observer(),
                profile=profile,
                progress=qualifier.Progress(),
                receipt=qualifier.Receipt(path=tmp_path / 'receipt.json',
                                          service_name='paid-e2e',
                                          profile=profile)))
    assert len(observations) == 1


def test_request_baseline_fails_on_first_valid_nonzero_sample(tmp_path):
    profile = dataclasses.replace(qualifier.PROFILES['small'], poll_seconds=0)
    telemetry = [
        _request_telemetry(queue_depth=1, state_counts={'ACCEPTED': 1}),
        _request_telemetry(),
    ]

    class Observer:

        async def request_telemetry(self):
            return telemetry.pop(0)

        @staticmethod
        async def snapshot(**_kwargs):
            raise AssertionError('provider sampling must fail closed first')

    with pytest.raises(qualifier.QualificationError,
                       match='pre-demand request telemetry is nonzero'):
        asyncio.run(
            qualifier._wait_for_joined_baseline(
                observer=Observer(),
                profile=profile,
                progress=qualifier.Progress(),
                receipt=qualifier.Receipt(path=tmp_path / 'receipt.json',
                                          service_name='paid-e2e',
                                          profile=profile)))
    assert len(telemetry) == 1


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
        database=_database_state(
            paid_debit_units=1,
            # A successful provider request releases its admission claim.  A
            # retained immutable binding, not a current claim or plan head,
            # remains the proof for the live provider effect.
            claimed_units=0,
            bound_cluster_zones=(('paid-e2e-1', 'us-central1-a'),)))
    qualifier.validate_observation(bound, profile)


def test_provider_observation_allows_wall_clock_rollback():
    observation = dataclasses.replace(_observation(), observed_started_at=1001)
    qualifier.validate_observation(observation, qualifier.PROFILES['small'])


def test_provider_observation_rejects_reordered_monotonic_interval():
    observation = dataclasses.replace(_observation(),
                                      observed_started_monotonic=1001)
    with pytest.raises(qualifier.QualificationError,
                       match='invalid sample interval'):
        qualifier.validate_observation(observation, qualifier.PROFILES['small'])


def test_observation_census_latency_is_subtracted_from_poll_interval(
        monkeypatch):
    delays = []

    async def sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(qualifier.asyncio, 'sleep', sleep)
    observation = _observation(observed_monotonic=2000)

    asyncio.run(qualifier._sleep_after_observation(observation, 10))

    assert delays == [9.5]


def test_provider_canary_rejects_wrong_cloud_durable_binding():
    profile = qualifier.PROFILES['provider-canary']
    expectation = qualifier.provider_expectation(profile, 'aws')
    observation = _observation(database=_database_state(
        bound_cluster_zones=(('paid-e2e-gcp', 'us-central1-a'),)))

    with pytest.raises(qualifier.GuardViolation,
                       match='outside the qualification scope'):
        qualifier.validate_observation(observation, profile, expectation)


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


def test_progress_records_scale_slo_and_sustained_exact_zero():
    profile = qualifier.PROFILES['small']
    gcp_name = _provider_cluster_names('gcp', 1)[0]
    aws_name = _provider_cluster_names('aws', 1)[0]
    scaled = _observation(
        observed_at=1000,
        database=_database_state(
            paid_debit_units=2,
            bound_cluster_zones=((gcp_name, 'us-central1-a'),),
            aws_provider_identities=(_aws_identity(client_token='token-aws-0',
                                                   cluster_name=aws_name),)),
        provider=_cross_cloud_provider_state(gcp_running_count=1,
                                             aws_running_count=1),
        load_balancer=_load_balancer_state(demand_units=4, ready_replicas=2))
    qualifier.validate_observation(scaled, profile)
    progress = qualifier.Progress(scale_started_monotonic=900)
    progress.observe(scaled, profile)
    assert progress.scale_reached_monotonic == 1000
    assert progress.scale_slo_met is True

    zero = _observation(observed_at=1100)
    for observed_at in (1100, 1105, 1461):
        zero = _observation(observed_at=observed_at)
        progress.observe(zero, profile)
        progress.observe_zero(zero)
    assert progress.drain_complete(zero, profile)

    # Wall-clock movement cannot change the monotonic diagnostic result. A
    # provider that converges after the benchmark is still correct before the
    # broader timeout; the receipt records the missed benchmark.
    late = dataclasses.replace(scaled, observed_at=800, observed_monotonic=1600)
    late_progress = qualifier.Progress(scale_started_monotonic=1000)
    late_progress.observe(late, profile)
    assert late_progress.scale_reached_monotonic == 1600
    assert late_progress.scale_slo_met is False


def test_progress_scale_counts_physical_vms_not_logical_gpu_units():
    profile = qualifier.PROFILES['scale']
    observation = _observation(provider=_cross_cloud_provider_state(
        gcp_running_count=25, aws_running_count=25, gpu_units_per_instance=2))
    progress = qualifier.Progress(scale_started_monotonic=900)

    progress.observe(observation, profile)

    assert progress.peak_running == 50
    assert progress.peak_running_gpu_units == 100
    assert progress.scale_reached_monotonic is None


def test_economic_progress_does_not_require_an_artificial_provider_mix():
    profile = qualifier.PROFILES['scale']
    observation = _observation(provider=_cross_cloud_provider_state(
        gcp_running_count=100, aws_running_count=0))
    progress = qualifier.Progress(scale_started_monotonic=900)

    progress.observe(observation, profile)

    assert progress.peak_running == 100
    assert progress.peak_running_by_cloud == {'gcp': 100, 'aws': 0}
    assert progress.scale_reached_monotonic == 1000


def test_provider_canary_enforces_exact_one_gpu_cap():
    names = _provider_cluster_names('gcp', 2)
    observation = _observation(
        database=_database_state(bound_cluster_zones=tuple(
            (name, 'us-central1-a') for name in names)),
        provider=_cross_cloud_provider_state(gcp_running_count=2,
                                             aws_running_count=0))
    expectation = qualifier.provider_expectation(
        qualifier.PROFILES['provider-canary'], 'gcp')

    with pytest.raises(qualifier.GuardViolation, match='armed GPU cap'):
        qualifier.validate_observation(observation,
                                       qualifier.PROFILES['provider-canary'],
                                       expectation)


def test_scale_survives_transient_observer_blackout(tmp_path):
    """Model pressure surviving observer loss from 20 to 64 VMs."""
    gcp_names = _provider_cluster_names('gcp', 32)
    aws_names = _provider_cluster_names('aws', 32)
    profile = dataclasses.replace(qualifier.PROFILES['scale'],
                                  exact_requests=64,
                                  request_concurrency=64,
                                  minimum_running=64,
                                  poll_seconds=0,
                                  scale_timeout_seconds=2)
    database = _database_state(
        paid_debit_units=64,
        claimed_units=0,
        demand_units=64,
        bound_cluster_zones=tuple(
            (name, 'us-central1-a') for name in gcp_names),
        aws_provider_identities=tuple(
            _aws_identity(client_token=f'token-aws-{index}', cluster_name=name)
            for index, name in enumerate(aws_names)))
    now = qualifier.time.time()
    observations = [
        _observation(observed_at=now + 1,
                     database=dataclasses.replace(
                         database, provider_free_unbound_replica_ids=(7,)),
                     provider=_cross_cloud_provider_state(gcp_running_count=10,
                                                          aws_running_count=10),
                     load_balancer=_load_balancer_state(
                         demand_units=64,
                         unique_job_arrivals_60s=64,
                         unique_job_arrivals_300s=64)),
        qualifier.QualificationError('transient observer blackout'),
        _observation(observed_at=now + 2,
                     database=database,
                     provider=_cross_cloud_provider_state(gcp_running_count=32,
                                                          aws_running_count=32),
                     load_balancer=_load_balancer_state(
                         demand_units=64,
                         ready_replicas=20,
                         unique_job_arrivals_60s=64,
                         unique_job_arrivals_300s=64)),
    ]

    class Observer:
        """Attributed telemetry with transient provider observation loss."""

        @staticmethod
        async def request_telemetry():
            # Before a replica is READY, all exact stimulus identities are
            # attributable but queued and therefore have no ledger row.
            return _request_telemetry(queue_depth=64)

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
        traffic = asyncio.create_task(keep_alive.wait())
        try:
            await qualifier._wait_for_scale(
                observer=Observer(),
                profile=profile,
                progress=progress,
                receipt=receipt,
                traffic=traffic,
                baseline=_request_telemetry(),
                campaign_progress=(await _campaign_progress(profile)))
            return progress, receipt
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)

    progress, receipt = asyncio.run(exercise())
    assert progress.peak_running == 64
    assert progress.scale_reached_monotonic == now + 2
    samples = receipt._payload['samples']
    assert samples[0]['provider_running'] == 20
    assert samples[0]['provider_free_unbound_replicas'] == 1
    assert samples[2]['provider_free_unbound_replicas'] == 0
    assert [sample.get('observation_error_type') for sample in samples
           ] == [None, 'QualificationError', None]


def test_scale_observes_provider_with_incomplete_replica_occupancy(tmp_path):
    """Replica probe gaps cannot hide exact resident campaign pressure."""
    profile = dataclasses.replace(qualifier.PROFILES['scale'],
                                  minimum_running=100,
                                  poll_seconds=0,
                                  scale_timeout_seconds=1)
    started_monotonic = time.monotonic()
    started_at = time.time()
    cluster_names = _provider_cluster_names('gcp', 100)
    observation = _observation(
        observed_at=started_at + 1,
        observed_monotonic=started_monotonic + 1,
        database=_database_state(
            paid_debit_units=100,
            demand_units=800,
            bound_cluster_zones=tuple(
                (name, 'us-central1-a') for name in cluster_names)),
        provider=_cross_cloud_provider_state(gcp_running_count=100,
                                             aws_running_count=0),
        load_balancer=_load_balancer_state(demand_units=800,
                                           unique_job_arrivals_60s=800,
                                           unique_job_arrivals_300s=800))
    telemetry = qualifier.request_telemetry_from_summary(
        {
            'request_telemetry_observed_at': started_at,
            'request_telemetry_state': 'fresh',
            'request_telemetry_reason': 'in_flight_incomplete',
            'request_telemetry_compatibility_complete': True,
            'request_queue_depth': 762,
            'in_flight_requests': None,
            'processing_requests': None,
            'confirmed_in_flight_requests': 38,
            'confirmed_processing_requests': 16,
        }, {
            'ACCEPTED': 38,
            'SUCCEEDED': 418,
        })

    class Observer:

        def __init__(self):
            self.provider_reads = 0

        @staticmethod
        async def request_telemetry():
            return telemetry

        async def snapshot(self, *, require_complete_demand_report=True):
            assert not require_complete_demand_report
            self.provider_reads += 1
            return observation

    async def exercise():
        observer = Observer()
        traffic = asyncio.create_task(asyncio.Event().wait())
        try:
            progress = qualifier.Progress(
                scale_started_monotonic=started_monotonic,
                scale_started_at=started_at)
            receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                        service_name='paid-e2e',
                                        profile=profile)
            await qualifier._wait_for_scale(observer=observer,
                                            profile=profile,
                                            progress=progress,
                                            receipt=receipt,
                                            traffic=traffic,
                                            baseline=_request_telemetry(),
                                            campaign_progress=await
                                            _campaign_progress(profile,
                                                               turnover=418))
            return observer, progress, receipt
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)

    observer, progress, receipt = asyncio.run(exercise())
    assert observer.provider_reads == 1
    assert progress.peak_running == 100
    assert progress.scale_reached_monotonic == started_monotonic + 1
    sample = receipt._payload['request_telemetry_samples'][0]
    assert sample['reason'] == 'in_flight_incomplete'
    assert sample['queue_depth'] == 762
    assert sample['in_flight_requests'] is None
    assert sample['ledger_active'] == 38


def test_scale_wait_allows_attributed_arrivals_to_age_out(tmp_path):
    """The resident stimulus outlives the LB's rolling arrival window."""
    profile = dataclasses.replace(qualifier.PROFILES['scale'], poll_seconds=0)
    gcp_names = _provider_cluster_names('gcp', 50)
    aws_names = _provider_cluster_names('aws', 50)
    database = _database_state(
        paid_debit_units=100,
        demand_units=800,
        bound_cluster_zones=tuple(
            (name, 'us-central1-a') for name in gcp_names),
        aws_provider_identities=tuple(
            _aws_identity(client_token=f'token-aws-{index}', cluster_name=name)
            for index, name in enumerate(aws_names)))
    started_monotonic = time.monotonic()
    started_at = time.time()
    observations = [
        _observation(observed_at=started_at + 1,
                     observed_monotonic=started_monotonic + 1,
                     database=database,
                     provider=_cross_cloud_provider_state(gcp_running_count=49,
                                                          aws_running_count=50),
                     load_balancer=_load_balancer_state(
                         demand_units=800,
                         unique_job_arrivals_60s=800,
                         unique_job_arrivals_300s=800)),
        _observation(observed_at=started_at + 301,
                     observed_monotonic=started_monotonic + 301,
                     database=database,
                     provider=_cross_cloud_provider_state(gcp_running_count=50,
                                                          aws_running_count=50),
                     load_balancer=_load_balancer_state(
                         demand_units=800,
                         unique_job_arrivals_60s=0,
                         unique_job_arrivals_300s=0)),
    ]

    class Observer:

        @staticmethod
        async def request_telemetry():
            return _request_telemetry(queue_depth=800)

        async def snapshot(self, *, require_complete_demand_report=True):
            assert not require_complete_demand_report
            return observations.pop(0)

    async def exercise():
        progress = qualifier.Progress(scale_started_monotonic=started_monotonic,
                                      scale_started_at=started_at)
        receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                    service_name='paid-e2e',
                                    profile=profile)
        traffic = asyncio.create_task(asyncio.Event().wait())
        try:
            await qualifier._wait_for_scale(
                observer=Observer(),
                profile=profile,
                progress=progress,
                receipt=receipt,
                traffic=traffic,
                baseline=_request_telemetry(),
                campaign_progress=(await _campaign_progress(profile)))
            return progress, receipt
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)

    progress, receipt = asyncio.run(exercise())
    assert progress.scale_reached_monotonic == started_monotonic + 301
    assert [
        sample['lb_unique_job_arrivals_300s']
        for sample in receipt._payload['samples']
    ] == [800, 0]


def test_scale_wait_allows_bounded_arrivals_from_sliding_turnover(tmp_path):
    profile = dataclasses.replace(qualifier.PROFILES['scale'], poll_seconds=0)
    gcp_names = _provider_cluster_names('gcp', 50)
    aws_names = _provider_cluster_names('aws', 50)
    database = _database_state(
        paid_debit_units=100,
        demand_units=800,
        bound_cluster_zones=tuple(
            (name, 'us-central1-a') for name in gcp_names),
        aws_provider_identities=tuple(
            _aws_identity(client_token=f'token-aws-{index}', cluster_name=name)
            for index, name in enumerate(aws_names)))
    started_monotonic = time.monotonic()
    started_at = time.time()
    observations = [
        _observation(observed_at=started_at + 1,
                     observed_monotonic=started_monotonic + 1,
                     database=database,
                     provider=_cross_cloud_provider_state(gcp_running_count=49,
                                                          aws_running_count=50),
                     load_balancer=_load_balancer_state(
                         demand_units=800,
                         unique_job_arrivals_60s=800,
                         unique_job_arrivals_300s=800)),
        _observation(observed_at=started_at + 301,
                     observed_monotonic=started_monotonic + 301,
                     database=database,
                     provider=_cross_cloud_provider_state(gcp_running_count=49,
                                                          aws_running_count=50),
                     load_balancer=_load_balancer_state(
                         demand_units=800,
                         unique_job_arrivals_60s=0,
                         unique_job_arrivals_300s=0)),
        _observation(observed_at=started_at + 302,
                     observed_monotonic=started_monotonic + 302,
                     database=database,
                     provider=_cross_cloud_provider_state(gcp_running_count=50,
                                                          aws_running_count=50),
                     load_balancer=_load_balancer_state(
                         demand_units=800,
                         unique_job_arrivals_60s=1,
                         unique_job_arrivals_300s=1)),
    ]

    class Observer:

        @staticmethod
        async def request_telemetry():
            return _request_telemetry(queue_depth=800)

        async def snapshot(self, *, require_complete_demand_report=True):
            assert not require_complete_demand_report
            return observations.pop(0)

    async def exercise():
        progress = qualifier.Progress(scale_started_monotonic=started_monotonic,
                                      scale_started_at=started_at)
        receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                    service_name='paid-e2e',
                                    profile=profile)
        traffic = asyncio.create_task(asyncio.Event().wait())
        try:
            await qualifier._wait_for_scale(
                observer=Observer(),
                profile=profile,
                progress=progress,
                receipt=receipt,
                traffic=traffic,
                baseline=_request_telemetry(),
                campaign_progress=(await _campaign_progress(profile,
                                                            turnover=1)))
            return progress
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)

    progress = asyncio.run(exercise())
    assert progress.scale_reached_monotonic == started_monotonic + 302


def test_scale_wait_retries_transient_request_projection_skew(tmp_path):
    """LB demand and exact-ledger publications need not become visible together."""
    profile = dataclasses.replace(qualifier.PROFILES['scale'],
                                  exact_requests=8,
                                  request_concurrency=8,
                                  max_replicas=8,
                                  max_units=8,
                                  minimum_running=8,
                                  poll_seconds=0,
                                  scale_timeout_seconds=10)
    started_monotonic = time.monotonic()
    started_at = time.time()
    cluster_names = _provider_cluster_names('gcp', 8)
    observation = _observation(
        observed_at=started_at + 3,
        observed_monotonic=started_monotonic + 3,
        database=_database_state(
            paid_debit_units=8,
            demand_units=8,
            bound_cluster_zones=tuple(
                (name, 'us-central1-a') for name in cluster_names)),
        provider=_cross_cloud_provider_state(gcp_running_count=8,
                                             aws_running_count=0),
        load_balancer=_load_balancer_state(demand_units=8,
                                           unique_job_arrivals_60s=8,
                                           unique_job_arrivals_300s=8))
    telemetry = [
        # The ledger bind committed after the last LB demand report.
        _request_telemetry(queue_depth=8,
                           state_counts={'DISPATCH_MAY_HAVE_OCCURRED': 1}),
        # The next LB report captured in-flight work before its ledger bind.
        _request_telemetry(queue_depth=7, in_flight=1, processing=0),
        # Eventual exact evidence is the only sample paired to provider state.
        _request_telemetry(queue_depth=7,
                           in_flight=1,
                           processing=1,
                           state_counts={'ACCEPTED': 1}),
    ]

    class Observer:

        def __init__(self):
            self.provider_reads = 0

        async def request_telemetry(self):
            return telemetry.pop(0)

        async def snapshot(self, *, require_complete_demand_report=True):
            assert not require_complete_demand_report
            self.provider_reads += 1
            return observation

    async def exercise():
        observer = Observer()
        receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                    service_name='paid-e2e',
                                    profile=profile)
        traffic = asyncio.create_task(asyncio.Event().wait())
        try:
            progress = qualifier.Progress(
                scale_started_monotonic=started_monotonic,
                scale_started_at=started_at)
            await qualifier._wait_for_scale(
                observer=observer,
                profile=profile,
                progress=progress,
                receipt=receipt,
                traffic=traffic,
                baseline=_request_telemetry(),
                campaign_progress=(await _campaign_progress(profile)))
            return observer, receipt
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)

    observer, receipt = asyncio.run(exercise())
    assert observer.provider_reads == 1
    assert not telemetry
    assert len(receipt._payload['request_telemetry_samples']) == 1
    assert receipt._payload['request_telemetry_samples'][0][
        'scale_iteration_id'] == 1


def test_scale_wait_treats_refill_gap_as_non_qualifying(tmp_path):
    profile = dataclasses.replace(qualifier.PROFILES['scale'],
                                  exact_requests=8,
                                  max_replicas=8,
                                  max_units=8,
                                  minimum_running=8,
                                  poll_seconds=0)
    telemetry = [
        _request_telemetry(queue_depth=7),
        _request_telemetry(queue_depth=8),
    ]
    cluster_names = _provider_cluster_names('gcp', 8)

    class Observer:

        def __init__(self):
            self.provider_reads = 0

        async def request_telemetry(self):
            return telemetry.pop(0)

        async def snapshot(self, *, require_complete_demand_report=True):
            assert not require_complete_demand_report
            self.provider_reads += 1
            return _observation(
                observed_at=time.time(),
                observed_monotonic=time.monotonic(),
                database=_database_state(
                    paid_debit_units=8,
                    demand_units=8,
                    bound_cluster_zones=tuple(
                        (name, 'us-central1-a') for name in cluster_names)),
                provider=_cross_cloud_provider_state(gcp_running_count=8,
                                                     aws_running_count=0),
                load_balancer=_load_balancer_state(demand_units=8,
                                                   unique_job_arrivals_60s=8,
                                                   unique_job_arrivals_300s=8))

    async def exercise():
        observer = Observer()
        traffic = asyncio.create_task(asyncio.Event().wait())
        try:
            await qualifier._wait_for_scale(
                observer=observer,
                profile=profile,
                progress=qualifier.Progress(
                    scale_started_monotonic=time.monotonic()),
                receipt=qualifier.Receipt(path=tmp_path / 'receipt.json',
                                          service_name='paid-e2e',
                                          profile=profile),
                traffic=traffic,
                baseline=_request_telemetry(),
                campaign_progress=await _campaign_progress(profile))
            return observer
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)

    observer = asyncio.run(exercise())
    assert observer.provider_reads == 1


def test_scale_and_positive_gates_accept_fully_dispatched_cohort_concurrently(
        tmp_path):
    """Positive telemetry must not wait for the physical provider gate."""
    profile = dataclasses.replace(qualifier.PROFILES['scale'], poll_seconds=0)
    gcp_names = _provider_cluster_names('gcp', 50)
    aws_names = _provider_cluster_names('aws', 50)
    database = _database_state(
        paid_debit_units=100,
        demand_units=800,
        bound_cluster_zones=tuple(
            (name, 'us-central1-a') for name in gcp_names),
        aws_provider_identities=tuple(
            _aws_identity(client_token=f'token-aws-{index}', cluster_name=name)
            for index, name in enumerate(aws_names)))
    started_monotonic = time.monotonic()
    started_at = time.time()

    class Observer:
        """Dispatch the whole cohort while provider scale is still at 99."""

        def __init__(self):
            self.provider_reads = 0

        @staticmethod
        async def request_telemetry():
            return _request_telemetry(queue_depth=0,
                                      in_flight=800,
                                      processing=100,
                                      state_counts={'ACCEPTED': 800})

        async def snapshot(self, *, require_complete_demand_report=True):
            assert not require_complete_demand_report
            self.provider_reads += 1
            running = 99 if self.provider_reads == 1 else 100
            gcp_running = running - 50
            observation = _observation(
                observed_at=started_at + self.provider_reads,
                observed_monotonic=started_monotonic + self.provider_reads,
                database=database,
                provider=_cross_cloud_provider_state(
                    gcp_running_count=gcp_running, aws_running_count=50),
                load_balancer=_load_balancer_state(
                    demand_units=800,
                    unique_job_arrivals_60s=800,
                    unique_job_arrivals_300s=800))
            if running == 99:
                await asyncio.sleep(0)
            return observation

    async def exercise():
        observer = Observer()
        progress = qualifier.Progress(scale_started_monotonic=started_monotonic,
                                      scale_started_at=started_at)
        receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                    service_name='paid-e2e',
                                    profile=profile)
        receipt.request_telemetry('scale-stimulus',
                                  _request_telemetry(queue_depth=800))
        traffic = asyncio.create_task(asyncio.Event().wait())
        try:
            await qualifier._wait_for_scale_and_positive_request_telemetry(
                observer=observer,
                profile=profile,
                progress=progress,
                receipt=receipt,
                traffic=traffic,
                baseline=_request_telemetry(),
                campaign_progress=await _campaign_progress(profile),
                expectation=qualifier.provider_expectation(profile, None),
                positive_deadline_monotonic=started_monotonic + 590)
            return observer, progress, receipt
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)

    observer, progress, receipt = asyncio.run(exercise())
    assert observer.provider_reads == 2
    assert progress.scale_reached_monotonic == started_monotonic + 2
    phases = [
        sample['phase']
        for sample in receipt._payload['request_telemetry_samples']
    ]
    assert phases == ['scale-stimulus', 'scale', 'positive', 'scale']
    positive = next(
        sample for sample in receipt._payload['request_telemetry_samples']
        if sample['phase'] == 'positive')
    assert positive['queue_depth'] == 0
    assert positive['in_flight_requests'] == 800
    assert positive['processing_requests'] == 100


def test_scale_sliding_window_finishes_before_delayed_provider_proof(tmp_path):
    """Provider observation cannot hold real work or its terminal callback."""
    profile = dataclasses.replace(qualifier.PROFILES['scale'],
                                  max_replicas=2,
                                  max_units=2,
                                  minimum_running=2,
                                  exact_requests=6,
                                  request_concurrency=2,
                                  poll_seconds=0,
                                  scale_timeout_seconds=2)
    offered: set[str] = set()
    accepted: set[str] = set()
    processing: set[str] = set()
    succeeded: set[str] = set()
    processing_tasks: set[asyncio.Task[None]] = set()
    peak_resident = 0
    completed_at_provider_threshold: int | None = None
    names = _provider_cluster_names('gcp', 2)

    def receipt_headers(request_id, *, state, revision):
        return {
            qualifier.serve_constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER: '1',
            qualifier.serve_constants.LB_ASYNC_SERVICE_INCARNATION_HEADER: 'incarnation',
            qualifier.serve_constants.LB_ASYNC_ATTEMPT_ID_HEADER: str(
                uuid.uuid5(uuid.NAMESPACE_URL, request_id)),
            qualifier.serve_constants.LB_ASYNC_ATTEMPT_NO_HEADER: '1',
            qualifier.serve_constants.LB_ASYNC_LEDGER_REVISION_HEADER:
                str(revision),
            qualifier.serve_constants.LB_ASYNC_LEDGER_STATE_HEADER: state,
        }

    async def predict(request):
        nonlocal peak_resident
        payload = await request.json()
        request_id = payload['request_id']
        duration = payload['payload']['duration_seconds']
        assert request_id not in offered
        offered.add(request_id)
        accepted.add(request_id)
        processing.add(request_id)
        peak_resident = max(peak_resident, len(processing))

        async def finish_processing():
            await asyncio.sleep(duration)
            processing.remove(request_id)

        task = asyncio.create_task(finish_processing())
        processing_tasks.add(task)
        task.add_done_callback(processing_tasks.discard)
        return aiohttp.web.json_response(
            {
                'request_id': request_id,
                'status': 'accepted',
            },
            status=202,
            headers=receipt_headers(request_id, state='ACCEPTED', revision=2))

    async def complete(request):
        payload = await request.json()
        request_id = payload['request_id']
        while request_id in processing:
            await asyncio.sleep(0)
        assert request_id in accepted
        accepted.remove(request_id)
        succeeded.add(request_id)
        return aiohttp.web.Response(status=204,
                                    headers=receipt_headers(request_id,
                                                            state='SUCCEEDED',
                                                            revision=3))

    class Observer:

        async def request_telemetry(self):
            # This is the production reduction: real asynchronous occupancy
            # must equal active exact-ledger attempts at every accepted sample.
            while processing != accepted:
                await asyncio.sleep(0)
            return _request_telemetry(queue_depth=0,
                                      in_flight=len(processing),
                                      processing=len(processing),
                                      state_counts={
                                          'ACCEPTED': len(accepted),
                                          'SUCCEEDED': len(succeeded),
                                      },
                                      observed_at=time.time())

        async def snapshot(self, *, require_complete_demand_report=True):
            nonlocal completed_at_provider_threshold
            assert not require_complete_demand_report
            running = 2 if len(succeeded) >= 2 else 1
            if running == 2 and completed_at_provider_threshold is None:
                completed_at_provider_threshold = len(succeeded)
            return _observation(
                observed_at=time.time(),
                observed_monotonic=time.monotonic(),
                database=_database_state(
                    paid_debit_units=2,
                    demand_units=2,
                    bound_cluster_zones=tuple(
                        (name, 'us-central1-a') for name in names)),
                provider=_cross_cloud_provider_state(gcp_running_count=running,
                                                     aws_running_count=0),
                load_balancer=_load_balancer_state(
                    demand_units=2,
                    unique_job_arrivals_60s=len(offered),
                    unique_job_arrivals_300s=len(offered)))

    async def exercise():
        app = aiohttp.web.Application()
        app.router.add_post('/v1/models/model:predict', predict)
        app.router.add_post(
            qualifier.serve_constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
            complete)
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        assert site._server is not None
        endpoint = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
        observer = Observer()
        receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                    service_name='paid-e2e',
                                    profile=profile)
        progress = qualifier.Progress()
        progress.start_scale()
        assert progress.scale_started_monotonic is not None
        campaign_progress = qualifier.ExactRequestCampaignProgress(
            total_count=profile.exact_requests,
            window_size=qualifier.scale_stimulus_count(profile))
        traffic = asyncio.create_task(
            qualifier.send_exact_async_requests(
                endpoint=endpoint,
                token='secret',
                service_hash='incarnation',
                prefix='sliding',
                count=profile.exact_requests,
                concurrency=qualifier.scale_stimulus_count(profile),
                hold_requests=profile.exact_requests,
                hold_seconds=0.02,
                timeout_seconds=2,
                request_queue_timeout_seconds=2,
                terminal_timeout_seconds=2,
                campaign_progress=campaign_progress))
        try:
            await qualifier._wait_for_scale_stimulus(
                observer=observer,
                profile=profile,
                receipt=receipt,
                traffic=traffic,
                baseline=_request_telemetry(observed_at=time.time() - 1),
                expected_resident=qualifier.scale_stimulus_count(profile),
                deadline_monotonic=time.monotonic() + 1)
            await qualifier._wait_for_scale_and_positive_request_telemetry(
                observer=observer,
                profile=profile,
                progress=progress,
                receipt=receipt,
                traffic=traffic,
                baseline=_request_telemetry(observed_at=time.time() - 1),
                campaign_progress=campaign_progress,
                expectation=qualifier.provider_expectation(profile, None),
                positive_deadline_monotonic=time.monotonic() + 1)
            assert await traffic == profile.exact_requests
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)
            if processing_tasks:
                await asyncio.gather(*processing_tasks)
            await runner.cleanup()

    asyncio.run(exercise())
    assert completed_at_provider_threshold is not None
    assert completed_at_provider_threshold >= 2
    assert len(offered) == len(succeeded) == profile.exact_requests
    assert peak_resident <= qualifier.scale_stimulus_count(profile)


def test_generated_concurrent_receipt_passes_aggregate_gate(tmp_path):
    """Exercise the production proof join before validating its receipt."""
    canonical_profile = qualifier.PROFILES['scale']
    profile = dataclasses.replace(canonical_profile, poll_seconds=0)
    service_name = 'generated-economic'
    source_sha256 = 'e' * 64
    scope = _provider_scope(service_hash=f'{service_name}-hash',
                            lifecycle_epoch=1,
                            service_version=1,
                            max_live_paid_gpu_units=canonical_profile.max_units,
                            qualification_profile=canonical_profile.name,
                            qualification_source_sha256=source_sha256,
                            qualification_projection_sha256=(
                                qualifier._qualification_projection_sha256(
                                    source_sha256=source_sha256,
                                    profile=canonical_profile,
                                    providers=('aws', 'gcp'))))
    gcp_names = _provider_cluster_names('gcp', 50)
    aws_names = _provider_cluster_names('aws', 50)
    claims = [{
        'priority': qualifier._REQUEST_PRIORITY,
        'capacity_plan_generation': 9,
        'capacity_plan_sha256': 'f' * 64,
        'persisted_plan_sha256': 'f' * 64,
        'capacity_plan_accelerator': 'L4',
        'capacity_plan_units': width,
    } for width in (4, 8)]
    claim_census = qualifier.paid_claim_census(claims)
    paid_database = _database_state(
        service_hash=scope.service_hash,
        paid_debit_units=100,
        claimed_units=claim_census.gpu_units,
        claim_priority_units=claim_census.priority_units,
        demand_units=canonical_profile.max_units,
        bound_cluster_zones=tuple(
            (name, 'us-central1-a') for name in gcp_names),
        aws_provider_identities=tuple(
            _aws_identity(client_token=f'token-aws-{index}', cluster_name=name)
            for index, name in enumerate(aws_names)))
    zero_database = _database_state(service_hash=scope.service_hash)
    baseline = _request_telemetry(observed_at=0.75)
    started_monotonic = time.monotonic()
    receipt_path = tmp_path / 'generated-economic.json'
    receipt = qualifier.Receipt(path=receipt_path,
                                service_name=service_name,
                                profile=profile,
                                expectation=qualifier.provider_expectation(
                                    profile, None),
                                scope=scope)
    campaign_prefix = f'{service_name}-campaign'
    receipt.bind_campaign_manifest(campaign_prefix, 10_000)
    progress = qualifier.Progress(baseline_qualified_iteration_id=3,
                                  baseline_qualified_observed_at=3.0,
                                  scale_started_monotonic=started_monotonic,
                                  scale_started_at=4.0)

    for iteration_id, observed_at in ((1, 1.0), (2, 2.0), (3, 3.0)):
        paired = _request_telemetry(observed_at=observed_at - 0.25)
        receipt.request_telemetry('baseline',
                                  paired,
                                  baseline_iteration_id=iteration_id,
                                  baseline_pair_observed_at=observed_at)
        receipt.sample('baseline',
                       _observation(observed_at=observed_at,
                                    database=zero_database,
                                    load_balancer=_load_balancer_state(
                                        service_hash=scope.service_hash)),
                       baseline_iteration_id=iteration_id,
                       baseline_pair_observed_at=observed_at)
    receipt.request_telemetry(
        'scale-stimulus',
        _request_telemetry(queue_depth=canonical_profile.max_units,
                           observed_at=5.0))

    class Observer:
        """Yield positive demand before the second physical scale sample."""

        def __init__(self):
            self.request_reads = 0
            self.provider_reads = 0

        async def request_telemetry(self):
            self.request_reads += 1
            return _request_telemetry(
                queue_depth=0,
                in_flight=canonical_profile.max_units,
                processing=100,
                state_counts={'ACCEPTED': canonical_profile.max_units},
                observed_at=100.0 + 10.0 * self.request_reads)

        async def snapshot(self, *, require_complete_demand_report=True):
            assert not require_complete_demand_report
            self.provider_reads += 1
            running = 99 if self.provider_reads == 1 else 100
            if running == 99:
                await asyncio.sleep(0)
            provider = _cross_cloud_provider_state(gcp_running_count=running -
                                                   50,
                                                   aws_running_count=50)
            provider = dataclasses.replace(
                provider,
                clouds=tuple(
                    dataclasses.replace(
                        cloud,
                        shapes=(qualifier.ProviderShapeState(
                            gpu_units_per_instance=1,
                            instance_count=cloud.instance_count,
                            instance_type=('g2-standard-4' if cloud.cloud ==
                                           'gcp' else 'g6.xlarge'),
                            running_count=cloud.running_count,
                            running_gpu_units=cloud.running_gpu_units),))
                    for cloud in provider.clouds))
            return _observation(
                observed_at=240.0 + 10.0 * self.provider_reads,
                observed_monotonic=(started_monotonic + self.provider_reads),
                database=paid_database,
                provider=provider,
                load_balancer=_load_balancer_state(
                    service_hash=scope.service_hash,
                    demand_units=canonical_profile.max_units,
                    unique_job_arrivals_60s=canonical_profile.max_units,
                    unique_job_arrivals_300s=canonical_profile.max_units))

    async def generate_concurrent_proof():
        traffic = asyncio.create_task(asyncio.Event().wait())
        try:
            await qualifier._wait_for_scale_and_positive_request_telemetry(
                observer=Observer(),
                profile=profile,
                progress=progress,
                receipt=receipt,
                traffic=traffic,
                baseline=baseline,
                campaign_progress=await _campaign_progress(profile),
                expectation=qualifier.provider_expectation(profile, None),
                positive_deadline_monotonic=started_monotonic + 590)
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)

    asyncio.run(generate_concurrent_proof())
    final = _request_telemetry(state_counts={'SUCCEEDED': 10_000},
                               observed_at=500.0)
    receipt.bind_campaign_terminal_membership(
        qualifier._campaign_manifest_sha256(campaign_prefix, 10_000))
    receipt.request_telemetry('final', final)
    for observed_at in (1000.0, 1180.0, 1360.0):
        receipt.sample(
            'drain',
            _observation(observed_at=observed_at,
                         database=zero_database,
                         load_balancer=_load_balancer_state(
                             service_hash=scope.service_hash)))
    receipt.finish(progress=progress,
                   exact_request_successes=10_000,
                   ledger_baseline=baseline,
                   ledger_final=final)

    cleanup_path = tmp_path / 'generated-economic-cleanup.json'
    _write_aggregate_cleanup(cleanup_path,
                             receipt_path,
                             service_name=service_name,
                             providers=['aws', 'gcp'])
    output = tmp_path / 'generated-aggregate.json'
    qualifier.aggregate_evidence(
        type(
            'Args', (), {
                'economic_receipt': str(receipt_path),
                'economic_cleanup_receipt': str(cleanup_path),
                'canary_receipt': [],
                'canary_cleanup_receipt': [],
                'output': str(output),
            })())

    assert json.loads(output.read_text(encoding='utf-8'))['outcome'] == 'passed'


def test_scale_and_positive_proof_failure_cancels_sibling():
    sibling_started = asyncio.Event()
    sibling_cancelled = asyncio.Event()

    async def fail_scale():
        await sibling_started.wait()
        raise qualifier.QualificationError('physical gate failed')

    async def wait_positive():
        sibling_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            sibling_cancelled.set()

    async def exercise():
        campaign_progress = qualifier.ExactRequestCampaignProgress(
            total_count=1, window_size=1)

        async def wait_for_stop():
            while (await campaign_progress.snapshot()).accepting_offers:
                await asyncio.sleep(0)
            return 0

        traffic = asyncio.create_task(wait_for_stop())
        with pytest.raises(qualifier.QualificationError,
                           match='physical gate failed'):
            await qualifier._join_independent_proofs(
                fail_scale(),
                wait_positive(),
                traffic=traffic,
                campaign_progress=campaign_progress)
        assert sibling_cancelled.is_set()
        assert traffic.result() == 0

    asyncio.run(exercise())


def test_proof_consumers_receive_only_read_only_campaign_evidence():
    traffic_consumers = (
        qualifier._wait_for_scale_stimulus,
        qualifier._wait_for_positive_request_telemetry,
        qualifier._wait_for_scale,
    )
    for consumer in traffic_consumers:
        assert typing.get_type_hints(
            consumer)['traffic'] is qualifier.ExactRequestTrafficEvidence
    assert typing.get_type_hints(
        qualifier._wait_for_scale)['campaign_progress'] == (
            qualifier.ExactRequestCampaignEvidence | None)


def test_observer_failure_before_202_drains_offered_request_before_exit():
    offered: list[str] = []
    completed: list[str] = []
    post_received = asyncio.Event()
    release_acceptance = asyncio.Event()
    progress = qualifier.ExactRequestCampaignProgress(total_count=3,
                                                      window_size=1)

    def receipt_headers(request_id, *, state, revision):
        return {
            qualifier.serve_constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER: '1',
            qualifier.serve_constants.LB_ASYNC_SERVICE_INCARNATION_HEADER: 'incarnation',
            qualifier.serve_constants.LB_ASYNC_ATTEMPT_ID_HEADER: str(
                uuid.uuid5(uuid.NAMESPACE_URL, request_id)),
            qualifier.serve_constants.LB_ASYNC_ATTEMPT_NO_HEADER: '1',
            qualifier.serve_constants.LB_ASYNC_LEDGER_REVISION_HEADER:
                str(revision),
            qualifier.serve_constants.LB_ASYNC_LEDGER_STATE_HEADER: state,
        }

    async def predict(request):
        payload = await request.json()
        request_id = payload['request_id']
        offered.append(request_id)
        post_received.set()
        await release_acceptance.wait()
        return aiohttp.web.json_response(
            {
                'request_id': request_id,
                'status': 'accepted',
            },
            status=202,
            headers=receipt_headers(request_id, state='ACCEPTED', revision=2))

    async def complete(request):
        payload = await request.json()
        request_id = payload['request_id']
        completed.append(request_id)
        return aiohttp.web.Response(status=204,
                                    headers=receipt_headers(request_id,
                                                            state='SUCCEEDED',
                                                            revision=3))

    async def exercise():
        app = aiohttp.web.Application()
        app.router.add_post('/v1/models/model:predict', predict)
        app.router.add_post(
            qualifier.serve_constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
            complete)
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        assert site._server is not None
        endpoint = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
        traffic = asyncio.create_task(
            qualifier.send_exact_async_requests(endpoint=endpoint,
                                                token='secret',
                                                service_hash='incarnation',
                                                prefix='observer-failure',
                                                count=3,
                                                concurrency=1,
                                                hold_requests=3,
                                                hold_seconds=0.01,
                                                timeout_seconds=2,
                                                request_queue_timeout_seconds=2,
                                                terminal_timeout_seconds=2,
                                                campaign_progress=progress))

        async def fail_observer():
            await post_received.wait()
            raise qualifier.QualificationError('observer failed')

        async def sibling_observer():
            try:
                await asyncio.Event().wait()
            finally:
                # The proof coordinator must close only future offers before
                # releasing an in-flight POST to receive its 202 obligation.
                assert not (await progress.snapshot()).accepting_offers
                release_acceptance.set()

        try:
            with pytest.raises(qualifier.QualificationError,
                               match='observer failed'):
                await qualifier._join_independent_proofs(
                    fail_observer(),
                    sibling_observer(),
                    traffic=traffic,
                    campaign_progress=progress)
            return await progress.snapshot()
        finally:
            if not traffic.done():
                traffic.cancel()
                await asyncio.gather(traffic, return_exceptions=True)
            await runner.cleanup()

    snapshot = asyncio.run(exercise())
    assert offered == completed
    assert len(completed) == 1
    assert snapshot == qualifier.ExactRequestCampaignCounters(
        offered=1, succeeded=1, accepting_offers=False)


def test_scale_wait_accepts_provider_convergence_after_diagnostic_slo(tmp_path):
    profile = dataclasses.replace(qualifier.PROFILES['scale'],
                                  exact_requests=100,
                                  request_concurrency=100,
                                  poll_seconds=0,
                                  scale_slo_seconds=1,
                                  scale_timeout_seconds=10)
    gcp_names = _provider_cluster_names('gcp', 100)
    started_monotonic = time.monotonic() - 2
    observation = _observation(
        observed_at=time.time(),
        observed_monotonic=time.monotonic(),
        database=_database_state(
            paid_debit_units=100,
            demand_units=100,
            bound_cluster_zones=tuple(
                (name, 'us-central1-a') for name in gcp_names)),
        provider=_cross_cloud_provider_state(gcp_running_count=100,
                                             aws_running_count=0),
        load_balancer=_load_balancer_state(demand_units=100,
                                           unique_job_arrivals_60s=100,
                                           unique_job_arrivals_300s=100))

    class Observer:

        def __init__(self):
            self.telemetry_reads = 0

        async def request_telemetry(self):
            self.telemetry_reads += 1
            return _request_telemetry(queue_depth=100)

        @staticmethod
        async def snapshot(**_kwargs):
            return observation

    async def exercise():
        observer = Observer()
        traffic = asyncio.create_task(asyncio.Event().wait())
        try:
            progress = qualifier.Progress(
                scale_started_monotonic=started_monotonic)
            await qualifier._wait_for_scale(
                observer=observer,
                profile=profile,
                progress=progress,
                receipt=qualifier.Receipt(path=tmp_path / 'receipt.json',
                                          service_name='paid-e2e',
                                          profile=profile),
                traffic=traffic,
                baseline=_request_telemetry(),
                campaign_progress=(await _campaign_progress(profile)))
            assert observer.telemetry_reads == 1
            assert progress.scale_slo_met is False
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)

    asyncio.run(exercise())


def test_scale_wait_stops_at_absolute_qualification_timeout(tmp_path):
    profile = dataclasses.replace(qualifier.PROFILES['scale'],
                                  poll_seconds=1,
                                  scale_slo_seconds=300,
                                  scale_timeout_seconds=900)

    class Observer:

        def __init__(self):
            self.telemetry_reads = 0

        async def request_telemetry(self):
            self.telemetry_reads += 1
            raise AssertionError('polled after the qualification timeout')

    async def exercise():
        observer = Observer()
        traffic = asyncio.create_task(asyncio.Event().wait())
        try:
            with pytest.raises(qualifier.QualificationError,
                               match='Provider did not reach'):
                await qualifier._wait_for_scale(
                    observer=observer,
                    profile=profile,
                    progress=qualifier.Progress(
                        scale_started_monotonic=time.monotonic() - 901),
                    receipt=qualifier.Receipt(path=tmp_path / 'receipt.json',
                                              service_name='paid-e2e',
                                              profile=profile),
                    traffic=traffic,
                    baseline=_request_telemetry())
            assert observer.telemetry_reads == 0
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)

    asyncio.run(exercise())


def test_non_scale_wait_retains_relative_profile_timeout(tmp_path):
    profile = dataclasses.replace(qualifier.PROFILES['small'],
                                  scale_timeout_seconds=1)

    async def exercise():

        async def complete():
            return 0

        traffic = asyncio.create_task(complete())
        assert await traffic == 0
        with pytest.raises(qualifier.QualificationError,
                           match='ended before scale convergence'):
            await qualifier._wait_for_scale(
                observer=object(),
                profile=profile,
                progress=qualifier.Progress(
                    scale_started_monotonic=time.monotonic() - 301),
                receipt=qualifier.Receipt(path=tmp_path / 'receipt.json',
                                          service_name='paid-e2e',
                                          profile=profile),
                traffic=traffic,
                baseline=_request_telemetry())

    asyncio.run(exercise())


def test_aggregate_accepts_queued_only_physical_scale_before_positive(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    request_scale = next(
        sample for sample in payload['request_telemetry_samples']
        if sample['phase'] == 'scale')
    request_scale.update(
        _request_evidence_sample(phase='scale',
                                 queue_depth=800,
                                 in_flight=0,
                                 processing=0,
                                 observed_at=249.0,
                                 scale_iteration_id=1))
    positive = next(sample for sample in payload['request_telemetry_samples']
                    if sample['phase'] == 'positive')
    positive['observed_at'] = 251.0
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    qualifier.aggregate_evidence(args)


@pytest.mark.parametrize('positive_observed_at', [4.5, 595.0])
def test_aggregate_rejects_positive_outside_post_scale_queue_window(
        tmp_path, positive_observed_at):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    positive = next(sample for sample in payload['request_telemetry_samples']
                    if sample['phase'] == 'positive')
    positive['observed_at'] = positive_observed_at
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='scale demand|scale-stimulus'):
        qualifier.aggregate_evidence(args)


def test_scale_never_accepts_unattributed_mixed_demand(tmp_path):
    profile = dataclasses.replace(qualifier.PROFILES['small'],
                                  poll_seconds=0,
                                  scale_timeout_seconds=0.01)

    class Observer:
        """Expose demand that cannot belong solely to the campaign."""

        @staticmethod
        async def request_telemetry():
            # Three demand-bearing requests but only two exact ledger rows.
            return _request_telemetry(queue_depth=2,
                                      in_flight=1,
                                      processing=1,
                                      state_counts={'ACCEPTED': 2})

        @staticmethod
        async def snapshot(**_kwargs):
            raise AssertionError('provider sampling must fail closed first')

    async def exercise():
        traffic = asyncio.create_task(asyncio.Event().wait())
        try:
            with pytest.raises(qualifier.QualificationError,
                               match='Provider did not reach'):
                await qualifier._wait_for_scale(
                    observer=Observer(),
                    profile=profile,
                    progress=qualifier.Progress(
                        scale_started_monotonic=time.monotonic()),
                    receipt=qualifier.Receipt(path=tmp_path / 'receipt.json',
                                              service_name='paid-e2e',
                                              profile=profile),
                    traffic=traffic,
                    baseline=_request_telemetry())
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)

    asyncio.run(exercise())


def test_scale_stimulus_requires_only_the_bounded_resident_cohort(tmp_path):
    profile = dataclasses.replace(qualifier.PROFILES['scale'], poll_seconds=0)
    stimulus = _request_telemetry(queue_depth=800)

    class Observer:

        @staticmethod
        async def request_telemetry():
            return stimulus

    async def exercise():
        observer = Observer()
        receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                    service_name='paid-e2e',
                                    profile=profile)
        traffic = asyncio.create_task(asyncio.Event().wait())
        try:
            deadline = time.monotonic() + 1
            observed_stimulus = await qualifier._wait_for_scale_stimulus(
                observer=observer,
                profile=profile,
                receipt=receipt,
                traffic=traffic,
                baseline=_request_telemetry(),
                expected_resident=800,
                deadline_monotonic=deadline)
            return observed_stimulus, receipt
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)

    observed_stimulus, receipt = asyncio.run(exercise())
    assert observed_stimulus == stimulus
    assert [
        sample['phase']
        for sample in receipt._payload['request_telemetry_samples']
    ] == ['scale-stimulus']


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
        qualifier._one_exact_async_request(
            session,
            endpoint='https://service.test',
            token='secret',
            service_hash='incarnation-a',
            request_id='execution-1',
            stable_job_id='job-1',
            duration_seconds=0,
            admission_deadline=(qualifier.time.monotonic() + 2),
            terminal_timeout_seconds=2))

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


class _ExactAdmissionResponse:
    """Scripted response from the public exact-admission protocol."""

    def __init__(self,
                 status,
                 *,
                 state=None,
                 revision=None,
                 attempt_id='11111111-1111-4111-8111-111111111111',
                 attempt_no=1,
                 body=b'{}',
                 exact_fence=True):
        self.status = status
        self._body = body
        self.headers = {'Retry-After': '0.1'}
        if exact_fence:
            self.headers.update({
                'X-SkyServe-Async-Ledger-Protocol': '1',
                'X-SkyServe-Service-Incarnation': 'incarnation-a',
            })
        if state is not None:
            assert revision is not None
            self.headers.update({
                'X-SkyServe-Async-Attempt-Id': attempt_id,
                'X-SkyServe-Async-Attempt-No': str(attempt_no),
                'X-SkyServe-Async-Ledger-Revision': str(revision),
                'X-SkyServe-Async-Ledger-State': state,
            })

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def read(self):
        if isinstance(self._body, BaseException):
            raise self._body
        return self._body


class _ExactAdmissionSession:
    """Script exact admission and read-only receipt lookup outcomes."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _accepted_exact_response(*,
                             status=202,
                             state='ACCEPTED',
                             revision=2,
                             attempt_id=('11111111-1111-4111-8111-'
                                         '111111111111'),
                             attempt_no=1):
    body = json.dumps({
        'request_id': 'execution-1',
        'status': 'accepted',
    }).encode()
    return _ExactAdmissionResponse(status,
                                   state=state,
                                   revision=revision,
                                   attempt_id=attempt_id,
                                   attempt_no=attempt_no,
                                   body=body)


def _submit_exact_with_session(session):
    admission = asyncio.run(
        qualifier._submit_exact_async_request(
            session,
            endpoint='https://service.test',
            token='secret',
            service_hash='incarnation-a',
            request_id='execution-1',
            stable_job_id='job-1',
            duration_seconds=0,
            deadline=qualifier.time.monotonic() + 2))
    return admission.receipt, admission.intent_sha256


async def _skip_exact_retry_delay(_delay):
    return None


_EXACT_SUBMIT_URL = 'https://service.test/v1/models/model:predict'
_EXACT_RECEIPT_URL = 'https://service.test/_lb/async-request-receipt'


def test_exact_async_request_recovers_lost_response_via_lookup_before_replay(
        monkeypatch):
    """A lookup-proven miss permits replay of only the immutable exact POST."""
    session = _ExactAdmissionSession([
        qualifier.aiohttp.ServerDisconnectedError('lost response'),
        _ExactAdmissionResponse(404),
        _accepted_exact_response(),
    ])
    monkeypatch.setattr(qualifier.asyncio, 'sleep', _skip_exact_retry_delay)

    receipt, _ = _submit_exact_with_session(session)

    assert receipt.state == 'ACCEPTED'
    assert [call[0] for call in session.calls] == [
        _EXACT_SUBMIT_URL,
        _EXACT_RECEIPT_URL,
        _EXACT_SUBMIT_URL,
    ]
    first_submit = session.calls[0][1]
    replay = session.calls[2][1]
    assert replay == first_submit
    assert session.calls[1][1]['json'] == {
        'ledger_protocol_version': 1,
        'request_id': 'execution-1',
        'intent_sha256': first_submit['headers']
                         ['X-SkyServe-Async-Intent-Sha256'],
    }
    assert session.calls[1][1]['headers'] == {
        'X-SkyPilot-Serve-Authorization': 'Bearer secret',
        'Content-Type': 'application/json',
        'X-SkyServe-Service-Incarnation': 'incarnation-a',
    }


def test_exact_async_request_recovers_truncated_response_body_via_lookup(
        monkeypatch):
    """Headers without a complete body do not strand the exact campaign."""
    session = _ExactAdmissionSession([
        _ExactAdmissionResponse(
            202,
            state='ACCEPTED',
            revision=2,
            body=qualifier.aiohttp.ClientPayloadError('truncated body')),
        _ExactAdmissionResponse(200, state='ACCEPTED', revision=2),
    ])
    monkeypatch.setattr(qualifier.asyncio, 'sleep', _skip_exact_retry_delay)

    receipt, _ = _submit_exact_with_session(session)

    assert receipt.state == 'ACCEPTED'
    assert [call[0] for call in session.calls] == [
        _EXACT_SUBMIT_URL,
        _EXACT_RECEIPT_URL,
    ]


@pytest.mark.parametrize(('hop', 'tail', 'state', 'expected_urls'), [
    (_ExactAdmissionResponse(
        200,
        state='DISPATCH_MAY_HAVE_OCCURRED',
        revision=1,
        attempt_id='22222222-2222-4222-8222-222222222222',
        attempt_no=2), [
            _ExactAdmissionResponse(
                200,
                state='ACCEPTED',
                revision=2,
                attempt_id='22222222-2222-4222-8222-222222222222',
                attempt_no=2)
        ], 'ACCEPTED', [
            _EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL, _EXACT_RECEIPT_URL,
            _EXACT_RECEIPT_URL
        ]),
    (_ExactAdmissionResponse(200,
                             state='ACCEPTED',
                             revision=2,
                             attempt_id='22222222-2222-4222-8222-222222222222',
                             attempt_no=2), [], 'ACCEPTED',
     [_EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL, _EXACT_RECEIPT_URL]),
    (_ExactAdmissionResponse(200,
                             state='ACCEPTED',
                             revision=2,
                             attempt_id='44444444-4444-4444-8444-444444444444',
                             attempt_no=4), [], 'ACCEPTED',
     [_EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL, _EXACT_RECEIPT_URL]),
    (_ExactAdmissionResponse(200,
                             state='SUCCEEDED',
                             revision=2,
                             attempt_id='22222222-2222-4222-8222-222222222222',
                             attempt_no=2), [], 'SUCCEEDED',
     [_EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL, _EXACT_RECEIPT_URL]),
    (_ExactAdmissionResponse(
        200,
        state='REJECTED_PRE_DISPATCH',
        revision=2,
        attempt_id='22222222-2222-4222-8222-222222222222',
        attempt_no=2), [
            _accepted_exact_response(
                attempt_id='33333333-3333-4333-8333-333333333333', attempt_no=3)
        ], 'ACCEPTED', [
            _EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL, _EXACT_RECEIPT_URL,
            _EXACT_SUBMIT_URL
        ]),
],
                         ids=[
                             'dispatch', 'accepted', 'multiple-rebinds',
                             'succeeded', 'rejected'
                         ])
def test_exact_async_request_accepts_internal_rebinds_after_dispatch(
        monkeypatch, hop, tail, state, expected_urls):
    """Pre-send failures may durably reject N and internally bind N+k."""
    first_dispatch = _ExactAdmissionResponse(200,
                                             state='DISPATCH_MAY_HAVE_OCCURRED',
                                             revision=1)
    session = _ExactAdmissionSession([
        qualifier.aiohttp.ClientConnectionError('lost response'),
        first_dispatch,
        hop,
        *tail,
    ])
    monkeypatch.setattr(qualifier.asyncio, 'sleep', _skip_exact_retry_delay)

    receipt, _ = _submit_exact_with_session(session)

    assert receipt.state == state
    assert [call[0] for call in session.calls] == expected_urls


@pytest.mark.parametrize(
    ('pending', 'current'), [
        (qualifier.ExactAsyncReceipt(
            attempt_id='11111111-1111-4111-8111-111111111111',
            attempt_no=1,
            state='DISPATCH_MAY_HAVE_OCCURRED',
            revision=1),
         qualifier.ExactAsyncReceipt(
             attempt_id='11111111-1111-4111-8111-111111111111',
             attempt_no=1,
             state='REJECTED_PRE_DISPATCH',
             revision=1)),
        (qualifier.ExactAsyncReceipt(
            attempt_id='11111111-1111-4111-8111-111111111111',
            attempt_no=1,
            state='DISPATCH_MAY_HAVE_OCCURRED',
            revision=1),
         qualifier.ExactAsyncReceipt(
             attempt_id='11111111-1111-4111-8111-111111111111',
             attempt_no=1,
             state='ACCEPTED',
             revision=3)),
        (qualifier.ExactAsyncReceipt(
            attempt_id='22222222-2222-4222-8222-222222222222',
            attempt_no=2,
            state='DISPATCH_MAY_HAVE_OCCURRED',
            revision=1),
         qualifier.ExactAsyncReceipt(
             attempt_id='11111111-1111-4111-8111-111111111111',
             attempt_no=1,
             state='ACCEPTED',
             revision=2)),
    ],
    ids=['same-rejection-revision-one', 'same-invalid-revision', 'backwards'])
def test_exact_recovery_rejects_invalid_internal_rebind_hop(pending, current):
    """Recovery rejects malformed same-attempt and backwards observations."""
    with pytest.raises(qualifier.QualificationError,
                       match='conflicting receipt transition'):
        qualifier._validate_recovered_submission_receipt(
            current,
            request_id='execution-1',
            previous_rejection=None,
            pending_dispatch=pending)


@pytest.mark.parametrize(('outcomes', 'expected_urls', 'state'), [
    ([
        qualifier.aiohttp.ClientConnectionError('lost response'),
        _ExactAdmissionResponse(200, state='ACCEPTED', revision=2)
    ], [_EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL], 'ACCEPTED'),
    ([
        qualifier.aiohttp.ClientConnectionError('lost response'),
        _ExactAdmissionResponse(200, state='SUCCEEDED', revision=3)
    ], [_EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL], 'SUCCEEDED'),
    ([_ExactAdmissionResponse(409, state='ACCEPTED', revision=2)
     ], [_EXACT_SUBMIT_URL], 'ACCEPTED'),
    ([_ExactAdmissionResponse(409, state='SUCCEEDED', revision=3)
     ], [_EXACT_SUBMIT_URL], 'SUCCEEDED'),
    ([
        qualifier.aiohttp.ClientConnectionError('lost response'),
        _ExactAdmissionResponse(
            200, state='DISPATCH_MAY_HAVE_OCCURRED', revision=1),
        _ExactAdmissionResponse(200, state='ACCEPTED', revision=2)
    ], [_EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL, _EXACT_RECEIPT_URL], 'ACCEPTED'),
    ([
        _ExactAdmissionResponse(
            409, state='DISPATCH_MAY_HAVE_OCCURRED', revision=1),
        _ExactAdmissionResponse(200, state='ACCEPTED', revision=2)
    ], [_EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL], 'ACCEPTED'),
],
                         ids=[
                             'lost-accepted', 'lost-succeeded',
                             'duplicate-accepted', 'duplicate-succeeded',
                             'lost-dispatch-poll', 'duplicate-dispatch-poll'
                         ])
def test_exact_async_request_recovers_durable_success_without_redispatch(
        monkeypatch, outcomes, expected_urls, state):
    """Lookup and complete 409 receipts share one read-only recovery path."""
    monkeypatch.setattr(qualifier.asyncio, 'sleep', _skip_exact_retry_delay)
    session = _ExactAdmissionSession(outcomes)

    receipt, _ = _submit_exact_with_session(session)

    assert receipt.state == state
    assert [call[0] for call in session.calls] == expected_urls


@pytest.mark.parametrize(('initial_outcomes', 'expected_urls'), [
    ([
        qualifier.aiohttp.ClientConnectionError('lost response'),
        _ExactAdmissionResponse(200, state='REJECTED_PRE_DISPATCH', revision=2)
    ], [_EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL, _EXACT_SUBMIT_URL]),
    ([_ExactAdmissionResponse(409, state='REJECTED_PRE_DISPATCH', revision=2)
     ], [_EXACT_SUBMIT_URL, _EXACT_SUBMIT_URL]),
],
                         ids=['lookup-rejection', 'duplicate-rejection'])
def test_exact_async_request_retries_only_after_durable_predispatch_rejection(
        monkeypatch, initial_outcomes, expected_urls):
    """A durable pre-dispatch rejection alone authorizes a later attempt."""
    successor = '22222222-2222-4222-8222-222222222222'
    session = _ExactAdmissionSession([
        *initial_outcomes,
        _accepted_exact_response(attempt_id=successor, attempt_no=2),
    ])
    monkeypatch.setattr(qualifier.asyncio, 'sleep', _skip_exact_retry_delay)

    receipt, _ = _submit_exact_with_session(session)

    assert receipt.attempt_id == successor
    assert [call[0] for call in session.calls] == expected_urls
    submit_calls = [
        kwargs for url, kwargs in session.calls if url == _EXACT_SUBMIT_URL
    ]
    assert len({call['data'] for call in submit_calls}) == 1


@pytest.mark.parametrize('via_lookup', [True, False],
                         ids=['lookup', 'duplicate-post'])
@pytest.mark.parametrize('state',
                         ['AMBIGUOUS', 'FAILED', 'CANCELLED', 'EXPIRED'])
def test_exact_async_request_fails_closed_on_non_success_durable_receipt(
        monkeypatch, state, via_lookup):
    """Ambiguous or non-success terminal attempts can never be replayed."""
    outcomes = [
        _ExactAdmissionResponse(200 if via_lookup else 409,
                                state=state,
                                revision=2)
    ]
    if via_lookup:
        outcomes.insert(
            0, qualifier.aiohttp.ClientConnectionError('lost response'))
    session = _ExactAdmissionSession(outcomes)
    monkeypatch.setattr(qualifier.asyncio, 'sleep', _skip_exact_retry_delay)

    with pytest.raises(qualifier.QualificationError, match=state):
        _submit_exact_with_session(session)

    assert [call[0] for call in session.calls].count(_EXACT_SUBMIT_URL) == 1


def test_exact_async_request_does_not_trust_unfenced_lookup_miss(monkeypatch):
    """Only an exact endpoint 404, not a generic proxy miss, permits replay."""
    session = _ExactAdmissionSession([
        qualifier.aiohttp.ClientConnectionError('lost response'),
        _ExactAdmissionResponse(404, exact_fence=False),
    ])
    monkeypatch.setattr(qualifier.asyncio, 'sleep', _skip_exact_retry_delay)

    with pytest.raises(qualifier.QualificationError,
                       match='Async-Ledger-Protocol'):
        _submit_exact_with_session(session)

    assert len(session.calls) == 2


@pytest.mark.parametrize(('outcomes', 'expected_urls'), [
    ([
        qualifier.aiohttp.ClientConnectionError('lost response'),
        _ExactAdmissionResponse(
            200, state='DISPATCH_MAY_HAVE_OCCURRED', revision=1),
        _ExactAdmissionResponse(404)
    ], [_EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL, _EXACT_RECEIPT_URL]),
    ([
        _ExactAdmissionResponse(
            202,
            state='ACCEPTED',
            revision=2,
            body=qualifier.aiohttp.ClientPayloadError('truncated body')),
        _ExactAdmissionResponse(404)
    ], [_EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL]),
    ([
        qualifier.aiohttp.ClientConnectionError('lost response'),
        _ExactAdmissionResponse(200, state='REJECTED_PRE_DISPATCH', revision=2),
        qualifier.aiohttp.ClientConnectionError('lost successor response'),
        _ExactAdmissionResponse(404)
    ], [
        _EXACT_SUBMIT_URL, _EXACT_RECEIPT_URL, _EXACT_SUBMIT_URL,
        _EXACT_RECEIPT_URL
    ]),
],
                         ids=[
                             'pending-dispatch', 'truncated-acceptance',
                             'previous-rejection'
                         ])
def test_exact_async_request_fails_closed_if_durable_receipt_disappears(
        monkeypatch, outcomes, expected_urls):
    """A 404 cannot erase already observed durable attempt evidence."""
    session = _ExactAdmissionSession(outcomes)
    monkeypatch.setattr(qualifier.asyncio, 'sleep', _skip_exact_retry_delay)

    with pytest.raises(qualifier.QualificationError,
                       match='lost a previously durable exact admission'):
        _submit_exact_with_session(session)

    assert [call[0] for call in session.calls] == expected_urls


def test_exact_async_request_bounds_unreadable_receipt_recovery(monkeypatch):
    """Transport loss polls read-only until the shared deadline, never POSTs."""

    class Session:

        def __init__(self):
            self.calls = []

        def post(self, url, **kwargs):
            self.calls.append((url, kwargs))
            raise qualifier.aiohttp.ClientConnectionError('unreadable')

    ticks = iter([0.0, 0.5, 1.0])
    monkeypatch.setattr(qualifier, 'time',
                        types.SimpleNamespace(monotonic=lambda: next(ticks)))

    async def fake_sleep(_delay):
        return None

    monkeypatch.setattr(qualifier.asyncio, 'sleep', fake_sleep)
    session = Session()
    with pytest.raises(qualifier.QualificationError,
                       match='exhausted its exact admission deadline'):
        asyncio.run(
            qualifier._submit_exact_async_request(
                session,
                endpoint='https://service.test',
                token='secret',
                service_hash='incarnation-a',
                request_id='execution-1',
                stable_job_id='job-1',
                duration_seconds=0,
                deadline=1.0))

    assert [call[0] for call in session.calls] == [
        'https://service.test/v1/models/model:predict',
        'https://service.test/_lb/async-request-receipt',
    ]


def test_exact_async_request_retries_429_503_with_stable_identity_and_jitter(
        monkeypatch):
    rejected_attempt_id = '11111111-1111-4111-8111-111111111111'
    accepted_attempt_id = '22222222-2222-4222-8222-222222222222'

    class Response:
        """One exact async admission response."""

        def __init__(self,
                     status,
                     state,
                     revision,
                     body=b'{}',
                     *,
                     attempt_id=rejected_attempt_id,
                     attempt_no=1):
            self.status = status
            self._body = body
            self.headers = {
                'Retry-After': '1',
                'X-SkyServe-Async-Ledger-Protocol': '1',
                'X-SkyServe-Service-Incarnation': 'incarnation-a',
                'X-SkyServe-Async-Attempt-Id': attempt_id,
                'X-SkyServe-Async-Attempt-No': str(attempt_no),
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
        """Return two pre-dispatch rejections and one acceptance."""

        def __init__(self):
            accepted = json.dumps({
                'request_id': 'execution-1',
                'status': 'accepted',
            }).encode()
            self.responses = [
                Response(429, 'REJECTED_PRE_DISPATCH', 1),
                Response(503, 'REJECTED_PRE_DISPATCH', 1),
                Response(202,
                         'ACCEPTED',
                         2,
                         accepted,
                         attempt_id=accepted_attempt_id,
                         attempt_no=2),
            ]
            self.calls = []

        def post(self, url, **kwargs):
            self.calls.append((url, kwargs))
            return self.responses.pop(0)

    delays = []

    async def fake_sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(qualifier.asyncio, 'sleep', fake_sleep)
    session = Session()
    admission = asyncio.run(
        qualifier._submit_exact_async_request(
            session,
            endpoint='https://service.test',
            token='secret',
            service_hash='incarnation-a',
            request_id='execution-1',
            stable_job_id='job-1',
            duration_seconds=0,
            deadline=qualifier.time.monotonic() + 2))
    receipt = admission.receipt

    assert receipt.attempt_id == accepted_attempt_id
    assert receipt.attempt_no == 2
    assert len(session.calls) == 3
    assert len({call[1]['data'] for call in session.calls}) == 1
    assert {call[1]['headers']['X-SkyServe-Job-Id'] for call in session.calls
           } == {'job-1'}
    assert len(delays) == 2
    assert 0.1 <= delays[0] <= 10
    assert 0.1 <= delays[1] <= 10
    assert delays[0] != delays[1]
    assert (qualifier._bounded_retry_delay('1',
                                           attempt=0,
                                           request_id='execution-1')
            != qualifier._bounded_retry_delay('1',
                                              attempt=0,
                                              request_id='execution-2'))


@pytest.mark.parametrize(
    ('previous', 'current', 'accepted_response', 'valid'), [
        (None, ('accepted', 1, 'ACCEPTED', 2), True, True),
        (None, ('accepted', 1, 'SUCCEEDED', 2), True, True),
        (None, ('accepted', 1, 'SUCCEEDED', 3), True, True),
        (None, ('accepted', 1, 'ACCEPTED', 1), True, False),
        (None, ('accepted', 1, 'ACCEPTED', 99), True, False),
        (None, ('accepted', 1, 'SUCCEEDED', 99), True, False),
        (None, ('rejected', 1, 'REJECTED_PRE_DISPATCH', 1), False, True),
        (None, ('rejected', 1, 'REJECTED_PRE_DISPATCH', 2), False, True),
        (None, ('rejected', 2, 'REJECTED_PRE_DISPATCH', 1), False, False),
        (None, ('rejected', 2, 'REJECTED_PRE_DISPATCH', 2), False, True),
        (None, ('rejected', 2, 'REJECTED_PRE_DISPATCH', 99), False, False),
        (('rejected', 1, 'REJECTED_PRE_DISPATCH', 1),
         ('rejected', 1, 'REJECTED_PRE_DISPATCH', 1), False, True),
        (('rejected', 1, 'REJECTED_PRE_DISPATCH', 1),
         ('rejected', 1, 'REJECTED_PRE_DISPATCH', 2), False, False),
        (('rejected', 1, 'REJECTED_PRE_DISPATCH', 1),
         ('successor', 2, 'REJECTED_PRE_DISPATCH', 1), False, False),
        (('rejected', 1, 'REJECTED_PRE_DISPATCH', 1),
         ('successor', 2, 'REJECTED_PRE_DISPATCH', 2), False, True),
        (('rejected', 1, 'REJECTED_PRE_DISPATCH', 1),
         ('successor', 3, 'ACCEPTED', 2), True, True),
        (('rejected', 1, 'REJECTED_PRE_DISPATCH', 1),
         ('successor', 4, 'SUCCEEDED', 3), True, True),
        (('rejected', 1, 'REJECTED_PRE_DISPATCH', 1),
         ('successor', 2, 'REJECTED_PRE_DISPATCH', 99), False, False),
        (('rejected', 1, 'REJECTED_PRE_DISPATCH', 1),
         ('successor', 2, 'ACCEPTED', 99), True, False),
        (('rejected', 1, 'REJECTED_PRE_DISPATCH', 1),
         ('successor', 2, 'SUCCEEDED', 99), True, False),
        (('rejected', 1, 'REJECTED_PRE_DISPATCH', 1),
         ('rejected', 1, 'ACCEPTED', 2), True, False),
        (None, ('accepted', 1, 'FAILED', 2), True, False),
    ])
def test_exact_async_submission_receipt_state_machine(previous, current,
                                                      accepted_response, valid):
    """Only ledger-reachable submission receipts qualify the campaign."""

    def receipt(fields):
        if fields is None:
            return None
        attempt_id, attempt_no, state, revision = fields
        attempt_ids = {
            'rejected': '11111111-1111-4111-8111-111111111111',
            'accepted': '22222222-2222-4222-8222-222222222222',
            'successor': '33333333-3333-4333-8333-333333333333',
        }
        return qualifier.ExactAsyncReceipt(attempt_id=attempt_ids[attempt_id],
                                           attempt_no=attempt_no,
                                           state=state,
                                           revision=revision)

    previous_receipt = receipt(previous)
    current_receipt = receipt(current)
    assert current_receipt is not None
    if valid:
        qualifier._validate_submission_receipt(
            current_receipt,
            previous_rejection=previous_receipt,
            accepted_response=accepted_response)
    else:
        with pytest.raises(qualifier.QualificationError,
                           match='conflicting receipt transition'):
            qualifier._validate_submission_receipt(
                current_receipt,
                previous_rejection=previous_receipt,
                accepted_response=accepted_response)


@pytest.mark.parametrize(('current_revision', 'valid'), [(3, True),
                                                         (99, False)])
def test_exact_async_completion_receipt_revision(current_revision, valid):
    """Terminal success advances the accepted attempt exactly once."""
    attempt_id = '11111111-1111-4111-8111-111111111111'
    accepted = qualifier.ExactAsyncReceipt(attempt_id=attempt_id,
                                           attempt_no=1,
                                           state='ACCEPTED',
                                           revision=2)
    current = qualifier.ExactAsyncReceipt(attempt_id=attempt_id,
                                          attempt_no=1,
                                          state='SUCCEEDED',
                                          revision=current_revision)
    if valid:
        qualifier._validate_completion_receipt(accepted, current)
    else:
        with pytest.raises(qualifier.QualificationError,
                           match='conflicting receipt transition'):
            qualifier._validate_completion_receipt(accepted, current)


def test_exact_async_request_accepts_terminal_success_race():
    attempt_id = '11111111-1111-4111-8111-111111111111'

    class Response:

        status = 202
        headers = {
            'X-SkyServe-Async-Ledger-Protocol': '1',
            'X-SkyServe-Service-Incarnation': 'incarnation-a',
            'X-SkyServe-Async-Attempt-Id': attempt_id,
            'X-SkyServe-Async-Attempt-No': '1',
            'X-SkyServe-Async-Ledger-Revision': '2',
            'X-SkyServe-Async-Ledger-State': 'SUCCEEDED',
        }

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        @staticmethod
        async def read():
            return json.dumps({
                'request_id': 'execution-1',
                'status': 'accepted',
            }).encode()

    class Session:

        def __init__(self):
            self.calls = 0

        def post(self, _url, **_kwargs):
            self.calls += 1
            return Response()

    session = Session()
    asyncio.run(
        qualifier._one_exact_async_request(
            session,
            endpoint='https://service.test',
            token='secret',
            service_hash='incarnation-a',
            request_id='execution-1',
            stable_job_id='job-1',
            duration_seconds=0,
            admission_deadline=(qualifier.time.monotonic() + 2),
            terminal_timeout_seconds=2))
    assert session.calls == 1


def test_exact_async_request_rejects_non_successor_after_typed_rejection(
        monkeypatch):
    attempt_id = '11111111-1111-4111-8111-111111111111'

    class Response:

        def __init__(self, status, state, revision, body=b'{}'):
            self.status = status
            self._body = body
            self.headers = {
                'Retry-After': '0.1',
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

        def __init__(self):
            accepted = json.dumps({
                'request_id': 'execution-1',
                'status': 'accepted',
            }).encode()
            self.responses = [
                Response(503, 'REJECTED_PRE_DISPATCH', 1),
                # A rejected attempt cannot itself become accepted. The
                # ledger must authorize a distinct successor attempt.
                Response(202, 'ACCEPTED', 2, accepted),
            ]

        def post(self, _url, **_kwargs):
            return self.responses.pop(0)

    async def fake_sleep(_delay):
        return None

    monkeypatch.setattr(qualifier.asyncio, 'sleep', fake_sleep)
    with pytest.raises(qualifier.QualificationError,
                       match='conflicting receipt transition'):
        asyncio.run(
            qualifier._submit_exact_async_request(
                Session(),
                endpoint='https://service.test',
                token='secret',
                service_hash='incarnation-a',
                request_id='execution-1',
                stable_job_id='job-1',
                duration_seconds=0,
                deadline=qualifier.time.monotonic() + 2))


def test_exact_completion_retries_with_stable_exponential_jitter(monkeypatch):
    attempt_id = '11111111-1111-4111-8111-111111111111'

    class Response:

        def __init__(self, status, *, terminal=False):
            self.status = status
            self.headers = {'Retry-After': '0.5'}
            if terminal:
                self.headers.update({
                    'X-SkyServe-Async-Ledger-Protocol': '1',
                    'X-SkyServe-Service-Incarnation': 'incarnation-a',
                    'X-SkyServe-Async-Attempt-Id': attempt_id,
                    'X-SkyServe-Async-Attempt-No': '1',
                    'X-SkyServe-Async-Ledger-Revision': '3',
                    'X-SkyServe-Async-Ledger-State': 'SUCCEEDED',
                })

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        @staticmethod
        async def read():
            return b''

    class Session:

        def __init__(self):
            self.responses = [
                Response(503),
                Response(409),
                Response(204, terminal=True),
            ]
            self.calls = []

        def post(self, url, **kwargs):
            self.calls.append((url, kwargs))
            return self.responses.pop(0)

    delays = []

    async def fake_sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(qualifier.asyncio, 'sleep', fake_sleep)
    session = Session()
    accepted = qualifier.ExactAsyncReceipt(attempt_id=attempt_id,
                                           attempt_no=1,
                                           state='ACCEPTED',
                                           revision=2)
    asyncio.run(
        qualifier._complete_exact_async_request(
            session,
            endpoint='https://service.test',
            token='secret',
            service_hash='incarnation-a',
            request_id='execution-1',
            intent_sha256='a' * 64,
            accepted=accepted,
            processing_time_us=10,
            deadline=qualifier.time.monotonic() + 2))

    assert len(session.calls) == 3
    assert len({
        json.dumps(call[1]['json'], sort_keys=True) for call in session.calls
    }) == 1
    assert delays == [
        qualifier._bounded_retry_delay('0.5',
                                       attempt=attempt,
                                       request_id='execution-1')
        for attempt in (0, 1)
    ]


def test_exact_async_request_publishes_terminal_after_declared_work():
    """A request's terminal callback is not controlled by an observer."""
    admitted: set[str] = set()
    completed: set[str] = set()
    active_requests = 0
    peak_active_requests = 0

    def receipt_headers(request_id, *, state, revision):
        attempt_id = str(uuid.uuid5(uuid.NAMESPACE_URL, request_id))
        return {
            qualifier.serve_constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER: '1',
            qualifier.serve_constants.LB_ASYNC_SERVICE_INCARNATION_HEADER: 'incarnation-a',
            qualifier.serve_constants.LB_ASYNC_ATTEMPT_ID_HEADER: attempt_id,
            qualifier.serve_constants.LB_ASYNC_ATTEMPT_NO_HEADER: '1',
            qualifier.serve_constants.LB_ASYNC_LEDGER_REVISION_HEADER:
                str(revision),
            qualifier.serve_constants.LB_ASYNC_LEDGER_STATE_HEADER: state,
        }

    async def predict(request):
        nonlocal active_requests, peak_active_requests
        active_requests += 1
        peak_active_requests = max(peak_active_requests, active_requests)
        try:
            payload = await request.json()
            request_id = payload['request_id']
            await asyncio.sleep(0.002)
            assert request_id not in admitted
            admitted.add(request_id)
            return aiohttp.web.json_response(
                {
                    'request_id': request_id,
                    'status': 'accepted',
                },
                status=202,
                headers=receipt_headers(request_id,
                                        state='ACCEPTED',
                                        revision=2))
        finally:
            active_requests -= 1

    async def complete(request):
        payload = await request.json()
        request_id = payload['request_id']
        assert request_id in admitted
        assert request_id not in completed
        completed.add(request_id)
        return aiohttp.web.Response(status=204,
                                    headers=receipt_headers(request_id,
                                                            state='SUCCEEDED',
                                                            revision=3))

    async def exercise():
        app = aiohttp.web.Application()
        app.router.add_post('/v1/models/model:predict', predict)
        app.router.add_post(
            qualifier.serve_constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
            complete)
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        assert site._server is not None
        endpoint = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
        traffic = asyncio.create_task(
            qualifier.send_exact_async_requests(endpoint=endpoint,
                                                token='secret',
                                                service_hash='incarnation-a',
                                                prefix='held',
                                                count=24,
                                                concurrency=24,
                                                hold_requests=24,
                                                hold_seconds=0.01,
                                                timeout_seconds=5,
                                                request_queue_timeout_seconds=5,
                                                terminal_timeout_seconds=5))
        try:
            assert await asyncio.wait_for(traffic, timeout=3) == 24
        finally:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)
            await runner.cleanup()

    asyncio.run(exercise())
    assert completed == admitted
    assert peak_active_requests <= 24


def _aiohttp_exact_receipt_headers(request_id, *, state, revision):
    return {
        qualifier.serve_constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER: '1',
        qualifier.serve_constants.LB_ASYNC_SERVICE_INCARNATION_HEADER: 'incarnation-a',
        qualifier.serve_constants.LB_ASYNC_ATTEMPT_ID_HEADER: str(
            uuid.uuid5(uuid.NAMESPACE_URL, request_id)),
        qualifier.serve_constants.LB_ASYNC_ATTEMPT_NO_HEADER: '1',
        qualifier.serve_constants.LB_ASYNC_LEDGER_REVISION_HEADER:
            str(revision),
        qualifier.serve_constants.LB_ASYNC_LEDGER_STATE_HEADER: state,
    }


async def _start_exact_protocol_server(predict, complete):
    app = aiohttp.web.Application()
    app.router.add_post('/v1/models/model:predict', predict)
    app.router.add_post(
        qualifier.serve_constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH,
        complete)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    assert site._server is not None
    endpoint = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
    return runner, endpoint


def test_exact_campaign_worker_failure_drains_accepted_sibling():
    """One pre-admission failure cannot strand two accepted siblings."""
    offered = []
    terminalized = []
    accepted_siblings = 0
    both_siblings_accepted = asyncio.Event()

    async def predict(request):
        nonlocal accepted_siblings
        payload = await request.json()
        request_id = payload['request_id']
        offered.append(request_id)
        if request_id.endswith('00000'):
            await both_siblings_accepted.wait()
            return aiohttp.web.Response(status=400)
        accepted_siblings += 1
        if accepted_siblings == 2:
            both_siblings_accepted.set()
        return aiohttp.web.json_response(
            {
                'request_id': request_id,
                'status': 'accepted',
            },
            status=202,
            headers=_aiohttp_exact_receipt_headers(request_id,
                                                   state='ACCEPTED',
                                                   revision=2))

    async def complete(request):
        payload = await request.json()
        request_id = payload['request_id']
        terminalized.append(request_id)
        return aiohttp.web.Response(status=204,
                                    headers=_aiohttp_exact_receipt_headers(
                                        request_id,
                                        state='SUCCEEDED',
                                        revision=3))

    async def exercise():
        runner, endpoint = await _start_exact_protocol_server(predict, complete)
        progress = qualifier.ExactRequestCampaignProgress(total_count=4,
                                                          window_size=3)
        try:
            with pytest.raises(qualifier.QualificationError,
                               match='00000 returned HTTP 400'):
                await qualifier.send_exact_async_requests(
                    endpoint=endpoint,
                    token='secret',
                    service_hash='incarnation-a',
                    prefix='worker-failure',
                    count=4,
                    concurrency=3,
                    hold_requests=4,
                    hold_seconds=0.02,
                    timeout_seconds=2,
                    request_queue_timeout_seconds=2,
                    terminal_timeout_seconds=2,
                    campaign_progress=progress)
            return await progress.snapshot()
        finally:
            await asyncio.sleep(0.05)
            await runner.cleanup()

    snapshot = asyncio.run(exercise())
    assert len(offered) == 3
    assert set(
        terminalized) == set(offered) - {'worker-failure-execution-00000'}
    assert snapshot == qualifier.ExactRequestCampaignCounters(
        offered=3, succeeded=2, accepting_offers=False)


def test_exact_campaign_caller_cancellation_drains_accepted_workers():
    """Caller cancellation stops admission but cannot cancel accepted work."""
    offered = []
    terminalized = []
    both_accepted = asyncio.Event()

    async def predict(request):
        payload = await request.json()
        request_id = payload['request_id']
        offered.append(request_id)
        if len(offered) == 2:
            both_accepted.set()
        return aiohttp.web.json_response(
            {
                'request_id': request_id,
                'status': 'accepted',
            },
            status=202,
            headers=_aiohttp_exact_receipt_headers(request_id,
                                                   state='ACCEPTED',
                                                   revision=2))

    async def complete(request):
        payload = await request.json()
        request_id = payload['request_id']
        terminalized.append(request_id)
        return aiohttp.web.Response(status=204,
                                    headers=_aiohttp_exact_receipt_headers(
                                        request_id,
                                        state='SUCCEEDED',
                                        revision=3))

    async def exercise():
        runner, endpoint = await _start_exact_protocol_server(predict, complete)
        progress = qualifier.ExactRequestCampaignProgress(total_count=3,
                                                          window_size=2)
        traffic = asyncio.create_task(
            qualifier.send_exact_async_requests(endpoint=endpoint,
                                                token='secret',
                                                service_hash='incarnation-a',
                                                prefix='caller-cancel',
                                                count=3,
                                                concurrency=2,
                                                hold_requests=3,
                                                hold_seconds=0.02,
                                                timeout_seconds=2,
                                                request_queue_timeout_seconds=2,
                                                terminal_timeout_seconds=2,
                                                campaign_progress=progress))
        try:
            await both_accepted.wait()
            traffic.cancel()
            with pytest.raises(asyncio.CancelledError):
                await traffic
            return await progress.snapshot()
        finally:
            if not traffic.done():
                traffic.cancel()
                await asyncio.gather(traffic, return_exceptions=True)
            await runner.cleanup()

    snapshot = asyncio.run(exercise())
    assert len(offered) == 2
    assert set(terminalized) == set(offered)
    assert snapshot == qualifier.ExactRequestCampaignCounters(
        offered=2, succeeded=2, accepting_offers=False)


def test_late_exact_acceptance_gets_a_fresh_terminal_deadline():
    """A valid late 202 has callback budget independent of admission cutoff."""
    terminalized = []

    async def predict(request):
        payload = await request.json()
        request_id = payload['request_id']
        await asyncio.sleep(0.08)
        return aiohttp.web.json_response(
            {
                'request_id': request_id,
                'status': 'accepted',
            },
            status=202,
            headers=_aiohttp_exact_receipt_headers(request_id,
                                                   state='ACCEPTED',
                                                   revision=2))

    async def complete(request):
        payload = await request.json()
        request_id = payload['request_id']
        terminalized.append(request_id)
        return aiohttp.web.Response(status=204,
                                    headers=_aiohttp_exact_receipt_headers(
                                        request_id,
                                        state='SUCCEEDED',
                                        revision=3))

    async def exercise():
        runner, endpoint = await _start_exact_protocol_server(predict, complete)
        try:
            return await qualifier.send_exact_async_requests(
                endpoint=endpoint,
                token='secret',
                service_hash='incarnation-a',
                prefix='late-acceptance',
                count=1,
                concurrency=1,
                hold_requests=1,
                hold_seconds=0,
                timeout_seconds=0.05,
                request_queue_timeout_seconds=0.2,
                terminal_timeout_seconds=0.2)
        finally:
            await runner.cleanup()

    assert asyncio.run(exercise()) == 1
    assert terminalized == ['late-acceptance-execution-00000']


def test_malformed_accepted_body_terminalizes_before_protocol_error():
    """Accepted headers own a callback even when the 202 body is malformed."""
    terminalized = []

    async def predict(request):
        payload = await request.json()
        request_id = payload['request_id']
        return aiohttp.web.Response(body=b'{malformed',
                                    status=202,
                                    headers=_aiohttp_exact_receipt_headers(
                                        request_id,
                                        state='ACCEPTED',
                                        revision=2))

    async def complete(request):
        payload = await request.json()
        request_id = payload['request_id']
        terminalized.append(request_id)
        return aiohttp.web.Response(status=204,
                                    headers=_aiohttp_exact_receipt_headers(
                                        request_id,
                                        state='SUCCEEDED',
                                        revision=3))

    async def exercise():
        runner, endpoint = await _start_exact_protocol_server(predict, complete)
        progress = qualifier.ExactRequestCampaignProgress(total_count=1,
                                                          window_size=1)
        try:
            with pytest.raises(qualifier.QualificationError,
                               match='returned invalid JSON'):
                await qualifier.send_exact_async_requests(
                    endpoint=endpoint,
                    token='secret',
                    service_hash='incarnation-a',
                    prefix='malformed-acceptance',
                    count=1,
                    concurrency=1,
                    hold_requests=1,
                    hold_seconds=0,
                    timeout_seconds=1,
                    request_queue_timeout_seconds=1,
                    terminal_timeout_seconds=1,
                    campaign_progress=progress)
            return await progress.snapshot()
        finally:
            await runner.cleanup()

    snapshot = asyncio.run(exercise())
    assert terminalized == ['malformed-acceptance-execution-00000']
    assert snapshot == qualifier.ExactRequestCampaignCounters(
        offered=1, succeeded=1, accepting_offers=False)


def test_request_telemetry_requires_exact_positive_and_terminal_delta(tmp_path):
    baseline = _request_telemetry()
    positive = _request_telemetry(queue_depth=7,
                                  in_flight=5,
                                  processing=3,
                                  state_counts={'ACCEPTED': 5})
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

        @staticmethod
        async def campaign_terminal_membership(prefix, count):
            return qualifier._campaign_manifest_sha256(prefix, count)

    async def exercise():
        profile = dataclasses.replace(qualifier.PROFILES['small'],
                                      poll_seconds=0)
        receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                    service_name='paid-e2e',
                                    profile=profile)
        receipt.bind_campaign_manifest('terminal-proof', 16)
        held = asyncio.create_task(asyncio.Event().wait())
        try:
            observed_positive = await (
                qualifier._wait_for_positive_request_telemetry(
                    observer=Observer([positive]),
                    profile=profile,
                    receipt=receipt,
                    traffic=held,
                    baseline=baseline,
                    deadline_monotonic=time.monotonic() + 1))
            observed_final = await qualifier._wait_for_final_request_telemetry(
                observer=Observer([final]),
                profile=profile,
                receipt=receipt,
                baseline=baseline,
                expected_succeeded_delta=16,
                campaign_prefix='terminal-proof')
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
    assert receipt._payload['campaign_terminal_membership_sha256'] == (
        qualifier._campaign_manifest_sha256('terminal-proof', 16))


def test_campaign_terminal_membership_rejects_equal_count_substitution():
    prefix = 'exact-membership'
    keys = qualifier._campaign_request_key_sha256s(prefix, 3)
    expected_digest = qualifier._campaign_manifest_sha256(prefix, 3)
    rows = [{
        'request_key_sha256': key,
        'state': 'SUCCEEDED',
    } for key in keys]

    assert qualifier._validate_campaign_terminal_rows(
        prefix=prefix, count=3, rows=rows) == expected_digest
    rows[-1] = {
        'request_key_sha256': hashlib.sha256(b'unrelated').hexdigest(),
        'state': 'SUCCEEDED',
    }
    with pytest.raises(qualifier.QualificationError,
                       match='exact campaign membership'):
        qualifier._validate_campaign_terminal_rows(prefix=prefix,
                                                   count=3,
                                                   rows=rows)


def _request_evidence_sample(*,
                             phase,
                             queue_depth,
                             in_flight,
                             processing,
                             accepted=0,
                             succeeded=0,
                             observed_at=1000.0,
                             **extra):
    values = {
        'ACCEPTED': accepted,
        'SUCCEEDED': succeeded,
    }
    counts = [[state.value, values.get(state.value, 0)]
              for state in qualifier.async_request_ledger.AsyncRequestState]
    return {
        'phase': phase,
        'observed_at': observed_at,
        'state': 'fresh',
        'reason': 'complete',
        'compatibility_complete': True,
        'queue_depth': queue_depth,
        'in_flight_requests': in_flight,
        'processing_requests': processing,
        'confirmed_in_flight_requests': in_flight,
        'confirmed_processing_requests': processing,
        'ledger_state_counts': counts,
        'ledger_active': accepted,
        'ledger_succeeded': succeeded,
        'ledger_total': accepted + succeeded,
        **extra,
    }


def _qualification_provider_projection(peaks):
    projection = {}
    for cloud in ('aws', 'gcp'):
        count = peaks.get(cloud, 0)
        shapes = ([] if count == 0 else [{
            'gpu_units_per_instance': 1,
            'instance_count': count,
            'instance_type': f'{cloud}-l4-qualification',
            'running_count': count,
            'running_gpu_units': count,
        }])
        projection[cloud] = {
            'instances': count,
            'running': count,
            'gpu_units': count,
            'running_gpu_units': count,
            'disks': count,
            'inflight_operations': 0,
            'shapes': shapes,
        }
    return projection


def _zero_qualification_sample(observed_at,
                               *,
                               phase='drain',
                               iteration_id=None):
    sample = {
        'phase': phase,
        'observed_at': observed_at,
        'exact_zero': True,
        **{
            field: 0 for field in qualifier._ZERO_OBSERVATION_FIELDS
        },
        'provider_by_cloud': _qualification_provider_projection({}),
        'paid_claim_priority_units': [],
        'lb_offered_arrival_tracking_saturated': False,
    }
    if phase == 'baseline':
        sample['baseline_iteration_id'] = iteration_id
        sample['baseline_pair_observed_at'] = observed_at
    return sample


def _write_aggregate_qualification(path,
                                   *,
                                   service_name,
                                   kind,
                                   providers,
                                   peaks,
                                   source_sha256='e' * 64,
                                   authorized_economic_receipt_sha256=None):
    economic = kind == 'economic'
    count = 10_000 if economic else 1
    profile = (qualifier.PROFILES['scale']
               if economic else qualifier.PROFILES['provider-canary'])
    campaign_prefix = f'{service_name}-campaign'
    campaign_manifest_sha256 = qualifier._campaign_manifest_sha256(
        campaign_prefix, count)
    stimulus = qualifier.scale_stimulus_count(profile)
    in_flight = min(128, stimulus) if economic else 1
    active = in_flight
    queue_depth = stimulus - in_flight if economic else 0
    processing = max(1, in_flight // 2)
    provider_projection = _qualification_provider_projection(peaks)
    scale_started_at = 4.0
    scale_observed_at = 250.0 if economic else 130.0
    scale_sample = {
        **{
            field: 0 for field in qualifier._ZERO_OBSERVATION_FIELDS
        },
        'phase': 'scale',
        'scale_iteration_id': 1,
        'observed_at': scale_observed_at,
        'exact_zero': False,
        'provider_instances': sum(peaks.values()),
        'provider_running': sum(peaks.values()),
        'provider_gpu_units': sum(peaks.values()),
        'provider_running_gpu_units': sum(peaks.values()),
        'provider_disks': sum(peaks.values()),
        'provider_inflight_operations': 0,
        'provider_by_cloud': provider_projection,
        'paid_claim_priority_units': [],
        'postgres_demand_units': stimulus,
        'lb_demand_units': stimulus,
        'lb_unique_job_arrivals_60s': stimulus,
        'lb_unique_job_arrivals_300s': stimulus,
        'lb_offered_arrival_tracking_saturated': False,
        **({
            'campaign_offered': stimulus,
            'campaign_succeeded': 0,
        } if economic else {}),
    }
    payload = {
        'schema_version': 13,
        'service_name': service_name,
        'service_hash': f'{service_name}-hash',
        'lifecycle_epoch': 1,
        'service_version': 1,
        'controller_config_digest': 'a' * 64,
        'controller_config_snapshot_id': 'b' * 64,
        'service_yaml_sha256': ('c' if economic else 'd') * 64,
        'qualification_profile': profile.name,
        'qualification_source_sha256': source_sha256,
        'qualification_projection_sha256':
            qualifier._qualification_projection_sha256(
                source_sha256=source_sha256,
                profile=profile,
                providers=tuple(providers)),
        'profile': profile.name,
        'expectation_kind': kind,
        'expected_providers': providers,
        'request_priority': qualifier._REQUEST_PRIORITY,
        'max_units': profile.max_units,
        'minimum_running': 100 if economic else 1,
        'peak_running': sum(peaks.values()),
        'peak_running_by_cloud': {
            cloud: peaks.get(cloud, 0) for cloud in ('aws', 'gcp')
        },
        'peak_running_gpu_units': sum(peaks.values()),
        'peak_running_gpu_units_by_cloud': {
            cloud: peaks.get(cloud, 0) for cloud in ('aws', 'gcp')
        },
        'scale_started_at': scale_started_at,
        'scale_slo_seconds': profile.scale_slo_seconds,
        'scale_timeout_seconds': profile.scale_timeout_seconds,
        'scale_slo_met': (scale_observed_at - scale_started_at
                          <= profile.scale_slo_seconds),
        'scale_qualified_observed_at': scale_observed_at,
        'scale_qualified_iteration_id': 1,
        'baseline_qualified_iteration_id': 3,
        'baseline_qualified_observed_at': 3.0,
        'exact_request_count': count,
        'exact_request_successes': count,
        'terminal_publication_timeout_seconds':
            profile.terminal_publication_timeout_seconds,
        'campaign_prefix': campaign_prefix,
        'campaign_manifest_sha256': campaign_manifest_sha256,
        'campaign_terminal_membership_sha256': campaign_manifest_sha256,
        'ledger_request_delta': count,
        'ledger_succeeded_delta': count,
        'samples': [
            _zero_qualification_sample(1.0, phase='baseline', iteration_id=1),
            _zero_qualification_sample(2.0, phase='baseline', iteration_id=2),
            _zero_qualification_sample(3.0, phase='baseline', iteration_id=3),
            scale_sample,
            _zero_qualification_sample(1000.0),
            _zero_qualification_sample(1180.0),
            _zero_qualification_sample(1360.0),
        ],
        'request_telemetry_samples': [
            *[
                _request_evidence_sample(phase='baseline',
                                         queue_depth=0,
                                         in_flight=0,
                                         processing=0,
                                         observed_at=pair_at - 0.25,
                                         baseline_iteration_id=iteration_id,
                                         baseline_pair_observed_at=pair_at)
                for iteration_id, pair_at in ((1, 1.0), (2, 2.0), (3, 3.0))
            ],
            *([
                _request_evidence_sample(phase='scale-stimulus',
                                         queue_depth=stimulus,
                                         in_flight=0,
                                         processing=0,
                                         observed_at=5.0)
            ] if economic else []),
            _request_evidence_sample(
                phase='scale',
                queue_depth=(stimulus if economic else queue_depth),
                in_flight=(0 if economic else in_flight),
                processing=(0 if economic else processing),
                accepted=(0 if economic else active),
                observed_at=(249.0 if economic else 129.0)) | {
                    'scale_iteration_id': 1,
                },
            *([
                _request_evidence_sample(phase='positive',
                                         queue_depth=queue_depth,
                                         in_flight=in_flight,
                                         processing=processing,
                                         accepted=active,
                                         observed_at=251.0)
            ] if economic else []),
            _request_evidence_sample(phase='final',
                                     queue_depth=0,
                                     in_flight=0,
                                     processing=0,
                                     succeeded=count,
                                     observed_at=500.0),
        ],
        'finished_at': 1400.0,
        'outcome': 'passed',
    }
    if not economic:
        payload['authorized_economic_receipt_sha256'] = (
            authorized_economic_receipt_sha256)
    path.write_text(json.dumps(payload), encoding='utf-8')


def _write_aggregate_cleanup(path,
                             qualification_path,
                             *,
                             service_name,
                             providers,
                             service_hash=None,
                             outcome='passed'):
    qualification = json.loads(qualification_path.read_text(encoding='utf-8'))

    def zero(index):
        return {
            'observed_at': float(index),
            'exact_zero': True,
            'zero_samples': index,
            'cleanup_claims': 0,
            'cleanup_debit_units': 0,
            'cleanup_provider_disks': 0,
            'cleanup_provider_instances': 0,
            'cleanup_provider_operations': 0,
            'cleanup_waiters': 0,
            'cleanup_provider_by_cloud': {
                cloud: {
                    'cloud': cloud,
                    'instance_count': 0,
                    'running_count': 0,
                    'gpu_units': 0,
                    'running_gpu_units': 0,
                    'disk_count': 0,
                    'inflight_operation_count': 0,
                    'shapes': [],
                } for cloud in ('aws', 'gcp')
            },
        }

    payload = {
        'schema_version': 2,
        'service_name': service_name,
        'service_hash': service_hash or f'{service_name}-hash',
        'lifecycle_epoch': 1,
        'service_version': 1,
        'controller_config_digest': 'a' * 64,
        'controller_config_snapshot_id': 'b' * 64,
        'expected_providers': providers,
        'service_yaml_sha256': qualification['service_yaml_sha256'],
        'qualification_profile': qualification['qualification_profile'],
        'qualification_source_sha256':
            qualification['qualification_source_sha256'],
        'qualification_projection_sha256':
            qualification['qualification_projection_sha256'],
        'qualification_receipt_sha256': hashlib.sha256(
            qualification_path.read_bytes()).hexdigest(),
        'outcome': outcome,
        'zero_samples': 3,
        'samples': [zero(1), zero(2), zero(3)],
    }
    path.write_text(json.dumps(payload), encoding='utf-8')


def _aggregate_args(tmp_path,
                    *,
                    with_canary=True,
                    economic_peaks=None,
                    source_sha256='e' * 64):
    economic = tmp_path / 'economic.json'
    economic_cleanup = tmp_path / 'economic-cleanup.json'
    _write_aggregate_qualification(economic,
                                   service_name='economic',
                                   kind='economic',
                                   providers=['aws', 'gcp'],
                                   source_sha256=source_sha256,
                                   peaks=economic_peaks or {
                                       'aws': 100,
                                       'gcp': 0,
                                   })
    _write_aggregate_cleanup(economic_cleanup,
                             economic,
                             service_name='economic',
                             providers=['aws', 'gcp'])
    canary_receipts = []
    canary_cleanup_receipts = []
    if with_canary:
        canary = tmp_path / 'gcp-canary.json'
        canary_cleanup = tmp_path / 'gcp-canary-cleanup.json'
        economic_sha256 = hashlib.sha256(economic.read_bytes()).hexdigest()
        _write_aggregate_qualification(
            canary,
            service_name='gcp-canary',
            kind='provider-canary',
            providers=['gcp'],
            source_sha256=source_sha256,
            peaks={'gcp': 1},
            authorized_economic_receipt_sha256=(economic_sha256))
        _write_aggregate_cleanup(canary_cleanup,
                                 canary,
                                 service_name='gcp-canary',
                                 providers=['gcp'])
        canary_receipts.append(str(canary))
        canary_cleanup_receipts.append(str(canary_cleanup))
    return type(
        'Args', (), {
            'economic_receipt': str(economic),
            'economic_cleanup_receipt': str(economic_cleanup),
            'canary_receipt': canary_receipts,
            'canary_cleanup_receipt': canary_cleanup_receipts,
            'output': str(tmp_path / 'aggregate.json'),
        })()


def test_aggregate_accepts_economic_aws_plus_absent_gcp_canary(tmp_path):
    args = _aggregate_args(tmp_path)

    qualifier.aggregate_evidence(args)

    payload = json.loads(pathlib.Path(args.output).read_text(encoding='utf-8'))
    assert payload['outcome'] == 'passed'
    assert payload['positive_provider_union'] == ['aws', 'gcp']
    assert payload['economic_exact_request_count'] == 10_000
    assert payload['economic_scale_slo_met'] is True


@pytest.mark.parametrize('positive_observed_at', [200.0, 251.0])
def test_schema_eleven_accepts_positive_before_or_after_physical_scale(
        tmp_path, positive_observed_at):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    positive = next(sample for sample in payload['request_telemetry_samples']
                    if sample['phase'] == 'positive')
    positive['observed_at'] = positive_observed_at
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    evidence = qualifier._read_qualification_evidence(
        receipt, qualifier.ExpectationKind.ECONOMIC)

    assert evidence.scale_elapsed_seconds == 246.0
    assert evidence.scale_slo_met is True


def test_schema_eleven_accepts_late_provider_convergence_and_records_slo_miss(
        tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    provider_scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    request_scale = next(
        sample for sample in payload['request_telemetry_samples']
        if sample['phase'] == 'scale')
    provider_scale['observed_at'] = 350.0
    request_scale['observed_at'] = 349.0
    payload['scale_qualified_observed_at'] = 350.0
    payload['scale_slo_met'] = False
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    evidence = qualifier._read_qualification_evidence(
        receipt, qualifier.ExpectationKind.ECONOMIC)

    assert evidence.scale_elapsed_seconds == 346.0
    assert evidence.scale_slo_met is False


@pytest.mark.parametrize(('field', 'value'), [
    ('scale_slo_seconds', '300'),
    ('scale_timeout_seconds', None),
    ('scale_slo_met', 1),
])
def test_schema_eleven_rejects_untyped_scale_timing_policy(
        tmp_path, field, value):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    payload[field] = value
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='invalid scale timing policy'):
        qualifier._read_qualification_evidence(
            receipt, qualifier.ExpectationKind.ECONOMIC)


def test_schema_eleven_accepts_positive_after_queue_fully_dispatches(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    positive = next(sample for sample in payload['request_telemetry_samples']
                    if sample['phase'] == 'positive')
    positive.update(
        _request_evidence_sample(phase='positive',
                                 queue_depth=0,
                                 in_flight=800,
                                 processing=100,
                                 accepted=800,
                                 observed_at=200.0))
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    qualifier._read_qualification_evidence(receipt,
                                           qualifier.ExpectationKind.ECONOMIC)


def test_schema_thirteen_accepts_attributed_resident_scale_with_incomplete_occupancy(
        tmp_path):
    """Offline verification applies the same partial-observation capability."""
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(sample for sample in payload['request_telemetry_samples']
                 if sample['phase'] == 'scale')
    scale.update(
        _request_evidence_sample(phase='scale',
                                 queue_depth=762,
                                 in_flight=None,
                                 processing=None,
                                 accepted=38,
                                 observed_at=249.0,
                                 reason='in_flight_incomplete',
                                 confirmed_in_flight_requests=38,
                                 confirmed_processing_requests=16,
                                 scale_iteration_id=1))
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    evidence = qualifier._read_qualification_evidence(
        receipt, qualifier.ExpectationKind.ECONOMIC)

    assert evidence.scale_elapsed_seconds == 246.0


@pytest.mark.parametrize(
    ('field', 'value'),
    [('confirmed_in_flight_requests', 39),
     ('confirmed_processing_requests', 39), ('in_flight_requests', 38),
     ('queue_depth', 761)],
)
def test_schema_thirteen_rejects_invalid_incomplete_occupancy_scale_evidence(
        tmp_path, field, value):
    args = _aggregate_args(tmp_path)
    payload = json.loads(
        pathlib.Path(args.economic_receipt).read_text(encoding='utf-8'))
    scale = next(sample for sample in payload['request_telemetry_samples']
                 if sample['phase'] == 'scale')
    scale.update(
        _request_evidence_sample(phase='scale',
                                 queue_depth=762,
                                 in_flight=None,
                                 processing=None,
                                 accepted=38,
                                 observed_at=249.0,
                                 reason='in_flight_incomplete',
                                 confirmed_in_flight_requests=38,
                                 confirmed_processing_requests=16,
                                 scale_iteration_id=1))
    scale[field] = value

    with pytest.raises(qualifier.QualificationError,
                       match='unattributed scale demand'):
        qualifier._validate_request_evidence(
            payload, profile=qualifier.PROFILES['scale'], exact_count=10_000)


@pytest.mark.parametrize(
    ('positive_observed_at', 'final_observed_at'),
    [(4.5, 500.0), (1495.0, 1600.0), (501.0, 500.0)],
)
def test_schema_eleven_rejects_positive_outside_stimulus_and_final_bounds(
        tmp_path, positive_observed_at, final_observed_at):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    positive = next(sample for sample in payload['request_telemetry_samples']
                    if sample['phase'] == 'positive')
    final = next(sample for sample in payload['request_telemetry_samples']
                 if sample['phase'] == 'final')
    positive['observed_at'] = positive_observed_at
    final['observed_at'] = final_observed_at
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='scale-stimulus|Terminal request evidence'):
        qualifier._read_qualification_evidence(
            receipt, qualifier.ExpectationKind.ECONOMIC)


@pytest.mark.parametrize('iteration_ids', [(1, 3), (2, 1)])
def test_request_scale_iteration_ids_are_contiguous_in_receipt_order(
        tmp_path, iteration_ids):
    args = _aggregate_args(tmp_path)
    payload = json.loads(
        pathlib.Path(args.economic_receipt).read_text(encoding='utf-8'))
    samples = payload['request_telemetry_samples']
    scale_index = next(index for index, sample in enumerate(samples)
                       if sample['phase'] == 'scale')
    original = samples[scale_index]
    replacements = []
    for offset, iteration_id in enumerate(iteration_ids):
        sample = copy.deepcopy(original)
        sample['scale_iteration_id'] = iteration_id
        sample['observed_at'] = 240.0 + offset
        replacements.append(sample)
    samples[scale_index:scale_index + 1] = replacements

    with pytest.raises(qualifier.QualificationError, match='contiguous'):
        qualifier._validate_request_evidence(
            payload, profile=qualifier.PROFILES['scale'], exact_count=10_000)


def test_request_scale_timestamps_are_strictly_increasing(tmp_path):
    args = _aggregate_args(tmp_path)
    payload = json.loads(
        pathlib.Path(args.economic_receipt).read_text(encoding='utf-8'))
    samples = payload['request_telemetry_samples']
    scale_index = next(index for index, sample in enumerate(samples)
                       if sample['phase'] == 'scale')
    second_scale = copy.deepcopy(samples[scale_index])
    second_scale['scale_iteration_id'] = 2
    second_scale['observed_at'] = samples[scale_index]['observed_at']
    samples.insert(scale_index + 1, second_scale)

    with pytest.raises(qualifier.QualificationError,
                       match='scale timestamps.*strictly increasing'):
        qualifier._validate_request_evidence(
            payload, profile=qualifier.PROFILES['scale'], exact_count=10_000)


def test_request_evidence_requires_canonical_phase_order(tmp_path):
    args = _aggregate_args(tmp_path)
    payload = json.loads(
        pathlib.Path(args.economic_receipt).read_text(encoding='utf-8'))
    samples = payload['request_telemetry_samples']
    scale = next(sample for sample in samples if sample['phase'] == 'scale')
    samples.remove(scale)
    samples.append(scale)

    with pytest.raises(qualifier.QualificationError,
                       match='canonical request phase order'):
        qualifier._validate_request_evidence(
            payload, profile=qualifier.PROFILES['scale'], exact_count=10_000)


@pytest.mark.parametrize(
    ('field', 'value'),
    [('queue_depth', None), ('queue_depth', '800'),
     ('in_flight_requests', None), ('in_flight_requests', [])],
)
def test_malformed_request_scale_fields_raise_qualification_error(
        tmp_path, field, value):
    args = _aggregate_args(tmp_path)
    payload = json.loads(
        pathlib.Path(args.economic_receipt).read_text(encoding='utf-8'))
    scale = next(sample for sample in payload['request_telemetry_samples']
                 if sample['phase'] == 'scale')
    if value is None:
        del scale[field]
    else:
        scale[field] = value

    with pytest.raises(qualifier.QualificationError,
                       match='request telemetry|scale demand'):
        qualifier._validate_request_evidence(
            payload, profile=qualifier.PROFILES['scale'], exact_count=10_000)


def test_provider_scale_must_precede_terminal_request_evidence(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    request_samples = payload['request_telemetry_samples']
    first_request_scale = next(
        sample for sample in request_samples if sample['phase'] == 'scale')
    second_request_scale = copy.deepcopy(first_request_scale)
    second_request_scale['scale_iteration_id'] = 2
    second_request_scale['observed_at'] = 490.0
    positive_index = next(index for index, sample in enumerate(request_samples)
                          if sample['phase'] == 'positive')
    request_samples.insert(positive_index, second_request_scale)
    request_samples[positive_index + 1]['observed_at'] = 495.0

    provider_samples = payload['samples']
    first_provider_scale = next(
        sample for sample in provider_samples if sample['phase'] == 'scale')
    second_provider_scale = copy.deepcopy(first_provider_scale)
    second_provider_scale['scale_iteration_id'] = 2
    second_provider_scale['observed_at'] = 600.0
    first_drain_index = next(
        index for index, sample in enumerate(provider_samples)
        if sample['phase'] == 'drain')
    provider_samples.insert(first_drain_index, second_provider_scale)
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='provider scale evidence|lifecycle evidence'):
        qualifier._read_qualification_evidence(
            receipt, qualifier.ExpectationKind.ECONOMIC)


def test_provider_samples_require_canonical_phase_order(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    samples = payload['samples']
    final_baseline = samples.pop(2)
    scale_index = next(index for index, sample in enumerate(samples)
                       if sample['phase'] == 'scale')
    samples.insert(scale_index + 1, final_baseline)
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='canonical provider phase order'):
        qualifier._read_qualification_evidence(
            receipt, qualifier.ExpectationKind.ECONOMIC)


def test_provider_sample_timestamps_are_strictly_increasing(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    samples = payload['samples']
    first_drain_index = next(index for index, sample in enumerate(samples)
                             if sample['phase'] == 'drain')
    samples[first_drain_index]['observed_at'] = samples[first_drain_index -
                                                        1]['observed_at']
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='provider timestamps.*strictly increasing'):
        qualifier._read_qualification_evidence(
            receipt, qualifier.ExpectationKind.ECONOMIC)


@pytest.mark.parametrize('request_priority', [49, 51, True, None, '50'])
def test_qualification_receipt_binds_exact_request_priority(
        tmp_path, request_priority):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    payload['request_priority'] = request_priority
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='request priority|malformed'):
        qualifier._read_qualification_evidence(
            receipt, qualifier.ExpectationKind.ECONOMIC)


@pytest.mark.parametrize('mutation', ('prefix', 'manifest', 'terminal'))
def test_qualification_receipt_rejects_substituted_campaign_membership(
        tmp_path, mutation):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    if mutation == 'prefix':
        payload['campaign_prefix'] = 'unrelated-campaign'
    elif mutation == 'manifest':
        payload['campaign_manifest_sha256'] = '0' * 64
    else:
        payload['campaign_terminal_membership_sha256'] = '0' * 64
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError, match='malformed'):
        qualifier._read_qualification_evidence(
            receipt, qualifier.ExpectationKind.ECONOMIC)


@pytest.mark.parametrize(
    ('claimed_units', 'priority_units'),
    [(1, [{
        'priority': 49,
        'gpu_units': 1
    }]), (2, [{
        'priority': 50,
        'gpu_units': 1
    }]), (1, [{
        'priority': '50',
        'gpu_units': 1
    }]), (1, [{
        'priority': True,
        'gpu_units': 1
    }]), (1, [{
        'priority': 50,
        'gpu_units': True
    }]), (1, [{
        'priority': 50,
        'gpu_units': 0
    }]),
     (2, [{
         'priority': 50,
         'gpu_units': 1
     }, {
         'priority': 50,
         'gpu_units': 1
     }]),
     (2, [{
         'priority': 51,
         'gpu_units': 1
     }, {
         'priority': 50,
         'gpu_units': 1
     }]), (1, [{
         'priority': 50,
         'gpu_units': 1,
         'unexpected': 0
     }]), (0, [{
         'priority': 50,
         'gpu_units': 1
     }]), (801, [{
         'priority': 50,
         'gpu_units': 801
     }]), (1, [{
         'priority': 50
     }]), (1, ['not-an-entry']), (0, None)],
)
def test_qualification_receipt_rejects_tampered_paid_claim_priority_units(
        tmp_path, claimed_units, priority_units):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    scale['claimed_units'] = claimed_units
    if priority_units is None:
        del scale['paid_claim_priority_units']
    else:
        scale['paid_claim_priority_units'] = priority_units
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='paid claim priorit'):
        qualifier._read_qualification_evidence(
            receipt, qualifier.ExpectationKind.ECONOMIC)


def test_qualification_receipt_rejects_legacy_paid_claim_priority_field(
        tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    scale['paid_claim_priorities'] = []
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='paid claim priority-unit'):
        qualifier._read_qualification_evidence(
            receipt, qualifier.ExpectationKind.ECONOMIC)


def test_aggregate_needs_no_canary_when_economic_run_proves_both(tmp_path):
    args = _aggregate_args(tmp_path,
                           with_canary=False,
                           economic_peaks={
                               'aws': 60,
                               'gcp': 40,
                           })

    qualifier.aggregate_evidence(args)

    payload = json.loads(pathlib.Path(args.output).read_text(encoding='utf-8'))
    assert len(payload['qualification_receipts']) == 1


def test_aggregate_rejects_unnecessary_provider_canary(tmp_path):
    args = _aggregate_args(tmp_path, economic_peaks={
        'aws': 60,
        'gcp': 40,
    })

    with pytest.raises(qualifier.QualificationError,
                       match='exactly cover providers absent'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_missing_provider_union(tmp_path):
    args = _aggregate_args(tmp_path, with_canary=False)

    with pytest.raises(qualifier.QualificationError,
                       match='exactly cover providers absent'):
        qualifier.aggregate_evidence(args)


@pytest.mark.parametrize('missing_field',
                         ['samples', 'request_telemetry_samples'])
def test_aggregate_rejects_receipt_without_production_evidence(
        tmp_path, missing_field):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    del payload[missing_field]
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError, match='lacks .* evidence'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_unattributed_scale_demand(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(sample for sample in payload['request_telemetry_samples']
                 if sample['phase'] == 'scale')
    scale['queue_depth'] += 1
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='unattributed scale demand'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_zero_scale_demand_even_with_provider_capacity(
        tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(sample for sample in payload['request_telemetry_samples']
                 if sample['phase'] == 'scale')
    scale.update(
        _request_evidence_sample(phase='scale',
                                 queue_depth=0,
                                 in_flight=0,
                                 processing=0))
    scale['scale_iteration_id'] = 1
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='unattributed scale demand'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_unpaired_provider_scale_iteration(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    scale['scale_iteration_id'] = 2
    payload['scale_qualified_iteration_id'] = 2
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='canonical contiguous request pairing'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_provider_scale_before_paired_request_sample(
        tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    scale['observed_at'] = 6.5
    payload['scale_qualified_observed_at'] = 6.5
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='paired exact demand|scale-stimulus'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_terminal_before_post_scale_positive(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    final = next(sample for sample in payload['request_telemetry_samples']
                 if sample['phase'] == 'final')
    final['observed_at'] = 249.5
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='Terminal request evidence'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_scale_without_same_observation_demand(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    scale['postgres_demand_units'] = 0
    scale['lb_demand_units'] = 0
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='same-observation demand'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_nonfinite_request_timestamp(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    positive = next(sample for sample in payload['request_telemetry_samples']
                    if sample['phase'] == 'positive')
    positive['observed_at'] = float('nan')
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError, match='strict timestamp'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_scale_sample_without_campaign_frontier(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    del scale['campaign_succeeded']
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='unattributed offered arrivals'):
        qualifier.aggregate_evidence(args)


def test_aggregate_accepts_arrivals_aged_out_after_stimulus_commit(tmp_path):
    args = _aggregate_args(tmp_path,
                           with_canary=False,
                           economic_peaks={
                               'aws': 50,
                               'gcp': 50,
                           })
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    first_scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    qualified_scale = copy.deepcopy(first_scale)
    qualified_scale.update({
        'scale_iteration_id': 2,
        'observation_started_at': 349.5,
        'observation_finished_at': 350.0,
        'observed_at': 350.0,
        'lb_unique_job_arrivals_60s': 0,
        'lb_unique_job_arrivals_300s': 0,
    })
    first_scale['provider_running'] = 99
    first_scale['provider_running_gpu_units'] = 99
    first_gcp = first_scale['provider_by_cloud']['gcp']
    first_gcp['running'] = 49
    first_gcp['running_gpu_units'] = 49
    first_gcp['shapes'][0]['running_count'] = 49
    first_gcp['shapes'][0]['running_gpu_units'] = 49
    first_index = payload['samples'].index(first_scale)
    payload['samples'].insert(first_index + 1, qualified_scale)

    first_request_scale = next(
        sample for sample in payload['request_telemetry_samples']
        if sample['phase'] == 'scale')
    qualified_request_scale = copy.deepcopy(first_request_scale)
    qualified_request_scale.update({
        'scale_iteration_id': 2,
        'observed_at': 349.0,
    })
    final_index = next(
        index
        for index, sample in enumerate(payload['request_telemetry_samples'])
        if sample['phase'] == 'final')
    payload['request_telemetry_samples'].insert(final_index,
                                                qualified_request_scale)
    payload.update({
        'scale_qualified_observed_at': 350.0,
        'scale_qualified_iteration_id': 2,
        'scale_slo_met': False,
    })
    receipt.write_text(json.dumps(payload), encoding='utf-8')
    _write_aggregate_cleanup(pathlib.Path(args.economic_cleanup_receipt),
                             receipt,
                             service_name='economic',
                             providers=['aws', 'gcp'])

    qualifier.aggregate_evidence(args)

    aggregate = json.loads(
        pathlib.Path(args.output).read_text(encoding='utf-8'))
    assert aggregate['outcome'] == 'passed'
    assert aggregate['economic_scale_slo_met'] is False


def test_aggregate_accepts_arrivals_after_terminal_success_frontier(tmp_path):
    args = _aggregate_args(tmp_path,
                           with_canary=False,
                           economic_peaks={
                               'aws': 50,
                               'gcp': 50,
                           })
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    first_scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    first_scale['provider_running'] = 99
    first_scale['provider_running_gpu_units'] = 99
    first_gcp = first_scale['provider_by_cloud']['gcp']
    first_gcp['running'] = 49
    first_gcp['running_gpu_units'] = 49
    first_gcp['shapes'][0]['running_count'] = 49
    first_gcp['shapes'][0]['running_gpu_units'] = 49
    aged_scale = copy.deepcopy(first_scale)
    aged_scale.update({
        'scale_iteration_id': 2,
        'observation_started_at': 349.5,
        'observation_finished_at': 350.0,
        'observed_at': 350.0,
        'lb_unique_job_arrivals_60s': 0,
        'lb_unique_job_arrivals_300s': 0,
    })
    increased_scale = copy.deepcopy(aged_scale)
    increased_scale.update({
        'scale_iteration_id': 3,
        'observation_started_at': 350.5,
        'observation_finished_at': 351.0,
        'observed_at': 351.0,
        'lb_unique_job_arrivals_60s': 1,
        'lb_unique_job_arrivals_300s': 1,
        'provider_running': 100,
        'provider_running_gpu_units': 100,
        'campaign_offered': 801,
        'campaign_succeeded': 1,
    })
    increased_gcp = increased_scale['provider_by_cloud']['gcp']
    increased_gcp['running'] = 50
    increased_gcp['running_gpu_units'] = 50
    increased_gcp['shapes'][0]['running_count'] = 50
    increased_gcp['shapes'][0]['running_gpu_units'] = 50
    first_index = payload['samples'].index(first_scale)
    payload['samples'][first_index + 1:first_index +
                       1] = [aged_scale, increased_scale]

    first_request_scale = next(
        sample for sample in payload['request_telemetry_samples']
        if sample['phase'] == 'scale')
    extra_request_scales = []
    for iteration_id, observed_at in ((2, 349.0), (3, 350.0)):
        request_scale = copy.deepcopy(first_request_scale)
        request_scale.update({
            'scale_iteration_id': iteration_id,
            'observed_at': observed_at,
        })
        extra_request_scales.append(request_scale)
    final_index = next(
        index
        for index, sample in enumerate(payload['request_telemetry_samples'])
        if sample['phase'] == 'final')
    payload['request_telemetry_samples'][final_index:final_index] = (
        extra_request_scales)
    payload.update({
        'scale_qualified_observed_at': 351.0,
        'scale_qualified_iteration_id': 3,
        'scale_slo_met': False,
    })
    receipt.write_text(json.dumps(payload), encoding='utf-8')
    _write_aggregate_cleanup(pathlib.Path(args.economic_cleanup_receipt),
                             receipt,
                             service_name='economic',
                             providers=['aws', 'gcp'])

    qualifier.aggregate_evidence(args)


def test_aggregate_rejects_arrivals_ahead_of_terminal_success_frontier(
        tmp_path):
    args = _aggregate_args(tmp_path, with_canary=False)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    scale['lb_unique_job_arrivals_60s'] = 801
    scale['lb_unique_job_arrivals_300s'] = 801
    scale['campaign_offered'] = 801
    scale['campaign_succeeded'] = 0
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='unattributed offered arrivals'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_impossible_provider_shape_sample(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    scale['provider_by_cloud']['aws']['shapes'] = []
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='contradictory provider shape'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_canary_above_one_physical_instance(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.canary_receipt[0])
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    gcp = scale['provider_by_cloud']['gcp']
    for field in ('instances', 'running', 'gpu_units', 'running_gpu_units',
                  'disks'):
        gcp[field] = 2
    shape = gcp['shapes'][0]
    for field in ('instance_count', 'running_count', 'running_gpu_units'):
        shape[field] = 2
    for field in ('provider_instances', 'provider_running',
                  'provider_gpu_units', 'provider_running_gpu_units',
                  'provider_disks'):
        scale[field] = 2
    payload['peak_running'] = 2
    payload['peak_running_gpu_units'] = 2
    payload['peak_running_by_cloud']['gcp'] = 2
    payload['peak_running_gpu_units_by_cloud']['gcp'] = 2
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError, match='totals or cap'):
        qualifier.aggregate_evidence(args)


@pytest.mark.parametrize('mutation',
                         ['diagnostic', 'timeout', 'nan', 'unbound'])
def test_aggregate_derives_scale_elapsed_from_bound_sample(tmp_path, mutation):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    scale = next(
        sample for sample in payload['samples'] if sample['phase'] == 'scale')
    if mutation == 'diagnostic':
        scale['observed_at'] = payload['scale_started_at'] + 301
        payload['scale_qualified_observed_at'] = scale['observed_at']
        payload['scale_slo_met'] = True
    elif mutation == 'timeout':
        scale['observed_at'] = (payload['scale_started_at'] +
                                payload['scale_timeout_seconds'] + 1)
        payload['scale_qualified_observed_at'] = scale['observed_at']
        payload['scale_slo_met'] = False
        request_scale = next(
            sample for sample in payload['request_telemetry_samples']
            if sample['phase'] == 'scale')
        request_scale['observed_at'] = scale['observed_at'] - 1
        final = next(sample for sample in payload['request_telemetry_samples']
                     if sample['phase'] == 'final')
        final['observed_at'] = scale['observed_at'] + 1
    elif mutation == 'nan':
        payload['scale_started_at'] = float('nan')
    else:
        payload['scale_qualified_observed_at'] += 1
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(
            qualifier.QualificationError,
            match='timestamp|provider scale evidence|typed evidence'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_nonzero_provider_baseline_before_zero(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    first = payload['samples'][0]
    first.update({
        'exact_zero': False,
        'provider_instances': 1,
        'provider_running': 1,
        'provider_gpu_units': 1,
        'provider_running_gpu_units': 1,
        'provider_disks': 1,
        'provider_by_cloud': _qualification_provider_projection({'aws': 1}),
    })
    payload['samples'].insert(3, _zero_qualification_sample(4,
                                                            phase='baseline'))
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='nonzero provider baseline'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_nonzero_telemetry_baseline_before_zero(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    baseline = payload['request_telemetry_samples'][0]
    baseline['queue_depth'] = 1
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='nonzero.*request baseline'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_noncanonical_ledger_state_projection(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    del payload['request_telemetry_samples'][0]['ledger_state_counts'][0]
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='telemetry evidence is malformed'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_canary_from_a_different_task_source(tmp_path):
    args = _aggregate_args(tmp_path)
    canary = pathlib.Path(args.canary_receipt[0])
    economic_sha256 = hashlib.sha256(
        pathlib.Path(args.economic_receipt).read_bytes()).hexdigest()
    _write_aggregate_qualification(
        canary,
        service_name='gcp-canary',
        kind='provider-canary',
        providers=['gcp'],
        peaks={'gcp': 1},
        source_sha256='f' * 64,
        authorized_economic_receipt_sha256=(economic_sha256))

    with pytest.raises(qualifier.QualificationError,
                       match='not projections of the economic task'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_canary_authorized_by_replaced_economic_receipt(
        tmp_path):
    args = _aggregate_args(tmp_path)
    economic = pathlib.Path(args.economic_receipt)
    payload = json.loads(economic.read_text(encoding='utf-8'))
    payload['service_hash'] = 'replacement-economic-hash'
    economic.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='not authorized by the exact economic receipt'):
        qualifier.aggregate_evidence(args)


def test_aggregate_rejects_replayed_natural_drain_sample(tmp_path):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    payload['samples'][-1]['observed_at'] = payload['samples'][-2][
        'observed_at']
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='provider timestamps.*strictly increasing'):
        qualifier.aggregate_evidence(args)


@pytest.mark.parametrize('mutation', ['running', 'shape', 'nonfinite'])
def test_aggregate_rejects_incomplete_natural_drain_projection(
        tmp_path, mutation):
    args = _aggregate_args(tmp_path)
    receipt = pathlib.Path(args.economic_receipt)
    payload = json.loads(receipt.read_text(encoding='utf-8'))
    sample = payload['samples'][-1]
    if mutation == 'running':
        sample['provider_running'] = 1
    elif mutation == 'shape':
        sample['provider_by_cloud']['aws']['shapes'] = [{
            'gpu_units_per_instance': 1,
            'instance_count': 1,
            'instance_type': 'impossible-zero-shape',
            'running_count': 0,
            'running_gpu_units': 0,
        }]
    else:
        sample['observed_at'] = float('nan')
    receipt.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError):
        qualifier.aggregate_evidence(args)


@pytest.mark.parametrize('mutation', ['identity', 'outcome'])
def test_aggregate_rejects_cleanup_without_matching_exact_zero(
        tmp_path, mutation):
    args = _aggregate_args(tmp_path)
    cleanup_path = pathlib.Path(args.economic_cleanup_receipt)
    payload = json.loads(cleanup_path.read_text(encoding='utf-8'))
    if mutation == 'identity':
        payload['service_hash'] = 'wrong-hash'
    else:
        payload['outcome'] = 'failed'
    cleanup_path.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='Cleanup receipt is malformed'):
        qualifier.aggregate_evidence(args)


@pytest.mark.parametrize('mutation', ['timestamp', 'counter', 'per_cloud'])
def test_aggregate_rejects_replayed_or_incomplete_cleanup_samples(
        tmp_path, mutation):
    args = _aggregate_args(tmp_path)
    cleanup_path = pathlib.Path(args.economic_cleanup_receipt)
    payload = json.loads(cleanup_path.read_text(encoding='utf-8'))
    if mutation == 'timestamp':
        payload['samples'][-1]['observed_at'] = payload['samples'][-2][
            'observed_at']
    elif mutation == 'counter':
        payload['samples'][-1]['zero_samples'] = payload['samples'][-2][
            'zero_samples']
    else:
        del payload['samples'][-1]['cleanup_provider_by_cloud']['aws']
    cleanup_path.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(qualifier.QualificationError,
                       match='sustained exact zero'):
        qualifier.aggregate_evidence(args)


@pytest.mark.parametrize('gpu_units', (1, 4))
def test_worker_exposes_exact_multi_gpu_capacity(gpu_units):
    config = yaml.safe_load(
        (_FIXTURE_DIR / 'service.yaml').read_text(encoding='utf-8'))
    with socket.socket() as port_socket:
        port_socket.bind(('127.0.0.1', 0))
        port = port_socket.getsockname()[1]
    process = subprocess.Popen(
        ['bash', '-c', config['run']],
        env={
            **os.environ,
            'PORT': str(port),
            'SKYPILOT_NUM_GPUS_PER_NODE': str(gpu_units),
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True)
    endpoint = f'http://127.0.0.1:{port}'

    def exact_request(index: int, duration: float) -> urllib.request.Request:
        request_id = f'exact-execution-{index}'
        body, intent = qualifier._canonical_exact_request(request_id, duration)
        return urllib.request.Request(
            f'{endpoint}/v1/models/model:predict',
            data=body,
            headers={
                'Content-Type': 'application/json',
                'X-SkyServe-Async-Ledger-Protocol': '1',
                'X-SkyServe-Service-Incarnation': 'incarnation-a',
                'X-SkyServe-Async-Intent-Sha256': intent,
                'X-SkyServe-Execution-Request-Id': request_id,
                'X-SkyServe-Async-Attempt-Id': str(uuid.UUID(int=index + 1)),
                'X-SkyServe-Async-Attempt-No': '1',
                'X-SkyServe-Async-Ledger-Revision': '1',
            },
            method='POST')

    capacity_request = urllib.request.Request(
        f'{endpoint}/v1/models/model:predict',
        data=json.dumps({
            'action': 'async_capacity'
        }).encode(),
        headers={'Content-Type': 'application/json'},
        method='POST')
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

        with urllib.request.urlopen(capacity_request, timeout=2) as response:
            capacity = json.load(response)
        assert capacity['running_count'] == 0
        assert capacity['predict_concurrency'] == gpu_units
        assert capacity['max_workers'] == gpu_units

        for index in range(gpu_units):
            with urllib.request.urlopen(exact_request(index, 0.75),
                                        timeout=2) as response:
                assert response.status == 202
                assert json.load(response) == {
                    'request_id': f'exact-execution-{index}',
                    'status': 'accepted',
                }
        with urllib.request.urlopen(capacity_request, timeout=2) as response:
            assert json.load(response)['running_count'] == gpu_units

        with pytest.raises(urllib.error.HTTPError) as rejected:
            urllib.request.urlopen(exact_request(gpu_units, 0.75), timeout=2)
        assert rejected.value.code == 429
        assert json.load(rejected.value) == {'error': 'worker capacity full'}

        time.sleep(0.85)
        with urllib.request.urlopen(capacity_request, timeout=2) as response:
            assert json.load(response)['running_count'] == 0
    finally:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
