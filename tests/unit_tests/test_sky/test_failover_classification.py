"""Tests for AWS capacity classification and cache scoping."""
# pylint: disable=protected-access
import contextlib
import importlib
import inspect
import os
import pathlib
import pickle
import types
import unittest.mock as mock
import uuid

import botocore.exceptions
import pytest

from sky import clouds
from sky import exceptions
from sky import resources as resources_lib
from sky.backends import cloud_vm_ray_backend as backend
from sky.catalog import aws_catalog
from sky.provision import capacity_cache
from sky.provision import capacity_policy
from sky.provision import common as provision_common
from sky.provision import failover_error_policy
from sky.provision.aws import instance as aws_instance
from sky.provision.gcp import instance as gcp_instance
from sky.provision.gcp import instance_utils as gcp_instance_utils
from sky.serve import ordinary_launch_binding
from sky.serve import provider_phase
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.server.requests import ordinary_launch as ordinary_launch_request
from sky.server.requests import storage as request_storage

_CAPACITY_POLICY_SIGNATURES = {
    '_iter_error_chain': '(error)',
    '_provider_error_codes': '(error)',
    '_classify_capacity_error': '(cloud, error)',
    '_terminal_failover_leaves': '(error)',
    '_terminal_leaf_cause_nodes': '(failure, *, history_depth, remaining_nodes)',
    'classify_resources_unavailable_error': '(cloud, error)',
    '_is_quota_error': '(error)',
    '_canonical_accelerators': '(to_provision)',
    '_capacity_cache_cloud_name': '(to_provision)',
    '_capacity_cache_account': '(cloud, cloud_user_identity)',
    '_capacity_cache_key': '(to_provision, region, zones, num_nodes, account)',
    '_quota_cooldown_key': '(to_provision, region, num_nodes, account)',
    '_fully_created_fresh_demand': '(provision_record, num_nodes, cluster_exists)',
    '_failure_requested_full_demand': '(error, num_nodes)',
    '_placement_error_code': '(error)',
    '_placement_outcome': '(error, capacity_reason=None)',
}

_FAILOVER_ERROR_POLICY_SIGNATURES = {
    '_add_to_blocked_resources': '(blocked_resources, resources)',
    'FailoverCloudErrorHandlerV1._handle_errors': '(stdout, stderr, is_error_str_known)',
    'FailoverCloudErrorHandlerV1._ibm_handler': '(blocked_resources, launchable_resources, region, zones, stdout, stderr)',
    'FailoverCloudErrorHandlerV1.update_blocklist_on_error': '(blocked_resources, launchable_resources, region, zones, stdout, stderr)',
    'FailoverCloudErrorHandlerV2._azure_handler': '(blocked_resources, launchable_resources, region, zones, err)',
    'FailoverCloudErrorHandlerV2._gcp_handler': '(blocked_resources, launchable_resources, region, zones, err)',
    'FailoverCloudErrorHandlerV2._lambda_handler': '(blocked_resources, launchable_resources, region, zones, error)',
    'FailoverCloudErrorHandlerV2._aws_handler': '(blocked_resources, launchable_resources, region, zones, error)',
    'FailoverCloudErrorHandlerV2._scp_handler': '(blocked_resources, launchable_resources, region, zones, error)',
    'FailoverCloudErrorHandlerV2._default_handler': '(blocked_resources, launchable_resources, region, zones, error)',
    'FailoverCloudErrorHandlerV2.update_blocklist_on_error': '(blocked_resources, launchable_resources, region, zones, error)',
}


@pytest.fixture(autouse=True)
def _use_static_aws_catalog(monkeypatch):
    """Keep capacity-policy unit tests independent of AWS credentials."""
    monkeypatch.setattr(aws_catalog, '_user_df', aws_catalog._default_df)


def _call_signature(symbol) -> str:
    """Returns the version-stable callable portion of a signature."""
    signature = inspect.signature(symbol)
    parameters = [
        parameter.replace(annotation=inspect.Parameter.empty)
        for parameter in signature.parameters.values()
    ]
    return str(
        signature.replace(parameters=parameters,
                          return_annotation=inspect.Signature.empty))


def test_capacity_policy_historical_contract():
    for name, signature in _CAPACITY_POLICY_SIGNATURES.items():
        symbol = getattr(backend, name)
        assert getattr(capacity_policy, name) is symbol
        assert _call_signature(symbol) == signature
        assert symbol.__module__ == 'sky.backends.cloud_vm_ray_backend'
        assert pickle.loads(pickle.dumps(symbol)) is symbol


def _resolve_backend_symbol(path: str):
    symbol = backend
    for name in path.split('.'):
        symbol = getattr(symbol, name)
    return symbol


def _resolve_failover_policy_symbol(path: str):
    symbol = failover_error_policy
    for name in path.split('.'):
        symbol = getattr(symbol, name)
    return symbol


def test_failover_error_policy_historical_contract():
    assert (backend._RSYNC_NOT_FOUND_MESSAGE
            is failover_error_policy._RSYNC_NOT_FOUND_MESSAGE)
    for handler_name in ('FailoverCloudErrorHandlerV1',
                         'FailoverCloudErrorHandlerV2'):
        handler = getattr(backend, handler_name)
        assert getattr(failover_error_policy, handler_name) is handler
        assert handler.__module__ == 'sky.backends.cloud_vm_ray_backend'
        assert handler.__qualname__ == handler_name
        assert pickle.loads(pickle.dumps(handler)) is handler

    for path, signature in _FAILOVER_ERROR_POLICY_SIGNATURES.items():
        symbol = _resolve_backend_symbol(path)
        assert _resolve_failover_policy_symbol(path) is symbol
        assert _call_signature(symbol) == signature
        assert symbol.__module__ == 'sky.backends.cloud_vm_ray_backend'
        assert symbol.__qualname__ == path
        assert pickle.loads(pickle.dumps(symbol)) is symbol


class _FakeClientError(Exception):

    def __init__(self, code: str):
        self.response = {'Error': {'Code': code, 'Message': code}}
        super().__init__(f'An error occurred ({code})')


def _aggregate_error(*codes: str) -> provision_common.ProvisionerError:
    error = provision_common.ProvisionerError('Failed to launch instances.')
    error.errors = [{
        'code': code,
        'message': code,
    } for code in codes]
    if codes:
        error.__cause__ = _FakeClientError(codes[-1])
    return error


def _gcp_launchable_resources() -> resources_lib.Resources:
    return resources_lib.Resources(
        cloud=clouds.GCP(),
        instance_type='n1-standard-4',
        region='us-central1',
        zone='us-central1-a',
    )


def test_failover_blocklist_skips_already_covered_resources():
    launchable = _gcp_launchable_resources()
    blocked = {launchable.copy(region=None, zone=None)}

    backend._add_to_blocked_resources(blocked, launchable)
    assert len(blocked) == 1
    blocked_resource = next(iter(blocked))
    assert blocked_resource.region is None
    assert blocked_resource.zone is None

    blocked.clear()
    backend._add_to_blocked_resources(blocked, launchable)
    backend._add_to_blocked_resources(blocked, launchable)
    assert len(blocked) == 1
    blocked_resource = next(iter(blocked))
    assert blocked_resource.region == 'us-central1'
    assert blocked_resource.zone == 'us-central1-a'


def test_failover_v1_known_errors_preserve_order_and_stripping():
    errors = backend.FailoverCloudErrorHandlerV1._handle_errors(
        '  ERR first  \nignored',
        'PANIC second\nignored',
        lambda line: line.startswith(('ERR', 'PANIC')),
    )
    assert errors == ['ERR first', 'PANIC second']


def test_failover_v1_rsync_error_preserves_detailed_reason():
    with pytest.raises(RuntimeError,
                       match='`rsync` command is not found') as exc_info:
        backend.FailoverCloudErrorHandlerV1._handle_errors(
            'launch output',
            'bash: rsync: command not found',
            lambda _: False,
        )

    assert exc_info.value.detailed_reason == (
        'stdout: launch output\n'
        'stderr: bash: rsync: command not found')


def test_failover_v1_gang_failure_blocks_each_zone():
    launchable = _gcp_launchable_resources()
    blocked: set[resources_lib.Resources] = set()
    zones = [clouds.Zone('us-central1-a'), clouds.Zone('us-central1-b')]

    definitely_no_nodes_launched = (
        backend.FailoverCloudErrorHandlerV1.update_blocklist_on_error(
            blocked,
            launchable,
            clouds.Region('us-central1'),
            zones,
            None,
            None,
        ))

    assert not definitely_no_nodes_launched
    assert {resource.zone for resource in blocked} == {
        'us-central1-a',
        'us-central1-b',
    }


@pytest.mark.parametrize(
    ('code', 'message', 'expected_region', 'expected_zone'),
    [
        ('ZONE_RESOURCE_POOL_EXHAUSTED', 'capacity', 'us-central1',
         'us-central1-a'),
        ('QUOTA_EXCEEDED', 'regional quota', 'us-central1', None),
        ('QUOTA_EXCEEDED', "'GPUS_ALL_REGIONS' exceeded", None, None),
        ('IAM_PERMISSION_DENIED', 'permission', None, None),
    ],
)
def test_failover_v2_gcp_error_block_width(code, message, expected_region,
                                           expected_zone):
    launchable = _gcp_launchable_resources()
    blocked: set[resources_lib.Resources] = set()
    error = provision_common.ProvisionerError('provision failed')
    error.errors = [{'code': code, 'message': message}]

    backend.FailoverCloudErrorHandlerV2._gcp_handler(
        blocked,
        launchable,
        clouds.Region('us-central1'),
        [clouds.Zone('us-central1-a')],
        error,
    )

    assert len(blocked) == 1
    blocked_resource = next(iter(blocked))
    assert blocked_resource.region == expected_region
    assert blocked_resource.zone == expected_zone


def test_failover_v2_default_handler_blocks_each_zone():
    launchable = _gcp_launchable_resources()
    blocked: set[resources_lib.Resources] = set()

    backend.FailoverCloudErrorHandlerV2._default_handler(
        blocked,
        launchable,
        clouds.Region('us-central1'),
        [clouds.Zone('us-central1-a'),
         clouds.Zone('us-central1-b')],
        RuntimeError('unparsed'),
    )

    assert {resource.zone for resource in blocked} == {
        'us-central1-a',
        'us-central1-b',
    }


@pytest.mark.parametrize('code', [
    'VcpuLimitExceeded',
    'MaxSpotInstanceCountExceeded',
    'InstanceLimitExceeded',
])
def test_classify_quota(code):
    assert backend._classify_capacity_error(clouds.AWS(),
                                            _FakeClientError(code)) == 'quota'


def test_classify_capacity_through_explicit_cause():
    cause = _FakeClientError('InsufficientInstanceCapacity')
    outer = RuntimeError('Failed to launch instances.')
    outer.__cause__ = cause
    assert backend._classify_capacity_error(clouds.AWS(), outer) == 'capacity'


def test_classify_aggregate_requires_only_known_codes():
    assert backend._classify_capacity_error(
        clouds.AWS(),
        _aggregate_error('InsufficientInstanceCapacity',
                         'InsufficientInstanceCapacity')) == 'capacity'
    assert backend._classify_capacity_error(
        clouds.AWS(),
        _aggregate_error('VcpuLimitExceeded',
                         'MaxSpotInstanceCountExceeded')) == 'quota'
    assert backend._classify_capacity_error(
        clouds.AWS(),
        _aggregate_error('InvalidParameterValue',
                         'InsufficientInstanceCapacity')) is None


def test_classify_known_mixed_capacity_and_quota_as_quota():
    assert backend._classify_capacity_error(
        clouds.AWS(),
        _aggregate_error('InsufficientInstanceCapacity',
                         'VcpuLimitExceeded')) == 'quota'


def test_terminal_resources_unavailable_requires_all_structured_evidence():
    capacity = exceptions.ResourcesUnavailableError(
        'capacity',
        failover_history=[
            _aggregate_error('InsufficientInstanceCapacity'),
            _aggregate_error('InsufficientInstanceCapacity'),
        ])
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        capacity) == 'capacity'

    quota = exceptions.ResourcesUnavailableError(
        'quota',
        failover_history=[
            _aggregate_error('InsufficientInstanceCapacity'),
            _aggregate_error('MaxSpotInstanceCountExceeded'),
        ])
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        quota) == 'quota'

    mixed = exceptions.ResourcesUnavailableError(
        'not availability',
        failover_history=[
            _aggregate_error('InsufficientInstanceCapacity'),
            _aggregate_error('RequestLimitExceeded'),
        ])
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        mixed) is None


def _nested_terminal_error(
        *failures: Exception) -> exceptions.ResourcesUnavailableError:
    per_location = exceptions.ResourcesUnavailableError(
        'location unavailable', failover_history=list(failures))
    return exceptions.ResourcesUnavailableError('optimizer exhausted',
                                                failover_history=[per_location])


@pytest.mark.parametrize(
    ('cloud', 'capacity_code', 'quota_code'),
    [
        (clouds.AWS(), 'InsufficientInstanceCapacity',
         'MaxSpotInstanceCountExceeded'),
        (clouds.GCP(), 'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
         'QUOTA_EXCEEDED'),
    ],
)
def test_terminal_resources_unavailable_recurses_nested_histories(
        cloud, capacity_code, quota_code):
    capacity = _aggregate_error(capacity_code)
    assert backend.classify_resources_unavailable_error(
        cloud, _nested_terminal_error(capacity)) == 'capacity'

    quota = _aggregate_error(quota_code)
    assert backend.classify_resources_unavailable_error(
        cloud, _nested_terminal_error(capacity, quota)) == 'quota'


def test_terminal_resources_unavailable_nested_history_is_conservative(
        monkeypatch):
    capacity = _aggregate_error('InsufficientInstanceCapacity')
    unknown = _aggregate_error('RequestLimitExceeded')
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), _nested_terminal_error(capacity, unknown)) is None

    malformed = _nested_terminal_error(capacity)
    malformed.failover_history.append(
        'not an exception')  # type: ignore[arg-type]
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        malformed) is None

    malformed_container = _nested_terminal_error(capacity)
    malformed_container.failover_history = ()  # type: ignore[assignment]
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), malformed_container) is None

    class _HiddenHistory(list):

        def __len__(self):
            return 0

    hidden_history = exceptions.ResourcesUnavailableError('hidden history')
    hidden_history.failover_history = _HiddenHistory(
        [unknown])  # type: ignore[assignment]
    hidden_history.__cause__ = capacity
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        hidden_history) is None

    cause_leaf = RuntimeError('leaf')
    malformed_cause = exceptions.ResourcesUnavailableError('malformed cause')
    malformed_cause.failover_history = ()  # type: ignore[assignment]
    cause_leaf.__cause__ = malformed_cause
    malformed_cause.__cause__ = capacity
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), _nested_terminal_error(cause_leaf)) is None

    mixed_cause_leaf = RuntimeError('leaf')
    mixed_cause_leaf.__cause__ = exceptions.ResourcesUnavailableError(
        'history-bearing cause', failover_history=[capacity])
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), _nested_terminal_error(mixed_cause_leaf)) is None

    history_cycle = exceptions.ResourcesUnavailableError('cycle')
    history_cycle.failover_history.append(history_cycle)
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        history_cycle) is None

    cause_cycle = _FakeClientError('InsufficientInstanceCapacity')
    cause_wrapper = RuntimeError('cause wrapper')
    cause_cycle.__cause__ = cause_wrapper
    cause_wrapper.__cause__ = cause_cycle
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), _nested_terminal_error(cause_cycle)) is None

    too_deep: Exception = capacity
    for _ in range(backend._MAX_TERMINAL_FAILOVER_HISTORY_DEPTH + 1):
        too_deep = exceptions.ResourcesUnavailableError(
            'nested', failover_history=[too_deep])
    assert isinstance(too_deep, exceptions.ResourcesUnavailableError)
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        too_deep) is None

    cause_too_deep: Exception = capacity
    for _ in range(backend._MAX_TERMINAL_FAILOVER_HISTORY_DEPTH + 1):
        cause_wrapper = RuntimeError('nested cause')
        cause_wrapper.__cause__ = cause_too_deep
        cause_too_deep = cause_wrapper
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), _nested_terminal_error(cause_too_deep)) is None

    too_wide = exceptions.ResourcesUnavailableError(
        'wide',
        failover_history=[capacity] *
        (backend._MAX_TERMINAL_FAILOVER_HISTORY_NODES + 1))
    with monkeypatch.context() as patch:
        patch.setitem(
            backend._terminal_failover_leaves.__globals__,
            'reversed',
            mock.Mock(side_effect=AssertionError(
                'oversized history must not be scanned')),
        )
        assert backend.classify_resources_unavailable_error(
            clouds.AWS(), too_wide) is None

    cause_budget_overflow = exceptions.ResourcesUnavailableError(
        'wide',
        failover_history=[
            _aggregate_error('InsufficientInstanceCapacity')
            for _ in range(backend._MAX_TERMINAL_FAILOVER_HISTORY_NODES // 2)
        ],
    )
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), cause_budget_overflow) is None


def test_terminal_resources_unavailable_wrapper_history_is_authoritative():
    capacity = _aggregate_error('InsufficientInstanceCapacity')
    leaf_wrapper = exceptions.ResourcesUnavailableError('leaf')
    leaf_wrapper.__cause__ = capacity
    inner = exceptions.ResourcesUnavailableError(
        'inner', failover_history=[leaf_wrapper])
    outer = exceptions.ResourcesUnavailableError('outer',
                                                 failover_history=[inner])
    outer.__cause__ = _aggregate_error('RequestLimitExceeded')
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        outer) == 'capacity'


def test_terminal_resources_unavailable_allows_shared_leaf():
    capacity = _aggregate_error('InsufficientInstanceCapacity')
    left = exceptions.ResourcesUnavailableError('left',
                                                failover_history=[capacity])
    right = exceptions.ResourcesUnavailableError('right',
                                                 failover_history=[capacity])
    outer = exceptions.ResourcesUnavailableError('outer',
                                                 failover_history=[left, right])
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        outer) == 'capacity'


def test_terminal_resources_unavailable_does_not_parse_error_text():
    error = exceptions.ResourcesUnavailableError(
        'InsufficientInstanceCapacity',
        failover_history=[
            RuntimeError('InsufficientInstanceCapacity'),
            AssertionError('security group already exists'),
        ])
    assert backend.classify_resources_unavailable_error(clouds.AWS(),
                                                        error) is None


def test_shared_hint_requires_full_demand_request_metadata():
    error = _aggregate_error('InsufficientInstanceCapacity')
    assert not backend._failure_requested_full_demand(error, 4)

    error.requested_count = 4
    outer = RuntimeError('wrapped')
    outer.__cause__ = error
    assert backend._failure_requested_full_demand(outer, 4)
    assert not backend._failure_requested_full_demand(outer, 1)


def test_aws_create_instances_preserves_every_failure(monkeypatch):
    monkeypatch.setattr(aws_instance, 'BOTO_CREATE_MAX_RETRIES', 2)

    class _FakeEC2:

        def create_instances(self, **kwargs):
            subnet_id = kwargs['NetworkInterfaces'][0]['SubnetId']
            code = ('InsufficientInstanceCapacity'
                    if subnet_id == 'subnet-a' else 'InvalidParameterValue')
            raise botocore.exceptions.ClientError(
                {'Error': {
                    'Code': code,
                    'Message': code,
                }}, 'RunInstances')

    with pytest.raises(provision_common.ProvisionerError) as exc_info:
        aws_instance._create_instances(
            _FakeEC2(), 'cluster', {
                'SubnetIds': ['subnet-a', 'subnet-b'],
                'SecurityGroupIds': ['sg-1'],
                'InstanceType': 'g6.4xlarge',
            }, {}, 1, True, 0)

    assert [error['code'] for error in exc_info.value.errors
           ] == ['InsufficientInstanceCapacity', 'InvalidParameterValue']
    assert [error['subnet_id'] for error in exc_info.value.errors
           ] == ['subnet-a', 'subnet-b']
    assert exc_info.value.requested_count == 1


def test_classify_is_structured_and_aws_only():
    text_only = RuntimeError('InsufficientInstanceCapacity')
    assert backend._classify_capacity_error(clouds.AWS(), text_only) is None
    assert backend._classify_capacity_error(
        clouds.GCP(), _FakeClientError('VcpuLimitExceeded')) is None


def test_placement_classification_tolerates_malformed_provider_response():

    class _MalformedResponseError(Exception):

        def __init__(self):
            self.response = {'Error': 'not-an-object'}
            super().__init__('provider request failed')

    error = _MalformedResponseError()
    assert backend._placement_error_code(error) is None
    assert backend._placement_outcome(error) == 'failed'


def test_placement_outcome_reads_gcp_capacity_code_behind_summary_code():
    # GCP bulk insert reports the uninformative summary code first, so a
    # first-code-only check mislabels real exhaustion as a generic failure.
    error = _aggregate_error('VM_MIN_COUNT_NOT_REACHED',
                             'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS')
    assert backend._placement_outcome(error) == 'capacity_failed'


def test_placement_outcome_prefers_quota_over_capacity_in_mixed_batch():
    error = _aggregate_error('VM_MIN_COUNT_NOT_REACHED',
                             'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
                             'QUOTA_EXCEEDED')
    assert backend._placement_outcome(error) == 'quota_failed'


def test_placement_outcome_ignores_non_capacity_codes():
    # UNSUPPORTED_OPERATION blocks the zone on failover but means preemption
    # during creation, not an exhausted pool.
    assert backend._placement_outcome(
        _aggregate_error('VM_MIN_COUNT_NOT_REACHED',
                         'UNSUPPORTED_OPERATION')) == 'failed'


def test_placement_outcome_keeps_heterogeneous_aggregate_conservative():
    # AWS retries every subnet and appends one entry per distinct failure, so
    # a batch mixing capacity with an unrelated error is not exhaustion.
    assert backend._placement_outcome(
        _aggregate_error('InvalidParameterValue',
                         'InsufficientInstanceCapacity')) == 'failed'
    assert backend._placement_outcome(
        _aggregate_error('InsufficientInstanceCapacity',
                         'InsufficientInstanceCapacity')) == 'capacity_failed'


def test_placement_outcome_ignores_summary_only_batch():
    assert backend._placement_outcome(
        _aggregate_error('VM_MIN_COUNT_NOT_REACHED')) == 'failed'


def test_placement_outcome_separates_tpu_quota_from_tpu_capacity():
    # sky/provision/gcp/tpu_node.py raises RESOURCE_EXHAUSTED for quota
    # exhaustion and CapacityExceeded for an exhausted zone.
    assert backend._placement_outcome(
        _aggregate_error('RESOURCE_EXHAUSTED')) == 'quota_failed'
    assert backend._placement_outcome(
        _aggregate_error('CapacityExceeded')) == 'capacity_failed'


def test_capacity_codes_do_not_cross_providers():
    """One cloud's capacity code must never classify another cloud's failure."""
    gcp_code = _aggregate_error('ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS')
    aws_code = _aggregate_error('InsufficientInstanceCapacity')
    assert backend._classify_capacity_error(clouds.AWS(), gcp_code) is None
    assert backend._classify_capacity_error(clouds.GCP(), aws_code) is None
    # Each still classifies its own.
    assert backend._classify_capacity_error(clouds.GCP(),
                                            gcp_code) == 'capacity'
    assert backend._classify_capacity_error(clouds.AWS(),
                                            aws_code) == 'capacity'
    # GCP's uninformative summary code is not neutral for AWS.
    assert backend._classify_capacity_error(
        clouds.AWS(),
        _aggregate_error('InsufficientInstanceCapacity',
                         'VM_MIN_COUNT_NOT_REACHED')) is None


def test_gcp_classification_reads_past_the_summary_code():
    assert backend._classify_capacity_error(
        clouds.GCP(),
        _aggregate_error(
            'VM_MIN_COUNT_NOT_REACHED',
            'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS')) == 'capacity'
    # Quota dominates a mixed known batch because it is regional.
    assert backend._classify_capacity_error(
        clouds.GCP(),
        _aggregate_error('VM_MIN_COUNT_NOT_REACHED',
                         'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
                         'QUOTA_EXCEEDED')) == 'quota'
    # An unknown code keeps the conservative failover path.
    assert backend._classify_capacity_error(
        clouds.GCP(),
        _aggregate_error('VM_MIN_COUNT_NOT_REACHED', 'SOMETHING_ELSE')) is None
    # A summary code alone says nothing.
    assert backend._classify_capacity_error(
        clouds.GCP(), _aggregate_error('VM_MIN_COUNT_NOT_REACHED')) is None


@pytest.mark.parametrize('code', [
    'VcpuLimitExceeded',
    'quotaExceeded',
    'QUOTA_EXCEEDED',
    'type.googleapis.com/google.rpc.QuotaFailure',
])
def test_operator_quota_detection_uses_structured_codes(code):
    assert backend._is_quota_error(_aggregate_error(code))


def test_operator_quota_detection_ignores_capacity_and_text_only_errors():
    assert not backend._is_quota_error(
        _aggregate_error('InsufficientInstanceCapacity'))
    assert not backend._is_quota_error(RuntimeError('quotaExceeded'))


def test_classify_ignores_implicit_context():
    capacity = _FakeClientError('InsufficientInstanceCapacity')
    try:
        try:
            raise capacity
        except Exception:  # pylint: disable=broad-except
            raise ValueError(  # pylint: disable=raise-missing-from
                'unrelated failure')
    except ValueError as unrelated:
        assert unrelated.__context__ is capacity
        assert unrelated.__cause__ is None
        assert backend._classify_capacity_error(clouds.AWS(), unrelated) is None


def _to_provision(*, use_spot: bool = True, cloud=None):
    return resources_lib.Resources(cloud=cloud or clouds.AWS(),
                                   region='us-east-1',
                                   zone='us-east-1a',
                                   instance_type='g6.4xlarge',
                                   accelerators={'L4': 1},
                                   use_spot=use_spot)


def test_cache_key_is_exact_aws_spot_zone_only():
    region = clouds.Region('us-east-1')
    zone_a = clouds.Zone('us-east-1a')
    zone_b = clouds.Zone('us-east-1b')
    key = backend._capacity_cache_key(_to_provision(), region, [zone_a], 4,
                                      'acct')
    assert key == capacity_cache.ResourceKey(cloud='aws',
                                             account='acct',
                                             region='us-east-1',
                                             zone='us-east-1a',
                                             instance_type='g6.4xlarge',
                                             accelerators='L4:1',
                                             num_nodes=4)

    assert backend._capacity_cache_key(_to_provision(use_spot=False), region,
                                       [zone_a], 4, 'acct') is None
    assert backend._capacity_cache_key(_to_provision(), region,
                                       [zone_a, zone_b], 4, 'acct') is None
    assert backend._capacity_cache_key(_to_provision(), region, [zone_a], 4,
                                       None) is None
    # A cloud with no structured capacity codes never participates.
    assert backend._capacity_cache_key(_to_provision(cloud=clouds.Azure()),
                                       region, [zone_a], 4, 'acct') is None


def test_quota_cooldown_key_is_exact_spot_regional_demand():
    region = clouds.Region('us-east-1')
    key = backend._quota_cooldown_key(_to_provision(), region, 4, 'acct')
    assert key == capacity_cache.QuotaCooldownKey(cloud='aws',
                                                  account='acct',
                                                  region='us-east-1',
                                                  instance_type='g6.4xlarge',
                                                  accelerators='L4:1',
                                                  num_nodes=4)
    assert backend._quota_cooldown_key(_to_provision(), region, 4, None) is None
    assert backend._quota_cooldown_key(_to_provision(use_spot=False), region, 4,
                                       'acct') is None
    assert backend._quota_cooldown_key(_to_provision(cloud=clouds.Azure()),
                                       region, 4, 'acct') is None


def test_consult_returns_only_active_zone(monkeypatch):
    to_provision = _to_provision()
    region = clouds.Region('us-east-1')
    zones = [clouds.Zone('us-east-1a')]
    expected = backend._capacity_cache_key(to_provision, region, zones, 1,
                                           'acct')
    assert expected is not None
    monkeypatch.setattr(capacity_cache, 'active_exhausted_keys',
                        lambda keys: {expected})
    assert backend._capacity_cache_exhausted_zone_names(
        to_provision, region, zones, 1, 'acct') == {'us-east-1a'}


def test_consult_failure_falls_back_to_real_probe(monkeypatch):
    monkeypatch.setattr(
        capacity_cache, 'active_exhausted_keys', lambda keys:
        (_ for _ in ()).throw(RuntimeError('db unavailable')))
    assert backend._capacity_cache_exhausted_zone_names(
        _to_provision(), clouds.Region('us-east-1'),
        [clouds.Zone('us-east-1a')], 1, 'acct') == set()


def test_quota_consult_hit_and_failure(monkeypatch):
    key = capacity_cache.QuotaCooldownKey(cloud='aws',
                                          account='acct',
                                          region='us-east-1',
                                          instance_type='g6.4xlarge',
                                          accelerators='L4:1',
                                          num_nodes=1)
    monkeypatch.setattr(capacity_cache, 'is_quota_cooldown_active',
                        lambda _: True)
    assert backend._quota_cooldown_is_active(key)

    monkeypatch.setattr(
        capacity_cache, 'is_quota_cooldown_active', lambda _:
        (_ for _ in ()).throw(RuntimeError('db unavailable')))
    assert not backend._quota_cooldown_is_active(key)


def _call_retry_zones(provisioner,
                      to_provision,
                      *,
                      num_nodes=1,
                      dryrun=False,
                      skip_if_config_hash_matches=None):
    return provisioner._retry_zones(
        to_provision=to_provision,
        num_nodes=num_nodes,
        requested_resources={to_provision},
        dryrun=dryrun,
        stream_logs=False,
        cluster_name='test-cluster',
        cloud_user_identity=['arn:aws:iam::123456789012:role/test', 'acct'],
        prev_cluster_status=None,
        prev_handle=None,
        prev_cluster_ever_up=False,
        skip_if_config_hash_matches=skip_if_config_hash_matches,
        volume_mounts=None,
        task=None,
    )


def _early_retry_provisioner(tmp_path, monkeypatch):
    provisioner = object.__new__(backend.RetryingVmProvisioner)
    provisioner.log_dir = str(tmp_path)
    provisioner._blocked_resources = set()
    provisioner._extra_launch_context = {}
    monkeypatch.setattr(backend.os, 'system', lambda _: 0)
    monkeypatch.setattr(backend.rich_utils, 'force_update_status',
                        lambda _: None)
    return provisioner


def test_retry_zones_spot_quota_cooldown_precedes_quota_check_and_zone_yield(
        tmp_path, monkeypatch):
    provisioner = _early_retry_provisioner(tmp_path, monkeypatch)
    to_provision = _to_provision()
    expected_key = capacity_cache.QuotaCooldownKey(cloud='aws',
                                                   account='acct',
                                                   region='us-east-1',
                                                   instance_type='g6.4xlarge',
                                                   accelerators='L4:1',
                                                   num_nodes=1)
    cooldown_active = mock.Mock(return_value=True)
    check_quota = mock.Mock(return_value=False)
    yield_zones = mock.Mock(
        side_effect=AssertionError('zone iteration must be skipped'))
    monkeypatch.setattr(capacity_cache, 'is_quota_cooldown_active',
                        cooldown_active)
    monkeypatch.setattr(backend, '_record_capacity_metric', lambda *_: None)
    monkeypatch.setattr(clouds.AWS, 'check_quota_available', check_quota)
    monkeypatch.setattr(provisioner, '_yield_zones', yield_zones)

    with pytest.raises(exceptions.ResourcesUnavailableError,
                       match='quota-failure cooldown'):
        _call_retry_zones(provisioner, to_provision)

    cooldown_active.assert_called_once_with(expected_key)
    check_quota.assert_not_called()
    yield_zones.assert_not_called()
    assert len(provisioner._blocked_resources) == 1
    blocked = next(iter(provisioner._blocked_resources))
    assert blocked.region == 'us-east-1'
    assert blocked.zone is None


def test_retry_zones_quota_cooldown_does_not_apply_to_on_demand(
        tmp_path, monkeypatch):
    provisioner = _early_retry_provisioner(tmp_path, monkeypatch)
    to_provision = _to_provision(use_spot=False)
    cooldown_active = mock.Mock(return_value=True)
    check_quota = mock.Mock(return_value=False)
    yield_zones = mock.Mock(
        side_effect=AssertionError('zero quota must skip zone iteration'))
    notify = mock.Mock(return_value=False)
    monkeypatch.setattr(capacity_cache, 'is_quota_cooldown_active',
                        cooldown_active)
    monkeypatch.setattr(clouds.AWS, 'check_quota_available', check_quota)
    monkeypatch.setattr(provisioner, '_yield_zones', yield_zones)
    monkeypatch.setattr(backend, '_record_insufficient_quota_notification',
                        notify)

    with pytest.raises(exceptions.ResourcesUnavailableError,
                       match='Found no quota'):
        _call_retry_zones(provisioner, to_provision)

    cooldown_active.assert_not_called()
    check_quota.assert_called_once()
    yield_zones.assert_not_called()
    notify.assert_called_once_with(to_provision)
    assert not provisioner._blocked_resources


def test_retry_zones_preserves_structured_provider_failure(
        tmp_path, monkeypatch):
    provisioner = _early_retry_provisioner(tmp_path, monkeypatch)
    provisioner._local_wheel_path = None
    provisioner._wheel_hash = None
    provisioner._active_cluster_hash = None
    provisioner._is_managed = False
    provisioner._workload_type = 'service'
    provisioner._extra_launch_context = {}
    to_provision = _to_provision()
    provider_error = _aggregate_error('InsufficientInstanceCapacity')
    provider_error.requested_count = 1

    monkeypatch.setattr(clouds.AWS, 'check_quota_available', lambda *_: True)
    monkeypatch.setattr(provisioner, '_yield_zones',
                        lambda *_: iter([[clouds.Zone('us-east-1a')]]))
    monkeypatch.setattr(backend, '_capacity_cache_exhausted_zone_names',
                        lambda *_: set())
    monkeypatch.setattr(backend, '_get_image_demand_attribution',
                        lambda *_: mock.MagicMock())
    monkeypatch.setattr(backend, '_resolve_container_image_for_placement',
                        lambda resources, **_: resources)
    monkeypatch.setattr(backend, '_get_cluster_config_template',
                        lambda *_: '/tmp/template')
    monkeypatch.setattr(
        backend.backend_utils, 'write_cluster_config', lambda *_, **__: {
            'ray': '/tmp/cluster.yaml',
            'cluster_name_on_cloud': 'test-cluster',
        })
    monkeypatch.setattr(backend, '_get_workload_attribution', lambda *_:
                        (None, None))
    monkeypatch.setattr(backend.global_user_state, 'add_or_update_cluster',
                        lambda *_, **__: 'cluster-hash')
    monkeypatch.setattr(backend.global_user_state, 'add_cluster_event',
                        lambda *_, **__: None)
    monkeypatch.setattr(backend.global_user_state,
                        'set_owner_identity_for_cluster', lambda *_, **__: None)
    monkeypatch.setattr(backend.usage_lib.messages.usage,
                        'update_final_cluster_status', lambda *_: None)
    monkeypatch.setattr(backend.controller_utils.Controllers, 'from_name',
                        lambda *_: None)
    monkeypatch.setattr(backend.provisioner, 'bulk_provision',
                        mock.Mock(side_effect=provider_error))
    monkeypatch.setattr(backend.CloudVmRayBackend, 'post_teardown_cleanup',
                        lambda *_, **__: None)
    monkeypatch.setattr(backend.FailoverCloudErrorHandlerV2,
                        'update_blocklist_on_error', lambda *_, **__: None)
    monkeypatch.setattr(backend, '_record_service_placement_event',
                        lambda *_, **__: None)
    monkeypatch.setattr(backend.capacity_cache, 'mark_exhausted',
                        lambda *_, **__: None)

    with pytest.raises(exceptions.ResourcesUnavailableError) as exc_info:
        _call_retry_zones(provisioner, to_provision)

    assert exc_info.value.failover_history == [provider_error]
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), exc_info.value) == 'capacity'


def test_retry_zones_passes_template_override_to_config_writer(
        tmp_path, monkeypatch):
    provisioner = _early_retry_provisioner(tmp_path, monkeypatch)
    provisioner._local_wheel_path = None
    provisioner._wheel_hash = None
    provisioner._active_cluster_hash = None
    provisioner._is_managed = False
    provisioner._workload_type = 'service'
    provisioner._extra_launch_context = {'source': 'test'}
    provisioner._is_launched_by_jobs_controller = False
    to_provision = _to_provision()
    provider_error = _aggregate_error('InsufficientInstanceCapacity')
    provider_error.requested_count = 1
    template_override = mock.Mock(return_value=backend.provision_lib.
                                  TemplateSpec('/tmp/plugin-template', {
                                      'plugin_value': 'yes',
                                  }))
    write_cluster_config = mock.Mock(return_value={
        'ray': '/tmp/cluster.yaml',
        'cluster_name_on_cloud': 'test-cluster',
    })

    monkeypatch.setattr(backend.provision_lib, '_registered_provisioners', {})
    monkeypatch.setattr(backend.provision_lib,
                        '_registered_provisioner_bundles', {})
    monkeypatch.setattr(backend.provision_lib,
                        '_legacy_mixed_owner_diagnostics', set())
    backend.provision_lib.register_provisioner(
        'aws',
        mock.Mock(spec=[]),
        template_override=template_override,
    )
    monkeypatch.setattr(clouds.AWS, 'check_quota_available', lambda *_: True)
    monkeypatch.setattr(provisioner, '_yield_zones',
                        lambda *_: iter([[clouds.Zone('us-east-1a')]]))
    monkeypatch.setattr(backend, '_capacity_cache_exhausted_zone_names',
                        lambda *_: set())
    monkeypatch.setattr(backend, '_get_image_demand_attribution',
                        lambda *_: mock.MagicMock())
    monkeypatch.setattr(backend, '_resolve_container_image_for_placement',
                        lambda resources, **_: resources)
    monkeypatch.setattr(
        backend,
        '_get_cluster_config_template',
        mock.Mock(side_effect=AssertionError('unexpected default template')),
    )
    monkeypatch.setattr(backend.backend_utils, 'write_cluster_config',
                        write_cluster_config)
    monkeypatch.setattr(backend, '_get_workload_attribution', lambda *_:
                        (None, None))
    monkeypatch.setattr(backend.global_user_state, 'add_or_update_cluster',
                        lambda *_, **__: 'cluster-hash')
    monkeypatch.setattr(backend.global_user_state, 'add_cluster_event',
                        lambda *_, **__: None)
    monkeypatch.setattr(backend.global_user_state,
                        'set_owner_identity_for_cluster', lambda *_, **__: None)
    monkeypatch.setattr(backend.usage_lib.messages.usage,
                        'update_final_cluster_status', lambda *_: None)
    monkeypatch.setattr(backend.controller_utils.Controllers, 'from_name',
                        lambda *_: None)
    monkeypatch.setattr(backend.provisioner, 'bulk_provision',
                        mock.Mock(side_effect=provider_error))
    monkeypatch.setattr(backend.CloudVmRayBackend, 'post_teardown_cleanup',
                        lambda *_, **__: None)
    monkeypatch.setattr(backend.FailoverCloudErrorHandlerV2,
                        'update_blocklist_on_error', lambda *_, **__: None)
    monkeypatch.setattr(backend, '_record_service_placement_event',
                        lambda *_, **__: None)
    monkeypatch.setattr(backend.capacity_cache, 'mark_exhausted',
                        lambda *_, **__: None)

    with pytest.raises(exceptions.ResourcesUnavailableError):
        _call_retry_zones(provisioner, to_provision)

    template_override.assert_called_once_with(
        None,
        to_provision,
        _extra_launch_context={'source': 'test'},
        _is_launched_by_jobs_controller=False,
    )
    write_cluster_config.assert_called_once()
    config_args, config_kwargs = write_cluster_config.call_args
    assert config_args[2] == '/tmp/plugin-template'
    assert config_kwargs['extra_template_variables'] == {
        'plugin_value': 'yes',
    }


def _configure_new_provisioner_callback_attempt(tmp_path,
                                                monkeypatch,
                                                events,
                                                provider_outcomes,
                                                *,
                                                config_hash='generated-hash',
                                                bulk_error=None):
    """Configures one isolated current new-provisioner attempt."""
    provisioner = _early_retry_provisioner(tmp_path, monkeypatch)
    provisioner._active_cluster_hash = None
    provisioner._is_managed = False
    provisioner._workload_type = 'service'
    provisioner._extra_launch_context = {}
    provisioner._is_launched_by_jobs_controller = False
    to_provision = resources_lib.Resources(cloud=clouds.DO(),
                                           region='nyc3',
                                           instance_type='g-2vcpu-8gb',
                                           use_spot=False)
    outcomes = iter(provider_outcomes)
    provider_call_count = 0
    writer_results = []

    monkeypatch.setenv('SKYPILOT_USER', 'test-user')
    skypilot_config = backend.backend_utils.skypilot_config
    monkeypatch.setenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, os.devnull)
    monkeypatch.setattr(skypilot_config, '_global_config_context',
                        skypilot_config.ConfigContext())
    skypilot_config.reload_config()
    assert not skypilot_config.loaded()

    input_dir = tmp_path / 'do-writer-inputs'
    input_dir.mkdir(exist_ok=True)
    private_key_path = input_dir / 'test-key'
    private_key_path.write_text('test-private-key', encoding='utf-8')
    public_key_path = input_dir / 'test-key.pub'
    public_key_path.write_text('test-public-key', encoding='utf-8')
    wheel_path = input_dir / 'sky.whl'
    wheel_path.write_bytes(b'test-wheel')
    provisioner._local_wheel_path = wheel_path
    provisioner._wheel_hash = 'b1bd84059bc0342f7843fcbe04ab563e'

    monkeypatch.setattr(backend.backend_utils.auth_utils,
                        'get_or_generate_keys', lambda:
                        (str(private_key_path), str(public_key_path)))
    monkeypatch.setattr(backend.backend_utils.sky_check,
                        'get_cloud_credential_file_mounts', lambda *_: {})
    monkeypatch.setattr(backend.backend_utils.logs, 'get_logging_agent',
                        lambda: None)
    monkeypatch.setattr(backend.backend_utils.common_utils, 'get_user_hash',
                        lambda: '00000000')
    monkeypatch.setattr(skypilot_config, 'get_active_workspace',
                        lambda: 'default')
    monkeypatch.setattr(backend.backend_utils.sky, '__version__', '1.0.0')
    output_path = tmp_path / 'do-callback-attempt' / 'cluster.yaml'
    monkeypatch.setattr(backend.backend_utils,
                        '_get_yaml_path_from_cluster_name',
                        lambda *_args, **_kwargs: str(output_path))
    monkeypatch.setattr(backend.backend_utils.global_user_state,
                        'get_cluster_yaml_str', lambda *_: None)
    monkeypatch.setattr(backend.backend_utils.global_user_state,
                        'set_cluster_yaml', lambda *_, **__: None)
    monkeypatch.setattr(backend.backend_utils, '_optimize_file_mounts',
                        lambda *_: None)
    monkeypatch.setattr(backend.backend_utils.usage_lib.messages.usage,
                        'update_ray_yaml', lambda *_: None)

    def make_deploy_resources_variables(self,
                                        resources,
                                        cluster_name,
                                        region,
                                        zones,
                                        num_nodes,
                                        dryrun=False,
                                        volume_mounts=None):
        del self, resources, cluster_name, region, zones, num_nodes, dryrun
        del volume_mounts
        nonlocal provider_call_count
        stage = 'writer' if provider_call_count == 0 else 'post_bulk'
        provider_call_count += 1
        events.append(f'deploy_vars:{stage}')
        outcome = next(outcomes)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    real_make_deploy_variables = resources_lib.Resources.make_deploy_variables

    def make_deploy_variables(self, *args, **kwargs):
        events.append('resources_deploy_vars')
        result = real_make_deploy_variables(self, *args, **kwargs)
        writer_results.append(result)
        return result

    real_write_cluster_config = backend.backend_utils.write_cluster_config

    def write_cluster_config(*args, **kwargs):
        events.append('config_writer')
        result = real_write_cluster_config(*args, **kwargs)
        result['config_hash'] = config_hash
        return result

    provision_record = provision_common.ProvisionRecord(
        provider_name='do',
        region='nyc3',
        zone=None,
        cluster_name='test-cluster',
        head_instance_id='head',
        resumed_instance_ids=['head'],
        created_instance_ids=[],
    )

    def bulk_provision(*args, **kwargs):
        del args, kwargs
        events.append('bulk_provision')
        if bulk_error is not None:
            raise bulk_error
        return provision_record

    bulk_provision_mock = mock.Mock(side_effect=bulk_provision)
    cleanup_mock = mock.Mock()
    monkeypatch.setattr(resources_lib.Resources, 'make_deploy_variables',
                        make_deploy_variables)
    monkeypatch.setattr(clouds.DO, 'make_deploy_resources_variables',
                        make_deploy_resources_variables)
    monkeypatch.setattr(clouds.DO, 'check_quota_available', lambda *_: True)
    monkeypatch.setattr(provisioner, '_yield_zones', lambda *_: iter([None]))
    monkeypatch.setattr(provisioner, '_insufficient_resources_msg',
                        lambda *_, **__: 'test resources unavailable')
    monkeypatch.setattr(backend, '_capacity_cache_exhausted_zone_names',
                        lambda *_: set())
    monkeypatch.setattr(backend, '_get_image_demand_attribution',
                        lambda *_: mock.MagicMock())
    monkeypatch.setattr(backend, '_resolve_container_image_for_placement',
                        lambda resources, **_: resources)
    monkeypatch.setattr(backend.provision_lib,
                        'get_provisioner_template_override', lambda *_: None)
    monkeypatch.setattr(backend.backend_utils, 'write_cluster_config',
                        write_cluster_config)
    monkeypatch.setattr(backend, '_get_workload_attribution', lambda *_:
                        (None, None))
    monkeypatch.setattr(backend.global_user_state, 'add_or_update_cluster',
                        lambda *_, **__: 'cluster-hash')
    monkeypatch.setattr(backend.global_user_state, 'add_cluster_event',
                        lambda *_, **__: None)
    monkeypatch.setattr(backend.global_user_state,
                        'set_owner_identity_for_cluster', lambda *_, **__: None)
    monkeypatch.setattr(backend.usage_lib.messages.usage,
                        'update_final_cluster_status', lambda *_: None)
    monkeypatch.setattr(backend.controller_utils.Controllers, 'from_name',
                        lambda *_, **__: None)
    monkeypatch.setattr(backend.provisioner, 'bulk_provision',
                        bulk_provision_mock)
    monkeypatch.setattr(backend.CloudVmRayBackend, 'post_teardown_cleanup',
                        cleanup_mock)
    monkeypatch.setattr(backend.FailoverCloudErrorHandlerV2,
                        'update_blocklist_on_error', lambda *_, **__: None)
    monkeypatch.setattr(backend, '_record_service_placement_event',
                        lambda *_, **__: None)

    return (provisioner, to_provision, provision_record, bulk_provision_mock,
            cleanup_mock, writer_results)


def _configure_reserved_fill_kubernetes_attempt(tmp_path,
                                                monkeypatch,
                                                events,
                                                provider_outcomes,
                                                *,
                                                mock_launch_fence=True,
                                                mock_provider_guard=True):
    """Converts the callback harness into one protocol-v2 Kubernetes try."""
    (provisioner, _, provision_record, bulk_provision, cleanup,
     _) = _configure_new_provisioner_callback_attempt(tmp_path, monkeypatch,
                                                      events, provider_outcomes)
    to_provision = resources_lib.Resources(cloud=clouds.Kubernetes(),
                                           region='phx-context',
                                           instance_type='4CPU--16GB--H200:1',
                                           accelerators={'H200': 1},
                                           use_spot=False)
    pool_key = reserved_capacity_broker.make_pool_key(
        'phx-context',
        'H200',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='physical-uid')
    launch_context = reserved_capacity.make_protocol_v2_launch_fence(
        pool_key=pool_key,
        service_generation=7,
        service_version=3,
        physical_cluster_uid='physical-uid',
        kubernetes_context='phx-context',
        accelerator='H200',
        accelerator_count=1)
    launch_context, association_id, request_id = _bound_reserved_fill_context(
        launch_context)
    provisioner._extra_launch_context = launch_context
    provisioner._kueue_admission_runtime = None

    # Protocol-v2 provider retries are generic bound non-pool requests.  Keep
    # this provider-focused harness on that complete production envelope so
    # its passive preflight reaches the behavior each test is exercising.
    claim = types.SimpleNamespace(request_id=request_id,
                                  worker_instance_id=str(uuid.uuid4()))
    monkeypatch.setattr(request_storage, 'active_execution_claim',
                        lambda: claim)
    monkeypatch.setattr(
        ordinary_launch_binding, 'binding_allows_request',
        lambda actual_association_id, actual_request_id:
        (actual_association_id == association_id and actual_request_id ==
         request_id))

    deploy_variables = clouds.DO.make_deploy_resources_variables
    monkeypatch.setattr(clouds.Kubernetes, 'make_deploy_resources_variables',
                        deploy_variables)
    monkeypatch.setattr(clouds.Kubernetes, 'check_quota_available',
                        lambda *_: True)
    monkeypatch.setattr(clouds.Kubernetes,
                        'yield_cloud_specific_failover_overrides',
                        lambda *_args, **_kwargs: [None])
    monkeypatch.setattr(backend, '_get_cluster_config_template',
                        lambda *_: '/tmp/kubernetes-template')
    monkeypatch.setattr(backend, '_capacity_cache_account', lambda *_: None)

    def write_cluster_config(*_args, **_kwargs):
        events.append('config_writer')
        return {
            'ray': str(tmp_path / 'kubernetes-cluster.yaml'),
            'cluster_name_on_cloud': 'test-cluster',
            'config_hash': 'generated-hash',
        }

    monkeypatch.setattr(backend.backend_utils, 'write_cluster_config',
                        write_cluster_config)
    if mock_launch_fence:
        monkeypatch.setattr(provisioner,
                            '_validate_service_replica_launch_fence',
                            lambda: None)
    monkeypatch.setattr(provisioner, '_validate_reserved_fill_candidate',
                        lambda _resources: None)
    monkeypatch.setattr(backend.provisioner, '_BUILTIN_BULK_PROVISION',
                        bulk_provision)
    monkeypatch.setattr(provisioner, '_record_fresh_provision_evidence',
                        lambda *_args, **_kwargs: None)
    blocklist = mock.Mock()
    monkeypatch.setattr(backend.FailoverCloudErrorHandlerV2,
                        'update_blocklist_on_error', blocklist)

    @contextlib.contextmanager
    def fresh_guard():
        events.append('guard-enter')
        try:
            yield
        finally:
            events.append('guard-exit')

    if mock_provider_guard:
        monkeypatch.setattr(provisioner,
                            '_service_replica_launch_provider_guard',
                            fresh_guard)
    return (provisioner, to_provision, provision_record, bulk_provision,
            cleanup, blocklist, fresh_guard)


def _bound_reserved_fill_context(
        context: dict[str, object]) -> tuple[dict[str, object], str, str]:
    """Adds one complete generic association envelope to a physical fence."""
    association_id = str(uuid.uuid4())
    request_id = str(uuid.uuid4())
    replica_record_id = str(uuid.uuid4())
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
        authorization_reference='reserved-fill:test',
        authorization_generation=1,
        authorization_payload={'allocation': 'test'})
    context.update({
        ordinary_launch_binding.ASSOCIATION_ID_KEY: association_id,
        ordinary_launch_binding.LAUNCH_GENERATION_KEY: 1,
        ordinary_launch_binding.BOUND_REQUEST_ID_KEY: request_id,
        ordinary_launch_binding.INPUT_DIGEST_KEY: 'a' * 64,
        ordinary_launch_binding.REPLICA_ID_KEY: 1,
        ordinary_launch_binding.REPLICA_RECORD_ID_KEY: replica_record_id,
        backend.serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
        ordinary_launch_binding.BINDING_PROTOCOL_VERSION_KEY:
            ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION,
        ordinary_launch_binding.PROFILE_KIND_KEY: profile.kind.value,
        ordinary_launch_binding.PROFILE_VERSION_KEY: profile.version,
        ordinary_launch_binding.PROFILE_DIGEST_KEY: profile.digest,
        ordinary_launch_binding.CAPABILITY_COHORT_EPOCH_KEY:
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH,
        ordinary_launch_binding.CAPABILITY_PROFILE_SET_DIGEST_KEY:
            ordinary_launch_binding.supported_non_pool_profile_set_digest(),
        ordinary_launch_binding.RECEIPT_PROTOCOL_VERSION_KEY:
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION,
        ordinary_launch_binding.AUTHORIZATION_KIND_KEY:
            profile.authorization_kind.value,
        ordinary_launch_binding.AUTHORIZATION_REFERENCE_KEY:
            profile.authorization_reference,
        ordinary_launch_binding.AUTHORIZATION_GENERATION_KEY:
            profile.authorization_generation,
        ordinary_launch_binding.AUTHORIZATION_DIGEST_KEY:
            profile.authorization_digest,
    })
    return context, association_id, request_id


def test_new_provisioner_post_bulk_callback_is_authoritative(
        tmp_path, monkeypatch):
    events = []
    writer_variables = {
        'instance_type': 'g-2vcpu-8gb',
        'custom_resources': 'writer-value',
        'region': 'nyc3',
    }
    post_bulk_variables = {
        'instance_type': 'g-2vcpu-8gb',
        'custom_resources': 'post-bulk-value',
        'region': 'nyc3',
    }
    (provisioner, to_provision, provision_record, bulk_provision, cleanup,
     writer_results) = _configure_new_provisioner_callback_attempt(
         tmp_path,
         monkeypatch,
         events,
         [writer_variables, post_bulk_variables],
     )

    result = _call_retry_zones(provisioner, to_provision)

    assert events == [
        'config_writer',
        'resources_deploy_vars',
        'deploy_vars:writer',
        'bulk_provision',
        'deploy_vars:post_bulk',
    ]
    assert {
        key: writer_results[0][key] for key in writer_variables
    } == writer_variables
    assert result['resources_vars'] == post_bulk_variables
    assert result['provision_record'] is provision_record
    bulk_provision.assert_called_once()
    cleanup.assert_not_called()


def test_new_provisioner_builtin_bulk_receives_exact_cluster_incarnation(
        tmp_path, monkeypatch):
    reloaded = importlib.reload(backend.provisioner)
    assert reloaded.bulk_provision is reloaded._BUILTIN_BULK_PROVISION

    events = []
    writer_variables = {
        'instance_type': 'g-2vcpu-8gb',
        'custom_resources': 'writer-value',
        'region': 'nyc3',
    }
    post_bulk_variables = {
        'instance_type': 'g-2vcpu-8gb',
        'custom_resources': 'post-bulk-value',
        'region': 'nyc3',
    }
    (retrying_provisioner, to_provision, provision_record, _, cleanup,
     _) = _configure_new_provisioner_callback_attempt(
         tmp_path,
         monkeypatch,
         events,
         [writer_variables, post_bulk_variables],
     )
    cluster_incarnation = ''.join(['exact', '-cluster', '-incarnation'])
    monkeypatch.setattr(backend.global_user_state, 'add_or_update_cluster',
                        lambda *_, **__: cluster_incarnation)

    def get_cluster_yaml_str(path):
        if path is None:
            return None
        yaml_path = pathlib.Path(path)
        debug_path = pathlib.Path(f'{path}.debug')
        if yaml_path.exists():
            return yaml_path.read_text(encoding='utf-8')
        if debug_path.exists():
            return debug_path.read_text(encoding='utf-8')
        return None

    monkeypatch.setattr(backend.global_user_state, 'get_cluster_yaml_str',
                        get_cluster_yaml_str)
    captured_configs = []

    def fake_bulk_provision(cloud, region, cluster_name, bootstrap_config):
        del cloud, region, cluster_name
        events.append('bulk_provision')
        captured_configs.append(bootstrap_config)
        return provision_record

    monkeypatch.setattr(backend.provisioner, '_bulk_provision',
                        fake_bulk_provision)
    monkeypatch.setattr(backend.provisioner, 'bulk_provision',
                        backend.provisioner._BUILTIN_BULK_PROVISION)

    result = _call_retry_zones(retrying_provisioner, to_provision)

    assert events == [
        'config_writer',
        'resources_deploy_vars',
        'deploy_vars:writer',
        'bulk_provision',
        'deploy_vars:post_bulk',
    ]
    assert len(captured_configs) == 1
    assert captured_configs[0].cluster_incarnation is cluster_incarnation
    assert result['cluster_hash'] is cluster_incarnation
    assert cluster_incarnation not in get_cluster_yaml_str(result['ray'])
    cleanup.assert_not_called()


def test_new_provisioner_old_signature_bulk_replacement_keeps_old_call_shape(
        tmp_path, monkeypatch):
    events = []
    (retrying_provisioner, to_provision, provision_record, _, cleanup,
     _) = _configure_new_provisioner_callback_attempt(
         tmp_path,
         monkeypatch,
         events,
         [
             {
                 'instance_type': 'g-2vcpu-8gb',
                 'custom_resources': 'writer-value',
                 'region': 'nyc3',
             },
             {
                 'instance_type': 'g-2vcpu-8gb',
                 'custom_resources': 'post-bulk-value',
                 'region': 'nyc3',
             },
         ],
     )
    calls = []

    def old_bulk_provision(cloud,
                           region,
                           zones,
                           cluster_name,
                           num_nodes,
                           cluster_yaml,
                           prev_cluster_ever_up,
                           log_dir,
                           ports_to_open_on_launch=None):
        calls.append(
            (cloud, region, zones, cluster_name, num_nodes, cluster_yaml,
             prev_cluster_ever_up, log_dir, ports_to_open_on_launch))
        events.append('bulk_provision')
        return provision_record

    monkeypatch.setattr(backend.provisioner, 'bulk_provision',
                        old_bulk_provision)

    result = _call_retry_zones(retrying_provisioner, to_provision)

    assert result['provision_record'] is provision_record
    assert len(calls) == 1
    assert 'cluster_incarnation' not in inspect.signature(
        old_bulk_provision).parameters
    cleanup.assert_not_called()


def test_new_provisioner_replacement_module_keeps_old_call_shape(
        tmp_path, monkeypatch):
    events = []
    (retrying_provisioner, to_provision, provision_record, _, cleanup,
     _) = _configure_new_provisioner_callback_attempt(
         tmp_path,
         monkeypatch,
         events,
         [
             {
                 'instance_type': 'g-2vcpu-8gb',
                 'custom_resources': 'writer-value',
                 'region': 'nyc3',
             },
             {
                 'instance_type': 'g-2vcpu-8gb',
                 'custom_resources': 'post-bulk-value',
                 'region': 'nyc3',
             },
         ],
     )
    calls = []

    def old_bulk_provision(cloud,
                           region,
                           zones,
                           cluster_name,
                           num_nodes,
                           cluster_yaml,
                           prev_cluster_ever_up,
                           log_dir,
                           ports_to_open_on_launch=None):
        calls.append(
            (cloud, region, zones, cluster_name, num_nodes, cluster_yaml,
             prev_cluster_ever_up, log_dir, ports_to_open_on_launch))
        events.append('bulk_provision')
        return provision_record

    replacement_module = types.SimpleNamespace(
        bulk_provision=old_bulk_provision)
    monkeypatch.setattr(backend, 'provisioner', replacement_module)

    result = _call_retry_zones(retrying_provisioner, to_provision)

    assert result['provision_record'] is provision_record
    assert len(calls) == 1
    cleanup.assert_not_called()


@pytest.mark.parametrize(
    ('dryrun', 'config_hash', 'matching_hash', 'provisioning_skipped'),
    [
        (True, 'dryrun-hash', None, False),
        (False, 'same-hash', 'same-hash', True),
    ],
    ids=['dryrun', 'matching-config-hash'],
)
def test_new_provisioner_short_circuit_skips_bulk_and_post_bulk_callback(
        tmp_path, monkeypatch, dryrun, config_hash, matching_hash,
        provisioning_skipped):
    events = []
    writer_variables = {
        'instance_type': 'g-2vcpu-8gb',
        'custom_resources': 'writer-value',
        'region': 'nyc3',
    }
    (provisioner, to_provision, _, bulk_provision, cleanup,
     writer_results) = _configure_new_provisioner_callback_attempt(
         tmp_path,
         monkeypatch,
         events,
         [writer_variables],
         config_hash=config_hash,
     )

    result = _call_retry_zones(
        provisioner,
        to_provision,
        dryrun=dryrun,
        skip_if_config_hash_matches=matching_hash,
    )

    assert events == [
        'config_writer',
        'resources_deploy_vars',
        'deploy_vars:writer',
    ]
    assert {
        key: writer_results[0][key] for key in writer_variables
    } == writer_variables
    assert result['provisioning_skipped'] is provisioning_skipped
    bulk_provision.assert_not_called()
    cleanup.assert_not_called()


def test_new_provisioner_bulk_failure_skips_post_bulk_callback_and_cleans_up(
        tmp_path, monkeypatch):
    events = []
    bulk_error = RuntimeError('bulk mutation failed')
    (provisioner, to_provision, _, bulk_provision, cleanup,
     _) = _configure_new_provisioner_callback_attempt(
         tmp_path,
         monkeypatch,
         events,
         [{
             'instance_type': 'g-2vcpu-8gb',
             'custom_resources': 'writer-value',
             'region': 'nyc3',
         }],
         bulk_error=bulk_error,
     )

    with pytest.raises(exceptions.ResourcesUnavailableError) as exc_info:
        _call_retry_zones(provisioner, to_provision)

    assert events == [
        'config_writer',
        'resources_deploy_vars',
        'deploy_vars:writer',
        'bulk_provision',
    ]
    assert exc_info.value.failover_history == [bulk_error]
    bulk_provision.assert_called_once()
    cleanup.assert_called_once()


def test_serve_provider_guard_spans_builtin_bulk_provision(
        tmp_path, monkeypatch):
    events = []
    (provisioner, to_provision, provision_record, bulk_provision, cleanup,
     _) = _configure_new_provisioner_callback_attempt(
         tmp_path,
         monkeypatch,
         events,
         [{
             'instance_type': 'g-2vcpu-8gb',
             'custom_resources': 'writer-value',
             'region': 'nyc3',
         }, {
             'instance_type': 'g-2vcpu-8gb',
             'custom_resources': 'post-bulk-value',
             'region': 'nyc3',
         }],
     )
    provisioner._extra_launch_context = {
        backend.serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
    }
    fence_holds = mock.Mock(return_value=True)
    monkeypatch.setattr(backend.serve_state,
                        'service_replica_launch_fence_holds', fence_holds)
    monkeypatch.setattr(backend.serve_state,
                        'service_replica_launch_authority_guard_is_valid',
                        lambda _: True)
    guard = object()

    @contextlib.contextmanager
    def shared_guard(service_name):
        assert service_name == 'svc'
        events.append('guard-enter')
        try:
            yield guard
        finally:
            events.append('guard-exit')

    monkeypatch.setattr(backend.serve_state,
                        'service_replica_launch_authority_guard', shared_guard)

    result = _call_retry_zones(provisioner, to_provision)

    assert result['provision_record'] is provision_record
    assert events == [
        'config_writer',
        'resources_deploy_vars',
        'deploy_vars:writer',
        'guard-enter',
        'bulk_provision',
        'guard-exit',
        'deploy_vars:post_bulk',
    ]
    assert fence_holds.call_count >= 2
    bulk_provision.assert_called_once()
    cleanup.assert_not_called()


def test_only_reserved_fill_builtin_kubernetes_splits_provider_effect_guard(
) -> None:

    def builtin():
        return None

    def replacement():
        return None

    @contextlib.contextmanager
    def guard_factory():
        yield

    selector = backend._reserved_fill_kubernetes_provider_effect_guard_factory
    assert selector(clouds.Kubernetes(),
                    builtin,
                    builtin,
                    guard_factory,
                    reserved_fill=True) is guard_factory
    assert selector(clouds.Kubernetes(),
                    replacement,
                    builtin,
                    guard_factory,
                    reserved_fill=True) is None
    assert selector(clouds.DO(),
                    builtin,
                    builtin,
                    guard_factory,
                    reserved_fill=True) is None
    assert selector(clouds.Kubernetes(),
                    builtin,
                    builtin,
                    guard_factory,
                    reserved_fill=False) is None


def test_reserved_fill_builtin_success_marks_materialized_and_checkpoints(
        tmp_path, monkeypatch):
    events = []
    post_bulk_variables = {
        'instance_type': '4CPU--16GB--H200:1',
        'custom_resources': 'H200:1',
        'region': 'phx-context',
    }
    (provisioner, to_provision, provision_record, _, cleanup, blocklist,
     fresh_guard) = _configure_reserved_fill_kubernetes_attempt(
         tmp_path, monkeypatch, events, [post_bulk_variables])

    def builtin_bulk_provision(*_args, **kwargs):
        events.append('bulk-start')
        assert kwargs['provider_effect_guard_factory'] is fresh_guard
        with kwargs['provider_effect_guard_factory']():
            events.append('provider-create')
        events.append('bulk-return')
        return provision_record

    monkeypatch.setattr(backend.provisioner, '_BUILTIN_BULK_PROVISION',
                        builtin_bulk_provision)
    monkeypatch.setattr(backend.provisioner, 'bulk_provision',
                        builtin_bulk_provision)

    result = _call_retry_zones(provisioner, to_provision)

    assert result['_reserved_fill_pod_materialized'] is True
    bulk_return = events.index('bulk-return')
    post_bulk_variables_index = events.index('deploy_vars:writer')
    # The passive provisioner wait ends at bulk-return. A fresh guard must be
    # acquired after that point and before any post-create provider tail runs.
    assert 'guard-enter' in events[bulk_return + 1:post_bulk_variables_index]
    assert result['provision_record'] is provision_record
    cleanup.assert_not_called()
    blocklist.assert_not_called()


def test_reserved_fill_kueue_pause_preserves_created_pod_without_failover(
        tmp_path, monkeypatch):
    """A provider-internal admission pause precedes the bulk-return marker."""
    events = []
    post_bulk_variables = {
        'instance_type': '4CPU--16GB--H200:1',
        'custom_resources': 'H200:1',
        'region': 'phx-context',
    }
    (provisioner, to_provision, _, bulk_provision, cleanup, blocklist,
     _) = _configure_reserved_fill_kubernetes_attempt(tmp_path, monkeypatch,
                                                      events,
                                                      [post_bulk_variables])
    paused = exceptions.ExecutionPausedError(
        'Required Kueue Pod is policy-gated.',
        hint='retry exact Pod admission',
        retry_wait_seconds=5)
    bulk_provision.side_effect = paused

    with pytest.raises(exceptions.ExecutionPausedError) as exc_info:
        _call_retry_zones(provisioner, to_provision)

    assert exc_info.value is paused
    bulk_provision.assert_called_once()
    # bulk_provision owns the created/adopted Pod and intentionally raises
    # before returning the post-create materialization marker.  The pause must
    # therefore bypass both provider cleanup and capacity failover; retry
    # adopts the exact Pod rather than launching another one.
    cleanup.assert_not_called()
    blocklist.assert_not_called()
    assert not provisioner._blocked_resources


def test_reserved_fill_retry_preflight_is_passive_but_bulk_has_active_guard(
        tmp_path, monkeypatch):
    """Regression: v2 planning cannot require an effect-only ContextVar."""
    events = []
    post_bulk_variables = {
        'instance_type': '4CPU--16GB--H200:1',
        'custom_resources': 'H200:1',
        'region': 'phx-context',
    }
    (provisioner, to_provision, provision_record, _, cleanup, blocklist,
     _) = _configure_reserved_fill_kubernetes_attempt(tmp_path,
                                                      monkeypatch,
                                                      events,
                                                      [post_bulk_variables],
                                                      mock_launch_fence=False,
                                                      mock_provider_guard=False)
    context, association_id, request_id = _bound_reserved_fill_context(
        provisioner._extra_launch_context)
    provisioner._extra_launch_context = context
    active = False

    claim = types.SimpleNamespace(request_id=request_id,
                                  worker_instance_id=str(uuid.uuid4()))
    monkeypatch.setattr(request_storage, 'active_execution_claim',
                        lambda: claim)
    binding_allows = mock.Mock(return_value=True)
    monkeypatch.setattr(ordinary_launch_binding, 'binding_allows_request',
                        binding_allows)

    @contextlib.contextmanager
    def effect_guard(actual_context):
        nonlocal active
        assert actual_context is context
        assert not active
        active = True
        events.append('effect-enter')
        try:
            yield
        finally:
            events.append('effect-exit')
            active = False

    def require_active(actual_context):
        assert actual_context is context
        assert active
        return types.SimpleNamespace(
            durable_replica_info=mock.sentinel.durable_replica)

    @contextlib.contextmanager
    def committed_guard(_snapshot):
        assert active
        yield

    monkeypatch.setattr(ordinary_launch_request, '_provider_effect_guard',
                        effect_guard)
    monkeypatch.setattr(ordinary_launch_binding,
                        'require_active_provider_effect_authorization',
                        require_active)
    monkeypatch.setattr(provisioner, '_reserved_fill_committed_provider_guard',
                        committed_guard)
    monkeypatch.setattr(provider_phase, 'provider_phase',
                        lambda _mode: contextlib.nullcontext())

    def builtin_bulk_provision(*_args, **kwargs):
        # The two local preflights above this call run with ``active == False``.
        assert not active
        guard = kwargs['provider_effect_guard_factory']
        with guard():
            assert active
            events.append('provider-create')
        return provision_record

    monkeypatch.setattr(backend.provisioner, '_BUILTIN_BULK_PROVISION',
                        builtin_bulk_provision)
    monkeypatch.setattr(backend.provisioner, 'bulk_provision',
                        builtin_bulk_provision)
    monkeypatch.setattr(clouds.Kubernetes, 'get_identity_from_context_name',
                        lambda *_: ['user'])
    monkeypatch.setattr(clouds.Kubernetes, 'check_features_are_supported',
                        lambda *_: None)
    provisioner._dag = None
    provisioner._optimize_target = None
    provisioner._requested_features = set()
    task = mock.Mock()
    task.is_controller_task.return_value = False
    task.resources = {to_provision}
    task.best_resources = to_provision
    config = backend.RetryingVmProvisioner.ToProvisionConfig(
        cluster_name='test-cluster',
        resources=to_provision,
        num_nodes=1,
        prev_cluster_status=None,
        prev_handle=None,
        prev_cluster_ever_up=False,
        prev_config_hash=None)

    result = provisioner.provision_with_retries(
        task,
        config,
        dryrun=False,
        stream_logs=False,
        skip_unnecessary_provisioning=False)

    assert result['provision_record'] is provision_record
    assert events.count('provider-create') == 1
    assert binding_allows.call_count >= 3
    binding_allows.assert_any_call(association_id, request_id)
    cleanup.assert_not_called()
    blocklist.assert_not_called()


def test_reserved_fill_opaque_provisioner_is_rejected_before_mutation(
        tmp_path, monkeypatch):
    events = []
    post_bulk_variables = {
        'instance_type': '4CPU--16GB--H200:1',
        'custom_resources': 'H200:1',
        'region': 'phx-context',
    }
    (provisioner, to_provision, _, bulk_provision, cleanup, blocklist,
     _) = _configure_reserved_fill_kubernetes_attempt(tmp_path, monkeypatch,
                                                      events,
                                                      [post_bulk_variables])
    monkeypatch.setattr(backend.provisioner, '_BUILTIN_BULK_PROVISION',
                        lambda *_args, **_kwargs: None)

    with pytest.raises(exceptions.ReservedFillLaunchFenceError) as exc_info:
        _call_retry_zones(provisioner, to_provision)

    assert 'instrumented in-tree Kubernetes provisioner' in str(exc_info.value)
    bulk_provision.assert_not_called()
    cleanup.assert_not_called()
    blocklist.assert_not_called()
    assert not provisioner._blocked_resources


def test_reserved_fill_v2_config_hash_match_still_calls_bulk_adoption(
        tmp_path, monkeypatch):
    events = []
    post_bulk_variables = {
        'instance_type': '4CPU--16GB--H200:1',
        'custom_resources': 'H200:1',
        'region': 'phx-context',
    }
    (provisioner, to_provision, provision_record, bulk_provision, cleanup,
     blocklist,
     _) = _configure_reserved_fill_kubernetes_attempt(tmp_path, monkeypatch,
                                                      events,
                                                      [post_bulk_variables])

    result = _call_retry_zones(provisioner,
                               to_provision,
                               skip_if_config_hash_matches='generated-hash')

    # A matching local config hash cannot attest a reused Pod's current Kueue
    # lifecycle or frozen worker identity. Protocol v2 must therefore enter the
    # canonical bulk/adoption path rather than infer Pod materialization.
    assert result['provisioning_skipped'] is False
    assert result['_reserved_fill_pod_materialized'] is True
    assert result['provision_record'] is provision_record
    bulk_provision.assert_called_once()
    cleanup.assert_not_called()
    blocklist.assert_not_called()


def test_reserved_fill_backend_installs_successful_adoption_guard(
        tmp_path, monkeypatch):
    """Carries the provisioner's exact guard across the post-Pod tail."""
    events = []
    post_bulk_variables = {
        'instance_type': '4CPU--16GB--H200:1',
        'custom_resources': 'H200:1',
        'region': 'phx-context',
    }
    (retry_provisioner, to_provision, provision_record, bulk_provision, cleanup,
     blocklist, fresh_guard) = _configure_reserved_fill_kubernetes_attempt(
         tmp_path, monkeypatch, events, [post_bulk_variables])

    # Produce the exact result of a matching-hash launch. Protocol v2 must
    # nevertheless run bulk provisioning/adoption to attest the live Pod.
    config_result = _call_retry_zones(
        retry_provisioner,
        to_provision,
        skip_if_config_hash_matches='generated-hash')
    assert config_result['provisioning_skipped'] is False
    assert config_result['_reserved_fill_pod_materialized'] is True
    bulk_provision.assert_called_once()

    provision_record.runtime_metadata = provision_common.ProvisionRuntimeMetadata(
        has_job_queue=False, ssh_available=False, runtime_setup_done=True)
    handle = config_result['handle']
    update_ips = mock.Mock()
    update_ports = mock.Mock()
    monkeypatch.setattr(handle, 'update_cluster_ips', update_ips)
    monkeypatch.setattr(handle, 'update_ssh_ports', update_ports)

    provision_with_retries = mock.Mock(return_value=config_result)
    monkeypatch.setattr(retry_provisioner, 'provision_with_retries',
                        provision_with_retries)
    backend_instance = backend.CloudVmRayBackend()
    backend_instance.log_dir = str(tmp_path)
    backend_instance.register_info(
        extra_launch_context=retry_provisioner._extra_launch_context,
        workload_type='service')
    to_provision_config = mock.MagicMock(resources=to_provision,
                                         num_nodes=1,
                                         prev_cluster_status=None,
                                         prev_handle=None)
    task = mock.MagicMock(resources={to_provision}, blocked_resources=set())
    cluster_info = mock.MagicMock(docker_user=None)
    update_after_provisioned = mock.Mock()

    monkeypatch.setattr(backend_instance, '_check_existing_cluster',
                        mock.Mock(return_value=to_provision_config))
    monkeypatch.setattr(backend_instance,
                        '_maybe_clear_external_cluster_failures', mock.Mock())
    monkeypatch.setattr(backend_instance, '_update_after_cluster_provisioned',
                        update_after_provisioned)
    monkeypatch.setattr(backend.wheel_utils, 'build_sky_wheel',
                        mock.Mock(return_value=('/tmp/sky.whl', 'wheel-hash')))
    monkeypatch.setattr(backend, 'RetryingVmProvisioner',
                        mock.Mock(return_value=retry_provisioner))
    monkeypatch.setattr(backend.provision_lib, 'get_cluster_info',
                        mock.Mock(return_value=cluster_info))
    monkeypatch.setattr(backend.global_user_state, 'get_cluster_yaml_dict',
                        mock.Mock(return_value={'provider': {}}))
    monkeypatch.setattr(backend.rich_utils, 'force_update_status', mock.Mock())
    monkeypatch.setattr(backend.lock_events, 'DistributedLockEvent',
                        lambda *_args, **_kwargs: contextlib.nullcontext())
    monkeypatch.setattr(backend.usage_lib.messages.usage,
                        'update_cluster_resources', mock.Mock())
    monkeypatch.setattr(backend.usage_lib.messages.usage,
                        'update_cluster_status', mock.Mock())

    result = backend_instance._locked_provision(
        'lock-id',
        task,
        to_provision,
        False,
        False,
        'test-cluster',
        skip_unnecessary_provisioning=True)

    assert result == (handle, False)
    assert (backend_instance._reserved_fill_materialized_guard_factory
            is fresh_guard)
    assert backend_instance._reserved_fill_pod_materialized is True
    assert events.count('guard-enter') >= 3
    provision_with_retries.assert_called_once_with(task, to_provision_config,
                                                   False, False, True)
    update_ips.assert_called_once()
    update_ports.assert_called_once()
    update_after_provisioned.assert_called_once()
    cleanup.assert_not_called()
    blocklist.assert_not_called()


def test_serve_provider_guard_spans_whole_legacy_ray_up(tmp_path, monkeypatch):
    events = []
    (provisioner, to_provision, _, bulk_provision, cleanup,
     _) = _configure_new_provisioner_callback_attempt(
         tmp_path,
         monkeypatch,
         events,
         [{
             'instance_type': 'g-2vcpu-8gb',
             'custom_resources': 'writer-value',
             'region': 'nyc3',
         }],
     )
    provisioner._extra_launch_context = {
        backend.serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
    }
    monkeypatch.setattr(clouds.DO, 'PROVISIONER_VERSION',
                        clouds.ProvisionerVersion.RAY_AUTOSCALER)
    monkeypatch.setitem(backend._NODES_LAUNCHING_PROGRESS_TIMEOUT, clouds.DO,
                        90)
    monkeypatch.setattr(backend.serve_state,
                        'service_replica_launch_fence_holds', lambda _: True)
    monkeypatch.setattr(backend.serve_state,
                        'service_replica_launch_authority_guard_is_valid',
                        lambda _: True)
    guard_held = False
    guard = object()

    @contextlib.contextmanager
    def shared_guard(service_name):
        nonlocal guard_held
        assert service_name == 'svc'
        assert not guard_held
        guard_held = True
        events.append('guard-enter')
        try:
            yield guard
        finally:
            events.append('guard-exit')
            guard_held = False

    def run_ray_up(*args, **kwargs):
        del args, kwargs
        assert guard_held
        events.append('ray-up')
        return 0, 'head ready', ''

    def wait_until_ready(*args, **kwargs):
        del args, kwargs
        assert guard_held
        events.append('ray-ready')
        return True, None

    def update_cluster_ips(*args, **kwargs):
        del args, kwargs
        assert not guard_held
        events.append('update-ips')

    def update_ssh_ports(*args, **kwargs):
        del args, kwargs
        assert not guard_held
        events.append('update-ports')

    monkeypatch.setattr(backend.serve_state,
                        'service_replica_launch_authority_guard', shared_guard)
    monkeypatch.setattr(backend,
                        'write_ray_up_script_with_patched_launch_hash_fn',
                        lambda *_args, **_kwargs: '/tmp/ray-up.py')
    monkeypatch.setattr(backend.log_lib, 'run_with_log', run_ray_up)
    monkeypatch.setattr(backend.backend_utils, 'wait_until_ray_cluster_ready',
                        wait_until_ready)
    monkeypatch.setattr(backend.CloudVmRayResourceHandle, 'update_cluster_ips',
                        update_cluster_ips)
    monkeypatch.setattr(backend.CloudVmRayResourceHandle, 'update_ssh_ports',
                        update_ssh_ports)

    result = _call_retry_zones(provisioner, to_provision, num_nodes=2)

    assert result['handle'].launched_nodes == 2
    assert events == [
        'config_writer',
        'resources_deploy_vars',
        'deploy_vars:writer',
        'guard-enter',
        'ray-up',
        'ray-ready',
        'guard-exit',
        'update-ips',
        'update-ports',
    ]
    bulk_provision.assert_not_called()
    cleanup.assert_not_called()


@pytest.mark.parametrize(('validity_results', 'message', 'body_runs'), [
    ([False], 'before the provider operation started', False),
    ([True, False], 'while the provider operation was in progress', True),
])
def test_serve_provider_guard_validity_failures_are_terminal(
        monkeypatch, validity_results, message, body_runs):
    provisioner = object.__new__(backend.RetryingVmProvisioner)
    provisioner._workload_type = 'service'
    provisioner._extra_launch_context = {
        backend.serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
    }
    validate = mock.Mock()
    monkeypatch.setattr(provisioner, '_validate_service_replica_launch_fence',
                        validate)
    guard = object()

    @contextlib.contextmanager
    def shared_guard(_service_name):
        yield guard

    validity = mock.Mock(side_effect=validity_results)
    monkeypatch.setattr(backend.serve_state,
                        'service_replica_launch_authority_guard', shared_guard)
    monkeypatch.setattr(backend.serve_state,
                        'service_replica_launch_authority_guard_is_valid',
                        validity)
    ran = False

    with pytest.raises(exceptions.ServeReplicaLaunchFenceError, match=message):
        with provisioner._service_replica_launch_provider_guard():
            ran = True

    assert ran is body_runs
    assert validate.call_count == (1 if body_runs else 0)
    assert validity.call_count == len(validity_results)


def test_serve_provider_guard_preserves_provider_exception(monkeypatch):
    provisioner = object.__new__(backend.RetryingVmProvisioner)
    provisioner._workload_type = 'service'
    provisioner._extra_launch_context = {
        backend.serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
    }
    monkeypatch.setattr(provisioner, '_validate_service_replica_launch_fence',
                        mock.Mock())
    guard = object()

    @contextlib.contextmanager
    def shared_guard(_service_name):
        yield guard

    monkeypatch.setattr(backend.serve_state,
                        'service_replica_launch_authority_guard', shared_guard)
    monkeypatch.setattr(backend.serve_state,
                        'service_replica_launch_authority_guard_is_valid',
                        lambda _: True)
    provider_error = provision_common.ProvisionerError(
        'provider capacity classification')

    with pytest.raises(provision_common.ProvisionerError) as exc_info:
        with provisioner._service_replica_launch_provider_guard():
            raise provider_error

    assert exc_info.value is provider_error


@pytest.mark.parametrize('workload_type', ['service', 'pool'])
def test_serve_generation_change_at_terminal_boundary_skips_provider(
        tmp_path, monkeypatch, workload_type):
    """An admitted request cannot outlive its controller generation."""
    events = []
    (provisioner, to_provision, _, bulk_provision, cleanup,
     _) = _configure_new_provisioner_callback_attempt(
         tmp_path,
         monkeypatch,
         events,
         [{
             'instance_type': 'g-2vcpu-8gb',
             'custom_resources': 'writer-value',
             'region': 'nyc3',
         }],
     )
    provisioner._extra_launch_context = {
        backend.serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
        backend.serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'incarnation-a',
        backend.serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 1,
        backend.serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
        backend.serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.1',
    }
    provisioner._workload_type = workload_type
    authorized_v1 = {
        'hash': 'incarnation-a',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
        'status': backend.serve_state.ServiceStatus.READY,
        'launch_authorized_version': 1,
        'launch_version_required': True,
    }
    elected_v2 = dict(authorized_v1, launch_authorized_version=2)
    get_authorization = mock.Mock(
        side_effect=[authorized_v1, authorized_v1, elected_v2])
    monkeypatch.setattr(backend.serve_state,
                        'get_service_replica_launch_authorization',
                        get_authorization)

    with pytest.raises(exceptions.RequestCancelled,
                       match='durable service generation changed'):
        _call_retry_zones(provisioner, to_provision)

    assert events == [
        'config_writer',
        'resources_deploy_vars',
        'deploy_vars:writer',
    ]
    assert get_authorization.call_count == 3
    bulk_provision.assert_not_called()
    # The terminal fence is not a capacity failure and must not trigger a
    # provider cleanup/failover that could mutate a successor generation.
    cleanup.assert_not_called()


def test_serve_provider_fence_db_failure_is_terminal(tmp_path, monkeypatch):
    provisioner = _early_retry_provisioner(tmp_path, monkeypatch)
    provisioner._workload_type = 'service'
    provisioner._extra_launch_context = {
        backend.serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
    }
    monkeypatch.setattr(
        backend.serve_state, 'service_replica_launch_fence_holds',
        mock.Mock(side_effect=RuntimeError('database unavailable')))

    with pytest.raises(exceptions.RequestCancelled,
                       match='Unable to prove durable'):
        provisioner._validate_service_replica_launch_fence()


def test_new_provisioner_post_bulk_callback_failure_is_after_mutation_and_cleans_up(
        tmp_path, monkeypatch):
    events = []
    callback_error = RuntimeError('post-bulk callback failed')
    (provisioner, to_provision, _, bulk_provision, cleanup,
     _) = _configure_new_provisioner_callback_attempt(
         tmp_path,
         monkeypatch,
         events,
         [
             {
                 'instance_type': 'g-2vcpu-8gb',
                 'custom_resources': 'writer-value',
                 'region': 'nyc3',
             },
             callback_error,
         ],
     )

    with pytest.raises(exceptions.ResourcesUnavailableError) as exc_info:
        _call_retry_zones(provisioner, to_provision)

    assert events == [
        'config_writer',
        'resources_deploy_vars',
        'deploy_vars:writer',
        'bulk_provision',
        'deploy_vars:post_bulk',
    ]
    assert exc_info.value.failover_history == [callback_error]
    bulk_provision.assert_called_once()
    cleanup.assert_called_once()


def test_provision_with_retries_preserves_nested_terminal_failure(
        tmp_path, monkeypatch):
    to_provision = _to_provision()
    provider_error = _aggregate_error('InsufficientInstanceCapacity')
    per_location_error = exceptions.ResourcesUnavailableError(
        'location unavailable', failover_history=[provider_error])
    provisioner = backend.RetryingVmProvisioner(
        log_dir=str(tmp_path),
        dag=mock.Mock(),
        optimize_target=mock.Mock(),
        requested_features=set(),
        local_wheel_path=tmp_path / 'wheel',
        wheel_hash='',
        extra_launch_context={},
    )
    task = mock.Mock()
    task.is_controller_task.return_value = False
    task.num_nodes = 1
    task.resources = {to_provision}
    task.best_resources = to_provision
    task.volume_mounts = None
    config = backend.RetryingVmProvisioner.ToProvisionConfig(
        cluster_name='test-cluster',
        resources=to_provision,
        num_nodes=1,
        prev_cluster_status=None,
        prev_handle=None,
        prev_cluster_ever_up=False,
        prev_config_hash=None,
    )

    monkeypatch.setattr(clouds.AWS, 'get_active_user_identity',
                        lambda *_: ['acct'])
    monkeypatch.setattr(provisioner, '_retry_zones',
                        mock.Mock(side_effect=per_location_error))
    optimize = mock.Mock(
        side_effect=exceptions.ResourcesUnavailableError('optimizer exhausted'))
    monkeypatch.setattr(backend.optimizer.Optimizer, 'optimize', optimize)
    monkeypatch.setattr(backend, '_format_provision_failure_blocks',
                        lambda *_: '')
    monkeypatch.setattr(backend.rich_utils, 'force_update_status',
                        lambda *_: None)

    with pytest.raises(exceptions.ResourcesUnavailableError) as exc_info:
        provisioner.provision_with_retries(
            task,
            config,
            dryrun=False,
            stream_logs=False,
            skip_unnecessary_provisioning=False,
        )

    assert exc_info.value.failover_history == [per_location_error]
    optimize.assert_called_once()
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), exc_info.value) == 'capacity'


@pytest.mark.parametrize('no_failover', [False, True])
def test_reserved_fill_exact_candidate_failure_is_terminal_before_optimizer(
        tmp_path, monkeypatch, no_failover):

    def launchable_resources(context, count):

        class _LaunchableResources:
            """Minimal optimizer-selected resource candidate."""

            cloud = clouds.Kubernetes()
            region = context
            accelerators = {'H200': count}

            def assert_launchable(self):
                return self

        return _LaunchableResources()

    initial_resources = launchable_resources('phx-context', 1)
    pool_key = reserved_capacity_broker.make_pool_key(
        'phx-context',
        'H200',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='physical-uid')
    launch_context = reserved_capacity.make_protocol_v2_launch_fence(
        pool_key=pool_key,
        service_generation=7,
        service_version=3,
        physical_cluster_uid='physical-uid',
        kubernetes_context='phx-context',
        accelerator='H200',
        accelerator_count=1)
    launch_context, association_id, request_id = _bound_reserved_fill_context(
        launch_context)
    task = mock.Mock()
    task.is_controller_task.return_value = False
    task.num_nodes = 1
    task.resources = {initial_resources}
    task.best_resources = initial_resources
    task.volume_mounts = None
    dag = mock.Mock()
    dag.tasks = [task]
    provisioner = backend.RetryingVmProvisioner(
        log_dir=str(tmp_path),
        dag=dag,
        optimize_target=mock.Mock(),
        requested_features=set(),
        local_wheel_path=tmp_path / 'wheel',
        wheel_hash='',
        extra_launch_context=launch_context,
    )
    config = backend.RetryingVmProvisioner.ToProvisionConfig(
        cluster_name='svc-replica',
        resources=initial_resources,
        num_nodes=1,
        prev_cluster_status=None,
        prev_handle=None,
        prev_cluster_ever_up=False,
        prev_config_hash=None,
    )
    provider_leaf = _aggregate_error('InsufficientInstanceCapacity')
    provider_error = exceptions.ResourcesUnavailableError(
        'first exact attempt unavailable',
        no_failover=no_failover,
        failover_history=[provider_leaf])
    provider_attempt = mock.Mock(side_effect=provider_error)
    monkeypatch.setattr(provisioner, '_retry_zones', provider_attempt)
    claim = types.SimpleNamespace(request_id=request_id,
                                  worker_instance_id=str(uuid.uuid4()))
    monkeypatch.setattr(request_storage, 'active_execution_claim',
                        lambda: claim)
    monkeypatch.setattr(
        ordinary_launch_binding, 'binding_allows_request',
        lambda actual_association_id, actual_request_id:
        (actual_association_id == association_id and actual_request_id ==
         request_id))
    monkeypatch.setattr(clouds.Kubernetes, 'get_identity_from_context_name',
                        lambda *_: ['user'])
    monkeypatch.setattr(clouds.Kubernetes, 'check_features_are_supported',
                        lambda *_: None)

    optimize = mock.Mock(
        side_effect=AssertionError('v2 must not invoke optimizer'))
    monkeypatch.setattr(backend.optimizer.Optimizer, 'optimize', optimize)
    monkeypatch.setattr(backend.rich_utils, 'force_update_status',
                        lambda *_: None)

    with pytest.raises(exceptions.ResourcesUnavailableError,
                       match='first exact attempt unavailable') as exc_info:
        provisioner.provision_with_retries(task,
                                           config,
                                           dryrun=False,
                                           stream_logs=False,
                                           skip_unnecessary_provisioning=False)

    assert exc_info.value is provider_error
    assert exc_info.value.failover_history == [provider_leaf]
    assert backend.classify_resources_unavailable_error(
        clouds.AWS(), exc_info.value) == 'capacity'
    provider_attempt.assert_called_once()
    optimize.assert_not_called()


def test_quota_notification_has_generic_actionable_context(monkeypatch):
    record = mock.Mock(return_value=True)
    monkeypatch.setattr(backend.operator_notifications, 'record_notification',
                        record)

    assert backend._record_insufficient_quota_notification(_to_provision())
    category, message = record.call_args.args
    assert category == (backend.operator_notifications.
                        OperatorNotificationCategory.INSUFFICIENT_QUOTA)
    assert 'AWS' in message
    assert 'us-east-1' in message
    assert 'g6.4xlarge' in message
    assert 'service' not in message.lower()
    assert record.call_args.kwargs['dedupe_window_seconds'] == 3600


def test_quota_notification_is_fail_open(monkeypatch):
    monkeypatch.setattr(
        backend.operator_notifications,
        'record_notification',
        mock.Mock(side_effect=RuntimeError('notification unavailable')),
    )

    assert not backend._record_insufficient_quota_notification(_to_provision())


def test_capacity_metric_failure_is_fail_open(monkeypatch):
    counter = mock.Mock()
    counter.labels.return_value.inc.side_effect = RuntimeError(
        'metrics unavailable')
    metrics = mock.Mock(METRICS_ENABLED=True,
                        SKY_PROVISION_CAPACITY_EVENTS_TOTAL=counter)
    monkeypatch.setattr(backend, 'metrics_utils', metrics)

    backend._record_capacity_metric('quota', 'hit')

    counter.labels.assert_called_once_with(reason='quota', action='hit')
    counter.labels.return_value.inc.assert_called_once_with()


def _provision_record(*, created, resumed=(), zone='us-east-1a'):
    return provision_common.ProvisionRecord(
        provider_name='aws',
        region='us-east-1',
        zone=zone,
        cluster_name='test-cluster',
        head_instance_id='i-head',
        resumed_instance_ids=list(resumed),
        created_instance_ids=list(created),
    )


@pytest.mark.parametrize(
    ('record', 'num_nodes', 'cluster_exists', 'expected'), [
        (_provision_record(created=['i-1', 'i-2']), 2, False, True),
        (_provision_record(created=[], resumed=[]), 1, False, False),
        (_provision_record(created=['i-1'], resumed=['i-2']), 2, False, False),
        (_provision_record(created=['i-1', 'i-2']), 2, True, False),
    ])
def test_fully_created_fresh_demand_clear_eligibility(record, num_nodes,
                                                      cluster_exists, expected):
    assert backend._fully_created_fresh_demand(record, num_nodes,
                                               cluster_exists) is expected


def _enable_gcp_cache(monkeypatch, enabled: bool = True):
    real_get_nested = backend.skypilot_config.get_nested

    def fake_get_nested(keys, *args, **kwargs):
        if tuple(keys) == ('provision', 'gcp_capacity_cache'):
            return enabled
        return real_get_nested(keys, *args, **kwargs)

    monkeypatch.setattr(backend.skypilot_config, 'get_nested', fake_get_nested)


def _gcp_provision(*, accelerators=None, instance_type='g2-standard-4'):
    return resources_lib.Resources(cloud=clouds.GCP(),
                                   region='asia-northeast3',
                                   zone='asia-northeast3-b',
                                   instance_type=instance_type,
                                   accelerators=accelerators,
                                   use_spot=True)


def test_gcp_cache_key_is_on_by_default_and_can_be_disabled(monkeypatch):
    region = clouds.Region('asia-northeast3')
    zone = clouds.Zone('asia-northeast3-b')

    # Disabling is the escape hatch: no key means nothing is written or read.
    _enable_gcp_cache(monkeypatch, enabled=False)
    assert backend._capacity_cache_key(_gcp_provision(), region, [zone], 1,
                                       'proj') is None
    assert backend._quota_cooldown_key(_gcp_provision(), region, 1,
                                       'proj') is None

    # With no configuration at all, GCP participates.
    monkeypatch.undo()
    key = backend._capacity_cache_key(_gcp_provision(accelerators={'L4': 1}),
                                      region, [zone], 1, 'proj')
    assert key == capacity_cache.ResourceKey(cloud='gcp',
                                             account='proj',
                                             region='asia-northeast3',
                                             zone='asia-northeast3-b',
                                             instance_type='g2-standard-4',
                                             accelerators='L4:1',
                                             num_nodes=1)


def test_gcp_key_separates_accelerators_on_the_same_machine_type(monkeypatch):
    """N1 attaches accelerators separately, so the machine type is not enough."""
    _enable_gcp_cache(monkeypatch)
    region = clouds.Region('asia-northeast3')
    zone = clouds.Zone('asia-northeast3-b')
    t4 = backend._capacity_cache_key(
        _gcp_provision(instance_type='n1-standard-8', accelerators={'T4': 1}),
        region, [zone], 1, 'proj')
    v100 = backend._capacity_cache_key(
        _gcp_provision(instance_type='n1-standard-8', accelerators={'V100': 1}),
        region, [zone], 1, 'proj')
    assert t4 is not None and v100 is not None
    assert t4 != v100


def test_cache_keys_separate_clouds(monkeypatch):
    _enable_gcp_cache(monkeypatch)
    region = clouds.Region('us-east-1')
    zone = clouds.Zone('us-east-1a')
    aws_key = backend._capacity_cache_key(_to_provision(), region, [zone], 1,
                                          'same')
    gcp_key = backend._capacity_cache_key(
        _gcp_provision(instance_type='g6.4xlarge'), region, [zone], 1, 'same')
    assert aws_key is not None and gcp_key is not None
    assert aws_key != gcp_key


def test_capacity_cache_account_scopes_by_project_without_the_email():
    aws = backend._capacity_cache_account(clouds.AWS(), ['user-id', '1234567'])
    assert aws == '1234567'

    gcp = backend._capacity_cache_account(
        clouds.GCP(), ['someone@example.com [project_id=my-project]'])
    assert gcp == 'my-project'

    # An identity that does not carry a project must not be cached under the
    # raw string, which would also leak the account email into the key.
    assert backend._capacity_cache_account(clouds.GCP(),
                                           ['someone@example.com']) is None
    assert backend._capacity_cache_account(clouds.GCP(), None) is None
    assert backend._capacity_cache_account(clouds.Azure(), ['x']) is None


def test_gcp_touches_no_cache_entry_point_when_flag_is_off(monkeypatch):
    """With the flag off, GCP must not read or write the cache at all."""
    _enable_gcp_cache(monkeypatch, enabled=False)

    def _boom(*args, **kwargs):
        raise AssertionError('cache must not be touched when the flag is off')

    for name in ('mark_exhausted', 'active_exhausted_keys', 'clear',
                 'mark_quota_failure', 'is_quota_cooldown_active',
                 'clear_quota_cooldown'):
        monkeypatch.setattr(capacity_cache, name, _boom)

    region = clouds.Region('asia-northeast3')
    zones = [clouds.Zone('asia-northeast3-b')]
    to_provision = _gcp_provision(accelerators={'L4': 1})

    assert backend._capacity_cache_key(to_provision, region, zones, 1,
                                       'proj') is None
    quota_key = backend._quota_cooldown_key(to_provision, region, 1, 'proj')
    assert quota_key is None
    # The consult helpers must short-circuit on the absent key rather than
    # reaching the cache.
    assert backend._capacity_cache_exhausted_zone_names(to_provision, region,
                                                        zones, 1,
                                                        'proj') == set()
    assert not backend._quota_cooldown_is_active(quota_key)


def test_gcp_provisioner_sets_requested_count_so_failures_can_cache(
        monkeypatch):
    """A GCP bulk-insert failure must prove it covered the whole demand.

    Without `requested_count`, `_failure_requested_full_demand` is False and
    the failure is silently never cached, making the GCP cache a no-op.
    """
    codes = [
        'VM_MIN_COUNT_NOT_REACHED', 'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS'
    ]

    class _FakeCompute:
        """Minimal stand-in for the GCP compute resource."""

        STOPPING_STATES: list = []
        NON_TERMINATED_STATES: list = []
        PENDING_STATES: list = []

        @classmethod
        def filter(cls, *args, **kwargs):
            del args, kwargs
            return {}

        @classmethod
        def create_instances(cls, *args, **kwargs):
            del args, kwargs
            return ([{'code': code, 'message': code} for code in codes], [])

    monkeypatch.setattr(gcp_instance_utils, 'GCPComputeInstance', _FakeCompute)
    monkeypatch.setattr(gcp_instance.instance_utils, 'GCPComputeInstance',
                        _FakeCompute)
    monkeypatch.setattr(gcp_instance.instance_utils, 'get_node_type',
                        lambda _: gcp_instance_utils.GCPNodeType.COMPUTE)

    config = provision_common.ProvisionConfig(
        provider_config={
            'project_id': 'proj',
            'availability_zone': 'asia-northeast3-b',
        },
        authentication_config={},
        docker_config={},
        node_config={},
        count=2,
        tags={},
        resume_stopped_nodes=True,
        ports_to_open_on_launch=None,
    )

    with pytest.raises(provision_common.ProvisionerError) as exc_info:
        gcp_instance._run_instances('asia-northeast3', 'cluster', config)

    error = exc_info.value
    assert [entry['code'] for entry in error.errors] == codes
    # The wiring under test: without this the failure can never be cached.
    assert error.requested_count == 2
    assert backend._failure_requested_full_demand(error, 2)
    assert backend._classify_capacity_error(clouds.GCP(), error) == 'capacity'
