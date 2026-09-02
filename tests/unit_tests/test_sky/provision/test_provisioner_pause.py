"""bulk_provision must not tear down resources when execution pauses.

A paused execution (ExecutionPausedError) is waiting on an external condition
and wants its partially provisioned resources kept so it can resume. This pins
that bulk_provision re-raises the pause without tearing down, while still
tearing down on an ordinary provisioning failure.
"""
import contextlib
from unittest import mock

import pytest

from sky import clouds
from sky import exceptions
from sky import global_user_state
from sky.provision import common
from sky.provision import provisioner
from sky.utils import resources_utils

_CLUSTER_YAML_DICT = {
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


@pytest.fixture()
def patched_bulk_provision(monkeypatch):
    """Drive bulk_provision with its filesystem/state deps stubbed out.

    Returns the teardown_cluster mock so tests can assert on it; the caller
    sets _bulk_provision's side effect.
    """
    monkeypatch.setattr(global_user_state, 'get_cluster_yaml_dict',
                        lambda *a, **k: dict(_CLUSTER_YAML_DICT))
    monkeypatch.setattr(provisioner.provision_logging,
                        'setup_provision_logging',
                        lambda *a, **k: contextlib.nullcontext())
    teardown_mock = mock.MagicMock()
    monkeypatch.setattr(provisioner, 'teardown_cluster', teardown_mock)
    return teardown_mock


def _call_bulk_provision(
    tmp_path,
    provider_effect_guard_factory: common.ProviderEffectGuardFactory |
    None = None,
    *,
    cloud: clouds.Cloud | None = None,
    provider_create_idempotency_token: str | None = None,
    provider_create_account_id: str | None = None,
):
    return provisioner.bulk_provision(
        cloud=cloud or clouds.Kubernetes(),
        region=clouds.Region('us'),
        zones=None,
        cluster_name=resources_utils.ClusterName('c', 'c-on-cloud'),
        num_nodes=1,
        cluster_yaml='/fake/cluster.yaml',
        prev_cluster_ever_up=False,
        log_dir=str(tmp_path),
        provider_create_idempotency_token=(provider_create_idempotency_token),
        provider_create_account_id=provider_create_account_id,
        provider_effect_guard_factory=(provider_effect_guard_factory))


def _provider_negative_ack() -> dict:
    client_token = 'a' * 64
    return {
        'schema_version': 1,
        'provider': 'aws',
        'operation': 'RunInstances',
        'reason': 'capacity',
        'aws_account_id': '123456789012',
        'aws_principal_arn': 'arn:aws:sts::123456789012:assumed-role/test/run',
        'cluster_name_on_cloud': 'c-on-cloud',
        'requested_count': 1,
        'market': 'spot',
        'instance_type': 'g6.4xlarge',
        'region': 'us-east-1',
        'availability_zone': 'us-east-1a',
        'client_token': client_token,
        'invocations': [{
            'region': 'us-east-1',
            'availability_zone': 'us-east-1a',
            'initial_nonterminated_instance_ids': [],
            'resumed_instance_ids': [],
            'created_instance_ids': [],
            'successful_create_calls': 0,
            'ambiguous_create_calls': 0,
            'create_call_count': 1,
            'attempts': [{
                'provider_request_id': 'request-1',
                'error_code': 'InsufficientInstanceCapacity',
                'reason': 'capacity',
                'http_status_code': 500,
                'aws_account_id': '123456789012',
                'aws_principal_arn': 'arn:aws:sts::123456789012:assumed-role/test/run',
                'region': 'us-east-1',
                'availability_zone': 'us-east-1a',
                'subnet_id': 'subnet-a',
                'market': 'spot',
                'instance_type': 'g6.4xlarge',
                'cluster_name_on_cloud': 'c-on-cloud',
                'min_count': 1,
                'max_count': 1,
                'capacity_reservation_id': None,
                'client_token': client_token,
            }],
        }],
    }


def test_bulk_provision_does_not_teardown_on_pause(patched_bulk_provision,
                                                   monkeypatch, tmp_path):
    """A pause propagates without tearing down the kept resources."""
    paused = exceptions.ExecutionPausedError('Waiting on admission.',
                                             hint='resume later',
                                             retry_wait_seconds=5)
    monkeypatch.setattr(provisioner, '_bulk_provision',
                        mock.MagicMock(side_effect=paused))

    with pytest.raises(exceptions.ExecutionPausedError):
        _call_bulk_provision(tmp_path)

    patched_bulk_provision.assert_not_called()


def test_bulk_provision_does_not_teardown_ambiguous_provider_create(
        patched_bulk_provision, monkeypatch, tmp_path):
    paused = exceptions.ProviderCreateAmbiguousError(
        'RunInstances response lost',
        hint='replay same association and ClientToken',
        retry_wait_seconds=5)
    monkeypatch.setattr(provisioner, '_bulk_provision',
                        mock.MagicMock(side_effect=paused))

    with pytest.raises(exceptions.ProviderCreateAmbiguousError) as exc_info:
        _call_bulk_provision(tmp_path)

    assert exc_info.value is paused
    patched_bulk_provision.assert_not_called()


def test_bulk_provision_tears_down_on_ordinary_failure(patched_bulk_provision,
                                                       monkeypatch, tmp_path):
    """Negative control: an ordinary failure still tears down.

    Proves the test harness actually reaches the teardown branch, so the
    pause test above is meaningful rather than superfluous.
    """
    monkeypatch.setattr(
        provisioner, '_bulk_provision',
        mock.MagicMock(side_effect=RuntimeError('provisioning failed')))

    with pytest.raises(RuntimeError, match='provisioning failed'):
        _call_bulk_provision(tmp_path)

    patched_bulk_provision.assert_called_once()


def test_bulk_provision_retries_teardown_then_reraises_original_failure(
        patched_bulk_provision, monkeypatch, tmp_path):
    """Transient teardown failures are retried before failover continues."""
    provisioning_error = RuntimeError('provisioning failed')
    monkeypatch.setattr(provisioner, '_bulk_provision',
                        mock.MagicMock(side_effect=provisioning_error))
    patched_bulk_provision.side_effect = [
        RuntimeError('teardown failed once'),
        RuntimeError('teardown failed twice'),
        None,
    ]
    sleep_mock = mock.MagicMock()
    monkeypatch.setattr(provisioner.time, 'sleep', sleep_mock)

    with pytest.raises(RuntimeError, match='provisioning failed') as exc_info:
        _call_bulk_provision(tmp_path)

    assert exc_info.value is provisioning_error
    assert patched_bulk_provision.call_count == 3
    assert sleep_mock.call_args_list == [mock.call(5), mock.call(5)]


def test_bulk_provision_stops_failover_when_teardown_retries_exhausted(
        patched_bulk_provision, monkeypatch, tmp_path):
    """Failover stops after bounded teardown retries to avoid resource leaks."""
    monkeypatch.setattr(
        provisioner, '_bulk_provision',
        mock.MagicMock(side_effect=RuntimeError('provisioning failed')))
    teardown_error = RuntimeError('teardown still failing')
    patched_bulk_provision.side_effect = teardown_error
    sleep_mock = mock.MagicMock()
    monkeypatch.setattr(provisioner.time, 'sleep', sleep_mock)

    with pytest.raises(common.StopFailoverError,
                       match='resource leakage') as exc_info:
        _call_bulk_provision(tmp_path)

    assert exc_info.value.__cause__ is teardown_error
    assert patched_bulk_provision.call_count == 3
    assert sleep_mock.call_args_list == [mock.call(5), mock.call(5)]


def test_bulk_provision_skips_teardown_for_exact_provider_negative_ack(
        patched_bulk_provision, monkeypatch, tmp_path):
    receipt = _provider_negative_ack()
    rejected = common.ProviderCreateRejectedError('provider rejected create')
    rejected.provider_negative_ack = receipt
    monkeypatch.setattr(provisioner, '_bulk_provision',
                        mock.MagicMock(side_effect=rejected))

    with pytest.raises(common.ProviderCreateRejectedError) as exc_info:
        _call_bulk_provision(
            tmp_path,
            cloud=clouds.AWS(),
            provider_create_idempotency_token=receipt['client_token'],
            provider_create_account_id=receipt['aws_account_id'])

    assert exc_info.value.provider_negative_ack == receipt
    patched_bulk_provision.assert_not_called()


def test_bulk_provision_does_not_trust_malformed_provider_negative_ack(
        patched_bulk_provision, monkeypatch, tmp_path):
    receipt = _provider_negative_ack()
    receipt['aws_account_id'] = '210987654321'
    rejected = common.ProviderCreateRejectedError('untrusted rejection')
    rejected.provider_negative_ack = receipt
    monkeypatch.setattr(provisioner, '_bulk_provision',
                        mock.MagicMock(side_effect=rejected))

    with pytest.raises(common.ProviderCreateRejectedError):
        _call_bulk_provision(
            tmp_path,
            cloud=clouds.AWS(),
            provider_create_idempotency_token=receipt['client_token'],
            provider_create_account_id='123456789012')

    patched_bulk_provision.assert_called_once()


def test_bulk_provision_tears_down_on_ordinary_request_cancellation(
        patched_bulk_provision, monkeypatch, tmp_path):
    monkeypatch.setattr(
        provisioner, '_bulk_provision',
        mock.MagicMock(side_effect=exceptions.RequestCancelled('cancelled')))

    with pytest.raises(exceptions.RequestCancelled, match='cancelled'):
        _call_bulk_provision(tmp_path)

    patched_bulk_provision.assert_called_once()


def test_policy_bound_kubernetes_failure_defers_cleanup_to_replica_owner(
        patched_bulk_provision, monkeypatch, tmp_path):
    monkeypatch.setattr(
        provisioner, '_bulk_provision',
        mock.MagicMock(side_effect=RuntimeError('passive wait failed')))

    @contextlib.contextmanager
    def mutation_guard():
        yield

    with pytest.raises(exceptions.ReservedFillLaunchFenceError,
                       match='durable reserved-fill reconciliation'):
        _call_bulk_provision(tmp_path, mutation_guard)

    patched_bulk_provision.assert_not_called()


def test_policy_bound_kubernetes_preserves_terminal_fence_classification(
        patched_bulk_provision, monkeypatch, tmp_path):
    terminal = exceptions.ReservedFillLaunchFenceError('lost guard session')
    monkeypatch.setattr(provisioner, '_bulk_provision',
                        mock.MagicMock(side_effect=terminal))

    @contextlib.contextmanager
    def mutation_guard():
        yield

    with pytest.raises(exceptions.ReservedFillLaunchFenceError) as exc_info:
        _call_bulk_provision(tmp_path, mutation_guard)

    assert exc_info.value is terminal
    patched_bulk_provision.assert_not_called()


def test_policy_bound_kubernetes_preserves_provider_present_for_adjudication(
        patched_bulk_provision, monkeypatch, tmp_path):
    """The terminal PRESENT receipt never enters legacy teardown/failover."""
    terminal = exceptions.ReservedFillProviderPresentError(
        'Kueue admission timed out', ('ns/pod@uid',))
    monkeypatch.setattr(provisioner, '_bulk_provision',
                        mock.MagicMock(side_effect=terminal))

    @contextlib.contextmanager
    def mutation_guard():
        yield

    with pytest.raises(exceptions.ReservedFillProviderPresentError) as exc_info:
        _call_bulk_provision(tmp_path, mutation_guard)

    assert exc_info.value is terminal
    assert exc_info.value.provider_resource_ids == ('ns/pod@uid',)
    patched_bulk_provision.assert_not_called()
