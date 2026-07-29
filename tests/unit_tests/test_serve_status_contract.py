"""Characterization tests for the public SkyServe status contract."""

import pickle

import colorama
import pytest

from sky.serve import serve_state
from sky.serve import serve_statuses


@pytest.mark.parametrize(
    ('status_type', 'expected_values'),
    [
        (
            serve_state.ReplicaStatus,
            [
                'PENDING',
                'PROVISIONING',
                'STARTING',
                'READY',
                'NOT_READY',
                'SHUTTING_DOWN',
                'FAILED',
                'FAILED_INITIAL_DELAY',
                'FAILED_PROBING',
                'FAILED_PROVISION',
                'FAILED_CLEANUP',
                'PREEMPTED',
                'UNKNOWN',
            ],
        ),
        (
            serve_state.ServiceStatus,
            [
                'CONTROLLER_INIT',
                'REPLICA_INIT',
                'CONTROLLER_FAILED',
                'READY',
                'SHUTTING_DOWN',
                'FAILED',
                'FAILED_CLEANUP',
                'NO_REPLICA',
            ],
        ),
    ],
)
def test_status_values_order_module_and_pickle(status_type, expected_values):
    assert [status.value for status in status_type] == expected_values
    assert status_type.__module__ == 'sky.serve.serve_state'
    assert pickle.loads(pickle.dumps(status_type)) is status_type
    for status in status_type:
        assert pickle.loads(pickle.dumps(status)) is status


def test_replica_status_classifications_and_scale_down_order():
    assert serve_state.ReplicaStatus.failed_statuses() == [
        serve_state.ReplicaStatus.FAILED,
        serve_state.ReplicaStatus.FAILED_CLEANUP,
        serve_state.ReplicaStatus.FAILED_INITIAL_DELAY,
        serve_state.ReplicaStatus.FAILED_PROBING,
        serve_state.ReplicaStatus.FAILED_PROVISION,
        serve_state.ReplicaStatus.UNKNOWN,
    ]
    assert serve_state.ReplicaStatus.terminal_statuses() == [
        serve_state.ReplicaStatus.SHUTTING_DOWN,
        serve_state.ReplicaStatus.PREEMPTED,
        serve_state.ReplicaStatus.UNKNOWN,
        *serve_state.ReplicaStatus.failed_statuses(),
    ]
    assert serve_state.ReplicaStatus.scale_down_decision_order() == [
        serve_state.ReplicaStatus.PENDING,
        serve_state.ReplicaStatus.PROVISIONING,
        serve_state.ReplicaStatus.STARTING,
        serve_state.ReplicaStatus.NOT_READY,
        serve_state.ReplicaStatus.READY,
    ]


def test_service_status_classifications_and_derivation():
    assert serve_state.ServiceStatus.failed_statuses() == [
        serve_state.ServiceStatus.CONTROLLER_FAILED,
        serve_state.ServiceStatus.FAILED_CLEANUP,
    ]
    assert serve_state.ServiceStatus.terminal_statuses() == [
        serve_state.ServiceStatus.CONTROLLER_FAILED,
        serve_state.ServiceStatus.FAILED_CLEANUP,
        serve_state.ServiceStatus.SHUTTING_DOWN,
    ]
    assert serve_state.ServiceStatus.replica_launch_blocking_statuses() == [
        serve_state.ServiceStatus.FAILED_CLEANUP,
        serve_state.ServiceStatus.SHUTTING_DOWN,
    ]
    assert serve_state.ServiceStatus.from_replica_statuses(
        []) is serve_state.ServiceStatus.NO_REPLICA
    assert serve_state.ServiceStatus.from_replica_statuses([
        serve_state.ReplicaStatus.PROVISIONING
    ]) is serve_state.ServiceStatus.REPLICA_INIT
    assert serve_state.ServiceStatus.from_replica_statuses([
        serve_state.ReplicaStatus.FAILED_PROVISION
    ]) is serve_state.ServiceStatus.FAILED
    assert serve_state.ServiceStatus.from_replica_statuses([
        serve_state.ReplicaStatus.FAILED,
        serve_state.ReplicaStatus.READY,
    ]) is serve_state.ServiceStatus.READY


def test_status_colored_strings():
    assert serve_state.ReplicaStatus.READY.colored_str() == (
        f'{colorama.Fore.GREEN}READY{colorama.Style.RESET_ALL}')
    assert serve_state.ServiceStatus.FAILED.colored_str() == (
        f'{colorama.Fore.RED}FAILED{colorama.Style.RESET_ALL}')


def test_status_facade_uses_direct_aliases():
    assert serve_state.ReplicaStatus is serve_statuses.ReplicaStatus
    assert serve_state.ServiceStatus is serve_statuses.ServiceStatus
