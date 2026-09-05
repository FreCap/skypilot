"""Tests for failure-isolated non-pool provider reconciliation."""
# pylint: disable=protected-access

import io
import json
import os
import pathlib
import signal
import subprocess
import sys
import threading
import time
import types
from unittest import mock
import uuid

import pytest

from sky import exceptions
from sky.events import api_models as event_api_models
from sky.provision import common as provision_common
from sky.serve import non_pool_launch_reconciliation as reconciliation
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
from sky.serve import replica_info
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import resource_actions
from sky.utils import subprocess_utils


def test_empty_observation_lane_close_does_not_consume_a_deadline(monkeypatch):
    """A lane without workers has no join or process-reap horizon."""
    lane = reconciliation.OneShotProviderObservationLane()
    monotonic = mock.Mock(side_effect=AssertionError('clock must not be read'))
    monkeypatch.setattr(reconciliation.time, 'monotonic', monotonic)

    lane.close()

    monotonic.assert_not_called()
    assert not lane.mutation_is_allowed


def test_one_shot_provider_observation_lane_is_bounded() -> None:
    lane = reconciliation.OneShotProviderObservationLane()
    release = threading.Event()
    all_started = threading.Event()
    started = 0
    started_lock = threading.Lock()

    def _observe() -> str:
        nonlocal started
        with started_lock:
            started += 1
            if started == lane.MAX_CONCURRENT:
                all_started.set()
        assert release.wait(timeout=5)
        return 'observed'

    for replica_id in range(lane.MAX_CONCURRENT):
        assert lane.schedule((replica_id, f'record-{replica_id}'), _observe)
    assert all_started.wait(timeout=5)
    assert lane.available_slots == 0
    assert not lane.schedule((lane.MAX_CONCURRENT, 'overflow'), _observe)

    release.set()
    for worker in tuple(lane._workers.values()):
        worker.join(timeout=5)
    completions = lane.take_completed()
    assert len(completions) == lane.MAX_CONCURRENT
    assert all(completion.result == 'observed' for completion in completions)
    assert all(completion.error is None for completion in completions)
    assert not lane.has_work()


def test_one_shot_provider_observation_lane_reports_worker_exception() -> None:
    lane = reconciliation.OneShotProviderObservationLane()
    key = (3, 'record-3')

    def _fail() -> None:
        raise RuntimeError('provider read failed')

    assert lane.schedule(key, _fail)
    lane._workers[key].join(timeout=5)

    completions = lane.take_completed()
    assert len(completions) == 1
    completion = completions[0]
    assert completion.key == key
    assert completion.result is None
    assert isinstance(completion.error, RuntimeError)
    assert completion.formatted_error == 'RuntimeError: provider read failed'
    assert not lane.has_work()


def test_one_shot_provider_observation_lane_reserves_key_atomically(
        monkeypatch: pytest.MonkeyPatch) -> None:
    lane = reconciliation.OneShotProviderObservationLane()
    constructor_barrier = threading.Barrier(2)
    operation_release = threading.Event()
    real_thread = reconciliation.thread_utils.SafeThread

    class RacingConstructionThread(real_thread):

        def __init__(self, *args, **kwargs):
            constructor_barrier.wait(timeout=5)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(reconciliation.thread_utils, 'SafeThread',
                        RacingConstructionThread)
    operation_calls = 0
    operation_lock = threading.Lock()

    def _observe() -> str:
        nonlocal operation_calls
        with operation_lock:
            operation_calls += 1
        assert operation_release.wait(timeout=5)
        return 'observed'

    results: list[bool] = []

    def _schedule() -> None:
        results.append(lane.schedule((3, 'record-3'), _observe))

    callers = [threading.Thread(target=_schedule) for _ in range(2)]
    for caller in callers:
        caller.start()
    for caller in callers:
        caller.join(timeout=5)
    assert not any(caller.is_alive() for caller in callers)
    assert sorted(results) == [False, True]
    operation_release.set()
    for worker in tuple(lane._workers.values()):
        worker.join(timeout=5)
    assert len(lane.take_completed()) == 1
    assert operation_calls == 1


@pytest.mark.skipif(not hasattr(os, 'killpg'),
                    reason='requires POSIX process groups')
def test_provider_census_worker_kills_stubborn_descendant_at_deadline(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    worker_pid_path = tmp_path / 'worker.pid'
    descendant_pid_path = tmp_path / 'descendant.pid'
    descendant_program = (
        'import os, pathlib, signal, sys, time; '
        'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
        'pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); '
        'time.sleep(60)')
    command = [
        sys.executable,
        '-c',
        ('import os, pathlib, signal, subprocess, sys, time; '
         'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
         'subprocess.Popen([sys.executable, "-c", sys.argv[3], sys.argv[2]]); '
         'pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); '
         'time.sleep(60)'),
        str(worker_pid_path),
        str(descendant_pid_path),
        descendant_program,
    ]
    monkeypatch.setattr(reconciliation, '_provider_census_worker_command',
                        lambda: command)

    with pytest.raises(TimeoutError, match='provider census'):
        reconciliation._run_paid_provider_census_worker(  # pylint: disable=protected-access
            {
                'cloud': 'gcp',
                'cluster_name': 'svc-3',
                'provider_identity': {
                    'cluster_name_on_cloud': 'svc-3-abc',
                    'project_id': 'boltz-498512',
                    'zone': 'us-east4-a',
                },
                'protocol_version': 1,
            },
            deadline_monotonic=time.monotonic() + 1.0)

    for pid_path in (worker_pid_path, descendant_pid_path):
        process_id = int(pid_path.read_text())
        with pytest.raises(ProcessLookupError):
            os.kill(process_id, 0)


@pytest.mark.skipif(not hasattr(os, 'killpg'),
                    reason='requires POSIX process groups')
def test_provider_census_worker_reaps_descendant_after_leader_response(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    descendant_pid_path = tmp_path / 'descendant.pid'
    descendant_program = (
        'import os, pathlib, signal, sys, time; '
        'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
        'pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); '
        'time.sleep(60)')
    command = [
        sys.executable,
        '-c',
        ('import json, pathlib, subprocess, sys, time; '
         'subprocess.Popen([sys.executable, "-c", sys.argv[2], sys.argv[1]], '
         'stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, '
         'stderr=subprocess.DEVNULL); '
         'p = pathlib.Path(sys.argv[1]); deadline = time.monotonic() + 5; '
         'exec("while not p.exists() and time.monotonic() < deadline:'
         '\\n time.sleep(0.01)"); '
         'print(json.dumps({"ok": True, "result": []}), flush=True)'),
        str(descendant_pid_path),
        descendant_program,
    ]
    monkeypatch.setattr(reconciliation, '_provider_census_worker_command',
                        lambda: command)
    lane = reconciliation.OneShotProviderObservationLane()
    key = (3, 'record-3')

    assert lane.schedule(
        key, lambda: reconciliation._run_paid_provider_census_worker(
            {
                'cloud': 'aws',
                'protocol_version': 1,
                'provider_identity': {},
            },
            deadline_monotonic=time.monotonic() + 10))
    lane._workers[key].join(timeout=10)
    completion, = lane.take_completed()

    assert isinstance(completion.error, RuntimeError)
    assert 'live descendant' in str(completion.error)
    assert descendant_pid_path.exists()
    with pytest.raises(ProcessLookupError):
        os.kill(int(descendant_pid_path.read_text()), 0)
    assert lane._process_registry.process_count == 0
    lane.close()


def test_process_group_runner_closes_all_parent_pipes(
        monkeypatch: pytest.MonkeyPatch) -> None:
    process = mock.Mock(pid=123, returncode=0, args=['worker'])
    process.stdin = io.StringIO()
    process.stdout = io.StringIO()
    process.stderr = io.StringIO()
    process.communicate.return_value = ('result', 'diagnostic')
    process.poll.return_value = 0
    monkeypatch.setattr(subprocess_utils.subprocess, 'Popen',
                        mock.Mock(return_value=process))
    monkeypatch.setattr(subprocess_utils, '_process_group_exists',
                        lambda _process: False)

    result = subprocess_utils.run_in_process_group(
        ['worker'],
        deadline_monotonic=time.monotonic() + 5,
        term_grace_seconds=0.1,
        reap_grace_seconds=0.1,
        input_text='request')

    assert result.stdout == 'result'
    assert process.stdin.closed
    assert process.stdout.closed
    assert process.stderr.closed


@pytest.mark.skipif(not hasattr(os, 'killpg'),
                    reason='requires POSIX process groups')
def test_provider_census_aggregate_shutdown_has_one_shared_horizon(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reconciliation,
                        '_PROVIDER_CENSUS_WORKER_TERM_GRACE_SECONDS', 0.05)
    monkeypatch.setattr(reconciliation,
                        '_PROVIDER_CENSUS_WORKER_REAP_GRACE_SECONDS', 1.0)
    command = [
        sys.executable, '-c',
        ('import signal, time; '
         'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
         'time.sleep(60)')
    ]
    owned = tuple(
        subprocess.Popen(  # pylint: disable=consider-using-with
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
            start_new_session=True) for _ in range(
                reconciliation.OneShotProviderObservationLane.MAX_CONCURRENT))
    try:
        started = time.monotonic()

        subprocess_utils.terminate_and_reap_process_groups(
            owned,
            term_grace_seconds=(
                reconciliation._PROVIDER_CENSUS_WORKER_TERM_GRACE_SECONDS),
            reap_grace_seconds=(
                reconciliation._PROVIDER_CENSUS_WORKER_REAP_GRACE_SECONDS))

        assert time.monotonic() - started < 2.0
        assert all(process.returncode is not None for process in owned)
        assert all(not subprocess_utils._process_group_exists(process)
                   for process in owned)
    finally:
        for process in owned:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)


def test_provider_census_worker_env_keeps_cloud_credentials_only(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('SKYPILOT_DB_CONNECTION_URI', 'postgresql://secret')
    monkeypatch.setenv('SKYPILOT_SERVICE_ACCOUNT_TOKEN', 'request-secret')
    monkeypatch.setenv('SKYPILOT_SERVE_CONTROLLER_AUTH_TOKEN',
                       'controller-secret')
    monkeypatch.setenv('PGPASSWORD', 'database-secret')
    monkeypatch.setenv('AWS_SESSION_TOKEN', 'aws-provider-secret')
    monkeypatch.setenv('GOOGLE_APPLICATION_CREDENTIALS', '/gcp/identity.json')
    monkeypatch.setenv('GITHUB_TOKEN', 'unrelated-secret')
    monkeypatch.setenv('OPENAI_API_KEY', 'unrelated-secret')

    aws_env = reconciliation._provider_census_worker_env('aws')  # pylint: disable=protected-access
    gcp_env = reconciliation._provider_census_worker_env('gcp')  # pylint: disable=protected-access

    for child_env in (aws_env, gcp_env):
        assert 'SKYPILOT_DB_CONNECTION_URI' not in child_env
        assert 'SKYPILOT_SERVICE_ACCOUNT_TOKEN' not in child_env
        assert 'SKYPILOT_SERVE_CONTROLLER_AUTH_TOKEN' not in child_env
        assert 'PGPASSWORD' not in child_env
        assert 'GITHUB_TOKEN' not in child_env
        assert 'OPENAI_API_KEY' not in child_env
    assert aws_env['AWS_SESSION_TOKEN'] == 'aws-provider-secret'
    assert 'GOOGLE_APPLICATION_CREDENTIALS' not in aws_env
    assert gcp_env['GOOGLE_APPLICATION_CREDENTIALS'] == '/gcp/identity.json'
    assert 'AWS_SESSION_TOKEN' not in gcp_env


@pytest.mark.skipif(not hasattr(os, 'killpg'),
                    reason='requires POSIX process groups')
def test_observation_lane_close_kills_registered_provider_process(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    worker_pid_path = tmp_path / 'worker.pid'
    command = [
        sys.executable,
        '-c',
        ('import os, pathlib, signal, sys, time; '
         'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
         'pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); '
         'time.sleep(60)'),
        str(worker_pid_path),
    ]
    monkeypatch.setattr(reconciliation, '_provider_census_worker_command',
                        lambda: command)
    lane = reconciliation.OneShotProviderObservationLane()

    def _observe() -> object:
        return reconciliation._run_paid_provider_census_worker(  # pylint: disable=protected-access
            {
                'cloud': 'gcp',
                'cluster_name': 'svc-3',
                'provider_identity': {
                    'cluster_name_on_cloud': 'svc-3-abc',
                    'project_id': 'boltz-498512',
                    'zone': 'us-east4-a',
                },
                'protocol_version': 1,
            },
            deadline_monotonic=time.monotonic() + 60)

    assert lane.schedule((3, 'record-3'), _observe)
    deadline = time.monotonic() + 5
    while not worker_pid_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert worker_pid_path.exists()

    lane.close()

    assert not lane.has_work()
    with pytest.raises(ProcessLookupError):
        os.kill(int(worker_pid_path.read_text()), 0)


def test_lane_close_between_evidence_and_projection_fences_second_mutation(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """A surviving Python worker cannot cross a post-close DB boundary."""
    lane = reconciliation.OneShotProviderObservationLane()
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    observation = reconciliation.ProviderObservation(
        ordinary_launch_binding.ProviderEvidence.ABSENT, {'result': 'ABSENT'})
    project = mock.Mock()
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_reconciliation_ready',
                        lambda *_args: True)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_absence_is_recorded',
                        lambda *_args: False)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_terminal_provider_absence_payload',
                        lambda *_args: None)
    monkeypatch.setattr(reconciliation, 'observe_provider',
                        lambda *_args, **_kwargs: observation)
    record = mock.Mock(side_effect=lambda *_args, **_kwargs: lane.close())
    monkeypatch.setattr(reconciliation.request_postgres,
                        'record_bound_non_pool_provider_evidence', record)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'project_bound_non_pool_provider_absence', project)

    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='lifecycle authority'):
        reconciliation.reconcile(
            context,
            _paid_replica('aws'),
            object(),
            mock.Mock(),
            continue_guard=lambda: lane.mutation_is_allowed)

    record.assert_called_once()
    project.assert_not_called()


def _context(
    kind: ordinary_launch_binding.NonPoolLaunchProfileKind,
) -> ordinary_launch_binding.BoundNonPoolLaunchContext:
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        kind,
        authorization_reference=f'test:{kind.value}',
        authorization_generation=1,
        authorization_payload={'kind': kind.value})
    return ordinary_launch_binding.BoundNonPoolLaunchContext(
        association_id=uuid.UUID('11111111-1111-4111-8111-111111111111'),
        request_id='request-1',
        service_name='svc',
        replica_id=3,
        replica_record_id=uuid.UUID('22222222-2222-4222-8222-222222222222'),
        launch_generation=1,
        input_digest='a' * 64,
        profile=profile,
        capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        capability_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=1)


def _reserved_replica() -> types.SimpleNamespace:
    context = 'on-prem-a'
    physical_uid = 'physical-cluster-a'
    pool_key = reserved_capacity_broker.make_pool_key(
        context,
        'L4',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid=physical_uid)
    return types.SimpleNamespace(
        cluster_name='svc-3',
        reserved_fill=True,
        reserved_fill_pool_key=pool_key,
        reserved_fill_service_generation=7,
        reserved_fill_physical_cluster_uid=physical_uid,
        reserved_fill_kubernetes_context=context,
        location={
            'cloud': 'Kubernetes',
            'region': context,
            'accelerators': {
                'L4': 1,
            },
        },
        resources_override={
            'cloud': 'Kubernetes',
            'region': context,
            'accelerators': {
                'L4': 1,
            },
        })


def _paid_replica(cloud: str) -> types.SimpleNamespace:
    payload = {
        'accelerators': [['l4', 1]],
        'cloud': cloud,
        'instance_type': ('g6.2xlarge' if cloud == 'aws' else 'g2-standard-4'),
        'num_nodes': 1,
        'region': 'us-east-2' if cloud == 'aws' else 'us-east4',
        'use_spot': True,
        'version': 2 if cloud == 'aws' else 1,
        'workspace': 'workspace-a',
        'zone': 'us-east-2c' if cloud == 'aws' else 'us-east4-a',
    }
    if cloud == 'aws':
        payload['provider_identity'] = {'aws_account_id': '096766144388'}
    return types.SimpleNamespace(cluster_name='svc-3',
                                 paid_capacity_pool_key=json.dumps(
                                     payload,
                                     sort_keys=True,
                                     separators=(',', ':')))


def _paid_cleanup_replica(down_status,) -> types.SimpleNamespace:
    replica = _paid_replica('aws')
    replica.replica_id = 3
    replica.replica_record_id = '22222222-2222-4222-8222-222222222222'
    replica.reserved_fill = False
    replica.is_zero_cost = False
    replica.is_spot = True
    replica.service_job_id = None
    replica.zero_cost_materialization_sequence = None
    replica.status_property = types.SimpleNamespace(
        sky_launch_status=reconciliation.common_utils.ProcessStatus.INTERRUPTED,
        sky_down_status=down_status,
        service_ready_now=False,
        is_scale_down=True,
        preempted=False,
        purged=False,
        failed_spot_availability=False,
        wait_for_idle_before_termination=False,
        drain_cap_seconds=0,
        drain_started_at=None,
        logical_retirement_version=None,
        logical_retirement_controller_epoch=None,
        logical_retirement_generation=None,
        logical_retirement_target_capacity=None,
        logical_retirement_confirmed_generation=None,
        logical_retirement_bounded_deadline=False,
        logical_retirement_committed=False)
    return replica


def test_paid_teardown_phase_adapter_uses_n_minus_one_safe_failed_encoding(
) -> None:
    info = _paid_cleanup_replica(
        reconciliation.common_utils.ProcessStatus.RUNNING)

    assert ordinary_launch_binding.provider_present_teardown_phase(info) is (
        ordinary_launch_binding.ProviderPresentTeardownPhase.SUBMISSION_RUNNING)
    ordinary_launch_binding.transition_provider_present_teardown_phase(
        info,
        expected=(ordinary_launch_binding.ProviderPresentTeardownPhase.
                  SUBMISSION_RUNNING),
        target=(ordinary_launch_binding.ProviderPresentTeardownPhase.
                ABSENCE_OBSERVATION_PENDING))

    assert ordinary_launch_binding.provider_present_teardown_phase(info) is (
        ordinary_launch_binding.ProviderPresentTeardownPhase.
        ABSENCE_OBSERVATION_PENDING)
    assert info.status_property.sky_down_status is (
        reconciliation.common_utils.ProcessStatus.FAILED)
    # FAILED was already a provider-present cleanup marker in the previous
    # writer, so rollback redrives exact cleanup rather than retiring the row.
    assert ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
        info)

    ordinary_launch_binding.transition_provider_present_teardown_phase(
        info,
        expected=(ordinary_launch_binding.ProviderPresentTeardownPhase.
                  ABSENCE_OBSERVATION_PENDING),
        target=(ordinary_launch_binding.ProviderPresentTeardownPhase.
                SUBMISSION_SCHEDULED))
    assert info.status_property.sky_down_status is (
        reconciliation.common_utils.ProcessStatus.SCHEDULED)


def _association_for_context(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    pool_key: str,
) -> dict:
    profile = context.profile
    return {
        'association_id': context.association_id,
        'request_id': context.request_id,
        'service_name': context.service_name,
        'replica_id': context.replica_id,
        'replica_record_id': context.replica_record_id,
        'launch_generation': context.launch_generation,
        'input_digest': context.input_digest,
        'cluster_name': 'svc-3',
        'tenant_scope': 'tenant-a',
        'paid_capacity_pool_key': pool_key,
        'profile_kind': profile.kind.value,
        'profile_version': profile.version,
        'profile_digest': profile.digest,
        'capability_cohort_epoch': context.capability_cohort_epoch,
        'capability_profile_set_digest': context.capability_profile_set_digest,
        'receipt_protocol_version': context.receipt_protocol_version,
        'authorization_kind': profile.authorization_kind.value,
        'authorization_reference': profile.authorization_reference,
        'authorization_generation': profile.authorization_generation,
        'authorization_digest': profile.authorization_digest,
    }


def _aws_census_scope(
    provider_identity: dict,
    *,
    credential_profile: str = 'prod',
) -> reconciliation.request_postgres.BoundAwsProviderCensusScope:
    return reconciliation.request_postgres.BoundAwsProviderCensusScope(
        provider_identity=provider_identity,
        credential_profile=credential_profile)


@pytest.mark.parametrize(('presence', 'expected'),
                         [(reserved_capacity.PhysicalReplicaPresence.PRESENT,
                           ordinary_launch_binding.ProviderEvidence.PRESENT),
                          (reserved_capacity.PhysicalReplicaPresence.ABSENT,
                           ordinary_launch_binding.ProviderEvidence.ABSENT),
                          (reserved_capacity.PhysicalReplicaPresence.UNPROVEN,
                           ordinary_launch_binding.ProviderEvidence.UNKNOWN)])
def test_reserved_fill_provider_observation_is_closed(
        monkeypatch: pytest.MonkeyPatch,
        presence: reserved_capacity.PhysicalReplicaPresence,
        expected: ordinary_launch_binding.ProviderEvidence) -> None:
    replica = _reserved_replica()
    monkeypatch.setattr(
        reserved_capacity, 'get_kubernetes_physical_cluster_uid', lambda
        _context, force_refresh: replica.reserved_fill_physical_cluster_uid)
    monkeypatch.setattr(reserved_capacity, 'probe_physical_replica_presence',
                        lambda *_args, **_kwargs: presence)

    observed = reconciliation.observe_provider(
        _context(
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL),
        replica)

    assert observed.evidence == expected
    assert observed.payload['result'] == presence.value
    assert observed.payload['physical_cluster_uid'] == (
        replica.reserved_fill_physical_cluster_uid)


@pytest.mark.parametrize(('initial_launch', 'initial_down', 'expected_down'), [
    (reconciliation.common_utils.ProcessStatus.INTERRUPTED,
     reconciliation.common_utils.ProcessStatus.RUNNING,
     reconciliation.common_utils.ProcessStatus.RUNNING),
    (reconciliation.common_utils.ProcessStatus.SCHEDULED, None,
     reconciliation.common_utils.ProcessStatus.SCHEDULED),
])
def test_reserved_fill_exact_absence_projects_one_cleanup_marker(
        initial_launch: reconciliation.common_utils.ProcessStatus,
        initial_down: reconciliation.common_utils.ProcessStatus | None,
        expected_down: reconciliation.common_utils.ProcessStatus) -> None:
    status_property = replica_info.ReplicaStatusProperty(
        sky_launch_status=initial_launch,
        sky_down_status=initial_down,
        service_ready_now=True,
        is_scale_down=False,
        preempted=True,
        purged=True,
        failed_spot_availability=True,
        drain_cap_seconds=60,
        drain_started_at=123.0,
        wait_for_idle_before_termination=True,
        logical_retirement_version=1,
        logical_retirement_controller_epoch='epoch',
        logical_retirement_generation=2,
        logical_retirement_target_capacity=3,
        logical_retirement_confirmed_generation=4,
        logical_retirement_bounded_deadline=True,
        logical_retirement_committed=True)
    projection = types.SimpleNamespace(
        provider_evidence=ordinary_launch_binding.ProviderEvidence.ABSENT,
        context=_context(
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL),
        pre_effect_terminal=False,
        service_job_id=None,
        locked_replica_info=types.SimpleNamespace(
            status_property=status_property),
        paid_capacity_pool_key=None,
        # Reserved-fill cancellation retains the pre-existing reserved
        # reducer semantics instead of acquiring the paid-only GCP mapping.
        status=types.SimpleNamespace(value='CANCELLED'),
        cause=types.SimpleNamespace(value='explicit_cancel'))

    result = reconciliation.apply_exact_provider_absence_replica_projection(
        projection)

    assert result == reconciliation.ProviderAbsenceReplicaProjection(
        paid_capacity_pool_key=None, paid_capacity_outcome=None)
    assert status_property.sky_launch_status is (
        reconciliation.common_utils.ProcessStatus.INTERRUPTED)
    assert status_property.sky_down_status is expected_down
    assert status_property.service_ready_now is False
    assert status_property.is_scale_down is True
    assert status_property.preempted is False
    assert status_property.purged is False
    assert status_property.failed_spot_availability is False
    assert status_property.drain_cap_seconds == 0
    assert status_property.drain_started_at is None
    assert status_property.wait_for_idle_before_termination is False
    assert status_property.logical_retirement_version is None
    assert status_property.logical_retirement_controller_epoch is None
    assert status_property.logical_retirement_generation is None
    assert status_property.logical_retirement_target_capacity is None
    assert status_property.logical_retirement_confirmed_generation is None
    assert status_property.logical_retirement_bounded_deadline is False
    assert status_property.logical_retirement_committed is False


def test_retargeted_reserved_fill_context_is_replaced(
        monkeypatch: pytest.MonkeyPatch) -> None:
    replica = _reserved_replica()
    monkeypatch.setattr(reserved_capacity,
                        'get_kubernetes_physical_cluster_uid',
                        lambda _context, force_refresh: 'replacement-uid')
    probe = monkeypatch.setattr(
        reserved_capacity, 'probe_physical_replica_presence',
        lambda *_args, **_kwargs: pytest.fail('replacement must not be probed'))
    del probe

    observed = reconciliation.observe_provider(
        _context(
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL),
        replica)

    assert observed.evidence == ordinary_launch_binding.ProviderEvidence.REPLACED
    assert observed.payload[
        'observed_physical_cluster_uid'] == 'replacement-uid'


def test_post_teardown_absence_receipt_reuses_exact_provider_read(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL)
    replica = _reserved_replica()
    authority = object()
    projector = lambda *_args: True
    receipt = reserved_capacity.ProtocolV2PhysicalAbsenceReceipt(
        cleanup_fence=reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context=replica.reserved_fill_kubernetes_context,
            physical_cluster_uid=replica.reserved_fill_physical_cluster_uid),
        cluster_name=replica.cluster_name)
    calls = []
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_reconciliation_ready',
                        lambda *_args: True)
    monkeypatch.setattr(
        reconciliation, 'observe_provider', lambda *_args: pytest.fail(
            'post-teardown receipt must prevent another provider read'))
    monkeypatch.setattr(
        reserved_capacity, 'get_kubernetes_physical_cluster_uid', lambda *_args,
        **_kwargs: pytest.fail('receipt must prevent another UID read'))
    monkeypatch.setattr(
        reserved_capacity, 'probe_physical_replica_presence', lambda *_args, **
        _kwargs: pytest.fail('receipt must prevent another Pod read'))
    monkeypatch.setattr(reconciliation.request_postgres,
                        'record_bound_non_pool_provider_evidence',
                        lambda *_args: calls.append('record'))
    monkeypatch.setattr(reconciliation.request_postgres,
                        'project_bound_non_pool_provider_absence',
                        lambda *_args, **_kwargs: calls.append('project'))

    observed = reconciliation.reconcile_post_teardown_absence(
        context, replica, authority, projector, receipt)

    assert observed.evidence == ordinary_launch_binding.ProviderEvidence.ABSENT
    assert observed.payload == {
        'association_id': str(context.association_id),
        'cluster_name': replica.cluster_name,
        'kubernetes_context': replica.reserved_fill_kubernetes_context,
        'physical_cluster_uid': replica.reserved_fill_physical_cluster_uid,
        'probe_contract': 'kubernetes-physical-replica-presence-v1',
        'profile_kind': 'RESERVED_FILL',
        'replica_record_id': str(context.replica_record_id),
        'result': 'ABSENT',
    }
    assert calls == ['record', 'project']


@pytest.mark.parametrize('receipt', [
    reserved_capacity.ProtocolV2PhysicalAbsenceReceipt(
        cleanup_fence=reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='wrong-context',
            physical_cluster_uid='physical-cluster-a'),
        cluster_name='svc-3'),
    reserved_capacity.ProtocolV2PhysicalAbsenceReceipt(
        cleanup_fence=reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='on-prem-a', physical_cluster_uid='wrong-uid'),
        cluster_name='svc-3'),
    reserved_capacity.ProtocolV2PhysicalAbsenceReceipt(
        cleanup_fence=reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='on-prem-a',
            physical_cluster_uid='physical-cluster-a'),
        cluster_name='other-replica'),
])
def test_post_teardown_absence_receipt_rejects_wrong_identity(
        monkeypatch: pytest.MonkeyPatch,
        receipt: reserved_capacity.ProtocolV2PhysicalAbsenceReceipt) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_reconciliation_ready',
                        lambda *_args: True)
    monkeypatch.setattr(
        reconciliation.request_postgres,
        'record_bound_non_pool_provider_evidence', lambda *_args: pytest.fail(
            'mismatched receipt must not reach durable evidence'))

    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='does not match the exact'):
        reconciliation.reconcile_post_teardown_absence(context,
                                                       _reserved_replica(),
                                                       object(),
                                                       lambda *_args: True,
                                                       receipt)


def test_profile_without_durable_provider_uid_remains_unknown(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        reserved_capacity, 'get_kubernetes_physical_cluster_uid',
        lambda *_args, **_kwargs: pytest.fail('no provider UID may be guessed'))
    replica = types.SimpleNamespace(cluster_name='svc-3')

    observed = reconciliation.observe_provider(
        _context(
            ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID),
        replica)

    assert observed.evidence == ordinary_launch_binding.ProviderEvidence.UNKNOWN
    assert observed.payload['reason'] == 'profile-has-no-durable-provider-uid'


@pytest.mark.parametrize(('instances', 'expected'), [
    ([], ordinary_launch_binding.ProviderEvidence.ABSENT),
    ([{
        'availability_zone': 'us-east-2c',
        'client_token': 'a' * 64,
        'cluster_name_on_cloud': 'svc-3-abc',
        'instance_id': 'i-0123456789abcdef0',
        'instance_type': 'g6.2xlarge',
        'market': 'spot',
        'state': 'running',
    }], ordinary_launch_binding.ProviderEvidence.PRESENT),
])
@pytest.mark.parametrize(
    'profile_kind',
    (ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
     ordinary_launch_binding.NonPoolLaunchProfileKind.
     UNKNOWN_CAPACITY_REPLACEMENT))
def test_aws_paid_observation_uses_exact_client_token_scope(
        monkeypatch: pytest.MonkeyPatch, instances, profile_kind,
        expected: ordinary_launch_binding.ProviderEvidence) -> None:
    context = _context(profile_kind)
    identity = {
        'aws_account_id': '096766144388',
        'client_token': 'a' * 64,
        'cluster_name_on_cloud': 'svc-3-abc',
        'credential_profile': None,
        'instance_type': 'g6.2xlarge',
        'num_nodes': 1,
        'region': 'us-east-2',
        'use_spot': True,
        'workspace': 'workspace-a',
        'zone': 'us-east-2c',
    }
    scope = _aws_census_scope(identity)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_aws_provider_census_scope',
                        lambda *_args: scope)
    calls = []
    monkeypatch.setattr(
        reconciliation, '_query_aws_paid_provider_census_isolated',
        lambda *_args, **_kwargs: calls.append(
            'census') or [dict(instance) for instance in instances])
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_aws_provider_absence_is_settled',
                        lambda *_args: True)
    monkeypatch.setattr(reconciliation.time, 'sleep', lambda _seconds: None)

    observed = reconciliation.observe_provider(context, _paid_replica('aws'),
                                               'authority')

    assert observed.evidence is expected
    assert observed.payload['provider_identity'] == identity
    assert observed.payload['instances'] == instances
    assert calls == (['census', 'census'] if not instances else ['census'])


def test_aws_empty_census_before_settle_horizon_is_unknown(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    scope = _aws_census_scope({'region': 'us-east-2'})
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_aws_provider_census_scope',
                        lambda *_args: scope)
    monkeypatch.setattr(reconciliation,
                        '_query_aws_paid_provider_census_isolated',
                        lambda *_args, **_kwargs: [])
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_aws_provider_absence_is_settled',
                        lambda *_args: False)

    observed = reconciliation.observe_provider(context, _paid_replica('aws'),
                                               'authority')

    assert observed.evidence is ordinary_launch_binding.ProviderEvidence.UNKNOWN
    assert observed.payload['reason'] == 'aws-create-settling'


def test_aws_paid_census_uses_retained_profile_account_and_client_token(
        monkeypatch: pytest.MonkeyPatch) -> None:
    identity = {
        'aws_account_id': '096766144388',
        'client_token': 'a' * 64,
        'cluster_name_on_cloud': 'svc-3-abc',
        'credential_profile': None,
        'instance_type': 'g6.2xlarge',
        'num_nodes': 1,
        'region': 'us-east-2',
        'use_spot': True,
        'workspace': 'workspace-a',
        'zone': 'us-east-2c',
    }
    calls = []

    class _Paginator:
        """Scripted EC2 paginator."""

        def paginate(self, **kwargs):
            calls.append(('paginate', kwargs))
            return [{
                'Reservations': [{
                    'Instances': [{
                        'BlockDeviceMappings': [{
                            'Ebs': {
                                'DeleteOnTermination': True,
                            },
                        }],
                        'ClientToken': identity['client_token'],
                        'InstanceId': 'i-0123456789abcdef0',
                        'InstanceLifecycle': 'spot',
                        'InstanceType': identity['instance_type'],
                        'Placement': {
                            'AvailabilityZone': identity['zone'],
                        },
                        'State': {
                            'Name': 'running',
                        },
                        'Tags': [{
                            'Key': reconciliation.provision_constants.
                                   TAG_RAY_CLUSTER_NAME,
                            'Value': identity['cluster_name_on_cloud'],
                        }],
                    }]
                }]
            }]

    class _Client:
        """Scripted STS and EC2 client."""

        def __init__(self, service):
            self.service = service

        def get_caller_identity(self):
            calls.append(('caller', self.service))
            return {'Account': identity['aws_account_id']}

        def get_paginator(self, operation):
            calls.append(('paginator', self.service, operation))
            return _Paginator()

    class _Session:

        def client(self, service, **kwargs):
            calls.append(('client', service, kwargs))
            return _Client(service)

    def _session(**kwargs):
        calls.append(('session', kwargs))
        return _Session()

    monkeypatch.setattr(reconciliation.aws_adaptor, 'session', _session)

    observed = reconciliation._query_aws_paid_provider_census(
        _aws_census_scope(identity))

    assert observed == [{
        'availability_zone': identity['zone'],
        'client_token': identity['client_token'],
        'cluster_name_on_cloud': identity['cluster_name_on_cloud'],
        'instance_id': 'i-0123456789abcdef0',
        'instance_type': identity['instance_type'],
        'market': 'spot',
        'state': 'running',
    }]
    assert calls == [
        ('session', {
            'profile': 'prod'
        }),
        ('client', 'sts', {
            'region_name': identity['region']
        }),
        ('caller', 'sts'),
        ('client', 'ec2', {
            'region_name': identity['region']
        }),
        ('paginator', 'ec2', 'describe_instances'),
        ('paginate', {
            'Filters': [{
                'Name': 'client-token',
                'Values': [identity['client_token']],
            }],
        }),
    ]


def test_aws_paid_census_rejects_profile_in_wrong_account(
        monkeypatch: pytest.MonkeyPatch) -> None:
    identity = {
        'aws_account_id': '096766144388',
        'region': 'us-east-2',
    }
    scope = _aws_census_scope(identity, credential_profile='durable-profile')

    class _Session:

        def client(self, service, **_kwargs):
            if service != 'sts':
                pytest.fail('Wrong-account credentials reached EC2.')
            return mock.Mock(
                get_caller_identity=lambda: {'Account': '999999999999'})

    def _session(*, profile):
        assert profile == 'durable-profile'
        return _Session()

    monkeypatch.setattr(reconciliation.aws_adaptor, 'session', _session)

    with pytest.raises(ValueError, match='another account'):
        reconciliation._query_aws_paid_provider_census(scope)


def test_aws_paid_observation_fails_closed_when_profile_is_unusable(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    identity = {
        'aws_account_id': '096766144388',
        'credential_profile': None,
        'region': 'us-east-2',
    }
    scope = _aws_census_scope(identity, credential_profile='durable-profile')
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_aws_provider_census_scope',
                        lambda *_args: scope)
    monkeypatch.setattr(
        reconciliation, '_query_aws_paid_provider_census_isolated',
        mock.Mock(side_effect=RuntimeError('profile unavailable')))

    observed = reconciliation.observe_provider(context, _paid_replica('aws'),
                                               'authority')

    assert observed.evidence is ordinary_launch_binding.ProviderEvidence.UNKNOWN
    assert observed.payload['reason'] == 'aws-provider-read-failed'
    assert observed.payload['error_type'] == 'RuntimeError'
    assert observed.payload['provider_identity'] == identity


def test_aws_client_token_absence_evidence_is_canonical() -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    replica = _paid_replica('aws')
    association = _association_for_context(context,
                                           replica.paid_capacity_pool_key)
    identity = ordinary_launch_binding.ordinary_paid_aws_provider_identity(
        association, credential_profile='prod')
    payload = {
        'association_id': str(context.association_id),
        'cluster_name': replica.cluster_name,
        'instances': [],
        'probe_contract': 'aws-client-token-instance-presence-v1',
        'profile_kind': context.profile.kind.value,
        'provider_identity': identity,
        'replica_record_id': str(context.replica_record_id),
        'result': ordinary_launch_binding.ProviderEvidence.ABSENT.value,
    }

    canonical, digest = (
        ordinary_launch_binding._ordinary_paid_provider_evidence(  # pylint: disable=protected-access
            association,
            replica.cluster_name,
            ordinary_launch_binding.ProviderEvidence.ABSENT,
            evidence_payload=payload))

    assert canonical == payload
    assert len(digest) == 64


@pytest.mark.parametrize('profile_kind', [
    ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
    ordinary_launch_binding.NonPoolLaunchProfileKind.
    UNKNOWN_CAPACITY_REPLACEMENT
])
def test_aws_paid_teardown_submits_only_exact_instance_ids_without_waiting(
        monkeypatch: pytest.MonkeyPatch,
        profile_kind: ordinary_launch_binding.NonPoolLaunchProfileKind) -> None:
    context = _context(profile_kind)
    identity = {
        'aws_account_id': '096766144388',
        'credential_profile': None,
        'region': 'us-east-2',
    }
    scope = _aws_census_scope(identity)
    live = {
        'instance_id': 'i-0123456789abcdef0',
        'state': 'running',
    }
    events = []
    monkeypatch.setattr(
        reconciliation.request_postgres,
        'bound_non_pool_provider_present_cleanup_is_authorized',
        lambda *_args: True)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_aws_provider_census_scope',
                        lambda *_args: scope)
    monkeypatch.setattr(reconciliation, '_query_aws_paid_provider_census',
                        lambda *_args: events.append('census') or [live])

    class _Client:

        def get_caller_identity(self):
            return {'Account': identity['aws_account_id']}

        def terminate_instances(self, *, InstanceIds):
            events.append(('terminate', InstanceIds))

    class _Session:

        def client(self, _service, **_kwargs):
            return _Client()

    monkeypatch.setattr(reconciliation.aws_adaptor, 'session',
                        lambda **_kwargs: _Session())
    submission = reconciliation.submit_aws_paid_provider_teardown(
        context, _paid_replica('aws'), object(), continue_guard=lambda: True)

    assert submission.disposition is (
        resource_actions.ProviderSubmissionDisposition.ACCEPTED)
    assert events == ['census', ('terminate', [live['instance_id']])]


@pytest.mark.parametrize(
    ('instances', 'disks', 'expected'),
    [({}, [], ordinary_launch_binding.ProviderEvidence.ABSENT),
     ({
         'svc-3-abc-head-1234abcd-compute': (object(), None)
     }, [], ordinary_launch_binding.ProviderEvidence.PRESENT),
     ({}, ['svc-3-abc-head-1234abcd-compute'
          ], ordinary_launch_binding.ProviderEvidence.PRESENT)])
@pytest.mark.parametrize('profile_kind', [
    ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
    ordinary_launch_binding.NonPoolLaunchProfileKind.
    UNKNOWN_CAPACITY_REPLACEMENT
])
def test_gcp_paid_observation_uses_frozen_exact_label_scope(
        monkeypatch: pytest.MonkeyPatch, instances, disks,
        expected: ordinary_launch_binding.ProviderEvidence,
        profile_kind: ordinary_launch_binding.NonPoolLaunchProfileKind) -> None:
    context = _context(profile_kind)
    identity = {
        'cluster_name_on_cloud': 'svc-3-abc',
        'instance_type': 'g2-standard-4',
        'num_nodes': 1,
        'project_id': 'boltz-498512',
        'region': 'us-east4',
        'use_spot': True,
        'workspace': 'workspace-a',
        'zone': 'us-east4-a',
    }
    monkeypatch.setattr(
        reconciliation.request_postgres, 'bound_non_pool_gcp_provider_identity',
        lambda actual_context, actual_authority: identity
        if (actual_context, actual_authority) ==
        (context, 'authority') else pytest.fail('wrong GCP identity authority'))
    calls = []

    def _census(actual_replica, actual_identity, **_kwargs):
        assert actual_replica.cluster_name == 'svc-3'
        assert actual_identity == identity
        calls.append('census')
        return sorted(instances), list(disks), {
            'failed': [],
            'inflight': [],
            'succeeded': [],
        }

    monkeypatch.setattr(reconciliation,
                        '_query_gcp_paid_provider_census_isolated', _census)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_gcp_provider_absence_is_settled',
                        lambda *_args, **_kwargs: True)
    monkeypatch.setattr(reconciliation.time, 'sleep', lambda _seconds: None)

    observed = reconciliation.observe_provider(context, _paid_replica('gcp'),
                                               'authority')

    assert observed.evidence is expected
    assert observed.payload['profile_kind'] == profile_kind.value
    assert observed.payload['provider_identity'] == identity
    assert observed.payload['instance_ids'] == sorted(instances)
    assert observed.payload['disk_ids'] == disks
    expected_calls = (2 if expected
                      is ordinary_launch_binding.ProviderEvidence.ABSENT else 1)
    assert calls == ['census'] * expected_calls


def test_gcp_done_error_insert_and_empty_resources_is_absent(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    identity = {
        'cluster_name_on_cloud': 'svc-3-abc',
        'instance_type': 'g2-standard-4',
        'num_nodes': 1,
        'project_id': 'boltz-498512',
        'region': 'us-east4',
        'use_spot': True,
        'workspace': 'workspace-a',
        'zone': 'us-east4-a',
    }
    failed_target = 'svc-3-abc-head-1234abcd-compute'
    operation_targets = {
        'failed': [failed_target],
        'inflight': [],
        'succeeded': [],
    }
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_gcp_provider_identity',
                        lambda *_args: identity)
    monkeypatch.setattr(reconciliation,
                        '_query_gcp_paid_provider_census_isolated',
                        lambda *_args, **_kwargs: ([], [], operation_targets))
    settled_calls = []
    monkeypatch.setattr(
        reconciliation.request_postgres,
        'bound_non_pool_gcp_provider_absence_is_settled',
        lambda *_args, **kwargs: settled_calls.append(kwargs) or True)
    monkeypatch.setattr(reconciliation.time, 'sleep', lambda _seconds: None)

    observed = reconciliation.observe_provider(context, _paid_replica('gcp'),
                                               'authority')

    assert observed.evidence is ordinary_launch_binding.ProviderEvidence.ABSENT
    assert observed.payload['create_operation_targets'] == operation_targets
    assert settled_calls == [{
        'completed_create_targets': [failed_target],
    }]


def test_gcp_non_done_insert_and_empty_resources_is_unknown(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    identity = {
        'cluster_name_on_cloud': 'svc-3-abc',
        'project_id': 'boltz-498512',
        'zone': 'us-east4-a',
    }
    inflight_target = 'svc-3-abc-head-1234abcd-compute'
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_gcp_provider_identity',
                        lambda *_args: identity)
    monkeypatch.setattr(
        reconciliation, '_query_gcp_paid_provider_census_isolated',
        lambda *_args, **_kwargs: ([], [], {
            'failed': [],
            'inflight': [inflight_target],
            'succeeded': [],
        }))
    monkeypatch.setattr(
        reconciliation.request_postgres,
        'bound_non_pool_gcp_provider_absence_is_settled', lambda *_args, **
        _kwargs: pytest.fail('in-flight create must not reach absence gate'))

    observed = reconciliation.observe_provider(context, _paid_replica('gcp'),
                                               'authority')

    assert observed.evidence is ordinary_launch_binding.ProviderEvidence.UNKNOWN
    assert observed.payload['reason'] == 'gcp-create-operation-in-flight'
    assert observed.payload['create_operation_targets']['inflight'] == [
        inflight_target
    ]


def test_gcp_paid_observation_fails_closed_without_frozen_project(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_gcp_provider_identity',
                        lambda *_args: None)
    monkeypatch.setattr(
        reconciliation.provision, 'query_instances', lambda **_kwargs: pytest.
        fail('missing frozen identity must not reach GCP'))

    observed = reconciliation.observe_provider(context, _paid_replica('gcp'),
                                               object())

    assert observed.evidence is ordinary_launch_binding.ProviderEvidence.UNKNOWN
    assert observed.payload[
        'reason'] == 'missing-immutable-gcp-provider-identity'


@pytest.mark.parametrize('profile_kind', [
    ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
    ordinary_launch_binding.NonPoolLaunchProfileKind.
    UNKNOWN_CAPACITY_REPLACEMENT
])
def test_gcp_paid_teardown_submits_vm_then_disk_without_waiting_for_absence(
        monkeypatch: pytest.MonkeyPatch,
        profile_kind: ordinary_launch_binding.NonPoolLaunchProfileKind) -> None:
    context = _context(profile_kind)
    replica = types.SimpleNamespace(cluster_name='svc-3')
    identity = {
        'cluster_name_on_cloud': 'svc-3-abc',
        'project_id': 'boltz-498512',
        'zone': 'us-east4-a',
    }
    events = []
    monkeypatch.setattr(
        reconciliation.request_postgres,
        'bound_non_pool_provider_present_cleanup_is_authorized',
        lambda *_args: True)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_gcp_provider_identity',
                        lambda *_args: identity)
    censuses = iter([
        (['svc-3-abc-head-1234abcd-compute'], [], {
            'failed': [],
            'inflight': [],
            'succeeded': [],
        }),
        ([], ['svc-3-abc-head-1234abcd-compute'], {
            'failed': [],
            'inflight': [],
            'succeeded': [],
        }),
    ])
    monkeypatch.setattr(
        reconciliation, '_query_gcp_paid_provider_census',
        lambda *_args: events.append('census') or next(censuses))
    monkeypatch.setattr(reconciliation.gcp_provision,
                        'submit_terminate_exact_instances',
                        lambda *_args: events.append('terminate-instances'))
    monkeypatch.setattr(reconciliation.gcp_provision,
                        'submit_terminate_exact_managed_boot_disks',
                        lambda *_args: events.append('terminate-disks'))
    vm_submission = reconciliation.submit_gcp_paid_provider_teardown(
        context, replica, object(), continue_guard=lambda: True)
    disk_submission = reconciliation.submit_gcp_paid_provider_teardown(
        context, replica, object(), continue_guard=lambda: True)

    assert vm_submission.disposition is (
        resource_actions.ProviderSubmissionDisposition.ACCEPTED)
    assert disk_submission.disposition is (
        resource_actions.ProviderSubmissionDisposition.ACCEPTED)
    assert events == [
        'census', 'terminate-instances', 'census', 'terminate-disks'
    ]


def test_gcp_paid_teardown_reports_ambiguous_disk_submission_without_polling(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    replica = types.SimpleNamespace(cluster_name='svc-3')
    identity = {
        'cluster_name_on_cloud': 'svc-3-abc',
        'project_id': 'boltz-498512',
        'zone': 'us-east4-a',
    }
    events = []
    monkeypatch.setattr(
        reconciliation.request_postgres,
        'bound_non_pool_provider_present_cleanup_is_authorized',
        lambda *_args: True)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_gcp_provider_identity',
                        lambda *_args: identity)
    monkeypatch.setattr(
        reconciliation, '_query_gcp_paid_provider_census', lambda *_args:
        ([], ['svc-3-abc-head-1234abcd-compute'], {
            'failed': [],
            'inflight': [],
            'succeeded': [],
        }))
    disk_attempts = iter([RuntimeError('disk is still attached'), None])

    def _terminate_disks(*_args):
        events.append('terminate-disks')
        error = next(disk_attempts)
        if error is not None:
            raise error

    monkeypatch.setattr(reconciliation.gcp_provision,
                        'submit_terminate_exact_managed_boot_disks',
                        _terminate_disks)
    first = reconciliation.submit_gcp_paid_provider_teardown(
        context, replica, object())
    second = reconciliation.submit_gcp_paid_provider_teardown(
        context, replica, object())

    assert first.disposition is (
        resource_actions.ProviderSubmissionDisposition.AMBIGUOUS)
    assert second.disposition is (
        resource_actions.ProviderSubmissionDisposition.ACCEPTED)
    assert events == ['terminate-disks', 'terminate-disks']


@pytest.mark.parametrize(('code', 'expected'), [
    ('ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
     paid_capacity.LaunchOutcome.CAPACITY_FAILURE),
    ('QUOTA_EXCEEDED', paid_capacity.LaunchOutcome.QUOTA_FAILURE),
    ('UNSUPPORTED_PROVIDER_FAILURE', paid_capacity.LaunchOutcome.OTHER_FAILURE)
])
def test_gcp_exact_absence_preserves_typed_provider_failure(code, expected):
    provider_error = provision_common.ProvisionerError('GCP create failed')
    provider_error.errors = [{'code': code, 'message': code}]
    location_error = exceptions.ResourcesUnavailableError(
        'location unavailable', failover_history=[provider_error])
    request_error = exceptions.ResourcesUnavailableError(
        'optimizer exhausted', failover_history=[location_error])
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    pool_key = 'gcp-pool'
    status_property = types.SimpleNamespace(
        sky_launch_status=reconciliation.common_utils.ProcessStatus.FAILED,
        failed_spot_availability=False)
    info = types.SimpleNamespace(paid_capacity_pool_key=pool_key,
                                 is_spot=True,
                                 is_zero_cost=False,
                                 reserved_fill=False,
                                 service_job_id=None,
                                 status_property=status_property)
    projection = types.SimpleNamespace(
        provider_evidence=ordinary_launch_binding.ProviderEvidence.ABSENT,
        provider_evidence_payload={
            'probe_contract': 'gcp-vm-disk-operation-presence-v1',
            'result': 'ABSENT',
            'instance_ids': [],
            'disk_ids': [],
            'create_operation_targets': {
                'failed': [],
                'inflight': [],
                'succeeded': [],
            },
        },
        context=context,
        pre_effect_terminal=False,
        service_job_id=None,
        locked_replica_info=info,
        paid_capacity_pool_key=pool_key,
        request=types.SimpleNamespace(error=request_error),
        status=types.SimpleNamespace(value='FAILED'),
        cause=types.SimpleNamespace(value='handler_failed'))

    result = reconciliation.apply_exact_provider_absence_replica_projection(
        projection)

    assert result is not None
    assert result.paid_capacity_outcome is expected
    assert status_property.failed_spot_availability is True


def test_gcp_paid_unknown_replacement_exact_absence_is_projectable() -> None:
    context = _context(ordinary_launch_binding.NonPoolLaunchProfileKind.
                       UNKNOWN_CAPACITY_REPLACEMENT)
    pool_key = json.dumps(
        {
            'accelerators': [['l4', 1]],
            'cloud': 'gcp',
            'instance_type': 'g2-standard-4',
            'num_nodes': 1,
            'region': 'us-east4',
            'use_spot': True,
            'version': 1,
            'workspace': 'workspace-a',
            'zone': 'us-east4-a',
        },
        sort_keys=True,
        separators=(',', ':'))
    status_property = types.SimpleNamespace(
        sky_launch_status=reconciliation.common_utils.ProcessStatus.FAILED,
        failed_spot_availability=False)
    info = types.SimpleNamespace(paid_capacity_pool_key=pool_key,
                                 is_spot=True,
                                 is_zero_cost=False,
                                 reserved_fill=False,
                                 service_job_id=None,
                                 status_property=status_property)
    projection = types.SimpleNamespace(
        provider_evidence=ordinary_launch_binding.ProviderEvidence.ABSENT,
        provider_evidence_payload={
            'probe_contract': 'gcp-vm-disk-operation-presence-v1',
            'result': 'ABSENT',
            'instance_ids': [],
            'disk_ids': [],
            'create_operation_targets': {
                'failed': [],
                'inflight': [],
                'succeeded': [],
            },
        },
        context=context,
        pre_effect_terminal=False,
        service_job_id=None,
        locked_replica_info=info,
        paid_capacity_pool_key=pool_key,
        request=types.SimpleNamespace(error=None),
        status=types.SimpleNamespace(value='FAILED'),
        cause=types.SimpleNamespace(value='handler_failed'))

    result = reconciliation.apply_exact_provider_absence_replica_projection(
        projection)

    assert result is not None
    assert result.paid_capacity_pool_key == pool_key
    assert result.paid_capacity_outcome is paid_capacity.LaunchOutcome.OTHER_FAILURE
    assert status_property.failed_spot_availability is True


def test_cancelled_gcp_exact_absence_is_neutral_cleanup() -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    pool_key = json.dumps(
        {
            'accelerators': [['l4', 1]],
            'cloud': 'gcp',
            'instance_type': 'g2-standard-4',
            'num_nodes': 1,
            'region': 'us-east4',
            'use_spot': True,
            'version': 1,
            'workspace': 'workspace-a',
            'zone': 'us-east4-a',
        },
        sort_keys=True,
        separators=(',', ':'))
    status_property = types.SimpleNamespace(
        sky_launch_status=reconciliation.common_utils.ProcessStatus.FAILED,
        failed_spot_availability=True)
    info = types.SimpleNamespace(paid_capacity_pool_key=pool_key,
                                 is_spot=True,
                                 is_zero_cost=False,
                                 reserved_fill=False,
                                 service_job_id=None,
                                 status_property=status_property)
    projection = types.SimpleNamespace(
        provider_evidence=ordinary_launch_binding.ProviderEvidence.ABSENT,
        provider_evidence_payload={
            'probe_contract': 'gcp-vm-disk-operation-presence-v1',
            'result': 'ABSENT',
            'instance_ids': [],
            'disk_ids': [],
            'create_operation_targets': {
                'failed': [],
                'inflight': [],
                'succeeded': [],
            },
        },
        context=context,
        pre_effect_terminal=False,
        service_job_id=None,
        locked_replica_info=info,
        paid_capacity_pool_key=pool_key,
        request=types.SimpleNamespace(error=None),
        status=types.SimpleNamespace(value='CANCELLED'),
        cause=types.SimpleNamespace(value='explicit_cancel'))

    result = reconciliation.apply_exact_provider_absence_replica_projection(
        projection)

    assert result is not None
    assert result.paid_capacity_outcome is paid_capacity.LaunchOutcome.OTHER_FAILURE
    assert status_property.failed_spot_availability is False
    assert (status_property.sky_launch_status
            is reconciliation.common_utils.ProcessStatus.INTERRUPTED)
    aws_pool_key = json.dumps(
        {
            'accelerators': [['l4', 1]],
            'cloud': 'aws',
            'instance_type': 'g6.2xlarge',
            'num_nodes': 1,
            'provider_identity': {
                'aws_account_id': '123456789012',
            },
            'region': 'us-east-2',
            'use_spot': True,
            'version': 2,
            'workspace': 'workspace-a',
            'zone': 'us-east-2a',
        },
        sort_keys=True,
        separators=(',', ':'))
    assert ordinary_launch_binding.ordinary_paid_provider_terminal_shape_matches(
        'CANCELLED', 'explicit_cancel', aws_pool_key)
    assert not ordinary_launch_binding.ordinary_paid_provider_terminal_shape_matches(
        'CANCELLED', 'explicit_cancel',
        aws_pool_key.replace('123456789012', 'unknown'))


def test_paid_pending_teardown_becomes_succeeded_only_after_exact_absence(
) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    info = _paid_cleanup_replica(
        reconciliation.common_utils.ProcessStatus.FAILED)
    info.paid_capacity_pool_key = _paid_replica('gcp').paid_capacity_pool_key
    projection = types.SimpleNamespace(
        provider_evidence=ordinary_launch_binding.ProviderEvidence.ABSENT,
        provider_evidence_payload={
            'probe_contract': 'gcp-vm-disk-operation-presence-v1',
            'result': 'ABSENT',
            'instance_ids': [],
            'disk_ids': [],
            'create_operation_targets': {
                'failed': [],
                'inflight': [],
                'succeeded': [],
            },
        },
        context=context,
        pre_effect_terminal=False,
        service_job_id=None,
        locked_replica_info=info,
        paid_capacity_pool_key=info.paid_capacity_pool_key,
        request=types.SimpleNamespace(error=None),
        status=types.SimpleNamespace(value='CANCELLED'),
        cause=types.SimpleNamespace(value='explicit_cancel'))

    result = reconciliation.apply_exact_provider_absence_replica_projection(
        projection)

    assert result is not None
    assert ordinary_launch_binding.provider_present_teardown_phase(info) is (
        ordinary_launch_binding.ProviderPresentTeardownPhase.CLEANUP_SUCCEEDED)
    assert ordinary_launch_binding.replica_has_projected_provider_absence_cleanup_marker(
        info)


@pytest.mark.parametrize('status', ordinary_launch_binding.TerminalStatus)
@pytest.mark.parametrize('cause', event_api_models.EventCause)
def test_exact_v2_provider_cleanup_uses_structural_terminal_evidence(
        status: ordinary_launch_binding.TerminalStatus,
        cause: event_api_models.EventCause) -> None:
    aws_pool_key = _paid_replica('aws').paid_capacity_pool_key
    legacy_gcp_pool_key = _paid_replica('gcp').paid_capacity_pool_key
    gcp_pool = json.loads(legacy_gcp_pool_key)
    gcp_pool['version'] = 2
    gcp_pool['provider_identity'] = {'gcp_project_id': 'project-a'}
    gcp_pool_key = json.dumps(gcp_pool, sort_keys=True, separators=(',', ':'))

    assert ordinary_launch_binding.ordinary_paid_provider_terminal_shape_matches(
        status, cause, aws_pool_key)
    assert ordinary_launch_binding.ordinary_paid_provider_terminal_shape_matches(
        status, cause, gcp_pool_key)


def test_structural_terminal_cleanup_retains_narrow_legacy_contracts() -> None:
    aws_pool_key = _paid_replica('aws').paid_capacity_pool_key
    legacy_gcp_pool_key = _paid_replica('gcp').paid_capacity_pool_key
    malformed_aws = aws_pool_key.replace('096766144388', 'unknown')

    assert ordinary_launch_binding.ordinary_paid_provider_terminal_shape_matches(
        'FAILED', 'handler_failed', malformed_aws)
    assert ordinary_launch_binding.ordinary_paid_provider_terminal_shape_matches(
        'CANCELLED', 'explicit_cancel', legacy_gcp_pool_key)
    assert not ordinary_launch_binding.ordinary_paid_provider_terminal_shape_matches(
        'FAILED', 'dispatcher_submit_failed', malformed_aws)
    assert not ordinary_launch_binding.ordinary_paid_provider_terminal_shape_matches(
        'CANCELLED', 'execution_lease_expired', legacy_gcp_pool_key)


def test_exact_v2_provider_cleanup_accepts_future_diagnostic_cause() -> None:
    aws_pool_key = _paid_replica('aws').paid_capacity_pool_key

    assert ordinary_launch_binding.ordinary_paid_provider_terminal_shape_matches(
        'FAILED', 'future_terminal_cause', aws_pool_key)
    assert not ordinary_launch_binding.ordinary_paid_provider_terminal_shape_matches(
        'FAILED', '', aws_pool_key)
    assert not ordinary_launch_binding.ordinary_paid_provider_terminal_shape_matches(
        'PENDING', 'future_terminal_cause', aws_pool_key)


def test_ordinary_paid_reconcile_prefers_exact_terminal_negative_ack(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    authority = object()
    payload = {
        'association_id': str(context.association_id),
        'cluster_name': 'svc-3',
        'probe_contract': 'aws-run-instances-negative-ack-v1',
        'profile_kind': 'ORDINARY_PAID',
        'receipt': {
            'provider': 'aws',
        },
        'replica_record_id': str(context.replica_record_id),
        'result': 'ABSENT',
    }
    calls = []
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_reconciliation_ready',
                        lambda *_args: True)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_absence_is_recorded',
                        lambda *_args: False)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_terminal_provider_absence_payload',
                        lambda *_args: payload)
    monkeypatch.setattr(
        reconciliation, 'observe_provider', lambda *_args: pytest.fail(
            'an exact request receipt must replace guessed provider state'))
    monkeypatch.setattr(
        reconciliation.request_postgres,
        'record_bound_non_pool_provider_evidence',
        lambda _context, _authority, evidence, observed_payload: calls.append(
            ('record', evidence, observed_payload)))
    monkeypatch.setattr(reconciliation.request_postgres,
                        'project_bound_non_pool_provider_absence',
                        lambda *_args, **_kwargs: calls.append(('project',)))

    observed = reconciliation.reconcile(context, _paid_replica('aws'),
                                        authority, lambda *_args: True)

    assert observed == reconciliation.ProviderObservation(
        ordinary_launch_binding.ProviderEvidence.ABSENT, payload)
    assert calls == [('record', ordinary_launch_binding.ProviderEvidence.ABSENT,
                      payload), ('project',)]


def test_ordinary_paid_reconcile_without_exact_receipt_remains_unknown(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    authority = object()
    calls = []
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_reconciliation_ready',
                        lambda *_args: True)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_absence_is_recorded',
                        lambda *_args: False)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_terminal_provider_absence_payload',
                        lambda *_args: None)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_aws_provider_census_scope',
                        lambda *_args: None)
    monkeypatch.setattr(
        reconciliation.request_postgres,
        'record_bound_non_pool_provider_evidence',
        lambda _context, _authority, evidence, payload: calls.append(
            ('record', evidence, payload)))
    monkeypatch.setattr(
        reconciliation.request_postgres,
        'project_bound_non_pool_provider_absence', lambda *_args, **_kwargs:
        pytest.fail('UNKNOWN must never release paid capacity'))

    observed = reconciliation.reconcile(context, _paid_replica('aws'),
                                        authority, lambda *_args: True)

    assert observed.evidence == ordinary_launch_binding.ProviderEvidence.UNKNOWN
    assert observed.payload['reason'] == 'missing-immutable-aws-provider-access'
    assert calls == [
        ('record', ordinary_launch_binding.ProviderEvidence.UNKNOWN,
         observed.payload)
    ]


@pytest.mark.parametrize(
    ('evidence', 'expected_calls'),
    ((ordinary_launch_binding.ProviderEvidence.ABSENT, ['record', 'project']),
     (ordinary_launch_binding.ProviderEvidence.PRESENT, ['record', 'authorize'
                                                        ]),
     (ordinary_launch_binding.ProviderEvidence.UNKNOWN, ['record']),
     (ordinary_launch_binding.ProviderEvidence.REPLACED, ['record'])))
def test_reconcile_has_closed_evidence_actions(
        monkeypatch: pytest.MonkeyPatch,
        evidence: ordinary_launch_binding.ProviderEvidence,
        expected_calls: list[str]) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL)
    authority = object()
    replica = _reserved_replica()
    observation = reconciliation.ProviderObservation(evidence, {
        'result': evidence.value,
    })
    calls = []
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_reconciliation_ready',
                        lambda *_args: True)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_absence_is_recorded',
                        lambda *_args: False)
    monkeypatch.setattr(reconciliation, 'observe_provider',
                        lambda *_args: observation)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'record_bound_non_pool_provider_evidence',
                        lambda *_args: calls.append('record'))
    monkeypatch.setattr(reconciliation.request_postgres,
                        'project_bound_non_pool_provider_absence',
                        lambda *_args, **_kwargs: calls.append('project'))
    monkeypatch.setattr(reconciliation.request_postgres,
                        'authorize_bound_non_pool_provider_present_cleanup',
                        lambda *_args, **_kwargs: calls.append('authorize'))
    projector = lambda *_args: True

    assert reconciliation.reconcile(context, replica, authority,
                                    projector) == observation
    assert calls == expected_calls


def test_reconcile_projects_recorded_absence_without_provider_reread(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL)
    authority = object()
    calls = []
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_reconciliation_ready',
                        lambda *_args: True)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_absence_is_recorded',
                        lambda *_args: True)
    monkeypatch.setattr(
        reconciliation, 'observe_provider', lambda *_args: pytest.fail(
            'recorded exact absence must not be observed again'))
    monkeypatch.setattr(
        reconciliation.request_postgres,
        'record_bound_non_pool_provider_evidence', lambda *_args: pytest.fail(
            'recorded exact absence must not be rewritten'))
    monkeypatch.setattr(reconciliation.request_postgres,
                        'project_bound_non_pool_provider_absence',
                        lambda *_args, **_kwargs: calls.append('project'))

    observed = reconciliation.reconcile(context, _reserved_replica(), authority,
                                        lambda *_args: True)

    assert observed == reconciliation.ProviderObservation(
        ordinary_launch_binding.ProviderEvidence.ABSENT, {
            'result': 'ABSENT',
            'source': 'durable-provider-evidence',
        })
    assert calls == ['project']


def test_reconcile_records_fresh_absence_before_projection_then_settles(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    authority = object()
    observation = reconciliation.ProviderObservation(
        ordinary_launch_binding.ProviderEvidence.ABSENT, {'result': 'ABSENT'})
    calls = []
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_reconciliation_ready',
                        lambda *_args: True)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_absence_is_recorded',
                        lambda *_args: False)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_terminal_provider_absence_payload',
                        lambda *_args: None)
    monkeypatch.setattr(reconciliation, 'observe_provider',
                        lambda *_args: calls.append('observe') or observation)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'record_bound_non_pool_provider_evidence',
                        lambda *_args: calls.append('record'))
    monkeypatch.setattr(reconciliation.request_postgres,
                        'project_bound_non_pool_provider_absence',
                        lambda *_args, **_kwargs: calls.append('project'))

    assert reconciliation.reconcile(context, _paid_replica('aws'), authority,
                                    lambda *_args: True) == observation
    assert calls == ['observe', 'record', 'project']


def test_absence_survives_projection_failure_and_retry_skips_provider(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    authority = object()
    observation = reconciliation.ProviderObservation(
        ordinary_launch_binding.ProviderEvidence.ABSENT, {'result': 'ABSENT'})
    calls = []
    recorded = False

    def _is_recorded(*_args):
        return recorded

    def _record(*_args):
        nonlocal recorded
        calls.append('record')
        recorded = True

    projection_attempt = 0

    def _project(*_args, **_kwargs):
        nonlocal projection_attempt
        projection_attempt += 1
        calls.append(f'project-{projection_attempt}')
        if projection_attempt == 1:
            raise RuntimeError('injected projection failure')

    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_reconciliation_ready',
                        lambda *_args: True)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_absence_is_recorded',
                        _is_recorded)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_terminal_provider_absence_payload',
                        lambda *_args: None)
    monkeypatch.setattr(reconciliation, 'observe_provider',
                        lambda *_args: calls.append('observe') or observation)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'record_bound_non_pool_provider_evidence', _record)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'project_bound_non_pool_provider_absence', _project)

    with pytest.raises(RuntimeError, match='injected projection failure'):
        reconciliation.reconcile(context, _paid_replica('aws'), authority,
                                 lambda *_args: True)

    assert calls == ['observe', 'record', 'project-1']

    retried = reconciliation.reconcile(context, _paid_replica('aws'), authority,
                                       lambda *_args: True)

    assert retried.evidence is ordinary_launch_binding.ProviderEvidence.ABSENT
    assert calls == ['observe', 'record', 'project-1', 'project-2']


@pytest.mark.parametrize('evidence', [
    ordinary_launch_binding.ProviderEvidence.PRESENT,
    ordinary_launch_binding.ProviderEvidence.UNKNOWN,
])
def test_pending_paid_teardown_retains_present_authority_until_absent(
        monkeypatch: pytest.MonkeyPatch,
        evidence: ordinary_launch_binding.ProviderEvidence) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    authority = object()
    info = _paid_cleanup_replica(
        reconciliation.common_utils.ProcessStatus.FAILED)
    observation = reconciliation.ProviderObservation(evidence, {
        'result': evidence.value,
    })
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_reconciliation_ready',
                        lambda *_args: True)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_absence_is_recorded',
                        lambda *_args: False)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_terminal_provider_absence_payload',
                        lambda *_args: None)
    monkeypatch.setattr(reconciliation, 'observe_provider',
                        lambda *_args: observation)
    monkeypatch.setattr(
        reconciliation.request_postgres,
        'record_bound_non_pool_provider_evidence', lambda *_args: pytest.fail(
            'non-ABSENT observation must retain cleanup authority'))
    monkeypatch.setattr(
        reconciliation.request_postgres,
        'project_bound_non_pool_provider_absence', lambda *_args, **_kwargs:
        pytest.fail('non-ABSENT observation must not settle'))

    assert reconciliation.reconcile(context, info, authority,
                                    lambda *_args: True) == observation


@pytest.mark.parametrize(('evidence', 'expected'), [
    (ordinary_launch_binding.ProviderEvidence.ABSENT,
     reconciliation.PaidTeardownObservationDisposition.SETTLED_ABSENT),
    (ordinary_launch_binding.ProviderEvidence.PRESENT,
     reconciliation.PaidTeardownObservationDisposition.RESUBMIT_PRESENT),
    (ordinary_launch_binding.ProviderEvidence.UNKNOWN,
     reconciliation.PaidTeardownObservationDisposition.RETRY_UNKNOWN),
])
def test_paid_teardown_observation_has_one_closed_next_action(
        monkeypatch: pytest.MonkeyPatch,
        evidence: ordinary_launch_binding.ProviderEvidence,
        expected: reconciliation.PaidTeardownObservationDisposition) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)
    info = _paid_cleanup_replica(
        reconciliation.common_utils.ProcessStatus.FAILED)
    observation = reconciliation.ProviderObservation(evidence,
                                                     {'result': evidence.value})
    scheduled = object()
    requeue = mock.Mock(return_value=scheduled)
    monkeypatch.setattr(reconciliation, 'reconcile',
                        mock.Mock(return_value=observation))
    monkeypatch.setattr(reconciliation.request_postgres,
                        'requeue_bound_non_pool_provider_teardown_submission',
                        requeue)

    step = reconciliation.advance_paid_teardown_observation(
        context, info, object(), lambda *_args: True)

    assert step.disposition is expected
    if evidence is ordinary_launch_binding.ProviderEvidence.PRESENT:
        requeue.assert_called_once_with(context, mock.ANY)
        assert step.scheduled_replica_info is scheduled
    else:
        requeue.assert_not_called()
        assert step.scheduled_replica_info is None


def test_forced_reconcile_rereads_provider_before_absence_projection(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = _context(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL)
    authority = object()
    observation = reconciliation.ProviderObservation(
        ordinary_launch_binding.ProviderEvidence.ABSENT, {'result': 'ABSENT'})
    calls = []
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_reconciliation_ready',
                        lambda *_args: True)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'bound_non_pool_provider_absence_is_recorded',
                        lambda *_args: True)
    monkeypatch.setattr(reconciliation, 'observe_provider',
                        lambda *_args: calls.append('observe') or observation)
    monkeypatch.setattr(reconciliation.request_postgres,
                        'record_bound_non_pool_provider_evidence',
                        lambda *_args: calls.append('record'))
    monkeypatch.setattr(reconciliation.request_postgres,
                        'project_bound_non_pool_provider_absence',
                        lambda *_args, **_kwargs: calls.append('project'))

    observed = reconciliation.reconcile(context,
                                        _reserved_replica(),
                                        authority,
                                        lambda *_args: True,
                                        force_provider_read=True)

    assert observed == observation
    assert calls == ['observe', 'record', 'project']
