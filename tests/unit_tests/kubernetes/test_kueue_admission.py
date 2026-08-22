"""Tests for pure reserved-fill Kueue Pod admission classification."""

import contextlib
import copy
import datetime
import types
from unittest import mock

import pytest

from sky import exceptions
from sky.provision import common
from sky.provision import constants as provision_constants
from sky.provision.kubernetes import constants
from sky.provision.kubernetes import instance
from sky.provision.kubernetes import kueue_admission

_INTENT = '1' * 64
_PROJECTION = '2' * 64
_RECORD = '12345678-1234-5678-9234-567812345678'


def _identity() -> common.KueuePodAdmissionIdentity:
    return common.KueuePodAdmissionIdentity(
        intent_key=_INTENT,
        replica_record_uuid=_RECORD,
        pool_physical_uid='physical-cluster-uid',
        worker_projection_sha256=_PROJECTION)


def _expectation() -> kueue_admission.KueuePodAdmissionExpectation:
    return kueue_admission.KueuePodAdmissionExpectation(
        namespace='inference',
        cluster_name_on_cloud='replica-7',
        local_queue_name='be',
        cluster_queue_name='skypilot-be',
        workload_priority_class_name='be-lt',
        pod_group_total_count=1,
        priority_class_name='skypilot-low',
        priority_value=-1000,
        preemption_policy='Never',
        service_account_name='skypilot-pool-sa',
        scheduler_name='default-scheduler',
        accelerator='H200',
        accelerator_label_key='nvidia.com/gpu.product',
        accelerator_label_values=('NVIDIA-H200',),
        accelerator_resource_key='nvidia.com/gpu',
        accelerator_count=1,
        identity=_identity())


def _pod(*, admitted: bool = False) -> types.SimpleNamespace:
    labels = {
        provision_constants.TAG_SKYPILOT_CLUSTER_NAME: 'replica-7',
        constants.KUEUE_MANAGED_KEY: constants.KUEUE_MANAGED_VALUE,
        constants.KUEUE_QUEUE_LABEL: 'be',
        constants.KUEUE_POD_GROUP_LABEL: 'replica-7',
        constants.KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL: 'be-lt',
    }
    annotations = {
        constants.KUEUE_POD_GROUP_TOTAL_COUNT_ANNOTATION: '1',
        constants.KUEUE_RETRIABLE_IN_GROUP_ANNOTATION: 'false',
        constants.KUEUE_ROLE_HASH_ANNOTATION: '1234abcd',
        **kueue_admission.identity_annotations(_identity()),
    }
    scheduling_gates = [
        types.SimpleNamespace(name=constants.KUEUE_ADMISSION_SCHEDULING_GATE)
    ]
    if admitted:
        labels.update({
            constants.KUEUE_PODSET_LABEL: '1234abcd',
            constants.KUEUE_LOCAL_QUEUE_LABEL: 'be',
            constants.KUEUE_CLUSTER_QUEUE_LABEL: 'skypilot-be',
        })
        annotations[constants.KUEUE_WORKLOAD_ANNOTATION] = 'replica-7'
        annotations[
            constants.KUEUE_PODSET_UNCONSTRAINED_TOPOLOGY_ANNOTATION] = 'true'
        scheduling_gates = []
    resources = types.SimpleNamespace(requests={'nvidia.com/gpu': '1'},
                                      limits={'nvidia.com/gpu': '1'},
                                      claims=None)
    affinity = types.SimpleNamespace(node_affinity=types.SimpleNamespace(
        required_during_scheduling_ignored_during_execution=(
            types.SimpleNamespace(node_selector_terms=[
                types.SimpleNamespace(match_expressions=[
                    types.SimpleNamespace(key='nvidia.com/gpu.product',
                                          operator='In',
                                          values=['NVIDIA-H200'])
                ])
            ]))))
    spec = types.SimpleNamespace(priority_class_name='skypilot-low',
                                 priority=-1000,
                                 preemption_policy='Never',
                                 service_account_name='skypilot-pool-sa',
                                 scheduler_name='default-scheduler',
                                 scheduling_gates=scheduling_gates,
                                 node_name=None,
                                 affinity=affinity,
                                 containers=[
                                     types.SimpleNamespace(name='ray-node',
                                                           resources=resources)
                                 ],
                                 init_containers=[],
                                 resource_claims=None)
    return types.SimpleNamespace(metadata=types.SimpleNamespace(
        namespace='inference',
        name='replica-7-head',
        uid='pod-uid-7',
        labels=labels,
        annotations=annotations,
        finalizers=[constants.KUEUE_MANAGED_FINALIZER],
        deletion_timestamp=None),
                                 spec=spec,
                                 status=types.SimpleNamespace(phase='Pending'))


@pytest.mark.parametrize(
    ('admitted', 'expected_state'),
    [(False, common.KueuePodAdmissionState.POD_WAITING),
     (True, common.KueuePodAdmissionState.POLICY_ADMITTED)])
def test_classify_exact_waiting_and_admitted_pod(admitted, expected_state):
    observation = kueue_admission.classify_pod(
        _pod(admitted=admitted),
        _expectation(),
        expected_pod_name='replica-7-head',
        expected_pod_uid='pod-uid-7')

    assert observation.state is expected_state
    assert observation.namespace == 'inference'
    assert observation.pod_name == 'replica-7-head'
    assert observation.pod_uid == 'pod-uid-7'
    assert observation.accelerator == 'H200'
    assert observation.accelerator_count == 1
    assert observation.identity == _identity()
    assert len(observation.receipt_sha256) == 64
    assert observation.receipt_sha256 == observation.receipt.sha256
    if admitted:
        assert observation.receipt_sha256 == (
            '99c4ed169d18194bc4986be9c49fa9307da239017d0d320c8f7e33bbe14c19fd')
        assert observation.receipt.canonical_dict(
        )['kueue']['admission_cluster_queue_name'] == 'skypilot-be'


@pytest.mark.parametrize('mutation', [
    lambda pod: pod.metadata.annotations.__setitem__(
        constants.RESERVED_FILL_WORKER_PROJECTION_SHA256_ANNOTATION, '3' * 64),
    lambda pod: setattr(pod.spec, 'scheduler_name', 'other-scheduler'),
    lambda pod: pod.spec.containers[0].resources.requests.__setitem__(
        'nvidia.com/gpu', '2'),
    lambda pod: setattr(pod.metadata, 'uid', 'replacement-uid'),
])
def test_classify_rejects_dynamic_and_static_identity_drift(mutation):
    pod = _pod()
    mutation(pod)

    with pytest.raises(kueue_admission.KueuePodAdmissionClassificationError):
        kueue_admission.classify_pod(pod,
                                     _expectation(),
                                     expected_pod_name='replica-7-head',
                                     expected_pod_uid='pod-uid-7')


def test_install_dynamic_annotations_rejects_caller_collision_atomically():
    pod = {
        'metadata': {
            'annotations': {
                'example.com/retained': 'yes',
                constants.RESERVED_FILL_INTENT_KEY_ANNOTATION: 'forged',
            }
        }
    }
    before = copy.deepcopy(pod)

    with pytest.raises(ValueError, match='server-owned'):
        kueue_admission.install_dynamic_identity_annotations(pod, _identity())

    assert pod == before


def test_install_dynamic_annotations_is_closed_and_carries_static_digest():
    pod = {'metadata': {'annotations': {'example.com/retained': 'yes'}}}

    kueue_admission.install_dynamic_identity_annotations(pod, _identity())

    annotations = pod['metadata']['annotations']
    assert annotations['example.com/retained'] == 'yes'
    assert ({
        key: annotations[key]
        for key in constants.RESERVED_FILL_IDENTITY_ANNOTATION_KEYS
    } == kueue_admission.identity_annotations(_identity()))
    assert annotations[
        constants.RESERVED_FILL_WORKER_PROJECTION_SHA256_ANNOTATION] == (
            _PROJECTION)


@pytest.mark.parametrize('recovering', [False, True])
def test_lane_pauses_without_resident_poll_and_adopts_exact_pod_on_retry(
        monkeypatch, recovering):
    initial_pod = _pod(admitted=recovering)
    if not recovering:
        # Kubernetes create responses are not durable admission observations;
        # a fresh exact GET supplies the first classifiable phase.
        initial_pod.status.phase = None
    observed_pods = ([_pod(
        admitted=True), _pod(admitted=True)] if recovering else
                     [_pod(), _pod(admitted=True),
                      _pod(admitted=True)])
    core_api = mock.Mock()
    core_api.read_namespaced_pod.side_effect = observed_pods
    monkeypatch.setattr(instance.kubernetes, 'core_api',
                        lambda _context: core_api)
    sleep = mock.Mock(side_effect=AssertionError(
        'the durable Kueue lane must not retain a resident polling thread'))
    monkeypatch.setattr(instance.time, 'sleep', sleep)
    full_attester = mock.Mock()
    observations = []
    guard_entries = []
    guard_depth = 0

    @contextlib.contextmanager
    def guard():
        nonlocal guard_depth
        guard_entries.append('entered')
        guard_depth += 1
        try:
            yield
        finally:
            guard_depth -= 1

    clock_tokens = []

    class Observer:

        def begin_observation(self):
            token = (
                datetime.datetime(2026, 8, 21, tzinfo=datetime.timezone.utc) +
                datetime.timedelta(seconds=len(clock_tokens)))
            clock_tokens.append(token)
            return token

        def __call__(self, observation, provider_read_started_at):
            assert guard_depth == 0
            assert provider_read_started_at in clock_tokens
            observations.append(observation)

    observer = Observer()

    def wait_once():
        return instance._wait_for_required_kueue_admission(  # pylint: disable=protected-access
            'inference',
            'phx', [initial_pod],
            full_attester,
            guard,
            timeout=10,
            lane_expectation=_expectation(),
            lane_observer=observer)

    if recovering:
        pod_uids = wait_once()
    else:
        with pytest.raises(exceptions.ExecutionPausedError) as exc_info:
            wait_once()
        assert exc_info.value.retry_wait_seconds == 5
        assert exc_info.value.continue_condition is None
        assert 'policy-gated' in str(exc_info.value)
        # The request executor redelivers the same launch.  Kubernetes create
        # is not repeated here: the provisioner adopts and freshly reads the
        # exact immutable Pod UID before continuing.
        pod_uids = wait_once()

    assert pod_uids == {'replica-7-head': 'pod-uid-7'}
    expected_states = ([common.KueuePodAdmissionState.POLICY_ADMITTED]
                       if recovering else [
                           common.KueuePodAdmissionState.POD_WAITING,
                           common.KueuePodAdmissionState.POLICY_ADMITTED,
                       ])
    assert [observation.state for observation in observations
           ] == expected_states
    assert core_api.read_namespaced_pod.call_count == len(observed_pods)
    core_api.list_namespaced_pod.assert_not_called()
    core_api.read_node.assert_not_called()
    full_attester.assert_not_called()
    assert len(clock_tokens) == len(observed_pods)
    assert guard_entries == []
    sleep.assert_not_called()


def test_callback_failure_is_terminal_after_exact_pod_materialization(
        monkeypatch):
    core_api = mock.Mock()
    core_api.read_namespaced_pod.return_value = _pod()
    monkeypatch.setattr(instance.kubernetes, 'core_api',
                        lambda _context: core_api)
    observer = mock.Mock(side_effect=RuntimeError('database unavailable'))
    observer.begin_observation.return_value = datetime.datetime(
        2026, 8, 21, tzinfo=datetime.timezone.utc)

    with pytest.raises(exceptions.ReservedFillProviderPresentError) as exc_info:
        instance._wait_for_required_kueue_admission(  # pylint: disable=protected-access
            'inference',
            'phx', [_pod()],
            mock.Mock(),
            contextlib.nullcontext,
            timeout=10,
            lane_expectation=_expectation(),
            lane_observer=observer)

    assert exc_info.value.provider_resource_ids == (
        'inference/replica-7-head@pod-uid-7',)
    assert 'receipt commit failed after exact Pod materialization' in str(
        exc_info.value)
    observer.assert_called_once()
    core_api.delete_namespaced_pod.assert_not_called()
