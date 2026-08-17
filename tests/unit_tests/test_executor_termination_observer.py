"""Tests for fail-closed Kubernetes executor termination observation."""

import datetime
from types import SimpleNamespace
from unittest import mock
import uuid

from sky.server import executor_termination_observer as observer
from sky.server.requests import postgres as request_postgres


def _pod(*,
         deleting: bool = True,
         current_terminated: bool = True,
         last_terminated: bool = False,
         resource_version: str = '42'):
    now = datetime.datetime.now(datetime.timezone.utc)
    terminated = SimpleNamespace(finished_at=now,
                                 exit_code=0,
                                 reason='Completed')
    state = SimpleNamespace(
        terminated=terminated if current_terminated else None,
        running=None if current_terminated else SimpleNamespace())
    last_state = SimpleNamespace(
        terminated=terminated if last_terminated else None)
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=str(uuid.uuid4()),
            namespace='skypilot',
            name='executor-pod',
            resource_version=resource_version,
            deletion_timestamp=(now - datetime.timedelta(seconds=1)
                                if deleting else None)),
        status=SimpleNamespace(container_statuses=[
            SimpleNamespace(
                name='skypilot-executor', state=state, last_state=last_state)
        ]))


def test_observation_requires_deleting_pod_and_current_terminated_state():
    cluster_uid = str(uuid.uuid4())
    exact = observer.observation_from_pod(_pod(),
                                          kubernetes_cluster_uid=cluster_uid)
    assert exact is not None
    assert exact.kubernetes_cluster_uid == cluster_uid
    assert exact.container_name == 'skypilot-executor'

    assert observer.observation_from_pod(
        _pod(deleting=False), kubernetes_cluster_uid=cluster_uid) is None
    assert observer.observation_from_pod(
        _pod(current_terminated=False, last_terminated=True),
        kubernetes_cluster_uid=cluster_uid) is None


def test_start_requires_owner_to_equal_pod_uid():
    owner = (str(uuid.uuid4()), 2)
    pod_identity = request_postgres.ServerPodIdentity(name='controller',
                                                      namespace='skypilot',
                                                      uid=str(uuid.uuid4()),
                                                      ip='10.0.0.1')
    try:
        observer.start(owner, pod_identity)
    except RuntimeError as error:
        assert 'must be this Pod UID' in str(error)
    else:
        raise AssertionError('mismatched observer identity was accepted')


def test_conflicting_pod_update_does_not_break_watch(monkeypatch):
    cluster_uid = str(uuid.uuid4())
    events = ({'object': _pod()}, {'object': _pod()})
    watcher = mock.Mock()
    watcher.stream.return_value = events
    core_api = mock.Mock()
    core_api.read_namespace.return_value = SimpleNamespace(
        metadata=SimpleNamespace(uid=cluster_uid))
    core_api.list_namespaced_pod.return_value = SimpleNamespace(
        metadata=SimpleNamespace(resource_version='40'), items=[])
    monkeypatch.setattr(observer.kubernetes, 'in_cluster_context_name',
                        mock.Mock(return_value='in-cluster'))
    monkeypatch.setattr(observer.kubernetes, 'core_api',
                        mock.Mock(return_value=core_api))
    monkeypatch.setattr(observer.kubernetes, 'watch',
                        mock.Mock(return_value=watcher))
    record = mock.Mock(side_effect=[
        request_postgres.ExecutorTerminationEvidenceConflict('different'),
        ('evidence-id',),
    ])
    monkeypatch.setattr(request_postgres,
                        'record_executor_termination_evidence', record)
    instance = observer.ExecutorTerminationEvidenceObserver(
        (str(uuid.uuid4()), 1), 'skypilot')

    instance._observe_once()  # pylint: disable=protected-access

    assert record.call_count == 2
    watcher.stream.assert_called_once_with(core_api.list_namespaced_pod,
                                           namespace='skypilot',
                                           resource_version='40',
                                           timeout_seconds=5)
    watcher.stop.assert_called_once_with()


def test_watch_reconnect_resumes_after_last_consumed_resource_version(
        monkeypatch):
    cluster_uid = str(uuid.uuid4())
    watcher_one = mock.Mock()
    watcher_one.stream.return_value = ({
        'object': _pod(deleting=False, resource_version='41')
    },)
    watcher_two = mock.Mock()
    watcher_two.stream.return_value = ()
    core_api = mock.Mock()
    core_api.read_namespace.return_value = SimpleNamespace(
        metadata=SimpleNamespace(uid=cluster_uid))
    core_api.list_namespaced_pod.return_value = SimpleNamespace(
        metadata=SimpleNamespace(resource_version='40'), items=[])
    monkeypatch.setattr(observer.kubernetes, 'in_cluster_context_name',
                        mock.Mock(return_value='in-cluster'))
    monkeypatch.setattr(observer.kubernetes, 'core_api',
                        mock.Mock(return_value=core_api))
    monkeypatch.setattr(observer.kubernetes, 'watch',
                        mock.Mock(side_effect=[watcher_one, watcher_two]))
    instance = observer.ExecutorTerminationEvidenceObserver(
        (str(uuid.uuid4()), 1), 'skypilot')

    instance._observe_once()  # pylint: disable=protected-access
    instance._observe_once()  # pylint: disable=protected-access

    core_api.list_namespaced_pod.assert_called_once_with(namespace='skypilot',
                                                         _request_timeout=10)
    watcher_one.stream.assert_called_once_with(core_api.list_namespaced_pod,
                                               namespace='skypilot',
                                               resource_version='40',
                                               timeout_seconds=5)
    watcher_two.stream.assert_called_once_with(core_api.list_namespaced_pod,
                                               namespace='skypilot',
                                               resource_version='41',
                                               timeout_seconds=5)


def test_initial_list_records_deleting_pod_before_watch_anchor(monkeypatch):
    cluster_uid = str(uuid.uuid4())
    deleting_pod = _pod(resource_version='41')
    watcher = mock.Mock()
    watcher.stream.return_value = ()
    core_api = mock.Mock()
    core_api.read_namespace.return_value = SimpleNamespace(
        metadata=SimpleNamespace(uid=cluster_uid))
    core_api.list_namespaced_pod.return_value = SimpleNamespace(
        metadata=SimpleNamespace(resource_version='42'), items=[deleting_pod])
    monkeypatch.setattr(observer.kubernetes, 'in_cluster_context_name',
                        mock.Mock(return_value='in-cluster'))
    monkeypatch.setattr(observer.kubernetes, 'core_api',
                        mock.Mock(return_value=core_api))
    monkeypatch.setattr(observer.kubernetes, 'watch',
                        mock.Mock(return_value=watcher))
    record = mock.Mock(return_value=('evidence-id',))
    monkeypatch.setattr(request_postgres,
                        'record_executor_termination_evidence', record)
    instance = observer.ExecutorTerminationEvidenceObserver(
        (str(uuid.uuid4()), 1), 'skypilot')

    instance._observe_once()  # pylint: disable=protected-access

    record.assert_called_once()
    watcher.stream.assert_called_once_with(core_api.list_namespaced_pod,
                                           namespace='skypilot',
                                           resource_version='42',
                                           timeout_seconds=5)


def test_unexpected_persistence_failure_replays_same_watch_event(monkeypatch):
    cluster_uid = str(uuid.uuid4())
    deleting_pod = _pod(resource_version='41')
    failing_watcher = mock.Mock()
    failing_watcher.stream.return_value = ({'object': deleting_pod},)
    resumed_watcher = mock.Mock()
    resumed_watcher.stream.return_value = ({'object': deleting_pod},)
    core_api = mock.Mock()
    core_api.read_namespace.return_value = SimpleNamespace(
        metadata=SimpleNamespace(uid=cluster_uid))
    core_api.list_namespaced_pod.return_value = SimpleNamespace(
        metadata=SimpleNamespace(resource_version='40'), items=[])
    monkeypatch.setattr(observer.kubernetes, 'in_cluster_context_name',
                        mock.Mock(return_value='in-cluster'))
    monkeypatch.setattr(observer.kubernetes, 'core_api',
                        mock.Mock(return_value=core_api))
    monkeypatch.setattr(
        observer.kubernetes, 'watch',
        mock.Mock(side_effect=[failing_watcher, resumed_watcher]))
    record = mock.Mock(side_effect=RuntimeError('database unavailable'))
    monkeypatch.setattr(request_postgres,
                        'record_executor_termination_evidence', record)
    instance = observer.ExecutorTerminationEvidenceObserver(
        (str(uuid.uuid4()), 1), 'skypilot')

    try:
        instance._observe_once()  # pylint: disable=protected-access
    except RuntimeError as error:
        assert 'database unavailable' in str(error)
    else:
        raise AssertionError('unexpected persistence failure was swallowed')
    assert instance._resource_version == '40'  # pylint: disable=protected-access
    record.side_effect = None
    record.return_value = ('evidence-id',)
    instance._observe_once()  # pylint: disable=protected-access

    assert record.call_count == 2
    assert instance._resource_version == '41'  # pylint: disable=protected-access
    resumed_watcher.stream.assert_called_once_with(core_api.list_namespaced_pod,
                                                   namespace='skypilot',
                                                   resource_version='40',
                                                   timeout_seconds=5)


def test_expired_resource_version_forces_fresh_list(monkeypatch):

    class FakeApiException(Exception):

        def __init__(self, status):
            super().__init__(status)
            self.status = status

    cluster_uid = str(uuid.uuid4())
    expired_watcher = mock.Mock()
    expired_watcher.stream.side_effect = FakeApiException(410)
    resumed_watcher = mock.Mock()
    resumed_watcher.stream.return_value = ()
    core_api = mock.Mock()
    core_api.read_namespace.return_value = SimpleNamespace(
        metadata=SimpleNamespace(uid=cluster_uid))
    core_api.list_namespaced_pod.side_effect = [
        SimpleNamespace(metadata=SimpleNamespace(resource_version='40'),
                        items=[]),
        SimpleNamespace(metadata=SimpleNamespace(resource_version='50'),
                        items=[]),
    ]
    monkeypatch.setattr(observer.kubernetes, 'in_cluster_context_name',
                        mock.Mock(return_value='in-cluster'))
    monkeypatch.setattr(observer.kubernetes, 'core_api',
                        mock.Mock(return_value=core_api))
    monkeypatch.setattr(
        observer.kubernetes, 'watch',
        mock.Mock(side_effect=[expired_watcher, resumed_watcher]))
    monkeypatch.setattr(observer.kubernetes, 'api_exception',
                        mock.Mock(return_value=FakeApiException))
    instance = observer.ExecutorTerminationEvidenceObserver(
        (str(uuid.uuid4()), 1), 'skypilot')

    instance._observe_once()  # pylint: disable=protected-access
    assert instance._resource_version is None  # pylint: disable=protected-access
    instance._observe_once()  # pylint: disable=protected-access

    assert core_api.list_namespaced_pod.call_count == 2
    resumed_watcher.stream.assert_called_once_with(core_api.list_namespaced_pod,
                                                   namespace='skypilot',
                                                   resource_version='50',
                                                   timeout_seconds=5)
