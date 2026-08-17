"""Tests for fail-closed Kubernetes executor termination observation."""

import datetime
from types import SimpleNamespace
from unittest import mock
import uuid

import pytest

from sky.server import executor_termination_observer as observer
from sky.server.requests import postgres as request_postgres


def _pod(*,
         deleting: bool = True,
         current_terminated: bool = True,
         last_terminated: bool = False):
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
            resource_version='42',
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


def test_start_is_dark_and_requires_owner_to_equal_pod_uid(monkeypatch):
    owner = (str(uuid.uuid4()), 2)
    pod_identity = request_postgres.ServerPodIdentity(name='controller',
                                                      namespace='skypilot',
                                                      uid=str(uuid.uuid4()),
                                                      ip='10.0.0.1')
    monkeypatch.delenv(observer.OBSERVER_ENABLED_ENV_VAR, raising=False)
    assert observer.start(owner, pod_identity) is None
    assert observer.start(owner, None) is None

    monkeypatch.setenv(observer.OBSERVER_ENABLED_ENV_VAR, 'true')
    with pytest.raises(RuntimeError, match='exact server Pod identity'):
        observer.start(owner, None)
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
    watcher.stop.assert_called_once_with()
