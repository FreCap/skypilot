"""Closed-result contracts for atomic reserved-fill admission."""

# pylint: disable=protected-access

import contextlib
from types import SimpleNamespace
from unittest import mock

import pytest

from sky.server.requests import reserved_fill_admission
from sky.utils import locks


class _InjectedFault(BaseException):
    pass


class _ExitTimeout:

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        raise locks.LockTimeout('exit acknowledgement lost')


class _InjectedInterrupt(KeyboardInterrupt):
    pass


@pytest.fixture
def admitted_preflight(monkeypatch):
    monkeypatch.setattr(
        reserved_fill_admission, '_frozen_identity', lambda _spec:
        (mock.sentinel.body, mock.sentinel.intent))
    monkeypatch.setattr(reserved_fill_admission.request_postgres,
                        'non_pool_launch_binding_fleet_capable', lambda: True)
    monkeypatch.setattr(reserved_fill_admission.request_postgres,
                        'prepare_non_pool_launch_binding_runtime',
                        lambda: mock.sentinel.request_runtime)
    monkeypatch.setattr(reserved_fill_admission.reserved_capacity_broker,
                        'run_fill_persist_transaction',
                        lambda callback: callback(37))


def test_definite_precommit_rollback_is_rejected_without_hydration(
        admitted_preflight, monkeypatch) -> None:
    transaction = mock.Mock(
        side_effect=reserved_fill_admission._Rejected('definite rollback'))
    monkeypatch.setattr(reserved_fill_admission, '_transaction', transaction)

    result = reserved_fill_admission.admit(mock.sentinel.spec)

    assert result.disposition is (
        reserved_fill_admission.AdmissionDisposition.REJECTED)
    assert result.receipt is None
    transaction.assert_called_once_with(mock.sentinel.spec,
                                        37,
                                        runtime=mock.sentinel.request_runtime,
                                        require_existing=False)


def test_commit_ack_loss_hydrates_exact_tuple_and_publishes_once(
        admitted_preflight, monkeypatch) -> None:
    staged = mock.Mock()
    receipt = reserved_fill_admission.AdmissionReceipt(
        replica_id=7,
        replica_record_id='11111111-1111-4111-8111-111111111111',
        association_id='22222222-2222-4222-8222-222222222222',
        request_id='request-7',
        launch_generation=1,
        context=mock.sentinel.context)
    transaction = mock.Mock(side_effect=(
        reserved_fill_admission.AdmissionAmbiguousError('ack lost'),
        (staged, receipt),
    ))
    monkeypatch.setattr(reserved_fill_admission, '_transaction', transaction)

    result = reserved_fill_admission.admit(mock.sentinel.spec)

    assert result == reserved_fill_admission.AdmissionResult(
        reserved_fill_admission.AdmissionDisposition.COMMITTED, receipt=receipt)
    assert transaction.call_args_list == [
        mock.call(mock.sentinel.spec,
                  37,
                  runtime=mock.sentinel.request_runtime,
                  require_existing=False),
        mock.call(mock.sentinel.spec,
                  37,
                  runtime=mock.sentinel.request_runtime,
                  require_existing=True),
    ]
    staged.publish_after_commit.assert_called_once_with()


def test_commit_ack_loss_and_failed_exact_read_is_ambiguous(
        admitted_preflight, monkeypatch) -> None:
    transaction = mock.Mock(side_effect=(
        reserved_fill_admission.AdmissionAmbiguousError('ack lost'),
        RuntimeError('read unavailable'),
    ))
    monkeypatch.setattr(reserved_fill_admission, '_transaction', transaction)

    result = reserved_fill_admission.admit(mock.sentinel.spec)

    assert result.disposition is (
        reserved_fill_admission.AdmissionDisposition.AMBIGUOUS)
    assert result.receipt is None
    assert 'hydration=RuntimeError' in result.detail


def test_postcommit_publication_failure_is_ambiguous(admitted_preflight,
                                                     monkeypatch) -> None:
    receipt = reserved_fill_admission.AdmissionReceipt(
        replica_id=7,
        replica_record_id='11111111-1111-4111-8111-111111111111',
        association_id='22222222-2222-4222-8222-222222222222',
        request_id='request-7',
        launch_generation=1,
        context=mock.sentinel.context)
    staged = mock.Mock()
    staged.publish_after_commit.side_effect = RuntimeError('publish failed')
    monkeypatch.setattr(reserved_fill_admission, '_transaction',
                        mock.Mock(return_value=(staged, receipt)))

    result = reserved_fill_admission.admit(mock.sentinel.spec)

    assert result == reserved_fill_admission.AdmissionResult(
        reserved_fill_admission.AdmissionDisposition.AMBIGUOUS,
        receipt=receipt,
        detail='postcommit=RuntimeError')


def test_preflight_rejection_never_enters_broker_transaction(
        monkeypatch) -> None:
    monkeypatch.setattr(
        reserved_fill_admission, '_frozen_identity',
        mock.Mock(side_effect=ValueError('malformed frozen launch')))
    broker = mock.Mock()
    monkeypatch.setattr(reserved_fill_admission.reserved_capacity_broker,
                        'run_fill_persist_transaction', broker)

    result = reserved_fill_admission.admit(SimpleNamespace())

    assert result.disposition is (
        reserved_fill_admission.AdmissionDisposition.REJECTED)
    broker.assert_not_called()


def test_preflight_nonexception_baseexception_is_not_a_rejection(
        monkeypatch) -> None:
    monkeypatch.setattr(reserved_fill_admission, '_frozen_identity',
                        mock.Mock(side_effect=_InjectedFault()))
    broker = mock.Mock()
    monkeypatch.setattr(reserved_fill_admission.reserved_capacity_broker,
                        'run_fill_persist_transaction', broker)

    with pytest.raises(_InjectedFault):
        reserved_fill_admission.admit(SimpleNamespace())

    broker.assert_not_called()


def test_broker_lock_exit_timeout_after_commit_hydrates_not_rejects(
        monkeypatch) -> None:
    receipt = reserved_fill_admission.AdmissionReceipt(
        replica_id=7,
        replica_record_id='11111111-1111-4111-8111-111111111111',
        association_id='22222222-2222-4222-8222-222222222222',
        request_id='request-7',
        launch_generation=1,
        context=mock.sentinel.context)
    staged = mock.Mock()
    lock = SimpleNamespace(acquire=mock.Mock(
        side_effect=(_ExitTimeout(), contextlib.nullcontext())))
    monkeypatch.setattr(
        reserved_fill_admission, '_frozen_identity', lambda _spec:
        (mock.sentinel.body, mock.sentinel.intent))
    monkeypatch.setattr(reserved_fill_admission.request_postgres,
                        'non_pool_launch_binding_fleet_capable', lambda: True)
    monkeypatch.setattr(reserved_fill_admission.request_postgres,
                        'prepare_non_pool_launch_binding_runtime',
                        lambda: mock.sentinel.request_runtime)
    monkeypatch.setattr(reserved_fill_admission.reserved_capacity_broker.locks,
                        'get_lock', lambda _lock_id: lock)
    transaction = mock.Mock(return_value=(staged, receipt))
    monkeypatch.setattr(reserved_fill_admission, '_transaction', transaction)

    result = reserved_fill_admission.admit(mock.sentinel.spec)

    assert result.disposition is (
        reserved_fill_admission.AdmissionDisposition.COMMITTED)
    assert result.receipt == receipt
    assert transaction.call_args_list == [
        mock.call(mock.sentinel.spec,
                  None,
                  runtime=mock.sentinel.request_runtime,
                  require_existing=False),
        mock.call(mock.sentinel.spec,
                  None,
                  runtime=mock.sentinel.request_runtime,
                  require_existing=True),
    ]
    staged.publish_after_commit.assert_called_once_with()


def test_broker_lock_exit_error_cannot_mask_callback_interrupt(
        monkeypatch) -> None:
    lock = SimpleNamespace(acquire=mock.Mock(return_value=_ExitTimeout()))
    monkeypatch.setattr(reserved_fill_admission.reserved_capacity_broker.locks,
                        'get_lock', lambda _lock_id: lock)

    with pytest.raises(_InjectedInterrupt) as raised:
        reserved_fill_admission.reserved_capacity_broker.run_fill_persist_transaction(
            lambda _token: (_ for _ in ()).throw(_InjectedInterrupt()))

    assert isinstance(raised.value.__cause__, locks.LockTimeout)


def test_broker_lock_acquisition_timeout_is_definite_rejection(
        monkeypatch) -> None:
    acquisition = mock.MagicMock()
    acquisition.__enter__.side_effect = locks.LockTimeout('busy')
    lock = SimpleNamespace(acquire=mock.Mock(return_value=acquisition))
    monkeypatch.setattr(
        reserved_fill_admission, '_frozen_identity', lambda _spec:
        (mock.sentinel.body, mock.sentinel.intent))
    monkeypatch.setattr(reserved_fill_admission.request_postgres,
                        'non_pool_launch_binding_fleet_capable', lambda: True)
    monkeypatch.setattr(reserved_fill_admission.request_postgres,
                        'prepare_non_pool_launch_binding_runtime',
                        lambda: mock.sentinel.request_runtime)
    monkeypatch.setattr(reserved_fill_admission.reserved_capacity_broker.locks,
                        'get_lock', lambda _lock_id: lock)
    transaction = mock.Mock()
    monkeypatch.setattr(reserved_fill_admission, '_transaction', transaction)

    result = reserved_fill_admission.admit(mock.sentinel.spec)

    assert result.disposition is (
        reserved_fill_admission.AdmissionDisposition.REJECTED)
    transaction.assert_not_called()


def test_connection_close_error_cannot_mask_transaction_interrupt(
        monkeypatch) -> None:
    connection = mock.Mock()
    connection.close.side_effect = RuntimeError('close acknowledgement lost')
    engine = mock.Mock()
    engine.connect.return_value = connection
    monkeypatch.setattr(reserved_fill_admission.request_postgres,
                        'initialize_and_get_db', lambda: engine)
    monkeypatch.setattr(reserved_fill_admission.serve_state,
                        'try_acquire_serve_mutation_admission_in_transaction',
                        lambda _conn: True)
    monkeypatch.setattr(reserved_fill_admission.serve_state,
                        'try_acquire_replica_launch_authority_in_transaction',
                        lambda _conn, _engine, _service_name: True)
    monkeypatch.setattr(reserved_fill_admission.serve_state,
                        'get_replica_mutation_counts_in_transaction',
                        lambda _conn: (0, 0))
    monkeypatch.setattr(reserved_fill_admission, '_stage_and_bind',
                        mock.Mock(side_effect=_InjectedInterrupt()))
    spec = SimpleNamespace(authority=SimpleNamespace(service_name='svc'),
                           launch_limit=1)

    with pytest.raises(_InjectedInterrupt) as raised:
        reserved_fill_admission._transaction(spec,
                                             37,
                                             runtime=mock.sentinel.runtime,
                                             require_existing=False)

    assert isinstance(raised.value.__cause__, RuntimeError)
    connection.begin.return_value.rollback.assert_called_once_with()


@pytest.mark.parametrize('point', ('commit', 'hydration', 'publication'))
def test_nonexception_baseexception_is_re_raised_after_evidence_handling(
        admitted_preflight, monkeypatch, point: str) -> None:
    receipt = reserved_fill_admission.AdmissionReceipt(
        replica_id=7,
        replica_record_id='11111111-1111-4111-8111-111111111111',
        association_id='22222222-2222-4222-8222-222222222222',
        request_id='request-7',
        launch_generation=1,
        context=mock.sentinel.context)
    staged = mock.Mock()
    if point == 'commit':
        transaction = mock.Mock(side_effect=(_InjectedFault(), (staged,
                                                                receipt)))
    elif point == 'hydration':
        transaction = mock.Mock(side_effect=(_InjectedFault(),
                                             _InjectedFault()))
    else:
        transaction = mock.Mock(return_value=(staged, receipt))
        staged.publish_after_commit.side_effect = _InjectedFault()
    monkeypatch.setattr(reserved_fill_admission, '_transaction', transaction)

    with pytest.raises(_InjectedFault):
        reserved_fill_admission.admit(mock.sentinel.spec)

    if point == 'commit':
        assert transaction.call_count == 2
        staged.publish_after_commit.assert_called_once_with()
    elif point == 'hydration':
        assert transaction.call_count == 2
        staged.publish_after_commit.assert_not_called()
    else:
        assert transaction.call_count == 1
        staged.publish_after_commit.assert_called_once_with()
