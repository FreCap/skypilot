"""Process-local coordination for provider registration and inspection.

The opaque receipt produced here prevents accidental use of a provider
registry observation after a later supported registration.  It is not a
security boundary against code executing in the same Python process.
"""

import contextlib
import dataclasses
import enum
import os
import secrets
import threading
import typing


class ProviderRegistrationReceiptFailureV1(enum.Enum):
    """Closed reasons for rejecting a provider-registration receipt."""

    MISSING_RECEIPT = 'missing_receipt'
    INVALID_RECEIPT = 'invalid_receipt'
    WRONG_PROCESS = 'wrong_process'
    STALE_EPOCH = 'stale_epoch'
    ACTIVE_SESSION = 'active_session'


class ProviderRegistrationReceiptError(RuntimeError):
    """Raised when a provider-registration receipt cannot be used."""

    def __init__(self, reason: ProviderRegistrationReceiptFailureV1):
        self.reason = reason
        super().__init__(f'Provider registration receipt rejected: '
                         f'{reason.value}.')


@dataclasses.dataclass(frozen=True)
class ProviderRegistrationBarrierV1:
    """Process-local evidence that one plugin registration pass completed."""

    context: str
    epoch: int
    process_id: int
    _nonce: bytes = dataclasses.field(repr=False)


class _ProviderRegistrationSessionV1:
    """One load session whose successful completion can publish a receipt."""

    def __init__(self, context: str, epoch: int, process_id: int, nonce: bytes):
        self._barrier = ProviderRegistrationBarrierV1(context=context,
                                                      epoch=epoch,
                                                      process_id=process_id,
                                                      _nonce=nonce)
        self._completed = False
        self._closed = False

    def complete(self) -> ProviderRegistrationBarrierV1:
        """Mark the active session complete and return its pending receipt."""
        with _MUTATION_LOCK:
            if self._closed or _active_session is not self:
                raise RuntimeError('Provider registration session is not '
                                   'active.')
            if os.getpid() != self._barrier.process_id:
                raise RuntimeError('Provider registration session cannot be '
                                   'completed in another process.')
            self._completed = True
            return self._barrier

    @property
    def barrier(self) -> ProviderRegistrationBarrierV1:
        return self._barrier

    @property
    def completed(self) -> bool:
        return self._completed

    def close(self) -> None:
        self._closed = True


# Loading is serialized independently from individual registry mutations.  The
# load mutex is reentrant so a recursive load reaches the explicit
# ACTIVE_SESSION check instead of deadlocking.
_LOAD_MUTEX = threading.RLock()
_MUTATION_LOCK = threading.RLock()

_coordinator_process_id = os.getpid()
_registration_epoch = 0
_active_session: _ProviderRegistrationSessionV1 | None = None
_current_receipt: ProviderRegistrationBarrierV1 | None = None


def _reset_after_fork() -> None:
    """Reinitialize inherited locks and coordinator state in a fork child."""
    global _LOAD_MUTEX, _MUTATION_LOCK, _active_session
    global _coordinator_process_id, _current_receipt, _registration_epoch

    _LOAD_MUTEX = threading.RLock()
    _MUTATION_LOCK = threading.RLock()
    _coordinator_process_id = os.getpid()
    _registration_epoch = 0
    _active_session = None
    _current_receipt = None


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_reset_after_fork)


def _reset_after_fork_locked() -> None:
    """Reset inherited coordinator state when first used in a child."""
    global _active_session, _coordinator_process_id, _current_receipt
    global _registration_epoch

    process_id = os.getpid()
    if process_id == _coordinator_process_id:
        return
    _coordinator_process_id = process_id
    _registration_epoch = 0
    _active_session = None
    _current_receipt = None


def _invalidate_receipt_locked() -> None:
    """Advance the process-local epoch and discard the current receipt."""
    global _current_receipt, _registration_epoch
    _registration_epoch += 1
    _current_receipt = None


def _validate_receipt_locked(receipt: object,) -> str:
    """Validate the exact current receipt and return its plugin context."""
    if receipt is None:
        raise ProviderRegistrationReceiptError(
            ProviderRegistrationReceiptFailureV1.MISSING_RECEIPT)
    if type(receipt) is not ProviderRegistrationBarrierV1:
        raise ProviderRegistrationReceiptError(
            ProviderRegistrationReceiptFailureV1.INVALID_RECEIPT)
    if (type(receipt.context) is not str or not receipt.context or
            type(receipt.epoch) is not int or receipt.epoch < 1 or
            type(receipt.process_id) is not int or
            type(receipt._nonce) is not bytes or  # pylint: disable=protected-access
            len(receipt._nonce) != 32):  # pylint: disable=protected-access
        raise ProviderRegistrationReceiptError(
            ProviderRegistrationReceiptFailureV1.INVALID_RECEIPT)

    if receipt.process_id != os.getpid():
        raise ProviderRegistrationReceiptError(
            ProviderRegistrationReceiptFailureV1.WRONG_PROCESS)
    if _active_session is not None:
        raise ProviderRegistrationReceiptError(
            ProviderRegistrationReceiptFailureV1.ACTIVE_SESSION)
    if (_current_receipt is None or receipt.epoch != _registration_epoch or
            receipt.epoch != _current_receipt.epoch or
            not secrets.compare_digest(
                receipt._nonce,  # pylint: disable=protected-access
                _current_receipt._nonce)):  # pylint: disable=protected-access
        raise ProviderRegistrationReceiptError(
            ProviderRegistrationReceiptFailureV1.STALE_EPOCH)
    if (receipt.context != _current_receipt.context or
            receipt.process_id != _current_receipt.process_id):
        raise ProviderRegistrationReceiptError(
            ProviderRegistrationReceiptFailureV1.INVALID_RECEIPT)
    return receipt.context


@contextlib.contextmanager
def provider_registration_session(
    context: str,) -> typing.Iterator[_ProviderRegistrationSessionV1]:
    """Open one serialized provider-registration session.

    The caller must invoke ``complete()`` before leaving the context.  A
    receipt is published only after the context exits successfully.
    """
    if type(context) is not str or not context:
        raise ValueError('Provider registration context must be non-empty.')

    global _active_session, _current_receipt
    with _LOAD_MUTEX:
        with _MUTATION_LOCK:
            _reset_after_fork_locked()
            if _active_session is not None:
                raise ProviderRegistrationReceiptError(
                    ProviderRegistrationReceiptFailureV1.ACTIVE_SESSION)
            _invalidate_receipt_locked()
            session = _ProviderRegistrationSessionV1(
                context=context,
                epoch=_registration_epoch,
                process_id=os.getpid(),
                nonce=secrets.token_bytes(32))
            _active_session = session

        try:
            yield session
        except BaseException:
            with _MUTATION_LOCK:
                if _active_session is session:
                    _active_session = None
                _current_receipt = None
                session.close()
            raise
        else:
            with _MUTATION_LOCK:
                if _active_session is not session:
                    _current_receipt = None
                    session.close()
                    raise RuntimeError('Provider registration session state '
                                       'changed before completion.')
                if not session.completed:
                    _active_session = None
                    _current_receipt = None
                    session.close()
                    raise RuntimeError('Provider registration session exited '
                                       'without complete().')
                _current_receipt = session.barrier
                _active_session = None
                session.close()


@contextlib.contextmanager
def provider_registration_mutation() -> typing.Iterator[None]:
    """Serialize one supported provider registration mutation."""
    with _MUTATION_LOCK:
        _reset_after_fork_locked()
        if _active_session is None:
            _invalidate_receipt_locked()
        yield


@contextlib.contextmanager
def provider_registration_capture(receipt: object,) -> typing.Iterator[str]:
    """Validate a receipt and hold the mutation lock for one capture phase."""
    with _MUTATION_LOCK:
        _reset_after_fork_locked()
        context = _validate_receipt_locked(receipt)
        yield context
        _validate_receipt_locked(receipt)
