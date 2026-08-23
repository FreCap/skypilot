"""Pure validation tests for exact asynchronous dispatch binding."""
# pylint: disable=protected-access

from unittest import mock
import uuid

import pytest

from sky.serve import async_request_ledger as ledger

_RECORD_ID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_PROJECTION = 'a' * 64


class _Result:
    """Minimal SQLAlchemy result double for worker-admission validation."""

    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value

    def mappings(self):
        return self

    def one_or_none(self):
        return self._value


def _connection(raw_projections, admission):
    connection = mock.Mock()
    connection.execute.side_effect = (_Result(raw_projections),
                                      _Result(admission))
    return connection


def _projection(*, kueue):
    return {
        'sha256': _PROJECTION,
        'kubernetes_context': 'ctx',
        'accelerator_name': 'H200',
        'accelerator_count': 1,
        'kueue_admission': kueue,
    }


def _worker_admission(connection):
    return ledger._worker_admission(
        connection, 'svc', 'svc-hash', 1, 4, _RECORD_ID, 'H200', 1, {
            'reserved_fill_worker_projection_sha256': _PROJECTION,
        }, {
            'kind': 'kubernetes',
            'kubernetes_context': 'ctx',
            'physical_cluster_uid': 'cluster-uid',
            'reserved_pool_key': 'pool-h200',
        })


def _patch_projection_validation(monkeypatch):
    monkeypatch.setattr(ledger.kubernetes_identity,
                        'validate_worker_placement_projections',
                        lambda value, allow_none: value)
    monkeypatch.setattr(ledger.kubernetes_identity, 'worker_projection_sha256',
                        lambda value: value['sha256'])


def test_schema_available_caches_only_positive_result(monkeypatch) -> None:
    engine = mock.Mock()
    revisions = mock.Mock(side_effect=('057', '058'))
    ledger._SCHEMA_AVAILABLE_ENGINES.clear()
    monkeypatch.setattr(ledger, '_postgres_engine', lambda given=None: engine)
    monkeypatch.setattr(ledger.migration_utils, 'get_current_alembic_revision',
                        revisions)

    assert ledger.schema_available(engine) is False
    assert ledger.schema_available(engine) is True
    assert ledger.schema_available(engine) is True
    assert revisions.call_count == 2


def test_missing_kueue_row_never_implies_projection_only(monkeypatch) -> None:
    _patch_projection_validation(monkeypatch)
    connection = _connection([_projection(kueue={'local_queue_name': 'be'})],
                             None)

    with pytest.raises(ledger.AsyncRequestLedgerConflict,
                       match='no admitted lineage'):
        _worker_admission(connection)


def test_explicit_no_kueue_projection_uses_projection_only(monkeypatch) -> None:
    _patch_projection_validation(monkeypatch)
    connection = _connection([_projection(kueue=None)], None)

    assert _worker_admission(connection) == {
        'kind': 'projection_only',
        'worker_projection_sha256': _PROJECTION,
        'pod_uid': None,
        'pod_receipt_sha256': None,
        'intent_idempotency_key': None,
    }


def _admission(**overrides):
    admission = {
        'service_hash': 'svc-hash',
        'service_version': 1,
        'replica_record_id': _RECORD_ID,
        'state': 'POLICY_ADMITTED',
        'pool_key': 'pool-h200',
        'physical_cluster_uid': 'cluster-uid',
        'kubernetes_context': 'ctx',
        'accelerator': 'h200',
        'accelerator_count': 1,
        'worker_projection_sha256': _PROJECTION,
        'pod_uid': 'pod-uid-1',
        'pod_receipt_sha256': 'b' * 64,
        'intent_idempotency_key': 'c' * 64,
    }
    admission.update(overrides)
    return admission


@pytest.mark.parametrize('overrides', ({
    'state': 'POD_WAITING'
}, {
    'accelerator_count': '1'
}))
def test_kueue_admission_must_be_exact_and_policy_admitted(
        monkeypatch, overrides) -> None:
    _patch_projection_validation(monkeypatch)
    connection = _connection([_projection(kueue={'local_queue_name': 'be'})],
                             _admission(**overrides))

    with pytest.raises(ledger.AsyncRequestLedgerConflict,
                       match='does not match'):
        _worker_admission(connection)
