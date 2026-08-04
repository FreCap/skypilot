"""Characterization tests for Serve request accounting."""
import pickle
from unittest import mock

from sky.serve import constants
from sky.serve import request_aggregator
from sky.serve import serve_utils


def test_request_timestamp_public_identity_and_pickle_round_trip():
    aggregator = serve_utils.RequestTimestamp()
    aggregator.timestamps.extend([1.0, 2.0])

    assert serve_utils.RequestsAggregator is (
        request_aggregator.RequestsAggregator)
    assert serve_utils.RequestTimestamp is request_aggregator.RequestTimestamp
    assert serve_utils.RequestTimestamp.__module__ == 'sky.serve.serve_utils'
    assert pickle.loads(
        pickle.dumps(aggregator)).__class__ is (serve_utils.RequestTimestamp)
    assert repr(aggregator) == 'RequestTimestamp(timestamps=[1.0, 2.0])'


def test_request_timestamp_accounts_and_acknowledges_one_bucket(monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    aggregator = serve_utils.RequestTimestamp()

    aggregator.add(None)
    aggregator.add_rejection()
    aggregator.add_prediction_time(0.1, 'succeeded')

    history = aggregator.request_history_snapshot()
    prediction_history = aggregator.prediction_time_history_snapshot()
    assert history == {
        'bucket_seconds': constants.LB_REQUEST_HISTORY_BUCKET_SECONDS,
        'buckets': [{
            'bucket_start': 120,
            'request_count': 1,
            'rejected_count': 1,
        }],
    }
    assert prediction_history is not None
    assert prediction_history['buckets'][0]['outcome_counts']['succeeded'][
        0] == 1

    aggregator.mark_request_history_accepted(history)
    aggregator.mark_prediction_time_history_accepted(prediction_history)
    assert aggregator.request_history_snapshot() is None
    assert aggregator.prediction_time_history_snapshot() is None


def test_request_classification_snapshot_always_advertises_v1(monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    aggregator = serve_utils.RequestTimestamp()

    assert aggregator.request_classification_history_snapshot() == {
        'classification_version': 1,
        'bucket_seconds': constants.LB_REQUEST_HISTORY_BUCKET_SECONDS,
        'buckets': [],
    }


def test_request_classification_records_paired_terminal_counters(monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    aggregator = serve_utils.RequestTimestamp()

    aggregator.add_request_classification(rejected=False)
    aggregator.add_request_classification(rejected=True)

    snapshot = aggregator.request_classification_history_snapshot()
    assert snapshot['buckets'] == [{
        'bucket_start': 120,
        'classified_request_count': 2,
        'counted_rejected_count': 1,
    }]
    aggregator.mark_request_classification_history_accepted(snapshot)
    assert not aggregator.request_classification_history_snapshot()['buckets']


def test_request_classification_ack_is_independent_and_in_flight_safe(
        monkeypatch):
    monkeypatch.setattr(serve_utils.time, 'time', lambda: 120.0)
    aggregator = serve_utils.RequestTimestamp()
    aggregator.add(None)
    aggregator.add_request_classification(rejected=True)
    request_snapshot = aggregator.request_history_snapshot()
    classification_snapshot = (
        aggregator.request_classification_history_snapshot())

    aggregator.mark_request_history_accepted(request_snapshot)
    assert aggregator.request_classification_history_snapshot(
    )['buckets'] == classification_snapshot['buckets']

    aggregator.add_request_classification(rejected=False)
    aggregator.mark_request_classification_history_accepted(
        classification_snapshot)
    assert aggregator.request_classification_history_snapshot()['buckets'] == [{
        'bucket_start': 120,
        'classified_request_count': 2,
        'counted_rejected_count': 1,
    }]


def test_request_timestamp_restore_keeps_newest_bounded_batch():
    aggregator = serve_utils.RequestTimestamp()
    cap = constants.LB_REQUEST_TIMESTAMP_CAP
    aggregator.timestamps.extend(range(cap))

    drained = aggregator.drain()
    aggregator.timestamps.append(cap)
    aggregator.restore(drained)

    restored = aggregator.to_dict()['timestamps']
    assert len(restored) == cap
    assert restored[0] == 1
    assert restored[-1] == cap


def test_request_history_prunes_only_on_minute_boundary(monkeypatch):
    now = [120.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    aggregator = serve_utils.RequestTimestamp()
    prune = mock.Mock(wraps=aggregator._prune_request_history)  # pylint: disable=protected-access
    monkeypatch.setattr(aggregator, '_prune_request_history', prune)

    aggregator.add(None)
    aggregator.add(None)
    assert prune.call_count == 1

    now[0] += constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
    aggregator.add(None)
    assert prune.call_count == 2
