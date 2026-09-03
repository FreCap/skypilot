"""Characterization tests for Serve request accounting."""
import pickle
import types
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


def test_previous_version_pickle_starts_coverage_fail_closed(monkeypatch):
    now = [240.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    aggregator = serve_utils.RequestTimestamp()
    del aggregator._request_history_coverage_started_at
    del aggregator._acknowledged_request_history_coverage
    restored = pickle.loads(pickle.dumps(aggregator))

    assert restored.request_history_snapshot(include_idle_coverage=True) is None
    now[0] = 300.0
    assert restored.request_history_snapshot(
        include_idle_coverage=True)['buckets'] == [{
            'bucket_start': 240,
            'request_count': 0,
            'rejected_count': 0,
            'coverage_complete': True,
        }]


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


def test_idle_request_history_coverage_waits_for_one_full_minute(monkeypatch):
    now = [125.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    aggregator = serve_utils.RequestTimestamp()

    # The partial minute in which this reporter starts is unknowable.  The
    # first exact zero is emitted only after the reporter observes all of the
    # following minute while it owns traffic.
    assert aggregator.request_history_snapshot(
        include_idle_coverage=True) is None
    now[0] = 239.0
    assert aggregator.request_history_snapshot(
        include_idle_coverage=True) is None
    now[0] = 240.0
    snapshot = aggregator.request_history_snapshot(include_idle_coverage=True)
    assert snapshot == {
        'bucket_seconds': 60,
        'buckets': [{
            'bucket_start': 180,
            'request_count': 0,
            'rejected_count': 0,
            'coverage_complete': True,
        }],
    }

    aggregator.mark_request_history_accepted(snapshot)
    assert aggregator.request_history_snapshot(
        include_idle_coverage=True) is None


def test_completed_nonzero_minute_is_republished_with_coverage(monkeypatch):
    now = [120.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    aggregator = serve_utils.RequestTimestamp()

    assert aggregator.request_history_snapshot(
        include_idle_coverage=True) is None
    aggregator.add(None)
    partial = aggregator.request_history_snapshot(include_idle_coverage=True)
    assert partial['buckets'] == [{
        'bucket_start': 120,
        'request_count': 1,
        'rejected_count': 0,
    }]
    aggregator.mark_request_history_accepted(partial)

    # Closing the minute advances its coverage proof even though the cumulative
    # request counter itself has already been acknowledged.
    now[0] = 180.0
    complete = aggregator.request_history_snapshot(include_idle_coverage=True)
    assert complete['buckets'] == [{
        'bucket_start': 120,
        'request_count': 1,
        'rejected_count': 0,
        'coverage_complete': True,
    }]
    aggregator.mark_request_history_accepted(complete)
    assert aggregator.request_history_snapshot(
        include_idle_coverage=True) is None


def test_dirty_counts_precede_coverage_for_previous_controller(monkeypatch):
    """An old controller may reject a whole batch containing a v2 marker."""
    now = [120.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    aggregator = serve_utils.RequestTimestamp()

    # Begin an ACTIVE interval, then let one fully idle minute elapse before a
    # request arrives in the current minute.  The real counter must travel in
    # a legacy-compatible batch by itself.
    assert aggregator.request_history_snapshot(
        include_idle_coverage=True) is None
    now[0] = 180.0
    aggregator.add(None)
    dirty = aggregator.request_history_snapshot(include_idle_coverage=True)
    assert dirty == {
        'bucket_seconds': 60,
        'buckets': [{
            'bucket_start': 180,
            'request_count': 1,
            'rejected_count': 0,
        }],
    }

    # Simulate the previous controller accepting that marker-free batch.  Only
    # then may the new LB offer the unsupported zero heartbeat, whose loss on
    # an old controller cannot erase the already-durable request.
    aggregator.mark_request_history_accepted(dirty)
    coverage = aggregator.request_history_snapshot(include_idle_coverage=True)
    assert coverage == {
        'bucket_seconds': 60,
        'buckets': [{
            'bucket_start': 120,
            'request_count': 0,
            'rejected_count': 0,
            'coverage_complete': True,
        }],
    }


def test_idle_request_history_coverage_resets_around_inactive_role(monkeypatch):
    now = [120.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    aggregator = serve_utils.RequestTimestamp()

    assert aggregator.request_history_snapshot(
        include_idle_coverage=True) is None
    now[0] = 170.0
    assert aggregator.request_history_snapshot(
        include_idle_coverage=False) is None
    now[0] = 190.0
    assert aggregator.request_history_snapshot(
        include_idle_coverage=True) is None

    # Neither the old authority's partial 120 bucket nor the transition's 180
    # bucket is reported as zero.  A complete post-transition bucket is.
    now[0] = 299.0
    assert aggregator.request_history_snapshot(
        include_idle_coverage=True) is None
    now[0] = 300.0
    assert aggregator.request_history_snapshot(
        include_idle_coverage=True)['buckets'] == [{
            'bucket_start': 240,
            'request_count': 0,
            'rejected_count': 0,
            'coverage_complete': True,
        }]


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


def test_durable_window_survives_controller_drain_and_expires(monkeypatch):
    now = [120.0]
    monkeypatch.setattr(serve_utils.time, 'time', lambda: now[0])
    aggregator = serve_utils.RequestTimestamp()
    request = types.SimpleNamespace(_skyserve_compatible_accelerators=['L4'],
                                    _skyserve_request_priority=50)

    aggregator.add(request)
    assert aggregator.drain()['timestamps'] == [120.0]
    snapshot = aggregator.demand_window_snapshot()
    assert snapshot == {
        'bucket_seconds': constants.LB_DEMAND_WINDOW_BUCKET_SECONDS,
        'window_seconds': constants.LB_DEMAND_WINDOW_SECONDS,
        'coverage_started_at': 120.0,
        'buckets': [{
            'bucket_start': 120,
            'request_count': 1,
            'compatibility_profiles': [{
                'priority': 50,
                'compatible_accelerators': ['L4'],
                'count': 1,
            }],
        }],
        'compatibility_complete': True,
        'saturated': False,
    }

    # The aggregate 300-second offered-arrival floor remains live here, so its
    # exact accelerator attribution must remain live too.
    now[0] += constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS + 1
    retained = aggregator.demand_window_snapshot()
    assert retained['buckets'] == snapshot['buckets']

    now[0] += (constants.LB_DEMAND_WINDOW_SECONDS -
               constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS)
    expired = aggregator.demand_window_snapshot()
    assert expired['buckets'] == []
    assert expired['coverage_started_at'] == 120.0
