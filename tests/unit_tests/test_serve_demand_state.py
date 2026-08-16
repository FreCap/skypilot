"""Tests for controller-independent SkyServe demand telemetry."""
# pylint: disable=protected-access
import copy
import datetime

import pytest

from sky.serve import constants
from sky.serve import demand_state


def _report() -> dict:
    return {
        'protocol_version': constants.LB_DEMAND_REPORT_PROTOCOL_VERSION,
        'sequence': 1,
        'reporter_session_id': 'process-a',
        'reporter_observed_at': 120.0,
        'lb_session_id': 'pod-a',
        'lb_slot': 'a',
        'routing_version': 3,
        'armed_generation': None,
        'applied_role': 'ACTIVE',
        'applied_generation': 2,
        'local_in_flight': 1,
        'http_in_flight': {
            'http://replica': 1
        },
        'async_occupancy': {
            'http://replica': 2
        },
        'occupancy_sample_generation': {
            'http://replica': 4
        },
        'occupancy_sample_age_seconds': {
            'http://replica': 0.1
        },
        'routing_urls': ['http://replica'],
        'unknown_in_flight_urls': [],
        'draining_urls': [],
        'demand_window': {
            'bucket_seconds': constants.LB_DEMAND_WINDOW_BUCKET_SECONDS,
            'window_seconds': constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS,
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
        },
        'request_history': None,
        'request_classification_history': {
            'classification_version': 1,
            'bucket_seconds': constants.LB_REQUEST_HISTORY_BUCKET_SECONDS,
            'buckets': [],
        },
        'prediction_time_history': None,
        'configured_accelerators': ['L4'],
        'request_accelerator_compatibility_version': 1,
        'queue_depth': 0,
        'queued_requests_by_compatibility': [],
        'rejected_requests_by_compatibility': [],
        'queue_depth_by_priority': {},
        'rejected_in_window': 0,
        'rejected_in_recent_window': 0,
        'rejected_in_window_by_priority': {},
        'rejected_in_recent_window_by_priority': {},
        'unique_job_arrivals_60s': 1,
        'unique_job_arrivals_300s': 1,
        'headerless_arrivals_60s': 0,
        'headerless_arrivals_300s': 0,
        'offered_arrival_tracking_saturated': False,
    }


def test_validate_report_accepts_complete_bounded_snapshot():
    normalized, digest, complete = demand_state._validate_report(_report())

    assert normalized['sequence'] == 1
    assert len(digest) == 64
    assert complete is True


def test_validate_report_rejects_occupancy_without_matching_freshness():
    report = _report()
    report['occupancy_sample_age_seconds'] = {}

    with pytest.raises(demand_state.DemandReportError, match='role/occupancy'):
        demand_state._validate_report(report)


def test_validate_report_never_promotes_incomplete_compatibility():
    report = copy.deepcopy(_report())
    report['demand_window']['compatibility_complete'] = False

    _, _, complete = demand_state._validate_report(report)

    assert complete is False


def test_validate_report_never_promotes_saturated_demand_window():
    report = copy.deepcopy(_report())
    report['demand_window']['saturated'] = True

    _, _, complete = demand_state._validate_report(report)

    assert complete is False


def test_validate_report_never_promotes_partial_queue_compatibility():
    report = copy.deepcopy(_report())
    report['queue_depth'] = 1
    report['queue_depth_by_priority'] = {'50': 1}

    _, _, complete = demand_state._validate_report(report)

    assert complete is False


def test_validate_report_never_promotes_saturated_offered_arrivals():
    report = copy.deepcopy(_report())
    report['offered_arrival_tracking_saturated'] = True

    _, _, complete = demand_state._validate_report(report)

    assert complete is False


def test_validate_report_rejects_incomplete_profiles_claimed_complete():
    report = copy.deepcopy(_report())
    report['demand_window']['buckets'][0]['compatibility_profiles'] = []

    with pytest.raises(demand_state.DemandReportError,
                       match='must equal request_count'):
        demand_state._validate_report(report)


def test_validate_report_rejects_profile_outside_configured_catalog():
    report = copy.deepcopy(_report())
    report['demand_window']['buckets'][0]['compatibility_profiles'][0][
        'compatible_accelerators'] = ['A100']

    with pytest.raises(demand_state.DemandReportError,
                       match='outside the configured catalog'):
        demand_state._validate_report(report)


def test_validate_report_rejects_future_demand_bucket():
    report = copy.deepcopy(_report())
    report['demand_window']['buckets'][0]['bucket_start'] = 125

    with pytest.raises(demand_state.DemandReportError,
                       match='outside the accepted window'):
        demand_state._validate_report(report)


def test_validate_report_rejects_unbounded_sequence_and_counts():
    report = copy.deepcopy(_report())
    report['sequence'] = 1 << 63
    with pytest.raises(demand_state.DemandReportError, match='bounded'):
        demand_state._validate_report(report)

    report = copy.deepcopy(_report())
    report['http_in_flight']['http://replica'] = 1 << 63
    with pytest.raises(demand_state.DemandReportError, match='bounded'):
        demand_state._validate_report(report)

    report = copy.deepcopy(_report())
    report['queue_depth'] = 1 << 63
    with pytest.raises(demand_state.DemandReportError, match='bounded'):
        demand_state._validate_report(report)


def test_aggregate_fresh_reports_rejects_corrupt_payload():
    now = datetime.datetime.now(datetime.timezone.utc)
    with pytest.raises((TypeError, ValueError)):
        demand_state._aggregate_fresh_reports([{
            'reporter_session_id': 'process-a',
            'lb_slot': 'a',
            'received_at': now,
            'reporter_observed_at': now,
            'complete': True,
            'payload': {
                'applied_role': 'ACTIVE',
                'applied_generation': 'corrupt',
            },
        }], 1, now)
