"""Tests for controller-independent SkyServe demand telemetry."""
# pylint: disable=protected-access
import copy
import datetime
from unittest import mock

import fastapi
import pytest

from sky.serve import constants
from sky.serve import demand_state
from sky.serve import load_balancer


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
        'occupancy_sampled_urls': ['http://replica'],
        'total_slots_by_url': {
            'http://replica': 4
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
        'queued_request_deadline_buckets': [],
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


def test_validate_report_accepts_capacity_attribution_window():
    report = _report()
    report['demand_window']['window_seconds'] = (
        constants.LB_DEMAND_WINDOW_SECONDS)

    normalized, _, complete = demand_state._validate_report(report)

    assert normalized['demand_window']['window_seconds'] == (
        constants.LB_DEMAND_WINDOW_SECONDS)
    assert complete is True


@pytest.mark.parametrize(
    ('window_seconds', 'event_age', 'expected_timestamps', 'expected_profiles'),
    [
        (constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS, 30, 1, 1),
        (constants.LB_DEMAND_WINDOW_SECONDS, 30, 1, 1),
        (constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS, 61, 0, 0),
        (constants.LB_DEMAND_WINDOW_SECONDS, 61, 0, 1),
        (constants.LB_DEMAND_WINDOW_SECONDS, 301, 0, 0),
    ])
def test_normalize_demand_window_preserves_only_exact_long_horizon(
        window_seconds, event_age, expected_timestamps, expected_profiles):
    now = 1_000.0
    effective_end = now - event_age
    bucket_seconds = constants.LB_DEMAND_WINDOW_BUCKET_SECONDS
    window = {
        'bucket_seconds': bucket_seconds,
        'window_seconds': window_seconds,
        'buckets': [{
            'bucket_start': int(effective_end - bucket_seconds),
            'request_count': 1,
            'compatibility_profiles': [{
                'priority': 50,
                'compatible_accelerators': ['L4'],
                'count': 1,
            }],
        }],
    }

    normalized = demand_state._normalize_demand_window_for_autoscaling(
        window,
        received_epoch=now,
        reporter_epoch=now,
        now_epoch=now,
        timestamp_limit=constants.LB_REQUEST_TIMESTAMP_CAP)

    assert normalized is not None
    timestamps, profiles = normalized
    assert len(timestamps) == expected_timestamps
    assert len(profiles) == expected_profiles


def test_validate_report_accepts_one_waiter_authoritative_queue_snapshot():
    """Outer retry pressure cannot corrupt the exact waiter queue contract."""
    lb = load_balancer.SkyServeLoadBalancer('http://controller:8001', 30001)
    lb._configured_accelerators = ('L4',)
    lb._queue_depth = 776
    lb._queue_depth_by_priority = {50: 776}
    now = 10_000.0
    waiters: dict[int, load_balancer._RequestQueueWaiter] = {}
    for sequence in range(353):
        request = mock.MagicMock(spec=fastapi.Request)
        setattr(request, '_skyserve_compatible_accelerators', ('L4',))
        waiters[sequence] = load_balancer._RequestQueueWaiter(
            request=request,
            priority=50,
            sequence=sequence,
            future=mock.MagicMock(),
            deadline_monotonic=now + 600)
    lb._request_queue_waiters = {50: waiters}
    report = _report()
    report.update(lb._request_queue_demand_snapshot(now).payload())

    normalized, _, complete = demand_state._validate_report(report)

    assert normalized['queue_depth'] == 353
    assert normalized['retry_handler_depth'] == 776
    assert complete is True


def test_validate_report_accepts_only_complete_route_projection_fence():
    report = _report()
    report.update({
        'route_projection_generation': 9,
        'route_projection_sha256': 'a' * 64,
        'route_source_epoch': 2,
    })
    normalized, _, _ = demand_state._validate_report(report)
    assert normalized['route_projection_generation'] == 9

    for field in ('route_projection_generation', 'route_projection_sha256',
                  'route_source_epoch'):
        partial = _report()
        partial[field] = report[field]
        with pytest.raises(demand_state.DemandReportError):
            demand_state._validate_report(partial)


def test_validate_report_rejects_occupancy_without_matching_freshness():
    report = _report()
    report['occupancy_sample_age_seconds'] = {}

    with pytest.raises(demand_state.DemandReportError):
        demand_state._validate_report(report)


@pytest.mark.parametrize('field', [
    'occupancy_sampled_urls',
    'total_slots_by_url',
    'async_occupancy',
])
def test_validate_report_protocol2_requires_one_exact_capacity_url_set(field):
    report = _report()
    report[field] = [] if field == 'occupancy_sampled_urls' else {}

    with pytest.raises(demand_state.DemandReportError,
                       match='slot and occupancy URLs'):
        demand_state._validate_report(report)


def test_validate_report_protocol2_accounts_for_every_routed_url():
    report = _report()
    report['routing_urls'].append('http://unaccounted')

    with pytest.raises(demand_state.DemandReportError,
                       match='sampled or explicitly unknown'):
        demand_state._validate_report(report)


def test_current_lb_authority_requires_exact_ha_active_generation():
    report = _report()
    rows = [{'lb_slot': 'a', 'payload': report}]
    service = {
        'lb_ha_enabled': 1,
        'lb_active_slot': 'a',
        'lb_cutover_generation': 2,
    }
    assert demand_state.current_demand_report_rows(rows, service) == rows

    report['applied_generation'] = 1
    assert demand_state.current_demand_report_rows(rows, service) is None
    report['applied_generation'] = 2
    report['applied_role'] = 'DRAINING'
    assert demand_state.current_demand_report_rows(rows, service) is None


@pytest.mark.parametrize('non_authoritative_role', ['STANDBY', 'ARMED'])
def test_current_lb_authority_excludes_non_authoritative_report(
        non_authoritative_role):
    active = _report()
    non_authoritative = copy.deepcopy(active)
    non_authoritative['applied_role'] = non_authoritative_role
    rows = [{
        'lb_slot': 'a',
        'payload': active,
    }, {
        'lb_slot': 'b',
        'payload': non_authoritative,
    }]
    service = {
        'lb_ha_enabled': 1,
        'lb_active_slot': 'a',
        'lb_cutover_generation': 2,
    }

    assert demand_state.current_demand_report_rows(rows, service) == rows[:1]


def test_fresh_aggregate_zero_requires_full_quiet_window_coverage():
    report = copy.deepcopy(_report())
    report['demand_window'].update(
        coverage_started_at=(report['reporter_observed_at'] -
                             constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS),
        buckets=[])
    report['local_in_flight'] = 0
    report['http_in_flight'] = {'http://replica': 0}
    rows = [{'payload': report}]

    assert demand_state.reports_prove_fresh_aggregate_zero(rows)

    report['demand_window']['coverage_started_at'] += 1
    assert not demand_state.reports_prove_fresh_aggregate_zero(rows)
    report['demand_window']['coverage_started_at'] -= 1
    report['queue_depth'] = 1
    assert not demand_state.reports_prove_fresh_aggregate_zero(rows)


def test_fresh_aggregate_zero_ignores_compatibility_and_occupancy_only():
    report = copy.deepcopy(_report())
    report['demand_window'].update(
        coverage_started_at=(report['reporter_observed_at'] -
                             constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS),
        buckets=[],
        compatibility_complete=False)
    report['local_in_flight'] = 7
    report['http_in_flight'] = {'http://replica': 7}
    report['async_occupancy'] = {'http://replica': 3}

    assert demand_state.reports_prove_fresh_aggregate_zero([{
        'payload': report
    }])


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
    report['queued_request_deadline_buckets'] = None

    _, _, complete = demand_state._validate_report(report)

    assert complete is False


def test_validate_report_accepts_exact_queue_deadline_buckets():
    report = copy.deepcopy(_report())
    report['queue_depth'] = 2
    report['queue_depth_by_priority'] = {'50': 2}
    report['queued_requests_by_compatibility'] = [{
        'priority': 50,
        'compatible_accelerators': ['L4'],
        'count': 2,
    }]
    report['queued_request_deadline_buckets'] = [{
        'priority': 50,
        'compatible_accelerators': ['L4'],
        'remaining_seconds': 55,
        'count': 2,
    }]

    normalized, _, complete = demand_state._validate_report(report)

    assert complete is True
    assert normalized['queued_request_deadline_buckets'][0][
        'remaining_seconds'] == 55


def test_validate_report_rejects_partial_queue_deadline_buckets():
    report = copy.deepcopy(_report())
    report['queue_depth'] = 2
    report['queue_depth_by_priority'] = {'50': 2}
    report['queued_requests_by_compatibility'] = [{
        'priority': 50,
        'compatible_accelerators': ['L4'],
        'count': 2,
    }]
    report['queued_request_deadline_buckets'] = [{
        'priority': 50,
        'compatible_accelerators': ['L4'],
        'remaining_seconds': 55,
        'count': 1,
    }]

    with pytest.raises(demand_state.DemandReportError,
                       match='must exactly cover'):
        demand_state._validate_report(report)


def test_validate_report_preserves_complete_saturated_offered_arrivals():
    report = copy.deepcopy(_report())
    report['offered_arrival_tracking_saturated'] = True

    normalized, _, complete = demand_state._validate_report(report)

    assert complete is True
    assert normalized['offered_arrival_tracking_saturated'] is True


def test_validate_report_saturation_does_not_hide_partial_compatibility():
    report = copy.deepcopy(_report())
    report['offered_arrival_tracking_saturated'] = True
    report['demand_window']['compatibility_complete'] = False
    report['demand_window']['buckets'][0]['compatibility_profiles'] = []

    normalized, _, complete = demand_state._validate_report(report)

    assert complete is False
    assert normalized['offered_arrival_tracking_saturated'] is True


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


def test_validate_report_rejects_invalid_profile_priority():
    report = copy.deepcopy(_report())
    report['demand_window']['buckets'][0]['compatibility_profiles'][0][
        'priority'] = 101

    with pytest.raises(demand_state.DemandReportError, match='invalid profile'):
        demand_state._validate_report(report)


def test_validate_report_rejects_conflicting_complete_priority_maps():
    report = copy.deepcopy(_report())
    report['queue_depth'] = 1
    report['queue_depth_by_priority'] = {'50': 1}
    report['queued_requests_by_compatibility'] = [{
        'priority': 40,
        'compatible_accelerators': ['L4'],
        'count': 1,
    }]

    with pytest.raises(demand_state.DemandReportError,
                       match='priorities conflict'):
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


def test_aggregate_fresh_reports_exposes_offered_arrival_windows():
    now = datetime.datetime.now(datetime.timezone.utc)
    now_epoch = now.timestamp()
    draining = _report()
    draining.update(
        reporter_observed_at=now_epoch,
        applied_role='DRAINING',
        unique_job_arrivals_60s=2,
        unique_job_arrivals_300s=4,
        headerless_arrivals_60s=1,
        headerless_arrivals_300s=3,
    )
    active = copy.deepcopy(draining)
    active.update(
        reporter_session_id='process-b',
        lb_session_id='pod-b',
        lb_slot='b',
        applied_role='ACTIVE',
        unique_job_arrivals_60s=5,
        unique_job_arrivals_300s=7,
        headerless_arrivals_60s=2,
        headerless_arrivals_300s=4,
        offered_arrival_tracking_saturated=True,
    )
    rows = [{
        'reporter_session_id': report['reporter_session_id'],
        'lb_slot': report['lb_slot'],
        'received_at': now,
        'reporter_observed_at': now,
        'complete': True,
        'payload': report,
    } for report in (draining, active)]

    summary = demand_state._aggregate_fresh_reports(rows, 3, now)

    assert summary['unique_job_arrivals_60s'] == 7
    assert summary['unique_job_arrivals_300s'] == 11
    assert summary['headerless_arrivals_60s'] == 3
    assert summary['headerless_arrivals_300s'] == 7
    assert summary['offered_arrival_tracking_saturated'] is True
