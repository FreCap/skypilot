"""Closed-label tests for SkyServe system-OOM rollout telemetry."""

from unittest import mock

import pytest

from sky.serve import system_oom_recovery_observability as observability


def test_unknown_event_is_rejected() -> None:
    with pytest.raises(ValueError, match='Unknown system-OOM recovery event'):
        observability.record('replica-secret-reason')


def test_unknown_provider_and_market_map_to_closed_other_labels() -> None:
    counter = mock.Mock()
    with mock.patch.object(observability, 'SYSTEM_OOM_RECOVERY_EVENTS',
                           counter):
        observability.record('recovery_started',
                             provider='aws-account-123',
                             market='private-market-name')

    counter.labels.assert_called_once_with(event='recovery_started',
                                           provider='other',
                                           market='other')
    counter.labels.return_value.inc.assert_called_once_with()


@pytest.mark.parametrize(('location', 'is_spot', 'expected'), [
    ({
        'cloud': 'AWS'
    }, False, ('aws', 'on_demand')),
    ({
        'cloud': 'aws'
    }, True, ('aws', 'spot')),
    ({
        'cloud': 'GCP'
    }, False, ('gcp', 'on_demand')),
    ({
        'cloud': 'private-provider'
    }, None, ('other', 'unknown')),
    (None, None, ('unknown', 'unknown')),
])
def test_replica_labels_are_bounded(location, is_spot, expected) -> None:
    info = mock.Mock(location=location,
                     is_spot=is_spot,
                     resources_override=None)
    assert observability.labels_for_replica(info) == expected


def test_resources_override_provider_is_bounded() -> None:
    info = mock.Mock(location=None,
                     is_spot=False,
                     resources_override={'cloud': 'Kubernetes'})
    assert observability.labels_for_replica(info) == ('kubernetes', 'on_demand')


def test_record_for_replica_emits_only_closed_dimensions() -> None:
    info = mock.Mock(location={'cloud': 'AWS'},
                     is_spot=True,
                     resources_override=None,
                     service_name='secret-service',
                     service_job_id=987654,
                     replica_id=123)
    with mock.patch.object(observability, 'record') as record:
        observability.record_for_replica('recovery_succeeded', info)

    record.assert_called_once_with('recovery_succeeded',
                                   provider='aws',
                                   market='spot')
