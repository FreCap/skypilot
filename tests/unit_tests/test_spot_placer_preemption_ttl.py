"""Benched spot locations must become retryable after a TTL.

Live finding (2026-07-06, 1000-replica L4 fleet): a location marked
PREEMPTED — including on transient quota errors like
MaxSpotInstanceCountExceeded — was NEVER selected again: selection draws
only from ACTIVE locations, and reactivation required a successful launch
there, which could never happen. Freed quota or recovered capacity was
therefore never picked up. The TTL decay retries each benched location
with one probe launch per window.
"""
# pylint: disable=redefined-outer-name,protected-access,unused-variable
from unittest import mock

import pytest
from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer

from sky import clouds
from sky.serve import constants
from sky.serve import spot_placer


@pytest.fixture
def placer_and_locations():
    cheap = make_location('seoul')
    other = make_location('oregon')
    third = make_location('iowa')
    placer = make_placer({
        cheap: 1.0,
        other: 2.0,
        third: 3.0,
    })
    return placer, cheap, other, third


class TestPreemptionTtlRetry:
    """TTL decay of PREEMPTED marks."""

    def test_benched_within_ttl_stays_excluded(self, placer_and_locations,
                                               monkeypatch):
        placer, cheap, other, third = placer_and_locations
        monkeypatch.setattr(spot_placer.time, 'time', lambda: 1000.0)
        placer.set_preemptive(cheap)
        assert cheap not in placer.active_locations()
        assert cheap in placer.preemptive_locations()
        assert set(placer.known_locations()) == {cheap, other, third}

    def test_benched_past_ttl_becomes_active_again(self, placer_and_locations,
                                                   monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(cheap)
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert cheap in placer.active_locations()
        assert cheap not in placer.preemptive_locations()
        # And it is selectable again as the cheapest active location.
        assert placer.select_next_location() == cheap

    def test_failed_retry_rebenches_for_full_ttl(self, placer_and_locations,
                                                 monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(cheap)
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert cheap in placer.active_locations()
        # Retry launch fails -> benched again for a fresh window.
        placer.set_preemptive(cheap)
        assert cheap not in placer.active_locations()
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT - 10
        assert cheap not in placer.active_locations()
        now[0] += 11
        assert cheap in placer.active_locations()

    def test_successful_retry_clears_the_mark(self, placer_and_locations,
                                              monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(cheap)
        placer.set_active(cheap)
        assert cheap in placer.active_locations()
        assert cheap not in placer.location2preempted_at

    def test_older_success_cannot_clear_newer_failure(self,
                                                      placer_and_locations,
                                                      monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])

        # The successful launch was selected before this newer failure.
        selected_at = now[0]
        now[0] += 1
        placer.set_preemptive(cheap)
        placer.set_active(cheap, selected_at=selected_at)

        assert cheap not in placer.active_locations()
        assert cheap in placer.preemptive_locations()

        # A later retry was selected after the bench, so its success is fresh
        # evidence that the location can be reactivated immediately.
        now[0] += 1
        placer.set_active(cheap, selected_at=now[0])
        assert cheap in placer.active_locations()
        assert cheap not in placer.preemptive_locations()

    def test_total_exhaustion_waits_for_ttl(self, placer_and_locations,
                                            monkeypatch):
        """Total exhaustion stays closed until a durable retry is eligible."""
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(cheap)
        placer.set_preemptive(other)
        assert placer.active_locations() == [third]
        placer.set_preemptive(third)
        assert placer.active_locations() == []
        assert placer.select_next_location() is None
        assert len(placer.location2preempted_at) == 3
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert len(placer.active_locations()) == 3

    def test_selection_consumes_the_retry_budget(self, placer_and_locations,
                                                 monkeypatch):
        """A burst of selections within one window must send exactly ONE
        probe launch to a benched location, not pile onto it."""
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(cheap)
        observed_at = placer.location2preempted_at[cheap]
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        # First selection picks the expired-benched cheapest location...
        assert placer.select_next_location() == cheap
        assert placer.location2preempted_at[cheap] == observed_at
        assert placer.location2retry_reserved_at[cheap] == now[0]
        # ...and consumes its retry: subsequent selections in the same
        # burst must go elsewhere until the next window.
        assert cheap not in placer.active_locations()
        assert placer.select_next_location() == other
        assert placer.select_next_location() == other
        # Next window: retryable again.
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert placer.select_next_location() == cheap

    def test_consumed_retry_remains_admissible_until_newer_failure(
            self, placer_and_locations, monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])

        assert placer.is_launch_admissible(other, selected_at=None)
        placer.set_preemptive(cheap)
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert placer.select_next_location() == cheap
        selected_at = now[0]

        # Selection consumed the retry, so the location is benched again.
        assert not placer.is_active_location(cheap)
        assert placer.is_launch_admissible(cheap, selected_at=selected_at)
        assert not placer.is_launch_admissible(cheap, selected_at=None)

        # A subsequent failure is newer than this queued placement and wins.
        now[0] += 1
        placer.set_preemptive(cheap)
        assert not placer.is_launch_admissible(cheap, selected_at=selected_at)

    def test_env_override_ttl(self, placer_and_locations, monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        monkeypatch.setenv(spot_placer._PREEMPTION_RETRY_SECONDS_ENV_VAR, '60')
        placer.set_preemptive(cheap)
        now[0] += 61
        assert cheap in placer.active_locations()

    def test_snapshot_does_not_consume_retry(self, placer_and_locations,
                                             monkeypatch):
        placer, cheap, other, third = placer_and_locations
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(cheap)
        benched_at = placer.location2preempted_at[cheap]
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1

        snapshot = placer.placement_snapshot()

        location = next(item for item in snapshot['locations']
                        if item['region'] == cheap.region)
        assert location['stored_status'] == 'PREEMPTED'
        assert location['effective_status'] == 'ACTIVE'
        assert location['probe_eligible'] is True
        assert location['benched_at'] == benched_at
        assert placer.location2preempted_at[cheap] == benched_at
        # The first real selection still gets the one probe.
        assert placer.select_next_location() == cheap

    def test_snapshot_uses_only_catalog_prices(self, placer_and_locations):
        placer, cheap, other, third = placer_and_locations

        snapshot = placer.placement_snapshot()

        prices = {
            item['region']: item['cached_hourly_cost']
            for item in snapshot['locations']
        }
        assert prices == {'seoul': 1.0, 'oregon': 2.0, 'iowa': 3.0}
        assert snapshot['cost_unit'] == 'machine_hour'
        assert snapshot['order_semantics'] == (
            'catalog_normalized_cost_then_location_identity')
        assert {
            item['region']: item['normalized_hourly_cost']
            for item in snapshot['locations']
        } == prices

    def test_snapshot_pages_all_resident_locations_deterministically(
            self, placer_and_locations):
        placer, cheap, other, third = placer_and_locations

        first = placer.placement_snapshot(limit=2)
        second = placer.placement_snapshot(
            limit=2,
            offset=first['next_offset'],
            expected_order_generation=first['order_generation'])

        assert first['page_offset'] == 0
        assert first[
            'pagination_version'] == constants.PLACEMENT_STATE_PAGINATION_VERSION
        assert first['next_offset'] == 2
        assert first['total_locations'] == 3
        assert first['truncated'] is True
        assert len(first['order_generation']) == 64
        assert second['order_generation'] == first['order_generation']
        assert second['page_offset'] == 2
        assert second['next_offset'] is None
        assert second['total_locations'] == 3
        assert second['truncated'] is False
        assert [
            item['region'] for item in first['locations'] + second['locations']
        ] == [cheap.region, other.region, third.region]

    @pytest.mark.parametrize('expected_order_generation', [None, '0' * 64])
    def test_noninitial_snapshot_page_requires_exact_order_generation(
            self, placer_and_locations, expected_order_generation):
        placer, cheap, other, third = placer_and_locations

        snapshot = placer.placement_snapshot(
            limit=1,
            offset=1,
            expected_order_generation=expected_order_generation)

        assert snapshot['available'] is False
        assert snapshot['reason'] == 'catalog_order_changed'
        assert snapshot['pagination_version'] == (
            constants.PLACEMENT_STATE_PAGINATION_VERSION)
        assert len(snapshot['order_generation']) == 64
        assert 'locations' not in snapshot

    def test_snapshot_rejects_page_when_workspace_membership_changes(
            self, placer_and_locations):
        placer, cheap, other, third = placer_and_locations

        with mock.patch.object(placer,
                               '_workspace_eligible_locations',
                               side_effect=[{cheap, other}, {other, third}]):
            first = placer.placement_snapshot(limit=1)
            second = placer.placement_snapshot(
                limit=1,
                offset=first['next_offset'],
                expected_order_generation=first['order_generation'])

        assert first['total_locations'] == 2
        assert [entry['region'] for entry in first['locations']
               ] == [cheap.region]
        assert second['available'] is False
        assert second['reason'] == 'catalog_order_changed'
        assert second['order_generation'] != first['order_generation']
        assert 'locations' not in second

    def test_snapshot_rejects_page_from_previous_service_incarnation(
            self, placer_and_locations):
        placer, cheap, other, third = placer_and_locations

        first = placer.placement_snapshot(limit=1, service_incarnation='hash-a')
        second = placer.placement_snapshot(
            limit=1,
            offset=first['next_offset'],
            expected_order_generation=first['order_generation'],
            service_incarnation='hash-b')

        assert second['available'] is False
        assert second['reason'] == 'catalog_order_changed'
        assert second['order_generation'] != first['order_generation']
        assert 'locations' not in second

    def test_snapshot_breaks_equal_cost_ties_by_location_identity(self):
        later = make_location('zeta', cloud_name='AWS')
        earlier = make_location('alpha', cloud_name='AWS')
        placer = make_placer({later: 1.0, earlier: 1.0})

        snapshot = placer.placement_snapshot()

        assert [item['region'] for item in snapshot['locations']
               ] == ['alpha', 'zeta']

    def test_snapshot_default_page_is_bounded(self):
        locations = [
            make_location(f'region-{index:03d}', cloud_name='AWS')
            for index in range(101)
        ]
        placer = make_placer({
            location: float(index) for index, location in enumerate(locations)
        })

        snapshot = placer.placement_snapshot()

        assert len(snapshot['locations']) == 100
        assert snapshot['next_offset'] == 100
        assert snapshot['total_locations'] == 101
        assert snapshot['truncated'] is True

    def test_retry_state_survives_restart_with_original_expiry(
            self, monkeypatch):
        location = spot_placer.Location(cloud=clouds.AWS(),
                                        region='us-east-1',
                                        zone='us-east-1a',
                                        accelerators={'L4': 1},
                                        use_spot=True,
                                        instance_type='g6.xlarge')
        original = make_placer({location: 1.0})
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        original.set_preemptive(location, reason='quota')
        state = original.dump_retry_state()

        now[0] = 1200.0
        restored = make_placer({location: 1.0})
        restored.load_retry_state(state)
        assert restored.location2preempted_at[location] == 1000.0
        assert restored.location2preempted_reason[location] == 'quota'
        assert location not in restored.active_locations()

        now[0] = 1601.0
        assert location in restored.active_locations()

    def test_probe_reservation_survives_restart_and_generic_failure_releases_it(
            self, monkeypatch):
        location = spot_placer.Location(cloud=clouds.AWS(),
                                        region='us-east-1',
                                        zone='us-east-1a',
                                        accelerators={'L4': 1},
                                        use_spot=True,
                                        instance_type='g6.xlarge')
        placer = make_placer({location: 1.0})
        now = [1000.0]
        monkeypatch.setattr(spot_placer.time, 'time', lambda: now[0])
        placer.set_preemptive(location, reason='capacity')
        original_observed_at = placer.location2preempted_at[location]
        now[0] += spot_placer._PREEMPTION_RETRY_SECONDS_DEFAULT + 1
        assert placer.select_next_location() == location

        restored = make_placer({location: 1.0})
        restored.load_retry_state(placer.dump_retry_state())
        assert restored.location2preempted_at[location] == original_observed_at
        assert restored.location2retry_reserved_at[location] == now[0]
        assert location not in restored.active_locations()

        restored.release_retry(location)
        assert location in restored.active_locations()
        assert location not in restored.location2retry_reserved_at
        assert restored.location2preempted_at[location] == original_observed_at
        assert restored.location2preempted_reason[location] == 'capacity'

    def test_quota_benches_matching_regional_scope(self):
        aws = clouds.AWS()
        failed = spot_placer.Location(cloud=aws,
                                      region='us-east-1',
                                      zone='us-east-1a',
                                      accelerators={'L4': 1},
                                      use_spot=True,
                                      instance_type='g6.xlarge')
        sibling_type = spot_placer.Location(cloud=aws,
                                            region='us-east-1',
                                            zone='us-east-1b',
                                            accelerators={'L4': 1},
                                            use_spot=True,
                                            instance_type='g6.2xlarge')
        on_demand = spot_placer.Location(cloud=aws,
                                         region='us-east-1',
                                         zone='us-east-1b',
                                         accelerators={'L4': 1},
                                         use_spot=False,
                                         instance_type='g6.xlarge')
        other_region = spot_placer.Location(cloud=aws,
                                            region='us-west-2',
                                            zone='us-west-2a',
                                            accelerators={'L4': 1},
                                            use_spot=True,
                                            instance_type='g6.xlarge')
        placer = make_placer({
            failed: 1.0,
            sibling_type: 1.1,
            on_demand: 3.0,
            other_region: 1.2,
        })

        placer.set_quota_limited(failed, observed_at=1000.0)

        assert {
            location for location, status in placer.location2status.items()
            if status == spot_placer.LocationStatus.PREEMPTED
        } == {failed, sibling_type}
        assert placer.location2preempted_reason[failed] == 'quota'
        assert on_demand in placer.active_locations()
        assert other_region in placer.active_locations()
