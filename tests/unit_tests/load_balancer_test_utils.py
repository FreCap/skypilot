"""Explicit state builders for SkyServe load-balancer unit tests."""
# pylint: disable=protected-access

from collections.abc import Mapping
import time

from sky.serve import load_balancer


def publish_current_occupancy_snapshot(
    balancer: load_balancer.SkyServeLoadBalancer,
    *,
    occupancy: Mapping[str, int],
    total_slots: Mapping[str, int],
    free_slots: Mapping[str, int],
    dispatch_generation_by_url: Mapping[str, int] | None = None,
    sample_generation_by_url: Mapping[str, int] | None = None,
) -> None:
    """Install the complete process-state contract emitted by one probe."""
    urls = set(occupancy)
    assert urls == set(total_slots) == set(free_slots)
    assert all(occupancy[url] >= 0 and total_slots[url] >= 0 and
               free_slots[url] == max(0, total_slots[url] - occupancy[url])
               for url in urls)
    if dispatch_generation_by_url is None:
        dispatch_generation_by_url = {
            url: balancer._occupancy_dispatch_generation.get(url, 0)
            for url in urls
        }
    else:
        assert urls == set(dispatch_generation_by_url)
    if sample_generation_by_url is None:
        sample_generation_by_url = dispatch_generation_by_url
    else:
        assert urls == set(sample_generation_by_url)
    sampled_at = time.monotonic()
    balancer._replica_occupancy = dict(occupancy)
    balancer._replica_total_slots = dict(total_slots)
    balancer._replica_free_slots = dict(free_slots)
    balancer._occupancy_capable.update(urls)
    balancer._occupancy_dispatch_generation = dict(dispatch_generation_by_url)
    balancer._occupancy_sample_generation = dict(sample_generation_by_url)
    balancer._occupancy_sample_time = {url: sampled_at for url in urls}
    balancer._occupancy_sample_role_epoch = {
        url: balancer._occupancy_role_epoch for url in urls
    }
    balancer._occupancy_current_round_sampled_urls = urls
    balancer._last_occupancy_probe_time = sampled_at
