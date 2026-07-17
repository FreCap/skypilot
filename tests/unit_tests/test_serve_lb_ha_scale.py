"""Tests for the bounded SkyServe LB HA scale lab."""

import asyncio
import importlib
import pathlib
import sys

import pytest


def _load_scale_lab_module():
    path = (pathlib.Path(__file__).resolve().parents[1] / 'load_tests' /
            'skyserve_lb_ha_scale.py')
    module_dir = str(path.parent)
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)
    return importlib.import_module('skyserve_lb_ha_scale')


skyserve_lb_ha_scale = _load_scale_lab_module()


def test_small_scale_lab_exercises_both_slots_and_probe_shapes():
    artifact = asyncio.run(
        skyserve_lb_ha_scale.run_scale_lab(
            skyserve_lb_ha_scale.ScaleConfig(services=2,
                                             backends_per_service=5,
                                             emulator_origins=4,
                                             emulator_workers=2,
                                             jitter_window_seconds=0.01)))

    assert artifact['topology']['logical_lb_instances'] == 4
    assert artifact['topology']['logical_backend_urls'] == 10
    assert artifact['probe']['attempted'] == 40
    assert artifact['probe']['succeeded'] == 40
    assert artifact['probe']['unknown'] == 0
    assert all(artifact['gates'].values())
    assert not artifact['topology']['distinct_ip_fidelity']
    assert not artifact['topology']['aggregate_network_fidelity']
    assert (artifact['topology']['probe_concurrency_fidelity'] ==
            'per-service-pair')


@pytest.mark.parametrize('override', [{
    'services': 11
}, {
    'backends_per_service': 1001
}, {
    'emulator_origins': 101
}, {
    'emulator_workers': 11
}, {
    'jitter_window_seconds': 11
}, {
    'scenario_cooldown_seconds': 121
}])
def test_scale_lab_rejects_runs_outside_resource_budget(override):
    values = {
        'services': 1,
        'backends_per_service': 1,
        'emulator_origins': 1,
        'emulator_workers': 1,
        'jitter_window_seconds': 0,
        **override,
    }
    with pytest.raises(ValueError):
        skyserve_lb_ha_scale.ScaleConfig(**values).validate()
