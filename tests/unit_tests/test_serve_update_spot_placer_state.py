"""State-transfer tests for publishing a versioned Serve spot placer."""

# pylint: disable=protected-access

import threading
from unittest import mock

from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer

from sky.serve import replica_managers
from sky.serve import serve_utils


def test_update_preserves_bench_for_unchanged_location(monkeypatch):
    """An unrelated update must not bypass a location's retry TTL."""
    unchanged = make_location('unchanged')
    reshaped_old = make_location('reshaped', accelerators={'L4': 1})
    fallback = make_location('fallback')
    reshaped_new = make_location('reshaped', accelerators={'A100': 1})
    added = make_location('added')
    old_placer = make_placer({
        unchanged: 1.0,
        reshaped_old: 2.0,
        fallback: 3.0,
    })
    new_placer = make_placer({
        unchanged: 1.0,
        reshaped_new: 2.0,
        added: 3.0,
    })
    old_placer.set_preemptive(unchanged)
    old_placer.set_preemptive(reshaped_old)
    benched_at = old_placer.location2preempted_at[unchanged]

    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager.lock = threading.RLock()
    manager._service_name = 'svc'
    manager.latest_version = 1
    manager.yaml_content = 'old: yaml'
    manager._update_mode = None
    manager._spot_placer = old_placer
    manager._uses_logical_replicas = False
    manager._version_specs = {}

    new_yaml = ('resources: {accelerators: L4:1}\n'
                'file_mounts: {}\n'
                'service: {readiness_probe: /}\n')
    monkeypatch.setattr(replica_managers.serve_state, 'get_yaml_content',
                        lambda *_args: new_yaml)
    monkeypatch.setattr(replica_managers.serve_state, 'get_replica_infos',
                        lambda *_args: [])
    monkeypatch.setattr(replica_managers, 'load_task_with_service_spec',
                        lambda *_args: mock.Mock())
    monkeypatch.setattr(replica_managers, '_uniform_whole_gpu_capacity',
                        lambda *_args: None)

    manager.update_version(2,
                           mock.Mock(spot_placer='dynamic_fallback'),
                           update_mode=serve_utils.UpdateMode.ROLLING,
                           new_spot_placer=new_placer)

    assert unchanged in new_placer.preemptive_locations()
    assert new_placer.location2preempted_at[unchanged] == benched_at
    assert reshaped_new in new_placer.active_locations()
    assert added in new_placer.active_locations()
    assert fallback not in new_placer.location2status
