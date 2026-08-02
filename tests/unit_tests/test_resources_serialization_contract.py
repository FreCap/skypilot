"""Characterization for the public Resources pickle compatibility seam."""

# The compatibility contract must inspect and construct historical state.
# pylint: disable=protected-access

import inspect
import pickle

from sky import clouds
from sky import resources as resources_lib


def test_pickle_methods_remain_on_resources_facade() -> None:
    getstate = resources_lib.Resources.__getstate__
    setstate = resources_lib.Resources.__setstate__

    assert getstate.__module__ == 'sky.resources'
    assert getstate.__qualname__ == 'Resources.__getstate__'
    assert tuple(inspect.signature(getstate).parameters) == ('self',)
    assert setstate.__module__ == 'sky.resources'
    assert setstate.__qualname__ == 'Resources.__setstate__'
    assert tuple(inspect.signature(setstate).parameters) == ('self', 'state')


def test_current_pickle_round_trip_preserves_state() -> None:
    resource = resources_lib.Resources(cpus='4+',
                                       memory='16+',
                                       use_spot=True,
                                       ports=['8080-8081'],
                                       autostop={
                                           'idle_minutes': 15,
                                           'down': True,
                                           'wait_for': 'jobs',
                                       })

    restored = pickle.loads(pickle.dumps(resource, protocol=5))

    assert type(restored) is resources_lib.Resources
    assert restored.__dict__ == resource.__dict__
    assert restored.to_yaml_config() == resource.to_yaml_config()


def test_earliest_unversioned_state_is_migrated() -> None:
    state = {
        'cloud': None,
        'instance_type': 'legacy-instance',
        'use_spot': True,
        'accelerator_args': None,
        'disk_size': 128,
    }
    restored = resources_lib.Resources.__new__(resources_lib.Resources)

    restored.__setstate__(state)

    assert restored._version == resources_lib.Resources._VERSION
    assert restored.instance_type == 'legacy-instance'
    assert restored.use_spot is True
    assert restored.disk_size == 128
    assert restored.container_image is None
    assert restored.job_recovery is None


def test_kubernetes_context_migration_uses_facade_patch(monkeypatch) -> None:
    resource = resources_lib.Resources()
    state = resource.__dict__.copy()
    state.update({
        '_version': 19,
        '_cloud': clouds.Kubernetes(),
        '_region': 'kubernetes',
        '_image_id': {
            'kubernetes': 'legacy-image'
        },
    })
    lookups = []

    def get_context() -> str:
        lookups.append('context')
        return 'test-context'

    monkeypatch.setattr(
        resources_lib.kubernetes_utils,
        'get_current_kube_config_context_name',
        get_context,
    )
    restored = resources_lib.Resources.__new__(resources_lib.Resources)

    restored.__setstate__(state)

    assert lookups == ['context']
    assert restored.region == 'test-context'
    assert restored.image_id is None
    assert restored.container_image.ref == 'legacy-image'


def test_kubernetes_image_migration_uses_facade_patch(monkeypatch) -> None:
    resource = resources_lib.Resources()
    state = resource.__dict__.copy()
    state.update({
        '_version': 24,
        '_cloud': clouds.Kubernetes(),
        '_region': 'test-context',
        '_image_id': {
            'test-context': 'legacy-image'
        },
    })
    calls = []
    original = resources_lib._maybe_add_docker_prefix_to_image_id

    def add_prefix(image_ids):
        calls.append(dict(image_ids))
        original(image_ids)

    monkeypatch.setattr(resources_lib, '_maybe_add_docker_prefix_to_image_id',
                        add_prefix)
    restored = resources_lib.Resources.__new__(resources_lib.Resources)

    restored.__setstate__(state)

    assert calls == [{'test-context': 'legacy-image'}]
    assert restored.image_id is None
    assert restored.container_image.ref == 'legacy-image'


def test_legacy_autostop_hook_uses_facade_normalizer(monkeypatch) -> None:
    resource = resources_lib.Resources(autostop={
        'idle_minutes': 5,
        'down': True,
    })
    state = resource.__dict__.copy()
    state['_version'] = 32
    state['_autostop_config'].hook = 'echo migrated'
    state['_autostop_config'].hook_timeout = 9
    normalized_entries = []
    original_normalizer = resources_lib._normalize_hook_entry

    def normalize(entry):
        normalized_entries.append(entry.copy())
        return original_normalizer(entry)

    monkeypatch.setattr(resources_lib, '_normalize_hook_entry', normalize)
    restored = resources_lib.Resources.__new__(resources_lib.Resources)

    restored.__setstate__(state)

    assert normalized_entries == [{
        'run': 'echo migrated',
        'events': ['down'],
        'timeout': 9,
    }]
    assert restored.hooks == [{
        'run': 'echo migrated',
        'events': ['down'],
        'timeout': 9,
    }]
    assert not hasattr(restored.autostop_config, 'hook')
    assert not hasattr(restored.autostop_config, 'hook_timeout')
