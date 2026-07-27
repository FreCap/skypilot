"""Regression tests for the on-node autostop indicator's lifecycle.

The API server reports a cluster as AUTOSTOPPING purely from an on-node
indicator (`autostop_lib.set_autostopping_started()` ->
`backend_utils._cluster_is_autostopping`). The indicator stores the current
boot time and `get_is_autostopping()` compares it against `psutil.boot_time()`,
so it is only invalidated by a reboot.

That is correct for a teardown that *succeeds* -- the node either disappears
(autodown) or comes back with a new boot time (autostop). It is wrong for a
teardown that *fails*: the node keeps running with the same boot time, so the
indicator stayed valid for the rest of the boot and pinned the cluster in
AUTOSTOPPING forever. `SkyletEvent.run()` swallows the exception and
`StopEvent._run()` never retries once autostop is cancelled, so nothing
un-latched it. Downstream that hangs `sky launch` (sky/execution.py waits while
the status is AUTOSTOPPING), fails `sky exec`, and makes managed-job and
SkyServe recovery skip the cluster.

These tests pin the invariant: the indicator is set only while a teardown
attempt is in flight, and a failed attempt clears it.
"""
# pylint: disable=protected-access
import os
import subprocess
from unittest import mock

import psutil
import pytest

from sky import clouds
from sky.skylet import autostop_lib
from sky.skylet import configs
from sky.skylet import constants
from sky.skylet import events
from sky.skylet import runtime_utils

_CLUSTER_CONFIG = {
    'cluster_name': 'cluster-abcd1234',
    'max_workers': 0,
    'provider': {},
}


@pytest.fixture(autouse=True)
def isolate_environ():
    """Restores `os.environ` around every test in this module.

    `_stop_cluster_with_new_provisioner` deliberately mutates the process
    environment -- it points the AWS SDK at /dev/null so the node falls back
    to its instance role -- and never restores it. Without this the pollution
    leaks into whatever else runs in the same pytest-xdist worker (notably
    `tests/unit_tests/test_aws.py`, which then fails on
    `AWS_CONFIG_FILE=/dev/null`).
    """
    with mock.patch.dict(events.os.environ, {}, clear=False):
        yield


@pytest.fixture(name='skylet_configs_db')
def skylet_configs_db_fixture(tmp_path, monkeypatch):
    """Points the skylet configs sqlite DB at a tmp dir.

    `configs._DB_PATH` is lazily set by `init_db` via
    `runtime_utils.get_runtime_dir_path`, so reset it and redirect the path
    helper. This keeps the indicator round-trip real rather than mocked.
    """
    monkeypatch.setattr(configs, '_DB_PATH', None)
    db_dir = tmp_path / 'sky-runtime'
    db_dir.mkdir()
    monkeypatch.setattr(runtime_utils,
                        'get_runtime_dir_path',
                        lambda relpath='': str(db_dir / relpath.lstrip('/')))
    return db_dir


def _config_path():
    """The on-node cluster YAML path `_stop_cluster` passes to `ray`."""
    return os.path.abspath(
        os.path.expanduser(events.cluster_utils.SKY_CLUSTER_YAML_REMOTE_PATH))


def _stop_event():
    return events.StopEvent.__new__(events.StopEvent)


def _autostop_config(down=True, backend=None):
    if backend is None:
        backend = events.cloud_vm_ray_backend.CloudVmRayBackend.NAME
    return mock.Mock(down=down, backend=backend)


def _legacy_cloud():
    """A cloud on the pre-SkyPilot-terminator provisioner (e.g. IBM)."""
    cloud = mock.Mock()
    cloud.PROVISIONER_VERSION = clouds.ProvisionerVersion.RAY_AUTOSCALER
    return cloud


def _patch_cluster_yaml(cloud, config=None):
    """Patches out the on-node YAML read so `_stop_cluster` reaches a branch."""
    return (mock.patch.object(events.yaml_utils,
                              'read_yaml',
                              return_value=dict(config or _CLUSTER_CONFIG)),
            mock.patch.object(events.cluster_utils,
                              'get_provider_name',
                              return_value='TestCloud'),
            mock.patch.object(events.registry.CLOUD_REGISTRY,
                              'from_str',
                              return_value=cloud))


# --- The indicator primitive ------------------------------------------------


def test_clear_autostopping_started_invalidates_the_indicator(
        skylet_configs_db):
    del skylet_configs_db  # Used for its side effect.
    assert not autostop_lib.get_is_autostopping()

    autostop_lib.set_autostopping_started()
    assert autostop_lib.get_is_autostopping()

    autostop_lib.clear_autostopping_started()
    assert not autostop_lib.get_is_autostopping(), (
        'A cleared indicator must not read back as autostopping, otherwise '
        'the API server pins the cluster in AUTOSTOPPING for this whole boot.')


def test_clear_then_set_relatches(skylet_configs_db):
    """A cleared indicator must still be settable by the next attempt."""
    del skylet_configs_db
    autostop_lib.set_autostopping_started()
    autostop_lib.clear_autostopping_started()

    autostop_lib.set_autostopping_started()
    assert autostop_lib.get_is_autostopping()


def test_clear_is_idempotent(skylet_configs_db):
    del skylet_configs_db
    autostop_lib.clear_autostopping_started()
    autostop_lib.clear_autostopping_started()
    assert not autostop_lib.get_is_autostopping()


def test_indicator_survives_only_the_current_boot(skylet_configs_db):
    """Sanity-check the pre-existing reboot invalidation still holds."""
    del skylet_configs_db
    autostop_lib.set_autostopping_started()
    assert autostop_lib.get_is_autostopping()

    with mock.patch.object(psutil,
                           'boot_time',
                           return_value=psutil.boot_time() + 1000):
        assert not autostop_lib.get_is_autostopping()


# --- Failed teardown attempts must un-latch ---------------------------------


def test_new_provisioner_terminate_failure_clears_indicator(skylet_configs_db):
    """An autodown whose terminate_instances fails must not latch forever.

    This is the AWS one-time-spot incident shape: StopInstances fails at every
    idle check, so the cluster stayed AUTOSTOPPING while still billing.
    """
    del skylet_configs_db
    cloud = clouds.Kubernetes()
    read_yaml, provider_name, from_str = _patch_cluster_yaml(cloud)

    with read_yaml, provider_name, from_str, \
         mock.patch.object(events.StopEvent, '_execute_hook_if_present'), \
         mock.patch('sky.provision.terminate_instances',
                    side_effect=RuntimeError('terminate failed')), \
         mock.patch('sky.provision.kubernetes.instance.'
                    'emit_autostop_event_best_effort'):
        with pytest.raises(RuntimeError, match='terminate failed'):
            _stop_event()._stop_cluster(_autostop_config(down=True))

    assert not autostop_lib.get_is_autostopping(), (
        'A failed teardown left the indicator latched; the cluster would be '
        'reported AUTOSTOPPING for the rest of this boot.')


def test_new_provisioner_stop_failure_clears_indicator(skylet_configs_db):
    """Same for autostop (down=False), which uses stop_instances."""
    del skylet_configs_db
    cloud = clouds.Kubernetes()
    read_yaml, provider_name, from_str = _patch_cluster_yaml(cloud)

    with read_yaml, provider_name, from_str, \
         mock.patch.object(events.StopEvent, '_execute_hook_if_present'), \
         mock.patch('sky.provision.stop_instances',
                    side_effect=RuntimeError('stop failed')):
        with pytest.raises(RuntimeError, match='stop failed'):
            _stop_event()._stop_cluster(_autostop_config(down=False))

    assert not autostop_lib.get_is_autostopping()


def test_teardown_hook_failure_clears_indicator(skylet_configs_db):
    """A raising `down`/`stop` hook must not latch the indicator either."""
    del skylet_configs_db
    cloud = clouds.Kubernetes()
    read_yaml, provider_name, from_str = _patch_cluster_yaml(cloud)

    with read_yaml, provider_name, from_str, \
         mock.patch.object(events.StopEvent,
                           '_execute_hook_if_present',
                           side_effect=RuntimeError('hook blew up')), \
         mock.patch('sky.provision.terminate_instances') as terminate:
        with pytest.raises(RuntimeError, match='hook blew up'):
            _stop_event()._stop_cluster(_autostop_config(down=True))

    terminate.assert_not_called()
    assert not autostop_lib.get_is_autostopping()


def test_legacy_provisioner_ray_down_failure_clears_indicator(
        skylet_configs_db):
    """The legacy `ray down` path must un-latch on a non-zero exit too."""
    del skylet_configs_db
    read_yaml, provider_name, from_str = _patch_cluster_yaml(_legacy_cloud())

    with read_yaml, provider_name, from_str, \
         mock.patch.object(events.StopEvent, '_execute_hook_if_present'), \
         mock.patch.object(events.StopEvent, '_replace_yaml_for_stopping'), \
         mock.patch.object(events.subprocess,
                           'run',
                           side_effect=subprocess.CalledProcessError(
                               1, 'ray down')):
        with pytest.raises(subprocess.CalledProcessError):
            _stop_event()._stop_cluster(_autostop_config(down=False))

    assert not autostop_lib.get_is_autostopping()


def test_relatches_on_a_later_successful_attempt(skylet_configs_db):
    """After a failed attempt un-latches, the next attempt must re-latch.

    `SkyletEvent.run()` swallows the exception, so the next 60s tick retries
    while the cluster is still idle and still armed. Un-latching must not
    disarm that retry.
    """
    del skylet_configs_db
    cloud = clouds.Kubernetes()

    read_yaml, provider_name, from_str = _patch_cluster_yaml(cloud)
    with read_yaml, provider_name, from_str, \
         mock.patch.object(events.StopEvent, '_execute_hook_if_present'), \
         mock.patch('sky.provision.terminate_instances',
                    side_effect=RuntimeError('transient')), \
         mock.patch('sky.provision.kubernetes.instance.'
                    'emit_autostop_event_best_effort'):
        with pytest.raises(RuntimeError):
            _stop_event()._stop_cluster(_autostop_config(down=True))
    assert not autostop_lib.get_is_autostopping()

    read_yaml, provider_name, from_str = _patch_cluster_yaml(cloud)
    with read_yaml, provider_name, from_str, \
         mock.patch.object(events.StopEvent, '_execute_hook_if_present'), \
         mock.patch('sky.provision.terminate_instances'), \
         mock.patch('sky.provision.kubernetes.instance.'
                    'emit_autostop_event_best_effort'):
        _stop_event()._stop_cluster(_autostop_config(down=True))

    assert autostop_lib.get_is_autostopping(), (
        'A successful teardown must leave the indicator set so the API '
        'server reports AUTOSTOPPING until the node actually goes away.')


# --- Successful / never-started attempts ------------------------------------


def test_successful_teardown_leaves_indicator_set(skylet_configs_db):
    """Do not over-clear: a successful teardown must stay latched."""
    del skylet_configs_db
    cloud = clouds.Kubernetes()
    read_yaml, provider_name, from_str = _patch_cluster_yaml(cloud)

    with read_yaml, provider_name, from_str, \
         mock.patch.object(events.StopEvent, '_execute_hook_if_present'), \
         mock.patch('sky.provision.terminate_instances'), \
         mock.patch('sky.provision.kubernetes.instance.'
                    'emit_autostop_event_best_effort'):
        _stop_event()._stop_cluster(_autostop_config(down=True))

    assert autostop_lib.get_is_autostopping()


def test_unreadable_cluster_yaml_never_latches(skylet_configs_db):
    """Failing before the teardown starts must not latch at all."""
    del skylet_configs_db
    with mock.patch.object(events.yaml_utils,
                           'read_yaml',
                           side_effect=FileNotFoundError('no cluster yaml')):
        with pytest.raises(FileNotFoundError):
            _stop_event()._stop_cluster(_autostop_config(down=True))

    assert not autostop_lib.get_is_autostopping()


def test_unsupported_backend_never_latches(skylet_configs_db):
    del skylet_configs_db
    with pytest.raises(NotImplementedError):
        _stop_event()._stop_cluster(_autostop_config(backend='local'))

    assert not autostop_lib.get_is_autostopping()


# --- Operator escape hatch --------------------------------------------------


def test_set_autostop_clears_a_stale_indicator(skylet_configs_db):
    """`sky autostop`/`--cancel` must un-latch an already-stranded cluster.

    `backend_utils.check_cluster_available` explicitly allows AUTOSTOPPING, so
    this is the only recovery path (short of a reboot or a terminate) for a
    cluster latched by a teardown that failed under an older skylet.
    """
    del skylet_configs_db
    autostop_lib.set_autostopping_started()
    assert autostop_lib.get_is_autostopping()

    autostop_lib.set_autostop(idle_minutes=-1,
                              backend='cloud-vm-ray',
                              wait_for=autostop_lib.AutostopWaitFor.JOBS,
                              down=False)

    assert not autostop_lib.get_is_autostopping()


def test_set_autostop_rearm_also_clears(skylet_configs_db):
    """Re-arming resets the idle clock, so no teardown is in flight."""
    del skylet_configs_db
    autostop_lib.set_autostopping_started()

    autostop_lib.set_autostop(idle_minutes=10,
                              backend='cloud-vm-ray',
                              wait_for=autostop_lib.AutostopWaitFor.JOBS,
                              down=True)

    assert not autostop_lib.get_is_autostopping()


# --- The legacy-path extraction is behaviour-preserving ---------------------


def test_legacy_single_node_runs_stop_then_down(skylet_configs_db):
    del skylet_configs_db
    read_yaml, provider_name, from_str = _patch_cluster_yaml(_legacy_cloud())

    with read_yaml, provider_name, from_str, \
         mock.patch.object(events.StopEvent, '_execute_hook_if_present'), \
         mock.patch.object(events.StopEvent,
                           '_replace_yaml_for_stopping') as replace_yaml, \
         mock.patch.object(events.subprocess, 'run') as run:
        _stop_event()._stop_cluster(_autostop_config(down=False))

    replace_yaml.assert_called_once()
    commands = [call.args[0] for call in run.call_args_list]
    assert commands == [
        f'{constants.SKY_RAY_CMD} stop',
        f'{constants.SKY_RAY_CMD} down -y {_config_path()}',
    ]


def test_legacy_multinode_runs_ray_up_and_workers_only_down(skylet_configs_db):
    del skylet_configs_db
    config = dict(_CLUSTER_CONFIG, max_workers=2)
    read_yaml, provider_name, from_str = _patch_cluster_yaml(_legacy_cloud(),
                                                             config=config)

    with read_yaml, provider_name, from_str, \
         mock.patch.object(events.StopEvent, '_execute_hook_if_present'), \
         mock.patch.object(events.StopEvent, '_replace_yaml_for_stopping'), \
         mock.patch.object(
             events.cloud_vm_ray_backend,
             'write_ray_up_script_with_patched_launch_hash_fn',
             return_value='/tmp/ray_up.py'), \
         mock.patch.object(events.subprocess, 'run') as run:
        _stop_event()._stop_cluster(_autostop_config(down=True))

    commands = [call.args[0] for call in run.call_args_list]
    assert commands == [
        f'{constants.SKY_PYTHON_CMD} /tmp/ray_up.py',
        f'{constants.SKY_RAY_CMD} down -y --workers-only {_config_path()}',
        f'{constants.SKY_RAY_CMD} stop',
        f'{constants.SKY_RAY_CMD} down -y {_config_path()}',
    ]
