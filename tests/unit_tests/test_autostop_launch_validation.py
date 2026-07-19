"""Launch-time validation of auto{stop,down} feature requirements.

Incident: a one-time AWS spot cluster launched with `-i 30` (autostop
WITHOUT down) was accepted, and at idle time the skylet's StopInstances
call failed forever (AWS rejects stopping one-time spot), pinning the
cluster in AUTOSTOPPING while it kept billing. `sky autostop` validates
the STOP feature post-launch; the launch path only requested AUTOSTOP,
which AWS does not restrict for spot. These tests pin the launch-time
contract: non-down autostop requires STOP too.
"""
# pylint: disable=protected-access
import pathlib
import pickle
from unittest import mock

import pytest

import sky
from sky import clouds
from sky import exceptions
from sky import execution
from sky import resources as resources_lib
from sky.backends import cloud_vm_ray_backend as backend_lib
from sky.utils import status_lib

_FEATURES = clouds.CloudImplementationFeatures


@pytest.mark.parametrize('helper_name', [
    '_compute_set_autostop_args_for_hooks_only_relaunch',
    '_check_autostop_feasibility_early',
    'autostop_requested_features',
])
def test_execution_autostop_helper_identity_is_stable(helper_name):
    helper = getattr(execution, helper_name)
    assert helper.__module__ == execution.__name__
    assert pickle.loads(pickle.dumps(helper)) is helper


def test_non_down_autostop_requires_stop_feature():
    assert execution.autostop_requested_features(down=False) == {
        _FEATURES.AUTOSTOP,
        _FEATURES.STOP,
    }


def test_autodown_requires_only_autodown_feature():
    assert execution.autostop_requested_features(down=True) == {
        _FEATURES.AUTODOWN,
    }


def test_aws_spot_rejects_the_non_down_autostop_feature_set():
    # The gate that makes the launch-time request meaningful: AWS
    # declares STOP unsupported for spot, so the non-down feature set
    # must be rejected for a spot candidate...
    spot = resources_lib.Resources(cloud=clouds.AWS(), use_spot=True)
    with pytest.raises(exceptions.NotSupportedError):
        clouds.AWS().check_features_are_supported(
            spot, execution.autostop_requested_features(down=False))
    # ...while AUTOSTOP alone (the pre-fix request) passes -- the exact
    # gap that let the incident config through.
    clouds.AWS().check_features_are_supported(spot, {_FEATURES.AUTOSTOP})


def test_aws_ondemand_accepts_both_feature_sets():
    ondemand = resources_lib.Resources(cloud=clouds.AWS(), use_spot=False)
    clouds.AWS().check_features_are_supported(
        ondemand, execution.autostop_requested_features(down=False))
    clouds.AWS().check_features_are_supported(
        ondemand, execution.autostop_requested_features(down=True))


def _task_with(*resources):
    task = sky.Task()
    task.set_resources(set(resources))
    return task


def test_early_check_rejects_when_all_candidates_unstoppable():
    task = _task_with(resources_lib.Resources(cloud=clouds.AWS(),
                                              use_spot=True))
    with pytest.raises(exceptions.NotSupportedError):
        execution._check_autostop_feasibility_early(
            task,
            execution.autostop_requested_features(down=False),
            cluster_name='my-cluster')


def test_early_check_passes_with_one_stoppable_candidate():
    # Mixed any_of [spot, on-demand]: one supported candidate is enough;
    # the provisioner's per-resource feature filtering handles the rest.
    task = _task_with(
        resources_lib.Resources(cloud=clouds.AWS(), use_spot=True),
        resources_lib.Resources(cloud=clouds.AWS(), use_spot=False))
    execution._check_autostop_feasibility_early(
        task,
        execution.autostop_requested_features(down=False),
        cluster_name='my-cluster')


def test_early_check_inconclusive_with_cloud_agnostic_candidate():
    task = _task_with(resources_lib.Resources(use_spot=True))
    execution._check_autostop_feasibility_early(
        task,
        execution.autostop_requested_features(down=False),
        cluster_name='my-cluster')


def test_early_check_allows_autodown_on_spot():
    task = _task_with(resources_lib.Resources(cloud=clouds.AWS(),
                                              use_spot=True))
    execution._check_autostop_feasibility_early(
        task,
        execution.autostop_requested_features(down=True),
        cluster_name='my-cluster')


def _make_provisioner(requested):
    return backend_lib.RetryingVmProvisioner(
        log_dir='/tmp',
        dag=mock.Mock(),
        optimize_target=mock.Mock(),
        requested_features=requested,
        local_wheel_path=pathlib.Path('/tmp/wheel'),
        wheel_hash='',
        extra_launch_context={},
    )


def _provision_once(prev_cluster_status, prev_cluster_ever_up=None):
    """Drive provision_with_retries one iteration with a cloud stub whose
    feature check raises NotSupportedError; returns (provisioner,
    to_provision, raised exception)."""
    # unsafe=True: assert_launchable trips Mock's assert_* typo guard.
    to_provision = mock.Mock(unsafe=True)
    to_provision.assert_launchable.return_value = to_provision
    cloud_stub = mock.Mock()
    cloud_stub.check_features_are_supported.side_effect = (
        exceptions.NotSupportedError('stop unsupported for spot'))
    cloud_stub.get_active_user_identity.return_value = None
    to_provision.cloud = cloud_stub

    task = mock.Mock()
    task.is_controller_task.return_value = False
    task.num_nodes = 1
    task.resources = {to_provision}

    provisioner = _make_provisioner(
        execution.autostop_requested_features(down=False))
    config = backend_lib.RetryingVmProvisioner.ToProvisionConfig(
        cluster_name='t-cluster',
        resources=to_provision,
        num_nodes=1,
        prev_cluster_status=prev_cluster_status,
        prev_handle=mock.Mock(),
        prev_cluster_ever_up=(prev_cluster_status is not None
                              if prev_cluster_ever_up is None else
                              prev_cluster_ever_up),
        prev_config_hash=None,
    )
    raised = None
    teardown = mock.patch.object(backend_lib.CloudVmRayBackend,
                                 'teardown_no_lock').start()
    try:
        with mock.patch.object(
                backend_lib.optimizer.Optimizer,
                'optimize',
                side_effect=exceptions.ResourcesUnavailableError(
                    'exhausted')), \
             mock.patch.object(backend_lib,
                               '_format_provision_failure_blocks',
                               return_value=''):
            # The failure formatter renders Resources for humans; it is
            # presentation-only and chokes on the Mock candidate.
            try:
                provisioner.provision_with_retries(
                    task,
                    config,
                    dryrun=False,
                    stream_logs=False,
                    skip_unnecessary_provisioning=False)
            except Exception as e:  # pylint: disable=broad-except
                raised = e
    finally:
        # An exception raised while entering the with-block must not
        # leak the started teardown patch into subsequent tests.
        mock.patch.stopall()
    return provisioner, to_provision, raised, teardown


def test_provisioner_blocks_exact_candidate_not_cloud_wide():
    # A per-resource feature failure must block only the failing
    # candidate: a cloud-wide Resources(cloud=...) block matches only
    # NON-spot siblings (use_spot is never None) and would break
    # any_of [spot, on-demand] fallback entirely.
    provisioner, to_provision, raised, _ = _provision_once(
        prev_cluster_status=None)
    assert isinstance(raised, exceptions.ResourcesUnavailableError)
    assert provisioner._blocked_resources == {to_provision}


def test_provisioner_surfaces_clean_error_for_existing_cluster():
    # Relaunching an existing UP cluster with a config its launched
    # resources cannot satisfy must surface the same clean
    # NotSupportedError `sky autostop` gives -- not the INIT-only
    # AssertionError from the loop tail, and no cloud-wide poisoning.
    provisioner, _, raised, teardown = _provision_once(
        prev_cluster_status=status_lib.ClusterStatus.UP)
    assert isinstance(raised, exceptions.NotSupportedError)
    assert not provisioner._blocked_resources
    # The clean-error path performs no teardown.
    teardown.assert_not_called()


def test_existing_cluster_error_does_not_chain_the_marker():
    # The internal marker must not appear in the propagated exception's
    # chain: failed API requests serialize the full stacktrace to
    # clients under debug.
    _, _, raised, _ = _provision_once(
        prev_cluster_status=status_lib.ClusterStatus.UP)
    assert isinstance(raised, exceptions.NotSupportedError)
    assert raised.__cause__ is None
    assert raised.__suppress_context__


def test_start_rejects_non_down_autostop_on_unstoppable_resources():
    # `sky start --force -i N` on an UP one-time-spot cluster reaches
    # set_autostop with no validation anywhere else on the path -- the
    # same incident class as the launch gap, through a different door.
    from sky import core  # pylint: disable=import-outside-toplevel
    spot = resources_lib.Resources(cloud=clouds.AWS(), use_spot=True)
    handle = mock.Mock()
    # Child mocks do not inherit unsafe=True (needed for the
    # assert_-prefixed method name).
    handle.launched_resources = mock.Mock(unsafe=True)
    handle.launched_resources.assert_launchable.return_value = spot
    record = {
        'status': status_lib.ClusterStatus.UP,
        'handle': handle,
        'autostop': -1,
        'to_down': False,
    }
    with mock.patch.object(
            core.backend_utils,
            'refresh_cluster_record',
            return_value=record), \
         mock.patch.object(core.backend_utils,
                           'get_backend_from_handle',
                           return_value=mock.Mock(
                               spec=backend_lib.CloudVmRayBackend)):
        with pytest.raises(exceptions.NotSupportedError):
            core._start('t-cluster',
                        idle_minutes_to_autostop=30,
                        down=False,
                        force=True)
    # (Autodown on the same spot resources passing the gate is covered
    # by test_early_check_allows_autodown_on_spot / the AWS feature
    # tests; driving _start further needs the whole provision stack.)


def test_provisioner_lets_never_up_init_cluster_fail_over():
    # NEVER-UP INIT is the retryable state (e.g. a failed first spot
    # attempt): a feature failure must NOT be treated as
    # non-failoverable -- the tail's INIT branch resets to a fresh
    # launch so the task can fall over to candidates that do support
    # the feature (on-demand).
    provisioner, _, raised, teardown = _provision_once(
        prev_cluster_status=status_lib.ClusterStatus.INIT,
        prev_cluster_ever_up=False)
    assert not isinstance(raised, exceptions.NotSupportedError)
    assert not isinstance(raised, AssertionError)
    # The stubbed optimizer ends the loop after the INIT reset pass;
    # what matters is that the INIT branch ran (no INIT-only assert, no
    # clean-error raise) and that this pass added NO block -- the old
    # broad handler would have cloud-wide poisoned here.
    assert isinstance(raised, exceptions.ResourcesUnavailableError)
    assert provisioner._blocked_resources == set()
    # The tail's INIT reset assumes the old cluster was terminated; the
    # marker path must do that cleanup itself (feature check fails
    # before _retry_zones runs). Unconditional terminate: a STOP
    # teardown is impossible on the very resources that tripped the
    # check.
    teardown.assert_called_once()
    assert teardown.call_args.kwargs['terminate'] is True


def test_provisioner_ever_up_init_cluster_gets_clean_error():
    # An EVER-UP INIT cluster (e.g. ctrl-c mid-restart of a previously
    # UP spot cluster) must take the clean-error path like UP/STOPPED:
    # _yield_zones forbids its failover to preserve data, and a
    # stop-teardown would fail on exactly the resources that tripped
    # the check.
    provisioner, _, raised, teardown = _provision_once(
        prev_cluster_status=status_lib.ClusterStatus.INIT,
        prev_cluster_ever_up=True)
    assert isinstance(raised, exceptions.NotSupportedError)
    teardown.assert_not_called()
    assert not provisioner._blocked_resources


def _start_with_stored_autostop(spot_resources, stored_autostop,
                                stored_to_down):
    """Drive core._start with NO explicit -i and a stored record value."""
    from sky import core  # pylint: disable=import-outside-toplevel
    handle = mock.Mock()
    handle.launched_resources = spot_resources
    handle.launched_nodes = 1
    backend = mock.Mock(spec=backend_lib.CloudVmRayBackend)
    backend.provision.return_value = (handle, None)
    record = {
        'status': status_lib.ClusterStatus.STOPPED,
        'handle': handle,
        'autostop': stored_autostop,
        'to_down': stored_to_down,
    }
    with mock.patch.object(
            core.backend_utils,
            'refresh_cluster_record',
            return_value=record), \
         mock.patch.object(core.backend_utils,
                           'get_backend_from_handle',
                           return_value=backend), \
         mock.patch.object(core.global_user_state,
                           'get_cluster_from_name') as second_read:
        core._start('t-cluster', idle_minutes_to_autostop=None, down=False)
    # Single-snapshot invariant: the stored autostop must come from the
    # already-refreshed record, with no second cluster-table read that a
    # concurrent `sky autostop` could race against.
    second_read.assert_not_called()
    return backend


def test_start_skips_restoring_unsupported_stored_autostop():
    # A stored non-down autostop on one-time spot predates the
    # launch-time validation: `sky start` (no -i) must not silently
    # re-arm the broken idle timer -- but must not fail the start
    # either (the user asked to start the cluster, not to be blocked
    # by a legacy value).
    spot = resources_lib.Resources(cloud=clouds.AWS(),
                                   instance_type='g6.4xlarge',
                                   use_spot=True)
    backend = _start_with_stored_autostop(spot,
                                          stored_autostop=30,
                                          stored_to_down=False)
    backend.set_autostop.assert_not_called()


def test_start_restores_supported_stored_autostop():
    # The same restore on stoppable resources keeps working.
    ondemand = resources_lib.Resources(cloud=clouds.AWS(),
                                       instance_type='g6.4xlarge',
                                       use_spot=False)
    backend = _start_with_stored_autostop(ondemand,
                                          stored_autostop=30,
                                          stored_to_down=False)
    backend.set_autostop.assert_called_once()
