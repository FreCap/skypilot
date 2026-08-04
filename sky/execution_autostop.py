"""Launch-time autostop policy for the execution layer."""

import logging
import typing
from typing import Any

import colorama

from sky import clouds
from sky import exceptions
from sky import global_user_state
from sky.utils import controller_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import sky
    from sky import backends
    from sky.skylet import autostop_lib


def _compute_set_autostop_args_for_hooks_only_relaunch(
        cluster_name: str, hooks_payload: list[dict[str,
                                                    Any]]) -> dict[str, Any]:
    """Build set_autostop kwargs when a re-launch updates only hooks.

    The `elif hooks_payload is not None:` branch in `_execute` updates
    stored hooks on the skylet without the user re-passing
    ``--idle-minutes-to-autostop`` (or YAML autostop) — for example,
    re-launching a cluster with a new preemption hook. Previously this
    path passed ``idle_minutes_to_autostop=-1`` (= "unset autostop"),
    which silently wiped any prior autostop config.

    We instead read the prior autostop value + ``to_down`` flag from
    local state and pass them through, so re-launches that change only
    hooks preserve the cluster's existing autostop. ``wait_for`` is not
    persisted client-side; on this path it defaults back to
    ``jobs_and_ssh`` (the documented default), which is the same value a
    user would get on a fresh ``sky autostop``.

    The helper is named ``set_autostop`` because the underlying RPC
    that propagates hooks IS ``SetAutostop`` (hooks ride on it for
    wire-compat reasons — see
    ``sky/schemas/proto/autostopv1.proto``). If a future PR adds a
    dedicated ``SetHooks`` RPC, this helper's name and call site can
    track that rename.
    """
    record = global_user_state.get_cluster_from_name(cluster_name)
    if record is None:
        prior_idle_minutes = -1
        prior_to_down = False
    else:
        prior_idle_minutes = record.get('autostop', -1)
        if prior_idle_minutes is None:
            prior_idle_minutes = -1
        prior_to_down = bool(record.get('to_down', False))
    return dict(
        idle_minutes_to_autostop=prior_idle_minutes,
        wait_for=None,
        down=prior_to_down,
        hooks=hooks_payload,
    )


def apply_launch_autostop(
    backend: 'backends.CloudVmRayBackend',
    handle: 'backends.CloudVmRayResourceHandle',
    idle_minutes_to_autostop: int,
    wait_for: 'autostop_lib.AutostopWaitFor | None',
    down: bool,
    *,
    hook: str | None,
    hook_timeout: int | None,
    hooks: list[dict[str, Any]] | None,
    refusal_is_fatal: bool,
    job_logger: logging.Logger,
) -> None:
    """Arms the launch's autostop config, handling a node that refuses it.

    A node refuses (``NotSupportedError``) when it provably cannot execute
    the requested teardown -- see ``sky/skylet/autostop_preflight.py``. The
    node reports capability; deciding whether that is fatal is this layer's
    job, because only the launch knows *who asked* for the autodown:

    - The user did (``sky launch -i N --down``): the promise cannot be
      kept, so fail rather than hand back a cluster that will sit there
      billing with a serene ``AUTOSTOP  Nm (down)`` in ``sky status``.
    - SkyPilot did: managed-job clusters and controllers get an autodown
      nobody asked for, purely as a leak backstop in case their controller
      dies (see ``jobs/recovery_strategy.py``, and the Kubernetes SkyServe
      controller force-convert in ``CloudVmRayBackend.set_autostop``).
      Failing those launches would trade a backstop that was never going to
      fire for an outage. They proceed with autostop explicitly cleared, so
      nothing advertises a teardown that cannot happen.
    """
    try:
        backend.set_autostop(handle,
                             idle_minutes_to_autostop,
                             wait_for,
                             down,
                             hook=hook,
                             hook_timeout=hook_timeout,
                             hooks=hooks)
        return
    except exceptions.NotSupportedError:
        if refusal_is_fatal:
            raise
    job_logger.warning(
        f'{colorama.Fore.YELLOW}Cluster {handle.cluster_name!r} cannot tear '
        'itself down, so the idle-teardown backstop is disabled for it. It '
        'will be torn down as usual when its work finishes; if the '
        'controller dies first, the cluster must be removed manually.'
        f'{colorama.Style.RESET_ALL}')
    # -1 cancels autostop. The hooks list rides along so it still lands on
    # the node -- hooks fire on preemption/down independently of autostop.
    # The deprecated single-hook fields deliberately do not: they were
    # picked for the `down` event, and only a pre-v7 skylet reads them --
    # which is also a skylet with no preflight, so it never refuses and
    # never reaches here.
    backend.set_autostop(handle, -1, wait_for, down=False, hooks=hooks)


def autostop_requested_features(
        down: bool) -> set[clouds.CloudImplementationFeatures]:
    """Cloud features a launch-time auto{stop,down} config requires.

    Autostop WITHOUT down ultimately performs a STOP, so any candidate
    that cannot stop cannot satisfy it -- most notably AWS one-time spot
    instances, which reject StopInstances. Without requesting STOP here,
    the launch accepts the config and the skylet's stop attempt then
    fails forever at idle time, leaving the cluster in AUTOSTOPPING
    while it keeps billing (core.autostop() validates STOP for the
    post-launch path; this mirrors it for launch, and set_autostop in
    the backend deliberately trusts its callers). The provisioner keeps
    its Kubernetes/RunPod CONTROLLER carve-out, where set_autostop
    force-converts to autodown/no-op.
    """
    if down:
        return {clouds.CloudImplementationFeatures.AUTODOWN}
    return {
        clouds.CloudImplementationFeatures.AUTOSTOP,
        clouds.CloudImplementationFeatures.STOP,
    }


def _check_autostop_feasibility_early(task: 'sky.Task', autostop_features: set[
    clouds.CloudImplementationFeatures], cluster_name: str | None) -> None:
    """Fail fast when NO candidate can satisfy the autostop config.

    Gives the crisp `sky autostop`-style NotSupportedError up front
    (e.g. `-i 30` without --down on an AWS one-time spot request)
    instead of surfacing it as provision failover noise after an
    optimizer round-trip. Only conclusive when every candidate names a
    cloud: a cloud-agnostic candidate might optimize onto a supported
    cloud, and a single supported candidate means the provisioner
    feature filtering can still do its job. Controllers skip entirely
    -- the provisioner carves autostop features out for them on
    Kubernetes/RunPod (set_autostop force-converts).
    """
    if cluster_name is not None and controller_utils.Controllers.from_name(
            cluster_name) is not None:
        return
    first_error: Exception | None = None
    # Stable iteration order: task.resources is a set, and which
    # candidate's error surfaces as first_error must not vary run to
    # run when several fail for different reasons.
    for resource in sorted(task.resources, key=str):
        if resource.cloud is None:
            return
        try:
            resource.cloud.check_features_are_supported(resource,
                                                        autostop_features)
            return
        except exceptions.NotSupportedError as e:
            if first_error is None:
                first_error = e
    if first_error is not None:
        with ux_utils.print_exception_no_traceback():
            raise first_error
