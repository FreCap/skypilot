"""Launch-time autostop policy for the execution layer."""

import typing
from typing import Any

from sky import clouds
from sky import exceptions
from sky import global_user_state
from sky.utils import controller_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import sky


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
