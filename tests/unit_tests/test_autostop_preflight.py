"""Tests for the arm-time autodown capability preflight.

A Kubernetes pod whose ServiceAccount cannot delete pods can accept an
autodown config perfectly happily -- storing it needs no RBAC -- and then
fail the actual teardown every 60s forever, invisibly, because
`SkyletEvent.run()` swallows the exception and `_stop_cluster` clears the
autostopping indicator on its way out. The cluster stays `UP` with a
serene `AUTOSTOP  1h (down)` and bills indefinitely.

These tests pin the split the fix introduces. The node answers only the
capability question, and only on an *explicit* denial (never on a probe
failure); `set_autostop` refuses such a config without persisting
anything; and the launch layer decides whether that refusal is fatal --
it is when the user asked for the autodown, and not when SkyPilot added
it as a leak backstop.

See docs/designs/kubernetes-autodown-capability-preflight.md.
"""
# pylint: disable=protected-access
from unittest import mock

import grpc
import pytest

from sky import exceptions
from sky import execution_autostop
from sky.backends import skylet_rpc
from sky.skylet import autostop_lib
from sky.skylet import autostop_preflight
from sky.skylet import configs
from sky.skylet import runtime_utils

_K8S_CLUSTER_CONFIG = {
    'cluster_name': 'cluster-abcd1234',
    'provider': {
        'module': 'sky.provision.kubernetes'
    },
}


@pytest.fixture(name='skylet_configs_db')
def skylet_configs_db_fixture(tmp_path, monkeypatch):
    """Points the skylet configs sqlite DB at a tmp dir.

    `configs._DB_PATH` is lazily set by `init_db` via
    `runtime_utils.get_runtime_dir_path`, so reset it and redirect the path
    helper. This keeps the stored-config assertions real rather than mocked.
    """
    monkeypatch.setattr(configs, '_DB_PATH', None)
    db_dir = tmp_path / 'sky-runtime'
    db_dir.mkdir()
    monkeypatch.setattr(runtime_utils,
                        'get_runtime_dir_path',
                        lambda relpath='': str(db_dir / relpath.lstrip('/')))
    return db_dir


def _review(allowed):
    """A SelfSubjectAccessReview response with the given verdict."""
    return mock.Mock(status=mock.Mock(allowed=allowed))


def _in_a_pod(cluster_config=None, namespace='ns'):
    """Patches the node to look like a SkyPilot Kubernetes pod."""
    config = _K8S_CLUSTER_CONFIG if cluster_config is None else cluster_config
    return [
        mock.patch.object(autostop_preflight.os.path,
                          'exists',
                          return_value=True),
        mock.patch.object(autostop_preflight,
                          '_read_node_cluster_config',
                          return_value=config),
        mock.patch.object(autostop_preflight,
                          '_read_namespace',
                          return_value=namespace),
    ]


def _run_preflight(review_result, idle_minutes=60, down=True, **kwargs):
    """Runs the preflight in a pod, with `create_self_subject_access_review`
    returning (or raising) `review_result`."""
    authz = mock.Mock()
    if isinstance(review_result, Exception):
        authz.create_self_subject_access_review.side_effect = review_result
    else:
        authz.create_self_subject_access_review.return_value = review_result
    patches = _in_a_pod(**kwargs) + [
        mock.patch.object(
            autostop_preflight.kubernetes, 'authz_api', return_value=authz),
        mock.patch.object(autostop_preflight.kubernetes,
                          'in_cluster_context_name',
                          return_value='in-cluster'),
    ]
    with mock.patch.multiple(autostop_preflight.kubernetes.kubernetes.client,
                             V1ResourceAttributes=mock.DEFAULT,
                             V1SelfSubjectAccessReview=mock.DEFAULT,
                             V1SelfSubjectAccessReviewSpec=mock.DEFAULT,
                             create=True):
        for patch in patches:
            patch.start()
        try:
            return autostop_preflight.autodown_denial_reason(
                idle_minutes, down), authz
        finally:
            for patch in reversed(patches):
                patch.stop()


# --- The preflight verdict --------------------------------------------------


def test_explicit_denial_is_reported():
    reason, _ = _run_preflight(_review(allowed=False))
    assert reason is not None


def test_allowed_is_not_reported():
    reason, _ = _run_preflight(_review(allowed=True))
    assert reason is None


def test_unissuable_review_degrades_to_allowed():
    """A probe that cannot run must never cost a cluster its autostop."""
    reason, _ = _run_preflight(RuntimeError('no selfsubjectaccessreviews'))
    assert reason is None


def test_empty_review_status_degrades_to_allowed():
    reason, _ = _run_preflight(mock.Mock(status=None))
    assert reason is None
    reason, _ = _run_preflight(_review(allowed=None))
    assert reason is None


# --- What the preflight does NOT touch --------------------------------------


@pytest.mark.parametrize(('idle_minutes', 'down'), [
    (60, False),
    (-1, True),
    (-1, False),
])
def test_skipped_unless_arming_an_autodown(idle_minutes, down):
    """A cancel, or a plain autostop, needs no teardown capability."""
    reason, authz = _run_preflight(_review(allowed=False),
                                   idle_minutes=idle_minutes,
                                   down=down)
    assert reason is None
    authz.create_self_subject_access_review.assert_not_called()


def test_skipped_when_not_in_a_pod():
    """Off Kubernetes there is no identity to review -- and the kubernetes
    client need not even be installed."""
    with mock.patch.object(autostop_preflight.os.path,
                           'exists',
                           return_value=False):
        with mock.patch.object(autostop_preflight,
                               '_teardown_goes_through_kubernetes') as probe:
            assert autostop_preflight.autodown_denial_reason(60, True) is None
    probe.assert_not_called()


def test_skipped_for_a_non_kubernetes_node():
    """A VM that happens to carry a service-account token still tears itself
    down through its own provisioner."""
    aws_config = {
        'cluster_name': 'cluster-abcd1234',
        'provider': {
            'module': 'sky.provision.aws'
        },
    }
    reason, authz = _run_preflight(_review(allowed=False),
                                   cluster_config=aws_config)
    assert reason is None
    authz.create_self_subject_access_review.assert_not_called()


def test_unreadable_cluster_yaml_skips_the_check():
    with mock.patch.object(autostop_preflight.os.path,
                           'exists',
                           return_value=True):
        with mock.patch.object(autostop_preflight,
                               '_read_node_cluster_config',
                               return_value=None):
            assert autostop_preflight.autodown_denial_reason(60, True) is None


# --- set_autostop honors the verdict ----------------------------------------


def test_set_autostop_refuses_and_persists_nothing(skylet_configs_db):
    del skylet_configs_db  # Used for its side effect.
    with mock.patch.object(autostop_preflight,
                           'autodown_denial_reason',
                           return_value='denied'):
        with pytest.raises(exceptions.NotSupportedError):
            autostop_lib.set_autostop(
                idle_minutes=60,
                backend='cloud-vm-ray',
                wait_for=autostop_lib.DEFAULT_AUTOSTOP_WAIT_FOR,
                down=True)
    # A stored config is what makes `sky status` advertise an autostop, and
    # what StopEvent would act on. Neither must exist.
    assert autostop_lib.get_autostop_config().autostop_idle_minutes < 0


def test_set_autostop_stores_the_config_when_allowed(skylet_configs_db):
    del skylet_configs_db  # Used for its side effect.
    with mock.patch.object(autostop_preflight,
                           'autodown_denial_reason',
                           return_value=None):
        autostop_lib.set_autostop(
            idle_minutes=60,
            backend='cloud-vm-ray',
            wait_for=autostop_lib.DEFAULT_AUTOSTOP_WAIT_FOR,
            down=True)
    config = autostop_lib.get_autostop_config()
    assert config.autostop_idle_minutes == 60
    assert config.down


# --- who the refusal is fatal for -------------------------------------------


def _apply(refusal_is_fatal, refuse=True):
    """Runs the launch-time policy against a backend that refuses the arm."""
    backend = mock.Mock()
    if refuse:
        backend.set_autostop.side_effect = [
            exceptions.NotSupportedError('cannot autodown'), None
        ]
    handle = mock.Mock(cluster_name='c')
    execution_autostop.apply_launch_autostop(backend,
                                             handle,
                                             60,
                                             None,
                                             True,
                                             hook=None,
                                             hook_timeout=None,
                                             hooks=None,
                                             refusal_is_fatal=refusal_is_fatal,
                                             job_logger=mock.Mock())
    return backend


def test_a_user_requested_autodown_that_cannot_run_fails_the_launch():
    with pytest.raises(exceptions.NotSupportedError):
        _apply(refusal_is_fatal=True)


def test_a_backstop_autodown_degrades_to_no_autostop():
    """Managed-job clusters and controllers get an autodown nobody asked
    for; failing their launch over a backstop that was never going to fire
    would trade a small leak for an outage. They proceed -- but with
    autostop cleared, so nothing advertises a teardown that cannot
    happen."""
    backend = _apply(refusal_is_fatal=False)
    assert backend.set_autostop.call_count == 2
    retry = backend.set_autostop.call_args
    assert retry.args[1] == -1  # idle_minutes: cancel autostop
    assert retry.kwargs['down'] is False


def test_an_accepted_arm_is_applied_once():
    backend = _apply(refusal_is_fatal=True, refuse=False)
    assert backend.set_autostop.call_count == 1


# --- the refusal survives the wire ------------------------------------------


def test_refusal_reaches_the_client_without_retries():
    """The node's refusal is permanent, so the transport must surface it as
    a NotSupportedError on the first attempt rather than spending retries
    and reporting the skylet as unavailable."""
    err = grpc.RpcError()
    err.code = lambda: grpc.StatusCode.FAILED_PRECONDITION  # type: ignore
    err.details = lambda: 'cannot autodown'  # type: ignore

    attempts = []

    def call():
        attempts.append(1)
        raise err

    with pytest.raises(exceptions.NotSupportedError):
        skylet_rpc.invoke_skylet_with_retries(call)
    assert len(attempts) == 1
