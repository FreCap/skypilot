"""Logic-only tests for the external load balancer RBAC preflight.

These tests exercise only the branching logic (raise / no-raise and which
SelfSubjectAccessReview calls are issued). They never assert on log or
exception message text.
"""
from unittest import mock

import pytest

from sky.serve import lb_rbac_preflight


def _make_review(allowed):
    review = mock.MagicMock()
    review.status.allowed = allowed
    return review


def _install(monkeypatch,
             authz_client,
             external=True,
             incluster=True,
             namespace='skypilot'):
    """Patch out the environment probes and the k8s client for the preflight."""
    monkeypatch.setattr(lb_rbac_preflight.serve_utils,
                        'is_external_load_balancer_mode', lambda: external)
    monkeypatch.setattr(lb_rbac_preflight.kubernetes_utils,
                        'is_incluster_config_available', lambda: incluster)
    monkeypatch.setattr(lb_rbac_preflight.kubernetes_utils,
                        'get_kube_config_context_namespace',
                        lambda ctx: namespace)
    monkeypatch.setattr(lb_rbac_preflight.kubernetes, 'in_cluster_context_name',
                        lambda: 'in-cluster')
    # Body construction goes through kubernetes.kubernetes.client.*; the real
    # kubernetes package is not installed, so stub it with a MagicMock.
    monkeypatch.setattr(lb_rbac_preflight.kubernetes, 'kubernetes',
                        mock.MagicMock())
    monkeypatch.setattr(lb_rbac_preflight.kubernetes,
                        'authz_api',
                        lambda ctx=None: authz_client)


# 2 resources (deployments, services) x 4 verbs (create/get/list/delete).
_EXPECTED_REVIEW_CALLS = 8


def test_all_allowed_does_not_raise(monkeypatch):
    authz = mock.MagicMock()
    authz.create_self_subject_access_review.return_value = _make_review(True)
    _install(monkeypatch, authz)

    lb_rbac_preflight.check_lb_rbac_preflight()

    assert (authz.create_self_subject_access_review.call_count ==
            _EXPECTED_REVIEW_CALLS)


def test_one_verb_denied_raises(monkeypatch):
    authz = mock.MagicMock()
    # First review allowed, then one denied; the loop should collect it and
    # ultimately raise.
    authz.create_self_subject_access_review.side_effect = [
        _make_review(True),
        _make_review(False),
        _make_review(True),
        _make_review(True),
        _make_review(True),
        _make_review(True),
        _make_review(True),
        _make_review(True),
    ]
    _install(monkeypatch, authz)

    with pytest.raises(RuntimeError):
        lb_rbac_preflight.check_lb_rbac_preflight()


def test_review_api_error_degrades_gracefully(monkeypatch):
    authz = mock.MagicMock()
    # We cannot even issue the review (e.g. no selfsubjectaccessreviews:create).
    authz.create_self_subject_access_review.side_effect = Exception('Forbidden')
    _install(monkeypatch, authz)

    # Must not raise: graceful degradation.
    lb_rbac_preflight.check_lb_rbac_preflight()

    # Bails out after the very first failed review.
    assert authz.create_self_subject_access_review.call_count == 1


def test_not_external_lb_mode_skips(monkeypatch):
    authz = mock.MagicMock()
    _install(monkeypatch, authz, external=False)

    lb_rbac_preflight.check_lb_rbac_preflight()

    authz.create_self_subject_access_review.assert_not_called()


def test_not_in_cluster_skips(monkeypatch):
    authz = mock.MagicMock()
    _install(monkeypatch, authz, incluster=False)

    lb_rbac_preflight.check_lb_rbac_preflight()

    authz.create_self_subject_access_review.assert_not_called()
