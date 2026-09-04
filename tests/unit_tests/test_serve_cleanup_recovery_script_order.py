"""Regression tests for owner-fenced teardown finalization.

``sky/serve/service.py::_cleanup`` used to delete the
``serve_ha_recovery_script`` row on its very first line, *before* the
(seconds-to-minutes) replica teardown.
If the controller pod was then killed mid-teardown (HA pod move / node drain),
the durable service row survived but its recovery script was gone, so
``ha_recovery_for_consolidation_mode`` logged 'recovery script does not exist.
Skipping recovery' forever and stranded the service with replicas still
consuming resources.

The recovery script must outlive replica teardown so a crash partway through
leaves recovery able to respawn the controller and re-run cleanup. A successful
finalizer removes it atomically with the exact service row. Generic persistent
failures publish ``FAILED_CLEANUP`` and remove the script; typed provider
uncertainty retains it so a later exact census can finish automatically.
"""
# pylint: disable=protected-access
import json
import threading
import types
from unittest import mock
import uuid

import pytest

from sky import exceptions
from sky.serve import non_pool_launch_reconciliation
from sky.serve import ordinary_launch_binding
from sky.serve import replica_managers
from sky.serve import resource_actions
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import service
from sky.utils import common_utils


def _replica(replica_id: int) -> replica_managers.ReplicaInfo:
    return replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'c{replica_id}',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)


def _failed_paid_provider_cleanup_replica(
        replica_id: int) -> replica_managers.ReplicaInfo:
    """Build the durable row left by a timed-out exact provider cleanup."""
    info = _replica(replica_id)
    info.is_spot = True
    info.is_zero_cost = False
    info.reserved_fill = False
    info.paid_capacity_pool_key = json.dumps(
        {
            'accelerators': [['l4', 1]],
            'cloud': 'aws',
            'instance_type': 'g6.2xlarge',
            'num_nodes': 1,
            'region': 'eu-south-2',
            'use_spot': True,
            'version': 1,
            'workspace': 'w',
            'zone': 'eu-south-2a',
        },
        sort_keys=True,
        separators=(',', ':'))
    non_pool_launch_reconciliation.apply_immediate_provider_cleanup_replica_marker(
        info)
    info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED
    assert ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
        info)
    return info


def _paid_cleanup_case(
    replica_id: int,
) -> tuple[replica_managers.ReplicaInfo,
           ordinary_launch_binding.BoundNonPoolLaunchContext]:
    info = _replica(replica_id)
    info.is_spot = True
    info.is_zero_cost = False
    info.reserved_fill = False
    info.paid_capacity_pool_key = json.dumps(
        {
            'accelerators': [['l4', 1]],
            'cloud': 'aws',
            'instance_type': 'g6.2xlarge',
            'num_nodes': 1,
            'region': 'eu-south-2',
            'use_spot': True,
            'version': 1,
            'workspace': 'w',
            'zone': 'eu-south-2a',
        },
        sort_keys=True,
        separators=(',', ':'))
    non_pool_launch_reconciliation.apply_immediate_provider_cleanup_replica_marker(
        info)
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
        authorization_reference=f'paid-capacity:{replica_id}',
        authorization_generation=7,
        authorization_payload={'pool_key': info.paid_capacity_pool_key})
    context = ordinary_launch_binding.BoundNonPoolLaunchContext(
        association_id=uuid.UUID(int=100 + replica_id),
        request_id=f'request-{replica_id}',
        service_name='svc',
        replica_id=replica_id,
        replica_record_id=uuid.UUID(info.replica_record_id),
        launch_generation=1,
        input_digest=f'{replica_id:x}'.zfill(64),
        profile=profile,
        capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        capability_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=(
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))
    return info, context


def _patch_common(monkeypatch, events, replicas):
    """Wire up _cleanup's collaborators to record an ordered event log."""
    monkeypatch.setattr(service.time, 'sleep', lambda *_a, **_k: None)
    monkeypatch.setattr(serve_state, 'get_service_from_name', lambda svc: None)
    monkeypatch.setattr(serve_state, 'service_owner_matches',
                        lambda *args, **kwargs: True)
    monkeypatch.setattr(service.serve_utils, 'lifecycle_lock_is_valid',
                        lambda lock: True)
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: list(replicas))
    monkeypatch.setattr(
        serve_state, 'get_replica_resource_action_identities', lambda svc,
        replica_ids: {replica_id: None for replica_id in replica_ids})
    cluster_names = {replica.cluster_name for replica in replicas}
    monkeypatch.setattr(
        service.serve_utils, 'get_existing_replica_cluster_names',
        lambda replica_infos: cluster_names.intersection(
            replica.cluster_name for replica in replica_infos))
    monkeypatch.setattr(serve_state, 'add_or_update_replica',
                        lambda *a, **k: None)
    monkeypatch.setattr(serve_state, 'remove_replica', lambda *a, **k: None)
    def _finalize_paid(_service_name, replica_id, _record_id, _cluster_name,
                       **_kwargs):
        info = next((replica for replica in replicas
                     if replica.replica_id == replica_id), None)
        if info is None:
            return False
        replicas.remove(info)
        return True

    monkeypatch.setattr(replica_managers,
                        'finalize_projected_paid_provider_absence',
                        _finalize_paid)
    monkeypatch.setattr(serve_state, 'get_service_versions', lambda svc: [])

    def _reserve(_service_name, candidates, **_kwargs):
        replicas_by_id = {replica.replica_id: replica for replica in replicas}
        admitted = {}
        for replica_id, _ in candidates:
            replica = replicas_by_id[replica_id]
            replica.status_property.sky_down_status = (
                common_utils.ProcessStatus.RUNNING)
            admitted[replica_id] = replica
        return admitted

    monkeypatch.setattr(serve_state,
                        'reserve_replica_teardowns_running_if_capacity',
                        _reserve)
    monkeypatch.setattr(replica_managers.kueue_lane_observer,
                        'project_exact_pod_absence_after_teardown',
                        lambda *_args, **_kwargs: False)
    monkeypatch.setattr(serve_state, 'remove_ha_recovery_script',
                        lambda svc: events.append('remove_recovery_script'))


def test_cleanup_preserves_recovery_script_through_replica_teardown(
        monkeypatch):
    """The recovery script remains durable throughout replica teardown."""
    events = []

    def _terminate(cluster_name,
                   continue_guard=None,
                   expected_cluster_record_uuid=None):
        assert continue_guard is not None
        assert continue_guard()
        assert expected_cluster_record_uuid is None
        events.append(f'teardown:{cluster_name}')

    monkeypatch.setattr(replica_managers, 'terminate_cluster', _terminate)
    _patch_common(monkeypatch, events, [_replica(1)])

    failed = service._cleanup('svc', False, 'incarnation-a', 123, None,
                              mock.Mock())

    assert failed is False
    assert events == ['teardown:c1']


def test_cleanup_uses_exact_scoped_cluster_identity_for_long_name(monkeypatch):
    """Truncating a scoped cluster prefix must not make cleanup miss it."""
    events = []
    service_name = 's' * 63
    info = _replica(1)
    info.cluster_name = serve_utils.generate_replica_cluster_name(
        service_name, 1, 'incarnation-a')
    assert not info.cluster_name.startswith(service_name)

    def _terminate(cluster_name,
                   continue_guard=None,
                   expected_cluster_record_uuid=None):
        assert continue_guard is not None and continue_guard()
        assert expected_cluster_record_uuid is None
        events.append(f'teardown:{cluster_name}')

    monkeypatch.setattr(replica_managers, 'terminate_cluster', _terminate)
    _patch_common(monkeypatch, events, [info])

    failed = service._cleanup(service_name,
                              False,
                              'incarnation-a',
                              123,
                              None,
                              mock.Mock(),
                              resource_scope='incarnation-a')

    assert failed is False
    assert events == [f'teardown:{info.cluster_name}']


# --- recovery must resume teardown, not resurrect a torn-down service ---


def _svc(status):
    return {'status': status}


def test_should_resume_teardown():
    """Teardown is resumed only on a recovery run of a service left in a
    teardown status; a healthy (e.g. READY) service is recovered normally and
    a fresh run never resumes teardown."""
    assert service._should_resume_teardown(
        True, _svc(serve_state.ServiceStatus.SHUTTING_DOWN)) is True
    assert service._should_resume_teardown(
        True, _svc(serve_state.ServiceStatus.FAILED_CLEANUP)) is True
    assert service._should_resume_teardown(
        True, _svc(serve_state.ServiceStatus.READY)) is False
    assert service._should_resume_teardown(False, None) is False
    assert service._should_resume_teardown(
        False, _svc(serve_state.ServiceStatus.SHUTTING_DOWN)) is False


def _patch_finalize(monkeypatch, calls):
    monkeypatch.setattr(serve_state, 'get_replica_infos', lambda _svc: [])
    monkeypatch.setattr(
        service.serve_utils, 'quiesce_service_replica_launch_requests',
        lambda *a, **k: calls.append(('quiesce_launches', a[0])) or True)
    monkeypatch.setattr(
        serve_state, 'acknowledge_service_controller_teardown_if_owner',
        lambda *a, **k: calls.append(('begin_teardown', a[0])) or True)
    monkeypatch.setattr(service.serve_utils, 'get_service_lifecycle_lock',
                        lambda name, **_kwargs: mock.MagicMock())
    monkeypatch.setattr(service.serve_utils, 'lifecycle_lock_is_valid',
                        lambda lock: True)
    monkeypatch.setattr(serve_state, 'service_owner_matches',
                        lambda *a, **k: True)
    monkeypatch.setattr(serve_state,
                        'set_service_status_and_active_versions_if_owner',
                        lambda *a, **k: calls.append(('status', a[4])) or True)
    monkeypatch.setattr(serve_state, 'remove_service_completely',
                        lambda *a, **k: calls.append(('removed', a[0])) or True)
    monkeypatch.setattr(
        serve_state, 'remove_ha_recovery_script_if_owner',
        lambda *a, **k: calls.append(('remove_script', a[0])) or True)
    monkeypatch.setattr(service.lb_k8s, 'get_api_deployment_owner_uid',
                        lambda **_kwargs: 'api-deployment-uid')
    monkeypatch.setattr(service.lb_k8s, 'delete_lb_objects',
                        lambda *a, **k: calls.append(('delete_lb', a[0])))
    monkeypatch.setattr(service, '_cleanup_task_run_script', lambda jid: None)


def test_finalize_removes_service_on_clean_teardown(monkeypatch):
    calls = []
    monkeypatch.setattr(service, '_cleanup', lambda *a, **k: False)
    _patch_finalize(monkeypatch, calls)

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert ('removed', 'svc') in calls
    assert ('status', serve_state.ServiceStatus.FAILED_CLEANUP) not in calls
    # The clean finalizer removes the script atomically with the service row,
    # not through a separate name-keyed delete.
    assert not any(c[0] == 'remove_script' for c in calls)
    assert calls.index(
        ('status', serve_state.ServiceStatus.SHUTTING_DOWN)) < (calls.index(
            ('quiesce_launches', 'svc')))
    assert calls.index(('quiesce_launches', 'svc')) < calls.index(
        ('begin_teardown', 'svc'))
    assert calls.index(('begin_teardown', 'svc')) < calls.index(
        ('delete_lb', 'svc'))


def test_finalize_claims_and_settles_bound_launches_before_generic_quiescence(
        monkeypatch):
    calls = []
    authority = types.SimpleNamespace(
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        service_name='svc')
    monkeypatch.setattr(service, '_cleanup', lambda *a, **k: False)
    _patch_finalize(monkeypatch, calls)
    monkeypatch.setattr(
        service.ordinary_launch_binding, 'claim_controller_incarnation',
        lambda *a, **k: calls.append(('claim_binding', a[0])) or authority)
    monkeypatch.setattr(
        service, '_settle_bound_ordinary_launches_for_teardown',
        lambda claimed, infos: calls.append(
            ('settle_binding', claimed.service_name, len(infos)
            )) or service._BoundLaunchTeardownSettlement({}, {}))

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert calls.index(
        ('status', serve_state.ServiceStatus.SHUTTING_DOWN)) < (calls.index(
            ('claim_binding', 'svc')))
    assert calls.index(('claim_binding', 'svc')) < calls.index(
        ('settle_binding', 'svc', 0))
    assert calls.index(('settle_binding', 'svc', 0)) < calls.index(
        ('quiesce_launches', 'svc'))


def test_finalize_refuses_generic_cleanup_when_bound_settlement_fails(
        monkeypatch):
    calls = []
    authority = types.SimpleNamespace(
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        service_name='svc')
    monkeypatch.setattr(serve_state,
                        'set_service_status_and_active_versions_if_owner',
                        lambda *a, **k: calls.append(('status', a[4])) or True)
    monkeypatch.setattr(service.ordinary_launch_binding,
                        'claim_controller_incarnation',
                        lambda *a, **k: authority)
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda _svc: [_replica(1)])
    monkeypatch.setattr(service, '_settle_bound_ordinary_launches_for_teardown',
                        mock.Mock(side_effect=RuntimeError('still claimed')))
    monkeypatch.setattr(
        service.serve_utils, 'quiesce_service_replica_launch_requests',
        lambda *a, **k: calls.append(('generic_quiesce', a[0])) or True)
    monkeypatch.setattr(serve_state,
                        'acknowledge_service_controller_teardown_if_owner',
                        lambda *a, **k: calls.append(('ack', a[0])) or True)

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert calls == [('status', serve_state.ServiceStatus.SHUTTING_DOWN)]


def test_teardown_reducer_cancels_then_polls_exact_association(monkeypatch):
    info = _replica(1)
    context = types.SimpleNamespace(association_id='association-a')
    authority = types.SimpleNamespace(
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        service_name='svc')
    initial = types.SimpleNamespace(context=context, cancel_reason=None)
    waiting = types.SimpleNamespace(disposition='WAIT_QUIESCENCE',
                                    projected=False)
    projected = types.SimpleNamespace(disposition='PROJECTED', projected=True)
    inspect = mock.Mock(return_value=initial)
    cancel = mock.Mock()
    reduce = mock.Mock(side_effect=[waiting, projected])
    monkeypatch.setattr(service.request_postgres,
                        'lookup_bound_ordinary_launch_cancel_target', inspect)
    monkeypatch.setattr(service.request_postgres,
                        'request_bound_ordinary_launch_cancel', cancel)
    monkeypatch.setattr(service.request_postgres,
                        'reduce_bound_ordinary_launch', reduce)
    monkeypatch.setattr(service.time, 'sleep', lambda _seconds: None)

    service._settle_bound_ordinary_launches_for_teardown(authority, [info])

    inspect.assert_called_once_with('svc', 1, info.replica_record_id)
    assert cancel.call_args.args[:3] == (context, authority, 'service-teardown')
    assert reduce.call_count == 2
    assert all(
        call.args == (context, authority) for call in reduce.call_args_list)
    assert all(call.kwargs['project_replica_result'] is not None
               for call in reduce.call_args_list)


def test_teardown_success_projects_provider_materialization_evidence(
        monkeypatch):
    info = _replica(1)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.INTERRUPTED
    authority = types.SimpleNamespace(service_name='svc',
                                      service_hash='service-hash')
    projection = types.SimpleNamespace(
        locked_replica_info=info,
        status=types.SimpleNamespace(value='SUCCEEDED'),
        pre_effect_terminal=False,
        paid_capacity_pool_key=None,
        context=types.SimpleNamespace(association_id='association-a'))
    update = mock.Mock(return_value=True)
    monkeypatch.setattr(
        serve_state, 'update_replica_for_bound_ordinary_launch_in_transaction',
        update)

    assert service._project_bound_ordinary_launch_for_teardown(
        authority, mock.sentinel.connection, projection)

    assert (info.status_property.sky_launch_status ==
            common_utils.ProcessStatus.INTERRUPTED)
    assert update.call_args.kwargs['provider_launch_succeeded'] is True


def test_teardown_reducer_reuses_prior_durable_cancel_reason(monkeypatch):
    info = _replica(1)
    context = types.SimpleNamespace(association_id='association-a')
    authority = types.SimpleNamespace(
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        service_name='svc')
    target = types.SimpleNamespace(context=context,
                                   cancel_reason='replica-teardown')
    projected = types.SimpleNamespace(disposition='PRE_EFFECT_TERMINAL',
                                      projected=True)
    monkeypatch.setattr(service.request_postgres,
                        'lookup_bound_ordinary_launch_cancel_target',
                        mock.Mock(return_value=target))
    cancel = mock.Mock()
    monkeypatch.setattr(service.request_postgres,
                        'request_bound_ordinary_launch_cancel', cancel)
    monkeypatch.setattr(service.request_postgres,
                        'reduce_bound_ordinary_launch',
                        mock.Mock(return_value=projected))

    service._settle_bound_ordinary_launches_for_teardown(authority, [info])

    assert cancel.call_args.args[:3] == (context, authority, 'replica-teardown')


def test_service_teardown_cancels_every_target_before_reducing(monkeypatch):
    infos = [_replica(1), _replica(2)]
    contexts = [
        types.SimpleNamespace(association_id=f'association-{index}')
        for index in (1, 2)
    ]
    authority = types.SimpleNamespace(
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        service_name='svc')
    targets = [
        types.SimpleNamespace(context=context, cancel_reason=None)
        for context in contexts
    ]
    events = []
    monkeypatch.setattr(service.request_postgres,
                        'lookup_bound_ordinary_launch_cancel_target',
                        mock.Mock(side_effect=targets))
    monkeypatch.setattr(
        service.request_postgres, 'request_bound_ordinary_launch_cancel',
        lambda context, *_args: events.append(
            ('cancel', context.association_id)))

    def _reduce(context, *_args, **_kwargs):
        events.append(('reduce', context.association_id))
        return types.SimpleNamespace(disposition='PROJECTED', projected=True)

    monkeypatch.setattr(service.request_postgres,
                        'reduce_bound_ordinary_launch', _reduce)

    service._settle_bound_ordinary_launches_for_teardown(authority, infos)

    assert events == [('cancel', 'association-1'), ('cancel', 'association-2'),
                      ('reduce', 'association-1'), ('reduce', 'association-2')]


def test_teardown_keeps_pre_token_paid_ambiguity_fail_closed(monkeypatch):
    """A cohort without immutable create identity is never guessed absent."""
    info = _replica(1)
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
        authorization_reference='paid-capacity:test',
        authorization_generation=1,
        authorization_payload={'pool': 'test'})
    context = ordinary_launch_binding.BoundNonPoolLaunchContext(
        association_id=uuid.UUID('11111111-1111-4111-8111-111111111111'),
        request_id='request-1',
        service_name='svc',
        replica_id=1,
        replica_record_id=uuid.UUID(info.replica_record_id),
        launch_generation=1,
        input_digest='a' * 64,
        profile=profile,
        capability_cohort_epoch=(
            ordinary_launch_binding.ORDINARY_PAID_AWS_CLIENT_TOKEN_COHORT_FLOOR
            - 1),
        capability_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=1)
    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='no supported AWS create token'):
        ordinary_launch_binding.ordinary_paid_aws_client_token(context)
    authority = types.SimpleNamespace(
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        service_name='svc')
    target = types.SimpleNamespace(context=context, cancel_reason=None)
    inspection = types.SimpleNamespace(context=context, disposition='AMBIGUOUS')
    cancel = mock.Mock()
    reduce = mock.Mock()
    reconcile = mock.Mock(return_value=types.SimpleNamespace(
        evidence=ordinary_launch_binding.ProviderEvidence.UNKNOWN))
    monkeypatch.setattr(service.request_postgres,
                        'lookup_bound_ordinary_launch_cancel_target',
                        mock.Mock(return_value=target))
    monkeypatch.setattr(service.request_postgres,
                        'inspect_bound_ordinary_launch',
                        mock.Mock(return_value=inspection))
    monkeypatch.setattr(
        service.request_postgres,
        'bound_non_pool_provider_present_cleanup_is_authorized',
        mock.Mock(return_value=False))
    monkeypatch.setattr(service.request_postgres,
                        'request_bound_ordinary_launch_cancel', cancel)
    monkeypatch.setattr(service.request_postgres,
                        'reduce_bound_ordinary_launch', reduce)
    monkeypatch.setattr(service.non_pool_launch_reconciliation, 'reconcile',
                        reconcile)

    settlement = service._settle_bound_ordinary_launches_for_teardown(
        authority, [info])

    assert not settlement.provider_present_cleanup_contexts
    assert settlement.provider_reconciliation_failures == {
        (info.replica_id, info.replica_record_id):
            ('teardown provider reconciliation returned UNKNOWN for '
             'ORDINARY_PAID; exact provider cleanup remains unproven')
    }

    cancel.assert_not_called()
    reduce.assert_not_called()
    reconcile.assert_called_once()


def test_teardown_reconciles_paid_replacement_before_cancellation(monkeypatch):
    """A terminal replacement uses exact GCP evidence, not cancellation."""
    info = _replica(1)
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.
        UNKNOWN_CAPACITY_REPLACEMENT,
        authorization_reference='replacement:test',
        authorization_generation=1,
        authorization_payload={'pool': 'gcp-spot'})
    context = ordinary_launch_binding.BoundNonPoolLaunchContext(
        association_id=uuid.UUID('11111111-1111-4111-8111-111111111111'),
        request_id='request-1',
        service_name='svc',
        replica_id=1,
        replica_record_id=uuid.UUID(info.replica_record_id),
        launch_generation=1,
        input_digest='a' * 64,
        profile=profile,
        capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        capability_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=1)
    authority = types.SimpleNamespace(
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        service_name='svc')
    target = types.SimpleNamespace(context=context, cancel_reason=None)
    inspection = types.SimpleNamespace(context=context, disposition='AMBIGUOUS')
    cancel = mock.Mock()
    reduce = mock.Mock()
    reconcile = mock.Mock(return_value=types.SimpleNamespace(
        evidence=ordinary_launch_binding.ProviderEvidence.ABSENT))
    monkeypatch.setattr(service.request_postgres,
                        'lookup_bound_ordinary_launch_cancel_target',
                        mock.Mock(return_value=target))
    monkeypatch.setattr(service.request_postgres,
                        'inspect_bound_ordinary_launch',
                        mock.Mock(return_value=inspection))
    monkeypatch.setattr(
        service.request_postgres,
        'bound_non_pool_provider_present_cleanup_is_authorized',
        mock.Mock(return_value=False))
    monkeypatch.setattr(service.request_postgres,
                        'request_bound_ordinary_launch_cancel', cancel)
    monkeypatch.setattr(service.request_postgres,
                        'reduce_bound_ordinary_launch', reduce)
    monkeypatch.setattr(service.non_pool_launch_reconciliation, 'reconcile',
                        reconcile)

    settlement = service._settle_bound_ordinary_launches_for_teardown(
        authority, [info])
    assert not settlement.provider_present_cleanup_contexts
    assert not settlement.provider_reconciliation_failures

    cancel.assert_not_called()
    reduce.assert_not_called()
    reconcile.assert_called_once()


def test_teardown_reconciliation_exception_isolated_from_peer(monkeypatch):
    infos = [_replica(1), _replica(2)]
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
        authorization_reference='paid-capacity:test',
        authorization_generation=1,
        authorization_payload={'pool': 'test'})
    contexts = [
        ordinary_launch_binding.BoundNonPoolLaunchContext(
            association_id=uuid.UUID(
                f'11111111-1111-4111-8111-{info.replica_id:012d}'),
            request_id=f'request-{info.replica_id}',
            service_name='svc',
            replica_id=info.replica_id,
            replica_record_id=uuid.UUID(info.replica_record_id),
            launch_generation=1,
            input_digest='a' * 64,
            profile=profile,
            capability_cohort_epoch=(
                ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
            capability_profile_set_digest=(
                ordinary_launch_binding.supported_non_pool_profile_set_digest()
            ),
            receipt_protocol_version=1) for info in infos
    ]
    authority = types.SimpleNamespace(
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        service_name='svc')
    targets = [
        types.SimpleNamespace(context=context, cancel_reason=None)
        for context in contexts
    ]
    inspections = [
        types.SimpleNamespace(context=context, disposition='AMBIGUOUS')
        for context in contexts
    ]
    reconcile = mock.Mock(side_effect=[
        RuntimeError('provider credentials unavailable'),
        types.SimpleNamespace(
            evidence=ordinary_launch_binding.ProviderEvidence.ABSENT),
    ])
    monkeypatch.setattr(service.request_postgres,
                        'lookup_bound_ordinary_launch_cancel_target',
                        mock.Mock(side_effect=targets))
    monkeypatch.setattr(service.request_postgres,
                        'inspect_bound_ordinary_launch',
                        mock.Mock(side_effect=inspections))
    monkeypatch.setattr(
        service.request_postgres,
        'bound_non_pool_provider_present_cleanup_is_authorized',
        mock.Mock(return_value=False))
    monkeypatch.setattr(service.non_pool_launch_reconciliation, 'reconcile',
                        reconcile)

    settlement = service._settle_bound_ordinary_launches_for_teardown(
        authority, infos)

    first_key = (infos[0].replica_id, infos[0].replica_record_id)
    assert settlement.provider_reconciliation_failures == {
        first_key: ('teardown provider reconciliation raised for replica 1: '
                    'RuntimeError: provider credentials unavailable')
    }
    assert not settlement.provider_present_cleanup_contexts
    assert reconcile.call_count == 2


def test_teardown_recovery_evidence_conflict_does_not_orphan(
        monkeypatch, caplog):
    authority = mock.sentinel.authority
    conflict = ordinary_launch_binding.OrdinaryLaunchBindingConflict(
        'provider absence is unproven')
    orphan_exit = mock.Mock()
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda _service_name: [_replica(1)])
    monkeypatch.setattr(service, '_settle_bound_ordinary_launches_for_teardown',
                        mock.Mock(side_effect=conflict))
    monkeypatch.setattr(
        service.serve_utils, 'quiesce_service_replica_launch_requests',
        mock.Mock(side_effect=AssertionError('unresolved rows cannot quiesce')))
    monkeypatch.setattr(service, '_orphan_exit', orphan_exit)

    assert not service._settle_teardown_recovery_launches(
        'svc', 'incarnation-a', (123, '10.0.0.2'), authority)

    orphan_exit.assert_not_called()
    assert 'not a controller ownership loss' in caplog.text
    assert 'provider absence is unproven' in caplog.text


def test_teardown_recovery_two_claimants_orphan_only_cas_loser(monkeypatch):
    winner = mock.sentinel.authority
    loser_conflict = ordinary_launch_binding.OrdinaryLaunchBindingConflict(
        'controller parent-owner fence changed')
    claim = mock.Mock(side_effect=[winner, loser_conflict])
    orphan_exit = mock.Mock()
    monkeypatch.setattr(service.ordinary_launch_binding,
                        'claim_controller_incarnation', claim)
    monkeypatch.setattr(
        serve_state, 'update_service_controller_pid_if_owner',
        mock.Mock(side_effect=AssertionError(
            'PostgreSQL binding claims never use legacy CAS')))
    monkeypatch.setattr(service, '_orphan_exit', orphan_exit)
    kwargs = {
        'expected_lifecycle_epoch': 4,
        'expected_status': serve_state.ServiceStatus.SHUTTING_DOWN,
        'binding_expected_recovery_version': 2,
        'legacy_expected_recovery_version': 2,
    }

    assert service._claim_teardown_recovery_controller('svc', 'incarnation-a',
                                                       (123, '10.0.0.2'),
                                                       (456, '10.0.0.3'),
                                                       **kwargs) is winner
    orphan_exit.assert_not_called()
    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                       match='parent-owner fence changed'):
        service._claim_teardown_recovery_controller('svc', 'incarnation-a',
                                                    (123, '10.0.0.2'),
                                                    (789, '10.0.0.4'), **kwargs)

    orphan_exit.assert_called_once_with(None)


def test_finalize_does_not_ack_or_delete_until_launches_quiesce(monkeypatch):
    calls = []
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda _svc: [_replica(1)])
    monkeypatch.setattr(
        service.serve_utils, 'quiesce_service_replica_launch_requests',
        lambda *a, **k: calls.append(('quiesce_failed', a[0])) or False)
    monkeypatch.setattr(
        serve_state, 'acknowledge_service_controller_teardown_if_owner',
        lambda *a, **k: calls.append(('begin_teardown', a[0])) or True)
    monkeypatch.setattr(serve_state,
                        'set_service_status_and_active_versions_if_owner',
                        lambda *a, **k: calls.append(('status', a[4])) or True)
    monkeypatch.setattr(service, '_cleanup', lambda *a, **k: calls.append(
        ('cleanup', a[0])))

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert calls == [('status', serve_state.ServiceStatus.SHUTTING_DOWN),
                     ('quiesce_failed', 'svc')]


def test_finalize_marks_failed_cleanup_when_teardown_fails(monkeypatch):
    calls = []
    monkeypatch.setattr(service, '_cleanup', lambda *a, **k: True)
    _patch_finalize(monkeypatch, calls)

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert ('status', serve_state.ServiceStatus.FAILED_CLEANUP) in calls
    assert not any(c[0] == 'removed' for c in calls)
    # FAILED_CLEANUP is published first, then the recovery script is removed
    # so a persistent cleanup failure cannot loop forever.
    assert ('remove_script', 'svc') in calls


def test_cleanup_restart_observes_pending_paid_teardown_without_resubmission(
        monkeypatch):
    """A crash after native submission resumes from the persisted phase."""
    binding = service.ordinary_launch_binding
    reconciliation = service.non_pool_launch_reconciliation
    info = _replica(1)
    info.is_spot = True
    info.is_zero_cost = False
    info.reserved_fill = False
    info.paid_capacity_pool_key = (
        '{"accelerators":[["l4",1]],"cloud":"aws",'
        '"instance_type":"g6.2xlarge","num_nodes":1,'
        '"region":"eu-south-2","use_spot":true,"version":1,'
        '"workspace":"w","zone":"eu-south-2a"}')
    reconciliation.apply_immediate_provider_cleanup_replica_marker(info)
    profile = binding.NonPoolLaunchProfile.create(
        binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
        authorization_reference='paid-capacity:test',
        authorization_generation=7,
        authorization_payload={'pool_key': info.paid_capacity_pool_key})
    context = binding.BoundNonPoolLaunchContext(
        association_id=uuid.UUID('11111111-1111-4111-8111-111111111111'),
        request_id='request-1',
        service_name='svc',
        replica_id=info.replica_id,
        replica_record_id=uuid.UUID(info.replica_record_id),
        launch_generation=1,
        input_digest='a' * 64,
        profile=profile,
        capability_cohort_epoch=binding.NON_POOL_CAPABILITY_COHORT_EPOCH,
        capability_profile_set_digest=(
            binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=binding.NON_POOL_RECEIPT_PROTOCOL_VERSION)
    authority = types.SimpleNamespace(capable=True,
                                      binding_mode=binding.BindingMode.BOUND,
                                      service_name='svc',
                                      service_hash='incarnation-a')
    binding.transition_provider_present_teardown_phase(
        info,
        expected=binding.ProviderPresentTeardownPhase.SUBMISSION_SCHEDULED,
        target=binding.ProviderPresentTeardownPhase.ABSENCE_OBSERVATION_PENDING)
    replicas = [info]
    calls = []
    submit = mock.Mock(side_effect=AssertionError(
        'an observation-pending restart must not resubmit provider teardown'))

    def _observe_once(actual_context, _info, actual_authority, _projector,
                      **_kwargs):
        assert actual_context is context
        assert actual_authority is authority
        calls.append(('provider_observation', info.replica_id))
        return reconciliation.PaidTeardownObservationStep(
            reconciliation.PaidTeardownObservationDisposition.SETTLED_ABSENT,
            reconciliation.ProviderObservation(binding.ProviderEvidence.ABSENT,
                                               {'instances': []}))

    _patch_finalize(monkeypatch, calls)
    _patch_common(monkeypatch, calls, replicas)
    monkeypatch.setattr(serve_state, 'get_service_controller_owner',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, 'cleanup_storage_intents',
                        lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        service.request_postgres,
        'bound_non_pool_provider_present_cleanup_is_authorized',
        lambda *_args, **_kwargs: True)
    monkeypatch.setattr(reconciliation, 'submit_paid_provider_teardown', submit)
    monkeypatch.setattr(reconciliation, 'advance_paid_teardown_observation',
                        _observe_once)

    def _finalize_paid(_service_name, replica_id, _record_id, _cluster_name,
                       **_kwargs):
        assert replica_id == info.replica_id
        replicas.clear()
        calls.append(('row_removed', replica_id))
        return True

    monkeypatch.setattr(replica_managers,
                        'finalize_projected_paid_provider_absence',
                        _finalize_paid)

    lifecycle_lock = types.SimpleNamespace(epoch=31)
    finalize_args = ('svc', types.SimpleNamespace(pool=True), '/tmp/svc', 1,
                     'incarnation-a', 123, None, lifecycle_lock)
    cleanup_contexts = {(info.replica_id, info.replica_record_id): context}

    service._run_cleanup_and_finalize_locked(
        *finalize_args,
        binding_authority=authority,
        provider_present_cleanup_contexts=cleanup_contexts)

    assert not replicas
    submit.assert_not_called()
    assert ('provider_observation', info.replica_id) in calls
    assert ('row_removed', info.replica_id) in calls
    assert calls.count(('removed', 'svc')) == 1
    assert ('remove_script', 'svc') not in calls


def test_paid_teardown_submission_exception_retains_recovery_authority(
        monkeypatch):
    """A provider-submit crash leaves the exact row and HA retry path."""
    info, context = _paid_cleanup_case(1)
    authority = types.SimpleNamespace(
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        service_name='svc',
        service_hash='incarnation-a')
    replicas = [info]
    calls = []
    _patch_finalize(monkeypatch, calls)
    _patch_common(monkeypatch, calls, replicas)
    monkeypatch.setattr(serve_state, 'get_service_controller_owner',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, 'cleanup_storage_intents',
                        lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        service.request_postgres,
        'bound_non_pool_provider_present_cleanup_is_authorized',
        lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        non_pool_launch_reconciliation, 'submit_paid_provider_teardown',
        mock.Mock(side_effect=RuntimeError('provider submission crashed')))

    service._run_cleanup_and_finalize_locked(
        'svc',
        types.SimpleNamespace(pool=True),
        '/tmp/svc',
        1,
        'incarnation-a',
        123,
        None,
        types.SimpleNamespace(epoch=31),
        binding_authority=authority,
        provider_present_cleanup_contexts={
            (info.replica_id, info.replica_record_id): context
        })

    assert replicas == [info]
    assert info.status_property.sky_down_status is common_utils.ProcessStatus.FAILED
    assert ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
        info)
    assert ('status', serve_state.ServiceStatus.FAILED_CLEANUP) in calls
    assert not any(call[0] == 'removed' for call in calls)
    assert ('remove_script', 'svc') not in calls


def test_paid_teardown_observer_uses_one_deadline_and_releases_coordinator(
        monkeypatch):
    """Every retry receives the replica's original cleanup deadline."""
    info, context = _paid_cleanup_case(1)
    authority = types.SimpleNamespace(
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        service_name='svc',
        service_hash='incarnation-a')
    ordinary_launch_binding.transition_provider_present_teardown_phase(
        info,
        expected=(ordinary_launch_binding.ProviderPresentTeardownPhase.
                  SUBMISSION_SCHEDULED),
        target=(ordinary_launch_binding.ProviderPresentTeardownPhase.
                ABSENCE_OBSERVATION_PENDING))
    replicas = [info]
    events = []
    observer_started = threading.Event()
    observer_deadlines = []
    _patch_common(monkeypatch, events, replicas)
    monkeypatch.setattr(service, 'cleanup_storage_intents',
                        lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        service.request_postgres,
        'bound_non_pool_provider_present_cleanup_is_authorized',
        lambda *_args, **_kwargs: True)

    clock = iter(range(0, 10_000, 100))
    monkeypatch.setattr(service.time, 'monotonic', lambda: next(clock))

    def _unknown_observer(*_args, **kwargs):
        observer_started.set()
        observer_deadlines.append(
            kwargs['provider_operation_deadline_monotonic'])
        return non_pool_launch_reconciliation.PaidTeardownObservationStep(
            non_pool_launch_reconciliation.PaidTeardownObservationDisposition.
            RETRY_UNKNOWN,
            non_pool_launch_reconciliation.ProviderObservation(
                ordinary_launch_binding.ProviderEvidence.UNKNOWN, {}))

    monkeypatch.setattr(non_pool_launch_reconciliation,
                        'advance_paid_teardown_observation', _unknown_observer)
    remove_replica = mock.Mock()
    monkeypatch.setattr(serve_state, 'remove_replica', remove_replica)

    failed = service._cleanup(
        'svc',
        True,
        'incarnation-a',
        123,
        None,
        types.SimpleNamespace(epoch=31),
        binding_authority=authority,
        provider_present_cleanup_contexts={
            (info.replica_id, info.replica_record_id): context
        })

    assert observer_started.is_set()
    assert observer_deadlines
    assert len(set(observer_deadlines)) == 1
    assert failed
    assert replicas == [info]
    assert info.status_property.sky_down_status is common_utils.ProcessStatus.FAILED
    assert ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
        info)
    remove_replica.assert_not_called()


def test_generic_failed_replica_is_not_paid_teardown_pending() -> None:
    """FAILED is a phase only when the exact paid cleanup marker agrees."""
    info = _replica(1)
    info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED

    assert not ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
        info)
    assert not service._replica_needs_exact_provider_cleanup_retry(info)


def test_eight_aws_teardowns_submit_in_d4_waves_before_absence_observation(
        monkeypatch):
    """The D=4 lane owns native submission, never provider polling."""
    binding = service.ordinary_launch_binding
    reconciliation = service.non_pool_launch_reconciliation
    authority = types.SimpleNamespace(capable=True,
                                      binding_mode=binding.BindingMode.BOUND,
                                      service_name='svc',
                                      service_hash='incarnation-a')
    replicas = []
    contexts = {}
    for replica_id in range(1, 9):
        info = _replica(replica_id)
        info.is_spot = True
        info.is_zero_cost = False
        info.reserved_fill = False
        info.paid_capacity_pool_key = json.dumps(
            {
                'accelerators': [['l4', 1]],
                'cloud': 'aws',
                'instance_type': 'g6.2xlarge',
                'num_nodes': 1,
                'region': 'eu-south-2',
                'use_spot': True,
                'version': 1,
                'workspace': 'w',
                'zone': 'eu-south-2a',
            },
            sort_keys=True,
            separators=(',', ':'))
        reconciliation.apply_immediate_provider_cleanup_replica_marker(info)
        profile = binding.NonPoolLaunchProfile.create(
            binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
            authorization_reference=f'paid-capacity:{replica_id}',
            authorization_generation=7,
            authorization_payload={'pool_key': info.paid_capacity_pool_key})
        context = binding.BoundNonPoolLaunchContext(
            association_id=uuid.UUID(int=100 + replica_id),
            request_id=f'request-{replica_id}',
            service_name='svc',
            replica_id=replica_id,
            replica_record_id=uuid.UUID(info.replica_record_id),
            launch_generation=1,
            input_digest=f'{replica_id:x}'.zfill(64),
            profile=profile,
            capability_cohort_epoch=binding.NON_POOL_CAPABILITY_COHORT_EPOCH,
            capability_profile_set_digest=(
                binding.supported_non_pool_profile_set_digest()),
            receipt_protocol_version=binding.NON_POOL_RECEIPT_PROTOCOL_VERSION)
        replicas.append(info)
        contexts[(replica_id, info.replica_record_id)] = context

    events = []
    reserve_batches = []
    _patch_common(monkeypatch, events, replicas)
    monkeypatch.setattr(service, 'cleanup_storage_intents',
                        lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        service.request_postgres,
        'bound_non_pool_provider_present_cleanup_is_authorized',
        lambda *_args, **_kwargs: True)

    def _reserve(_service_name, candidates, *, termination_limit, **_kwargs):
        assert termination_limit >= 4
        assert len(candidates) <= 4
        reserve_batches.append(tuple(
            replica_id for replica_id, _ in candidates))
        by_id = {info.replica_id: info for info in replicas}
        result = {}
        for replica_id, _ in candidates:
            info = by_id[replica_id]
            info.status_property.sky_down_status = common_utils.ProcessStatus.RUNNING
            result[replica_id] = info
        return result

    monkeypatch.setattr(serve_state,
                        'reserve_replica_teardowns_running_if_capacity',
                        _reserve)
    monkeypatch.setattr(
        serve_state, 'get_replica_info_from_id',
        lambda _service_name, replica_id: next(
            (info for info in replicas if info.replica_id == replica_id), None))

    def _submit(_context, info, _authority, **_kwargs):
        events.append(('submit', info.replica_id))
        binding.transition_provider_present_teardown_phase(
            info,
            expected=binding.ProviderPresentTeardownPhase.SUBMISSION_RUNNING,
            target=binding.ProviderPresentTeardownPhase.
            ABSENCE_OBSERVATION_PENDING)
        return types.SimpleNamespace(
            disposition=resource_actions.ProviderSubmissionDisposition.ACCEPTED)

    def _observe(_context, info, _authority, _projector, **_kwargs):
        events.append(('observe', info.replica_id))
        return reconciliation.PaidTeardownObservationStep(
            reconciliation.PaidTeardownObservationDisposition.SETTLED_ABSENT,
            reconciliation.ProviderObservation(binding.ProviderEvidence.ABSENT,
                                               {'instances': []}))

    monkeypatch.setattr(reconciliation, 'submit_paid_provider_teardown',
                        _submit)
    monkeypatch.setattr(reconciliation, 'advance_paid_teardown_observation',
                        _observe)

    def _remove(_service_name, replica_id, **_kwargs):
        info = next(info for info in replicas if info.replica_id == replica_id)
        replicas.remove(info)
        return True

    monkeypatch.setattr(serve_state, 'remove_replica', _remove)

    failed = service._cleanup('svc',
                              True,
                              'incarnation-a',
                              123,
                              None,
                              types.SimpleNamespace(epoch=31),
                              binding_authority=authority,
                              provider_present_cleanup_contexts=contexts)

    assert not failed
    assert not replicas
    assert max(map(len, reserve_batches)) <= 4
    assert [replica_id for batch in reserve_batches for replica_id in batch
           ] == list(range(1, 9))
    submit_indexes = [
        index for index, event in enumerate(events) if event[0] == 'submit'
    ]
    observe_indexes = [
        index for index, event in enumerate(events) if event[0] == 'observe'
    ]
    assert len(submit_indexes) == len(observe_indexes) == 8
    assert max(submit_indexes) < min(observe_indexes)


def test_provider_uncertainty_retains_recovery_script_for_exact_retry(
        monkeypatch):
    calls = []
    info = _replica(1)
    authority = types.SimpleNamespace(
        capable=True,
        binding_mode=ordinary_launch_binding.BindingMode.BOUND,
        service_name='svc')
    failure_key = (info.replica_id, info.replica_record_id)
    settlement = service._BoundLaunchTeardownSettlement(
        {}, {failure_key: 'AWS census remains unproven'})
    _patch_finalize(monkeypatch, calls)
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda _service_name: [info])
    monkeypatch.setattr(
        service.ordinary_launch_binding, 'begin_service_teardown_if_owner',
        lambda *_args, **_kwargs: types.SimpleNamespace(disposition=(
            ordinary_launch_binding.ServiceTeardownDisposition.MARKED_BOUND),
                                                        authority=authority))
    monkeypatch.setattr(service.ordinary_launch_binding,
                        'claim_controller_incarnation',
                        lambda *_args, **_kwargs: authority)
    monkeypatch.setattr(service, '_settle_bound_ordinary_launches_for_teardown',
                        lambda *_args, **_kwargs: settlement)

    def _cleanup(*_args, **kwargs):
        assert kwargs['provider_reconciliation_failures'] == {
            failure_key: 'AWS census remains unproven'
        }
        return True

    monkeypatch.setattr(service, '_cleanup', _cleanup)

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert ('status', serve_state.ServiceStatus.FAILED_CLEANUP) in calls
    assert ('remove_script', 'svc') not in calls


def test_late_provider_cleanup_failure_retains_recovery_script(monkeypatch):
    """A late provider timeout leaves durable work for the next owner."""
    calls = []
    info = _failed_paid_provider_cleanup_replica(1)
    cleanup_finished = False
    cleanup_results = iter((True, False))

    def _replica_infos(_service_name):
        # The initial teardown inventory is empty in this focused finalizer
        # test. The exact retry marker becomes visible only after _cleanup's
        # provider worker has failed, reproducing a late failure that is absent
        # from the earlier provider_reconciliation_failures input.
        return [info] if cleanup_finished else []

    def _cleanup(*_args, **kwargs):
        nonlocal cleanup_finished
        assert not kwargs['provider_reconciliation_failures']
        failed = next(cleanup_results)
        # Make the durable retry row disappear before allowing the next
        # finalizer pass to complete. Provider census behavior is tested at
        # the provider-reconciliation boundary, not mocked by this test.
        cleanup_finished = failed
        return failed

    _patch_finalize(monkeypatch, calls)
    monkeypatch.setattr(serve_state, 'get_replica_infos', _replica_infos)
    monkeypatch.setattr(service, '_cleanup', _cleanup)

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert ('status', serve_state.ServiceStatus.FAILED_CLEANUP) in calls
    assert ('remove_script', 'svc') not in calls

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert calls.count(('removed', 'svc')) == 1
    assert ('remove_script', 'svc') not in calls


def test_replica_read_failure_after_cleanup_retains_recovery_script(
        monkeypatch):
    """A failed authoritative row read is not proof exact cleanup finished."""
    calls = []
    cleanup_finished = False

    def _replica_infos(_service_name):
        if cleanup_finished:
            raise RuntimeError('replica rows unavailable')
        return []

    def _cleanup(*_args, **kwargs):
        nonlocal cleanup_finished
        assert not kwargs['provider_reconciliation_failures']
        cleanup_finished = True
        return True

    _patch_finalize(monkeypatch, calls)
    monkeypatch.setattr(serve_state, 'get_replica_infos', _replica_infos)
    monkeypatch.setattr(service, '_cleanup', _cleanup)

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert ('status', serve_state.ServiceStatus.FAILED_CLEANUP) in calls
    assert ('remove_script', 'svc') not in calls


def test_finalize_contains_cleanup_exception_and_breaks_recovery_loop(
        monkeypatch):
    """A _cleanup that RAISES must be contained, leave the service
    FAILED_CLEANUP, AND remove the HA recovery script -- otherwise a persistent
    cleanup error loops forever (FAILED_CLEANUP is a resume status and the
    script was never reached for removal inside _cleanup)."""
    calls = []

    def _boom(*args, **kwargs):
        raise RuntimeError('cleanup blew up')

    monkeypatch.setattr(service, '_cleanup', _boom)
    _patch_finalize(monkeypatch, calls)

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert ('status', serve_state.ServiceStatus.FAILED_CLEANUP) in calls
    assert ('remove_script', 'svc') in calls, (
        'a caught cleanup exception must remove the HA script to avoid a '
        'recovery loop')


def test_handle_signal_persists_shutting_down_before_consuming_signal(
        monkeypatch, tmp_path):
    """The terminate signal must not be consumed before SHUTTING_DOWN is durably
    set: otherwise a crash in that window loses the teardown intent and HA
    recovery would bring the (user-downed) service back up serving."""
    sig = tmp_path / 'svc.signal'
    sig.write_text('terminate')
    monkeypatch.setattr(service.constants, 'SIGNAL_FILE_PATH',
                        str(tmp_path / '{}.signal'))
    observed = []

    def _record_status(unused_name, unused_hash, unused_pid, unused_ip, status):
        # The signal file must still exist when we persist SHUTTING_DOWN.
        observed.append((status, sig.exists()))

    monkeypatch.setattr(
        serve_state, 'set_service_status_and_active_versions_if_owner',
        lambda *args, **kwargs: _record_status(*args[:5]) or True)
    owner_match = mock.Mock(
        side_effect=AssertionError('redundant owner read must not run'))
    monkeypatch.setattr(serve_state, 'service_owner_matches', owner_match)

    with pytest.raises(exceptions.ServeUserTerminatedError):
        service._handle_signal('svc', 'incarnation-a', 123, None)

    assert observed, 'SHUTTING_DOWN must be persisted on a terminate signal'
    status, signal_existed_at_status_time = observed[0]
    assert status == serve_state.ServiceStatus.SHUTTING_DOWN
    assert signal_existed_at_status_time is True, (
        'status must be set BEFORE the signal file is consumed')
    assert not sig.exists(), 'signal file is consumed after status is persisted'
    owner_match.assert_not_called()


def test_handle_signal_retries_status_cas_db_error_without_cleanup(
        monkeypatch, tmp_path):
    sig = tmp_path / 'svc.signal'
    sig.write_text('terminate')
    monkeypatch.setattr(service.constants, 'SIGNAL_FILE_PATH',
                        str(tmp_path / '{}.signal'))
    monkeypatch.setattr(
        serve_state, 'service_owner_matches', lambda *args, **kwargs:
        (_ for _ in
         ()).throw(AssertionError('redundant owner read must not run')))
    persist = mock.Mock(side_effect=[RuntimeError('db unavailable'), True])
    monkeypatch.setattr(serve_state,
                        'set_service_status_and_active_versions_if_owner',
                        persist)

    # The DB error is contained and the wakeup remains durable; _start keeps
    # supervising instead of falling into unexpected-exception cleanup.
    assert service._handle_signal('svc', 'incarnation-a', 123, None)
    assert sig.exists()

    with pytest.raises(exceptions.ServeUserTerminatedError):
        service._handle_signal('svc', 'incarnation-a', 123, None)
    assert not sig.exists()
    assert persist.call_count == 2


def test_scoped_successor_discards_legacy_name_only_terminate(
        monkeypatch, tmp_path):
    sig = tmp_path / 'svc.signal'
    sig.write_text('terminate')
    monkeypatch.setattr(service.constants, 'SIGNAL_FILE_PATH',
                        str(tmp_path / '{}.signal'))
    set_status = mock.Mock()
    monkeypatch.setattr(serve_state,
                        'set_service_status_and_active_versions_if_owner',
                        set_status)

    assert service._handle_signal('svc',
                                  'incarnation-b',
                                  123,
                                  None,
                                  resource_scope='incarnation-b')
    assert not sig.exists()
    set_status.assert_not_called()


@pytest.mark.parametrize('malformed', ['not-a-signal', '{'])
def test_handle_signal_ignores_malformed_legacy_payload(monkeypatch, tmp_path,
                                                        malformed):
    sig = tmp_path / 'svc.signal'
    sig.write_text(malformed)
    monkeypatch.setattr(service.constants, 'SIGNAL_FILE_PATH',
                        str(tmp_path / '{}.signal'))
    set_status = mock.Mock()
    monkeypatch.setattr(serve_state,
                        'set_service_status_and_active_versions_if_owner',
                        set_status)

    assert service._handle_signal('svc', 'incarnation-a', 123, None)
    assert not sig.exists()
    set_status.assert_not_called()
