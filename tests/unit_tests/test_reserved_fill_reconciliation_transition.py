"""Tests for the one-way sequenced reserved-fill transition."""
# pylint: disable=protected-access

import contextlib
import dataclasses
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from sky.serve import reserved_fill_reclaim_attestation
from sky.serve import reserved_fill_reconciliation_transition as transition


@pytest.fixture(autouse=True)
def _valid_reclaim_gate_guard(monkeypatch):
    monkeypatch.setattr(transition.serve_state,
                        'reserved_fill_reclaim_gate_authority_guard_is_valid',
                        lambda _: True)
    monkeypatch.setattr(
        transition.serve_state,
        'run_reserved_fill_reclaim_activation_transaction',
        lambda _lock, operation: operation(mock.sentinel.activation_connection))


_AUTHORIZED_AT = 1234.5


def _status(
    *,
    gate_state: str = 'LEGACY_ACTIVE',
    gate_generation: int = 4,
    protocol_version: int = 2,
    serve_revision: str = transition.migration_utils.SERVE_VERSION,
    api_revision: str = transition.migration_utils.API_REQUESTS_VERSION,
    reclaim_policy_identity: dict[str, str] | None = None,
    reclaim_activation_receipt: dict[str, Any] | None = None,
    reclaim_authorized_at: float | None = None,
) -> transition.ReconciliationTransitionStatus:
    if gate_state == 'SEQUENCED_ACTIVE':
        if reclaim_activation_receipt is None:
            reclaim_activation_receipt = dataclasses.asdict(_receipt())
        if reclaim_policy_identity is None:
            reclaim_policy_identity = reclaim_activation_receipt['identity']
        if reclaim_authorized_at is None:
            reclaim_authorized_at = _AUTHORIZED_AT
    return transition.ReconciliationTransitionStatus(
        protocol_version=protocol_version,
        gate_state=gate_state,
        gate_generation=gate_generation,
        serve_schema_revision=serve_revision,
        api_request_schema_revision=api_revision,
        reclaim_policy_identity=reclaim_policy_identity,
        reclaim_activation_receipt=reclaim_activation_receipt,
        reclaim_authorized_at=reclaim_authorized_at)


def _attestation(
    completed_monotonic: float = 100.0,
    *,
    deployment_generation: str = 'generation',
) -> transition._WriterCohortAttestation:
    return transition._WriterCohortAttestation(
        image_digest='sha256:' + 'a' * 64,
        deployment_generation=deployment_generation,
        deployment_uid='uid',
        pod_inventory_count=3,
        pod_inventory_sha256='b' * 64,
        completed_monotonic=completed_monotonic)


def _reclaim_evidence(
    completed_monotonic: float = 100.0
) -> reserved_fill_reclaim_attestation.ReclaimEnforcementEvidence:
    return reserved_fill_reclaim_attestation.ReclaimEnforcementEvidence(
        contract=(reserved_fill_reclaim_attestation.ReclaimEnforcementContract.
                  GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2),
        fleet_bundle_sha256='c' * 64,
        policy_revision='policy-v1',
        provider_inventory_sha256='d' * 64,
        claimed_contexts=(),
        completed_monotonic=completed_monotonic)


def _receipt(
    attestation: transition._WriterCohortAttestation | None = None,
) -> reserved_fill_reclaim_attestation.ReclaimActivationReceipt:
    if attestation is None:
        attestation = _attestation()
    return reserved_fill_reclaim_attestation.activation_receipt(
        _reclaim_evidence(),
        writer_image_digest=attestation.image_digest,
        writer_deployment_generation=attestation.deployment_generation,
        writer_deployment_uid=attestation.deployment_uid,
        writer_pod_inventory_count=attestation.pod_inventory_count,
        writer_pod_inventory_sha256=attestation.pod_inventory_sha256)


def _receipt_from_dict(
    value: dict[str, Any],
) -> reserved_fill_reclaim_attestation.ReclaimActivationReceipt:
    identity = value['identity']
    return reserved_fill_reclaim_attestation.ReclaimActivationReceipt(
        identity=reserved_fill_reclaim_attestation.ReclaimPolicyIdentity(
            fleet_bundle_sha256=identity['fleet_bundle_sha256'],
            policy_revision=identity['policy_revision'],
            provider_inventory_sha256=identity['provider_inventory_sha256']),
        claim_scope_count=value['claim_scope_count'],
        claim_scope_sha256=value['claim_scope_sha256'],
        evidence_sha256=value['evidence_sha256'],
        writer_image_digest=value['writer_image_digest'],
        writer_deployment_generation=value['writer_deployment_generation'],
        writer_deployment_uid=value['writer_deployment_uid'],
        writer_pod_inventory_count=value['writer_pod_inventory_count'],
        writer_pod_inventory_sha256=value['writer_pod_inventory_sha256'])


def _gate(
    status: transition.ReconciliationTransitionStatus,
) -> transition.pool_capacity_observation.ReconciliationGate:
    identity = status.reclaim_policy_identity
    receipt = status.reclaim_activation_receipt
    return transition.pool_capacity_observation.ReconciliationGate(
        state=(transition.pool_capacity_observation.ReconciliationGateState(
            status.gate_state)),
        generation=status.gate_generation,
        reclaim_policy_identity=(
            None if identity is None else
            reserved_fill_reclaim_attestation.ReclaimPolicyIdentity(
                fleet_bundle_sha256=identity['fleet_bundle_sha256'],
                policy_revision=identity['policy_revision'],
                provider_inventory_sha256=identity['provider_inventory_sha256'])
        ),
        reclaim_activation_receipt=(None if receipt is None else
                                    _receipt_from_dict(receipt)),
        reclaim_authorized_at=status.reclaim_authorized_at)


def _repository(
    before: transition.ReconciliationTransitionStatus,
    *,
    receipt: reserved_fill_reclaim_attestation.ReclaimActivationReceipt |
    None = None,
    changed: bool = True,
) -> mock.Mock:
    repository = mock.Mock()
    before_gate = _gate(before)
    if receipt is None:
        receipt = _receipt()
    if changed:
        successor = transition.pool_capacity_observation.ReconciliationGate(
            state=(transition.pool_capacity_observation.ReconciliationGateState.
                   SEQUENCED_ACTIVE),
            generation=before.gate_generation + 1,
            reclaim_policy_identity=receipt.identity,
            reclaim_activation_receipt=receipt,
            reclaim_authorized_at=_AUTHORIZED_AT)
    else:
        successor = before_gate
    repository.lock_reconciliation_gate_for_activation.return_value = (
        before_gate)
    repository.authorize_sequenced_reconciliation.return_value = (
        transition.pool_capacity_observation.ReconciliationAuthorizationResult(
            changed=changed, gate=successor))
    return repository


def test_activation_schema_requires_current_successor_heads_and_protocol_v2(
) -> None:
    transition._require_activation_schema(_status())
    with pytest.raises(transition.ReconciliationTransitionError,
                       match='protocol v2'):
        transition._require_activation_schema(_status(protocol_version=1))
    with pytest.raises(transition.ReconciliationTransitionError,
                       match='Serve schema revision 047 or a successor'):
        transition._require_activation_schema(_status(serve_revision='042'))
    with pytest.raises(transition.ReconciliationTransitionError,
                       match='API-request schema revision 011 or a successor'):
        transition._require_activation_schema(_status(api_revision='008'))
    with pytest.raises(transition.ReconciliationTransitionError,
                       match=('current deployed Serve schema head '
                              f'{transition.migration_utils.SERVE_VERSION}')):
        transition._require_activation_schema(_status(serve_revision='047'))
    with pytest.raises(
            transition.ReconciliationTransitionError,
            match=('current deployed API-request schema head '
                   f'{transition.migration_utils.API_REQUESTS_VERSION}')):
        transition._require_activation_schema(_status(api_revision='011'))


def test_activation_schema_rejects_malformed_or_invalid_deployed_head() -> None:
    with pytest.raises(
            transition.ReconciliationTransitionError,
            match="Serve schema revision 'uninitialized' is malformed"):
        transition._require_activation_schema(
            _status(serve_revision='uninitialized'))
    with mock.patch.object(transition.migration_utils, 'SERVE_VERSION', '046'), \
         pytest.raises(transition.ReconciliationTransitionError,
                       match='does not contain required revision 047'):
        transition._require_activation_schema(_status(serve_revision='046'))


def test_writer_attestation_rejects_compatibility_all_topology() -> None:
    rollout = SimpleNamespace(deployments=(SimpleNamespace(role='api'),),
                              writer_instances=(SimpleNamespace(role='all'),),
                              image_digest='sha256:' + 'a' * 64)
    with mock.patch.object(transition.reserved_capacity_broker,
                           '_read_stable_writer_rollout',
                           return_value=rollout), pytest.raises(
                               transition.ReconciliationTransitionError,
                               match='split api/controller/executor'):
        transition._attest_split_role_writer_cohort()


def test_activation_attests_then_commits_one_successor() -> None:
    before = _status()
    after = _status(gate_state='SEQUENCED_ACTIVE', gate_generation=5)
    repository = _repository(before)
    with mock.patch.object(transition, '_engine', return_value=object()), \
         mock.patch.object(
             transition, '_status', side_effect=[before, after]), \
         mock.patch.object(
             transition,
             '_attest_split_role_writer_cohort',
             return_value=_attestation()) as attest, \
         mock.patch.object(transition, '_durable_claim_scope', return_value=()), \
         mock.patch.object(transition,
                           '_attest_reclaim_enforcement',
                           return_value=_reclaim_evidence()), \
         mock.patch.object(transition.time, 'monotonic', return_value=100.0), \
         mock.patch.object(
             transition.serve_state,
             'reserved_fill_reclaim_gate_authority_guard',
             return_value=contextlib.nullcontext(mock.sentinel.gate_guard)), \
         mock.patch.object(
             transition.pool_capacity_observation,
             'PoolCapacityObservationRepository',
             return_value=repository):
        changed, observed = transition.activate()

    assert changed is True
    assert observed == after
    attest.assert_called_once_with()
    repository.authorize_sequenced_reconciliation.assert_called_once_with(
        expected_generation=4,
        receipt=_receipt(),
        connection=mock.sentinel.activation_connection)


def test_activation_never_reads_rollout_while_authority_is_held() -> None:
    before = _status()
    after = _status(gate_state='SEQUENCED_ACTIVE', gate_generation=5)
    lock_held = False

    @contextlib.contextmanager
    def authority_guard(*, shared: bool):
        nonlocal lock_held
        assert shared is False
        assert not lock_held
        lock_held = True
        try:
            yield mock.sentinel.gate_guard
        finally:
            lock_held = False

    repository = _repository(before)

    def attest() -> transition._WriterCohortAttestation:
        assert not lock_held
        return _attestation()

    with mock.patch.object(transition, '_engine', return_value=object()), \
         mock.patch.object(
             transition, '_status', side_effect=[before, after]), \
         mock.patch.object(
             transition,
             '_attest_split_role_writer_cohort',
             side_effect=attest) as rollout_reader, \
         mock.patch.object(transition, '_durable_claim_scope', return_value=()), \
         mock.patch.object(transition,
                           '_attest_reclaim_enforcement',
                           return_value=_reclaim_evidence()), \
         mock.patch.object(transition.time, 'monotonic', return_value=100.0), \
         mock.patch.object(
             transition.serve_state,
             'reserved_fill_reclaim_gate_authority_guard',
             side_effect=authority_guard), \
         mock.patch.object(
             transition.pool_capacity_observation,
             'PoolCapacityObservationRepository',
             return_value=repository):
        changed, observed = transition.activate()

    assert changed is True
    assert observed == after
    rollout_reader.assert_called_once_with()
    assert not lock_held


def test_activation_rejects_attestation_that_ages_out_waiting_for_lock(
) -> None:
    before = _status()
    repository = _repository(before)
    with mock.patch.object(transition, '_engine', return_value=object()), \
         mock.patch.object(transition, '_status', return_value=before) as status, \
         mock.patch.object(
             transition,
             '_attest_split_role_writer_cohort',
             return_value=_attestation()), \
         mock.patch.object(transition, '_durable_claim_scope', return_value=()), \
         mock.patch.object(transition,
                           '_attest_reclaim_enforcement',
                           return_value=_reclaim_evidence()), \
         mock.patch.object(
             transition.time,
             'monotonic',
             side_effect=[102.0, 102.0, 106.0]), \
         mock.patch.object(
             transition.serve_state,
             'reserved_fill_reclaim_gate_authority_guard',
             return_value=contextlib.nullcontext(mock.sentinel.gate_guard)), \
         mock.patch.object(
             transition.pool_capacity_observation,
             'PoolCapacityObservationRepository',
             return_value=repository), \
         pytest.raises(
             transition.ReconciliationTransitionError,
             match='rollout attestation is stale'):
        transition.activate()

    status.assert_called_once_with(mock.ANY)
    repository.authorize_sequenced_reconciliation.assert_not_called()


def test_current_build_blocks_before_authority_or_gate_cas() -> None:
    before = _status()
    repository = _repository(before)
    with mock.patch.object(transition, '_engine', return_value=object()), \
         mock.patch.object(transition, '_status', return_value=before), \
         mock.patch.object(
             transition,
             '_attest_split_role_writer_cohort',
             return_value=_attestation()), \
         mock.patch.object(transition, '_durable_claim_scope', return_value=()), \
         mock.patch.object(
             transition.reserved_fill_reclaim_attestation,
             'require_unique_policy',
             side_effect=(reserved_fill_reclaim_attestation.
                          ReclaimAttestationError('no policy'))), \
         mock.patch.object(
             transition.serve_state,
             'reserved_fill_reclaim_gate_authority_guard') as guard, \
         mock.patch.object(
             transition.pool_capacity_observation,
             'PoolCapacityObservationRepository',
             return_value=repository), \
         pytest.raises(
             transition.ReconciliationTransitionError,
             match='could not be attested'):
        transition.activate()

    guard.assert_not_called()
    repository.authorize_sequenced_reconciliation.assert_not_called()


def test_status_remains_readable_without_deployment_reclaim_policy() -> None:
    with mock.patch.object(transition, '_engine', return_value=object()), \
         mock.patch.object(transition, '_status', return_value=_status()), \
         mock.patch.object(
             transition.reserved_fill_reclaim_attestation,
             'require_unique_policy') as reclaim_policy:
        exit_code, output = transition.run_cli(['status', '--json'])

    assert exit_code == 0
    assert '"gate_state":"LEGACY_ACTIVE"' in output
    reclaim_policy.assert_not_called()


def test_active_service_reattests_and_reauthorizes_next_generation() -> None:
    old_receipt = _receipt(
        _attestation(deployment_generation='previous-generation'))
    active = _status(gate_state='SEQUENCED_ACTIVE',
                     gate_generation=5,
                     reclaim_activation_receipt=dataclasses.asdict(old_receipt))
    after = _status(gate_state='SEQUENCED_ACTIVE', gate_generation=6)
    repository = _repository(active)
    with mock.patch.object(transition, '_engine', return_value=object()), \
         mock.patch.object(
             transition, '_status', side_effect=[active, after]), \
         mock.patch.object(
             transition,
             '_attest_split_role_writer_cohort',
             return_value=_attestation()) as attest, \
         mock.patch.object(transition, '_durable_claim_scope', return_value=()), \
         mock.patch.object(
             transition,
             '_attest_reclaim_enforcement',
             return_value=_reclaim_evidence()) as reclaim_attest, \
         mock.patch.object(transition.time, 'monotonic', return_value=100.0), \
         mock.patch.object(
             transition.serve_state,
             'reserved_fill_reclaim_gate_authority_guard',
             return_value=contextlib.nullcontext(mock.sentinel.gate_guard)), \
         mock.patch.object(
             transition.pool_capacity_observation,
             'PoolCapacityObservationRepository',
             return_value=repository):
        changed, observed = transition.activate()

    assert changed is True
    assert observed == after
    attest.assert_called_once_with()
    reclaim_attest.assert_called_once_with(_attestation(), ())
    repository.authorize_sequenced_reconciliation.assert_called_once_with(
        expected_generation=5,
        receipt=_receipt(),
        connection=mock.sentinel.activation_connection)


def test_active_service_exact_receipt_is_idempotent_after_reattest() -> None:
    active = _status(gate_state='SEQUENCED_ACTIVE', gate_generation=5)
    repository = _repository(active, changed=False)
    with mock.patch.object(transition, '_engine', return_value=object()), \
         mock.patch.object(
             transition, '_status', side_effect=[active, active]), \
         mock.patch.object(
             transition,
             '_attest_split_role_writer_cohort',
             return_value=_attestation()) as attest, \
         mock.patch.object(transition, '_durable_claim_scope', return_value=()), \
         mock.patch.object(
             transition,
             '_attest_reclaim_enforcement',
             return_value=_reclaim_evidence()) as reclaim_attest, \
         mock.patch.object(transition.time, 'monotonic', return_value=100.0), \
         mock.patch.object(
             transition.serve_state,
             'reserved_fill_reclaim_gate_authority_guard',
             return_value=contextlib.nullcontext(mock.sentinel.gate_guard)), \
         mock.patch.object(
             transition.pool_capacity_observation,
             'PoolCapacityObservationRepository',
             return_value=repository):
        changed, observed = transition.activate()

    assert changed is False
    assert observed == active
    attest.assert_called_once_with()
    reclaim_attest.assert_called_once_with(_attestation(), ())
    repository.authorize_sequenced_reconciliation.assert_called_once_with(
        expected_generation=5,
        receipt=_receipt(),
        connection=mock.sentinel.activation_connection)


def test_activation_rejects_claim_change_after_external_attestation() -> None:
    before = _status()
    repository = _repository(before)
    repository.authorize_sequenced_reconciliation.side_effect = (
        transition.pool_capacity_observation.ReconciliationGateConflictError(
            'claims changed'))
    with mock.patch.object(transition, '_engine', return_value=object()), \
         mock.patch.object(transition, '_status', return_value=before), \
         mock.patch.object(
             transition,
             '_attest_split_role_writer_cohort',
             return_value=_attestation()), \
         mock.patch.object(transition, '_durable_claim_scope', return_value=()), \
         mock.patch.object(
             transition,
             '_attest_reclaim_enforcement',
             return_value=_reclaim_evidence()), \
         mock.patch.object(transition.time, 'monotonic', return_value=100.0), \
         mock.patch.object(
             transition.serve_state,
             'reserved_fill_reclaim_gate_authority_guard',
             return_value=contextlib.nullcontext(mock.sentinel.gate_guard)), \
         mock.patch.object(
             transition.pool_capacity_observation,
             'PoolCapacityObservationRepository',
             return_value=repository), \
         pytest.raises(transition.ReconciliationTransitionError,
                       match='authorization changed or is not safe'):
        transition.activate()

    repository.authorize_sequenced_reconciliation.assert_called_once_with(
        expected_generation=4,
        receipt=_receipt(),
        connection=mock.sentinel.activation_connection)


def test_activation_rejects_post_cas_receipt_mismatch() -> None:
    before = _status()
    mismatched_receipt = dataclasses.asdict(_receipt())
    mismatched_receipt['writer_pod_inventory_sha256'] = 'e' * 64
    after = _status(gate_state='SEQUENCED_ACTIVE',
                    gate_generation=5,
                    reclaim_activation_receipt=mismatched_receipt)
    repository = _repository(before)
    with mock.patch.object(transition, '_engine', return_value=object()), \
         mock.patch.object(
             transition, '_status', side_effect=[before, after]), \
         mock.patch.object(
             transition,
             '_attest_split_role_writer_cohort',
             return_value=_attestation()), \
         mock.patch.object(transition, '_durable_claim_scope', return_value=()), \
         mock.patch.object(
             transition,
             '_attest_reclaim_enforcement',
             return_value=_reclaim_evidence()), \
         mock.patch.object(transition.time, 'monotonic', return_value=100.0), \
         mock.patch.object(
             transition.serve_state,
             'reserved_fill_reclaim_gate_authority_guard',
             return_value=contextlib.nullcontext(mock.sentinel.gate_guard)), \
         mock.patch.object(
             transition.pool_capacity_observation,
             'PoolCapacityObservationRepository',
             return_value=repository), \
         pytest.raises(transition.ReconciliationTransitionError,
                       match='did not persist its exact receipt'):
        transition.activate()


def test_reclaim_attestation_loads_the_unique_policy() -> None:
    policy = mock.create_autospec(
        reserved_fill_reclaim_attestation.ReservedFillReclaimPolicy,
        instance=True)
    evidence = _reclaim_evidence()
    policy.enforcement_contract.return_value = (
        reserved_fill_reclaim_attestation.ReclaimEnforcementContract.
        GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2)
    policy.policy_identity.return_value = evidence.identity
    policy.attest_activation.return_value = evidence
    with mock.patch.object(transition.reserved_fill_reclaim_attestation,
                           'require_unique_policy',
                           return_value=policy) as loader:
        result = transition._attest_reclaim_enforcement(_attestation(), ())

    assert result is evidence
    loader.assert_called_once_with()
    call = policy.attest_activation.call_args
    assert call.args == ((),)
    assert call.kwargs['writer_image_digest'] == 'sha256:' + 'a' * 64
    assert call.kwargs['deadline_monotonic'] > 0


def test_deployed_cli_context_selects_server_and_loads_main_plugins(
        monkeypatch) -> None:
    marker = transition.skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER
    monkeypatch.delenv(marker, raising=False)

    def load(context: Any) -> object:
        assert transition.os.environ[marker] == 'true'
        assert context.context == transition.plugins.PluginContext.MAIN
        return mock.sentinel.registration_barrier

    with mock.patch.object(transition.plugins,
                           'plugins_loaded',
                           return_value=False), \
         mock.patch.object(transition.plugins,
                           'load_plugins',
                           side_effect=load) as load_plugins:
        transition._initialize_deployed_cli_context()

    assert transition.os.environ[marker] == 'true'
    context = load_plugins.call_args.args[0]
    assert context.context == transition.plugins.PluginContext.MAIN


def test_deployed_cli_context_does_not_reload_plugins(monkeypatch) -> None:
    marker = transition.skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER
    monkeypatch.delenv(marker, raising=False)
    with mock.patch.object(transition.plugins,
                           'plugins_loaded',
                           return_value=True), \
         mock.patch.object(transition.plugins, 'load_plugins') as load_plugins:
        transition._initialize_deployed_cli_context()

    assert transition.os.environ[marker] == 'true'
    load_plugins.assert_not_called()


def test_main_initializes_deployed_context_before_running_command(
        capsys) -> None:
    with mock.patch.object(transition,
                           '_initialize_deployed_cli_context') as initialize, \
         mock.patch.object(transition,
                           'run_cli',
                           return_value=(0, 'ready')) as run_cli:
        exit_code = transition.main(['status', '--json'])

    assert exit_code == 0
    assert capsys.readouterr().out == 'ready\n'
    initialize.assert_called_once_with()
    run_cli.assert_called_once_with(['status', '--json'])
