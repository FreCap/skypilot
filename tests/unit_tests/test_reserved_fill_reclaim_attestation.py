"""Tests for the deployment-owned reserved-fill reclaim proof boundary."""

import concurrent.futures
import dataclasses
import threading

import pytest

from sky.serve import reserved_fill_reclaim_attestation as attestation


class _SelectableEntryPoints(tuple):

    def select(self, *, group: str):
        assert group == attestation.POLICY_ENTRY_POINT_GROUP
        return self


class _EntryPoint:

    def __init__(self, target, load_count: list[int] | None = None):
        self._target = target
        self._load_count = load_count

    def load(self):
        if self._load_count is not None:
            self._load_count[0] += 1
        return self._target


class _Policy(attestation.ReservedFillReclaimPolicy):
    """Minimal concrete deployment policy for entry-point tests."""

    def policy_identity(self):
        return attestation.ReclaimPolicyIdentity(
            fleet_bundle_sha256='a' * 64,
            policy_revision='bundle-policy-v1',
            provider_inventory_sha256='b' * 64)

    def enforcement_contract(self):
        return (attestation.ReclaimEnforcementContract.
                GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2)

    def attest_activation(self, claimed_contexts, *, writer_image_digest,
                          deadline_monotonic):
        del claimed_contexts, writer_image_digest, deadline_monotonic
        raise NotImplementedError

    def authorize_claim_set(self, scope, *, expected_identity,
                            expected_gate_generation, deadline_monotonic):
        del scope, expected_identity, expected_gate_generation
        del deadline_monotonic
        raise NotImplementedError

    def renew_provider_proofs(self, *, expected_identity,
                              expected_gate_generation, deadline_monotonic):
        del expected_identity, expected_gate_generation, deadline_monotonic
        raise NotImplementedError

    def authorize_launch(self, scope, *, expected_identity,
                         expected_gate_generation, deadline_monotonic):
        del scope, expected_identity, expected_gate_generation
        del deadline_monotonic
        raise NotImplementedError


@pytest.fixture(autouse=True)
def _empty_policy_cache(monkeypatch):
    monkeypatch.setattr(attestation, '_POLICY_CACHE', None)
    monkeypatch.setattr(attestation, '_POLICY_CACHE_PID', None)


def _admission(
    context: str = 'research',
    accelerator: str = 'a100-80gb',
) -> attestation.ReclaimProjectedAdmission:
    return attestation.ReclaimProjectedAdmission(
        worker_projection_sha256='e' * 64,
        kubernetes_context=context,
        namespace='inference',
        service_account_name='inference-sa',
        pod_identity_role_arn=None,
        scheduler_name='default-scheduler',
        priority_class_name='inference-low',
        priority_value=-1000,
        preemption_policy='Never',
        admission_mode=attestation.ReclaimAdmissionMode.KUEUE,
        local_queue_name='inference',
        workload_priority_class_name='inference-low',
        accelerator=accelerator,
        accelerator_count=1,
        accelerator_scheduling=attestation.ReclaimAcceleratorScheduling(
            label_key='nvidia.com/gpu.product',
            label_values=('NVIDIA-A100-SXM4-80GB',),
            resource_key='nvidia.com/gpu'),
    )


def _claim(context: str = 'research') -> attestation.ReservedContextClaim:
    return attestation.ReservedContextClaim(
        service_name='svc',
        service_version=4,
        service_generation=3,
        pool_key='["v2","uid",["a100-80gb"]]',
        access_context=context,
        physical_cluster_uid='uid',
        accelerator_names=('a100-80gb',),
        projected_admissions=(_admission(context),),
    )


def _launch_scope(context: str = 'research') -> attestation.ReclaimLaunchScope:
    return attestation.ReclaimLaunchScope(
        service_name='svc',
        service_version=4,
        pool_key='["v2","uid",["a100-80gb"]]',
        service_generation=3,
        physical_cluster_uid='uid',
        kubernetes_context=context,
        accelerator='a100-80gb',
        accelerator_count=1,
        projected_admission=_admission(context),
    )


def test_launch_authorization_binds_exact_provider_proof_reference() -> None:
    identity = attestation.ReclaimPolicyIdentity(
        fleet_bundle_sha256='a' * 64,
        policy_revision='bundle-policy-v1',
        provider_inventory_sha256='b' * 64)
    reference = attestation.ReclaimProviderProofReference(
        receipt_nonce='c' * 64,
        proof_sha256='d' * 64,
        identity=identity,
        gate_generation=7,
        kubernetes_context='research',
        completed_monotonic=10.0)

    authorization = attestation.ReclaimLaunchAuthorization(
        identity=identity,
        gate_generation=7,
        scope=_launch_scope(),
        provider_proof_reference=reference,
        completed_monotonic=10.0)

    assert authorization.provider_proof_reference == reference


def test_launch_authorization_handoff_requires_minimum_remaining_freshness(
) -> None:
    identity = attestation.ReclaimPolicyIdentity(
        fleet_bundle_sha256='a' * 64,
        policy_revision='bundle-policy-v1',
        provider_inventory_sha256='b' * 64)
    completed = 10.0
    reference = attestation.ReclaimProviderProofReference(
        receipt_nonce='c' * 64,
        proof_sha256='d' * 64,
        identity=identity,
        gate_generation=7,
        kubernetes_context='research',
        completed_monotonic=completed)
    authorization = attestation.ReclaimLaunchAuthorization(
        identity=identity,
        gate_generation=7,
        scope=_launch_scope(),
        provider_proof_reference=reference,
        completed_monotonic=completed)
    maximum_handoff_age = (
        attestation.AUTHORIZATION_MAX_AGE_SECONDS -
        attestation.LAUNCH_AUTHORIZATION_MIN_REMAINING_SECONDS)

    assert attestation.require_exact_launch_authorization(
        authorization,
        expected_identity=identity,
        expected_gate_generation=7,
        expected_scope=_launch_scope(),
        now_monotonic=completed + maximum_handoff_age - 0.001,
        minimum_remaining_seconds=(
            attestation.LAUNCH_AUTHORIZATION_MIN_REMAINING_SECONDS
        )) is authorization
    with pytest.raises(attestation.ReclaimAttestationError, match='stale'):
        attestation.require_exact_launch_authorization(
            authorization,
            expected_identity=identity,
            expected_gate_generation=7,
            expected_scope=_launch_scope(),
            now_monotonic=completed + maximum_handoff_age,
            minimum_remaining_seconds=(
                attestation.LAUNCH_AUTHORIZATION_MIN_REMAINING_SECONDS))


@pytest.mark.parametrize('mismatch', ('identity', 'gate', 'context'))
def test_launch_authorization_rejects_provider_proof_reference_mismatch(
        mismatch: str) -> None:
    identity = attestation.ReclaimPolicyIdentity(
        fleet_bundle_sha256='a' * 64,
        policy_revision='bundle-policy-v1',
        provider_inventory_sha256='b' * 64)
    reference = attestation.ReclaimProviderProofReference(
        receipt_nonce='c' * 64,
        proof_sha256='d' * 64,
        identity=identity,
        gate_generation=7,
        kubernetes_context='research',
        completed_monotonic=10.0)
    authorization_identity = identity
    authorization_gate = 7
    scope = _launch_scope()
    completed_monotonic = 10.0
    if mismatch == 'identity':
        authorization_identity = dataclasses.replace(
            identity, provider_inventory_sha256='e' * 64)
    elif mismatch == 'gate':
        authorization_gate = 8
    elif mismatch == 'context':
        scope = _launch_scope('secondary')
    with pytest.raises(ValueError, match='reference|completion'):
        attestation.ReclaimLaunchAuthorization(
            identity=authorization_identity,
            gate_generation=authorization_gate,
            scope=scope,
            provider_proof_reference=reference,
            completed_monotonic=completed_monotonic)


def test_launch_authorization_is_newer_than_provider_observation() -> None:
    identity = attestation.ReclaimPolicyIdentity(
        fleet_bundle_sha256='a' * 64,
        policy_revision='bundle-policy-v1',
        provider_inventory_sha256='b' * 64)
    reference = attestation.ReclaimProviderProofReference(
        receipt_nonce='c' * 64,
        proof_sha256='d' * 64,
        identity=identity,
        gate_generation=7,
        kubernetes_context='research',
        completed_monotonic=10.0)
    authorization = attestation.ReclaimLaunchAuthorization(
        identity=identity,
        gate_generation=7,
        scope=_launch_scope(),
        provider_proof_reference=reference,
        completed_monotonic=10.1)
    assert authorization.completed_monotonic > (
        authorization.provider_proof_reference.completed_monotonic)
    with pytest.raises(ValueError, match='cannot predate'):
        dataclasses.replace(authorization, completed_monotonic=9.9)


def test_projected_admission_rejects_invalid_pod_identity_role() -> None:
    with pytest.raises(ValueError, match='AWS IAM role ARN'):
        dataclasses.replace(_admission(), pod_identity_role_arn='not-an-arn')


def test_projected_admission_requires_typed_accelerator_scheduling() -> None:
    with pytest.raises(ValueError,
                       match='accelerator_scheduling must be typed'):
        dataclasses.replace(_admission(), accelerator_scheduling=None)


def test_projected_admission_accepts_scheduler_authority_without_kueue(
) -> None:
    admission = dataclasses.replace(
        _admission(),
        scheduler_name='gpu-binpack-scheduler',
        admission_mode=attestation.ReclaimAdmissionMode.KUBERNETES_SCHEDULER,
        local_queue_name=None,
        workload_priority_class_name=None)

    assert admission.admission_mode is (
        attestation.ReclaimAdmissionMode.KUBERNETES_SCHEDULER)
    assert admission.local_queue_name is None
    assert admission.workload_priority_class_name is None


def test_projection_derives_scheduler_authority_without_kueue() -> None:
    admission = attestation.projected_admission_from_worker_projection(
        {
            'kubernetes_context': 'research',
            'namespace': 'inference',
            'service_account_name': 'inference-sa',
            'pod_identity_role_arn': None,
            'scheduler_name': 'gpu-binpack-scheduler',
            'priority_class_name': 'inference-low',
            'priority_value': -1000,
            'preemption_policy': 'Never',
            'kueue_admission': None,
            'accelerator_name': 'A100',
            'accelerator_count': 1,
            'accelerator_scheduling': {
                'label_key': 'nvidia.com/gpu.product',
                'label_values': ['NVIDIA-A100-SXM4-40GB'],
                'resource_key': 'nvidia.com/gpu',
            },
        },
        worker_projection_sha256='f' * 64)

    assert admission.admission_mode is (
        attestation.ReclaimAdmissionMode.KUBERNETES_SCHEDULER)
    assert admission.scheduler_name == 'gpu-binpack-scheduler'
    assert admission.local_queue_name is None


def test_projected_admission_rejects_mode_payload_mismatch() -> None:
    with pytest.raises(ValueError, match='cannot carry Kueue queue identity'):
        dataclasses.replace(
            _admission(),
            admission_mode=(
                attestation.ReclaimAdmissionMode.KUBERNETES_SCHEDULER))


def test_accelerator_scheduling_requires_canonical_label_values() -> None:
    with pytest.raises(ValueError, match='unique sorted nonempty text'):
        attestation.ReclaimAcceleratorScheduling(
            label_key='nvidia.com/gpu.product',
            label_values=('NVIDIA-H200', 'NVIDIA-A100-SXM4-80GB'),
            resource_key='nvidia.com/gpu')


def _evidence(
    claims: tuple[attestation.ReservedContextClaim, ...]
) -> attestation.ReclaimEnforcementEvidence:
    return attestation.ReclaimEnforcementEvidence(
        contract=(attestation.ReclaimEnforcementContract.
                  GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2),
        fleet_bundle_sha256='a' * 64,
        policy_revision='bundle-policy-v1',
        provider_inventory_sha256='b' * 64,
        claimed_contexts=claims,
        completed_monotonic=10.0,
    )


def test_unique_policy_rejects_zero_or_multiple_entry_points(
        monkeypatch) -> None:
    monkeypatch.setattr(attestation.importlib.metadata, 'entry_points',
                        _SelectableEntryPoints)
    with pytest.raises(attestation.ReclaimAttestationError,
                       match='exactly one.*discovered 0'):
        attestation.require_unique_policy()

    monkeypatch.setattr(
        attestation.importlib.metadata, 'entry_points',
        lambda: _SelectableEntryPoints(
            (_EntryPoint(_Policy), _EntryPoint(_Policy))))
    with pytest.raises(attestation.ReclaimAttestationError,
                       match='exactly one.*discovered 2'):
        attestation.require_unique_policy()


def test_unique_policy_rejects_a_non_policy_target(monkeypatch) -> None:
    monkeypatch.setattr(attestation.importlib.metadata, 'entry_points',
                        lambda: _SelectableEntryPoints((_EntryPoint(object),)))

    with pytest.raises(attestation.ReclaimAttestationError,
                       match='could not be loaded'):
        attestation.require_unique_policy()


def test_unique_policy_loads_and_constructs_exactly_once(monkeypatch) -> None:
    load_count = [0]
    construction_count = [0]

    class CountingPolicy(_Policy):

        def __init__(self):
            construction_count[0] += 1

    monkeypatch.setattr(
        attestation.importlib.metadata, 'entry_points',
        lambda: _SelectableEntryPoints(
            (_EntryPoint(CountingPolicy, load_count),)))

    first = attestation.require_unique_policy()
    second = attestation.require_unique_policy()

    assert first is second
    assert load_count == [1]
    assert construction_count == [1]


def test_unique_policy_mutex_serializes_simultaneous_cache_misses(
        monkeypatch) -> None:
    worker_count = 8
    callers_ready = threading.Barrier(worker_count)
    constructor_entered = threading.Event()
    release_constructor = threading.Event()
    construction_count = [0]

    class BlockingPolicy(_Policy):

        def __init__(self):
            construction_count[0] += 1
            constructor_entered.set()
            assert release_constructor.wait(timeout=5)

    monkeypatch.setattr(
        attestation.importlib.metadata, 'entry_points',
        lambda: _SelectableEntryPoints((_EntryPoint(BlockingPolicy),)))

    def load_policy():
        callers_ready.wait(timeout=5)
        return attestation.require_unique_policy()

    with concurrent.futures.ThreadPoolExecutor(
            max_workers=worker_count) as executor:
        futures = [executor.submit(load_policy) for _ in range(worker_count)]
        assert constructor_entered.wait(timeout=5)
        release_constructor.set()
        policies = [future.result(timeout=5) for future in futures]

    assert construction_count == [1]
    assert all(policy is policies[0] for policy in policies)


def test_unique_policy_cache_is_process_scoped(monkeypatch) -> None:
    load_count = [0]
    monkeypatch.setattr(
        attestation.importlib.metadata, 'entry_points',
        lambda: _SelectableEntryPoints((_EntryPoint(_Policy, load_count),)))
    process_ids = iter((101, 101, 202))
    monkeypatch.setattr(attestation.os, 'getpid', process_ids.__next__)

    first = attestation.require_unique_policy()
    assert attestation.require_unique_policy() is first
    assert attestation.require_unique_policy() is not first
    assert load_count == [2]


def test_exact_evidence_requires_complete_current_scope() -> None:
    claim = _claim()
    assert attestation.require_exact_evidence(_evidence(
        (claim,)), (claim,)).claimed_contexts == (claim,)
    with pytest.raises(attestation.ReclaimAttestationError,
                       match='exactly cover'):
        attestation.require_exact_evidence(_evidence(()), (claim,))


def test_exact_evidence_rejects_pre_admission_v1_contract() -> None:
    evidence = attestation.ReclaimEnforcementEvidence(
        contract=(attestation.ReclaimEnforcementContract.
                  GLOBAL_FLEET_CLAIM_AND_LAUNCH_FENCES_V1),
        fleet_bundle_sha256='a' * 64,
        policy_revision='bundle-policy-v1',
        provider_inventory_sha256='b' * 64,
        claimed_contexts=(),
        completed_monotonic=10.0,
    )

    with pytest.raises(attestation.ReclaimAttestationError,
                       match='immutable worker admission'):
        attestation.require_exact_evidence(evidence, ())


def test_activation_receipt_projection_is_deterministic() -> None:
    claims = (_claim('research'), _claim('secondary'))
    writer = {
        'writer_image_digest': f'sha256:{"c" * 64}',
        'writer_deployment_generation': 'generation-7',
        'writer_deployment_uid': 'uid-7',
        'writer_pod_inventory_count': 2,
        'writer_pod_inventory_sha256': 'd' * 64,
    }

    first = attestation.activation_receipt(_evidence(claims), **writer)
    second = attestation.activation_receipt(_evidence(claims), **writer)

    assert first == second
    assert first.identity == _evidence(claims).identity
    assert first.claim_scope_count == 2
    assert first.claim_scope_sha256 == (
        '9fad48ed2c59631c2d0c4433edf8e0f7785a5667734288e980aa2f340e454264')
    assert first.evidence_sha256 == (
        'c0d6c131d94380ef7210fd855d562c55b15ca8833b036053cb26d13d393a7f9b')


@pytest.mark.parametrize(
    ('field', 'replacement', 'message'),
    [
        ('writer_image_digest', f'sha256:{"C" * 64}', 'sha256 digest'),
        ('writer_deployment_generation', '', 'must be nonempty'),
        ('writer_pod_inventory_count', 0, 'must be a positive integer'),
        ('writer_pod_inventory_sha256', 'not-a-digest', 'lowercase SHA-256'),
    ],
)
def test_activation_receipt_validates_writer_projection(field, replacement,
                                                        message) -> None:
    writer = {
        'writer_image_digest': f'sha256:{"c" * 64}',
        'writer_deployment_generation': 'generation-7',
        'writer_deployment_uid': 'uid-7',
        'writer_pod_inventory_count': 2,
        'writer_pod_inventory_sha256': 'd' * 64,
    }
    writer[field] = replacement

    with pytest.raises(ValueError, match=message):
        attestation.activation_receipt(_evidence((_claim(),)), **writer)


def test_claim_scope_projection_rejects_noncanonical_order() -> None:
    with pytest.raises(ValueError, match='unique sorted tuple'):
        attestation.claim_scope_projection(
            (_claim('secondary'), _claim('research')))


def test_evidence_contract_is_closed_to_global_claim_and_launch_fences(
) -> None:
    with pytest.raises(ValueError, match='ReclaimEnforcementContract'):
        attestation.ReclaimEnforcementEvidence(  # type: ignore[arg-type]
            contract='snapshot-only',
            fleet_bundle_sha256='a' * 64,
            policy_revision='bad',
            provider_inventory_sha256='b' * 64,
            claimed_contexts=(),
            completed_monotonic=10.0,
        )


def test_policy_operation_deadline_is_absolute_and_fail_closed(
        monkeypatch) -> None:
    monotonic = iter((10.0, 14.9, 15.1))
    monkeypatch.setattr(attestation.time, 'monotonic', lambda: next(monotonic))

    deadline = attestation.new_policy_operation_deadline()
    assert deadline == 15.0
    attestation.require_policy_operation_completed(deadline)
    with pytest.raises(attestation.ReclaimAttestationError,
                       match='exceeded its deadline'):
        attestation.require_policy_operation_completed(deadline)


def test_provider_proof_operation_has_its_distinct_refresh_deadline(
        monkeypatch) -> None:
    monkeypatch.setattr(attestation.time, 'monotonic', lambda: 10.0)

    deadline = attestation.new_provider_proof_operation_deadline()

    assert deadline == (10.0 +
                        attestation.PROVIDER_PROOF_REFRESH_TIMEOUT_SECONDS)
    assert deadline > 10.0 + attestation.POLICY_OPERATION_TIMEOUT_SECONDS


def test_policy_revision_uses_the_same_utf8_bound_as_serve045() -> None:
    attestation.ReclaimPolicyIdentity(fleet_bundle_sha256='a' * 64,
                                      policy_revision='x' *
                                      attestation.POLICY_REVISION_MAX_BYTES,
                                      provider_inventory_sha256='b' * 64)
    with pytest.raises(ValueError, match='at most 1024 UTF-8 bytes'):
        attestation.ReclaimPolicyIdentity(
            fleet_bundle_sha256='a' * 64,
            policy_revision='x' * (attestation.POLICY_REVISION_MAX_BYTES + 1),
            provider_inventory_sha256='b' * 64)


def test_claim_scope_is_canonical_and_duplicate_free() -> None:
    with pytest.raises(ValueError, match='unique sorted'):
        attestation.ReservedContextClaim(
            service_name='svc',
            service_version=1,
            service_generation=1,
            pool_key='pool',
            access_context='context',
            physical_cluster_uid='uid',
            accelerator_names=('h100', 'a100-80gb'),
            projected_admissions=(_admission(accelerator='h100'), _admission()),
        )
    claim = _claim()
    with pytest.raises(ValueError, match='unique and sorted'):
        _evidence((claim, claim))
