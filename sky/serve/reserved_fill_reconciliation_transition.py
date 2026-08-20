"""Operator entrypoint for reserved-fill authorization generations.

The command deliberately has no demotion path. It attests the complete
split-role writer cohort and reclaim contract, then either activates sequenced
reconciliation or reauthorizes its next fix-forward generation through the
same compare-and-swap. Runtime readers fail closed unless their exact
generation remains authoritative.
"""
# pylint: disable=protected-access

import argparse
from collections.abc import Sequence
import dataclasses
import json
import math
import os
import sys
import time
from typing import Any

from sky.serve import pool_capacity_observation
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_reclaim_attestation
from sky.serve import serve_state
from sky.server import plugins
from sky.skylet import constants as skylet_constants
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

_REQUIRED_SERVE_SCHEMA = '047'
_REQUIRED_API_REQUEST_SCHEMA = '011'
_REQUIRED_WRITER_ROLES = frozenset({'api', 'controller', 'executor'})
_WRITER_ATTESTATION_MAX_AGE_SECONDS = 5.0
_RECLAIM_ATTESTATION_MAX_AGE_SECONDS = 5.0


class ReconciliationTransitionError(RuntimeError):
    """The one-way reconciliation transition failed a mechanical proof."""


@dataclasses.dataclass(frozen=True)
class ReconciliationTransitionStatus:
    """Bounded operator-visible transition state."""

    protocol_version: int
    gate_state: str
    gate_generation: int
    serve_schema_revision: str
    api_request_schema_revision: str
    reclaim_policy_identity: dict[str, str] | None
    reclaim_activation_receipt: dict[str, Any] | None = None
    reclaim_authorized_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class _WriterCohortAttestation:
    """Immutable identity and completion time of one rollout proof."""

    image_digest: str
    deployment_generation: str
    deployment_uid: str
    pod_inventory_count: int
    pod_inventory_sha256: str
    completed_monotonic: float


def _engine() -> Any:
    if not os.environ.get(skylet_constants.ENV_VAR_DB_CONNECTION_URI):
        raise ReconciliationTransitionError(
            'Central PostgreSQL configuration is missing.')
    engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ReconciliationTransitionError(
            'Sequenced reconciliation requires central PostgreSQL.')
    return engine


def _status(engine: Any) -> ReconciliationTransitionStatus:
    serve_revision = migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME)
    api_revision = migration_utils.get_current_alembic_revision(
        engine, migration_utils.API_REQUESTS_DB_NAME)
    protocol = serve_state.get_reserved_fill_protocol_state()
    try:
        protocol_version = int(protocol['protocol_version'])
    except (KeyError, TypeError, ValueError) as error:
        raise ReconciliationTransitionError(
            'Reserved-fill protocol state is malformed.') from error
    repository = (
        pool_capacity_observation.PoolCapacityObservationRepository(engine))
    gate = repository.read_reconciliation_gate()
    return ReconciliationTransitionStatus(
        protocol_version=protocol_version,
        gate_state=gate.state.value,
        gate_generation=gate.generation,
        serve_schema_revision=serve_revision or 'uninitialized',
        api_request_schema_revision=api_revision or 'uninitialized',
        reclaim_policy_identity=(None if gate.reclaim_policy_identity is None
                                 else dataclasses.asdict(
                                     gate.reclaim_policy_identity)),
        reclaim_activation_receipt=(None if gate.reclaim_activation_receipt
                                    is None else dataclasses.asdict(
                                        gate.reclaim_activation_receipt)),
        reclaim_authorized_at=gate.reclaim_authorized_at)


def _require_activation_schema(status: ReconciliationTransitionStatus) -> None:
    if status.protocol_version != reserved_capacity_broker.PROTOCOL_V2:
        raise ReconciliationTransitionError(
            'Reserved-fill protocol v2 must already be active.')
    _require_current_successor_schema(
        schema_name='Serve',
        observed=status.serve_schema_revision,
        required=_REQUIRED_SERVE_SCHEMA,
        deployed_head=migration_utils.SERVE_VERSION)
    _require_current_successor_schema(
        schema_name='API-request',
        observed=status.api_request_schema_revision,
        required=_REQUIRED_API_REQUEST_SCHEMA,
        deployed_head=migration_utils.API_REQUESTS_VERSION)


def _require_current_successor_schema(*, schema_name: str, observed: str,
                                      required: str,
                                      deployed_head: str) -> None:
    """Requires the deployed linear migration head to retain a prerequisite."""
    try:
        observed_number = int(observed)
        required_number = int(required)
        deployed_head_number = int(deployed_head)
    except ValueError as error:
        raise ReconciliationTransitionError(
            f'{schema_name} schema revision {observed!r} is malformed.') \
            from error
    if deployed_head_number < required_number:
        raise ReconciliationTransitionError(
            f'The deployed {schema_name} schema head {deployed_head} does not '
            f'contain required revision {required}.')
    if observed_number < required_number:
        raise ReconciliationTransitionError(
            f'Sequenced reconciliation requires {schema_name} schema '
            f'revision {required} or a successor; observed {observed}.')
    if observed != deployed_head:
        raise ReconciliationTransitionError(
            f'Sequenced reconciliation requires the current deployed '
            f'{schema_name} schema head {deployed_head}; observed {observed}. '
            'Complete or repair the forward migration before activation.')


def _attest_split_role_writer_cohort() -> _WriterCohortAttestation:
    """Require one stable, same-image split-role writer cohort."""
    try:
        rollout = reserved_capacity_broker._read_stable_writer_rollout()
    except reserved_capacity_broker.ProtocolV2ActivationError as error:
        raise ReconciliationTransitionError(
            'The complete writer rollout could not be attested.') from error
    deployment_roles = {deployment.role for deployment in rollout.deployments}
    instance_roles = {instance.role for instance in rollout.writer_instances}
    if (deployment_roles != _REQUIRED_WRITER_ROLES or
            instance_roles != _REQUIRED_WRITER_ROLES or
            any(instance.role == 'all'
                for instance in rollout.writer_instances)):
        raise ReconciliationTransitionError(
            'Sequenced reconciliation supports exactly the split '
            'api/controller/executor PostgreSQL writer topology.')
    # Materialize the complete immutable rollout identity before returning.
    # The transition command itself is running from the aggregate image digest,
    # so no additional self-reported capability bit is trusted.
    return _WriterCohortAttestation(
        image_digest=rollout.image_digest,
        deployment_generation=rollout.deployment_generation,
        deployment_uid=rollout.deployment_uid,
        pod_inventory_count=rollout.pod_inventory_count,
        pod_inventory_sha256=rollout.pod_inventory_sha256,
        completed_monotonic=time.monotonic())


def _require_fresh_writer_attestation(
        attestation: _WriterCohortAttestation) -> None:
    """Fail closed if rollout evidence is stale or from another clock epoch."""
    now = time.monotonic()
    completed = attestation.completed_monotonic
    age = now - completed
    if (not math.isfinite(completed) or not math.isfinite(now) or age < 0 or
            age > _WRITER_ATTESTATION_MAX_AGE_SECONDS):
        raise ReconciliationTransitionError(
            'The split-role writer rollout attestation is stale; rerun the '
            'activation command against a stable rollout.')


def _require_same_writer_cohort(before: _WriterCohortAttestation,
                                after: _WriterCohortAttestation) -> None:
    """Fail closed if any immutable writer identity changed during proof."""
    before_identity = (
        before.image_digest,
        before.deployment_generation,
        before.deployment_uid,
        before.pod_inventory_count,
        before.pod_inventory_sha256,
    )
    after_identity = (
        after.image_digest,
        after.deployment_generation,
        after.deployment_uid,
        after.pod_inventory_count,
        after.pod_inventory_sha256,
    )
    if before_identity != after_identity:
        raise ReconciliationTransitionError(
            'The split-role writer rollout changed during reclaim '
            'attestation; rerun the activation command against a stable '
            'rollout.')


def _durable_claim_scope(
) -> tuple[reserved_fill_reclaim_attestation.ReservedContextClaim, ...]:
    """Read the exact activation scope later revalidated under locks."""
    try:
        repository = (
            pool_capacity_observation.PoolCapacityObservationRepository(
                _engine()))
        return repository.read_activation_claim_scope()
    except (pool_capacity_observation.PoolCapacityObservationError, TypeError,
            ValueError, RuntimeError) as error:
        raise ReconciliationTransitionError(
            'The complete durable reserved-context scope could not be read.') \
            from error


def _activate_in_authority_transaction(
    connection: Any,
    repository: pool_capacity_observation.PoolCapacityObservationRepository,
    receipt: reserved_fill_reclaim_attestation.ReclaimActivationReceipt,
) -> tuple[bool, pool_capacity_observation.ReconciliationGate]:
    """Authorize the exact receipt in the caller's locked transaction."""
    before = repository.lock_reconciliation_gate_for_activation(connection)
    if before.state not in (
            pool_capacity_observation.ReconciliationGateState.LEGACY_ACTIVE,
            pool_capacity_observation.ReconciliationGateState.SEQUENCED_ACTIVE,
    ):
        raise ReconciliationTransitionError(
            'Reserved-fill reconciliation gate is malformed.')
    try:
        result = repository.authorize_sequenced_reconciliation(
            expected_generation=before.generation,
            receipt=receipt,
            connection=connection)
    except pool_capacity_observation.PoolCapacityObservationError as error:
        raise ReconciliationTransitionError(
            'The durable reclaim authorization changed or is not safe.') \
            from error
    return result.changed, result.gate


def _attest_reclaim_enforcement(
    writer_attestation: _WriterCohortAttestation,
    claimed_contexts: tuple[
        reserved_fill_reclaim_attestation.ReservedContextClaim, ...],
) -> reserved_fill_reclaim_attestation.ReclaimEnforcementEvidence:
    """Run the deployment-owned provider proof outside the broker lock."""
    try:
        policy = reserved_fill_reclaim_attestation.require_unique_policy()
        local_identity = (
            reserved_fill_reclaim_attestation.require_policy_identity(policy))
        deadline = (
            reserved_fill_reclaim_attestation.new_policy_operation_deadline())
        evidence = policy.attest_activation(
            claimed_contexts,
            writer_image_digest=writer_attestation.image_digest,
            deadline_monotonic=deadline)
        (reserved_fill_reclaim_attestation.require_policy_operation_completed
        )(deadline)
        evidence = reserved_fill_reclaim_attestation.require_exact_evidence(
            evidence, claimed_contexts)
        if evidence.identity != local_identity:
            raise reserved_fill_reclaim_attestation.ReclaimAttestationError(
                'The activation proof identity differs from the local '
                'deployment policy identity.')
        reserved_fill_reclaim_attestation.require_exact_policy_identity(
            policy, evidence.identity)
        return evidence
    except (reserved_fill_reclaim_attestation.ReclaimAttestationError,
            TypeError, ValueError, RuntimeError) as error:
        raise ReconciliationTransitionError(
            'The deployment reclaim contract could not be attested.') from error


def _require_fresh_reclaim_attestation(
    evidence: reserved_fill_reclaim_attestation.ReclaimEnforcementEvidence,
) -> None:
    """Reject provider evidence that aged out before PostgreSQL revalidation."""
    now = time.monotonic()
    completed = evidence.completed_monotonic
    age = now - completed
    if (not math.isfinite(completed) or not math.isfinite(now) or age < 0 or
            age > _RECLAIM_ATTESTATION_MAX_AGE_SECONDS):
        raise ReconciliationTransitionError(
            'The deployment reclaim attestation is stale; rerun activation.')


def activate() -> tuple[bool, ReconciliationTransitionStatus]:
    """Attest and authorize the next exact reconciliation generation."""
    engine = _engine()
    initial = _status(engine)
    _require_activation_schema(initial)
    gate_states = pool_capacity_observation.ReconciliationGateState
    valid_states = {
        gate_states.LEGACY_ACTIVE.value,
        gate_states.SEQUENCED_ACTIVE.value,
    }
    if initial.gate_state not in valid_states:
        raise ReconciliationTransitionError(
            'Reserved-fill reconciliation gate is malformed.')

    # Kubernetes provider reads are deliberately complete before acquiring the
    # broker lock. Bind the potentially slow provider proof to one fresh writer
    # cohort, then prove that exact immutable cohort is still current. Only the
    # post-proof attestation may cross the lock boundary, within its short
    # process-monotonic freshness window.
    pre_proof_attestation = _attest_split_role_writer_cohort()
    claimed_contexts = _durable_claim_scope()
    _require_fresh_writer_attestation(pre_proof_attestation)
    reclaim_evidence = _attest_reclaim_enforcement(pre_proof_attestation,
                                                   claimed_contexts)
    post_proof_attestation = _attest_split_role_writer_cohort()
    _require_same_writer_cohort(pre_proof_attestation, post_proof_attestation)
    receipt = reserved_fill_reclaim_attestation.activation_receipt(
        reclaim_evidence,
        writer_image_digest=post_proof_attestation.image_digest,
        writer_deployment_generation=(
            post_proof_attestation.deployment_generation),
        writer_deployment_uid=post_proof_attestation.deployment_uid,
        writer_pod_inventory_count=post_proof_attestation.pod_inventory_count,
        writer_pod_inventory_sha256=(
            post_proof_attestation.pod_inventory_sha256))
    _require_fresh_writer_attestation(post_proof_attestation)
    _require_fresh_reclaim_attestation(reclaim_evidence)
    with serve_state.reserved_fill_reclaim_gate_authority_guard(
            shared=False) as gate_guard:
        if not (serve_state.reserved_fill_reclaim_gate_authority_guard_is_valid(
                gate_guard)):
            raise ReconciliationTransitionError(
                'The fleet reclaim guard lost its PostgreSQL session.')
        # Composite broker/fleet lock acquisition may block. Reject evidence
        # that aged out before consulting or mutating durable state.
        _require_fresh_writer_attestation(post_proof_attestation)
        _require_fresh_reclaim_attestation(reclaim_evidence)
        repository = (
            pool_capacity_observation.PoolCapacityObservationRepository(engine))
        changed, committed_gate = (
            serve_state.run_reserved_fill_reclaim_activation_transaction(
                gate_guard,
                lambda connection: _activate_in_authority_transaction(
                    connection, repository, receipt)))
        if not (serve_state.reserved_fill_reclaim_gate_authority_guard_is_valid(
                gate_guard)):
            raise ReconciliationTransitionError(
                'The fleet reclaim guard became indeterminate during '
                'activation; inspect the durable gate before retrying.')
        after = _status(engine)
        if (after.gate_state != committed_gate.state.value or
                after.gate_generation != committed_gate.generation or
                after.reclaim_policy_identity
                != (None if committed_gate.reclaim_policy_identity is None else
                    dataclasses.asdict(committed_gate.reclaim_policy_identity))
                or after.reclaim_activation_receipt !=
            (None if committed_gate.reclaim_activation_receipt is None else
             dataclasses.asdict(committed_gate.reclaim_activation_receipt)) or
                after.reclaim_authorized_at
                != committed_gate.reclaim_authorized_at):
            raise ReconciliationTransitionError(
                'The reconciliation authorization CAS did not persist its '
                'exact receipt.')
        return changed, after


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=('Inspect or authorize a SkyServe sequenced reserved-fill '
                     'generation.'))
    subparsers = parser.add_subparsers(dest='command', required=True)
    status_parser = subparsers.add_parser('status')
    status_parser.add_argument('--json', action='store_true')
    subparsers.add_parser('activate')
    return parser


def run_cli(argv: Sequence[str] | None = None) -> tuple[int, str]:
    """Run one transition command and return status plus one output line."""
    args = _build_parser().parse_args(argv)
    try:
        if args.command == 'status':
            status = _status(_engine())
            payload = status.to_dict()
            if args.json:
                return 0, json.dumps(payload,
                                     sort_keys=True,
                                     separators=(',', ':'))
            return 0, ', '.join(
                f'{key}={value}' for key, value in payload.items())
        changed, status = activate()
        payload = {'changed': changed, **status.to_dict()}
        return 0, json.dumps(payload, sort_keys=True, separators=(',', ':'))
    except (KeyError, TypeError, ValueError,
            pool_capacity_observation.PoolCapacityObservationError,
            ReconciliationTransitionError, RuntimeError) as error:
        return 1, f'Reserved-fill reconciliation transition failed: {error}'


def _initialize_deployed_cli_context() -> None:
    """Loads the same server-owned policy registry used by the main role."""
    os.environ.setdefault(skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER, 'true')
    if not plugins.plugins_loaded():
        plugins.load_plugins(
            plugins.ExtensionContext(context=plugins.PluginContext.MAIN))


def main(argv: Sequence[str] | None = None) -> int:
    try:
        _initialize_deployed_cli_context()
        exit_code, output = run_cli(argv)
    except (ImportError, KeyError, TypeError, ValueError,
            RuntimeError) as error:
        exit_code = 1
        output = ('Reserved-fill reconciliation transition failed to '
                  f'initialize the deployed server context: {error}')
    print(output, file=sys.stderr if exit_code else sys.stdout)
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
