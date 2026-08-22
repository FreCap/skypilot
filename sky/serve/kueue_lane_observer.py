"""Serve-owned durable callback for Pod-only Kueue admission facts."""
from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import datetime
import logging
import re
from typing import Any
import uuid

import sqlalchemy

from sky import sky_logging
from sky.adaptors import kubernetes as kubernetes_adaptor
from sky.provision import common as provision_common
from sky.serve import constants as serve_constants
from sky.serve import kueue_lane_lineage
from sky.serve import ordinary_launch_binding
from sky.serve import provider_phase
from sky.serve import reserved_capacity
from sky.serve import reserved_fill_reclaim_attestation
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import zero_cost_actuation_schema

_SHA256_RE = re.compile(r'[0-9a-f]{64}')
_SERVICES = serve_state_schema.services_table
_LIFECYCLES = serve_state_schema.service_lifecycle_fences_table
_REPLICAS = serve_state_schema.replicas_table
_INTENTS = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
_ASSOCIATIONS = ordinary_launch_binding.ordinary_launch_associations_table

logger: logging.Logger = sky_logging.init_logger(__name__)


def project_exact_pod_absence_after_teardown(
    service_name: str,
    replica_id: int,
    replica_record_id: str | uuid.UUID,
) -> bool:
    """Record exact Pod absence after a normal Kueue replica teardown.

    ``False`` means the replica does not own a Kueue admission.  Every other
    non-ABSENT outcome fails closed: a same-UID Pod is still present, a
    different UID is a replacement, and provider/context failures are
    unknown.  No SQL or advisory lock spans the CoreV1 read.
    """
    service_name = _nonempty(service_name, 'service_name')
    replica_id = _positive_int(replica_id, 'replica_id')
    try:
        record_uuid = (replica_record_id if
                       isinstance(replica_record_id, uuid.UUID) else uuid.UUID(
                           str(replica_record_id)))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError('replica_record_id must be a UUID.') from error

    engine = serve_state_schema.get_database_engine()
    repository = kueue_lane_lineage.KueueAdmissionRepository(engine)
    with engine.connect() as connection:
        decision = repository.load_exact_pod_absence_probe_target_in_connection(
            connection,
            service_name=service_name,
            replica_id=replica_id,
            replica_record_id=record_uuid)
    decision.validate()
    if decision.state is (
            kueue_lane_lineage.PhysicalAbsenceLoadState.ALREADY_PROVEN):
        return True
    if decision.state is (
            kueue_lane_lineage.PhysicalAbsenceLoadState.NOT_APPLICABLE):
        return project_admissionless_physical_absence_after_teardown(
            service_name, replica_id, record_uuid)
    target = decision.target
    assert target is not None

    try:
        with provider_phase.provider_phase(
                provider_phase.ProviderPhaseMode.V2_FENCED):
            with kubernetes_adaptor.physical_cluster_uid_fence(
                    target.identity.kubernetes_context,
                    target.identity.physical_cluster_uid):
                # Sample immediately before the unlocked provider read.  The
                # short connection is closed before CoreV1 is invoked, so it
                # retains neither a transaction nor a row/advisory lock.
                with engine.connect() as connection:
                    provider_read_started_at = connection.execute(
                        sqlalchemy.select(
                            sqlalchemy.func.clock_timestamp())).scalar_one()
                try:
                    pod = kubernetes_adaptor.core_api(
                        target.identity.kubernetes_context).read_namespaced_pod(
                            target.pod_name,
                            target.pod_namespace,
                            _request_timeout=kubernetes_adaptor.API_TIMEOUT)
                except kubernetes_adaptor.api_exception() as error:
                    if error.status != 404:
                        raise kueue_lane_lineage.KueueAdmissionConflict(
                            'Exact Kueue Pod absence is UNKNOWN after provider '
                            'API failure.') from error
                else:
                    metadata = getattr(pod, 'metadata', None)
                    observed_uid = getattr(metadata, 'uid', None)
                    if observed_uid == target.pod_uid:
                        raise kueue_lane_lineage.KueueAdmissionConflict(
                            'Exact Kueue Pod remains PRESENT after teardown.')
                    if isinstance(observed_uid, str) and observed_uid:
                        raise kueue_lane_lineage.KueueAdmissionConflict(
                            'Exact Kueue Pod name was REPLACED after teardown.')
                    raise kueue_lane_lineage.KueueAdmissionConflict(
                        'Exact Kueue Pod identity is UNKNOWN after teardown.')
    except kueue_lane_lineage.KueueAdmissionConflict:
        raise
    except Exception as error:  # pylint: disable=broad-except
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Exact Kueue Pod absence is UNKNOWN because its physical cluster '
            'identity could not be proved.') from error

    with engine.begin() as connection:
        repository.record_exact_pod_absence_after_normal_teardown_in_connection(
            connection,
            target,
            provider_read_started_at=provider_read_started_at)
    return True


def project_admissionless_physical_absence_after_teardown(
    service_name: str,
    replica_id: int,
    replica_record_id: str | uuid.UUID,
) -> bool:
    """Record physical absence for a teardown-fenced missing admission.

    Scheduler-admitted East replicas return ``False``. A Kueue-bound replica
    may enter this path only after whole-service teardown has revoked every
    provider writer. The physical read is uncached and spans no SQL lock.
    """
    service_name = _nonempty(service_name, 'service_name')
    replica_id = _positive_int(replica_id, 'replica_id')
    try:
        record_uuid = (replica_record_id if
                       isinstance(replica_record_id, uuid.UUID) else uuid.UUID(
                           str(replica_record_id)))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError('replica_record_id must be a UUID.') from error

    engine = serve_state_schema.get_database_engine()
    repository = kueue_lane_lineage.KueueAdmissionRepository(engine)
    with engine.connect() as connection:
        decision = (
            repository.
            load_admissionless_physical_absence_probe_target_in_connection(
                connection,
                service_name=service_name,
                replica_id=replica_id,
                replica_record_id=record_uuid))
    decision.validate()
    if decision.state is (
            kueue_lane_lineage.PhysicalAbsenceLoadState.NOT_APPLICABLE):
        return False
    if decision.state is (
            kueue_lane_lineage.PhysicalAbsenceLoadState.ALREADY_PROVEN):
        return True
    target = decision.target
    assert target is not None

    cleanup_fence = reserved_capacity.ProtocolV2CleanupFence(
        kubernetes_context=target.identity.kubernetes_context,
        physical_cluster_uid=target.identity.physical_cluster_uid)
    try:
        with provider_phase.provider_phase(
                provider_phase.ProviderPhaseMode.V2_FENCED):
            with kubernetes_adaptor.physical_cluster_uid_fence(
                    cleanup_fence.kubernetes_context,
                    cleanup_fence.physical_cluster_uid):
                with engine.connect() as connection:
                    provider_read_started_at = connection.execute(
                        sqlalchemy.select(
                            sqlalchemy.func.clock_timestamp())).scalar_one()
                presence = reserved_capacity.probe_physical_replica_presence(
                    cleanup_fence, target.cluster_name, use_cache=False)
                if presence is not (
                        reserved_capacity.PhysicalReplicaPresence.ABSENT):
                    raise kueue_lane_lineage.KueueAdmissionConflict(
                        'Admissionless physical replica is '
                        f'{presence.value}; provider absence is unproven.')
    except kueue_lane_lineage.KueueAdmissionConflict:
        raise
    except Exception as error:  # pylint: disable=broad-except
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Admissionless physical absence is UNKNOWN because its provider '
            'identity could not be proved.') from error

    with engine.begin() as connection:
        (repository.
         record_admissionless_physical_absence_after_teardown_in_connection)(
             connection,
             target,
             provider_read_started_at=provider_read_started_at)
    return True


@dataclasses.dataclass(frozen=True)
class _ObservationAuthority:
    """Frozen cross-layer authority for one admitted Kubernetes Pod."""

    service_name: str
    service_hash: str
    service_lifecycle_epoch: int
    service_version: int
    intent_key: str
    replica_id: int
    replica_record_id: uuid.UUID
    association_id: uuid.UUID
    provider_cluster_generation: int
    fence: reserved_capacity.ProtocolV2LaunchFence


@dataclasses.dataclass(frozen=True)
class _DurableKueuePodAdmissionObserver:
    """Serializable two-step observer with no lock spanning provider I/O."""

    authority: _ObservationAuthority

    def begin_observation(self) -> datetime.datetime:
        engine = serve_state_schema.get_database_engine()
        with engine.connect() as connection:
            sampled_at = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
        if (not isinstance(sampled_at, datetime.datetime) or
                sampled_at.tzinfo is None):
            raise kueue_lane_lineage.KueueAdmissionConflict(
                'Kueue observation could not sample an aware PostgreSQL '
                'clock token.')
        return sampled_at

    def __call__(
        self,
        observation: provision_common.KueuePodAdmissionObservation,
        provider_read_started_at: datetime.datetime,
    ) -> None:
        _observe(self.authority, observation, provider_read_started_at)


def _positive_int(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f'{name} must be a positive integer.')
    return value


def _nonempty(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f'{name} must be a non-empty string.')
    return value


def _lane_identity_from_row(
    row: kueue_lane_lineage.KueueAdmissionRow,
) -> kueue_lane_lineage.KueueAdmissionIdentity:
    identity = kueue_lane_lineage.KueueAdmissionIdentity(
        service_name=row.service_name,
        service_hash=row.service_hash,
        service_lifecycle_epoch=row.service_lifecycle_epoch,
        service_version=row.service_version,
        pool_key=row.pool_key,
        pool_epoch=row.pool_epoch,
        physical_cluster_uid=row.physical_cluster_uid,
        kubernetes_context=row.kubernetes_context,
        accelerator=row.accelerator,
        accelerator_count=row.accelerator_count,
        worker_projection_sha256=row.worker_projection_sha256)
    identity.validate()
    return identity


def _lock_and_validate_materialization(
    connection: sqlalchemy.engine.Connection,
    authority: _ObservationAuthority,
) -> tuple[kueue_lane_lineage.KueueAdmissionRepository, kueue_lane_lineage.
           KueueAdmissionIdentity, kueue_lane_lineage.KueueAdmissionRow]:
    """Lock protocol -> lifecycle/service -> intent/replica -> lineage."""
    serve_state.lock_zero_cost_protocol_for_bound_launch_projection(connection)
    lifecycle = connection.execute(
        sqlalchemy.select(_LIFECYCLES.c.epoch).where(
            _LIFECYCLES.c.name ==
            authority.service_name).with_for_update()).scalar_one_or_none()
    if lifecycle != authority.service_lifecycle_epoch:
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Kueue observation crossed the service lifecycle fence.')
    service = connection.execute(
        sqlalchemy.select(_SERVICES.c.hash, _SERVICES.c.lifecycle_epoch).where(
            _SERVICES.c.name ==
            authority.service_name).with_for_update()).one_or_none()
    if (service is None or service.hash != authority.service_hash or
            service.lifecycle_epoch != authority.service_lifecycle_epoch):
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Kueue observation lost its exact service incarnation.')
    intent = connection.execute(
        sqlalchemy.select(_INTENTS.c.intent_idempotency_key).where(
            _INTENTS.c.service_name == authority.service_name,
            _INTENTS.c.intent_idempotency_key ==
            authority.intent_key).with_for_update()).scalar_one_or_none()
    if intent != authority.intent_key:
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Kueue observation lost its exact durable intent.')
    replica = connection.execute(
        sqlalchemy.select(_REPLICAS.c.replica_id).where(
            _REPLICAS.c.service_name == authority.service_name,
            _REPLICAS.c.replica_id == authority.replica_id,
            _REPLICAS.c.reserved_fill_intent_idempotency_key ==
            authority.intent_key,
            _REPLICAS.c.replica_state['replica_record_id'].as_string() == str(
                authority.replica_record_id)).with_for_update()
    ).scalar_one_or_none()
    if replica != authority.replica_id:
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Kueue observation lost its exact replica record.')
    association = connection.execute(
        sqlalchemy.select(_ASSOCIATIONS.c.association_id).where(
            _ASSOCIATIONS.c.association_id == authority.association_id,
            _ASSOCIATIONS.c.service_name == authority.service_name,
            _ASSOCIATIONS.c.service_hash == authority.service_hash,
            _ASSOCIATIONS.c.service_lifecycle_epoch ==
            authority.service_lifecycle_epoch,
            _ASSOCIATIONS.c.service_version == authority.service_version,
            _ASSOCIATIONS.c.replica_id == authority.replica_id,
            _ASSOCIATIONS.c.replica_record_id == authority.replica_record_id,
            _ASSOCIATIONS.c.launch_generation ==
            authority.provider_cluster_generation,
            _ASSOCIATIONS.c.authorization_reference ==
            f'reserved-fill:{authority.intent_key}').with_for_update()
    ).scalar_one_or_none()
    if association is None:
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Kueue observation lost its exact launch association.')

    repository = kueue_lane_lineage.KueueAdmissionRepository(connection.engine)
    admission = repository.get_for_intent_in_connection(connection,
                                                        authority.service_name,
                                                        authority.intent_key,
                                                        for_update=True)
    if admission is None:
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Kueue observation lost its durable admission.')
    identity = _lane_identity_from_row(admission)
    fence = authority.fence
    if (identity.service_name != authority.service_name or
            identity.service_hash != authority.service_hash or
            identity.service_lifecycle_epoch
            != authority.service_lifecycle_epoch or
            identity.service_version != authority.service_version or
            identity.physical_cluster_uid != fence.physical_cluster_uid or
            identity.kubernetes_context != fence.kubernetes_context or
            identity.accelerator != fence.accelerator.casefold() or
            identity.accelerator_count != fence.accelerator_count or
            identity.worker_projection_sha256
            != fence.worker_projection_sha256):
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Kueue observation does not match its immutable lane projection.')
    materialized = repository.validate_materialized_in_connection(
        connection,
        identity,
        intent_idempotency_key=authority.intent_key,
        replica_id=authority.replica_id,
        replica_record_id=authority.replica_record_id,
        provider_cluster_generation=authority.provider_cluster_generation,
        association_id=authority.association_id)
    if materialized is None:
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Kueue observation lost its complete materialized graph.')
    return repository, identity, admission


def _persisted_pod_identity_from_admission(
    admission: kueue_lane_lineage.KueueAdmissionRow,
) -> provision_common.KueuePersistedPodIdentity | None:
    """Project the immutable Pod receipt into the provider runtime boundary."""
    pod_values = (admission.pod_namespace, admission.pod_name,
                  admission.pod_uid)
    if admission.state is kueue_lane_lineage.KueueAdmissionState.INTENT_PENDING:
        if any(value is not None for value in pod_values):
            raise kueue_lane_lineage.KueueAdmissionConflict(
                'Intent-pending Kueue admission unexpectedly carries a Pod '
                'identity.')
        return None
    if admission.state not in (
            kueue_lane_lineage.KueueAdmissionState.POD_WAITING,
            kueue_lane_lineage.KueueAdmissionState.POLICY_ADMITTED):
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Kueue admission has an unknown durable state.')
    if any(not isinstance(value, str) or not value for value in pod_values):
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Materialized Kueue admission lost its exact Pod identity.')
    assert isinstance(admission.pod_namespace, str)
    assert isinstance(admission.pod_name, str)
    assert isinstance(admission.pod_uid, str)
    return provision_common.KueuePersistedPodIdentity(
        namespace=admission.pod_namespace,
        pod_name=admission.pod_name,
        pod_uid=admission.pod_uid)


def _observe(authority: _ObservationAuthority,
             observation: provision_common.KueuePodAdmissionObservation,
             provider_read_started_at: datetime.datetime) -> None:
    if not isinstance(observation,
                      provision_common.KueuePodAdmissionObservation):
        raise TypeError('Kueue admission observer requires a typed receipt.')
    if (not isinstance(provider_read_started_at, datetime.datetime) or
            provider_read_started_at.tzinfo is None):
        raise TypeError('Kueue admission observer requires an aware durable '
                        'clock token.')
    receipt = observation.receipt
    worker_projection_sha256 = authority.fence.worker_projection_sha256
    if not isinstance(worker_projection_sha256, str):
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Kueue observation lost its worker projection digest.')
    expected_identity = provision_common.KueuePodAdmissionIdentity(
        intent_key=authority.intent_key,
        replica_record_uuid=str(authority.replica_record_id),
        pool_physical_uid=authority.fence.physical_cluster_uid,
        worker_projection_sha256=worker_projection_sha256)
    if (observation.identity != expected_identity or
            observation.accelerator.casefold()
            != authority.fence.accelerator.casefold() or
            observation.accelerator_count != authority.fence.accelerator_count):
        raise kueue_lane_lineage.KueueAdmissionConflict(
            'Kueue Pod receipt changed its server-owned identity or shape.')

    admitted = (observation.state
                is provision_common.KueuePodAdmissionState.POLICY_ADMITTED)
    engine = serve_state_schema.get_database_engine()
    with engine.begin() as connection:
        repository, identity, _ = _lock_and_validate_materialization(
            connection, authority)
        if admitted:
            repository.observe_policy_admitted_in_connection(
                connection,
                identity,
                intent_idempotency_key=authority.intent_key,
                replica_id=authority.replica_id,
                replica_record_id=authority.replica_record_id,
                provider_cluster_generation=(
                    authority.provider_cluster_generation),
                association_id=authority.association_id,
                provider_read_started_at=provider_read_started_at,
                pod_namespace=observation.namespace,
                pod_name=observation.pod_name,
                pod_uid=observation.pod_uid,
                pod_receipt=receipt.canonical_dict())
        elif (observation.state
              is provision_common.KueuePodAdmissionState.POD_WAITING):
            repository.observe_pod_waiting_in_connection(
                connection,
                identity,
                intent_idempotency_key=authority.intent_key,
                replica_id=authority.replica_id,
                replica_record_id=authority.replica_record_id,
                provider_cluster_generation=(
                    authority.provider_cluster_generation),
                association_id=authority.association_id,
                provider_read_started_at=provider_read_started_at,
                pod_namespace=observation.namespace,
                pod_name=observation.pod_name,
                pod_uid=observation.pod_uid,
                pod_receipt=receipt.canonical_dict())
        else:
            raise kueue_lane_lineage.KueueAdmissionConflict(
                'Kueue Pod receipt has an unsupported state.')


def runtime_for_reserved_fill_launch(
    launch_context: Mapping[str, Any],
    fence: reserved_capacity.ProtocolV2LaunchFence,
) -> provision_common.KueuePodAdmissionRuntime | None:
    """Build runtime-only Kueue observation state from a bound request."""
    if not isinstance(launch_context, Mapping) or not isinstance(
            fence, reserved_capacity.ProtocolV2LaunchFence):
        raise TypeError('Kueue lane runtime requires a bound launch context.')
    if not fence.policy_bound or not isinstance(fence.worker_projection_sha256,
                                                str):
        raise ValueError('Kueue lane runtime requires a sequenced fill fence.')
    bound = ordinary_launch_binding.parse_bound_non_pool_launch_context(
        launch_context)
    if (bound.profile.kind is not ordinary_launch_binding.
            NonPoolLaunchProfileKind.RESERVED_FILL or
            bound.profile.authorization_kind is not ordinary_launch_binding.
            NonPoolLaunchAuthorizationKind.RESERVED_FILL_ALLOCATION):
        raise ValueError('Kueue lane runtime requires reserved-fill authority.')
    reference = bound.profile.authorization_reference
    prefix = 'reserved-fill:'
    if not isinstance(reference, str) or not reference.startswith(prefix):
        raise ValueError('Reserved-fill launch has no exact intent reference.')
    intent_key = reference[len(prefix):]
    if _SHA256_RE.fullmatch(intent_key) is None:
        raise ValueError('Reserved-fill intent reference is malformed.')
    projections = launch_context.get(
        serve_constants.REPLICA_LAUNCH_WORKER_PROJECTIONS_KEY)
    _, admission = reserved_capacity.require_reclaim_worker_projection(
        fence, projections, require_current_protocol=True)
    if admission.admission_mode is (reserved_fill_reclaim_attestation.
                                    ReclaimAdmissionMode.KUBERNETES_SCHEDULER):
        return None
    if admission.admission_mode is not (
            reserved_fill_reclaim_attestation.ReclaimAdmissionMode.KUEUE):
        raise ValueError('Reserved-fill projection has unknown admission mode.')

    service_name = _nonempty(bound.service_name, 'service_name')
    service_hash = _nonempty(
        launch_context.get(
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY),
        'service_hash')
    service_version = _positive_int(
        launch_context.get(
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY),
        'service_version')
    lifecycle_epoch = _positive_int(
        launch_context.get(ordinary_launch_binding.LIFECYCLE_EPOCH_KEY),
        'service_lifecycle_epoch')
    if service_version != fence.service_version:
        raise ValueError('Reserved-fill service version changed at execution.')
    authority = _ObservationAuthority(
        service_name=service_name,
        service_hash=service_hash,
        service_lifecycle_epoch=lifecycle_epoch,
        service_version=service_version,
        intent_key=intent_key,
        replica_id=bound.replica_id,
        replica_record_id=bound.replica_record_id,
        association_id=bound.association_id,
        provider_cluster_generation=bound.launch_generation,
        fence=fence)
    identity = provision_common.KueuePodAdmissionIdentity(
        intent_key=intent_key,
        replica_record_uuid=str(bound.replica_record_id),
        pool_physical_uid=fence.physical_cluster_uid,
        worker_projection_sha256=fence.worker_projection_sha256)
    engine = serve_state_schema.get_database_engine()
    with engine.begin() as connection:
        _, _, durable_admission = _lock_and_validate_materialization(
            connection, authority)
    persisted_pod_identity = _persisted_pod_identity_from_admission(
        durable_admission)

    return provision_common.KueuePodAdmissionRuntime(
        identity=identity,
        accelerator=fence.accelerator.casefold(),
        observer=_DurableKueuePodAdmissionObserver(authority),
        persisted_pod_identity=persisted_pod_identity)
