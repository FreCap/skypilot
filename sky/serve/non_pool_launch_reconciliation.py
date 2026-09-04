"""Failure-isolated provider evidence for bound non-pool launches.

Provider observation is deliberately separate from association reduction.
Reserved-fill profiles retain an immutable physical provider identity. An
ordinary-paid AWS Spot failure may instead carry an exact zero-effect create
receipt on its terminal request. A quiescent ordinary-paid GCP Spot launch is
observed against its immutable project, zone, instance, and disk identity.
Other profiles and incomplete evidence remain ``UNKNOWN``; a missing SkyPilot
cluster record is never promoted into provider absence.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
import dataclasses
import enum
import json
import math
import subprocess
import sys
import threading
import time
import typing
from typing import Any, cast

from sky import exceptions
from sky.adaptors import common as adaptors_common
from sky.provision import capacity_policy
from sky.provision import constants as provision_constants
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
from sky.serve import reserved_capacity
from sky.utils import common_utils
from sky.utils import subprocess_utils
from sky.utils import thread_utils

if typing.TYPE_CHECKING:
    from sky.serve import resource_actions as resource_actions_types
    from sky.server.requests import postgres as request_postgres_types

request_postgres = adaptors_common.LazyImport('sky.server.requests.postgres')
api_requests = adaptors_common.LazyImport('sky.server.requests.requests')
provision = adaptors_common.LazyImport('sky.provision')
gcp_provision = adaptors_common.LazyImport('sky.provision.gcp')
gcp_cloud = adaptors_common.LazyImport('sky.clouds.gcp')
aws_adaptor = adaptors_common.LazyImport('sky.adaptors.aws')
resource_actions = adaptors_common.LazyImport('sky.serve.resource_actions')

_AWS_EMPTY_CENSUS_INTERVAL_SECONDS = 2.0
_GCP_EMPTY_CENSUS_INTERVAL_SECONDS = 2.0
_PROVIDER_CENSUS_WORKER_ARGUMENT = '__paid-provider-census-worker__'
_PROVIDER_CENSUS_PROTOCOL_VERSION = 1
_PROVIDER_CENSUS_DEFAULT_TIMEOUT_SECONDS = 30.0
_PROVIDER_CENSUS_WORKER_TERM_GRACE_SECONDS = 1.0
_PROVIDER_CENSUS_WORKER_REAP_GRACE_SECONDS = 5.0
_PROVIDER_CENSUS_MAX_PROTOCOL_BYTES = 1024 * 1024


@dataclasses.dataclass(frozen=True)
class ProviderObservation:
    """One closed provider classification and its canonical evidence."""

    evidence: ordinary_launch_binding.ProviderEvidence
    payload: dict[str, Any]


class PaidTeardownObservationDisposition(str, enum.Enum):
    """One canonical next action after a paid teardown observation."""

    SETTLED_ABSENT = 'SETTLED_ABSENT'
    RESUBMIT_PRESENT = 'RESUBMIT_PRESENT'
    RETRY_UNKNOWN = 'RETRY_UNKNOWN'


@dataclasses.dataclass(frozen=True)
class PaidTeardownObservationStep:
    """Result of one observation and its database transition."""

    disposition: PaidTeardownObservationDisposition
    observation: ProviderObservation
    scheduled_replica_info: Any | None = None


@dataclasses.dataclass(frozen=True)
class OneShotProviderObservationCompletion:
    """Finished work returned by the bounded one-shot observer lane."""

    key: tuple[int, str]
    result: Any | None
    error: BaseException | None
    formatted_error: str | None


class OneShotProviderObservationLane:
    """Bounded lazy one-shot lane shared by live and service teardown."""

    MAX_CONCURRENT = 16

    def __init__(self) -> None:
        self._workers: dict[tuple[int, str], thread_utils.SafeThread] = {}
        self._results: thread_utils.ThreadSafeDict[tuple[int, str], Any] = (
            thread_utils.ThreadSafeDict())
        self._state_lock = threading.RLock()
        self._process_registry = subprocess_utils.ProcessGroupRegistry()
        self._closed = False

    def schedule(self, key: tuple[int, str], operation: Callable[[],
                                                                 Any]) -> bool:
        """Start one operation when its exact key and a lane slot are free."""

        def _run() -> None:
            with self._process_registry.activate():
                self._results[key] = operation()

        worker = thread_utils.SafeThread(
            target=_run,
            name=f'replica-{key[0]}-teardown-observation',
            daemon=True)
        with self._state_lock:
            if self._closed:
                raise RuntimeError('Provider observation lane is closed.')
            if (key in self._workers or
                    len(self._workers) >= self.MAX_CONCURRENT):
                return False
            self._workers[key] = worker
            try:
                # Keep admission and start atomic with close().  A real worker
                # may block briefly while installing its process registration;
                # it proceeds as soon as this lock is released.
                worker.start()
            except BaseException:
                del self._workers[key]
                # A custom/instrumented Thread.start() can fail after invoking
                # the target. Never let that ambiguous start leave a result
                # which a later retry with the same exact key could consume.
                self._results.pop(key)
                raise
        return True

    def take_completed(
        self,
        key: tuple[int, str] | None = None,
    ) -> tuple[OneShotProviderObservationCompletion, ...]:
        """Join and remove completed one-shot work without blocking on live work."""
        completed = []
        with self._state_lock:
            workers = list(self._workers.items())
        for worker_key, worker in workers:
            if ((key is not None and worker_key != key) or worker.is_alive()):
                continue
            worker.join()
            with self._state_lock:
                if self._workers.get(worker_key) is not worker:
                    continue
                del self._workers[worker_key]
            result = self._results.pop(worker_key)
            completed.append(
                OneShotProviderObservationCompletion(
                    key=worker_key,
                    result=result,
                    error=worker.exception,
                    formatted_error=worker.format_exc))
        return tuple(completed)

    @property
    def available_slots(self) -> int:
        with self._state_lock:
            return self.MAX_CONCURRENT - len(self._workers)

    def contains(self, key: tuple[int, str]) -> bool:
        with self._state_lock:
            return key in self._workers

    def has_work(self) -> bool:
        with self._state_lock:
            return bool(self._workers)

    @property
    def mutation_is_allowed(self) -> bool:
        """Whether an admitted worker may still enter a mutation boundary."""
        with self._state_lock:
            return not self._closed

    def close(self) -> None:
        """Stop admission and quiesce all registered provider child groups.

        Python threads cannot be forcibly terminated. They are given one
        bounded join horizon, while callers fence their database mutations via
        ``mutation_is_allowed`` plus durable lifecycle authority. A surviving
        thread is reported explicitly; it is never described as quiescent.
        """
        with self._state_lock:
            self._closed = True
            workers = tuple(self._workers.values())
        self._process_registry.close(
            term_grace_seconds=_PROVIDER_CENSUS_WORKER_TERM_GRACE_SECONDS,
            reap_grace_seconds=_PROVIDER_CENSUS_WORKER_REAP_GRACE_SECONDS)
        join_deadline = (time.monotonic() +
                         _PROVIDER_CENSUS_WORKER_REAP_GRACE_SECONDS)
        for worker in workers:
            worker.join(timeout=max(0, join_deadline - time.monotonic()))
        self.take_completed()
        with self._state_lock:
            if self._process_registry.process_count:
                raise RuntimeError(
                    'Provider observation child groups did not drain at '
                    'shutdown.')
            if self._workers:
                raise RuntimeError(
                    'A Python provider observation worker survived lane '
                    'shutdown; child groups are quiescent and its mutation '
                    'gate is closed.')


def _provider_census_worker_command() -> list[str]:
    """Return the one private child entry point for paid provider reads."""
    return [
        sys.executable, '-m', 'sky.serve.non_pool_launch_reconciliation',
        _PROVIDER_CENSUS_WORKER_ARGUMENT
    ]


def _provider_census_worker_env(cloud: str) -> dict[str, str]:
    """Retain cloud credentials but remove unrelated control-plane secrets."""
    return subprocess_utils.provider_process_env(cloud)


def _run_paid_provider_census_worker(
    request: Mapping[str, Any],
    *,
    deadline_monotonic: float,
) -> Any:
    """Run one exact read in a deadline-owned, killable process group."""
    if (isinstance(deadline_monotonic, bool) or
            not isinstance(deadline_monotonic, (int, float)) or
            not math.isfinite(deadline_monotonic)):
        raise ValueError('Paid provider census deadline is malformed.')
    try:
        request_text = json.dumps(dict(request),
                                  sort_keys=True,
                                  separators=(',', ':'),
                                  allow_nan=False)
    except (TypeError, ValueError) as error:
        raise ValueError(
            'Paid provider census request is malformed.') from error
    if len(request_text.encode('utf-8')) > _PROVIDER_CENSUS_MAX_PROTOCOL_BYTES:
        raise ValueError('Paid provider census request is too large.')
    remaining = deadline_monotonic - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('Timed out waiting for paid provider census.')
    cloud = request.get('cloud')
    if cloud not in ('aws', 'gcp'):
        raise ValueError('Paid provider census cloud is malformed.')
    try:
        result = subprocess_utils.run_in_process_group(
            _provider_census_worker_command(),
            deadline_monotonic=deadline_monotonic,
            term_grace_seconds=_PROVIDER_CENSUS_WORKER_TERM_GRACE_SECONDS,
            reap_grace_seconds=_PROVIDER_CENSUS_WORKER_REAP_GRACE_SECONDS,
            input_text=request_text,
            env=_provider_census_worker_env(cloud),
            stderr=subprocess.DEVNULL)
    except TimeoutError as error:
        raise TimeoutError(
            'Timed out waiting for paid provider census.') from error
    if result.returncode != 0:
        raise RuntimeError(
            f'Paid provider census worker failed (exit={result.returncode}).')
    stdout = result.stdout
    if stdout is None:
        raise RuntimeError('Paid provider census worker returned no output.')
    if len(stdout.encode('utf-8')) > _PROVIDER_CENSUS_MAX_PROTOCOL_BYTES:
        raise RuntimeError('Paid provider census response is too large.')
    try:
        response = json.loads(stdout)
    except (TypeError, json.JSONDecodeError) as error:
        raise RuntimeError(
            'Paid provider census worker returned malformed output.') from error
    if (not isinstance(response, dict) or set(response) != {'ok', 'result'} or
            response['ok'] is not True):
        raise RuntimeError('Paid provider census worker failed closed.')
    return response['result']


@dataclasses.dataclass(frozen=True, kw_only=True)
class _AwsProviderCensusWorkerScope:
    provider_identity: Mapping[str, Any]
    credential_profile: str | None


@dataclasses.dataclass(frozen=True, kw_only=True)
class _GcpProviderCensusWorkerReplica:
    cluster_name: str


def _provider_census_worker_main() -> int:
    """Execute the private JSON protocol without exposing SDK diagnostics."""
    protocol_stdout = sys.stdout
    try:
        request_bytes = sys.stdin.buffer.read(
            _PROVIDER_CENSUS_MAX_PROTOCOL_BYTES + 1)
        if len(request_bytes) > _PROVIDER_CENSUS_MAX_PROTOCOL_BYTES:
            raise ValueError('Paid provider census request is too large.')
        request = json.loads(request_bytes)
        if (not isinstance(request, dict) or request.get('protocol_version')
                != _PROVIDER_CENSUS_PROTOCOL_VERSION or
                request.get('cloud') not in ('aws', 'gcp') or
                not isinstance(request.get('provider_identity'), dict)):
            raise ValueError('Paid provider census request is malformed.')
        # Provider clients occasionally write diagnostics to stdout. Keep the
        # parent's result channel singular and send all such output to stderr,
        # which the parent deliberately discards.
        sys.stdout = sys.stderr
        result: Any
        if request['cloud'] == 'aws':
            credential_profile = request.get('credential_profile')
            if (credential_profile is not None and
                (not isinstance(credential_profile, str) or
                 not credential_profile)):
                raise ValueError('AWS census credential profile is malformed.')
            result = _query_aws_paid_provider_census(
                _AwsProviderCensusWorkerScope(
                    provider_identity=request['provider_identity'],
                    credential_profile=credential_profile))
        else:
            cluster_name = request.get('cluster_name')
            if not isinstance(cluster_name, str) or not cluster_name:
                raise ValueError('GCP census cluster name is malformed.')
            instances, disks, operations = _query_gcp_paid_provider_census(
                _GcpProviderCensusWorkerReplica(cluster_name=cluster_name),
                request['provider_identity'])
            result = {
                'disks': disks,
                'instances': instances,
                'operations': operations,
            }
        response = {'ok': True, 'result': result}
    except BaseException as error:  # pylint: disable=broad-except
        response = {
            'ok': False,
            'result': {
                'error_type': type(error).__name__,
            },
        }
    finally:
        sys.stdout = protocol_stdout
    response_text = json.dumps(response,
                               sort_keys=True,
                               separators=(',', ':'),
                               allow_nan=False)
    if len(response_text.encode('utf-8')) > _PROVIDER_CENSUS_MAX_PROTOCOL_BYTES:
        response_text = json.dumps(
            {
                'ok': False,
                'result': {
                    'error_type': 'ResponseTooLarge',
                },
            },
            separators=(',', ':'))
    protocol_stdout.write(response_text)
    protocol_stdout.flush()
    return 0


@dataclasses.dataclass(frozen=True)
class ProviderAbsenceReplicaProjection:
    """Validated replica and paid-capacity result for exact absence."""

    paid_capacity_pool_key: str | None
    paid_capacity_outcome: paid_capacity.LaunchOutcome | None


def decoded_request_error(error: Any) -> BaseException | None:
    """Extract the exception from the exact durable request error shape."""
    if isinstance(error, BaseException):
        return error
    if not api_requests.decoded_error_is_valid(error):
        return None
    error_object = error['object']
    assert isinstance(error_object, BaseException)
    return error_object


def apply_immediate_provider_cleanup_replica_marker(info: Any) -> None:
    """Write the one closed marker used by exact immediate cleanup.

    The caller owns provider and database authority. This helper only
    normalizes the locked replica copy and performs no provider or database
    I/O.
    """
    status = info.status_property
    status.sky_launch_status = common_utils.ProcessStatus.INTERRUPTED
    if status.sky_down_status != common_utils.ProcessStatus.RUNNING:
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
    status.service_ready_now = False
    status.is_scale_down = True
    status.preempted = False
    status.purged = False
    status.failed_spot_availability = False
    status.drain_cap_seconds = 0
    status.drain_started_at = None
    status.wait_for_idle_before_termination = False
    status.logical_retirement_version = None
    status.logical_retirement_controller_epoch = None
    status.logical_retirement_generation = None
    status.logical_retirement_target_capacity = None
    status.logical_retirement_confirmed_generation = None
    status.logical_retirement_bounded_deadline = False
    status.logical_retirement_committed = False


def apply_exact_provider_absence_replica_projection(
        projection: Any) -> ProviderAbsenceReplicaProjection | None:
    """Validate exact ABSENT evidence and update its locked replica copy.

    This is the single replica-side reducer for provider absence.  The caller
    remains responsible for committing the replica, association, retention
    pin, and paid claim in one PostgreSQL transaction.  This function performs
    no provider or database I/O.
    """
    if (getattr(projection, 'provider_evidence', None)
            != ordinary_launch_binding.ProviderEvidence.ABSENT):
        return None
    context = getattr(projection, 'context', None)
    if (not isinstance(context,
                       ordinary_launch_binding.BoundNonPoolLaunchContext) or
            projection.pre_effect_terminal or
            projection.service_job_id is not None):
        return None

    info = projection.locked_replica_info
    pool_key = projection.paid_capacity_pool_key
    paid_outcome = None
    explicit_paid_cancel = False
    reserved_absence = False
    paid_teardown_observation_pending = False
    if (context.profile.kind ==
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL):
        if pool_key is not None:
            return None
        reserved_absence = True
    elif ordinary_launch_binding.is_paid_provider_reconciliation_profile(
            context.profile.kind):
        paid_teardown_observation_pending = bool(
            ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
                info) and
            ordinary_launch_binding.provider_present_teardown_phase(info)
            is ordinary_launch_binding.ProviderPresentTeardownPhase.
            ABSENCE_OBSERVATION_PENDING)
        request = getattr(projection, 'request', None)
        decoded_error = decoded_request_error(getattr(request, 'error', None))
        evidence_payload = getattr(projection, 'provider_evidence_payload',
                                   None)
        probe_contract = (evidence_payload.get('probe_contract') if isinstance(
            evidence_payload, Mapping) else None)
        status = getattr(getattr(projection, 'status', None), 'value', None)
        cause = getattr(getattr(projection, 'cause', None), 'value', None)
        explicit_paid_cancel = (status == 'CANCELLED' and
                                cause == 'explicit_cancel')
        replica_shape_matches = bool(
            isinstance(pool_key, str) and bool(pool_key) and
            info.paid_capacity_pool_key == pool_key and info.is_spot is True and
            info.is_zero_cost is False and info.reserved_fill is False and
            info.service_job_id is None)
        handler_failed = status == 'FAILED' and cause == 'handler_failed'
        if probe_contract == 'aws-client-token-instance-presence-v1':
            assert isinstance(evidence_payload, Mapping)
            pool_identity = (paid_capacity.pool_key_payload(pool_key)
                             if isinstance(pool_key, str) else None)
            instances = evidence_payload.get('instances')
            if (not replica_shape_matches or not ordinary_launch_binding.
                    ordinary_paid_provider_terminal_shape_matches(
                        status, cause, pool_key) or
                    not isinstance(pool_identity, Mapping) or
                    pool_identity.get('cloud') != 'aws' or
                    evidence_payload.get('result') != 'ABSENT' or
                    not isinstance(instances, list) or
                    any(not isinstance(instance, Mapping) or
                        instance.get('state') != 'terminated'
                        for instance in instances)):
                return None
            # A client-token census with no live instance proves cleanup, not
            # a typed Spot shortage. Do not poison this pool's capacity model.
            paid_outcome = paid_capacity.LaunchOutcome.OTHER_FAILURE
            info.status_property.failed_spot_availability = False
        elif probe_contract == 'gcp-vm-disk-operation-presence-v1':
            assert isinstance(evidence_payload, Mapping)
            pool_identity = (paid_capacity.pool_key_payload(pool_key)
                             if isinstance(pool_key, str) else None)
            # A GCP replacement census is exact for both the cleanup-only v1
            # pool and the project-scoped v2 pool; share the one pool-shape
            # contract with the association-side reducer instead of pinning
            # this replica-side copy to the legacy key version.
            replacement_shape_matches = bool(
                context.profile.kind is ordinary_launch_binding.
                NonPoolLaunchProfileKind.ORDINARY_PAID or
                (isinstance(pool_identity, Mapping) and
                 pool_identity.get('cloud') == 'gcp' and ordinary_launch_binding
                 .paid_provider_reconciliation_pool_shape_matches(
                     context.profile.kind, pool_key)))
            if (not replacement_shape_matches or not replica_shape_matches or
                    not ordinary_launch_binding.
                    ordinary_paid_provider_terminal_shape_matches(
                        status, cause, pool_key) or
                    evidence_payload.get('result') != 'ABSENT' or
                    evidence_payload.get('instance_ids') != [] or
                    evidence_payload.get('disk_ids') != [] or not isinstance(
                        evidence_payload.get('create_operation_targets'),
                        Mapping) or
                    evidence_payload['create_operation_targets'].get('inflight')
                    != []):
                return None
            if handler_failed:
                reason = None
                if isinstance(decoded_error,
                              exceptions.ResourcesUnavailableError):
                    reason = (
                        capacity_policy.classify_resources_unavailable_error(
                            gcp_cloud.GCP(), decoded_error))
                if reason == 'quota':
                    paid_outcome = paid_capacity.LaunchOutcome.QUOTA_FAILURE
                elif reason == 'capacity':
                    paid_outcome = paid_capacity.LaunchOutcome.CAPACITY_FAILURE
                else:
                    paid_outcome = paid_capacity.LaunchOutcome.OTHER_FAILURE
                info.status_property.failed_spot_availability = True
            else:
                # Explicit teardown cancellation is not negative GCP capacity
                # feedback. The exact provider census still owns cleanup and
                # OTHER_FAILURE leaves the adaptive pool unchanged.
                paid_outcome = paid_capacity.LaunchOutcome.OTHER_FAILURE
                info.status_property.failed_spot_availability = False
        elif (context.profile.kind ==
              ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID):
            expected_receipt = (evidence_payload.get('receipt') if isinstance(
                evidence_payload, Mapping) else None)
            expected_cloud_name = (expected_receipt.get('cluster_name_on_cloud')
                                   if isinstance(expected_receipt, Mapping) else
                                   None)
            try:
                expected_client_token = (
                    ordinary_launch_binding.ordinary_paid_aws_client_token(
                        context))
                expected_aws_account_id = (
                    ordinary_launch_binding.
                    ordinary_paid_aws_account_id_from_pool_key(pool_key))
            except (TypeError, ValueError,
                    ordinary_launch_binding.OrdinaryLaunchBindingConflict):
                return None
            provider_negative_ack = (
                capacity_policy.extract_provider_negative_ack(decoded_error)
                if decoded_error is not None else None)
            provider_negative_ack = (
                capacity_policy.validate_provider_negative_ack(
                    provider_negative_ack,
                    cluster_name=expected_cloud_name,
                    client_token=expected_client_token,
                    expected_aws_account_id=expected_aws_account_id)
                if isinstance(expected_cloud_name, str) and expected_cloud_name
                else None)
            if (provider_negative_ack is None or
                    provider_negative_ack != expected_receipt or
                    not replica_shape_matches or not handler_failed):
                return None
            reason = provider_negative_ack['reason']
            if reason == 'quota':
                paid_outcome = paid_capacity.LaunchOutcome.QUOTA_FAILURE
            elif reason == 'capacity':
                paid_outcome = paid_capacity.LaunchOutcome.CAPACITY_FAILURE
            else:
                return None
            info.status_property.failed_spot_availability = True
        else:
            return None
    else:
        return None

    if reserved_absence:
        # Exact post-quiescence ABSENT evidence makes provider cleanup a
        # database-only retirement. Normalize every current writer to the
        # same closed marker used by the restart-safe finalizer.
        apply_immediate_provider_cleanup_replica_marker(info)
    elif explicit_paid_cancel:
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.INTERRUPTED)
    elif (info.status_property.sky_launch_status
          != common_utils.ProcessStatus.INTERRUPTED):
        info.status_property.sky_launch_status = common_utils.ProcessStatus.FAILED
    if paid_teardown_observation_pending:
        ordinary_launch_binding.transition_provider_present_teardown_phase(
            info,
            expected=(ordinary_launch_binding.ProviderPresentTeardownPhase.
                      ABSENCE_OBSERVATION_PENDING),
            target=(ordinary_launch_binding.ProviderPresentTeardownPhase.
                    CLEANUP_SUCCEEDED))
    return ProviderAbsenceReplicaProjection(paid_capacity_pool_key=pool_key,
                                            paid_capacity_outcome=paid_outcome)


def _reserved_fill_observation_payload(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    fence: reserved_capacity.ProtocolV2CleanupFence,
) -> dict[str, Any]:
    """Build the canonical exact reserved-fill provider evidence payload."""
    return {
        'association_id': str(context.association_id),
        'cluster_name': getattr(replica_info, 'cluster_name', None),
        'kubernetes_context': fence.kubernetes_context,
        'physical_cluster_uid': fence.physical_cluster_uid,
        'probe_contract': 'kubernetes-physical-replica-presence-v1',
        'profile_kind': context.profile.kind.value,
        'replica_record_id': str(context.replica_record_id),
    }


def _gcp_observation_payload(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    provider_identity: Mapping[str, Any],
    instance_ids: list[str],
    disk_ids: list[str],
    create_operation_targets: Mapping[str, list[str]],
    evidence: ordinary_launch_binding.ProviderEvidence,
) -> dict[str, Any]:
    """Build one closed exact-resource GCP observation envelope."""
    return {
        'association_id': str(context.association_id),
        'cluster_name': getattr(replica_info, 'cluster_name', None),
        'create_operation_targets': dict(create_operation_targets),
        'disk_ids': disk_ids,
        'instance_ids': instance_ids,
        'probe_contract': 'gcp-vm-disk-operation-presence-v1',
        'profile_kind': context.profile.kind.value,
        'provider_identity': dict(provider_identity),
        'replica_record_id': str(context.replica_record_id),
        'result': evidence.value,
    }


def _query_aws_paid_provider_census(
    scope: request_postgres_types.BoundAwsProviderCensusScope |
    _AwsProviderCensusWorkerScope,
) -> list[dict[str, str]]:
    """Perform one uncached, account-checked EC2 client-token census."""
    provider_identity = scope.provider_identity
    session = aws_adaptor.session(profile=scope.credential_profile)
    region = provider_identity['region']
    caller = session.client('sts', region_name=region).get_caller_identity()
    if caller.get('Account') != provider_identity['aws_account_id']:
        raise ValueError('AWS credential profile resolved to another account.')
    client = session.client('ec2', region_name=region)
    pages = client.get_paginator('describe_instances').paginate(Filters=[{
        'Name': 'client-token',
        'Values': [provider_identity['client_token']],
    }])
    instances: list[dict[str, str]] = []
    for page in pages:
        reservations = page.get('Reservations')
        if not isinstance(reservations, list):
            raise ValueError('DescribeInstances returned no reservations.')
        for reservation in reservations:
            if not isinstance(reservation, Mapping):
                raise ValueError(
                    'DescribeInstances returned a bad reservation.')
            values = reservation.get('Instances')
            if not isinstance(values, list):
                raise ValueError('DescribeInstances returned no instances.')
            for instance in values:
                if not isinstance(instance, Mapping):
                    raise ValueError(
                        'DescribeInstances returned a bad instance.')
                tags_list = instance.get('Tags', [])
                if not isinstance(tags_list, list):
                    raise ValueError('DescribeInstances returned invalid tags.')
                tags: dict[str, str] = {}
                for tag in tags_list:
                    if (not isinstance(tag, Mapping) or
                            not isinstance(tag.get('Key'), str) or
                            not isinstance(tag.get('Value'), str) or
                            tag['Key'] in tags):
                        raise ValueError(
                            'DescribeInstances returned non-canonical tags.')
                    tags[tag['Key']] = tag['Value']
                placement = instance.get('Placement')
                state = instance.get('State')
                lifecycle = instance.get('InstanceLifecycle')
                block_devices = instance.get('BlockDeviceMappings')
                if not isinstance(block_devices, list):
                    raise ValueError(
                        'DescribeInstances returned no block-device census.')
                for block_device in block_devices:
                    if not isinstance(block_device, Mapping):
                        raise ValueError(
                            'DescribeInstances returned a bad block device.')
                    ebs = block_device.get('Ebs')
                    if (ebs is not None and
                        (not isinstance(ebs, Mapping) or
                         ebs.get('DeleteOnTermination') is not True)):
                        raise ValueError(
                            'Exact AWS instance retains a non-ephemeral EBS '
                            'volume; native cleanup is unsafe.')
                raw_canonical = {
                    'availability_zone': placement.get('AvailabilityZone')
                                         if isinstance(placement, Mapping) else
                                         None,
                    'client_token': instance.get('ClientToken'),
                    'cluster_name_on_cloud': tags.get(
                        provision_constants.TAG_RAY_CLUSTER_NAME),
                    'instance_id': instance.get('InstanceId'),
                    'instance_type': instance.get('InstanceType'),
                    'market': ('spot' if lifecycle == 'spot' else
                               'on_demand' if lifecycle is None else lifecycle),
                    'state': state.get('Name')
                             if isinstance(state, Mapping) else None,
                }
                if any(not isinstance(value, str) or not value
                       for value in raw_canonical.values()):
                    raise ValueError(
                        'DescribeInstances returned incomplete identity.')
                canonical = cast(dict[str, str], raw_canonical)
                instances.append(canonical)
    instances = sorted(instances, key=lambda instance: instance['instance_id'])
    if (len(instances) > provider_identity['num_nodes'] or len(
        {instance['instance_id'] for instance in instances}) != len(instances)):
        raise ValueError(
            'EC2 client-token census exceeded its immutable allocation.')
    allowed_states = {
        'pending', 'running', 'shutting-down', 'terminated', 'stopping',
        'stopped'
    }
    for instance in instances:
        if (instance['availability_zone'] != provider_identity['zone'] or
                instance['client_token'] != provider_identity['client_token'] or
                instance['cluster_name_on_cloud']
                != provider_identity['cluster_name_on_cloud'] or
                instance['instance_type'] != provider_identity['instance_type']
                or instance['market'] != 'spot' or
                instance['state'] not in allowed_states):
            raise ValueError(
                'EC2 client-token census escaped its immutable allocation.')
    return instances


def _query_aws_paid_provider_census_isolated(
    scope: request_postgres_types.BoundAwsProviderCensusScope,
    *,
    deadline_monotonic: float,
) -> list[dict[str, str]]:
    """Query exact AWS identity behind the killable provider boundary."""
    result = _run_paid_provider_census_worker(
        {
            'cloud': 'aws',
            'credential_profile': scope.credential_profile,
            'protocol_version': _PROVIDER_CENSUS_PROTOCOL_VERSION,
            'provider_identity': dict(scope.provider_identity),
        },
        deadline_monotonic=deadline_monotonic)
    if not isinstance(result, list) or any(
            not isinstance(instance, dict) for instance in result):
        raise RuntimeError('AWS provider census result is malformed.')
    return cast(list[dict[str, str]], result)


def _aws_observation_payload(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    provider_identity: Mapping[str, Any],
    instances: list[dict[str, str]],
    evidence: ordinary_launch_binding.ProviderEvidence,
) -> dict[str, Any]:
    """Build one closed exact-resource AWS observation envelope."""
    return {
        'association_id': str(context.association_id),
        'cluster_name': getattr(replica_info, 'cluster_name', None),
        'instances': instances,
        'probe_contract': 'aws-client-token-instance-presence-v1',
        'profile_kind': context.profile.kind.value,
        'provider_identity': dict(provider_identity),
        'replica_record_id': str(context.replica_record_id),
        'result': evidence.value,
    }


def _sleep_provider_quiet_interval(seconds: float,
                                   deadline_monotonic: float) -> None:
    """Sleep only when the same observation deadline covers the interval."""
    remaining = deadline_monotonic - time.monotonic()
    if remaining < seconds:
        raise TimeoutError('Timed out waiting for paid provider census.')
    time.sleep(seconds)


def _observe_aws_paid_provider(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    *,
    deadline_monotonic: float,
) -> ProviderObservation:
    """Query frozen AWS account, placement, and EC2 client-token identity."""
    scope = request_postgres.bound_non_pool_aws_provider_census_scope(
        context, authority)
    base = {
        'association_id': str(context.association_id),
        'cluster_name': getattr(replica_info, 'cluster_name', None),
        'probe_contract': 'aws-client-token-instance-presence-v1',
        'profile_kind': context.profile.kind.value,
        'replica_record_id': str(context.replica_record_id),
    }
    if scope is None:
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'reason': 'missing-immutable-aws-provider-access',
            })
    identity = scope.provider_identity
    try:
        instances = _query_aws_paid_provider_census_isolated(
            scope, deadline_monotonic=deadline_monotonic)
    except Exception as error:  # pylint: disable=broad-except
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'error_type': type(error).__name__,
                'provider_identity': identity,
                'reason': 'aws-provider-read-failed',
            })
    live = [
        instance for instance in instances if instance['state'] != 'terminated'
    ]
    if live:
        evidence = ordinary_launch_binding.ProviderEvidence.PRESENT
        return ProviderObservation(
            evidence,
            _aws_observation_payload(context, replica_info, identity, instances,
                                     evidence))
    if not request_postgres.bound_non_pool_aws_provider_absence_is_settled(
            context, authority):
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'instances': instances,
                'provider_identity': identity,
                'reason': 'aws-create-settling',
            })
    _sleep_provider_quiet_interval(_AWS_EMPTY_CENSUS_INTERVAL_SECONDS,
                                   deadline_monotonic)
    try:
        instances = _query_aws_paid_provider_census_isolated(
            scope, deadline_monotonic=deadline_monotonic)
    except Exception as error:  # pylint: disable=broad-except
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'error_type': type(error).__name__,
                'provider_identity': identity,
                'reason': 'aws-provider-second-read-failed',
            })
    evidence = (ordinary_launch_binding.ProviderEvidence.PRESENT if any(
        instance['state'] != 'terminated' for instance in instances) else
                ordinary_launch_binding.ProviderEvidence.ABSENT)
    return ProviderObservation(
        evidence,
        _aws_observation_payload(context, replica_info, identity, instances,
                                 evidence))


def _query_gcp_paid_provider_census(
    replica_info: Any,
    provider_identity: Mapping[str, Any],
) -> tuple[list[str], list[str], dict[str, list[str]]]:
    """Perform one uncached VM, disk, and retained-operation census."""
    provider_config = {
        'availability_zone': provider_identity['zone'],
        'project_id': provider_identity['project_id'],
    }
    instances = provision.query_instances(
        provider_name='gcp',
        cluster_name=str(getattr(replica_info, 'cluster_name', '')),
        cluster_name_on_cloud=provider_identity['cluster_name_on_cloud'],
        provider_config=provider_config,
        non_terminated_only=False)
    disks = gcp_provision.query_managed_boot_disks(
        provider_identity['cluster_name_on_cloud'], provider_config)
    create_targets = gcp_provision.query_instance_create_operation_targets(
        provider_identity['cluster_name_on_cloud'], provider_config)
    return sorted(instances), sorted(disks), create_targets


def _query_gcp_paid_provider_census_isolated(
    replica_info: Any,
    provider_identity: Mapping[str, Any],
    *,
    deadline_monotonic: float,
) -> tuple[list[str], list[str], dict[str, list[str]]]:
    """Query exact GCP identity behind the killable provider boundary."""
    result = _run_paid_provider_census_worker(
        {
            'cloud': 'gcp',
            'cluster_name': str(getattr(replica_info, 'cluster_name', '')),
            'protocol_version': _PROVIDER_CENSUS_PROTOCOL_VERSION,
            'provider_identity': dict(provider_identity),
        },
        deadline_monotonic=deadline_monotonic)
    if not isinstance(result, dict) or set(result) != {
            'disks', 'instances', 'operations'
    }:
        raise RuntimeError('GCP provider census result is malformed.')
    instances = result['instances']
    disks = result['disks']
    operations = result['operations']
    expected_operation_states = {'failed', 'inflight', 'succeeded'}
    if (not isinstance(instances, list) or
            any(not isinstance(value, str) for value in instances) or
            not isinstance(disks, list) or
            any(not isinstance(value, str) for value in disks) or
            not isinstance(operations, dict) or
            set(operations) != expected_operation_states or
            any(not isinstance(values, list) or any(not isinstance(value, str)
                                                    for value in values)
                for values in operations.values())):
        raise RuntimeError('GCP provider census result is malformed.')
    return (instances, disks, cast(dict[str, list[str]], operations))


def _gcp_unknown_observation(
    base: Mapping[str, Any],
    identity: Mapping[str, Any],
    reason: str,
    instance_ids: list[str],
    disk_ids: list[str],
    create_operation_targets: Mapping[str, list[str]],
) -> ProviderObservation:
    """Build a non-authorizing GCP observation with complete census facts."""
    return ProviderObservation(
        ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
            **base,
            'create_operation_targets': dict(create_operation_targets),
            'disk_ids': disk_ids,
            'instance_ids': instance_ids,
            'provider_identity': dict(identity),
            'reason': reason,
        })


def _observe_gcp_paid_provider(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    *,
    deadline_monotonic: float,
) -> ProviderObservation:
    """Query frozen GCP VM, disk, and retained create-operation identity."""
    identity = request_postgres.bound_non_pool_gcp_provider_identity(
        context, authority)
    base = {
        'association_id': str(context.association_id),
        'cluster_name': getattr(replica_info, 'cluster_name', None),
        'probe_contract': 'gcp-vm-disk-operation-presence-v1',
        'profile_kind': context.profile.kind.value,
        'replica_record_id': str(context.replica_record_id),
    }
    if identity is None:
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'reason': 'missing-immutable-gcp-provider-identity',
            })
    try:
        instance_ids, disk_ids, create_targets = (
            _query_gcp_paid_provider_census_isolated(
                replica_info, identity, deadline_monotonic=deadline_monotonic))
    except Exception as error:  # pylint: disable=broad-except
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'error_type': type(error).__name__,
                'provider_identity': identity,
                'reason': 'gcp-provider-read-failed',
            })
    if instance_ids or disk_ids:
        evidence = ordinary_launch_binding.ProviderEvidence.PRESENT
        return ProviderObservation(
            evidence,
            _gcp_observation_payload(context, replica_info, identity,
                                     instance_ids, disk_ids, create_targets,
                                     evidence))
    if create_targets['inflight']:
        return _gcp_unknown_observation(base, identity,
                                        'gcp-create-operation-in-flight',
                                        instance_ids, disk_ids, create_targets)
    completed_targets = sorted(create_targets['failed'] +
                               create_targets['succeeded'])
    if not request_postgres.bound_non_pool_gcp_provider_absence_is_settled(
            context, authority, completed_create_targets=completed_targets):
        return _gcp_unknown_observation(base, identity,
                                        'gcp-legacy-create-settling',
                                        instance_ids, disk_ids, create_targets)
    # A retained operation can become visible or materialize a VM after the
    # first empty read. Require a second complete uncached census; operation
    # retention is the durable fence, and this quiet interval closes list
    # propagation races around terminal request quiescence.
    _sleep_provider_quiet_interval(_GCP_EMPTY_CENSUS_INTERVAL_SECONDS,
                                   deadline_monotonic)
    try:
        instance_ids, disk_ids, create_targets = (
            _query_gcp_paid_provider_census_isolated(
                replica_info, identity, deadline_monotonic=deadline_monotonic))
    except Exception as error:  # pylint: disable=broad-except
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'error_type': type(error).__name__,
                'provider_identity': identity,
                'reason': 'gcp-provider-second-read-failed',
            })
    if instance_ids or disk_ids:
        evidence = ordinary_launch_binding.ProviderEvidence.PRESENT
    elif create_targets['inflight']:
        return _gcp_unknown_observation(base, identity,
                                        'gcp-create-operation-in-flight',
                                        instance_ids, disk_ids, create_targets)
    else:
        evidence = ordinary_launch_binding.ProviderEvidence.ABSENT
    return ProviderObservation(
        evidence,
        _gcp_observation_payload(context, replica_info, identity, instance_ids,
                                 disk_ids, create_targets, evidence))


def observe_provider(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority | None = None,
    *,
    provider_operation_deadline_monotonic: float | None = None,
) -> ProviderObservation:
    """Read only the exact provider identity retained by the profile."""
    if not isinstance(context,
                      ordinary_launch_binding.BoundNonPoolLaunchContext):
        raise TypeError('context must be a bound non-pool launch context.')
    base = {
        'association_id': str(context.association_id),
        'cluster_name': getattr(replica_info, 'cluster_name', None),
        'profile_kind': context.profile.kind.value,
        'replica_record_id': str(context.replica_record_id),
    }
    if (ordinary_launch_binding.is_paid_provider_reconciliation_profile(
            context.profile.kind) and authority is not None):
        deadline_monotonic = provider_operation_deadline_monotonic
        if deadline_monotonic is None:
            deadline_monotonic = (time.monotonic() +
                                  _PROVIDER_CENSUS_DEFAULT_TIMEOUT_SECONDS)
        elif (isinstance(deadline_monotonic, bool) or
              not isinstance(deadline_monotonic, (int, float)) or
              not math.isfinite(deadline_monotonic)):
            raise ValueError('Paid provider census deadline is malformed.')
        pool_key = getattr(replica_info, 'paid_capacity_pool_key', None)
        pool_identity = (paid_capacity.pool_key_payload(pool_key) if isinstance(
            pool_key, str) else None)
        cloud = (pool_identity.get('cloud') if isinstance(
            pool_identity, Mapping) else None)
        if cloud == 'aws':
            return _observe_aws_paid_provider(
                context,
                replica_info,
                authority,
                deadline_monotonic=deadline_monotonic)
        if cloud == 'gcp':
            return _observe_gcp_paid_provider(
                context,
                replica_info,
                authority,
                deadline_monotonic=deadline_monotonic)
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'probe_contract': 'immutable-paid-pool-presence-v1',
                'reason': 'missing-immutable-paid-pool-identity',
            })
    if (context.profile.kind
            != ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL):
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'probe_contract': 'immutable-provider-identity-v1',
                'reason': 'profile-has-no-durable-provider-uid',
            })

    try:
        fence = reserved_capacity.parse_protocol_v2_cleanup_fence(replica_info)
    except Exception as error:  # pylint: disable=broad-except
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'error_type': type(error).__name__,
                'probe_contract': 'kubernetes-physical-replica-presence-v1',
                'reason': 'malformed-provider-identity',
            })
    if fence is None:
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'probe_contract': 'kubernetes-physical-replica-presence-v1',
                'reason': 'missing-provider-identity',
            })

    base = _reserved_fill_observation_payload(context, replica_info, fence)
    current_uid = reserved_capacity.get_kubernetes_physical_cluster_uid(
        fence.kubernetes_context, force_refresh=True)
    if current_uid is None:
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'reason': 'physical-cluster-identity-unreadable',
            })
    if current_uid != fence.physical_cluster_uid:
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.REPLACED, {
                **base,
                'observed_physical_cluster_uid': current_uid,
                'reason': 'kubernetes-context-retargeted',
            })

    provider_read_boundary = time.monotonic()
    presence = reserved_capacity.probe_physical_replica_presence(
        fence, replica_info.cluster_name, observed_after=provider_read_boundary)
    classification = {
        reserved_capacity.PhysicalReplicaPresence.ABSENT:
            ordinary_launch_binding.ProviderEvidence.ABSENT,
        reserved_capacity.PhysicalReplicaPresence.PRESENT:
            ordinary_launch_binding.ProviderEvidence.PRESENT,
        reserved_capacity.PhysicalReplicaPresence.UNPROVEN:
            ordinary_launch_binding.ProviderEvidence.UNKNOWN,
    }[presence]
    return ProviderObservation(classification, {
        **base,
        'result': presence.value,
    })


def observe_post_teardown_absence_receipt(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    receipt: reserved_capacity.ProtocolV2PhysicalAbsenceReceipt,
) -> ProviderObservation:
    """Authenticate an already-observed exact post-teardown ABSENT result.

    The teardown worker obtained this receipt from the uncached provider read
    performed under the replica's immutable physical-cluster fence. Reusing it
    here prevents a second provider read from turning proven absence back into
    transient UNKNOWN.
    """
    if not isinstance(receipt,
                      reserved_capacity.ProtocolV2PhysicalAbsenceReceipt):
        raise TypeError('receipt must be a protocol-v2 absence receipt.')
    if (context.profile.kind
            != ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Post-teardown physical absence requires a reserved-fill profile.')
    try:
        fence = reserved_capacity.parse_protocol_v2_cleanup_fence(replica_info)
    except Exception as error:  # pylint: disable=broad-except
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Post-teardown physical absence lost its durable provider '
            'identity.') from error
    if (fence is None or
            receipt.cleanup_fence != fence or receipt.cluster_name != getattr(
                replica_info, 'cluster_name', None)):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Post-teardown physical absence does not match the exact '
            'reserved-fill replica.')
    return ProviderObservation(
        ordinary_launch_binding.ProviderEvidence.ABSENT, {
            **_reserved_fill_observation_payload(context, replica_info, fence),
            'result': reserved_capacity.PhysicalReplicaPresence.ABSENT.value,
        })


def _reduce_observation(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    project_replica_result: Callable[..., bool],
    observation: ProviderObservation,
    *,
    continue_guard: Callable[[], bool] | None = None,
) -> None:
    """Persist and reduce one already-completed exact provider observation."""
    if continue_guard is not None and not continue_guard():
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider reconciliation lost lifecycle authority before evidence '
            'persistence.')
    request_postgres.record_bound_non_pool_provider_evidence(
        context, authority, observation.evidence, observation.payload)
    # Both transactions revalidate the immutable lifecycle/association
    # authority in PostgreSQL. Recheck the process-local lane fence between
    # them too, so a worker outliving close() cannot begin a second mutation.
    if continue_guard is not None and not continue_guard():
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider reconciliation lost lifecycle authority before evidence '
            'projection.')
    if observation.evidence == ordinary_launch_binding.ProviderEvidence.ABSENT:
        request_postgres.project_bound_non_pool_provider_absence(
            context, authority, project_replica_result=project_replica_result)
    elif (observation.evidence ==
          ordinary_launch_binding.ProviderEvidence.PRESENT):
        request_postgres.authorize_bound_non_pool_provider_present_cleanup(
            context, authority, project_replica_result=project_replica_result)


def _provider_teardown_submission(
    disposition: resource_actions_types.ProviderSubmissionDisposition,
    error: Exception | SystemExit | KeyboardInterrupt | None = None,
) -> resource_actions_types.ProviderSubmissionV1:
    """Build bounded normalized evidence for one teardown submission."""
    normalized_error = None
    if error is not None:
        normalized_error = resource_actions.ProviderErrorV1(
            category=resource_actions.ProviderErrorCategory.UNKNOWN,
            provider_code=None,
            retry_after_seconds=None,
            normalized_message=common_utils.format_exception(error))
    return resource_actions.ProviderSubmissionV1(
        disposition=disposition,
        provider_operation_id=None,
        normalized_response_sha256=None,
        normalized_error=normalized_error)


def submit_gcp_paid_provider_teardown(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    *,
    continue_guard: Callable[[], bool] | None = None,
) -> resource_actions_types.ProviderSubmissionV1:
    """Submit at most one exact GCP delete without waiting for absence."""
    if (not ordinary_launch_binding.is_paid_provider_reconciliation_profile(
            context.profile.kind) or not request_postgres.
            bound_non_pool_provider_present_cleanup_is_authorized(
                context, authority)):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'GCP provider cleanup lacks exact durable PRESENT authority.')
    identity = request_postgres.bound_non_pool_gcp_provider_identity(
        context, authority)
    if identity is None:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'GCP provider cleanup lost its immutable request identity.')
    if continue_guard is not None and not continue_guard():
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'GCP provider cleanup lost controller authority before teardown.')
    provider_config = {
        'availability_zone': identity['zone'],
        'project_id': identity['project_id'],
    }
    try:
        instance_ids, disks, create_targets = _query_gcp_paid_provider_census(
            replica_info, identity)
    except Exception as error:  # pylint: disable=broad-except
        return _provider_teardown_submission(
            resource_actions.ProviderSubmissionDisposition.NOT_SUBMITTED, error)
    if continue_guard is not None and not continue_guard():
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'GCP provider cleanup lost controller authority before provider '
            'submission.')
    try:
        exact_instance_targets = sorted(
            set(instance_ids + create_targets['inflight'] +
                create_targets['succeeded']))
        if exact_instance_targets:
            gcp_provision.submit_terminate_exact_instances(
                exact_instance_targets, provider_config)
        elif disks:
            gcp_provision.submit_terminate_exact_managed_boot_disks(
                disks, provider_config)
    except Exception as error:  # pylint: disable=broad-except
        return _provider_teardown_submission(
            resource_actions.ProviderSubmissionDisposition.AMBIGUOUS, error)
    return _provider_teardown_submission(
        resource_actions.ProviderSubmissionDisposition.ACCEPTED)


def submit_aws_paid_provider_teardown(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    *,
    continue_guard: Callable[[], bool] | None = None,
) -> resource_actions_types.ProviderSubmissionV1:
    """Submit at most one exact AWS terminate call without polling."""
    del replica_info
    if (not ordinary_launch_binding.is_paid_provider_reconciliation_profile(
            context.profile.kind) or not request_postgres.
            bound_non_pool_provider_present_cleanup_is_authorized(
                context, authority)):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'AWS provider cleanup lacks exact durable PRESENT authority.')
    scope = request_postgres.bound_non_pool_aws_provider_census_scope(
        context, authority)
    if scope is None:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'AWS provider cleanup lost immutable identity or access.')
    try:
        instances = _query_aws_paid_provider_census(scope)
    except Exception as error:  # pylint: disable=broad-except
        return _provider_teardown_submission(
            resource_actions.ProviderSubmissionDisposition.NOT_SUBMITTED, error)
    live_ids = [
        instance['instance_id']
        for instance in instances
        if instance['state'] != 'terminated'
    ]
    if continue_guard is not None and not continue_guard():
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'AWS provider cleanup lost controller authority before provider '
            'submission.')
    if not live_ids:
        return _provider_teardown_submission(
            resource_actions.ProviderSubmissionDisposition.ACCEPTED)
    identity = scope.provider_identity
    try:
        session = aws_adaptor.session(profile=scope.credential_profile)
        account = session.client(
            'sts', region_name=identity['region']).get_caller_identity()
        if account.get('Account') != identity['aws_account_id']:
            raise ValueError(
                'AWS cleanup credentials resolved to another account.')
        session.client('ec2',
                       region_name=identity['region']).terminate_instances(
                           InstanceIds=live_ids)
    except Exception as error:  # pylint: disable=broad-except
        return _provider_teardown_submission(
            resource_actions.ProviderSubmissionDisposition.AMBIGUOUS, error)
    return _provider_teardown_submission(
        resource_actions.ProviderSubmissionDisposition.ACCEPTED)


def submit_paid_provider_teardown(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    *,
    continue_guard: Callable[[], bool] | None = None,
) -> resource_actions_types.ProviderSubmissionV1:
    """Submit one exact paid delete and durably hand off to observation."""
    pool_identity = paid_capacity.pool_key_payload(
        str(replica_info.paid_capacity_pool_key))
    cloud = (pool_identity.get('cloud')
             if isinstance(pool_identity, Mapping) else None)
    if cloud == 'aws':
        submission = submit_aws_paid_provider_teardown(
            context, replica_info, authority, continue_guard=continue_guard)
    elif cloud == 'gcp':
        submission = submit_gcp_paid_provider_teardown(
            context, replica_info, authority, continue_guard=continue_guard)
    else:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Paid provider cleanup lost its immutable pool cloud.')
    persisted = (request_postgres.
                 mark_bound_non_pool_provider_teardown_observation_pending(
                     context, authority))
    expected_phase = (ordinary_launch_binding.ProviderPresentTeardownPhase.
                      ABSENCE_OBSERVATION_PENDING)
    if ordinary_launch_binding.provider_present_teardown_phase(
            persisted) is not expected_phase:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Paid provider teardown did not durably reach observation '
            'pending.')
    return submission


def reconcile_post_teardown_absence(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    project_replica_result: Callable[..., bool],
    receipt: reserved_capacity.ProtocolV2PhysicalAbsenceReceipt,
) -> ProviderObservation:
    """Project one exact post-teardown receipt without provider reread."""
    if not request_postgres.bound_non_pool_provider_reconciliation_ready(
            context, authority):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider reconciliation is waiting for exact request '
            'quiescence.')
    observation = observe_post_teardown_absence_receipt(context, replica_info,
                                                        receipt)
    _reduce_observation(context, authority, project_replica_result, observation)
    return observation


def reconcile(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    project_replica_result: Callable[..., bool],
    *,
    force_provider_read: bool = False,
    provider_operation_deadline_monotonic: float | None = None,
    continue_guard: Callable[[], bool] | None = None,
) -> ProviderObservation:
    """Observe outside locks, then reduce exact provider evidence."""
    if not request_postgres.bound_non_pool_provider_reconciliation_ready(
            context, authority):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider reconciliation is waiting for exact request '
            'quiescence.')
    if (not force_provider_read and
            request_postgres.bound_non_pool_provider_absence_is_recorded(
                context, authority)):
        # ABSENT is immutable exact evidence. Project it before another
        # provider read: a later transient UNKNOWN observation must not strand
        # a row whose absence was already proven after executor quiescence.
        if continue_guard is not None and not continue_guard():
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Provider reconciliation lost lifecycle authority before '
                'absence projection.')
        request_postgres.project_bound_non_pool_provider_absence(
            context, authority, project_replica_result=project_replica_result)
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.ABSENT, {
                'result':
                    reserved_capacity.PhysicalReplicaPresence.ABSENT.value,
                'source': 'durable-provider-evidence',
            })
    observation = None
    paid_teardown_observation_pending = bool(
        ordinary_launch_binding.is_paid_provider_reconciliation_profile(
            context.profile.kind) and
        ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
            replica_info) and
        ordinary_launch_binding.provider_present_teardown_phase(replica_info)
        is ordinary_launch_binding.ProviderPresentTeardownPhase.
        ABSENCE_OBSERVATION_PENDING)
    if (context.profile.kind ==
            ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID):
        paid_payload = (
            request_postgres.bound_non_pool_terminal_provider_absence_payload(
                context, authority))
        if paid_payload is not None:
            observation = ProviderObservation(
                ordinary_launch_binding.ProviderEvidence.ABSENT, paid_payload)
    if observation is None:
        deadline_kwargs = ({
            'provider_operation_deadline_monotonic': provider_operation_deadline_monotonic
        } if provider_operation_deadline_monotonic is not None else {})
        observation = observe_provider(context, replica_info, authority,
                                       **deadline_kwargs)
    if (paid_teardown_observation_pending and observation.evidence
            != ordinary_launch_binding.ProviderEvidence.ABSENT):
        # UNKNOWN cannot erase the last exact PRESENT cleanup authority, and
        # PRESENT needs only to hand the row back to the submission phase.
        # Only ABSENT is settlement evidence for this split teardown phase.
        return observation
    # _reduce_observation records before it projects.  If ABSENT projection
    # fails, the retry consumes that immutable receipt without another
    # provider read.  Local cluster-record finalization is a separate,
    # provider-free step after this transaction releases paid authority.
    _reduce_observation(context,
                        authority,
                        project_replica_result,
                        observation,
                        continue_guard=continue_guard)
    return observation


def advance_paid_teardown_observation(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    project_replica_result: Callable[..., bool],
    *,
    provider_operation_deadline_monotonic: float | None = None,
    continue_guard: Callable[[], bool] | None = None,
) -> PaidTeardownObservationStep:
    """Observe once and perform the only legal paid teardown transition."""
    if (not ordinary_launch_binding.is_paid_provider_reconciliation_profile(
            context.profile.kind) or
            ordinary_launch_binding.provider_present_teardown_phase(
                replica_info) is not ordinary_launch_binding.
            ProviderPresentTeardownPhase.ABSENCE_OBSERVATION_PENDING):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Paid teardown observation requires its durable pending phase.')
    observation = reconcile(
        context,
        replica_info,
        authority,
        project_replica_result,
        provider_operation_deadline_monotonic=(
            provider_operation_deadline_monotonic),
        continue_guard=continue_guard,
    )
    if observation.evidence is ordinary_launch_binding.ProviderEvidence.ABSENT:
        return PaidTeardownObservationStep(
            PaidTeardownObservationDisposition.SETTLED_ABSENT, observation)
    if observation.evidence is ordinary_launch_binding.ProviderEvidence.PRESENT:
        if continue_guard is not None and not continue_guard():
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Paid teardown observation lost lifecycle authority before '
                'resubmission.')
        scheduled = request_postgres.requeue_bound_non_pool_provider_teardown_submission(
            context, authority)
        return PaidTeardownObservationStep(
            PaidTeardownObservationDisposition.RESUBMIT_PRESENT,
            observation,
            scheduled_replica_info=scheduled)
    return PaidTeardownObservationStep(
        PaidTeardownObservationDisposition.RETRY_UNKNOWN, observation)


if __name__ == '__main__':
    if sys.argv[1:] != [_PROVIDER_CENSUS_WORKER_ARGUMENT]:
        raise SystemExit(2)
    raise SystemExit(_provider_census_worker_main())
