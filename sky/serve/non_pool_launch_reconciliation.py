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
import time
from typing import Any, cast

from sky import exceptions
from sky.adaptors import common as adaptors_common
from sky.provision import capacity_policy
from sky.provision import constants as provision_constants
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
from sky.serve import reserved_capacity
from sky.utils import common_utils

request_postgres = adaptors_common.LazyImport('sky.server.requests.postgres')
api_requests = adaptors_common.LazyImport('sky.server.requests.requests')
provision = adaptors_common.LazyImport('sky.provision')
gcp_provision = adaptors_common.LazyImport('sky.provision.gcp')
gcp_cloud = adaptors_common.LazyImport('sky.clouds.gcp')
aws_adaptor = adaptors_common.LazyImport('sky.adaptors.aws')

_AWS_EMPTY_CENSUS_INTERVAL_SECONDS = 2.0
_AWS_POST_TEARDOWN_ABSENCE_TIMEOUT_SECONDS = 420.0
_AWS_POST_TEARDOWN_ABSENCE_POLL_SECONDS = 2.0
_GCP_EMPTY_CENSUS_INTERVAL_SECONDS = 2.0
_GCP_POST_TEARDOWN_ABSENCE_TIMEOUT_SECONDS = 420.0
_GCP_POST_TEARDOWN_ABSENCE_POLL_SECONDS = 2.0


@dataclasses.dataclass(frozen=True)
class ProviderObservation:
    """One closed provider classification and its canonical evidence."""

    evidence: ordinary_launch_binding.ProviderEvidence
    payload: dict[str, Any]


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
    if (context.profile.kind ==
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL):
        if pool_key is not None:
            return None
        reserved_absence = True
    elif ordinary_launch_binding.is_paid_provider_reconciliation_profile(
            context.profile.kind):
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
            replacement_shape_matches = bool(
                context.profile.kind is ordinary_launch_binding.
                NonPoolLaunchProfileKind.ORDINARY_PAID or
                (isinstance(pool_identity, Mapping) and
                 pool_identity.get('cloud') == 'gcp' and
                 pool_identity.get('version') == 1 and
                 pool_identity.get('use_spot') is True))
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
    provider_identity: Mapping[str, Any],) -> list[dict[str, str]]:
    """Perform one uncached, account-checked EC2 client-token census."""
    session = aws_adaptor.session(
        profile=provider_identity['credential_profile'])
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


def _observe_aws_paid_provider(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
) -> ProviderObservation:
    """Query frozen AWS account, placement, and EC2 client-token identity."""
    identity = request_postgres.bound_non_pool_aws_provider_identity(
        context, authority)
    base = {
        'association_id': str(context.association_id),
        'cluster_name': getattr(replica_info, 'cluster_name', None),
        'probe_contract': 'aws-client-token-instance-presence-v1',
        'profile_kind': context.profile.kind.value,
        'replica_record_id': str(context.replica_record_id),
    }
    if identity is None:
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.UNKNOWN, {
                **base,
                'reason': 'missing-immutable-aws-provider-identity',
            })
    try:
        instances = _query_aws_paid_provider_census(identity)
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
    time.sleep(_AWS_EMPTY_CENSUS_INTERVAL_SECONDS)
    try:
        instances = _query_aws_paid_provider_census(identity)
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
            _query_gcp_paid_provider_census(replica_info, identity))
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
    time.sleep(_GCP_EMPTY_CENSUS_INTERVAL_SECONDS)
    try:
        instance_ids, disk_ids, create_targets = (
            _query_gcp_paid_provider_census(replica_info, identity))
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
        pool_key = getattr(replica_info, 'paid_capacity_pool_key', None)
        pool_identity = (paid_capacity.pool_key_payload(pool_key) if isinstance(
            pool_key, str) else None)
        cloud = (pool_identity.get('cloud') if isinstance(
            pool_identity, Mapping) else None)
        if cloud == 'aws':
            return _observe_aws_paid_provider(context, replica_info, authority)
        if cloud == 'gcp':
            return _observe_gcp_paid_provider(context, replica_info, authority)
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
) -> None:
    """Persist and reduce one already-completed exact provider observation."""
    request_postgres.record_bound_non_pool_provider_evidence(
        context, authority, observation.evidence, observation.payload)
    if observation.evidence == ordinary_launch_binding.ProviderEvidence.ABSENT:
        request_postgres.project_bound_non_pool_provider_absence(
            context, authority, project_replica_result=project_replica_result)
    elif (observation.evidence ==
          ordinary_launch_binding.ProviderEvidence.PRESENT):
        request_postgres.authorize_bound_non_pool_provider_present_cleanup(
            context, authority, project_replica_result=project_replica_result)


def terminate_gcp_paid_provider_allocation(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    project_replica_result: Callable[..., bool],
    *,
    continue_guard: Callable[[], bool] | None = None,
) -> ProviderObservation:
    """Delete one exact GCP allocation and require fresh provider absence."""
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
    deadline = (time.monotonic() + _GCP_POST_TEARDOWN_ABSENCE_TIMEOUT_SECONDS)
    provider_config = {
        'availability_zone': identity['zone'],
        'project_id': identity['project_id'],
    }
    last_cleanup_error: BaseException | None = None
    while True:
        if time.monotonic() >= deadline:
            timeout_error = ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'GCP provider cleanup has no fresh exact VM, disk, and create-'
                'operation ABSENT observation.')
            if last_cleanup_error is not None:
                raise timeout_error from last_cleanup_error
            raise timeout_error
        if continue_guard is not None and not continue_guard():
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'GCP provider cleanup lost authority while deleting the exact '
                'allocation.')
        try:
            instance_ids, _, create_targets = _query_gcp_paid_provider_census(
                replica_info, identity)
            if instance_ids or create_targets['inflight']:
                # Standard SkyPilot down does not wait for VM disappearance and
                # its cluster row can disappear first. Repeat this exact native
                # delete idempotently until the frozen provider census is empty.
                provision.terminate_instances(
                    provider_name='gcp',
                    cluster_name_on_cloud=identity['cluster_name_on_cloud'],
                    provider_config=provider_config)
                time.sleep(_GCP_POST_TEARDOWN_ABSENCE_POLL_SECONDS)
                continue
            # Boot disks can remain attached while a just-deleted VM drains.
            # Retry resource-in-use/deleting failures under the same deadline.
            gcp_provision.terminate_managed_boot_disks(
                identity['cluster_name_on_cloud'], provider_config)
            last_cleanup_error = None
        except Exception as error:  # pylint: disable=broad-except
            last_cleanup_error = error
        observation = _observe_gcp_paid_provider(context, replica_info,
                                                 authority)
        if (observation.evidence
                is ordinary_launch_binding.ProviderEvidence.ABSENT):
            break
        time.sleep(_GCP_POST_TEARDOWN_ABSENCE_POLL_SECONDS)
    _reduce_observation(context, authority, project_replica_result, observation)
    return observation


def terminate_aws_paid_provider_allocation(
    context: ordinary_launch_binding.BoundNonPoolLaunchContext,
    replica_info: Any,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    project_replica_result: Callable[..., bool],
    *,
    continue_guard: Callable[[], bool] | None = None,
) -> ProviderObservation:
    """Terminate exact client-token EC2 instances and prove fresh absence."""
    if (not ordinary_launch_binding.is_paid_provider_reconciliation_profile(
            context.profile.kind) or not request_postgres.
            bound_non_pool_provider_present_cleanup_is_authorized(
                context, authority)):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'AWS provider cleanup lacks exact durable PRESENT authority.')
    identity = request_postgres.bound_non_pool_aws_provider_identity(
        context, authority)
    if identity is None:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'AWS provider cleanup lost its immutable request identity.')
    deadline = time.monotonic() + _AWS_POST_TEARDOWN_ABSENCE_TIMEOUT_SECONDS
    last_cleanup_error: BaseException | None = None
    observation: ProviderObservation | None = None
    while True:
        if time.monotonic() >= deadline:
            timeout_error = ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'AWS provider cleanup has no fresh exact client-token ABSENT '
                'observation.')
            if last_cleanup_error is not None:
                raise timeout_error from last_cleanup_error
            raise timeout_error
        if continue_guard is not None and not continue_guard():
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'AWS provider cleanup lost authority while deleting the exact '
                'allocation.')
        try:
            instances = _query_aws_paid_provider_census(identity)
            live_ids = [
                instance['instance_id']
                for instance in instances
                if instance['state'] != 'terminated'
            ]
            if live_ids:
                session = aws_adaptor.session(
                    profile=identity['credential_profile'])
                account = session.client(
                    'sts',
                    region_name=identity['region']).get_caller_identity()
                if account.get('Account') != identity['aws_account_id']:
                    raise ValueError(
                        'AWS cleanup credentials resolved to another account.')
                session.client(
                    'ec2', region_name=identity['region']).terminate_instances(
                        InstanceIds=live_ids)
                last_cleanup_error = None
                time.sleep(_AWS_POST_TEARDOWN_ABSENCE_POLL_SECONDS)
                continue
            observation = _observe_aws_paid_provider(context, replica_info,
                                                     authority)
            if (observation.evidence
                    is ordinary_launch_binding.ProviderEvidence.ABSENT):
                break
            last_cleanup_error = None
        except Exception as error:  # pylint: disable=broad-except
            last_cleanup_error = error
        time.sleep(_AWS_POST_TEARDOWN_ABSENCE_POLL_SECONDS)
    assert observation is not None
    _reduce_observation(context, authority, project_replica_result, observation)
    return observation


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
) -> ProviderObservation:
    """Observe outside locks, then reduce exact absence or authorize cleanup."""
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
        request_postgres.project_bound_non_pool_provider_absence(
            context, authority, project_replica_result=project_replica_result)
        return ProviderObservation(
            ordinary_launch_binding.ProviderEvidence.ABSENT, {
                'result':
                    reserved_capacity.PhysicalReplicaPresence.ABSENT.value,
                'source': 'durable-provider-evidence',
            })
    observation = None
    if (context.profile.kind ==
            ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID):
        paid_payload = (
            request_postgres.bound_non_pool_terminal_provider_absence_payload(
                context, authority))
        if paid_payload is not None:
            observation = ProviderObservation(
                ordinary_launch_binding.ProviderEvidence.ABSENT, paid_payload)
    if observation is None:
        observation = observe_provider(context, replica_info, authority)
    _reduce_observation(context, authority, project_replica_result, observation)
    return observation
