"""Separately permissioned EC2 and EKS runtime-pull canary worker."""

from __future__ import annotations

import base64
from collections.abc import Callable
import concurrent.futures
import contextlib
import hashlib
import os
import shlex
import signal
import threading
import time
from typing import Any
import uuid

from sky.adaptors import kubernetes
from sky.container_images import aws
from sky.container_images import catalog_state
from sky.container_images import models
from sky.container_images import qualification
from sky.container_images import topology_state
from sky.container_images import worker_health
from sky.container_images import worker_lease
from sky.server import database_migrations

_DEFAULT_LEASE_SECONDS = 15 * 60
_POLL_SECONDS = 10
_MAX_QUALIFIED_EKS_NODES = 1000
_EC2_TEARDOWN_SECONDS = 5 * 60
_EC2_TEARDOWN_ATTEMPTS = 60
_EC2_TEARDOWN_POLL_SECONDS = 5
_EKS_TEARDOWN_SECONDS = 60
_EKS_TEARDOWN_POLL_SECONDS = 2
_EKS_ABSENCE_SETTLE_SECONDS = kubernetes.API_TIMEOUT + 1
_LeaseHeartbeat = worker_lease.LeaseHeartbeat
_CANARY_ERROR_CODES = frozenset({
    'CANARY_DUPLICATE_CHILD',
    'CANARY_FAILED',
    'CANARY_PRINCIPAL_UNVERIFIED',
    'CANARY_PULL_FAILED',
    'CANARY_TEARDOWN_FAILED',
    'CANARY_TIMEOUT',
    'IMAGE_LOCALITY_UNSUPPORTED',
    'PROFILE_NOT_ACTIVE',
    'QUALIFICATION_FAILED',
    'QUALIFIED_KUBERNETES_NODE_ROLE_REQUIRED',
    'QUALIFIED_RUNTIME_PRINCIPAL_REQUIRED',
})


class _CanaryDrainRequested(Exception):
    """Requests prompt canary teardown without terminalizing its operation."""


def _raise_if_draining(drain_event: threading.Event | None) -> None:
    if drain_event is not None and drain_event.is_set():
        raise _CanaryDrainRequested()


def _wait_for_canary_poll(drain_event: threading.Event | None) -> None:
    if drain_event is None:
        time.sleep(_POLL_SECONDS)
    elif drain_event.wait(_POLL_SECONDS):
        raise _CanaryDrainRequested()


class _FencedClient:
    """Proves the canary lease around every provider SDK call."""

    def __init__(self, client: Any,
                 heartbeat: worker_lease.LeaseHeartbeat) -> None:
        self._client = client
        self._heartbeat = heartbeat

    def _call(self,
              value: Callable[..., Any],
              args: tuple[Any, ...],
              kwargs: dict[str, Any],
              *,
              deadline: float | None = None,
              on_start: Callable[[], None] | None = None) -> Any:
        self._heartbeat.assert_owned()
        if deadline is not None and time.monotonic() >= deadline:
            raise ValueError('CANARY_TIMEOUT')
        if on_start is not None:
            on_start()
        try:
            result = value(*args, **kwargs)
        except Exception:  # pylint: disable=broad-except
            self._heartbeat.assert_owned()
            raise
        self._heartbeat.assert_owned()
        return result

    def __getattr__(self, name: str) -> Any:
        value = getattr(self._client, name)
        if not callable(value):
            return value

        def call(*args: Any, **kwargs: Any) -> Any:
            return self._call(value, args, kwargs)

        return call

    def call_before_deadline(self, name: str, deadline: float,
                             on_start: Callable[[], None], *args: Any,
                             **kwargs: Any) -> Any:
        """Fences ownership, then deadline, immediately before create."""
        value = getattr(self._client, name)
        if not callable(value):
            raise TypeError(f'Provider attribute {name!r} is not callable.')
        return self._call(value,
                          args,
                          kwargs,
                          deadline=deadline,
                          on_start=on_start)


def _assumed_client(role: aws.AwsRoleBinding, service: str, region: str,
                    heartbeat: worker_lease.LeaseHeartbeat) -> _FencedClient:
    heartbeat.assert_owned()
    try:
        client = aws.assumed_client(role,
                                    service,
                                    region,
                                    provider_fence=heartbeat.assert_owned)
    except Exception:  # pylint: disable=broad-except
        heartbeat.assert_owned()
        raise
    heartbeat.assert_owned()
    return _FencedClient(client, heartbeat)


def _kubernetes_core(context: str,
                     heartbeat: worker_lease.LeaseHeartbeat) -> _FencedClient:
    heartbeat.assert_owned()
    try:
        core = kubernetes.core_api(context)
    except Exception:  # pylint: disable=broad-except
        heartbeat.assert_owned()
        raise
    heartbeat.assert_owned()
    return _FencedClient(core, heartbeat)


def _ec2_client_token(operation_id: str) -> str:
    """Returns one stable EC2 idempotency token for a canary operation."""
    return hashlib.sha256(
        f'skypilot-image-canary:{operation_id}'.encode()).hexdigest()


def _attach_canary_child(operation: catalog_state.OperationRecord,
                         child_id: str,
                         heartbeat: worker_lease.LeaseHeartbeat) -> None:
    assert operation.lease_token is not None
    heartbeat.assert_owned()
    try:
        attached = qualification.attach_canary_child(operation.id,
                                                     operation.lease_token,
                                                     child_id)
    except ValueError as error:
        # A different durable child cannot be discarded by terminal failure.
        raise ValueError('CANARY_TEARDOWN_FAILED') from error
    except Exception as error:  # pylint: disable=broad-except
        if operation.child_launch_id is not None:
            raise ValueError('CANARY_TEARDOWN_FAILED') from error
        raise
    if not attached:
        raise worker_lease.LeaseLostError(
            'Canary operation lease or launch deadline was lost.')
    heartbeat.assert_owned()


def _authorized_launch_deadline(
        operation: catalog_state.OperationRecord, child_id: str,
        heartbeat: worker_lease.LeaseHeartbeat) -> float:
    """Maps the locked database deadline onto this process's monotonic clock."""
    assert operation.lease_token is not None
    started_at = time.monotonic()
    heartbeat.assert_owned()
    remaining = qualification.authorize_canary_launch(operation.id,
                                                      operation.lease_token,
                                                      child_id)
    if remaining is None:
        raise ValueError('CANARY_TIMEOUT')
    heartbeat.assert_owned()
    return started_at + remaining


def _preflight_error(operation: catalog_state.OperationRecord,
                     error_code: str) -> ValueError:
    if operation.child_launch_id is not None:
        return ValueError('CANARY_TEARDOWN_FAILED')
    return ValueError(error_code)


def _canary_role(
        profile: models.ManagedRegistryProfile,
        runtime_binding: models.RegistryAccessBinding) -> aws.AwsRoleBinding:
    authority_id = runtime_binding.canary_authority
    if authority_id is None:
        raise ValueError('QUALIFICATION_FAILED')
    authority = profile.bindings[authority_id]
    if (authority.kind != models.RegistryAccessBindingKind.AWS_ASSUME_ROLE or
            'canary_launch' not in authority.purposes or
            authority.authority is None):
        raise ValueError('QUALIFICATION_FAILED')
    return aws.AwsRoleBinding(
        role_arn=authority.authority,
        external_id=authority.external_id,
        session_name=f'sky-img-canary-{uuid.uuid4().hex[:12]}',
        catalog_tag=catalog_state.get_catalog_authority_id(),
        profile_tag=profile.name)


def _load_contract(
    operation: catalog_state.OperationRecord,
) -> tuple[dict[str, Any], topology_state.ProfileRevisionRecord,
           models.ManagedRegistryProfile, models.ManagedRegistryTarget,
           models.RegistryAccessBinding, str, str]:
    payload = qualification.canary_payload(operation)
    revision = topology_state.get_profile_revision(
        payload['profile_revision_id'])
    if revision is None:
        raise ValueError('QUALIFICATION_FAILED')
    profile = models.ManagedRegistryProfile.from_snapshot(
        revision.config_snapshot)
    if (revision.desired_generation != payload['desired_generation'] or
            revision.config_hash != payload['config_hash']):
        raise ValueError('QUALIFICATION_FAILED')
    target = profile.target(payload['target'])
    binding = profile.bindings[payload['binding_id']]
    if (target.target_fingerprint != payload['target_fingerprint'] or
            binding.fingerprint != payload['binding_fingerprint'] or
            target.runtime_binding(payload['backend']) != binding.id):
        raise ValueError('QUALIFICATION_FAILED')
    repository_name, _ = qualification.qualification_repository(
        revision, target)
    copy_key = models.profile_attestation_key('copy', target.name)
    copy_evidence = revision.attestations.get(copy_key)
    if (not isinstance(copy_evidence, dict) or
            copy_evidence.get('status') != 'READY' or
            copy_evidence.get('target_fingerprint') != target.target_fingerprint
            or not isinstance(copy_evidence.get('runtime_digest'), str) or
            copy_evidence.get('platform')
            != profile.qualification.canary_platform):
        raise ValueError('QUALIFICATION_FAILED')
    digest = models.validate_sha256_digest(copy_evidence['runtime_digest'],
                                           'Qualification canary digest')
    reference = f'{target.registry}/{repository_name}@{digest}'
    models.validate_oci_reference(reference, 'Qualification runtime reference')
    return payload, revision, profile, target, binding, digest, reference


def _instance_profile_role(iam: Any, instance_profile: str) -> str:
    response = iam.get_instance_profile(InstanceProfileName=instance_profile)
    profile = response.get('InstanceProfile', {})
    roles = profile.get('Roles', [])
    if len(roles) != 1 or not isinstance(roles[0].get('Arn'), str):
        raise ValueError('Qualified instance profile has an invalid role set.')
    return str(roles[0]['Arn'])


def _tagged_instances(ec2: Any, operation_id: str) -> list[dict[str, Any]]:
    response = ec2.describe_instances(Filters=[{
        'Name': 'tag:SkyPilotCanaryOperation',
        'Values': [operation_id],
    }, {
        'Name': 'instance-state-name',
        'Values': [
            'pending', 'running', 'stopping', 'stopped', 'shutting-down',
            'terminated'
        ],
    }])
    return [
        instance for reservation in response.get('Reservations', [])
        for instance in reservation.get('Instances', [])
    ]


def _instance_id(instance: dict[str, Any]) -> str:
    value = instance.get('InstanceId')
    if not isinstance(value, str) or not value:
        raise ValueError('QUALIFICATION_FAILED')
    return value


def _remember_instance_ids(instances: list[dict[str, Any]],
                           known_ids: set[str]) -> bool:
    """Retains concrete IDs and reports any unidentifiable child record."""
    unidentified_child_observed = False
    for instance in instances:
        value = instance.get('InstanceId')
        if isinstance(value, str) and value:
            known_ids.add(value)
        else:
            unidentified_child_observed = True
    return unidentified_child_observed


def _exact_canary_instance(ec2: Any, instance_id: str,
                           expected_tags: dict[str, str]) -> dict[str, Any]:
    """Reads all attested EC2 fields from one exact, correctly tagged child."""
    response = ec2.describe_instances(InstanceIds=[instance_id])
    instances = [
        instance for reservation in response.get('Reservations', [])
        for instance in reservation.get('Instances', [])
    ]
    if len(instances) != 1 or _instance_id(instances[0]) != instance_id:
        raise ValueError('QUALIFICATION_FAILED')
    instance = instances[0]
    raw_tags = instance.get('Tags')
    if not isinstance(raw_tags, list):
        raise ValueError('QUALIFICATION_FAILED')
    tags: dict[str, str] = {}
    for raw_tag in raw_tags:
        if not isinstance(raw_tag, dict):
            raise ValueError('QUALIFICATION_FAILED')
        key = raw_tag.get('Key')
        value = raw_tag.get('Value')
        if (not isinstance(key, str) or not isinstance(value, str) or
                key in tags):
            raise ValueError('QUALIFICATION_FAILED')
        tags[key] = value
    if any(tags.get(key) != value for key, value in expected_tags.items()):
        raise ValueError('QUALIFICATION_FAILED')
    return instance


def _instance_states(ec2: Any, instance_ids: set[str]) -> dict[str, str | None]:
    response = ec2.describe_instances(InstanceIds=sorted(instance_ids))
    return {
        str(instance['InstanceId']): (instance.get('State') or {}).get('Name')
        for reservation in response.get('Reservations', [])
        for instance in reservation.get('Instances', [])
        if isinstance(instance.get('InstanceId'), str)
    }


def _terminate_ec2_instances(
        ec2: Any,
        operation_id: str,
        instance_ids: list[str],
        *,
        settle_absence: bool,
        initial_unidentified_child_observed: bool = False) -> bool:
    known_ids = {
        instance_id for instance_id in instance_ids
        if isinstance(instance_id, str) and instance_id
    }
    unidentified_child_observed = (initial_unidentified_child_observed or any(
        not isinstance(instance_id, str) or not instance_id
        for instance_id in instance_ids))
    clean_observations_after_ambiguity = 0
    termination_requested: set[str] = set()
    cleanup_deadline = time.monotonic() + _EC2_TEARDOWN_SECONDS
    for attempt in range(_EC2_TEARDOWN_ATTEMPTS):
        if attempt > 0 and time.monotonic() >= cleanup_deadline:
            return False
        tagged = _tagged_instances(ec2, operation_id)
        unidentified_in_observation = _remember_instance_ids(tagged, known_ids)
        if unidentified_in_observation:
            unidentified_child_observed = True
            clean_observations_after_ambiguity = 0
        elif unidentified_child_observed:
            clean_observations_after_ambiguity += 1
        ambiguity_resolved = (not unidentified_child_observed or
                              clean_observations_after_ambiguity
                              >= _EC2_TEARDOWN_ATTEMPTS)
        terminal_ids = {
            str(instance['InstanceId'])
            for instance in tagged
            if isinstance(instance.get('InstanceId'), str) and
            instance.get('InstanceId') and
            (instance.get('State') or {}).get('Name') == 'terminated'
        }
        to_terminate = sorted(known_ids - termination_requested - terminal_ids)
        if to_terminate:
            ec2.terminate_instances(InstanceIds=to_terminate)
            termination_requested.update(to_terminate)
        all_known_terminated = False
        if known_ids:
            states = _instance_states(ec2, known_ids)
            all_known_terminated = (set(states) == known_ids and all(
                state == 'terminated' for state in states.values()))
            if (all_known_terminated and not settle_absence and
                    ambiguity_resolved):
                return True
        if not known_ids and not settle_absence and ambiguity_resolved:
            return True
        if (attempt == _EC2_TEARDOWN_ATTEMPTS - 1 or
                time.monotonic() >= cleanup_deadline):
            # A full provider-settle window with repeated exact tag absence is
            # the only safe no-child conclusion after an ambiguous launch. If
            # any child appeared, the final exact-state read must cover every
            # retained ID and prove all of them terminated. A child appearing
            # too late remains successor-owned cleanup instead of being
            # terminalized as an ordinary qualification failure.
            return (ambiguity_resolved and
                    (not known_ids or all_known_terminated))
        time.sleep(
            min(_EC2_TEARDOWN_POLL_SECONDS,
                max(0.0, cleanup_deadline - time.monotonic())))
    return False


def _ec2_user_data(reference: str, nonce: str, timeout_seconds: int) -> str:
    marker = f'SKYPILOT_IMAGE_CANARY_SUCCESS:{nonce}'
    return '\n'.join((
        '#!/usr/bin/env bash',
        'set -euo pipefail',
        f'timeout {timeout_seconds} docker pull {shlex.quote(reference)}',
        f'docker image inspect {shlex.quote(reference)} >/dev/null',
        f'echo {shlex.quote(marker)} >/dev/console',
        'shutdown -h now',
    ))


def _run_ec2_canary(
        operation: catalog_state.OperationRecord,
        payload: dict[str, Any],
        revision: topology_state.ProfileRevisionRecord,
        profile: models.ManagedRegistryProfile,
        target: models.ManagedRegistryTarget,
        binding: models.RegistryAccessBinding,
        digest: str,
        reference: str,
        heartbeat: worker_lease.LeaseHeartbeat,
        *,
        drain_event: threading.Event | None = None) -> dict[str, Any]:
    del revision
    if operation.child_launch_id is None:
        _raise_if_draining(drain_event)
    if (binding.kind
            != models.RegistryAccessBindingKind.AWS_EC2_INSTANCE_IDENTITY or
            binding.instance_profile is None or
            binding.canary_instance_type is None):
        raise _preflight_error(operation, 'QUALIFICATION_FAILED')
    try:
        expected_profile_arn = models.ec2_instance_profile_arn(binding)
    except ValueError as error:
        raise _preflight_error(operation, 'QUALIFICATION_FAILED') from error
    try:
        role = _canary_role(profile, binding)
    except Exception as error:  # pylint: disable=broad-except
        if operation.child_launch_id is not None:
            raise ValueError('CANARY_TEARDOWN_FAILED') from error
        raise
    child_id = f'ec2:{target.region}:{operation.id}'
    persisted_child = operation.child_launch_id is not None
    if not persisted_child:
        _raise_if_draining(drain_event)
    _attach_canary_child(operation, child_id, heartbeat)
    deadline: float | None = None
    ec2: _FencedClient | None = None
    instances: list[dict[str, Any]] = []
    known_instance_ids: set[str] = set()
    unidentified_tagged_child_observed = False
    instance_id: str | None = None
    launch_attempted = False
    launch_confirmed = False
    marker = f'SKYPILOT_IMAGE_CANARY_SUCCESS:{payload["nonce"]}'
    success = False
    instance_image_id: str | None = None
    instance_architecture: str | None = None
    actual_profile_arn: str | None = None
    actual_role: str | None = None
    teardown_verified = False

    def mark_launch_attempted() -> None:
        nonlocal launch_attempted
        _raise_if_draining(drain_event)
        launch_attempted = True

    try:
        if not persisted_child:
            _raise_if_draining(drain_event)
        ec2 = _assumed_client(role, 'ec2', target.region, heartbeat)
        _raise_if_draining(drain_event)
        expected_tags = {
            'SkyPilotCanaryOperation': operation.id,
            'SkyPilotCatalog': catalog_state.get_catalog_authority_id(),
            'SkyPilotProfile': profile.name,
        }
        instances = _tagged_instances(ec2, operation.id)
        unidentified_tagged_child_observed |= _remember_instance_ids(
            instances, known_instance_ids)
        _raise_if_draining(drain_event)
        if len(instances) > 1:
            raise ValueError('CANARY_DUPLICATE_CHILD')
        if instances:
            instance_id = _instance_id(instances[0])
            known_instance_ids.add(instance_id)
            launch_confirmed = True
            deadline = _authorized_launch_deadline(operation, child_id,
                                                   heartbeat)
        iam = _assumed_client(role, 'iam', target.region, heartbeat)
        _raise_if_draining(drain_event)
        actual_role = _instance_profile_role(iam, binding.instance_profile)
        _raise_if_draining(drain_event)
        if actual_role != binding.principals[0]:
            raise ValueError('QUALIFIED_RUNTIME_PRINCIPAL_REQUIRED')
        if not instances:
            subnet_values = dict(binding.canary_subnets)[target.region]
            index = int(payload['nonce'][:8], 16) % len(subnet_values)
            tags = [{
                'Key': key,
                'Value': value,
            } for key, value in expected_tags.items()]
            kwargs: dict[str, Any] = {
                'ClientToken': _ec2_client_token(operation.id),
                'ImageId': dict(binding.qualified_node_images)[target.region],
                'InstanceType': binding.canary_instance_type,
                'IamInstanceProfile': {
                    'Name': binding.instance_profile
                },
                'SubnetId': subnet_values[index],
                'MinCount': 1,
                'MaxCount': 1,
                'InstanceInitiatedShutdownBehavior': 'terminate',
                'UserData': _ec2_user_data(reference, payload['nonce'],
                                           payload['timeout_seconds']),
                'TagSpecifications': [{
                    'ResourceType': resource_type,
                    'Tags': tags,
                } for resource_type in ('instance', 'volume',
                                        'network-interface')],
                'SecurityGroupIds': list(
                    dict(binding.canary_security_groups)[target.region]),
            }
            deadline = _authorized_launch_deadline(operation, child_id,
                                                   heartbeat)
            _raise_if_draining(drain_event)
            response: dict[str, Any] = {}
            try:
                response = ec2.call_before_deadline('run_instances', deadline,
                                                    mark_launch_attempted,
                                                    **kwargs)
            except _CanaryDrainRequested:
                raise
            except worker_lease.LeaseLostError:
                raise
            except ValueError:
                raise
            except Exception:  # pylint: disable=broad-except
                # The stable ClientToken turns one bounded retry and every
                # successor replay into readback of the same provider child.
                deadline = _authorized_launch_deadline(operation, child_id,
                                                       heartbeat)
                response = ec2.call_before_deadline('run_instances', deadline,
                                                    mark_launch_attempted,
                                                    **kwargs)
            _raise_if_draining(drain_event)
            launched = response.get('Instances', [])
            if len(launched) != 1:
                raise RuntimeError(
                    'EC2 canary launch returned no unique child.')
            instances = [launched[0]]
            # A provider response is not confirmation until it supplies one
            # concrete child identity. If validation fails, launch_attempted
            # remains the durable ambiguity signal and teardown must run the
            # full discovery-settling path.
            instance_id = _instance_id(instances[0])
            known_instance_ids.add(instance_id)
            launch_confirmed = True
        if instance_id is None:
            instance_id = _instance_id(instances[0])
        assert deadline is not None
        while time.monotonic() < deadline:
            heartbeat.assert_owned()
            _raise_if_draining(drain_event)
            matching_instances = _tagged_instances(ec2, operation.id)
            unidentified_tagged_child_observed |= _remember_instance_ids(
                matching_instances, known_instance_ids)
            _raise_if_draining(drain_event)
            if len(matching_instances) > 1:
                raise ValueError('CANARY_DUPLICATE_CHILD')
            if not matching_instances:
                raise RuntimeError('EC2 canary child disappeared.')
            if _instance_id(matching_instances[0]) != instance_id:
                raise ValueError('CANARY_DUPLICATE_CHILD')
            instance = _exact_canary_instance(ec2, instance_id, expected_tags)
            known_instance_ids.add(_instance_id(instance))
            _raise_if_draining(drain_event)
            instance_image_id = instance.get('ImageId')
            if instance_image_id != dict(
                    binding.qualified_node_images)[target.region]:
                raise ValueError('QUALIFICATION_FAILED')
            instance_architecture = instance.get('Architecture')
            if instance_architecture != 'x86_64':
                raise ValueError('QUALIFICATION_FAILED')
            actual_profile_arn = (instance.get('IamInstanceProfile') or
                                  {}).get('Arn')
            if actual_profile_arn != expected_profile_arn:
                raise ValueError('QUALIFIED_RUNTIME_PRINCIPAL_REQUIRED')
            state = (instance.get('State') or {}).get('Name')
            if state in ('stopped', 'terminated'):
                output = ec2.get_console_output(InstanceId=instance_id,
                                                Latest=True).get('Output')
                if isinstance(output, str):
                    decoded = base64.b64decode(output).decode(errors='replace')
                    success = marker in decoded
                break
            _wait_for_canary_poll(drain_event)
        else:
            raise ValueError('CANARY_TIMEOUT')
    finally:
        try:
            if (not persisted_child and not launch_attempted and not instances):
                teardown_verified = True
            elif ec2 is None:
                heartbeat.assert_owned()
                raise RuntimeError('EC2 canary teardown authority unavailable.')
            else:
                live_instances = _tagged_instances(ec2, operation.id)
                unidentified_tagged_child_observed |= _remember_instance_ids(
                    live_instances, known_instance_ids)
                teardown_verified = _terminate_ec2_instances(
                    ec2,
                    operation.id,
                    sorted(known_instance_ids),
                    settle_absence=(persisted_child or
                                    ((launch_attempted or bool(instances)) and
                                     not launch_confirmed)),
                    initial_unidentified_child_observed=(
                        unidentified_tagged_child_observed))
        except worker_lease.LeaseLostError:
            raise
        except Exception:  # pylint: disable=broad-except
            teardown_verified = False
        if not teardown_verified:
            raise ValueError('CANARY_TEARDOWN_FAILED')
    if not success:
        raise ValueError('CANARY_PULL_FAILED')
    assert instance_id is not None
    assert instance_image_id is not None
    assert actual_role is not None
    assert instance_architecture == 'x86_64'
    return {
        'status': 'READY',
        'target': target.name,
        'target_fingerprint': target.target_fingerprint,
        'backend': 'aws_vm',
        'platform': profile.qualification.canary_platform,
        'runtime_id': payload['runtime_id'],
        'binding_fingerprint': binding.fingerprint,
        'runtime_digest': digest,
        'host_image_id': instance_image_id,
        'instance_architecture': instance_architecture,
        'instance_profile_arn': actual_profile_arn,
        'actual_principal': actual_role,
        'child_instance_id': instance_id,
        'nonce_hash': hashlib.sha256(payload['nonce'].encode()).hexdigest(),
        'teardown_verified': teardown_verified,
    }


def _api_error_status(error: BaseException) -> int | None:
    status = getattr(error, 'status', None)
    return int(status) if isinstance(status, int) else None


def _qualified_eks_nodes(
    core: Any,
    role: aws.AwsRoleBinding,
    target: models.ManagedRegistryTarget,
    qualified: models.QualifiedKubernetesCluster,
    heartbeat: worker_lease.LeaseHeartbeat,
    *,
    drain_event: threading.Event | None = None,
) -> tuple[int, str]:
    """Proves the runtime role for the complete bounded selector set."""
    selector = ','.join(
        f'{key}={value}' for key, value in qualified.node_selector)
    response = core.list_node(label_selector=selector,
                              limit=_MAX_QUALIFIED_EKS_NODES + 1,
                              _request_timeout=kubernetes.API_TIMEOUT)
    _raise_if_draining(drain_event)
    continuation = getattr(getattr(response, 'metadata', None), '_continue',
                           None)
    nodes = [
        node for node in (getattr(response, 'items', None) or []) if getattr(
            getattr(node, 'spec', None), 'unschedulable', False) is not True
    ]
    if (not nodes or len(nodes) > _MAX_QUALIFIED_EKS_NODES or continuation):
        raise ValueError('QUALIFIED_KUBERNETES_NODE_ROLE_REQUIRED')
    provider_ids: list[str] = []
    node_uids: list[str] = []
    for node in nodes:
        metadata = getattr(node, 'metadata', None)
        spec = getattr(node, 'spec', None)
        uid = getattr(metadata, 'uid', None)
        provider_id = getattr(spec, 'provider_id', None)
        labels = getattr(metadata, 'labels', None) or {}
        if (not isinstance(uid, str) or not uid or
                not isinstance(provider_id, str) or '/' not in provider_id or
                any(
                    labels.get(key) != value
                    for key, value in qualified.node_selector)):
            raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
        node_uids.append(uid)
        provider_ids.append(provider_id.rsplit('/', 1)[-1])
    if len(set(provider_ids)) != len(provider_ids):
        raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
    ec2 = _assumed_client(role, 'ec2', target.region, heartbeat)
    _raise_if_draining(drain_event)
    iam = _assumed_client(role, 'iam', target.region, heartbeat)
    _raise_if_draining(drain_event)
    instances: list[dict[str, Any]] = []
    for offset in range(0, len(provider_ids), 100):
        result = ec2.describe_instances(InstanceIds=provider_ids[offset:offset +
                                                                 100])
        _raise_if_draining(drain_event)
        instances.extend(item for reservation in result.get('Reservations', [])
                         for item in reservation.get('Instances', []))
    if ({item.get('InstanceId') for item in instances} != set(provider_ids)):
        raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
    roles: set[str] = set()
    for instance in instances:
        profile_arn = (instance.get('IamInstanceProfile') or {}).get('Arn')
        if not isinstance(profile_arn, str) or '/' not in profile_arn:
            raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
        roles.add(_instance_profile_role(iam, profile_arn.rsplit('/', 1)[-1]))
        _raise_if_draining(drain_event)
    if roles != {qualified.node_role}:
        raise ValueError('QUALIFIED_KUBERNETES_NODE_ROLE_REQUIRED')
    node_set_hash = hashlib.sha256('\n'.join(
        sorted(node_uids)).encode()).hexdigest()
    return len(nodes), node_set_hash


def _delete_eks_pod(core: Any, pod_name: str, namespace: str, *,
                    settle_absence: bool) -> bool:
    """Deletes one deterministic pod and fences ambiguous late creation."""
    try:
        core.delete_namespaced_pod(pod_name,
                                   namespace,
                                   grace_period_seconds=0,
                                   propagation_policy='Background',
                                   _request_timeout=kubernetes.API_TIMEOUT)
    except worker_lease.LeaseLostError:
        raise
    except Exception as error:  # pylint: disable=broad-except
        if _api_error_status(error) != 404:
            return False
    cleanup_deadline = time.monotonic() + _EKS_TEARDOWN_SECONDS
    absence_started: float | None = None
    while time.monotonic() < cleanup_deadline:
        try:
            core.read_namespaced_pod(pod_name,
                                     namespace,
                                     _request_timeout=kubernetes.API_TIMEOUT)
        except worker_lease.LeaseLostError:
            raise
        except Exception as error:  # pylint: disable=broad-except
            if _api_error_status(error) != 404:
                absence_started = None
            elif not settle_absence:
                return True
            else:
                current = time.monotonic()
                if absence_started is None:
                    absence_started = current
                if current - absence_started >= _EKS_ABSENCE_SETTLE_SECONDS:
                    return True
        else:
            # A predecessor's timed-out create may become visible after the
            # initial delete. Delete that same deterministic name again.
            absence_started = None
            try:
                core.delete_namespaced_pod(
                    pod_name,
                    namespace,
                    grace_period_seconds=0,
                    propagation_policy='Background',
                    _request_timeout=kubernetes.API_TIMEOUT)
            except worker_lease.LeaseLostError:
                raise
            except Exception as error:  # pylint: disable=broad-except
                if _api_error_status(error) != 404:
                    return False
        time.sleep(_EKS_TEARDOWN_POLL_SECONDS)
    return False


def _run_eks_canary(
        operation: catalog_state.OperationRecord,
        payload: dict[str, Any],
        revision: topology_state.ProfileRevisionRecord,
        profile: models.ManagedRegistryProfile,
        target: models.ManagedRegistryTarget,
        binding: models.RegistryAccessBinding,
        digest: str,
        reference: str,
        heartbeat: worker_lease.LeaseHeartbeat,
        *,
        drain_event: threading.Event | None = None) -> dict[str, Any]:
    del revision
    if operation.child_launch_id is None:
        _raise_if_draining(drain_event)
    if binding.kind != models.RegistryAccessBindingKind.AWS_EKS_KUBELET_IDENTITY:
        raise _preflight_error(operation, 'QUALIFICATION_FAILED')
    try:
        qualified = models.qualified_eks_cluster_for_target(
            target, binding, payload['runtime_id'])
    except ValueError as error:
        raise _preflight_error(
            operation, 'QUALIFIED_KUBERNETES_NODE_ROLE_REQUIRED') from error
    context = qualified.context
    cluster_arn = qualified.cluster_arn
    namespace = qualified.namespace
    try:
        role = _canary_role(profile, binding)
    except Exception as error:  # pylint: disable=broad-except
        if operation.child_launch_id is not None:
            raise ValueError('CANARY_TEARDOWN_FAILED') from error
        raise
    if ':cluster/' not in cluster_arn:
        raise _preflight_error(operation, 'CANARY_PRINCIPAL_UNVERIFIED')
    cluster_name = cluster_arn.rsplit(':cluster/', 1)[1]
    pod_name = f'sky-img-canary-{operation.id.replace("-", "")[:20]}'
    child_id = f'eks:{context}:{namespace}:{pod_name}'
    persisted_child = operation.child_launch_id is not None
    if not persisted_child:
        _raise_if_draining(drain_event)
    _attach_canary_child(operation, child_id, heartbeat)
    core: _FencedClient | None = None
    create_attempted = False
    create_confirmed = False
    deadline: float | None = None
    body = {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'name': pod_name,
            'namespace': namespace,
            'labels': {
                'skypilot.co/image-canary-operation': operation.id,
            },
        },
        'spec': {
            'restartPolicy': 'Never',
            'nodeSelector': dict(qualified.node_selector),
            'containers': [{
                'name': 'canary',
                'image': reference,
                'imagePullPolicy': 'Always',
                'command': ['/bin/sh', '-c'],
                'args': [f'echo {shlex.quote(payload["nonce"])}'],
                'resources': {
                    'requests': {
                        'cpu': '10m',
                        'memory': '16Mi',
                    },
                    'limits': {
                        'cpu': '100m',
                        'memory': '64Mi',
                    },
                },
            }],
        },
    }
    evidence: dict[str, Any] | None = None
    teardown_verified = False

    def mark_create_attempted() -> None:
        nonlocal create_attempted
        _raise_if_draining(drain_event)
        create_attempted = True

    try:
        if not persisted_child:
            _raise_if_draining(drain_event)
        core = _kubernetes_core(context, heartbeat)
        _raise_if_draining(drain_event)
        deadline = _authorized_launch_deadline(operation, child_id, heartbeat)
        _raise_if_draining(drain_event)
        eks = _assumed_client(role, 'eks', target.region, heartbeat)
        _raise_if_draining(drain_event)
        actual_cluster = eks.describe_cluster(name=cluster_name).get(
            'cluster', {})
        _raise_if_draining(drain_event)
        endpoint = actual_cluster.get('endpoint')
        configured_endpoint = getattr(
            getattr(getattr(core, 'api_client', None), 'configuration', None),
            'host', None)
        if (actual_cluster.get('arn') != cluster_arn or
                not isinstance(endpoint, str) or
                not isinstance(configured_endpoint, str) or
                endpoint.rstrip('/') != configured_endpoint.rstrip('/')):
            raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
        node_count, node_set_hash = _qualified_eks_nodes(
            core, role, target, qualified, heartbeat, drain_event=drain_event)
        deadline = _authorized_launch_deadline(operation, child_id, heartbeat)
        _raise_if_draining(drain_event)
        try:
            core.call_before_deadline('create_namespaced_pod',
                                      deadline,
                                      mark_create_attempted,
                                      namespace,
                                      body,
                                      _request_timeout=kubernetes.API_TIMEOUT)
            create_confirmed = True
            _raise_if_draining(drain_event)
        except worker_lease.LeaseLostError:
            raise
        except Exception as error:  # pylint: disable=broad-except
            if _api_error_status(error) != 409:
                raise
            create_confirmed = True
        pod = None
        while time.monotonic() < deadline:
            heartbeat.assert_owned()
            _raise_if_draining(drain_event)
            pod = core.read_namespaced_pod(
                pod_name, namespace, _request_timeout=kubernetes.API_TIMEOUT)
            _raise_if_draining(drain_event)
            labels = getattr(getattr(pod, 'metadata', None), 'labels', {}) or {}
            containers = getattr(getattr(pod, 'spec', None), 'containers', [])
            if (labels.get('skypilot.co/image-canary-operation') != operation.id
                    or len(containers) != 1 or
                    getattr(containers[0], 'image', None) != reference):
                raise ValueError('CANARY_DUPLICATE_CHILD')
            phase = getattr(getattr(pod, 'status', None), 'phase', None)
            if phase == 'Succeeded':
                break
            if phase == 'Failed':
                raise ValueError('CANARY_PULL_FAILED')
            _wait_for_canary_poll(drain_event)
        else:
            raise ValueError('CANARY_TIMEOUT')
        logs = core.read_namespaced_pod_log(
            pod_name, namespace, _request_timeout=kubernetes.API_TIMEOUT)
        _raise_if_draining(drain_event)
        if not isinstance(logs, str) or logs.strip() != payload['nonce']:
            raise ValueError('CANARY_PULL_FAILED')
        node_name = getattr(getattr(pod, 'spec', None), 'node_name', None)
        if not isinstance(node_name, str) or not node_name:
            raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
        node = core.read_node(node_name,
                              _request_timeout=kubernetes.API_TIMEOUT)
        _raise_if_draining(drain_event)
        node_uid = getattr(getattr(node, 'metadata', None), 'uid', None)
        provider_id = getattr(getattr(node, 'spec', None), 'provider_id', None)
        if (not isinstance(node_uid, str) or not isinstance(provider_id, str) or
                '/' not in provider_id):
            raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
        evidence = {
            'status': 'READY',
            'target': target.name,
            'target_fingerprint': target.target_fingerprint,
            'backend': 'aws_eks',
            'platform': profile.qualification.canary_platform,
            'runtime_id': payload['runtime_id'],
            'binding_fingerprint': binding.fingerprint,
            'runtime_digest': digest,
            'context': context,
            'cluster_arn': cluster_arn,
            'node_role': qualified.node_role,
            'node_selector': dict(qualified.node_selector),
            'qualified_node_count': node_count,
            'qualified_node_set_hash': node_set_hash,
            'node_uid': node_uid,
            'nonce_hash': hashlib.sha256(payload['nonce'].encode()).hexdigest(),
        }
    finally:
        if not persisted_child and not create_attempted:
            teardown_verified = True
        elif core is None:
            heartbeat.assert_owned()
            raise ValueError('CANARY_TEARDOWN_FAILED')
        else:
            teardown_verified = _delete_eks_pod(
                core,
                pod_name,
                namespace,
                settle_absence=(persisted_child or
                                (create_attempted and not create_confirmed)))
        if not teardown_verified:
            raise ValueError('CANARY_TEARDOWN_FAILED')
    if evidence is None:
        raise ValueError('CANARY_FAILED')
    evidence['teardown_verified'] = True
    return evidence


def _fail_owned_canary(operation: catalog_state.OperationRecord, code: str,
                       heartbeat: worker_lease.LeaseHeartbeat) -> None:
    heartbeat.assert_owned()
    qualification.fail_owned_canary(operation, code, teardown_verified=True)


def run_canary(operation: catalog_state.OperationRecord,
               *,
               lease_seconds: int = _DEFAULT_LEASE_SECONDS,
               drain_event: threading.Event | None = None) -> bool:
    """Runs or resumes one provider child under continuous lease ownership."""
    if operation.child_launch_id is None:
        try:
            _raise_if_draining(drain_event)
        except _CanaryDrainRequested:
            qualification.release_drained_canary(operation,
                                                 teardown_verified=True)
            return False
    token = operation.lease_token
    if token is None:
        return False
    heartbeat = _LeaseHeartbeat(
        lambda: qualification.heartbeat_canary(operation.id, token,
                                               lease_seconds),
        max(1.0, lease_seconds / 3))
    try:
        with heartbeat:
            try:
                try:
                    (payload, revision, profile, target, binding, digest,
                     reference) = _load_contract(operation)
                except Exception:  # pylint: disable=broad-except
                    if operation.child_launch_id is not None:
                        # The immutable child contract is required to discover
                        # and tear down its provider resource safely.
                        return False
                    raise
                if operation.child_launch_id is None:
                    _raise_if_draining(drain_event)
                if payload['backend'] == 'aws_vm':
                    evidence = _run_ec2_canary(operation,
                                               payload,
                                               revision,
                                               profile,
                                               target,
                                               binding,
                                               digest,
                                               reference,
                                               heartbeat,
                                               drain_event=drain_event)
                elif payload['backend'] == 'aws_eks':
                    evidence = _run_eks_canary(operation,
                                               payload,
                                               revision,
                                               profile,
                                               target,
                                               binding,
                                               digest,
                                               reference,
                                               heartbeat,
                                               drain_event=drain_event)
                else:
                    if operation.child_launch_id is not None:
                        return False
                    raise ValueError('IMAGE_LOCALITY_UNSUPPORTED')
                heartbeat.assert_owned()
                if qualification.complete_canary(operation, evidence):
                    return True
                _fail_owned_canary(operation, 'CANARY_FAILED', heartbeat)
                return False
            except _CanaryDrainRequested:
                qualification.release_drained_canary(operation,
                                                     teardown_verified=True)
                return False
            except worker_lease.LeaseLostError:
                return False
            except ValueError as error:
                code = str(error)
                if code not in _CANARY_ERROR_CODES:
                    code = 'CANARY_FAILED'
                if code == 'CANARY_TEARDOWN_FAILED':
                    # Preserve the deterministic child for successor teardown.
                    # Terminalizing here would discard its only durable owner.
                    return False
                _fail_owned_canary(operation, code, heartbeat)
                return False
            except Exception:  # pylint: disable=broad-except
                _fail_owned_canary(operation, 'CANARY_FAILED', heartbeat)
                return False
    except worker_lease.LeaseLostError:
        return False


class CanaryWorkerService:
    """Bounded canary claim loop, isolated from ECR copy/delete identities."""

    def __init__(self,
                 *,
                 worker_id: str,
                 version: str,
                 max_in_flight: int,
                 lease_seconds: int = _DEFAULT_LEASE_SECONDS,
                 health: worker_health.WorkerHealth | None = None) -> None:
        self.worker_id = worker_id
        self.version = version
        self.max_in_flight = max_in_flight
        self.lease_seconds = lease_seconds
        self._stop = threading.Event()
        self._health = health

    def stop(self) -> None:
        self._stop.set()

    def run_forever(self) -> None:
        topology_state.register_worker(self.worker_id,
                                       models.ImageWorkerKind.CANARY,
                                       self.version, self.max_in_flight)
        if self._health is not None:
            self._health.registered()
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_in_flight,
                thread_name_prefix='image-canary') as executor:
            futures: set[concurrent.futures.Future[bool]] = set()
            while not self._stop.is_set():
                if self._health is not None:
                    self._health.tick(len(futures))
                done = {future for future in futures if future.done()}
                for future in done:
                    with contextlib.suppress(Exception):
                        future.result()
                futures -= done
                heartbeat_ok = topology_state.heartbeat_worker(
                    self.worker_id, in_flight=len(futures), success=bool(done))
                if self._health is not None:
                    self._health.heartbeat(heartbeat_ok)
                if self._stop.is_set():
                    break
                while (len(futures) < self.max_in_flight and
                       not self._stop.is_set()):
                    operation = qualification.claim_canary(
                        worker_id=self.worker_id,
                        lease_seconds=self.lease_seconds)
                    if operation is None:
                        break
                    if self._stop.is_set():
                        with contextlib.suppress(Exception):
                            qualification.release_drained_canary(
                                operation, teardown_verified=True)
                        break
                    futures.add(
                        executor.submit(run_canary,
                                        operation,
                                        lease_seconds=self.lease_seconds,
                                        drain_event=self._stop))
                self._stop.wait(1 if futures else 5)


def main() -> None:
    max_in_flight = int(os.environ.get('SKYPILOT_IMAGE_MAX_IN_FLIGHT', '4'))
    if max_in_flight <= 0:
        raise ValueError('SKYPILOT_IMAGE_MAX_IN_FLIGHT must be positive.')
    # Do not launch paid runtime canaries while any central schema is stale.
    database_migrations.initialize_central_databases()
    health = worker_health.WorkerHealth(
        'canary',
        liveness_deadline_seconds=int(
            os.environ.get('SKYPILOT_IMAGE_LIVENESS_DEADLINE_SECONDS', '30')))
    health_server = worker_health.HealthServer(
        health, int(os.environ.get('SKYPILOT_IMAGE_HEALTH_PORT', '8081')))
    service = CanaryWorkerService(
        worker_id=os.environ.get('SKYPILOT_IMAGE_WORKER_ID', str(uuid.uuid4())),
        version=os.environ.get('SKYPILOT_IMAGE_WORKER_VERSION', 'dev'),
        max_in_flight=max_in_flight,
        lease_seconds=int(
            os.environ.get('SKYPILOT_IMAGE_LEASE_SECONDS',
                           str(_DEFAULT_LEASE_SECONDS))),
        health=health)
    signal.signal(signal.SIGTERM, lambda *_: service.stop())
    signal.signal(signal.SIGINT, lambda *_: service.stop())
    health_server.start()
    try:
        service.run_forever()
    finally:
        health_server.stop()


if __name__ == '__main__':
    main()
