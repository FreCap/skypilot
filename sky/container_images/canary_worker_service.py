"""Separately permissioned EC2 and EKS runtime-pull canary worker."""

from __future__ import annotations

import base64
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

_DEFAULT_LEASE_SECONDS = 15 * 60
_POLL_SECONDS = 10
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
        catalog_tag=catalog_state.get_catalog_authority_id() or 'unknown',
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


def _heartbeat(operation: catalog_state.OperationRecord,
               lease_seconds: int) -> None:
    assert operation.lease_token is not None
    if not qualification.heartbeat_canary(operation.id, operation.lease_token,
                                          lease_seconds):
        raise RuntimeError('Canary operation lease was lost.')


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
        'Values': ['pending', 'running', 'stopping', 'stopped'],
    }])
    return [
        instance for reservation in response.get('Reservations', [])
        for instance in reservation.get('Instances', [])
    ]


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


def _run_ec2_canary(operation: catalog_state.OperationRecord,
                    payload: dict[str, Any],
                    revision: topology_state.ProfileRevisionRecord,
                    profile: models.ManagedRegistryProfile,
                    target: models.ManagedRegistryTarget,
                    binding: models.RegistryAccessBinding, digest: str,
                    reference: str, lease_seconds: int) -> dict[str, Any]:
    del revision
    if (binding.kind
            != models.RegistryAccessBindingKind.AWS_EC2_INSTANCE_IDENTITY or
            binding.instance_profile is None or
            binding.canary_instance_type is None):
        raise ValueError('QUALIFICATION_FAILED')
    role = _canary_role(profile, binding)
    ec2 = aws.assumed_client(role, 'ec2', target.region)
    iam = aws.assumed_client(role, 'iam', target.region)
    child_id = f'ec2:{target.region}:{operation.id}'
    assert operation.lease_token is not None
    if not qualification.attach_canary_child(operation.id,
                                             operation.lease_token, child_id):
        raise RuntimeError('Canary operation lease was lost.')
    instances = _tagged_instances(ec2, operation.id)
    if len(instances) > 1:
        ec2.terminate_instances(
            InstanceIds=[str(item['InstanceId']) for item in instances])
        raise ValueError('CANARY_DUPLICATE_CHILD')
    deadline = operation.teardown_deadline or int(time.time())
    if not instances:
        if int(time.time()) >= deadline:
            raise ValueError('CANARY_TIMEOUT')
        subnet_values = dict(binding.canary_subnets)[target.region]
        index = int(payload['nonce'][:8], 16) % len(subnet_values)
        kwargs: dict[str, Any] = {
            'ImageId': dict(binding.qualified_node_images)[target.region],
            'InstanceType': binding.canary_instance_type,
            'IamInstanceProfile': {
                'Name': binding.instance_profile
            },
            'SubnetId': subnet_values[index],
            'MinCount': 1,
            'MaxCount': 1,
            'UserData': _ec2_user_data(reference, payload['nonce'],
                                       payload['timeout_seconds']),
            'TagSpecifications': [{
                'ResourceType': 'instance',
                'Tags': [{
                    'Key': 'SkyPilotCanaryOperation',
                    'Value': operation.id,
                }, {
                    'Key': 'SkyPilotCatalog',
                    'Value': catalog_state.get_catalog_authority_id()
                             or 'unknown',
                }, {
                    'Key': 'SkyPilotProfile',
                    'Value': profile.name,
                }],
            }],
        }
        security_groups = dict(binding.canary_security_groups).get(
            target.region, ())
        if security_groups:
            kwargs['SecurityGroupIds'] = list(security_groups)
        response = ec2.run_instances(**kwargs)
        launched = response.get('Instances', [])
        if len(launched) != 1:
            raise RuntimeError('EC2 canary launch returned no unique child.')
        instances = [launched[0]]
    instance_id = str(instances[0]['InstanceId'])
    marker = f'SKYPILOT_IMAGE_CANARY_SUCCESS:{payload["nonce"]}'
    success = False
    actual_profile_arn: str | None = None
    teardown_verified = False
    try:
        while int(time.time()) < deadline:
            _heartbeat(operation, lease_seconds)
            matching_instances = _tagged_instances(ec2, operation.id)
            if len(matching_instances) != 1:
                raise RuntimeError('EC2 canary child disappeared.')
            instance = matching_instances[0]
            actual_profile_arn = (instance.get('IamInstanceProfile') or
                                  {}).get('Arn')
            if (not isinstance(actual_profile_arn, str) or
                    not actual_profile_arn.endswith('/' +
                                                    binding.instance_profile)):
                raise ValueError('QUALIFIED_RUNTIME_PRINCIPAL_REQUIRED')
            state = (instance.get('State') or {}).get('Name')
            if state == 'stopped':
                output = ec2.get_console_output(InstanceId=instance_id,
                                                Latest=True).get('Output')
                if isinstance(output, str):
                    decoded = base64.b64decode(output).decode(errors='replace')
                    success = marker in decoded
                break
            time.sleep(_POLL_SECONDS)
    finally:
        try:
            ec2.terminate_instances(InstanceIds=[instance_id])
            waiter = ec2.get_waiter('instance_terminated')
            waiter.wait(InstanceIds=[instance_id],
                        WaiterConfig={
                            'Delay': 5,
                            'MaxAttempts': 60
                        })
            teardown_verified = True
        except Exception:  # pylint: disable=broad-except
            teardown_verified = False
        if not teardown_verified:
            raise ValueError('CANARY_TEARDOWN_FAILED')
    if not success:
        raise ValueError('CANARY_PULL_FAILED')
    actual_role = _instance_profile_role(iam, binding.instance_profile)
    if actual_role != binding.principals[0]:
        raise ValueError('QUALIFIED_RUNTIME_PRINCIPAL_REQUIRED')
    return {
        'status': 'READY',
        'observed_at': int(time.time()),
        'target': target.name,
        'target_fingerprint': target.target_fingerprint,
        'backend': 'aws_vm',
        'runtime_id': payload['runtime_id'],
        'binding_fingerprint': binding.fingerprint,
        'runtime_digest': digest,
        'host_image_id': dict(binding.qualified_node_images)[target.region],
        'instance_profile_arn': actual_profile_arn,
        'actual_principal': actual_role,
        'child_instance_id': instance_id,
        'nonce_hash': hashlib.sha256(payload['nonce'].encode()).hexdigest(),
        'teardown_verified': teardown_verified,
    }


def _api_error_status(error: BaseException) -> int | None:
    status = getattr(error, 'status', None)
    return int(status) if isinstance(status, int) else None


def _run_eks_canary(operation: catalog_state.OperationRecord,
                    payload: dict[str, Any],
                    revision: topology_state.ProfileRevisionRecord,
                    profile: models.ManagedRegistryProfile,
                    target: models.ManagedRegistryTarget,
                    binding: models.RegistryAccessBinding, digest: str,
                    reference: str, lease_seconds: int) -> dict[str, Any]:
    del revision
    if binding.kind != models.RegistryAccessBindingKind.AWS_EKS_KUBELET_IDENTITY:
        raise ValueError('QUALIFICATION_FAILED')
    qualified = next(
        (item for item in binding.qualified_clusters
         if item[0] == payload['runtime_id'] and f':{target.region}:' in item[1]
        ), None)
    if qualified is None:
        raise ValueError('QUALIFIED_KUBERNETES_NODE_ROLE_REQUIRED')
    context, cluster_arn, expected_node_role, namespace = qualified
    pod_name = f'sky-img-canary-{operation.id.replace("-", "")[:20]}'
    child_id = f'eks:{context}:{namespace}:{pod_name}'
    assert operation.lease_token is not None
    if not qualification.attach_canary_child(operation.id,
                                             operation.lease_token, child_id):
        raise RuntimeError('Canary operation lease was lost.')
    core = kubernetes.core_api(context)
    role = _canary_role(profile, binding)
    if ':cluster/' not in cluster_arn:
        raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
    cluster_name = cluster_arn.rsplit(':cluster/', 1)[1]
    eks = aws.assumed_client(role, 'eks', target.region)
    actual_cluster = eks.describe_cluster(name=cluster_name).get('cluster', {})
    endpoint = actual_cluster.get('endpoint')
    configured_endpoint = getattr(
        getattr(getattr(core, 'api_client', None), 'configuration', None),
        'host', None)
    if (actual_cluster.get('arn') != cluster_arn or
            not isinstance(endpoint, str) or
            not isinstance(configured_endpoint, str) or
            endpoint.rstrip('/') != configured_endpoint.rstrip('/')):
        raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
    deadline = operation.teardown_deadline or int(time.time())
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
    try:
        if int(time.time()) >= deadline:
            raise ValueError('CANARY_TIMEOUT')
        try:
            core.create_namespaced_pod(namespace,
                                       body,
                                       _request_timeout=kubernetes.API_TIMEOUT)
        except Exception as error:  # pylint: disable=broad-except
            if _api_error_status(error) != 409:
                raise
        pod = None
        while int(time.time()) < deadline:
            _heartbeat(operation, lease_seconds)
            pod = core.read_namespaced_pod(
                pod_name, namespace, _request_timeout=kubernetes.API_TIMEOUT)
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
            time.sleep(_POLL_SECONDS)
        else:
            raise ValueError('CANARY_TIMEOUT')
        logs = core.read_namespaced_pod_log(
            pod_name, namespace, _request_timeout=kubernetes.API_TIMEOUT)
        if not isinstance(logs, str) or logs.strip() != payload['nonce']:
            raise ValueError('CANARY_PULL_FAILED')
        node_name = getattr(getattr(pod, 'spec', None), 'node_name', None)
        if not isinstance(node_name, str) or not node_name:
            raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
        node = core.read_node(node_name,
                              _request_timeout=kubernetes.API_TIMEOUT)
        node_uid = getattr(getattr(node, 'metadata', None), 'uid', None)
        provider_id = getattr(getattr(node, 'spec', None), 'provider_id', None)
        if (not isinstance(node_uid, str) or not isinstance(provider_id, str) or
                '/' not in provider_id):
            raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
        instance_id = provider_id.rsplit('/', 1)[-1]
        ec2 = aws.assumed_client(role, 'ec2', target.region)
        iam = aws.assumed_client(role, 'iam', target.region)
        response = ec2.describe_instances(InstanceIds=[instance_id])
        instances = [
            item for reservation in response.get('Reservations', [])
            for item in reservation.get('Instances', [])
        ]
        if len(instances) != 1:
            raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
        profile_arn = (instances[0].get('IamInstanceProfile') or {}).get('Arn')
        if not isinstance(profile_arn, str) or '/' not in profile_arn:
            raise ValueError('CANARY_PRINCIPAL_UNVERIFIED')
        instance_profile = profile_arn.rsplit('/', 1)[-1]
        actual_role = _instance_profile_role(iam, instance_profile)
        if actual_role != expected_node_role:
            raise ValueError('QUALIFIED_KUBERNETES_NODE_ROLE_REQUIRED')
        evidence = {
            'status': 'READY',
            'observed_at': int(time.time()),
            'target': target.name,
            'target_fingerprint': target.target_fingerprint,
            'backend': 'aws_eks',
            'runtime_id': payload['runtime_id'],
            'binding_fingerprint': binding.fingerprint,
            'runtime_digest': digest,
            'context': context,
            'cluster_arn': cluster_arn,
            'node_uid': node_uid,
            'node_instance_id': instance_id,
            'actual_principal': actual_role,
            'nonce_hash': hashlib.sha256(payload['nonce'].encode()).hexdigest(),
        }
    finally:
        try:
            core.delete_namespaced_pod(pod_name,
                                       namespace,
                                       grace_period_seconds=0,
                                       propagation_policy='Background',
                                       _request_timeout=kubernetes.API_TIMEOUT)
        except Exception as error:  # pylint: disable=broad-except
            if _api_error_status(error) != 404:
                teardown_verified = False
        cleanup_deadline = time.time() + 60
        while time.time() < cleanup_deadline:
            try:
                core.read_namespaced_pod(
                    pod_name,
                    namespace,
                    _request_timeout=kubernetes.API_TIMEOUT)
            except Exception as error:  # pylint: disable=broad-except
                if _api_error_status(error) == 404:
                    teardown_verified = True
                    break
            time.sleep(2)
        if not teardown_verified:
            raise ValueError('CANARY_TEARDOWN_FAILED')
    if evidence is None:
        raise ValueError('CANARY_FAILED')
    evidence['teardown_verified'] = True
    return evidence


def run_canary(operation: catalog_state.OperationRecord,
               *,
               lease_seconds: int = _DEFAULT_LEASE_SECONDS) -> bool:
    """Runs or resumes one provider child and always attempts teardown."""
    try:
        (payload, revision, profile, target, binding, digest,
         reference) = _load_contract(operation)
        if payload['backend'] == 'aws_vm':
            evidence = _run_ec2_canary(operation, payload, revision, profile,
                                       target, binding, digest, reference,
                                       lease_seconds)
        elif payload['backend'] == 'aws_eks':
            evidence = _run_eks_canary(operation, payload, revision, profile,
                                       target, binding, digest, reference,
                                       lease_seconds)
        else:
            raise ValueError('IMAGE_LOCALITY_UNSUPPORTED')
        return qualification.complete_canary(operation, evidence)
    except ValueError as error:
        code = str(error)
        if code not in _CANARY_ERROR_CODES:
            code = 'CANARY_FAILED'
        qualification.fail_canary(operation, code)
        return False
    except Exception:  # pylint: disable=broad-except
        qualification.fail_canary(operation, 'CANARY_FAILED')
        return False


class CanaryWorkerService:
    """Bounded canary claim loop, isolated from ECR copy/delete identities."""

    def __init__(self,
                 *,
                 worker_id: str,
                 version: str,
                 max_in_flight: int,
                 lease_seconds: int = _DEFAULT_LEASE_SECONDS) -> None:
        self.worker_id = worker_id
        self.version = version
        self.max_in_flight = max_in_flight
        self.lease_seconds = lease_seconds
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run_forever(self) -> None:
        topology_state.register_worker(self.worker_id,
                                       models.ImageWorkerKind.CANARY,
                                       self.version, self.max_in_flight)
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_in_flight,
                thread_name_prefix='image-canary') as executor:
            futures: set[concurrent.futures.Future[bool]] = set()
            while not self._stop.is_set():
                done = {future for future in futures if future.done()}
                for future in done:
                    with contextlib.suppress(Exception):
                        future.result()
                futures -= done
                topology_state.heartbeat_worker(self.worker_id,
                                                in_flight=len(futures),
                                                success=bool(done))
                while len(futures) < self.max_in_flight:
                    operation = qualification.claim_canary(
                        worker_id=self.worker_id,
                        lease_seconds=self.lease_seconds)
                    if operation is None:
                        break
                    futures.add(
                        executor.submit(run_canary,
                                        operation,
                                        lease_seconds=self.lease_seconds))
                self._stop.wait(1 if futures else 5)


def main() -> None:
    max_in_flight = int(os.environ.get('SKYPILOT_IMAGE_MAX_IN_FLIGHT', '4'))
    if max_in_flight <= 0:
        raise ValueError('SKYPILOT_IMAGE_MAX_IN_FLIGHT must be positive.')
    service = CanaryWorkerService(
        worker_id=os.environ.get('SKYPILOT_IMAGE_WORKER_ID', str(uuid.uuid4())),
        version=os.environ.get('SKYPILOT_IMAGE_WORKER_VERSION', 'dev'),
        max_in_flight=max_in_flight,
        lease_seconds=int(
            os.environ.get('SKYPILOT_IMAGE_LEASE_SECONDS',
                           str(_DEFAULT_LEASE_SECONDS))))
    signal.signal(signal.SIGTERM, lambda *_: service.stop())
    signal.signal(signal.SIGINT, lambda *_: service.stop())
    service.run_forever()


if __name__ == '__main__':
    main()
