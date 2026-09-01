"""Common data structures for provisioning"""
import abc
from collections.abc import Callable
import contextlib
import dataclasses
import datetime
import enum
import functools
import hashlib
import json
import os
import re
from typing import Any, Protocol
import uuid

from sky import sky_logging
from sky.utils import config_utils
from sky.utils import env_options
from sky.utils import resources_utils

# NOTE: we can use pydantic instead of dataclasses or namedtuples, because
# pydantic provides more features like validation or parsing from
# nested dictionaries. This makes our API more extensible and easier
# to integrate with other frameworks like FastAPI etc.

# -------------------- input data model -------------------- #

InstanceId = str
_START_TITLE = '\n' + '-' * 20 + 'Start: {} ' + '-' * 20
_END_TITLE = '-' * 20 + 'End:   {} ' + '-' * 20 + '\n'

logger = sky_logging.init_logger(__name__)

ProviderEffectGuardFactory = Callable[[],
                                      contextlib.AbstractContextManager[None]]


class KueuePodAdmissionState(enum.Enum):
    """Pod-only Kueue admission facts reported by a provisioner."""

    POD_WAITING = 'POD_WAITING'
    POLICY_ADMITTED = 'POLICY_ADMITTED'


@dataclasses.dataclass(frozen=True)
class KueuePodAdmissionIdentity:
    """Dynamic server identity stamped on one reserved-fill Pod.

    These values are deliberately runtime-only.  They identify one durable
    fill intent, while the static worker projection remains independently
    content-addressed by ``worker_projection_sha256``.
    """

    intent_key: str
    replica_record_uuid: str
    pool_physical_uid: str
    worker_projection_sha256: str

    def __post_init__(self) -> None:
        for field_name in ('intent_key', 'worker_projection_sha256'):
            value = getattr(self, field_name)
            if (not isinstance(value, str) or
                    re.fullmatch(r'[0-9a-f]{64}', value) is None):
                raise ValueError(
                    f'{field_name} must be 64 lowercase hexadecimal characters.'
                )
        try:
            record_uuid = uuid.UUID(self.replica_record_uuid)
        except (AttributeError, TypeError, ValueError) as error:
            raise ValueError('replica_record_uuid must be a canonical UUID.') \
                from error
        if str(record_uuid) != self.replica_record_uuid:
            raise ValueError('replica_record_uuid must be a canonical UUID.')
        if (not isinstance(self.pool_physical_uid, str) or
                not self.pool_physical_uid):
            raise ValueError('pool_physical_uid must be a nonempty string.')


@dataclasses.dataclass(frozen=True)
class KueuePersistedPodIdentity:
    """Exact Pod identity already bound by a durable admission receipt."""

    namespace: str
    pod_name: str
    pod_uid: str

    def __post_init__(self) -> None:
        for field_name in ('namespace', 'pod_name', 'pod_uid'):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value:
                raise ValueError(f'{field_name} must be a nonempty string.')


@dataclasses.dataclass(frozen=True)
class KueuePodAdmissionReceipt:
    """Canonical immutable receipt for every verified CoreV1 Pod fact."""

    state: KueuePodAdmissionState
    namespace: str
    pod_name: str
    pod_uid: str
    pod_phase: str
    scheduling_gates: tuple[str, ...]
    cluster_name_on_cloud: str
    kueue_managed_finalizer: str
    local_queue_name: str
    cluster_queue_name: str
    admission_local_queue_name: str | None
    admission_cluster_queue_name: str | None
    workload_priority_class_name: str | None
    pod_group_name: str
    pod_group_total_count: int
    role_hash: str
    podset: str | None
    workload_name: str | None
    unconstrained_topology: str | None
    priority_class_name: str | None
    priority_value: int | None
    preemption_policy: str | None
    scheduler_name: str
    service_account_name: str
    accelerator: str
    accelerator_label_key: str
    accelerator_label_values: tuple[str, ...]
    accelerator_resource_key: str
    accelerator_count: int
    identity: KueuePodAdmissionIdentity

    def __post_init__(self) -> None:
        if not isinstance(self.state, KueuePodAdmissionState):
            raise TypeError('state must be a KueuePodAdmissionState.')
        for field_name in ('namespace', 'pod_name', 'pod_uid', 'pod_phase',
                           'cluster_name_on_cloud', 'kueue_managed_finalizer',
                           'local_queue_name', 'cluster_queue_name',
                           'pod_group_name', 'role_hash', 'scheduler_name',
                           'service_account_name', 'accelerator',
                           'accelerator_label_key', 'accelerator_resource_key'):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value:
                raise ValueError(f'{field_name} must be a nonempty string.')
        if self.pod_phase not in ('Pending', 'Running'):
            raise ValueError('pod_phase must be Pending or Running.')
        if (not isinstance(self.scheduling_gates, tuple) or
                any(not isinstance(value, str) or not value
                    for value in self.scheduling_gates)):
            raise ValueError('scheduling_gates must be a tuple of nonempty '
                             'strings.')
        if tuple(sorted(set(self.scheduling_gates))) != self.scheduling_gates:
            raise ValueError('scheduling_gates must be unique and sorted.')
        if (not isinstance(self.accelerator_label_values, tuple) or
                not self.accelerator_label_values or
                any(not isinstance(value, str) or not value
                    for value in self.accelerator_label_values)):
            raise ValueError('accelerator_label_values must be a nonempty '
                             'tuple of strings.')
        if len(set(self.accelerator_label_values)) != len(
                self.accelerator_label_values):
            raise ValueError('accelerator_label_values must be unique.')
        for field_name in ('admission_local_queue_name',
                           'admission_cluster_queue_name',
                           'workload_priority_class_name', 'podset',
                           'workload_name', 'unconstrained_topology',
                           'priority_class_name', 'preemption_policy'):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError(f'{field_name} must be null or nonempty.')
        if self.priority_value is not None and type(
                self.priority_value) is not int:
            raise TypeError('priority_value must be an integer or null.')
        priority_parts = (self.priority_class_name, self.priority_value,
                          self.preemption_policy)
        if any(value is None for value in priority_parts) and not all(
                value is None for value in priority_parts):
            raise ValueError('priority class, value, and preemption policy '
                             'must all be null or all be present.')
        if re.fullmatch(r'[0-9a-f]{8}', self.role_hash) is None:
            raise ValueError('role_hash must be 8 lowercase hexadecimal '
                             'characters.')
        admitted_outputs = (self.admission_local_queue_name,
                            self.admission_cluster_queue_name, self.podset)
        if self.state is KueuePodAdmissionState.POD_WAITING:
            if any(value is not None for value in admitted_outputs) or any(
                    value is not None
                    for value in (self.workload_name,
                                  self.unconstrained_topology)):
                raise ValueError('POD_WAITING cannot carry admitted outputs.')
        elif (self.admission_local_queue_name != self.local_queue_name or
              self.admission_cluster_queue_name != self.cluster_queue_name or
              self.podset != self.role_hash or
              self.workload_name not in (None, self.pod_group_name) or
              self.unconstrained_topology not in (None, 'true')):
            raise ValueError('POLICY_ADMITTED requires exact admitted queue, '
                             'PodSet, workload, and topology outputs.')
        if (type(self.pod_group_total_count) is not int or
                self.pod_group_total_count < 1):
            raise ValueError('pod_group_total_count must be positive.')
        if (type(self.accelerator_count) is not int or
                self.accelerator_count < 1):
            raise ValueError('accelerator_count must be a positive integer.')
        if not isinstance(self.identity, KueuePodAdmissionIdentity):
            raise TypeError('identity must be KueuePodAdmissionIdentity.')

    def canonical_dict(self) -> dict[str, Any]:
        """Return the closed JSON schema used by PostgreSQL audit state."""
        return {
            'schema_version': 1,
            'state': self.state.value,
            'pod': {
                'namespace': self.namespace,
                'name': self.pod_name,
                'uid': self.pod_uid,
                'phase': self.pod_phase,
                'deletion_timestamp_absent': True,
                'scheduling_gates': list(self.scheduling_gates),
            },
            'skypilot': {
                'cluster_name_on_cloud': self.cluster_name_on_cloud,
                'intent_key': self.identity.intent_key,
                'replica_record_uuid': self.identity.replica_record_uuid,
                'pool_physical_uid': self.identity.pool_physical_uid,
                'worker_projection_sha256':
                    self.identity.worker_projection_sha256,
            },
            'kueue': {
                'managed_finalizer': self.kueue_managed_finalizer,
                'managed_label': True,
                'local_queue_name': self.local_queue_name,
                'cluster_queue_name': self.cluster_queue_name,
                'admission_local_queue_name': self.admission_local_queue_name,
                'admission_cluster_queue_name':
                    self.admission_cluster_queue_name,
                'workload_priority_class_name':
                    self.workload_priority_class_name,
                'pod_group_name': self.pod_group_name,
                'pod_group_total_count': self.pod_group_total_count,
                'retriable_in_group': False,
                'role_hash': self.role_hash,
                'podset': self.podset,
                'workload_name': self.workload_name,
                'unconstrained_topology': self.unconstrained_topology,
            },
            'priority': {
                'class_name': self.priority_class_name,
                'value': self.priority_value,
                'preemption_policy': self.preemption_policy,
            },
            'scheduler_name': self.scheduler_name,
            'service_account_name': self.service_account_name,
            'accelerator': {
                'name': self.accelerator,
                'label_key': self.accelerator_label_key,
                'label_values': list(self.accelerator_label_values),
                'resource_key': self.accelerator_resource_key,
                'count': self.accelerator_count,
                'sole_ray_node_resource_owner': True,
                'dynamic_resource_claims_absent': True,
            },
        }

    def canonical_json(self) -> str:
        return json.dumps(self.canonical_dict(),
                          sort_keys=True,
                          separators=(',', ':'),
                          ensure_ascii=True,
                          allow_nan=False)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_json().encode('utf-8')).hexdigest()


@dataclasses.dataclass(frozen=True)
class KueuePodAdmissionObservation:
    """Exact receipt delivered to the Serve-owned durable state callback."""

    receipt: KueuePodAdmissionReceipt

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, KueuePodAdmissionReceipt):
            raise TypeError('receipt must be KueuePodAdmissionReceipt.')

    @property
    def state(self) -> KueuePodAdmissionState:
        return self.receipt.state

    @property
    def namespace(self) -> str:
        return self.receipt.namespace

    @property
    def pod_name(self) -> str:
        return self.receipt.pod_name

    @property
    def pod_uid(self) -> str:
        return self.receipt.pod_uid

    @property
    def accelerator(self) -> str:
        return self.receipt.accelerator

    @property
    def accelerator_count(self) -> int:
        return self.receipt.accelerator_count

    @property
    def identity(self) -> KueuePodAdmissionIdentity:
        return self.receipt.identity

    @property
    def receipt_sha256(self) -> str:
        return self.receipt.sha256


class KueuePodAdmissionObserver(Protocol):
    """Runtime observer that anchors provider reads to database time.

    ``begin_observation`` performs only a short clock read and returns after
    releasing its connection.  The provisioner then performs Kubernetes I/O
    without a SQL/advisory lock and passes the original token to ``__call__``.
    The durable implementation rejects a token whose freshness was consumed
    by provider latency or later lock contention.
    """

    def begin_observation(self) -> datetime.datetime:
        """Sample the durable clock immediately before provider I/O."""

    def __call__(self, observation: KueuePodAdmissionObservation,
                 provider_read_started_at: datetime.datetime) -> None:
        """Commit one exact observation against its original clock token."""


@dataclasses.dataclass(frozen=True)
class KueuePodAdmissionRuntime:
    """Complete runtime-only contract for one Kueue-managed provider Pod.

    The generic provisioner owns this transport boundary.  Serve supplies the
    immutable identity and callback implementation, but downstream provider
    code receives one all-or-none value instead of four independently optional
    arguments that could form an invalid partial runtime.
    """

    identity: KueuePodAdmissionIdentity
    accelerator: str
    observer: KueuePodAdmissionObserver
    persisted_pod_identity: KueuePersistedPodIdentity | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.identity, KueuePodAdmissionIdentity):
            raise TypeError('identity must be KueuePodAdmissionIdentity.')
        if not isinstance(self.accelerator, str) or not self.accelerator:
            raise ValueError('accelerator must be a nonempty string.')
        if (not callable(self.observer) or not callable(
                getattr(self.observer, 'begin_observation', None))):
            raise TypeError('observer must expose callable clock-begin and '
                            'commit boundaries.')
        if (self.persisted_pod_identity is not None and not isinstance(
                self.persisted_pod_identity, KueuePersistedPodIdentity)):
            raise TypeError('persisted_pod_identity must be '
                            'KueuePersistedPodIdentity or None.')


class ProvisionerError(RuntimeError):
    """Exception for provisioner."""
    # Values are not always strings: GCP TPU operations report integer gRPC
    # status codes (e.g. 3/8/9) and some producers store None.
    errors: list[dict[str, Any]]
    # Number of instances in the failed provider request, when known. This lets
    # higher layers distinguish a full-demand failure from filling a partial or
    # orphaned cluster without parsing provider-specific messages.
    requested_count: int | None = None


class ProviderCreateRejectedError(ProvisionerError):
    """A provider-native create rejection with exact zero-effect evidence.

    The provisioner which raises this type must attach a closed
    ``provider_negative_ack`` receipt.  Cleanup layers must independently
    validate that receipt against their immutable request scope before using
    it to skip provider teardown.
    """
    provider_negative_ack: dict[str, Any]


class StopFailoverError(Exception):
    """Exception for stopping failover.

    It will be raised when failed to cleaning up resources after a failed
    provision, so the caller should stop the failover process and raise.
    """


# These fields are sensitive and should be redacted from the config for logging
# purposes.
SENSITIVE_FIELDS: list[tuple[str, ...]] = [
    ('docker_config', 'docker_login_config', 'password'),
    ('provider_config', 'create_instance_kwargs', 'login'),
    ('provider_config', 'create_instance_kwargs', 'api_key'),
]


def register_sensitive_fields(fields: list[tuple[str, ...]]) -> None:
    """Register additional sensitive fields for redaction."""
    SENSITIVE_FIELDS.extend(fields)


@dataclasses.dataclass
class ProvisionConfig:
    """Configuration for provisioning."""
    # Global configurations for the cloud provider.
    provider_config: dict[str, Any]
    # Configurations for the authentication.
    authentication_config: dict[str, Any]
    # Configurations for the docker container to be run on the instance.
    docker_config: dict[str, Any]
    # Configurations for each instance.
    node_config: dict[str, Any]
    # Number of instances to start.
    count: int
    # Tags for the instances.
    tags: dict[str, str]
    # Whether or not to resume stopped instances.
    resume_stopped_nodes: bool
    # Optional ports to open on launch of the cluster.
    ports_to_open_on_launch: list[int] | None
    # Internal cluster-generation identity. This is deliberately keyword-only
    # so existing positional construction and required-field subclasses remain
    # compatible.
    cluster_incarnation: str | None = dataclasses.field(default=None,
                                                        kw_only=True,
                                                        repr=False)
    # Runtime-only idempotency identity for one logical provider create.  It
    # is deliberately separate from node_config so a persisted task or user
    # override cannot nominate the provider request identity.
    provider_create_idempotency_token: str | None = dataclasses.field(
        default=None, kw_only=True, repr=False, compare=False)
    # Account scope durably bound by the owning Serve association before the
    # first provider create.  The provider revalidates it with STS before any
    # EC2 inventory or mutation, preventing cross-account replay.
    provider_create_account_id: str | None = dataclasses.field(default=None,
                                                               kw_only=True,
                                                               repr=False,
                                                               compare=False)
    # Runtime-only authorization boundary. Built-in provisioners enter this
    # immediately around bounded provider mutations and release it before
    # passive capacity/readiness waits.
    provider_effect_guard_factory: ProviderEffectGuardFactory | None = (
        dataclasses.field(default=None, kw_only=True, repr=False,
                          compare=False))
    # Complete runtime-only Kueue admission contract.  The generic provisioner
    # classifies only CoreV1 Pod state; it neither imports Serve nor owns
    # PostgreSQL transitions.  A persisted Pod identity, when present inside
    # the contract, makes provisioning adoption-only for that exact object.
    kueue_admission_runtime: KueuePodAdmissionRuntime | None = (
        dataclasses.field(default=None, kw_only=True, repr=False,
                          compare=False))

    def get_redacted_config(self) -> dict[str, Any]:
        """Get the redacted config."""
        # Avoid deepcopying a bound guard factory (and therefore its backend)
        # while projecting this dataclass for logging.
        serializable = dataclasses.replace(self,
                                           provider_effect_guard_factory=None,
                                           kueue_admission_runtime=None)
        config = dataclasses.asdict(serializable)
        # This internal identity is not part of the provision-log contract.
        config.pop('cluster_incarnation', None)
        config.pop('provider_create_idempotency_token', None)
        config.pop('provider_create_account_id', None)
        config.pop('provider_effect_guard_factory', None)
        config.pop('kueue_admission_runtime', None)

        config_copy = config_utils.Config(config)

        for field_list in SENSITIVE_FIELDS:
            val = config_copy.get_nested(field_list, default_value=None)
            if val is not None:
                config_copy.set_nested(field_list, '<redacted>')
        return dict(**config_copy)


@contextlib.contextmanager
def provider_effect_guard(config: ProvisionConfig):
    """Enter one runtime-only provider mutation authorization, if supplied."""
    factory = config.provider_effect_guard_factory
    if factory is None:
        yield
        return
    with factory():
        yield


# -------------------- output data model -------------------- #


@dataclasses.dataclass(frozen=True)
class ProvisionRuntimeMetadata:
    """Record of what the provisioner set up and which runtime
    phases it handled. Set once at provision time.
    """

    # Whether ray is running on the cluster.
    has_ray: bool = True
    # Whether the skylet daemon is running on the cluster.
    has_skylet: bool = True
    # Whether the cluster runs a job queue (ray + skylet bookkeeping) that
    # can accept multiple ``sky exec`` submissions over its lifetime. False
    # for single-use clusters where the job is baked into the provisioned
    # runtime itself.
    has_job_queue: bool = True
    # Whether the cluster is reachable via SSH using the credentials and
    # endpoint recorded in its cluster YAML.
    ssh_available: bool = True
    # Whether the SkyPilot runtime (cloud credentials, wheel, ray, skylet)
    # has already been materialized on the cluster by the provisioner.
    runtime_setup_done: bool = False
    # Whether the user's workdir has already been synced to the cluster by
    # the provisioner.
    workdir_synced: bool = False
    # Whether the task's file_mounts have already been synced to the
    # cluster by the provisioner.
    file_mounts_synced: bool = False
    # Whether the user's ``setup`` commands have already been run on the
    # cluster by the provisioner.
    setup_done: bool = False
    # Whether the user's ``run`` command has already been started on the
    # cluster by the provisioner.
    run_started: bool = False


@dataclasses.dataclass(frozen=True)
class AWSInstanceIdentity:
    """Closed AWS facts captured by the exact provisioning credential scope."""

    aws_account_id: str
    region: str
    availability_zone: str
    ec2_instance_id: str
    instance_type: str
    market_type: str

    def __post_init__(self) -> None:
        if (not isinstance(self.aws_account_id, str) or
                len(self.aws_account_id) != 12 or
                not self.aws_account_id.isdecimal()):
            raise ValueError('aws_account_id must contain exactly 12 digits.')
        for field_name in ('region', 'availability_zone', 'ec2_instance_id',
                           'instance_type'):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value:
                raise ValueError(f'{field_name} must be a nonempty string.')
        if self.market_type not in ('on_demand', 'spot'):
            raise ValueError('market_type must be on_demand or spot.')


@dataclasses.dataclass(frozen=True)
class GCPInstanceIdentity:
    """Closed GCP facts read from one exact fresh Compute instance."""

    project_id: str
    zone: str
    instance_name: str
    instance_type: str
    market_type: str

    def __post_init__(self) -> None:
        for field_name in ('project_id', 'zone', 'instance_name',
                           'instance_type'):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value:
                raise ValueError(f'{field_name} must be a nonempty string.')
        if self.market_type not in ('on_demand', 'spot'):
            raise ValueError('market_type must be on_demand or spot.')


@dataclasses.dataclass
class ProvisionRecord:
    """Record for a provisioning process."""
    # The name of the cloud provider.
    provider_name: str
    # The name of the region.
    region: str
    # The name of the sub-zone in the region. It must be a single zone.
    # It can also be None if the cloud provider does not support zones.
    zone: str | None
    # The name of the cluster.
    cluster_name: str
    # The unique identifier of the head instance, i.e., the
    # `instance_info.instance_id` of the head node.
    head_instance_id: InstanceId
    # The IDs of all just resumed instances.
    resumed_instance_ids: list[InstanceId]
    # The IDs of all just created instances.
    created_instance_ids: list[InstanceId]
    # Metadata about the runtime materialized by provisioning.
    runtime_metadata: ProvisionRuntimeMetadata = dataclasses.field(
        default_factory=ProvisionRuntimeMetadata)
    # Present only when one exact fresh AWS create was re-read with the same
    # request-scoped credential session. It is optional operational evidence,
    # never provider lifecycle authority.
    fresh_aws_instance_identity: AWSInstanceIdentity | None = dataclasses.field(
        default=None, kw_only=True, repr=False)
    # Present only when one exact fresh GCP Compute instance was re-read as
    # RUNNING in the requested project and zone with the requested machine and
    # market type.  It is optional operational evidence, never provider
    # lifecycle authority.
    fresh_gcp_instance_identity: GCPInstanceIdentity | None = dataclasses.field(
        default=None, kw_only=True, repr=False)

    def is_instance_just_booted(self, instance_id: InstanceId) -> bool:
        """Whether or not the instance is just booted.

        Is an instance just booted,  so that there are no services running?
        """
        return (instance_id in self.resumed_instance_ids or
                instance_id in self.created_instance_ids)


@dataclasses.dataclass
class InstanceInfo:
    """Instance information."""
    instance_id: InstanceId
    internal_ip: str
    external_ip: str | None
    tags: dict[str, str]
    ssh_port: int = 22
    # The internal service address of the instance on Kubernetes.
    internal_svc: str | None = None
    # The infrastructure node name for display in dashboard.
    # For Kubernetes: the k8s node name the pod runs on.
    # For clouds: the instance name (e.g., from AWS Name tag, GCP name).
    node_name: str | None = None

    def get_feasible_ip(self) -> str:
        """Get the most feasible IPs of the instance. This function returns
        the public IP if it exist, otherwise it returns a private IP."""
        if self.external_ip is not None:
            return self.external_ip
        return self.internal_ip


@dataclasses.dataclass
class ClusterInfo:
    """Cluster Information."""
    instances: dict[InstanceId, list[InstanceInfo]]
    # The unique identifier of the head instance, i.e., the
    # `instance_info.instance_id` of the head node.
    head_instance_id: InstanceId | None
    # Provider related information.
    provider_name: str
    provider_config: dict[str, Any] | None = None

    docker_user: str | None = None
    # Override the ssh_user from the cluster config.
    ssh_user: str | None = None
    custom_ray_options: dict[str, Any] | None = None

    @property
    def num_instances(self) -> int:
        """Get the number of instances in the cluster."""
        return sum(len(instances) for instances in self.instances.values())

    def get_head_instance(self) -> InstanceInfo | None:
        """Get the instance metadata of the head node"""
        if self.head_instance_id is None:
            return None
        if self.head_instance_id not in self.instances:
            raise ValueError('Head instance ID not in the cluster metadata. '
                             f'ClusterInfo: {self.__dict__}')
        return self.instances[self.head_instance_id][0]

    def get_worker_instances(self) -> list[InstanceInfo]:
        """Get all worker instances."""
        worker_instances = []
        for inst_id, instances in self.instances.items():
            if inst_id == self.head_instance_id:
                worker_instances.extend(instances[1:])
            else:
                worker_instances.extend(instances)
        return worker_instances

    def ip_tuples(self) -> list[tuple[str, str | None]]:
        """Get IP tuples of all instances. Make sure that list always
        starts with head node IP, if head node exists.

        Returns:
            A list of tuples (internal_ip, external_ip) of all instances.
        """
        head_instance = self.get_head_instance()
        if head_instance is None:
            head_instance_ip = []
        else:
            head_instance_ip = [(head_instance.internal_ip,
                                 head_instance.external_ip)]
        other_ips = []
        for instance in self.get_worker_instances():
            pair = (instance.internal_ip, instance.external_ip)
            other_ips.append(pair)
        return head_instance_ip + other_ips

    def instance_ids(self) -> list[str]:
        """Return the instance ids in the same order of ip_tuples."""
        id_list = []
        if self.head_instance_id is not None:
            id_list.append(self.head_instance_id + '-0')
        for inst_id, instances in self.instances.items():
            start_idx = 0
            if inst_id == self.head_instance_id:
                start_idx = 1
            id_list.extend(
                [f'{inst_id}-{i}' for i in range(start_idx, len(instances))])
        return id_list

    def has_external_ips(self) -> bool:
        """True if the cluster has external IP."""
        ip_tuples = self.ip_tuples()
        if not ip_tuples:
            return False
        return ip_tuples[0][1] is not None

    def _get_ips(self, use_internal_ips: bool) -> list[str]:
        """Get public or private/internal IPs of all instances.

        It returns the IP of the head node first.
        """
        ip_tuples = self.ip_tuples()
        ip_list = []
        if use_internal_ips:
            for pair in ip_tuples:
                internal_ip = pair[0]
                if internal_ip is None:
                    raise ValueError('Not all instances have private IPs')
                ip_list.append(internal_ip)
        else:
            for pair in ip_tuples:
                public_ip = pair[1]
                if public_ip is None:
                    raise ValueError('Not all instances have public IPs')
                ip_list.append(public_ip)
        return ip_list

    def get_feasible_ips(self, force_internal_ips: bool = False) -> list[str]:
        """Get internal or external IPs depends on the settings."""
        use_internal_ips = (self.provider_config is not None and
                            self.provider_config.get('use_internal_ips', False))
        return self._get_ips(use_internal_ips or not self.has_external_ips() or
                             force_internal_ips)

    def get_ssh_ports(self) -> list[int]:
        """Get the SSH port of all the instances."""
        head_instance = self.get_head_instance()

        head_instance_port = []
        if head_instance is not None:
            head_instance_port = [head_instance.ssh_port]

        worker_instances = self.get_worker_instances()
        worker_instance_ports = [
            instance.ssh_port for instance in worker_instances
        ]
        return head_instance_port + worker_instance_ports

    def get_node_names(self) -> list[str] | None:
        """Get current node names as a list, head first.

        Returns:
            List of node names ordered head-first, or None if unavailable.
            For Kubernetes, this is the k8s node name the pod runs on.
            For clouds, this is the instance name.
        """
        node_names: list[str] = []
        head = self.get_head_instance()
        if head is not None and head.node_name:
            node_names.append(head.node_name)
        for worker in self.get_worker_instances():
            node_names.append(worker.node_name or '')
        return node_names if node_names else None


class Endpoint(abc.ABC):
    """Base class for endpoints."""

    @abc.abstractmethod
    def url(self, override_ip: str | None = None) -> str:
        raise NotImplementedError


@dataclasses.dataclass
class SocketEndpoint(Endpoint):
    """Socket endpoint accessible via a host and a port."""
    port: int | None
    host: str = ''

    def url(self, override_ip: str | None = None) -> str:
        host = override_ip if override_ip else self.host
        if env_options.Options.RUNNING_IN_BUILDKITE.get(
        ) and 'localhost' in host:
            # In Buildkite CI, we run a kind (Kubernetes in Docker) cluster.
            # The controller pod runs inside this kind cluster, which itself
            # runs in a container. When the pod tries to access 'localhost',
            # it can't reach the host machine's localhost. Using
            # 'host.docker.internal' allows the pod to properly communicate
            # with services running on the host machine's localhost.
            host = 'host.docker.internal'
        return f'{host}{":" + str(self.port) if self.port else ""}'


@dataclasses.dataclass
class HTTPEndpoint(SocketEndpoint):
    """HTTP endpoint accessible via a url."""
    path: str = ''

    def url(self, override_ip: str | None = None) -> str:
        host = override_ip if override_ip else self.host
        return f'http://{os.path.join(super().url(host), self.path)}'


@dataclasses.dataclass
class HTTPSEndpoint(SocketEndpoint):
    """HTTPS endpoint accessible via a url."""
    path: str = ''

    def url(self, override_ip: str | None = None) -> str:
        host = override_ip if override_ip else self.host
        return f'https://{os.path.join(super().url(host), self.path)}'


def query_ports_passthrough(
    ports: list[str],
    head_ip: str | None,
) -> dict[int, list[Endpoint]]:
    """Common function to get endpoints for AWS, GCP and Azure.

    Returns a list of socket endpoint using head_ip and ports."""
    assert head_ip is not None, head_ip
    ports = list(resources_utils.port_ranges_to_set(ports))
    result: dict[int, list[Endpoint]] = {}
    for port in ports:
        result[port] = [SocketEndpoint(port=port, host=head_ip)]
    return result


def log_function_start_end(func):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(_START_TITLE.format(func.__name__))
        try:
            return func(*args, **kwargs)
        finally:
            logger.info(_END_TITLE.format(func.__name__))

    return wrapper
