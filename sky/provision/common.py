"""Common data structures for provisioning"""
import abc
from collections.abc import Callable
import contextlib
import dataclasses
import functools
import os
from typing import Any

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


class ProvisionerError(RuntimeError):
    """Exception for provisioner."""
    # Values are not always strings: GCP TPU operations report integer gRPC
    # status codes (e.g. 3/8/9) and some producers store None.
    errors: list[dict[str, Any]]
    # Number of instances in the failed provider request, when known. This lets
    # higher layers distinguish a full-demand failure from filling a partial or
    # orphaned cluster without parsing provider-specific messages.
    requested_count: int | None = None


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
    # Runtime-only authorization boundary. Built-in provisioners enter this
    # immediately around bounded provider mutations and release it before
    # passive capacity/readiness waits.
    provider_effect_guard_factory: ProviderEffectGuardFactory | None = (
        dataclasses.field(default=None, kw_only=True, repr=False,
                          compare=False))

    def get_redacted_config(self) -> dict[str, Any]:
        """Get the redacted config."""
        # Avoid deepcopying a bound guard factory (and therefore its backend)
        # while projecting this dataclass for logging.
        serializable = dataclasses.replace(self,
                                           provider_effect_guard_factory=None)
        config = dataclasses.asdict(serializable)
        # This internal identity is not part of the provision-log contract.
        config.pop('cluster_incarnation', None)
        config.pop('provider_effect_guard_factory', None)

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
