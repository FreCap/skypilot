"""Responses for the API server."""

import enum
from typing import Any
import uuid

import pydantic

from sky import data
from sky import models
from sky.jobs import state as job_state
from sky.server import common
from sky.skylet import job_lib
from sky.utils import status_lib


class ResponseBaseModel(pydantic.BaseModel):
    """A pydantic model that acts like a dict.

    Supports the following syntax:
    class SampleResponse(DictLikePayload):
        field: str

    response = SampleResponse(field='value')
    print(response['field']) # prints 'value'
    response['field'] = 'value2'
    print(response['field']) # prints 'value2'
    print('field' in response) # prints True

    This model exists for backwards compatibility with the
    old SDK that used to return a dict.

    The backward compatibility may be removed
    in the future.
    """
    # Ignore extra fields in the request body, which is useful for backward
    # compatibility. The difference with `allow` is that `ignore` will not
    # include the unknown fields when dump the model, i.e., we can add new
    # fields to the request body without breaking the existing old API server
    # where the handler function does not accept the new field in function
    # signature.
    model_config = pydantic.ConfigDict(extra='ignore')

    # backward compatibility with dict
    # TODO(syang): remove this in v0.13.0
    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError as e:
            raise KeyError(key) from e

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def get(self, key, default=None):
        return getattr(self, key, default)

    def __contains__(self, key):
        return hasattr(self, key)

    def keys(self):
        return self.model_dump().keys()

    def values(self):
        return self.model_dump().values()

    def items(self):
        return self.model_dump().items()

    def __repr__(self):
        return self.__dict__.__repr__()


class APIHealthResponse(ResponseBaseModel):
    """Response for the API health endpoint."""
    status: common.ApiServerStatus
    api_version: str = ''
    version: str = ''
    version_on_disk: str = ''
    commit: str = ''
    # ISO 8601 committer timestamp for the exact release commit. None when
    # source-control metadata was unavailable while building the package.
    commit_timestamp: str | None = None
    # Monotonic build number (git commit count); auto-increments with every
    # commit. None when unknown (e.g. no git metadata at build time).
    build: str | None = None
    # ISO 8601 initialization time of the API process serving this response.
    # This advances after either a rollout or a process restart.
    deployment_timestamp: str | None = None
    # Whether basic auth on api server is enabled
    basic_auth_enabled: bool = False
    user: models.User | None = None
    # Whether service account token is enabled
    service_account_token_enabled: bool = False
    # Whether basic auth on ingress is enabled
    ingress_basic_auth_enabled: bool = False
    # Latest version info (if available)
    latest_version: str | None = None
    # Whether external proxy auth is enabled
    external_proxy_auth_enabled: bool = False
    # Whether telemetry/usage collection is enabled
    telemetry_enabled: bool = True
    # Fleet-wide, fail-closed readiness for the private ordinary Serve launch
    # binding protocol.  False includes mixed API/executor generations.
    ordinary_launch_binding_capable: bool = False


class OrdinaryLaunchBindingResponse(ResponseBaseModel):
    """Identity returned by the private ordinary Serve launch endpoint."""

    submission_uuid: uuid.UUID
    association_id: uuid.UUID
    request_id: uuid.UUID
    launch_generation: pydantic.PositiveInt
    created: pydantic.StrictBool


class StatusResponse(ResponseBaseModel):
    """Response for the status endpoint."""
    name: str
    launched_at: int
    # pydantic cannot generate the pydantic-core schema for
    # backends.ResourceHandle, so we use Any here.
    # This is an internally facing field anyway, so it's less
    # of a problem that it's not typed.
    handle: Any | None = None
    last_use: str | None = None
    status: status_lib.ClusterStatus
    autostop: int
    to_down: bool
    owner: list[str] | None = None
    # metadata is a JSON, so we use Any here.
    metadata: dict[str, Any] | None = None
    cluster_hash: str
    cluster_ever_up: bool
    status_updated_at: int | None = None
    user_hash: str
    user_name: str
    config_hash: str | None = None
    workspace: str
    last_creation_yaml: str | None = None
    last_creation_command: str | None = None
    is_managed: bool
    last_event: str | None = None
    # Latest LAUNCH_PROGRESS event reason for clusters in INIT status
    # (rendered as LAUNCHING on the dashboard). None for all other
    # statuses and for clusters that have not yet emitted a
    # launch-progress event.
    launch_status_reason: str | None = None
    resources_str: str | None = None
    resources_str_full: str | None = None
    # credentials is a JSON, so we use Any here.
    credentials: dict[str, Any] | None = None
    nodes: int
    cloud: str | None = None
    region: str | None = None
    cpus: str | None = None
    memory: str | None = None
    accelerators: str | None = None
    labels: dict[str, str] | None = None
    cluster_name_on_cloud: str | None = None
    node_names: str | None = None
    priority: int | None = None
    priority_class: str | None = None
    # External links surfaced on the dashboard's cluster detail page.
    # Currently populated with cloud-provider instance console URLs at launch
    # time (mirrors ManagedJobRecord.links). Shape: {label: url}.
    links: dict[str, str] | None = None


class ClusterJobRecord(ResponseBaseModel):
    """Response for the cluster job queue endpoint."""
    job_id: int
    job_name: str
    username: str
    user_hash: str
    submitted_at: float
    # None if the job has not started yet.
    start_at: float | None = None
    # None if the job has not ended yet.
    end_at: float | None = None
    resources: str
    status: job_lib.JobStatus
    log_path: str
    metadata: dict[str, Any] = {}


class UploadStatus(enum.Enum):
    """Status of the upload."""
    UPLOADING = 'uploading'
    COMPLETED = 'completed'


class StorageRecord(ResponseBaseModel):
    """Response for the storage list endpoint."""
    name: str
    launched_at: int
    store: list[data.StoreType]
    last_use: str
    status: status_lib.StorageStatus


# TODO (syang) figure out which fields are always present
# and therefore can be non-optional.
class ManagedJobRecord(ResponseBaseModel):
    """A single managed job record."""
    # The job_id in the spot table
    task_job_id: int | None = pydantic.Field(None, alias='_job_id')
    job_id: int | None = None
    task_id: int | None = None
    job_name: str | None = None
    task_name: str | None = None
    job_duration: float | None = None
    workspace: str | None = None
    status: job_state.ManagedJobStatus | None = None
    schedule_state: str | None = None
    resources: str | None = None
    cluster_resources: str | None = None
    cluster_resources_full: str | None = None
    cloud: str | None = None
    region: str | None = None
    zone: str | None = None
    infra: str | None = None
    recovery_count: int | None = None
    details: str | None = None
    failure_reason: str | None = None
    user_name: str | None = None
    user_hash: str | None = None
    submitted_at: float | None = None
    start_at: float | None = None
    end_at: float | None = None
    user_yaml: str | None = None
    entrypoint: str | None = None
    metadata: dict[str, Any] | None = None
    controller_pid: int | None = None
    controller_pid_started_at: float | None = None
    dag_yaml_path: str | None = None
    env_file_path: str | None = None
    last_recovered_at: float | None = None
    run_timestamp: str | None = None
    priority: int | None = None
    priority_class: str | None = None
    original_user_yaml_path: str | None = None
    pool: str | None = None
    pool_hash: str | None = None
    current_cluster_name: str | None = None
    cluster_name_on_cloud: str | None = None
    job_id_on_pool_cluster: int | None = None
    accelerators: dict[str, int] | None = None
    labels: dict[str, str] | None = None
    links: dict[str, str] | None = None
    # Node names for dashboard display (comma-separated)
    node_names: str | None = None
    # JobGroup fields
    # Execution mode: 'parallel' (job group) or 'serial' (pipeline/single job)
    execution: str | None = None
    is_job_group: bool | None = None
    # Whether this task is a primary task (True) or auxiliary task (False)
    # within a job group. NULL for non-job-group jobs (single jobs and
    # pipelines).
    is_primary_in_job_group: bool | None = None
    # Whether this job is a batch coordinator (ds.map())
    is_batch: bool | None = None
    # Batch progress fields (NULL for non-batch jobs)
    batch_total_batches: int | None = None
    batch_completed_batches: int | None = None
    # Network endpoint information (extracted from cluster handle)
    # List of (internal_ip, external_ip) tuples for all nodes
    internal_external_ips: list[tuple[str, str]] | None = None
    # K8s DNS entries mapping Pod name to internal_svc
    # Only populated for Kubernetes clusters
    internal_services: dict[str, str | None] | None = None


class VolumeRecord(ResponseBaseModel):
    """A single volume record."""
    name: str
    type: str
    launched_at: int
    cloud: str
    region: str | None = None
    zone: str | None = None
    size: str | None = None
    config: dict[str, Any]
    name_on_cloud: str
    user_hash: str
    user_name: str
    workspace: str
    last_attached_at: int | None = None
    last_use: str | None = None
    status: str | None = None
    usedby_pods: list[str]
    usedby_clusters: list[str]
    is_ephemeral: bool = False
    usedby_fetch_failed: bool = False
    # Error message for volume in ERROR state (e.g., PVC pending due to
    # access mode mismatch)
    error_message: str | None = None
    # YAML configuration used to create the volume
    creation_yaml: str | None = None
