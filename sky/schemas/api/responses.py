"""Responses for the API server."""

import enum
from typing import Any

import pydantic

from sky import data
from sky import models
from sky.container_images import models as container_image_models
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
    # Monotonic build number (git commit count); auto-increments with every
    # commit. None when unknown (e.g. no git metadata at build time).
    build: str | None = None
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


class ContainerImageLocationRecord(ResponseBaseModel):
    """Materialization status for one artifact at one registry target."""
    model_config = pydantic.ConfigDict(hide_input_in_errors=True)
    id: str
    image_id: str
    distribution: str = pydantic.Field(
        validation_alias=pydantic.AliasChoices('distribution', 'profile'))
    target_id: str
    target_fingerprint: str
    policy_fingerprint: str
    profile_revision: int
    canonical: bool
    canonical_location_id: str | None = None
    target_ref: str | None = None
    expected_digest: str
    state: str
    attempt_count: int
    next_retry_at: int | None = None
    last_verified_at: int | None = None
    verification_requested_at: int | None = None
    last_used_at: int | None = None
    auto_evict: bool = False
    last_error: str | None = None
    updated_at: int

    @property
    def profile(self) -> str:
        """Compatibility accessor for pre-release response consumers."""
        return self.distribution

    @pydantic.field_validator('id', 'image_id', 'canonical_location_id')
    @classmethod
    def validate_catalog_ids(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return container_image_models.validate_catalog_id(
            value, 'Container image catalog ID')

    @pydantic.field_validator('distribution')
    @classmethod
    def validate_distribution(cls, value: str) -> str:
        return container_image_models.validate_control_plane_identifier(
            value, 'Container image distribution')

    @pydantic.field_validator('target_id')
    @classmethod
    def validate_target(cls, value: str) -> str:
        return container_image_models.validate_control_plane_identifier(
            value, 'Container image target')

    @pydantic.field_validator('target_fingerprint', 'policy_fingerprint')
    @classmethod
    def validate_fingerprint(cls, value: str) -> str:
        return container_image_models.validate_fingerprint(
            value, 'Container image fingerprint')

    @pydantic.field_validator('target_ref')
    @classmethod
    def validate_target_ref(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return container_image_models.validate_oci_reference(
            value, 'Container image target reference')

    @pydantic.field_validator('expected_digest')
    @classmethod
    def validate_expected_digest(cls, value: str) -> str:
        return container_image_models.validate_sha256_digest(
            value, 'Container image expected digest')

    @pydantic.field_validator('state')
    @classmethod
    def validate_state(cls, value: str) -> str:
        try:
            return container_image_models.ImageLocationState(value).value
        except (TypeError, ValueError):
            raise ValueError(
                'Container image location state must be supported.') from None

    @pydantic.field_validator('last_error')
    @classmethod
    def validate_last_error(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            return container_image_models.ImageLocationErrorCode(value).value
        except (TypeError, ValueError):
            raise ValueError(
                'Container image location error must be a supported code.'
            ) from None


class ContainerImageRecord(ResponseBaseModel):
    """Workspace-scoped immutable container image artifact."""
    model_config = pydantic.ConfigDict(hide_input_in_errors=True)
    id: str
    workspace: str
    source_ref: str | None = None
    resolved_source_ref: str | None = None
    sources: list[str]
    source_digest: str
    releases: list[str]
    producer_kind: str
    producer_spec_hash: str | None = None
    builder_version: str | None = None
    platforms: list[str]
    compressed_size_bytes: int | None = None
    created_at: int
    updated_at: int
    locations: list[ContainerImageLocationRecord]

    @pydantic.field_validator('id')
    @classmethod
    def validate_id(cls, value: str) -> str:
        return container_image_models.validate_catalog_id(
            value, 'Container image artifact ID')

    @pydantic.field_validator('workspace')
    @classmethod
    def validate_workspace(cls, value: str) -> str:
        return container_image_models.validate_workspace_name(
            value, 'Container image workspace')

    @pydantic.field_validator('source_ref', 'resolved_source_ref')
    @classmethod
    def validate_source_ref(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return container_image_models.validate_oci_reference(
            value, 'Container image source reference')

    @pydantic.field_validator('sources')
    @classmethod
    def validate_sources(cls, sources: list[str]) -> list[str]:
        return [
            container_image_models.validate_oci_reference(
                source, 'Container image source reference')
            for source in sources
        ]

    @pydantic.field_validator('source_digest')
    @classmethod
    def validate_source_digest(cls, value: str) -> str:
        return container_image_models.validate_sha256_digest(
            value, 'Container image source digest')

    @pydantic.field_validator('releases')
    @classmethod
    def validate_releases(cls, releases: list[str]) -> list[str]:
        """Refuses unsafe catalog data before it can reach a terminal."""
        return [
            container_image_models.validate_release_label(
                release, 'container image release') for release in releases
        ]

    @pydantic.field_validator('producer_kind')
    @classmethod
    def validate_producer_kind(cls, value: str) -> str:
        return container_image_models.validate_image_producer_kind(
            value, 'Container image producer kind')

    @pydantic.field_validator('producer_spec_hash')
    @classmethod
    def validate_producer_spec_hash(cls, value: str | None) -> str | None:
        return container_image_models.validate_producer_spec_hash(
            value, 'Container image producer specification hash')

    @pydantic.field_validator('builder_version')
    @classmethod
    def validate_builder_version(cls, value: str | None) -> str | None:
        return container_image_models.validate_builder_version(
            value, 'Container image builder version')

    @pydantic.field_validator('platforms')
    @classmethod
    def validate_platforms(cls, platforms: list[str]) -> list[str]:
        return list(
            container_image_models.validate_oci_platforms(
                platforms, 'Container image platforms'))

    @pydantic.field_validator('compressed_size_bytes')
    @classmethod
    def validate_compressed_size_bytes(cls, value: int | None) -> int | None:
        return container_image_models.validate_compressed_size_bytes(
            value, 'Container image compressed size')
