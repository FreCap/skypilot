"""Versioned, credential-free models for the direct image API."""
# pylint: disable=missing-class-docstring

from __future__ import annotations

import hashlib
from typing import Any, Literal

import pydantic

from sky.container_images import catalog_state
from sky.container_images import demand_state
from sky.container_images import models
from sky.container_images import topology_state


class _ApiModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra='forbid',
                                       hide_input_in_errors=True)


class PublicationCreate(_ApiModel):
    source_ref: str
    release: str
    distribution: str
    workspace: str | None = None
    platform: str = 'linux/amd64'
    source_auth: str | None = None

    @pydantic.model_validator(mode='after')
    def validate_publication(self) -> 'PublicationCreate':
        self.source_ref = models.validate_oci_reference(self.source_ref,
                                                        'Publication source')
        if models.split_digest(self.source_ref)[1] is None:
            raise ValueError('Publication source must be digest-pinned.')
        self.release = models.validate_release_label(self.release,
                                                     'Publication release')
        self.distribution = models.validate_control_plane_identifier(
            self.distribution, 'Publication distribution')
        self.platform = models.validate_oci_platform(self.platform,
                                                     'Publication platform')
        if self.workspace is not None:
            self.workspace = models.validate_workspace_name(
                self.workspace, 'Publication workspace')
        if self.source_auth is not None:
            self.source_auth = models.validate_control_plane_identifier(
                self.source_auth, 'Publication source authentication binding')
        return self


class ArtifactPrepare(_ApiModel):
    distribution: str
    target: str
    workspace: str | None = None

    @pydantic.model_validator(mode='after')
    def validate_prepare(self) -> 'ArtifactPrepare':
        self.distribution = models.validate_control_plane_identifier(
            self.distribution, 'Preparation distribution')
        self.target = models.validate_control_plane_identifier(
            self.target, 'Preparation target')
        if self.workspace is not None:
            self.workspace = models.validate_workspace_name(
                self.workspace, 'Preparation workspace')
        return self


class WorkspaceMutation(_ApiModel):
    workspace: str | None = None

    @pydantic.field_validator('workspace')
    @classmethod
    def validate_workspace(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return models.validate_workspace_name(value, 'Image workspace')


class QualificationCreate(_ApiModel):
    manifest: dict[str, Any]


class CanaryCreate(_ApiModel):
    workspace: str
    target: str
    backend: Literal['aws_vm', 'aws_eks']
    runtime_id: str | None = None
    confirm_cost: bool

    @pydantic.model_validator(mode='after')
    def validate_canary(self) -> 'CanaryCreate':
        self.workspace = models.validate_workspace_name(self.workspace,
                                                        'Canary workspace')
        self.target = models.validate_control_plane_identifier(
            self.target, 'Canary target')
        if self.runtime_id is not None:
            self.runtime_id = models.validate_control_plane_identifier(
                self.runtime_id, 'Canary runtime ID')
        if not self.confirm_cost:
            raise ValueError('Canary cost confirmation is required.')
        return self


class OperationView(_ApiModel):
    id: str
    kind: str
    state: str
    result_kind: str | None
    result_id: str | None
    result: dict[str, Any] | None
    error_code: str | None
    created_at: int
    updated_at: int

    @classmethod
    def from_record(cls,
                    record: catalog_state.OperationRecord) -> 'OperationView':
        result = record.result
        if isinstance(result, dict) and 'nonce' in result:
            result = dict(result)
            nonce = result.pop('nonce')
            if isinstance(nonce, str):
                result['nonce_hash'] = hashlib.sha256(
                    nonce.encode()).hexdigest()
        return cls(id=record.id,
                   kind=record.kind,
                   state=record.state.value,
                   result_kind=record.result_kind,
                   result_id=record.result_id,
                   result=result,
                   error_code=record.error_code,
                   created_at=record.created_at,
                   updated_at=record.updated_at)


class ArtifactView(_ApiModel):
    id: str
    workspace: str
    runtime_digest: str
    platform: str
    config_digest: str
    manifest_media_type: str
    manifest_size_bytes: int
    declared_size_bytes: int
    producer_kind: str
    producer_spec_hash: str | None
    builder_version: str | None
    created_at: int
    updated_at: int

    @classmethod
    def from_record(cls,
                    record: catalog_state.ArtifactRecord) -> 'ArtifactView':
        return cls(id=record.id,
                   workspace=record.workspace,
                   runtime_digest=record.runtime_digest,
                   platform=record.platform,
                   config_digest=record.config_digest,
                   manifest_media_type=record.manifest_media_type,
                   manifest_size_bytes=record.manifest_size_bytes,
                   declared_size_bytes=record.declared_size_bytes,
                   producer_kind=record.producer_kind,
                   producer_spec_hash=record.producer_spec_hash,
                   builder_version=record.builder_version,
                   created_at=record.created_at,
                   updated_at=record.updated_at)


class CatalogArtifactView(ArtifactView):
    releases: list[str]
    distributions: list[str]
    source_refs: list[str]
    targets: list[str]
    location_states: dict[str, int]

    @classmethod
    def from_summary(
        cls,
        record: catalog_state.ArtifactRecord,
        summary: dict[str, Any],
    ) -> 'CatalogArtifactView':
        return cls(**ArtifactView.from_record(record).model_dump(), **summary)


class SourceView(_ApiModel):
    id: str
    image_id: str
    source_ref: str
    source_root_digest: str
    source_root_media_type: str
    requested_platform: str
    selected_child_digest: str
    source_auth_binding_id: str | None
    source_auth_fingerprint: str | None
    created_at: int

    @classmethod
    def from_record(cls, record: catalog_state.SourceRecord) -> 'SourceView':
        value = record.__dict__.copy()
        value.pop('workspace')
        return cls(**value)


class PublicationView(_ApiModel):
    id: str
    operation_id: str
    profile_revision_id: str
    requested_release: str
    published_release: str | None
    reservation_active: bool
    source_ref: str
    source_root_digest: str
    requested_platform: str
    source_auth_binding_id: str | None
    source_auth_fingerprint: str | None
    state: str
    attempt_count: int
    next_retry_at: int | None
    error_code: str | None
    image_id: str | None
    source_id: str | None
    canonical_location_id: str | None
    created_at: int
    updated_at: int

    @classmethod
    def from_record(
            cls, record: catalog_state.PublicationRecord) -> 'PublicationView':
        return cls(id=record.id,
                   operation_id=record.operation_id,
                   profile_revision_id=record.profile_revision_id,
                   requested_release=record.requested_release,
                   published_release=record.published_release,
                   reservation_active=record.reservation_active,
                   source_ref=record.source_ref,
                   source_root_digest=record.source_root_digest,
                   requested_platform=record.requested_platform,
                   source_auth_binding_id=record.source_auth_binding_id,
                   source_auth_fingerprint=record.source_auth_fingerprint,
                   state=record.state.value,
                   attempt_count=record.attempt_count,
                   next_retry_at=record.next_retry_at,
                   error_code=record.error_code,
                   image_id=record.image_id,
                   source_id=record.source_id,
                   canonical_location_id=record.canonical_location_id,
                   created_at=record.created_at,
                   updated_at=record.updated_at)


class ReleaseView(_ApiModel):
    publication_id: str
    release: str
    profile_revision_id: str
    image_id: str
    published_at: int

    @classmethod
    def from_record(cls,
                    record: catalog_state.PublicationRecord) -> 'ReleaseView':
        if record.published_release is None or record.image_id is None:
            raise ValueError('Only READY publications are releases.')
        return cls(publication_id=record.id,
                   release=record.published_release,
                   profile_revision_id=record.profile_revision_id,
                   image_id=record.image_id,
                   published_at=record.updated_at)


class LocationView(_ApiModel):
    id: str
    image_id: str
    shard_id: str
    distribution: str
    target_id: str
    target_fingerprint: str
    physical_fingerprint: str
    runtime_digest: str
    canonical: bool
    canonical_location_id: str | None
    target_ref: str
    state: str
    attempt_count: int
    next_retry_at: int | None
    error_code: str | None
    last_verified_at: int | None
    last_used_at: int | None
    reserved_declared_bytes: int
    created_at: int
    updated_at: int

    @classmethod
    def from_record(cls, record: topology_state.LocationRecord, target_id: str,
                    distribution: str) -> 'LocationView':
        return cls(id=record.id,
                   image_id=record.image_id,
                   shard_id=record.shard_id,
                   distribution=distribution,
                   target_id=target_id,
                   target_fingerprint=record.target_fingerprint,
                   physical_fingerprint=record.physical_fingerprint,
                   runtime_digest=record.runtime_digest,
                   canonical=record.canonical,
                   canonical_location_id=record.canonical_location_id,
                   target_ref=record.target_ref,
                   state=record.state.value,
                   attempt_count=record.attempt_count,
                   next_retry_at=record.next_retry_at,
                   error_code=record.error_code,
                   last_verified_at=record.last_verified_at,
                   last_used_at=record.last_used_at,
                   reserved_declared_bytes=record.reserved_declared_bytes,
                   created_at=record.created_at,
                   updated_at=record.updated_at)


class DemandView(_ApiModel):
    id: str
    consumer_kind: str
    consumer_owner: str
    consumer_generation: int
    target_key: str
    retry_epoch: int
    image_id: str
    runtime_digest: str
    profile_revision_id: str
    target_fingerprint: str
    location_id: str
    placement: dict[str, Any]
    state: str
    error_code: str | None
    consumer_attached: bool
    terminal_at: int | None
    created_at: int
    updated_at: int

    @classmethod
    def from_record(cls, record: demand_state.DemandRecord) -> 'DemandView':
        return cls(id=record.id,
                   consumer_kind=record.consumer_kind,
                   consumer_owner=record.consumer_owner,
                   consumer_generation=record.consumer_generation,
                   target_key=record.target_key,
                   retry_epoch=record.retry_epoch,
                   image_id=record.image_id,
                   runtime_digest=record.runtime_digest,
                   profile_revision_id=record.profile_revision_id,
                   target_fingerprint=record.target_fingerprint,
                   location_id=record.location_id,
                   placement=record.placement,
                   state=record.state.value,
                   error_code=record.error_code,
                   consumer_attached=record.consumer_attached,
                   terminal_at=record.terminal_at,
                   created_at=record.created_at,
                   updated_at=record.updated_at)


class ProfileView(_ApiModel):
    id: str
    profile: str
    revision: int
    desired_generation: int
    state: str
    config_hash: str
    terraform_hash: str | None
    physical_manifest_hash: str
    attestations: dict[str, Any]
    attestations_hash: str | None
    qualified_at: int | None
    failed_code: str | None
    created_at: int
    updated_at: int

    @classmethod
    def from_record(
            cls, record: topology_state.ProfileRevisionRecord) -> 'ProfileView':
        return cls(id=record.id,
                   profile=record.profile,
                   revision=record.revision,
                   desired_generation=record.desired_generation,
                   state=record.state.value,
                   config_hash=record.config_hash,
                   terraform_hash=record.terraform_hash,
                   physical_manifest_hash=record.physical_manifest_hash,
                   attestations=record.attestations,
                   attestations_hash=record.attestations_hash,
                   qualified_at=record.qualified_at,
                   failed_code=record.failed_code,
                   created_at=record.created_at,
                   updated_at=record.updated_at)


class WorkerView(_ApiModel):
    id: str
    kind: str
    version: str
    started_at: int
    heartbeat_at: int
    last_success_at: int | None
    in_flight: int
    max_in_flight: int

    @classmethod
    def from_record(cls, record: topology_state.WorkerRecord) -> 'WorkerView':
        return cls(id=record.id,
                   kind=record.kind.value,
                   version=record.version,
                   started_at=record.started_at,
                   heartbeat_at=record.heartbeat_at,
                   last_success_at=record.last_success_at,
                   in_flight=record.in_flight,
                   max_in_flight=record.max_in_flight)


class ProviderBudgetView(_ApiModel):
    provider: str
    partition: str
    account: str
    region: str
    api_family: str
    applied_rate_per_second: float
    burst: float
    available_tokens: float
    blocked_until: int | None
    throttle_count: int
    updated_at: int

    @classmethod
    def from_record(
        cls,
        record: topology_state.ProviderBudgetRecord,
    ) -> 'ProviderBudgetView':
        return cls(provider=record.provider,
                   partition=record.partition,
                   account=record.account,
                   region=record.region,
                   api_family=record.api_family,
                   applied_rate_per_second=record.applied_rate_milli / 1000,
                   burst=record.burst_milli / 1000,
                   available_tokens=record.tokens_milli / 1000,
                   blocked_until=record.blocked_until,
                   throttle_count=record.throttle_count,
                   updated_at=record.updated_at)


class DistributionTargetView(_ApiModel):
    name: str
    region: str
    canonical: bool
    runtime_backends: list[str]
    runtime_ids: dict[str, list[str]]


class DistributionCapabilityView(_ApiModel):
    name: str
    revision: int
    active: bool
    targets: list[DistributionTargetView]


class CapabilitiesView(_ApiModel):
    version: Literal[1] = 1
    workspace: str
    read: bool
    use: bool
    publish: bool
    admin: bool
    workspace_mode: str
    default_distribution: str | None
    source_bindings: list[str]
    distributions: list[DistributionCapabilityView]


class MutationResult(_ApiModel):
    version: Literal[1] = 1
    kind: Literal['publication', 'location', 'profile_qualification',
                  'profile_canary']
    operation: OperationView
    publication: PublicationView | None = None
    location: LocationView | None = None
    profile: ProfileView | None = None


class Page(_ApiModel):
    version: Literal[1] = 1
    items: list[dict[str, Any]]
    next_cursor: str | None = None


class ReadinessView(_ApiModel):
    version: Literal[1] = 1
    catalog_authority: str
    catalog_authority_base32: str
    workspace: str
    workspace_policy: dict[str, Any]
    profiles: list[ProfileView]
    profiles_truncated: bool
    shards: list[dict[str, Any]]
    shards_truncated: bool
    workers: list[WorkerView]
    workers_truncated: bool
    provider_budgets: list[ProviderBudgetView]
    provider_budgets_truncated: bool
    queues: list[dict[str, Any]]
    generated_at: int
