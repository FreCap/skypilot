"""Unit tests for the post-ABSENT paid auxiliary authority boundary."""

import dataclasses
import uuid

import pytest

from sky.serve import serve_state
from sky.server.requests import postgres as request_postgres

_REPLICA_RECORD_ID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_REPLICA_INCARNATION = uuid.UUID('22222222-2222-4222-8222-222222222222')
_CLUSTER_RECORD_ID = uuid.UUID('33333333-3333-4333-8333-333333333333')


def _resource_action_identity(
        desired_generation: int = 1
) -> serve_state.ReplicaResourceActionIdentity:
    return serve_state.ReplicaResourceActionIdentity(
        replica_id=7,
        cluster_name='service-7',
        replica_incarnation=_REPLICA_INCARNATION,
        desired_generation=desired_generation,
        sky_cluster_record_uuid=_CLUSTER_RECORD_ID)


def test_projected_paid_auxiliary_authority_composes_exact_leaf_scope() -> None:
    provider_identity = {'project_id': 'project-a'}
    scope = request_postgres.ProjectedPaidProviderAbsenceCleanupScope(
        cloud='gcp', provider_identity=provider_identity)
    authority = request_postgres.ProjectedPaidAuxiliaryCleanupAuthority(
        service_name='service',
        replica_record_id=_REPLICA_RECORD_ID,
        resource_action_identity=_resource_action_identity(),
        cleanup_scope=scope)

    provider_identity['project_id'] = 'changed-after-construction'
    assert authority.cloud == 'gcp'
    assert authority.provider_identity == {'project_id': 'project-a'}
    with pytest.raises(dataclasses.FrozenInstanceError):
        authority.service_name = 'other-service'


def test_projected_paid_auxiliary_authority_rejects_noninitial_generation(
) -> None:
    with pytest.raises(ValueError, match='authority is malformed'):
        request_postgres.ProjectedPaidAuxiliaryCleanupAuthority(
            service_name='service',
            replica_record_id=_REPLICA_RECORD_ID,
            resource_action_identity=_resource_action_identity(
                desired_generation=2),
            cleanup_scope=(
                request_postgres.ProjectedPaidProviderAbsenceCleanupScope(
                    cloud='aws', provider_identity={'client_token': 'token'})))
