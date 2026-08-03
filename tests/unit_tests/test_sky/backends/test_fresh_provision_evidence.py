"""Tests for one-shot backend system-OOM recovery evidence."""

import concurrent.futures
import copy
import dataclasses
import pickle

import pytest

from sky.backends import system_oom_recovery
from sky.provision import common as provision_common


def _provision_record(
    *,
    provider_name: str = 'aws',
    cluster_name: str = 'provider-cluster',
    head_instance_id: str = 'i-head',
    resumed_instance_ids: list[str] | None = None,
    created_instance_ids: list[str] | None = None,
    include_identity: bool = True,
) -> provision_common.ProvisionRecord:
    created_ids = (['i-head']
                   if created_instance_ids is None else created_instance_ids)
    return provision_common.ProvisionRecord(
        provider_name=provider_name,
        region='us-east-1',
        zone='us-east-1a',
        cluster_name=cluster_name,
        head_instance_id=head_instance_id,
        resumed_instance_ids=(resumed_instance_ids or []),
        created_instance_ids=created_ids,
        fresh_aws_instance_identity=(provision_common.AWSInstanceIdentity(
            aws_account_id='123456789012',
            region='us-east-1',
            availability_zone='us-east-1a',
            ec2_instance_id=head_instance_id,
            instance_type='g6.xlarge',
            market_type='on_demand') if include_identity else None))


def _evidence(
    provision_record: provision_common.ProvisionRecord | None = None,
    **overrides,
) -> system_oom_recovery.FreshProvisionEvidence:
    kwargs = {
        'request_id': 'request-1',
        'workspace': 'default',
        'cluster_name': 'replica-1',
        'cluster_name_on_cloud': 'provider-cluster',
        'cluster_hash': 'cluster-generation-1',
        'provider_name': 'aws',
        'requested_node_count': 1,
        'service_name': 'boltz-l4-fleet',
        'service_hash': 'service-hash-1',
        'cloud_user_identity': ['aws-user-id', '123456789012'],
        'catalog_instance_type': 'g6.xlarge',
        'catalog_memory_gib': 16.0,
        'cluster_existed': False,
        'dryrun': False,
        'provisioning_skipped': False,
    }
    kwargs.update(overrides)
    return system_oom_recovery.FreshProvisionEvidence.from_provision_record(
        provision_record or _provision_record(), **kwargs)


def test_fresh_provision_evidence_is_immutable_and_canonical():
    evidence = _evidence()

    assert evidence.created_instance_ids == ('i-head',)
    assert evidence.provision_owner_identity == ('aws-user-id', '123456789012')
    with pytest.raises(dataclasses.FrozenInstanceError):
        evidence.cluster_hash = 'changed'  # type: ignore[misc]


def test_fresh_provision_evidence_preserves_empty_workspace_text():
    assert _evidence(workspace='').workspace == ''


@pytest.mark.parametrize(('record', 'overrides'), [
    (_provision_record(resumed_instance_ids=['i-head']), {}),
    (_provision_record(created_instance_ids=[]), {}),
    (_provision_record(created_instance_ids=['i-head', 'i-head']), {}),
    (_provision_record(head_instance_id='i-missing'), {}),
    (_provision_record(include_identity=False), {}),
    (_provision_record(provider_name='gcp'), {}),
    (_provision_record(cluster_name='other'), {}),
    (_provision_record(), {
        'cluster_existed': True
    }),
    (_provision_record(), {
        'dryrun': True
    }),
    (_provision_record(), {
        'provisioning_skipped': True
    }),
    (_provision_record(), {
        'requested_node_count': True
    }),
    (_provision_record(), {
        'requested_node_count': 2
    }),
    (_provision_record(), {
        'cloud_user_identity': None
    }),
    (_provision_record(), {
        'cloud_user_identity': ['aws-user-id', '999999999999']
    }),
])
def test_fresh_provision_evidence_rejects_ambiguous_results(record, overrides):
    with pytest.raises(ValueError):
        _evidence(record, **overrides)


def test_fresh_provision_evidence_lease_aliases_share_one_take():
    evidence = _evidence()
    lease = system_oom_recovery.FreshProvisionEvidenceLease(evidence)
    alias = lease

    assert alias.take() is evidence
    assert lease.take() is None
    assert alias.take() is None


def test_fresh_provision_evidence_lease_take_is_atomic():
    evidence = _evidence()
    lease = system_oom_recovery.FreshProvisionEvidenceLease(evidence)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(lambda _: lease.take(), range(64)))

    assert sum(result is evidence for result in results) == 1
    assert sum(result is None for result in results) == 63


@pytest.mark.parametrize('operation', [copy.copy, copy.deepcopy, pickle.dumps])
def test_fresh_provision_evidence_lease_cannot_be_copied_or_serialized(
        operation):
    lease = system_oom_recovery.FreshProvisionEvidenceLease(_evidence())

    with pytest.raises(TypeError):
        operation(lease)
    # A rejected copy/serialization attempt must not consume the lease.
    assert lease.take() is not None
