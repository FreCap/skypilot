"""Tests for immutable paid-provider allocation evidence."""
import dataclasses
import hashlib
import json
from typing import Any

import pytest

from sky.serve import paid_capacity

_ASSOCIATION_ID = '11111111-1111-4111-8111-111111111111'
_REPLICA_RECORD_ID = '22222222-2222-4222-8222-222222222222'
_PROFILE_DIGEST = 'a' * 64


def _pool_key(*,
              provider: str = 'gcp',
              workspace: str = 'default',
              region: str = 'us-central1',
              zone: str | None = 'us-central1-a',
              instance_type: str = 'g2-standard-4',
              num_nodes: int = 2,
              use_spot: bool = True,
              provider_identity: str | None = None,
              accelerator: str = 'l4') -> str:
    payload: dict[str, object] = {
        'version': 1,
        'workspace': workspace,
        'cloud': provider,
        'region': region,
        'zone': zone,
        'instance_type': instance_type,
        'accelerators': [[accelerator, 1]],
        'use_spot': use_spot,
        'num_nodes': num_nodes,
    }
    if provider == 'aws':
        payload['version'] = 2
        payload['provider_identity'] = {
            'aws_account_id': provider_identity,
        }
    return json.dumps(payload, sort_keys=True, separators=(',', ':'))


def _receipt(**updates: Any) -> paid_capacity.PaidProviderAllocationReceipt:
    values: dict[str, Any] = {
        'association_id': _ASSOCIATION_ID,
        'replica_record_id': _REPLICA_RECORD_ID,
        'provider': 'gcp',
        'workspace': 'default',
        'provider_identity': None,
        'provider_project_id': 'boltz-spot-project',
        'region': 'us-central1',
        'zone': 'us-central1-a',
        'instance_type': 'g2-standard-4',
        'cluster_name_on_cloud': 'sky-svc-7',
        'requested_num_nodes': 2,
        'head_instance_id': 'instance-a',
        'created_instance_ids': ('instance-a', 'instance-b'),
        'resumed_instance_ids': (),
        'use_spot': True,
    }
    values.update(updates)
    return paid_capacity.PaidProviderAllocationReceipt(**values)


def test_receipt_is_frozen_canonical_and_authority_bound() -> None:
    receipt = _receipt()
    pool_key = _pool_key()
    payload = receipt.canonical_payload(pool_key=pool_key,
                                        profile_digest=_PROFILE_DIGEST)

    assert payload == {
        'association_id': _ASSOCIATION_ID,
        'cluster_name_on_cloud': 'sky-svc-7',
        'contract': 'in-tree-full-fresh-running-v1',
        'created_instance_ids': ['instance-a', 'instance-b'],
        'head_instance_id': 'instance-a',
        'instance_type': 'g2-standard-4',
        'paid_capacity_pool_key': pool_key,
        'profile_digest': _PROFILE_DIGEST,
        'provider': 'gcp',
        'provider_identity': None,
        'provider_project_id': 'boltz-spot-project',
        'region': 'us-central1',
        'replica_record_id': _REPLICA_RECORD_ID,
        'requested_num_nodes': 2,
        'resumed_instance_ids': [],
        'use_spot': True,
        'workspace': 'default',
        'zone': 'us-central1-a',
    }
    expected_digest = hashlib.sha256(
        json.dumps(payload,
                   sort_keys=True,
                   separators=(',', ':'),
                   allow_nan=False).encode('utf-8')).hexdigest()
    assert receipt.sha256(pool_key=pool_key,
                          profile_digest=_PROFILE_DIGEST) == expected_digest
    assert receipt.sha256(pool_key=pool_key,
                          profile_digest='b' * 64) != expected_digest
    assert _receipt(provider_project_id='other-project').sha256(
        pool_key=pool_key, profile_digest=_PROFILE_DIGEST) != expected_digest
    alternate_shape = _pool_key(accelerator='a100')
    assert receipt.sha256(pool_key=alternate_shape,
                          profile_digest=_PROFILE_DIGEST) != expected_digest
    with pytest.raises(dataclasses.FrozenInstanceError):
        receipt.head_instance_id = 'instance-b'  # type: ignore[misc]


@pytest.mark.parametrize(('updates', 'message'), [
    ({
        'contract': 'future-v2'
    }, 'contract'),
    ({
        'association_id': f'{{{_ASSOCIATION_ID}}}'
    }, 'noncanonical'),
    ({
        'replica_record_id': 'not-a-uuid'
    }, 'not a UUID'),
    ({
        'provider': 'azure'
    }, 'provider is unknown'),
    ({
        'workspace': ''
    }, 'workspace must be nonempty'),
    ({
        'region': ''
    }, 'region must be nonempty'),
    ({
        'zone': ''
    }, 'zone must be nonempty'),
    ({
        'instance_type': ''
    }, 'instance_type must be nonempty'),
    ({
        'cluster_name_on_cloud': ''
    }, 'cluster_name_on_cloud must be nonempty'),
    ({
        'requested_num_nodes': True
    }, 'node count'),
    ({
        'requested_num_nodes': 0
    }, 'node count'),
    ({
        'created_instance_ids': ['instance-a', 'instance-b']
    }, 'must be a tuple'),
    ({
        'created_instance_ids': ('instance-b', 'instance-a')
    }, 'noncanonical'),
    ({
        'created_instance_ids': ('instance-a', 'instance-a')
    }, 'noncanonical'),
    ({
        'created_instance_ids': ('instance-a',)
    }, 'not a full fresh'),
    ({
        'resumed_instance_ids': ('old-instance',)
    }, 'cannot contain resumed'),
    ({
        'head_instance_id': 'instance-c'
    }, 'head was not freshly created'),
    ({
        'use_spot': False
    }, 'must be Spot'),
])
def test_receipt_rejects_noncanonical_or_non_fresh_evidence(
        updates: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _receipt(**updates)


def test_receipt_enforces_provider_identity_contract() -> None:
    with pytest.raises(ValueError, match='exact account ID'):
        _receipt(provider='aws')
    with pytest.raises(ValueError, match='exact account ID'):
        _receipt(provider='aws', provider_identity='123')
    with pytest.raises(ValueError, match='no AWS account identity'):
        _receipt(provider_identity='123456789012')
    with pytest.raises(ValueError, match='exact project ID'):
        _receipt(provider_project_id=None)
    with pytest.raises(ValueError, match='exact project ID'):
        _receipt(provider_project_id='')
    with pytest.raises(ValueError, match='cannot contain a GCP project ID'):
        _receipt(provider='aws', provider_identity='123456789012')

    aws_receipt = _receipt(provider='aws',
                           provider_identity='123456789012',
                           provider_project_id=None,
                           region='us-east-1',
                           zone='us-east-1a',
                           instance_type='g6.xlarge')
    aws_pool = _pool_key(provider='aws',
                         provider_identity='123456789012',
                         region='us-east-1',
                         zone='us-east-1a',
                         instance_type='g6.xlarge')
    aws_receipt.validate_pool_key(aws_pool)

    legacy_aws_pool = json.loads(aws_pool)
    legacy_aws_pool['version'] = 1
    del legacy_aws_pool['provider_identity']
    with pytest.raises(ValueError, match='disagree'):
        aws_receipt.validate_pool_key(
            json.dumps(legacy_aws_pool, sort_keys=True, separators=(',', ':')))


@pytest.mark.parametrize('pool_updates', [
    {
        'provider': 'aws',
        'provider_identity': '123456789012'
    },
    {
        'workspace': 'other'
    },
    {
        'region': 'us-east1'
    },
    {
        'zone': 'us-central1-b'
    },
    {
        'instance_type': 'g2-standard-8'
    },
    {
        'num_nodes': 1
    },
    {
        'use_spot': False
    },
])
def test_receipt_rejects_a_different_provider_pool(
        pool_updates: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _receipt().validate_pool_key(_pool_key(**pool_updates))


def test_receipt_requires_canonical_pool_and_profile_digest() -> None:
    receipt = _receipt()
    pool_key = _pool_key()
    receipt.validate_pool_key(pool_key)

    with pytest.raises(ValueError, match='noncanonical'):
        receipt.validate_pool_key(json.dumps(json.loads(pool_key), indent=2))
    bad_digests: tuple[Any, ...] = ('A' * 64, 'a' * 63, '', None)
    for digest in bad_digests:
        with pytest.raises(ValueError, match='profile digest'):
            receipt.sha256(pool_key=pool_key, profile_digest=digest)
