"""Tests for the paid provider-allocation backend checkpoint."""
# pylint: disable=protected-access

import dataclasses
import types
from typing import Any
from unittest import mock
import uuid

import pytest

from sky import clouds
from sky.backends import cloud_vm_ray_backend as backend
from sky.provision import common as provision_common
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
from sky.server.requests import ordinary_launch as ordinary_launch_request

_ASSOCIATION_ID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_REPLICA_RECORD_ID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_LAUNCH_CONTEXT = {
    ordinary_launch_binding.BINDING_PROTOCOL_VERSION_KEY: 2,
}


def _builtin_bulk_provision() -> None:
    """Stable identity sentinel for the in-tree provisioner."""


def _custom_bulk_provision() -> None:
    """Stable identity sentinel for an opaque provisioner."""


def _install_bound_context(
    monkeypatch: pytest.MonkeyPatch,
    kind: ordinary_launch_binding.NonPoolLaunchProfileKind = (
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID),
) -> None:
    context = types.SimpleNamespace(
        association_id=_ASSOCIATION_ID,
        replica_record_id=_REPLICA_RECORD_ID,
        profile=types.SimpleNamespace(kind=kind),
    )
    monkeypatch.setattr(ordinary_launch_binding, 'has_bound_launch_context',
                        lambda _: True)
    monkeypatch.setattr(ordinary_launch_binding,
                        'parse_bound_non_pool_launch_context',
                        lambda _: context)


def _gcp_identity(**updates: Any) -> provision_common.GCPInstanceIdentity:
    values: dict[str, Any] = {
        'project_id': 'boltz-spot-project',
        'zone': 'us-central1-a',
        'instance_name': 'instance-a',
        'instance_type': 'g2-standard-4',
        'market_type': 'spot',
    }
    values.update(updates)
    return provision_common.GCPInstanceIdentity(**values)


def _gcp_record(**updates: Any) -> provision_common.ProvisionRecord:
    values: dict[str, Any] = {
        'provider_name': 'gcp',
        'region': 'us-central1',
        'zone': 'us-central1-a',
        'cluster_name': 'sky-service-7',
        'head_instance_id': 'instance-a',
        'resumed_instance_ids': [],
        'created_instance_ids': ['instance-a'],
        'fresh_gcp_instance_identity': _gcp_identity(),
    }
    values.update(updates)
    return provision_common.ProvisionRecord(**values)


def _aws_identity(**updates: Any) -> provision_common.AWSInstanceIdentity:
    values: dict[str, Any] = {
        'aws_account_id': '123456789012',
        'region': 'us-east-1',
        'availability_zone': 'us-east-1a',
        'ec2_instance_id': 'i-head',
        'instance_type': 'g6.xlarge',
        'market_type': 'spot',
    }
    values.update(updates)
    return provision_common.AWSInstanceIdentity(**values)


def _aws_record(
    identity: provision_common.AWSInstanceIdentity | None,
) -> provision_common.ProvisionRecord:
    return provision_common.ProvisionRecord(
        provider_name='aws',
        region='us-east-1',
        zone='us-east-1a',
        cluster_name='sky-service-8',
        head_instance_id='i-head',
        resumed_instance_ids=[],
        created_instance_ids=['i-head'],
        fresh_aws_instance_identity=identity,
    )


def _record_gcp(
    *,
    provision_record: provision_common.ProvisionRecord | None = None,
    cluster_existed: bool = False,
    use_spot: bool = True,
    requested_num_nodes: int = 1,
    bulk_provision_fn=_builtin_bulk_provision,
) -> bool:
    return backend._record_full_fresh_paid_provider_allocation(
        launch_context=_LAUNCH_CONTEXT,
        cloud=clouds.GCP(),
        workspace='prod-multiregion',
        region=clouds.Region('us-central1'),
        instance_type='g2-standard-4',
        use_spot=use_spot,
        requested_num_nodes=requested_num_nodes,
        cluster_existed=cluster_existed,
        cluster_name_on_cloud='sky-service-7',
        provision_record=provision_record or _gcp_record(),
        bulk_provision_fn=bulk_provision_fn,
        builtin_bulk_provision_fn=_builtin_bulk_provision,
    )


def test_full_fresh_gcp_spot_records_one_typed_receipt(
        monkeypatch: pytest.MonkeyPatch) -> None:
    _install_bound_context(monkeypatch)
    record_allocation = mock.Mock(return_value=(
        ordinary_launch_binding.ProviderAllocationDisposition.RECORDED))
    monkeypatch.setattr(ordinary_launch_request,
                        '_record_paid_provider_allocation', record_allocation)

    assert _record_gcp()
    record_allocation.assert_called_once()
    launch_context, receipt = record_allocation.call_args.args
    assert launch_context is _LAUNCH_CONTEXT
    assert isinstance(receipt, paid_capacity.PaidProviderAllocationReceipt)
    assert dataclasses.asdict(receipt) == {
        'association_id': str(_ASSOCIATION_ID),
        'replica_record_id': str(_REPLICA_RECORD_ID),
        'provider': 'gcp',
        'workspace': 'prod-multiregion',
        'provider_identity': None,
        'provider_project_id': 'boltz-spot-project',
        'region': 'us-central1',
        'zone': 'us-central1-a',
        'instance_type': 'g2-standard-4',
        'cluster_name_on_cloud': 'sky-service-7',
        'requested_num_nodes': 1,
        'head_instance_id': 'instance-a',
        'created_instance_ids': ('instance-a',),
        'resumed_instance_ids': (),
        'use_spot': True,
        'contract': paid_capacity.PAID_PROVIDER_ALLOCATION_CONTRACT,
    }


@pytest.mark.parametrize(
    ('record_updates', 'call_updates'),
    [
        ({
            'created_instance_ids': [],
            'resumed_instance_ids': ['instance-b'],
        }, {}),
        ({
            'created_instance_ids': [],
        }, {}),
        ({
            'created_instance_ids': ['instance-a', 'instance-b'],
        }, {
            'requested_num_nodes': 2,
        }),
        ({}, {
            'cluster_existed': True,
        }),
        ({}, {
            'bulk_provision_fn': _custom_bulk_provision,
        }),
        ({}, {
            'use_spot': False,
        }),
    ],
    ids=('resumed', 'partial', 'multi-node', 'existing', 'custom-provisioner',
         'non-spot'),
)
def test_gcp_checkpoint_skips_ineligible_provider_results(
        monkeypatch: pytest.MonkeyPatch, record_updates: dict[str, Any],
        call_updates: dict[str, Any]) -> None:
    _install_bound_context(monkeypatch)
    record_allocation = mock.Mock()
    monkeypatch.setattr(ordinary_launch_request,
                        '_record_paid_provider_allocation', record_allocation)

    assert not _record_gcp(provision_record=_gcp_record(**record_updates),
                           **call_updates)
    record_allocation.assert_not_called()


@pytest.mark.parametrize('identity', [
    None,
    _gcp_identity(instance_name='other-instance'),
    _gcp_identity(zone='us-central1-b'),
    _gcp_identity(instance_type='g2-standard-8'),
    _gcp_identity(market_type='on_demand'),
],
                         ids=('missing', 'wrong-instance', 'wrong-zone',
                              'wrong-instance-type', 'on-demand'))
def test_gcp_checkpoint_rejects_inexact_fresh_identity(
        monkeypatch: pytest.MonkeyPatch,
        identity: provision_common.GCPInstanceIdentity | None) -> None:
    _install_bound_context(monkeypatch)
    record_allocation = mock.Mock()
    monkeypatch.setattr(ordinary_launch_request,
                        '_record_paid_provider_allocation', record_allocation)

    assert not _record_gcp(provision_record=_gcp_record(
        fresh_gcp_instance_identity=identity))
    record_allocation.assert_not_called()


@pytest.mark.parametrize(
    'profile_kind',
    [
        ordinary_launch_binding.NonPoolLaunchProfileKind.
        UNKNOWN_CAPACITY_REPLACEMENT,
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_ZERO_COST,
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
    ],
)
def test_checkpoint_skips_every_non_ordinary_paid_profile(
        monkeypatch: pytest.MonkeyPatch,
        profile_kind: ordinary_launch_binding.NonPoolLaunchProfileKind) -> None:
    _install_bound_context(monkeypatch, profile_kind)
    record_allocation = mock.Mock()
    monkeypatch.setattr(ordinary_launch_request,
                        '_record_paid_provider_allocation', record_allocation)

    assert not _record_gcp()
    record_allocation.assert_not_called()


def test_full_fresh_aws_spot_requires_and_records_exact_identity(
        monkeypatch: pytest.MonkeyPatch) -> None:
    _install_bound_context(monkeypatch)
    record_allocation = mock.Mock(return_value=(
        ordinary_launch_binding.ProviderAllocationDisposition.RECORDED))
    monkeypatch.setattr(ordinary_launch_request,
                        '_record_paid_provider_allocation', record_allocation)

    assert backend._record_full_fresh_paid_provider_allocation(
        launch_context=_LAUNCH_CONTEXT,
        cloud=clouds.AWS(),
        workspace='prod-multiregion',
        region=clouds.Region('us-east-1'),
        instance_type='g6.xlarge',
        use_spot=True,
        requested_num_nodes=1,
        cluster_existed=False,
        cluster_name_on_cloud='sky-service-8',
        provision_record=_aws_record(_aws_identity()),
        bulk_provision_fn=_builtin_bulk_provision,
        builtin_bulk_provision_fn=_builtin_bulk_provision,
    )
    record_allocation.assert_called_once()
    launch_context, receipt = record_allocation.call_args.args
    assert launch_context is _LAUNCH_CONTEXT
    assert isinstance(receipt, paid_capacity.PaidProviderAllocationReceipt)
    assert receipt.provider == 'aws'
    assert receipt.provider_identity == '123456789012'
    assert receipt.created_instance_ids == ('i-head',)
    assert receipt.provider_project_id is None


@pytest.mark.parametrize(
    'identity',
    [
        None,
        _aws_identity(ec2_instance_id='i-other'),
        _aws_identity(region='us-west-2'),
        _aws_identity(availability_zone='us-east-1b'),
        _aws_identity(instance_type='g6.2xlarge'),
        _aws_identity(market_type='on_demand'),
    ],
    ids=('missing', 'wrong-instance', 'wrong-region', 'wrong-zone',
         'wrong-instance-type', 'on-demand'),
)
def test_aws_checkpoint_rejects_inexact_fresh_identity(
        monkeypatch: pytest.MonkeyPatch,
        identity: provision_common.AWSInstanceIdentity | None) -> None:
    _install_bound_context(monkeypatch)
    record_allocation = mock.Mock()
    monkeypatch.setattr(ordinary_launch_request,
                        '_record_paid_provider_allocation', record_allocation)

    assert not backend._record_full_fresh_paid_provider_allocation(
        launch_context=_LAUNCH_CONTEXT,
        cloud=clouds.AWS(),
        workspace='prod-multiregion',
        region=clouds.Region('us-east-1'),
        instance_type='g6.xlarge',
        use_spot=True,
        requested_num_nodes=1,
        cluster_existed=False,
        cluster_name_on_cloud='sky-service-8',
        provision_record=_aws_record(identity),
        bulk_provision_fn=_builtin_bulk_provision,
        builtin_bulk_provision_fn=_builtin_bulk_provision,
    )
    record_allocation.assert_not_called()


@pytest.mark.parametrize('provider', ['aws', 'gcp'])
def test_checkpoint_rejects_multi_node_allocation(
        monkeypatch: pytest.MonkeyPatch, provider: str) -> None:
    _install_bound_context(monkeypatch)
    record_allocation = mock.Mock()
    monkeypatch.setattr(ordinary_launch_request,
                        '_record_paid_provider_allocation', record_allocation)
    if provider == 'aws':
        cloud = clouds.AWS()
        region = clouds.Region('us-east-1')
        instance_type = 'g6.xlarge'
        cluster_name = 'sky-service-8'
        record = _aws_record(_aws_identity())
        record.created_instance_ids = ['i-head', 'i-worker']
    else:
        cloud = clouds.GCP()
        region = clouds.Region('us-central1')
        instance_type = 'g2-standard-4'
        cluster_name = 'sky-service-7'
        record = _gcp_record(created_instance_ids=['instance-a', 'instance-b'])

    assert not backend._record_full_fresh_paid_provider_allocation(
        launch_context=_LAUNCH_CONTEXT,
        cloud=cloud,
        workspace='prod-multiregion',
        region=region,
        instance_type=instance_type,
        use_spot=True,
        requested_num_nodes=2,
        cluster_existed=False,
        cluster_name_on_cloud=cluster_name,
        provision_record=record,
        bulk_provision_fn=_builtin_bulk_provision,
        builtin_bulk_provision_fn=_builtin_bulk_provision,
    )
    record_allocation.assert_not_called()
