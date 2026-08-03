"""One-shot provisioning evidence for SkyServe system-OOM recovery.

This module contains only API-server-side evidence.  It is deliberately
separate from :mod:`sky.skylet.system_oom_recovery`, whose values are embedded
in generated code and run on a replica VM.
"""

import dataclasses
import math
import threading
from typing import Any, NoReturn, SupportsIndex

from sky.provision import common as provision_common


def _require_nonempty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f'{field_name} must be a nonempty string.')
    return value


@dataclasses.dataclass(frozen=True, slots=True)
class FreshProvisionEvidence:
    """Immutable identity of one complete provider create.

    Constructing this payload is not sufficient to enable recovery.  The
    backend stores it only inside :class:`FreshProvisionEvidenceLease` and
    revalidates every field against the resulting handle when it makes the
    one submission decision allowed to consume the lease.
    """

    request_id: str
    workspace: str
    cluster_name: str
    cluster_name_on_cloud: str
    cluster_hash: str
    provider_name: str
    requested_node_count: int
    head_instance_id: str
    created_instance_ids: tuple[str, ...]
    aws_account_id: str
    provision_owner_identity: tuple[str, ...] = dataclasses.field(repr=False)
    region: str
    availability_zone: str
    instance_type: str
    market_type: str
    catalog_memory_gib: float
    service_name: str
    service_hash: str

    def __post_init__(self) -> None:
        for field_name in ('request_id', 'cluster_name',
                           'cluster_name_on_cloud', 'cluster_hash',
                           'provider_name', 'head_instance_id', 'region',
                           'availability_zone', 'instance_type', 'service_name',
                           'service_hash'):
            _require_nonempty_string(getattr(self, field_name), field_name)
        if not isinstance(self.workspace, str):
            raise ValueError('workspace must be a string.')
        if self.provider_name != 'aws':
            raise ValueError('Fresh recovery evidence is AWS-only.')
        if (not isinstance(self.aws_account_id, str) or
                len(self.aws_account_id) != 12 or
                not self.aws_account_id.isdecimal()):
            raise ValueError('aws_account_id must contain exactly 12 digits.')
        if (not isinstance(self.provision_owner_identity, tuple) or
                len(self.provision_owner_identity) != 2 or
                any(not isinstance(value, str) or not value
                    for value in self.provision_owner_identity) or
                self.provision_owner_identity[-1] != self.aws_account_id):
            raise ValueError(
                'provision_owner_identity must bind the AWS account.')
        if self.market_type not in ('on_demand', 'spot'):
            raise ValueError('market_type must be on_demand or spot.')
        if (not isinstance(self.catalog_memory_gib, (int, float)) or
                isinstance(self.catalog_memory_gib, bool) or
                not math.isfinite(self.catalog_memory_gib) or
                self.catalog_memory_gib <= 0):
            raise ValueError('catalog_memory_gib must be positive and finite.')
        if (not isinstance(self.requested_node_count, int) or
                isinstance(self.requested_node_count, bool) or
                self.requested_node_count != 1):
            raise ValueError('requested_node_count must be exactly one.')
        if not isinstance(self.created_instance_ids, tuple):
            raise ValueError('created_instance_ids must be a tuple.')
        if any(not isinstance(instance_id, str) or not instance_id
               for instance_id in self.created_instance_ids):
            raise ValueError(
                'created_instance_ids must contain nonempty strings.')
        if len(set(self.created_instance_ids)) != len(
                self.created_instance_ids):
            raise ValueError('created_instance_ids must be unique.')
        if len(self.created_instance_ids) != self.requested_node_count:
            raise ValueError(
                'created_instance_ids must cover every requested node.')
        if self.head_instance_id not in self.created_instance_ids:
            raise ValueError(
                'head_instance_id must be one of created_instance_ids.')

    @classmethod
    def from_provision_record(
        cls,
        provision_record: provision_common.ProvisionRecord,
        *,
        request_id: str,
        workspace: str,
        cluster_name: str,
        cluster_name_on_cloud: str,
        cluster_hash: str,
        provider_name: str,
        requested_node_count: int,
        service_name: str,
        service_hash: str,
        cloud_user_identity: list[str] | None,
        catalog_instance_type: str,
        catalog_memory_gib: float,
        cluster_existed: bool,
        dryrun: bool,
        provisioning_skipped: bool,
    ) -> 'FreshProvisionEvidence':
        """Build evidence from one exact successful provider result.

        The caller must pass the request facts that surrounded the provider
        call.  Any result that could contain a reused or only partially
        created fleet is rejected rather than weakened into negative flags.
        """
        if cluster_existed:
            raise ValueError('An existing cluster is not a fresh provision.')
        if dryrun:
            raise ValueError('A dry run has no provider-create evidence.')
        if provisioning_skipped:
            raise ValueError('A skipped provision has no create evidence.')
        if provision_record.resumed_instance_ids:
            raise ValueError('Resumed instances are not fresh creates.')
        if provision_record.cluster_name != cluster_name_on_cloud:
            raise ValueError('Provisioned cluster identity does not match.')
        if provision_record.provider_name != provider_name:
            raise ValueError('Provisioned provider identity does not match.')
        if provider_name != 'aws':
            raise ValueError('Fresh recovery evidence is AWS-only.')
        if (not isinstance(cloud_user_identity, list) or
                len(cloud_user_identity) != 2 or
                any(not isinstance(value, str) or not value
                    for value in cloud_user_identity)):
            raise ValueError('AWS provision owner identity is unavailable.')
        provision_owner_identity = tuple(cloud_user_identity)
        identity = provision_record.fresh_aws_instance_identity
        if identity is None:
            raise ValueError('Fresh AWS instance identity is unavailable.')
        if (identity.ec2_instance_id != provision_record.head_instance_id or
                identity.region != provision_record.region or
                identity.availability_zone != provision_record.zone or
                identity.instance_type != catalog_instance_type or
                identity.aws_account_id != provision_owner_identity[-1]):
            raise ValueError('Fresh AWS instance identity does not match.')
        created_instance_ids = tuple(
            sorted(provision_record.created_instance_ids))
        return cls(
            request_id=request_id,
            workspace=workspace,
            cluster_name=cluster_name,
            cluster_name_on_cloud=cluster_name_on_cloud,
            cluster_hash=cluster_hash,
            provider_name=provider_name,
            requested_node_count=requested_node_count,
            head_instance_id=provision_record.head_instance_id,
            created_instance_ids=created_instance_ids,
            aws_account_id=identity.aws_account_id,
            provision_owner_identity=provision_owner_identity,
            region=identity.region,
            availability_zone=identity.availability_zone,
            instance_type=identity.instance_type,
            market_type=identity.market_type,
            catalog_memory_gib=float(catalog_memory_gib),
            service_name=service_name,
            service_hash=service_hash,
        )


class FreshProvisionEvidenceLease:
    """Non-copyable, non-serializable lease for one evidence payload."""

    __slots__ = ('_lock', '_payload')

    def __init__(self, payload: FreshProvisionEvidence) -> None:
        if not isinstance(payload, FreshProvisionEvidence):
            raise TypeError('payload must be FreshProvisionEvidence.')
        self._lock = threading.Lock()
        self._payload: FreshProvisionEvidence | None = payload

    def take(self) -> FreshProvisionEvidence | None:
        """Atomically consume and return the payload, at most once."""
        with self._lock:
            payload = self._payload
            self._payload = None
            return payload

    def __copy__(self) -> 'FreshProvisionEvidenceLease':
        raise TypeError('FreshProvisionEvidenceLease cannot be copied.')

    def __deepcopy__(self, memo: dict[int,
                                      Any]) -> 'FreshProvisionEvidenceLease':
        del memo
        raise TypeError('FreshProvisionEvidenceLease cannot be deep-copied.')

    def __reduce__(self) -> NoReturn:
        raise TypeError('FreshProvisionEvidenceLease cannot be serialized.')

    def __reduce_ex__(self, protocol: SupportsIndex) -> NoReturn:
        del protocol
        raise TypeError('FreshProvisionEvidenceLease cannot be serialized.')

    def __getstate__(self) -> object:
        raise TypeError('FreshProvisionEvidenceLease cannot be serialized.')
