"""Deadline-bounded AWS EKS Pod Identity attestation."""

from collections.abc import Callable
from collections.abc import Mapping
import dataclasses
import datetime
import math
import threading
import time
from typing import Any

from sky.adaptors import common as adaptor_common

aws_adaptor = adaptor_common.LazyImport('sky.adaptors.aws')


class AwsAttestationError(RuntimeError):
    """AWS could not prove the exact code-owned provider inventory."""


class AwsAttestationNonconformanceError(AwsAttestationError):
    """Complete AWS reads disproved the expected reclaim inventory."""


@dataclasses.dataclass(frozen=True)
class PodIdentityProof:
    """Safe machine-readable result of one exact association read."""

    kubernetes_context: str
    cluster_arn: str
    namespace: str
    service_account_name: str
    expected_role_arn: str | None
    association_count: int
    identity_absence_proven: bool


def _remaining(deadline_monotonic: float,
               cancellation: threading.Event) -> float:
    if cancellation.is_set():
        raise AwsAttestationError('AWS attestation was cancelled.')
    remaining = deadline_monotonic - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise AwsAttestationError('AWS attestation exceeded its deadline.')
    return remaining


def _client_timeout(deadline_monotonic: float,
                    cancellation: threading.Event) -> float:
    # Bound connect plus read time below the remaining absolute budget.  Keep a
    # third of the horizon for local validation and the next API boundary.
    remaining = _remaining(deadline_monotonic, cancellation)
    return min(1.0, remaining / 3)


@dataclasses.dataclass(frozen=True)
class _TemporaryCredentials:
    access_key_id: str
    secret_access_key: str
    session_token: str
    expiration_epoch: float


class AuditSessionCache:
    """Caches only short-lived AssumeRole credentials, never observations."""

    def __init__(
        self,
        *,
        ambient_session_factory: Callable[..., Any] | None = None,
        assumed_session_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._ambient_session_factory = ambient_session_factory
        self._assumed_session_factory = assumed_session_factory
        self._condition = threading.Condition()
        self._credentials: dict[tuple[str, str], _TemporaryCredentials] = {}
        self._refreshing: set[tuple[str, str]] = set()

    @staticmethod
    def _parse_credentials(response: object) -> _TemporaryCredentials:
        if not isinstance(response, Mapping):
            raise AwsAttestationError('STS returned an invalid response.')
        value = response.get('Credentials')
        if not isinstance(value, Mapping):
            raise AwsAttestationError('STS returned no temporary credentials.')
        access_key_id = value.get('AccessKeyId')
        secret_access_key = value.get('SecretAccessKey')
        session_token = value.get('SessionToken')
        expiration = value.get('Expiration')
        if (not isinstance(access_key_id, str) or not access_key_id or
                not isinstance(secret_access_key, str) or
                not secret_access_key or not isinstance(session_token, str) or
                not session_token):
            raise AwsAttestationError('STS returned invalid credentials.')
        if isinstance(expiration, datetime.datetime):
            expiration_epoch = expiration.timestamp()
        elif isinstance(expiration, str):
            try:
                expiration_epoch = datetime.datetime.fromisoformat(
                    expiration.replace('Z', '+00:00')).timestamp()
            except ValueError as error:
                raise AwsAttestationError(
                    'STS returned an invalid credential expiration.') from error
        else:
            raise AwsAttestationError('STS returned no credential expiration.')
        if not math.isfinite(expiration_epoch):
            raise AwsAttestationError(
                'STS returned an invalid credential expiration.')
        return _TemporaryCredentials(access_key_id, secret_access_key,
                                     session_token, expiration_epoch)

    def _assume(self, role_arn: str, region: str, deadline_monotonic: float,
                cancellation: threading.Event) -> _TemporaryCredentials:
        timeout = _client_timeout(deadline_monotonic, cancellation)
        factory = self._ambient_session_factory
        if factory is None:
            factory = aws_adaptor.session_with_client_defaults
        ambient_session = factory(connect_timeout=timeout,
                                  read_timeout=timeout,
                                  total_max_attempts=1)
        sts = ambient_session.client('sts', region_name=region)
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName='skypilot-reserved-fill-reclaim-audit',
            DurationSeconds=900)
        _remaining(deadline_monotonic, cancellation)
        return self._parse_credentials(response)

    def session(self, role_arn: str, region: str, deadline_monotonic: float,
                cancellation: threading.Event) -> Any:
        """Return a per-call boto3 Session from cached temporary credentials."""
        key = (role_arn, region)
        while True:
            remaining = _remaining(deadline_monotonic, cancellation)
            with self._condition:
                credentials = self._credentials.get(key)
                if (credentials is not None and
                        credentials.expiration_epoch > time.time() + 60):
                    break
                if key not in self._refreshing:
                    self._refreshing.add(key)
                    credentials = None
                    break
                self._condition.wait(timeout=min(0.05, remaining))
        if credentials is None:
            try:
                credentials = self._assume(role_arn, region, deadline_monotonic,
                                           cancellation)
                with self._condition:
                    self._credentials[key] = credentials
            finally:
                with self._condition:
                    self._refreshing.discard(key)
                    self._condition.notify_all()
        _remaining(deadline_monotonic, cancellation)
        factory = self._assumed_session_factory
        if factory is None:
            factory = aws_adaptor.boto3.session.Session
        return factory(aws_access_key_id=credentials.access_key_id,
                       aws_secret_access_key=credentials.secret_access_key,
                       aws_session_token=credentials.session_token,
                       region_name=region)


def _require_cluster(response: object, expected: Mapping[str, Any]) -> None:
    if not isinstance(response, Mapping):
        raise AwsAttestationError('EKS returned an invalid cluster response.')
    cluster = response.get('cluster')
    if not isinstance(cluster, Mapping):
        raise AwsAttestationError('EKS returned no cluster inventory.')
    if (cluster.get('name') != expected['cluster_name'] or
            cluster.get('arn') != expected['cluster_arn'] or
            cluster.get('status') != 'ACTIVE'):
        raise AwsAttestationNonconformanceError(
            'EKS did not return the exact active cluster inventory.')


def _list_associations(
        eks: Any, *, cluster_name: str, namespace: str, service_account: str,
        deadline_monotonic: float,
        cancellation: threading.Event) -> list[Mapping[str, Any]]:
    associations: list[Mapping[str, Any]] = []
    next_token: str | None = None
    seen_tokens: set[str] = set()
    for _ in range(1000):
        _remaining(deadline_monotonic, cancellation)
        request: dict[str, Any] = {
            'clusterName': cluster_name,
            'namespace': namespace,
            'serviceAccount': service_account,
            'maxResults': 100,
        }
        if next_token is not None:
            request['nextToken'] = next_token
        response = eks.list_pod_identity_associations(**request)
        if not isinstance(response, Mapping):
            raise AwsAttestationError(
                'EKS returned an invalid Pod Identity page.')
        page = response.get('associations')
        if not isinstance(page, list):
            raise AwsAttestationError(
                'EKS returned an invalid Pod Identity inventory.')
        for summary in page:
            if not isinstance(summary, Mapping):
                raise AwsAttestationError(
                    'EKS returned an invalid Pod Identity summary.')
            associations.append(summary)
        token = response.get('nextToken')
        if token is None:
            return associations
        if not isinstance(token, str) or not token or token in seen_tokens:
            raise AwsAttestationError(
                'EKS returned an invalid Pod Identity pagination token.')
        seen_tokens.add(token)
        next_token = token
    raise AwsAttestationError('EKS returned too many Pod Identity pages.')


def validate_pod_identity_inventory(
    associations: list[Mapping[str, Any]],
    *,
    described_association: Mapping[str, Any] | None,
    cluster_name: str,
    namespace: str,
    service_account: str,
    expected_role_arn: str | None,
) -> None:
    """Validate exact absence or one exact association from fresh AWS reads."""
    if expected_role_arn is None:
        if associations or described_association is not None:
            raise AwsAttestationNonconformanceError(
                'The identity-free worker partition has a Pod Identity '
                'association.')
        return
    if len(associations) != 1 or described_association is None:
        raise AwsAttestationNonconformanceError(
            'The worker partition does not have exactly one Pod Identity '
            'association.')
    summary = associations[0]
    association_id = summary.get('associationId')
    association_arn = summary.get('associationArn')
    if (not isinstance(association_id, str) or not association_id or
            not isinstance(association_arn, str) or not association_arn or
            summary.get('clusterName') != cluster_name or
            summary.get('namespace') != namespace or
            summary.get('serviceAccount') != service_account):
        raise AwsAttestationError(
            'EKS returned an inconsistent Pod Identity summary.')
    if (described_association.get('associationId') != association_id or
            described_association.get('associationArn') != association_arn or
            described_association.get('clusterName') != cluster_name or
            described_association.get('namespace') != namespace or
            described_association.get('serviceAccount') != service_account or
            described_association.get('roleArn') != expected_role_arn or
            described_association.get('targetRoleArn') not in (None, '') or
            described_association.get('ownerArn') != summary.get('ownerArn')):
        raise AwsAttestationNonconformanceError(
            'EKS returned an unexpected Pod Identity association.')


def attest_pod_identity(
    fleet_context: Mapping[str, Any],
    provider_context: Mapping[str, Any],
    *,
    deadline_monotonic: float,
    cancellation: threading.Event,
    session_cache: AuditSessionCache,
) -> PodIdentityProof:
    """Prove one cluster and its exact worker Pod Identity inventory."""
    eks_contract = provider_context['eks']
    session = session_cache.session(eks_contract['audit_role_arn'],
                                    eks_contract['region'], deadline_monotonic,
                                    cancellation)
    timeout = _client_timeout(deadline_monotonic, cancellation)
    config = aws_adaptor.botocore_config().Config(
        connect_timeout=timeout,
        read_timeout=timeout,
        retries={'total_max_attempts': 1})
    eks = session.client('eks',
                         region_name=eks_contract['region'],
                         config=config)
    _require_cluster(eks.describe_cluster(name=eks_contract['cluster_name']),
                     eks_contract)
    associations = _list_associations(
        eks,
        cluster_name=eks_contract['cluster_name'],
        namespace=fleet_context['namespace'],
        service_account=fleet_context['service_account_name'],
        deadline_monotonic=deadline_monotonic,
        cancellation=cancellation)
    described: Mapping[str, Any] | None = None
    if fleet_context['pod_identity_role_arn'] is not None:
        if len(associations) != 1:
            validate_pod_identity_inventory(
                associations,
                described_association=None,
                cluster_name=eks_contract['cluster_name'],
                namespace=fleet_context['namespace'],
                service_account=fleet_context['service_account_name'],
                expected_role_arn=fleet_context['pod_identity_role_arn'])
        association_id = associations[0].get('associationId')
        if not isinstance(association_id, str) or not association_id:
            raise AwsAttestationError(
                'EKS returned an invalid Pod Identity association ID.')
        response = eks.describe_pod_identity_association(
            clusterName=eks_contract['cluster_name'],
            associationId=association_id)
        if not isinstance(response, Mapping) or not isinstance(
                response.get('association'), Mapping):
            raise AwsAttestationError(
                'EKS returned an invalid Pod Identity association.')
        described = response['association']
    validate_pod_identity_inventory(
        associations,
        described_association=described,
        cluster_name=eks_contract['cluster_name'],
        namespace=fleet_context['namespace'],
        service_account=fleet_context['service_account_name'],
        expected_role_arn=fleet_context['pod_identity_role_arn'])
    _remaining(deadline_monotonic, cancellation)
    return PodIdentityProof(
        kubernetes_context=fleet_context['kubernetes_context'],
        cluster_arn=eks_contract['cluster_arn'],
        namespace=fleet_context['namespace'],
        service_account_name=fleet_context['service_account_name'],
        expected_role_arn=fleet_context['pod_identity_role_arn'],
        association_count=len(associations),
        identity_absence_proven=(fleet_context['pod_identity_role_arn'] is None
                                 and not associations))
