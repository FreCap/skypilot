"""OCI graph and AWS qualification/data-plane boundary tests."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import threading
import types
from typing import Any
from unittest import mock
import uuid

import pytest

from sky.container_images import aws
from sky.container_images import models
from sky.container_images import oci
from sky.container_images import providers

_MANIFEST_MEDIA_TYPE = 'application/vnd.oci.image.manifest.v1+json'
_INDEX_MEDIA_TYPE = 'application/vnd.oci.image.index.v1+json'
_CONFIG_MEDIA_TYPE = 'application/vnd.oci.image.config.v1+json'
_LAYER_MEDIA_TYPE = 'application/vnd.oci.image.layer.v1.tar+gzip'
_DIGEST = 'sha256:' + 'a' * 64


def _raw(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def _digest(value: bytes) -> str:
    return 'sha256:' + hashlib.sha256(value).hexdigest()


def _image(platform: str = 'linux/amd64') -> tuple[bytes, bytes, bytes]:
    operating_system, architecture, *variant = platform.split('/')
    config: dict[str, Any] = {
        'os': operating_system,
        'architecture': architecture,
    }
    if variant:
        config['variant'] = variant[0]
    config_bytes = _raw(config)
    layer = b'compressed-layer'
    manifest = _raw({
        'schemaVersion': 2,
        'mediaType': _MANIFEST_MEDIA_TYPE,
        'config': {
            'mediaType': _CONFIG_MEDIA_TYPE,
            'digest': _digest(config_bytes),
            'size': len(config_bytes),
        },
        'layers': [{
            'mediaType': _LAYER_MEDIA_TYPE,
            'digest': _digest(layer),
            'size': len(layer),
        }],
    })
    return manifest, config_bytes, layer


def _graph_from_root(root: bytes,
                     config: bytes,
                     *,
                     platform: str,
                     child: bytes | None = None) -> oci.OciContentGraph:
    return oci.build_content_graph(
        raw_root=root,
        expected_root_digest=_digest(root),
        requested_platform=platform,
        fetch_manifest=lambda digest: child
        if child is not None and digest == _digest(child) else b'',
        fetch_blob=lambda digest: config if digest == _digest(config) else b'',
        limits=oci.OciInspectionLimits())


def test_single_manifest_graph_proves_raw_bytes_platform_and_size() -> None:
    manifest, config, layer = _image()
    graph = _graph_from_root(manifest, config, platform='linux/amd64')

    assert graph.source_root_digest == graph.runtime_digest == _digest(manifest)
    assert graph.raw_runtime_manifest == manifest
    assert graph.platform == 'linux/amd64'
    assert graph.declared_size_bytes == len(config) + len(layer)
    assert graph.manifest_units == 1


def test_index_selects_one_exact_amd64_child_without_copying_parent() -> None:
    amd64, config, _ = _image()
    arm64, _, _ = _image('linux/arm64')
    index = _raw({
        'schemaVersion': 2,
        'mediaType': _INDEX_MEDIA_TYPE,
        'manifests': [{
            'mediaType': _MANIFEST_MEDIA_TYPE,
            'digest': _digest(arm64),
            'size': len(arm64),
            'platform': {
                'os': 'linux',
                'architecture': 'arm64',
            },
        }, {
            'mediaType': _MANIFEST_MEDIA_TYPE,
            'digest': _digest(amd64),
            'size': len(amd64),
            'platform': {
                'os': 'linux',
                'architecture': 'amd64',
            },
        }],
    })
    graph = _graph_from_root(index, config, platform='linux/amd64', child=amd64)

    assert graph.source_root_digest == _digest(index)
    assert graph.runtime_digest == _digest(amd64)
    assert graph.raw_source_root == index
    assert graph.raw_runtime_manifest == amd64


@pytest.mark.parametrize('mutation', [
    'ambiguous', 'nested', 'artifact', 'external', 'foreign', 'wrong-platform'
])
def test_oci_graph_rejects_non_runnable_or_ambiguous_content(
        mutation: str) -> None:
    manifest, config, _ = _image()
    root = manifest
    child = None
    if mutation in ('ambiguous', 'nested'):
        descriptor_media = (_INDEX_MEDIA_TYPE
                            if mutation == 'nested' else _MANIFEST_MEDIA_TYPE)
        descriptors = [{
            'mediaType': descriptor_media,
            'digest': _digest(manifest),
            'size': len(manifest),
            'platform': {
                'os': 'linux',
                'architecture': 'amd64',
            },
        }]
        if mutation == 'ambiguous':
            descriptors.append(dict(descriptors[0]))
        root = _raw({
            'schemaVersion': 2,
            'mediaType': _INDEX_MEDIA_TYPE,
            'manifests': descriptors,
        })
        child = manifest
    elif mutation == 'artifact':
        value = json.loads(manifest)
        value['artifactType'] = 'application/example'
        root = _raw(value)
    elif mutation in ('external', 'foreign'):
        value = json.loads(manifest)
        if mutation == 'external':
            value['layers'][0]['urls'] = ['https://external.invalid/layer']
        else:
            value['layers'][0]['mediaType'] = (
                'application/vnd.docker.image.rootfs.foreign.diff.tar.gzip')
        root = _raw(value)
    else:
        wrong_manifest, wrong_config, _ = _image('linux/arm64')
        root, config = wrong_manifest, wrong_config

    with pytest.raises(ValueError):
        _graph_from_root(root, config, platform='linux/amd64', child=child)


def _qualified_shard(**overrides: Any) -> aws.QualifiedShard:
    values: dict[str, Any] = {
        'workspace': 'research',
        'target': 'canonical',
        'partition': 'aws',
        'account': '123456789012',
        'region': 'us-east-1',
        'shard_generation': 0,
        'shard_index': 0,
        'registry': '123456789012.dkr.ecr.us-east-1.amazonaws.com',
        'repository_name': 'skypilot/images/rabc/wabc/g00/s00',
        'repository_arn': ('arn:aws:ecr:us-east-1:123456789012:'
                           'repository/skypilot/images/rabc/wabc/g00/s00'),
        'encryption_type': 'AES256',
        'kms_key_arn': None,
        'tag_immutability': 'IMMUTABLE',
        'scanning_mode': 'SCAN_ON_PUSH',
        'policy_hash': '1' * 64,
        'ownership_tags_hash': '2' * 64,
        'max_manifests': 90_000,
        'max_declared_bytes': 10_995_116_277_760,
        'max_in_flight': 10,
        'physical_fingerprint': '0' * 64,
    }
    values.update(overrides)
    provisional = aws.QualifiedShard(**values)
    values['physical_fingerprint'] = provisional.calculated_fingerprint()
    return aws.QualifiedShard(**values)


def _terraform_manifest(shards: list[aws.QualifiedShard]) -> bytes:
    return _raw({
        'schema_version': 1,
        'catalog_authority': str(uuid.uuid4()),
        'workspace': 'research',
        'profile': 'gpu-production',
        'profile_revision': 1,
        'config_hash': '3' * 64,
        'physical_manifest_hash': '4' * 64,
        'generated_at': 100,
        'shards': [dataclasses.asdict(shard) for shard in shards],
        'role_fingerprints': {
            'us-east-1:copy_role_arn':
                ('arn:aws:iam::123456789012:role/SkyPilotImageCopy'),
        },
        'quota_facts': {
            'us-east-1:ecr_api_rate_per_second': 20,
        },
    })


def test_terraform_manifest_parser_is_closed_typed_and_fingerprinted() -> None:
    shard = _qualified_shard()
    parsed = aws.TerraformQualificationManifest.from_json(
        _terraform_manifest([shard]))
    assert parsed.shards == (shard,)
    assert parsed.manifest_hash == parsed.manifest_hash

    payload = json.loads(_terraform_manifest([shard]))
    payload['shards'][0]['max_in_flight'] = True
    with pytest.raises(ValueError, match='invalid limits'):
        aws.TerraformQualificationManifest.from_json(_raw(payload))

    payload = json.loads(_terraform_manifest([shard]))
    payload['shards'][0]['repository_name'] = '../escape'
    with pytest.raises(ValueError):
        aws.TerraformQualificationManifest.from_json(_raw(payload))

    with pytest.raises(ValueError, match='duplicate shards'):
        aws.TerraformQualificationManifest.from_json(
            _terraform_manifest([shard, shard]))


def test_handoff_ingest_keeps_candidate_limits_revision_scoped(
        monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    authority = '11111111-1111-4111-8111-111111111111'
    shards: list[aws.QualifiedShard] = []
    roles: dict[str, str] = {}
    quotas: dict[str, int] = {}
    for target in (profile.canonical,) + profile.targets:
        for index in range(target.shard_count):
            repository_name = (
                f'{target.repository_prefix}/rtest/wtest/g00/s{index:02x}')
            values = {
                'workspace': 'research',
                'target': target.name,
                'partition': profile.partition,
                'account': profile.registry_account,
                'region': target.region,
                'shard_generation': 0,
                'shard_index': index,
                'registry': target.registry,
                'repository_name': repository_name,
                'repository_arn':
                    (f'arn:{profile.partition}:ecr:{target.region}:'
                     f'{profile.registry_account}:repository/{repository_name}'
                    ),
                'encryption_type': 'AES256',
                'kms_key_arn': None,
                'tag_immutability': 'IMMUTABLE',
                'scanning_mode': 'SCAN_ON_PUSH',
                'policy_hash': '1' * 64,
                'ownership_tags_hash': '2' * 64,
                'max_manifests': 80,
                'max_declared_bytes': 800_000,
                'max_in_flight': 3,
                'physical_fingerprint': '0' * 64,
            }
            provisional = aws.QualifiedShard(**values)
            values['physical_fingerprint'] = provisional.calculated_fingerprint(
            )
            shards.append(aws.QualifiedShard(**values))
        copy = profile.bindings[target.write_authority]
        lifecycle = profile.bindings[target.qualification_delete_authority]
        qualification_name = (
            f'{target.repository_prefix}/rtest/qualification/{target.region}')
        roles.update({
            f'{target.region}:copy_role_arn': copy.authority,
            f'{target.region}:copy_policy_hash': '3' * 64,
            f'{target.region}:lifecycle_role_arn': lifecycle.authority,
            f'{target.region}:lifecycle_policy_hash': '4' * 64,
            f'{target.region}:copy_boundary_policy_hash': '5' * 64,
            f'{target.region}:lifecycle_boundary_policy_hash': '6' * 64,
            f'{target.region}:qualification_repo_arn':
                (f'arn:{profile.partition}:ecr:{target.region}:'
                 f'{profile.registry_account}:repository/{qualification_name}'),
        })
        quotas.update({
            f'{target.region}:ecr_api_rate_per_second': 7,
            f'{target.region}:ecr_api_burst': 3,
            f'{target.region}:images_per_repository': 100,
            f'{target.region}:reserved_headroom': 10,
        })
    payload = _raw({
        'schema_version': 1,
        'catalog_authority': authority,
        'workspace': 'research',
        'profile': profile.name,
        'profile_revision': profile.revision,
        'config_hash': profile.config_hash,
        'physical_manifest_hash': profile.physical_manifest_hash,
        'generated_at': 100,
        'shards': [dataclasses.asdict(shard) for shard in shards],
        'role_fingerprints': roles,
        'quota_facts': quotas,
    })
    desired = types.SimpleNamespace(id='candidate',
                                    desired_generation=2,
                                    config_hash=profile.config_hash,
                                    terraform_hash=None)
    policy = models.WorkspaceImagePolicy(
        mode=models.WorkspaceImageMode.MANAGED_REQUIRED,
        default_profile=profile.name,
        allowed_profiles=(profile.name,),
        locality=models.Locality.PREFER)
    monkeypatch.setattr(aws.catalog_state, 'get_catalog_authority_id',
                        lambda **_kwargs: authority)
    monkeypatch.setattr(aws.catalog_state, 'engine',
                        mock.Mock(return_value=object()))
    monkeypatch.setattr(aws.image_config, 'resolve_profile', lambda *_args:
                        (profile, policy))
    monkeypatch.setattr(aws.topology_state, 'stage_profile_revision',
                        lambda **_kwargs: desired)
    mutation_order: list[str] = []
    lock_shards = mock.Mock(
        side_effect=lambda *_args, **_kwargs: mutation_order.append('lock'))
    monkeypatch.setattr(aws.topology_state, 'lock_profile_shards', lock_shards)
    upsert_shard = mock.Mock()
    upsert_shard.side_effect = lambda *_args, **_kwargs: mutation_order.append(
        'upsert')
    monkeypatch.setattr(aws.topology_state, 'upsert_qualified_shard',
                        upsert_shard)
    ensure_budget = mock.Mock()
    monkeypatch.setattr(aws.topology_state, 'ensure_provider_budget',
                        ensure_budget)
    monkeypatch.setattr(
        aws.topology_state, 'upsert_provider_budget',
        mock.Mock(side_effect=AssertionError('candidate mutated live budget')))
    attest = mock.Mock(return_value=desired)
    monkeypatch.setattr(aws.topology_state, 'record_profile_attestation',
                        attest)
    session = mock.MagicMock()
    session_context = mock.MagicMock()
    session_context.__enter__.return_value = session
    monkeypatch.setattr(aws.orm, 'Session', lambda _engine: session_context)

    result = aws.ingest_terraform_qualification(payload, now=100)

    assert result is desired
    lock_shards.assert_called_once_with(session,
                                        workspace='research',
                                        profile=profile.name)
    assert mutation_order == ['lock'] + ['upsert'] * len(shards)
    assert upsert_shard.call_count == len(shards)
    assert ensure_budget.call_count == len((profile.canonical,) +
                                           profile.targets)
    shard_evidence = [
        call.kwargs['evidence']
        for call in attest.call_args_list
        if call.kwargs['kind'].startswith('terraform_shard:')
    ]
    budget_evidence = [
        call.kwargs['evidence']
        for call in attest.call_args_list
        if call.kwargs['kind'].startswith('terraform_budget:')
    ]
    assert len(shard_evidence) == len(shards)
    assert all(
        evidence['max_manifests'] == 80 and evidence['max_declared_bytes'] ==
        800_000 and evidence['max_in_flight'] == 3
        for evidence in shard_evidence)
    assert len(budget_evidence) == len((profile.canonical,) + profile.targets)
    assert all(
        evidence['applied_rate_per_second'] == 7 and evidence['burst'] == 3
        for evidence in budget_evidence)


class _EcrClient:
    """Small exact-digest ECR fake for repository convergence tests."""

    def __init__(self, graph: oci.OciContentGraph) -> None:
        self.graph = graph
        self.present = False
        self.deleted: list[str] = []
        self.put_error: BaseException | None = None
        self.delete_error: BaseException | None = None
        self.get_calls = 0

    def batch_get_image(self, **_: Any) -> dict[str, Any]:
        self.get_calls += 1
        if not self.present:
            return {
                'images': [],
                'failures': [{
                    'failureCode': 'ImageNotFound'
                }]
            }
        return {
            'images': [{
                'imageManifest': self.graph.raw_runtime_manifest.decode(),
                'imageManifestMediaType': self.graph.runtime_media_type,
                'imageId': {
                    'imageDigest': self.graph.runtime_digest,
                },
            }],
        }

    def batch_check_layer_availability(self, **kwargs: Any) -> dict[str, Any]:
        return {
            'layers': [{
                'layerDigest': digest,
                'layerAvailability': 'AVAILABLE',
            } for digest in kwargs['layerDigests']],
        }

    def put_image(self, **_: Any) -> None:
        if self.put_error is not None:
            raise self.put_error
        self.present = True

    def batch_delete_image(self, **kwargs: Any) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append(kwargs['imageIds'][0]['imageDigest'])
        self.present = False


class _AwsError(Exception):

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {'Error': {'Code': code}}


def test_ecr_copy_rejects_source_blob_above_descriptor_before_upload() -> None:
    descriptor = oci.OciDescriptor(media_type='application/octet-stream',
                                   digest='sha256:' + 'a' * 64,
                                   size=3)
    chunks = aws.EcrRepository._verified_chunks(  # pylint: disable=protected-access
        [b'ab', b'cd'], descriptor, threading.Event())

    assert next(chunks) == b'ab'
    with pytest.raises(ValueError, match='exceeds its declared descriptor'):
        next(chunks)


def test_ecr_copy_and_delete_converge_only_on_exact_digest() -> None:
    manifest, config, _ = _image()
    graph = _graph_from_root(manifest, config, platform='linux/amd64')
    client = _EcrClient(graph)
    repository = aws.EcrRepository(client, 'skypilot/images/shard')

    outcome = repository.copy_graph(graph, mock.Mock(), threading.Event())
    assert outcome == aws.CopyOutcome.WRITTEN
    assert repository.verify_graph(graph)
    assert repository.copy_graph(graph, mock.Mock(),
                                 threading.Event()) == aws.CopyOutcome.PRESENT
    assert repository.exact_delete(graph.runtime_digest)
    assert client.deleted == [graph.runtime_digest]

    client.put_error = _AwsError('ServiceUnavailable')
    assert repository.copy_graph(graph, mock.Mock(),
                                 threading.Event()) == aws.CopyOutcome.AMBIGUOUS


def test_copy_destination_race_does_not_acquire_source_stream() -> None:
    manifest, config, _ = _image()
    graph = _graph_from_root(manifest, config, platform='linux/amd64')
    client = mock.Mock()
    repository = aws.EcrRepository(client, 'skypilot/images/shard')
    repository.verify_graph = mock.Mock(return_value=False)
    presence_calls = 0

    def layers_present(digests) -> dict[str, bool]:
        nonlocal presence_calls
        presence_calls += 1
        values = list(digests)
        return {digest: presence_calls > 1 for digest in values}

    repository._layers_present = (  # pylint: disable=protected-access
        layers_present)
    read_blob = mock.Mock(side_effect=AssertionError('source stream acquired'))

    assert (repository.copy_graph(graph, read_blob,
                                  threading.Event()) == aws.CopyOutcome.WRITTEN)
    read_blob.assert_not_called()
    client.put_image.assert_called_once()


def test_upload_failure_explicitly_closes_acquired_source_stream() -> None:

    class CloseableChunks:

        def __init__(self) -> None:
            self.closed = False

        def __iter__(self):
            return iter((b'payload',))

        def close(self) -> None:
            self.closed = True

    payload = b'payload'
    descriptor = oci.OciDescriptor(
        media_type='application/octet-stream',
        digest=f'sha256:{hashlib.sha256(payload).hexdigest()}',
        size=len(payload))
    client = mock.Mock()
    client.initiate_layer_upload.side_effect = _AwsError(
        'AccessDeniedException')
    repository = aws.EcrRepository(client, 'skypilot/images/shard')
    repository._layers_present = (  # pylint: disable=protected-access
        lambda _digests: {
            descriptor.digest: False
        })
    chunks = CloseableChunks()

    with pytest.raises(_AwsError, match='AccessDeniedException'):
        repository._upload_layer(  # pylint: disable=protected-access
            descriptor, lambda: chunks, threading.Event())

    assert chunks.closed


def test_upload_initiation_failure_never_opens_registry_source_response(
) -> None:
    payload = b'payload'
    descriptor = oci.OciDescriptor(
        media_type='application/octet-stream',
        digest=f'sha256:{hashlib.sha256(payload).hexdigest()}',
        size=len(payload))
    source = providers.RegistryV2Source(
        f'registry.example/repository/image@{descriptor.digest}', lambda: None)
    request = mock.Mock(side_effect=AssertionError('source response opened'))
    destination_client = mock.Mock()
    destination_client.initiate_layer_upload.side_effect = _AwsError(
        'AccessDeniedException')
    destination = aws.EcrRepository(destination_client, 'skypilot/images/shard')
    destination._layers_present = (  # pylint: disable=protected-access
        lambda _digests: {
            descriptor.digest: False
        })

    with mock.patch.object(source, '_request', request), pytest.raises(
            _AwsError, match='AccessDeniedException'):
        destination._upload_layer(  # pylint: disable=protected-access
            descriptor, lambda: source.read_blob(descriptor), threading.Event())

    request.assert_not_called()


def test_upload_initiation_failure_never_opens_ecr_source_response() -> None:
    payload = b'payload'
    descriptor = oci.OciDescriptor(
        media_type='application/octet-stream',
        digest=f'sha256:{hashlib.sha256(payload).hexdigest()}',
        size=len(payload))
    source = aws.EcrRepository(mock.Mock(), 'source/repository')
    download = mock.Mock(side_effect=AssertionError('source response opened'))
    destination_client = mock.Mock()
    destination_client.initiate_layer_upload.side_effect = _AwsError(
        'AccessDeniedException')
    destination = aws.EcrRepository(destination_client, 'skypilot/images/shard')
    destination._layers_present = (  # pylint: disable=protected-access
        lambda _digests: {
            descriptor.digest: False
        })

    with mock.patch.object(source, '_download_response',
                           download), pytest.raises(
                               _AwsError, match='AccessDeniedException'):
        destination._upload_layer(  # pylint: disable=protected-access
            descriptor, lambda: source.read_blob(descriptor), threading.Event())

    download.assert_not_called()


@pytest.mark.parametrize('error', [
    TimeoutError('read timeout'),
    ConnectionError('connection reset'),
    _AwsError('ServiceUnavailable'),
])
def test_ecr_delete_transport_or_server_ambiguity_skips_readback(
        error: BaseException) -> None:
    manifest, config, _ = _image()
    graph = _graph_from_root(manifest, config, platform='linux/amd64')
    client = _EcrClient(graph)
    client.present = True
    client.delete_error = error
    repository = aws.EcrRepository(client, 'skypilot/images/shard')

    assert (repository.delete_outcome(
        graph.runtime_digest) == aws.DeleteOutcome.AMBIGUOUS)
    assert client.get_calls == 0


@pytest.mark.parametrize('code', [
    'AccessDeniedException',
    'ThrottlingException',
    'ValidationException',
])
def test_ecr_delete_explicit_no_mutation_rejection_allows_readback(
        code: str) -> None:
    manifest, config, _ = _image()
    graph = _graph_from_root(manifest, config, platform='linux/amd64')
    client = _EcrClient(graph)
    client.present = True
    client.delete_error = _AwsError(code)
    repository = aws.EcrRepository(client, 'skypilot/images/shard')

    assert (repository.delete_outcome(
        graph.runtime_digest) == aws.DeleteOutcome.PRESENT)
    assert client.get_calls == 1


def test_ecr_delete_finds_rejection_response_through_hooked_adapter() -> None:
    manifest, config, _ = _image()
    graph = _graph_from_root(manifest, config, platform='linux/amd64')
    client = _EcrClient(graph)
    client.present = True
    client.delete_error = _AwsError('ThrottlingException')
    hooks = aws.EcrCallHooks(before_call=mock.Mock(), on_throttle=mock.Mock())
    hooked = aws._HookedEcrClient(  # pylint: disable=protected-access
        client, hooks)
    repository = aws.EcrRepository(hooked, 'skypilot/images/shard')

    assert (repository.delete_outcome(
        graph.runtime_digest) == aws.DeleteOutcome.PRESENT)
    assert client.get_calls == 1
    assert hooks.before_call.call_count == 2
    hooks.on_throttle.assert_called_once_with()


def test_ecr_delete_hooked_transport_timeout_never_reads_back() -> None:
    manifest, config, _ = _image()
    graph = _graph_from_root(manifest, config, platform='linux/amd64')
    client = _EcrClient(graph)
    client.present = True
    client.delete_error = TimeoutError('read timeout')
    hooks = aws.EcrCallHooks(before_call=mock.Mock(), on_throttle=mock.Mock())
    hooked = aws._HookedEcrClient(  # pylint: disable=protected-access
        client, hooks)
    repository = aws.EcrRepository(hooked, 'skypilot/images/shard')

    assert (repository.delete_outcome(
        graph.runtime_digest) == aws.DeleteOutcome.AMBIGUOUS)
    assert client.get_calls == 0
    hooks.before_call.assert_called_once_with()
    hooks.on_throttle.assert_not_called()


def test_ecr_concluded_delete_readback_failure_is_retryable() -> None:
    manifest, config, _ = _image()
    graph = _graph_from_root(manifest, config, platform='linux/amd64')
    client = _EcrClient(graph)
    client.present = True
    client.batch_get_image = mock.Mock(
        side_effect=TimeoutError('readback timeout'))
    repository = aws.EcrRepository(client, 'skypilot/images/shard')

    assert (repository.delete_outcome(
        graph.runtime_digest) == aws.DeleteOutcome.READBACK_RETRY)
    assert client.deleted == [graph.runtime_digest]
    client.batch_get_image.assert_called_once()


def test_service_quota_calls_are_synchronously_provider_fenced(
        monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    class QuotaClient:

        def get_service_quota(self, **_kwargs: object) -> dict[str, object]:
            events.append('quota')
            return {'Quota': {'Value': 100.0}}

    def assumed_client(*_args: object,
                       provider_fence: Any = None) -> QuotaClient:
        assert provider_fence is not None
        provider_fence()
        events.append('sts')
        provider_fence()
        return QuotaClient()

    monkeypatch.setattr(aws, 'assumed_client', assumed_client)
    binding = aws.AwsRoleBinding(role_arn='arn:aws:iam::123:role/test',
                                 external_id=None,
                                 session_name='test',
                                 catalog_tag='catalog',
                                 profile_tag='profile')

    quota = aws.applied_ecr_images_per_repository_quota(
        binding, 'us-east-1', provider_fence=lambda: events.append('lease'))

    assert quota == 100
    assert events == ['lease', 'sts', 'lease', 'lease', 'quota', 'lease']


def test_ecr_role_acquisition_fences_actual_sts_boundary(
        monkeypatch: pytest.MonkeyPatch) -> None:
    sts = mock.Mock()
    ambient_session = mock.Mock()
    ambient_session.client.return_value = sts
    session = mock.Mock(return_value=ambient_session)
    monkeypatch.setattr(aws.aws_adaptor, 'session', session)
    binding = aws.AwsRoleBinding(role_arn='arn:aws:iam::123:role/test',
                                 external_id=None,
                                 session_name='test',
                                 catalog_tag='catalog',
                                 profile_tag='profile')
    lost = mock.Mock(side_effect=[
        None,
        RuntimeError('lease lost at STS boundary'),
    ])

    with pytest.raises(RuntimeError, match='lease lost at STS boundary'):
        aws.EcrRepository.from_role(binding,
                                    'us-east-1',
                                    'skypilot/images/shard',
                                    provider_fence=lost)
    session.assert_called_once_with(profile=None)
    ambient_session.client.assert_called_once_with('sts',
                                                   region_name='us-east-1')
    assert lost.call_count == 2
    lost.assert_has_calls([mock.call(), mock.call()])
    sts.assume_role.assert_not_called()


def test_ecr_sdk_client_is_fenced_before_and_after_each_call() -> None:
    events: list[str] = []

    class Client:

        def describe_repositories(self) -> str:
            events.append('provider')
            return 'result'

    fenced = aws._ProviderFencedEcrClient(  # pylint: disable=protected-access
        Client(), lambda: events.append('lease'))

    assert fenced.describe_repositories() == 'result'
    assert events == ['lease', 'provider', 'lease']


def test_ecr_layer_download_uses_guarded_no_proxy_session() -> None:
    repository = aws.EcrRepository(mock.Mock(), 'skypilot/images/shard')

    assert repository._download_session is None  # pylint: disable=protected-access
    session = repository._get_download_session(  # pylint: disable=protected-access
    )
    assert not session.trust_env
    adapter = session.get_adapter('https://public.example')
    with pytest.raises(ValueError, match='do not permit HTTP proxies'):
        adapter.proxy_manager_for('http://127.0.0.1:8080')


def test_ecr_layer_download_rejects_redirect_and_closes_response() -> None:
    client = mock.Mock()
    client.get_download_url_for_layer.return_value = {
        'downloadUrl': 'https://public.example/layer'
    }
    download = mock.Mock(status_code=307)
    repository = aws.EcrRepository(client, 'skypilot/images/shard')
    session = mock.Mock()
    repository._download_session = session  # pylint: disable=protected-access
    session.get.return_value = download

    with pytest.raises(ValueError, match='redirects are not allowed'):
        repository.read_blob_bytes(_DIGEST, max_bytes=100)

    session.get.assert_called_once_with('https://public.example/layer',
                                        timeout=60,
                                        stream=True,
                                        allow_redirects=False)
    download.close.assert_called_once_with()


def test_ecr_layer_download_is_fenced_per_chunk_and_closed_on_lease_loss(
) -> None:
    client = mock.Mock()
    client.get_download_url_for_layer.return_value = {
        'downloadUrl': 'https://public.example/layer'
    }
    download = mock.Mock(status_code=200)
    download.iter_content.return_value = [b'first', b'second']
    calls = 0

    def fence() -> None:
        nonlocal calls
        calls += 1
        # Five checks cover URL issuance and guarded HTTP setup. The sixth
        # starts streaming and the seventh fences the first yielded chunk.
        if calls == 7:
            raise RuntimeError('source lease lost while streaming')

    repository = aws.EcrRepository(client,
                                   'skypilot/images/shard',
                                   provider_fence=fence)
    session = mock.Mock()
    repository._download_session = session  # pylint: disable=protected-access
    session.get.return_value = download

    chunks = iter(
        repository.read_blob(
            oci.OciDescriptor(media_type='application/octet-stream',
                              digest=_DIGEST,
                              size=11)))
    with pytest.raises(RuntimeError, match='lease lost while streaming'):
        next(chunks)
    download.close.assert_called_once_with()
