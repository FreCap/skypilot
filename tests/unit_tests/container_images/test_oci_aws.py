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

_MANIFEST_MEDIA_TYPE = 'application/vnd.oci.image.manifest.v1+json'
_INDEX_MEDIA_TYPE = 'application/vnd.oci.image.index.v1+json'
_CONFIG_MEDIA_TYPE = 'application/vnd.oci.image.config.v1+json'
_LAYER_MEDIA_TYPE = 'application/vnd.oci.image.layer.v1.tar+gzip'


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

    def batch_get_image(self, **_: Any) -> dict[str, Any]:
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
        self.deleted.append(kwargs['imageIds'][0]['imageDigest'])
        self.present = False


class _AwsError(Exception):

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {'Error': {'Code': code}}


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
