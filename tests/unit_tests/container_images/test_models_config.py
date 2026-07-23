"""Pure model, selector, profile, and workspace-policy tests."""

from __future__ import annotations

import copy
import pickle
from typing import Any
import uuid

import pytest

from sky.container_images import config
from sky.container_images import models

ACCOUNT = '123456789012'
DIGEST = 'sha256:' + 'a' * 64
SOURCE = f'ghcr.io/boltz-bio/runtime@{DIGEST}'


@pytest.mark.parametrize('invalid', [
    'https://ghcr.io/boltz/runtime:latest',
    'user:password@registry.example/runtime:latest',
    'registry.example/runtime?token=secret',
    'registry.example/runtime#fragment',
    'registry.example/Uppercase@' + DIGEST,
    'registry.example/runtime@sha256:short',
])
def test_oci_references_reject_mutability_and_credential_syntax(
        invalid: str) -> None:
    with pytest.raises(ValueError):
        models.ContainerImage(ref=invalid)


def test_container_image_scalar_object_and_explicit_selectors() -> None:
    direct = models.ContainerImage.from_config(SOURCE)
    assert direct.to_yaml_config() == SOURCE
    assert direct.digest == DIGEST

    release = models.ContainerImage.from_config({
        'release': 'boltz-l4-2026-07-20',
        'distribution': 'gpu-production',
    })
    assert release.to_yaml_config() == {
        'release': 'boltz-l4-2026-07-20',
        'distribution': 'gpu-production',
    }
    artifact_id = str(uuid.uuid4())
    artifact = models.parse_explicit_image_selector(
        f'artifact_id={artifact_id}')
    assert artifact is not None
    assert models.format_explicit_image_selector(
        artifact) == f'artifact_id={artifact_id}'
    assert models.validate_operational_image_selector(
        f'ref={SOURCE}') == f'ref={SOURCE}'


def test_container_image_identity_is_closed_and_revalidated() -> None:
    with pytest.raises(ValueError):
        models.ContainerImage.from_config({'ref': SOURCE, 'unknown': 'value'})
    with pytest.raises(ValueError):
        models.ContainerImage(ref=SOURCE, artifact_id=str(uuid.uuid4()))
    with pytest.raises(ValueError):
        models.ContainerImage(release='release', distribution='direct')
    with pytest.raises(ValueError):
        models.ContainerImage.from_config(
            models.ContainerImage.from_legacy_ref('ubuntu:22.04'))

    restored = pickle.loads(pickle.dumps(models.ContainerImage(ref=SOURCE)))
    object.__setattr__(restored, 'ref', 'ubuntu:latest')
    with pytest.raises(ValueError):
        models.ContainerImage.from_config(restored)


def test_runtime_platform_matching_is_explicit() -> None:
    assert models.runtime_platform_from_architecture('x86_64') == 'linux/amd64'
    assert models.runtime_platform_from_architecture('aarch64') == 'linux/arm64'
    assert models.runtime_platform_from_architecture('mips') is None
    assert models.platforms_support_runtime(('linux/amd64',), 'linux/amd64')
    assert not models.platforms_support_runtime(('linux/arm64',), 'linux/amd64')
    assert models.platforms_support_runtime(('linux/amd64/v1',), 'linux/amd64')
    assert not models.platforms_support_runtime(
        ('linux/amd64/v3',), 'linux/amd64')


def test_aws_authority_covers_standard_and_china_partitions() -> None:
    assert models.aws_ecr_registry_authority(
        ACCOUNT, 'us-east-1') == f'{ACCOUNT}.dkr.ecr.us-east-1.amazonaws.com'
    assert models.aws_ecr_registry_authority(
        ACCOUNT,
        'cn-north-1') == f'{ACCOUNT}.dkr.ecr.cn-north-1.amazonaws.com.cn'


def test_profile_snapshot_is_complete_deterministic_and_revalidated(
        profile: models.ManagedRegistryProfile) -> None:
    snapshot = profile.to_snapshot()
    restored = models.ManagedRegistryProfile.from_snapshot(snapshot)
    assert restored == profile
    assert restored.config_hash == profile.config_hash
    assert restored.physical_manifest_hash == profile.physical_manifest_hash
    assert restored.canonical.target_fingerprint != (
        restored.targets[0].target_fingerprint)

    forged = copy.deepcopy(snapshot)
    forged['canonical']['registry'] = 'attacker.example'
    with pytest.raises(ValueError):
        models.ManagedRegistryProfile.from_snapshot(forged)


def test_profile_requires_exact_runtime_and_canary_bindings(
        registry_config: dict[str, Any]) -> None:
    bindings = config.parse_access_bindings(registry_config['access_bindings'])
    assert set(config.parse_profiles(registry_config['profiles'],
                                     bindings)) == {'gpu-production'}
    ec2_binding = bindings['aws-vm-pullers']
    assert models.ec2_instance_profile_arn(ec2_binding) == (
        'arn:aws:iam::210987654321:instance-profile/SkyPilotNodeProfile')

    invalid = copy.deepcopy(registry_config)
    invalid['profiles']['gpu-production']['targets'][0]['runtime_pull'][
        'aws_eks'] = 'aws-vm-pullers'
    with pytest.raises(ValueError):
        config.parse_profiles(invalid['profiles'], bindings)

    invalid = copy.deepcopy(registry_config)
    invalid['access_bindings']['aws-vm-pullers']['qualified_node_images'].pop(
        'us-west-2')
    with pytest.raises(ValueError):
        invalid_bindings = config.parse_access_bindings(
            invalid['access_bindings'])
        config.parse_profiles(invalid['profiles'], invalid_bindings)

    invalid = copy.deepcopy(registry_config)
    invalid['access_bindings']['aws-vm-pullers']['principals'] = [
        'arn:aws:iam::not-an-account:role/SkyPilotNodeRole'
    ]
    with pytest.raises(ValueError, match='principal ARN'):
        config.parse_access_bindings(invalid['access_bindings'])


@pytest.mark.parametrize('architecture', [None, 'arm64'])
def test_eks_qualification_requires_exact_amd64_selector(
        registry_config: dict[str, Any], architecture: str | None) -> None:
    invalid = copy.deepcopy(registry_config)
    selector = invalid['access_bindings']['aws-eks-pullers'][
        'qualified_clusters'][0]['node_selector']
    if architecture is None:
        selector.pop(models.KUBERNETES_ARCH_LABEL)
    else:
        selector[models.KUBERNETES_ARCH_LABEL] = architecture

    with pytest.raises(ValueError, match='kubernetes.io/arch=amd64'):
        config.parse_access_bindings(invalid['access_bindings'])


@pytest.mark.parametrize('mutation', ['empty', 'missing-region'])
def test_ec2_canary_binding_requires_explicit_security_groups_in_every_region(
        registry_config: dict[str, Any], mutation: str) -> None:
    invalid = copy.deepcopy(registry_config)
    security_groups = invalid['access_bindings']['aws-vm-pullers'][
        'canary_security_groups']
    if mutation == 'empty':
        security_groups['us-west-2'] = []
    else:
        security_groups.pop('us-west-2')

    with pytest.raises(ValueError, match='canary (security groups|network)'):
        config.parse_access_bindings(invalid['access_bindings'])


def test_workspace_policy_defaults_to_unchanged_direct_behavior() -> None:
    policy = config.parse_workspace_policy({})
    assert policy.mode == models.WorkspaceImageMode.DIRECT
    assert policy.default_profile is None
    assert policy.locality == models.Locality.PREFER
    assert policy.regional_cache_retention_weeks == 8


def test_workspace_policy_selection_and_allowlist(
        monkeypatch: pytest.MonkeyPatch, config_reader) -> None:
    monkeypatch.setattr(config.skypilot_config, 'get_nested', config_reader)
    monkeypatch.setattr(config.skypilot_config, 'get_active_workspace',
                        lambda: 'research')
    name, policy = config.resolve_profile_name(None, 'research')
    assert name == 'gpu-production'
    assert policy.mode == models.WorkspaceImageMode.MANAGED_REQUIRED
    profile, _ = config.resolve_profile(None, 'research')
    assert profile is not None and profile.name == name
    with pytest.raises(ValueError):
        config.resolve_profile_name('direct', 'research')
    with pytest.raises(ValueError):
        config.resolve_profile_name('unapproved-profile', 'research')


def test_only_declared_kubernetes_context_is_classified_as_managed_eks(
        monkeypatch: pytest.MonkeyPatch, config_reader) -> None:
    monkeypatch.setattr(config.skypilot_config, 'get_nested', config_reader)
    image = models.ContainerImage(ref=SOURCE)

    assert config.is_declared_managed_eks_context(image, 'boltz-west',
                                                  'research')
    assert not config.is_declared_managed_eks_context(image, 'generic-context',
                                                      'research')


def test_list_workspace_policies_preserves_retention_opt_out(
        monkeypatch: pytest.MonkeyPatch, config_reader) -> None:

    def reader(keys: tuple[str, ...], default_value=None, **kwargs):
        value = config_reader(keys, default_value, **kwargs)
        if keys == ('workspaces',):
            value['research']['container_images'][
                'regional_cache_retention_weeks'] = None
        return value

    monkeypatch.setattr(config.skypilot_config, 'get_nested', reader)

    policies = config.list_workspace_policies()

    assert policies['research'].regional_cache_retention_weeks is None


def test_managed_preferred_allows_explicit_direct_escape(
        monkeypatch: pytest.MonkeyPatch, config_reader) -> None:
    original = config_reader

    def preferred(keys: tuple[str, ...], default_value=None, **kwargs):
        value = original(keys, default_value, **kwargs)
        if keys == ('workspaces', 'research', 'container_images'):
            value['mode'] = 'managed_preferred'
        return value

    monkeypatch.setattr(config.skypilot_config, 'get_nested', preferred)
    selected, _ = config.resolve_profile_name('direct', 'research')
    assert selected is None


def test_policy_and_identifiers_are_bounded_and_value_free() -> None:
    with pytest.raises(ValueError, match='bounded stable user IDs'):
        config.parse_workspace_policy({'publishers': ['secret\nvalue']})
    with pytest.raises(ValueError, match='unsupported keys'):
        config.parse_workspace_policy({'password': 'do-not-reflect'})
    with pytest.raises(ValueError) as error:
        models.validate_control_plane_identifier('bad/value', 'Identifier')
    assert 'bad/value' not in str(error.value)
