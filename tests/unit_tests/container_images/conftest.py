"""Shared fixtures for the managed container image contract tests."""
# pylint: disable=redefined-outer-name

from __future__ import annotations

from collections.abc import Callable
import copy
from typing import Any

import pytest

from sky.container_images import config
from sky.container_images import models

DIGEST = 'sha256:' + 'a' * 64
OTHER_DIGEST = 'sha256:' + 'b' * 64
CONFIG_DIGEST = 'sha256:' + 'c' * 64
CANARY_DIGEST = 'sha256:' + 'd' * 64
SOURCE = f'ghcr.io/boltz-bio/runtime@{DIGEST}'
CANARY_SOURCE = f'public.ecr.aws/skypilot/image-canary@{CANARY_DIGEST}'
ACCOUNT = '123456789012'
COMPUTE_ACCOUNT = '210987654321'


@pytest.fixture
def registry_config() -> dict[str, Any]:
    """Returns one complete two-region AWS profile configuration."""
    return {
        'default_profile': 'gpu-production',
        'access_bindings': {
            'registry-copy': {
                'kind': 'aws_assume_role',
                'authority': f'arn:aws:iam::{ACCOUNT}:role/SkyPilotImageCopy',
                'purposes': ['source_read', 'destination_write', 'verify'],
            },
            'registry-lifecycle': {
                'kind': 'aws_assume_role',
                'authority': f'arn:aws:iam::{ACCOUNT}:role/SkyPilotImageLifecycle',
                'purposes': ['lifecycle_delete', 'verify'],
            },
            'compute-canary': {
                'kind': 'aws_assume_role',
                'authority': f'arn:aws:iam::{COMPUTE_ACCOUNT}:role/SkyPilotImageCanary',
                'purposes': ['canary_launch'],
            },
            'aws-vm-pullers': {
                'kind': 'aws_ec2_instance_identity',
                'purposes': ['runtime_pull'],
                'principals': [
                    f'arn:aws:iam::{COMPUTE_ACCOUNT}:role/SkyPilotNodeRole'
                ],
                'instance_profile': 'SkyPilotNodeProfile',
                'credential_helper': 'amazon-ecr-credential-helper',
                'qualified_node_images': {
                    'us-east-1': 'ami-0123456789abcdef0',
                    'us-west-2': 'ami-0fedcba9876543210',
                },
                'canary_authority': 'compute-canary',
                'canary_instance_type': 't3.micro',
                'canary_subnets': {
                    'us-east-1': ['subnet-0123456789abcdef0'],
                    'us-west-2': ['subnet-0fedcba9876543210'],
                },
                'canary_security_groups': {
                    'us-east-1': ['sg-0123456789abcdef0'],
                    'us-west-2': ['sg-0fedcba9876543210'],
                },
            },
            'aws-eks-pullers': {
                'kind': 'aws_eks_kubelet_identity',
                'purposes': ['runtime_pull'],
                'canary_authority': 'compute-canary',
                'qualified_clusters': [{
                    'context': 'boltz-west',
                    'cluster_arn': (f'arn:aws:eks:us-west-2:{COMPUTE_ACCOUNT}:'
                                    'cluster/boltz-west'),
                    'node_role':
                        (f'arn:aws:iam::{COMPUTE_ACCOUNT}:role/EksNodeRole'),
                    'namespace': 'skypilot-image-canaries',
                }],
            },
        },
        'profiles': {
            'gpu-production': {
                'revision': 1,
                'ownership': 'managed',
                'provider': 'aws',
                'partition': 'aws',
                'registry_account': ACCOUNT,
                'realm': 'production',
                'limits': {
                    'max_artifact_bytes': 2_000_000_000_000,
                    'max_releases_per_artifact': 32,
                    'max_regional_locations_per_artifact': 16,
                },
                'qualification': {
                    'runtime_attestation_max_age_seconds': 86400,
                    'automatic_canaries': True,
                    'max_daily_canary_cost_usd': 5,
                    'canary_worst_case_cost_usd': 0.1,
                    'canary_timeout_seconds': 900,
                    'canary_ref': CANARY_SOURCE,
                    'canary_platform': 'linux/amd64',
                },
                'canonical': {
                    'region': 'us-east-1',
                    'registry': (f'{ACCOUNT}.dkr.ecr.us-east-1.amazonaws.com'),
                    'repository_prefix': 'skypilot/images/canonical',
                    'shard_count': 16,
                    'max_manifests_per_shard': 90000,
                    'max_declared_bytes_per_shard': 10_995_116_277_760,
                    'max_in_flight': 16,
                    'write_authority': 'registry-copy',
                    'delete_authority': 'disabled',
                    'qualification_delete_authority': 'registry-lifecycle',
                    'runtime_pull': {
                        'aws_vm': 'aws-vm-pullers',
                    },
                },
                'targets': [{
                    'name': 'aws-us-west-2',
                    'region': 'us-west-2',
                    'registry': (f'{ACCOUNT}.dkr.ecr.us-west-2.amazonaws.com'),
                    'repository_prefix': 'skypilot/images/west',
                    'shard_count': 16,
                    'max_manifests_per_shard': 90000,
                    'max_declared_bytes_per_shard': 10_995_116_277_760,
                    'max_in_flight': 16,
                    'write_authority': 'registry-copy',
                    'delete_authority': 'registry-lifecycle',
                    'qualification_delete_authority': 'registry-lifecycle',
                    'runtime_pull': {
                        'aws_vm': 'aws-vm-pullers',
                        'aws_eks': 'aws-eks-pullers',
                    },
                }],
            },
        },
    }


@pytest.fixture
def profile(registry_config: dict[str, Any]) -> models.ManagedRegistryProfile:
    bindings = config.parse_access_bindings(registry_config['access_bindings'])
    return config.parse_profiles(registry_config['profiles'],
                                 bindings)['gpu-production']


@pytest.fixture
def config_reader(
    registry_config: dict[str, Any],) -> Callable[[tuple[str, ...], Any], Any]:
    values = {
        'container_registries': copy.deepcopy(registry_config),
        'workspaces': {
            'research': {
                'container_images': {
                    'mode': 'managed_required',
                    'default_profile': 'gpu-production',
                    'allowed_profiles': ['gpu-production'],
                    'publishers': ['publisher-1'],
                    'locality': 'prefer',
                    'regional_cache_retention_weeks': 8,
                },
            },
        },
    }

    def read(keys: tuple[str, ...], default_value: Any = None, **_: Any) -> Any:
        current: Any = values
        for key in keys:
            if not isinstance(current, dict) or key not in current:
                return default_value
            current = current[key]
        return copy.deepcopy(current)

    return read
