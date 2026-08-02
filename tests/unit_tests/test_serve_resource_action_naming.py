"""Pure provider workload naming contracts."""

from __future__ import annotations

from unittest import mock

import pytest

from sky.serve import resource_actions
from sky.utils import common_utils


def test_explicit_user_naming_preserves_fixed_historical_values() -> None:
    with mock.patch.object(common_utils,
                           'get_user_hash',
                           side_effect=AssertionError('ambient read')):
        assert common_utils.make_cluster_name_on_cloud_for_user(
            'Cuda_11.8', user_hash='ab12cd34') == 'cud-73-ab12cd34'
    assert common_utils.make_cluster_name_on_cloud_for_user(
        'Cuda_11.8', max_length=20,
        user_hash='ab12cd34') == 'cuda-11-8-ab12cd34'
    assert common_utils.make_cluster_name_on_cloud_for_user(
        'boltz25-feature-inference-hybrid-500-v1-68-ea9c52abfa',
        max_length=42,
        cluster_name_hash_length=8,
        user_hash='0ae18643') == ('boltz25-feature-inferenc-f41ggvch-0ae18643')


@pytest.mark.parametrize(
    ('display_name', 'max_length', 'cluster_name_hash_length'), [
        ('lora', 15, common_utils.CLUSTER_NAME_HASH_LENGTH),
        ('Cuda_11.8', 20, common_utils.CLUSTER_NAME_HASH_LENGTH),
        ('a-long-cluster-name', None, common_utils.CLUSTER_NAME_HASH_LENGTH),
        ('boltz25-feature-inference-hybrid-500-v1-68-ea9c52abfa', 42, 8),
    ])
def test_ambient_wrapper_has_explicit_helper_parity(
        display_name: str, max_length: int | None,
        cluster_name_hash_length: int) -> None:
    with mock.patch.object(common_utils,
                           'get_user_hash',
                           return_value='0ae18643') as get_user_hash:
        ambient = common_utils.make_cluster_name_on_cloud(
            display_name,
            max_length=max_length,
            cluster_name_hash_length=cluster_name_hash_length)
    explicit = common_utils.make_cluster_name_on_cloud_for_user(
        display_name,
        max_length=max_length,
        cluster_name_hash_length=cluster_name_hash_length,
        user_hash='0ae18643')
    assert ambient == explicit
    get_user_hash.assert_called_once_with()


def test_no_hash_compatibility_path_does_not_read_ambient_identity() -> None:
    with mock.patch.object(common_utils,
                           'get_user_hash',
                           side_effect=AssertionError('ambient read')):
        assert common_utils.make_cluster_name_on_cloud(
            'Cuda_11.8', add_user_hash=False) == 'cuda-11-8'
    assert common_utils.make_cluster_name_on_cloud_for_user(
        'Cuda_11.8', add_user_hash=False, user_hash='ignored') == 'cuda-11-8'


@pytest.mark.parametrize('user_hash', [
    '',
    '-leading',
    'has_underscore',
    'has space',
    'hash\n',
    'caf\N{LATIN SMALL LETTER E WITH ACUTE}',
])
def test_explicit_user_naming_rejects_invalid_hashes(user_hash: str) -> None:
    with pytest.raises(ValueError, match='user_hash'):
        common_utils.make_cluster_name_on_cloud_for_user('cluster',
                                                         user_hash=user_hash)


def test_explicit_user_naming_rejects_non_text_and_unrepresentable_hashes(
) -> None:
    with pytest.raises(TypeError, match='user_hash'):
        common_utils.make_cluster_name_on_cloud_for_user(
            'cluster', user_hash=None)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match='does not leave room'):
        common_utils.make_cluster_name_on_cloud_for_user(
            'cluster',
            max_length=42,
            cluster_name_hash_length=8,
            user_hash='a' * 100)


def _basis(**overrides: object) -> resource_actions.ProviderWorkloadNameBasisV1:
    values: dict[str, object] = {
        'version': 1,
        'display_name': 'boltz25-feature-inference-hybrid-500-v1-68-ea9c52abfa',
        'frozen_user_hash': '0ae18643',
        'max_length': 42,
        'cluster_name_hash_length': 8,
    }
    values.update(overrides)
    return resource_actions.ProviderWorkloadNameBasisV1(
        **values)  # type: ignore[arg-type]


def test_workload_name_basis_has_deterministic_derived_names() -> None:
    basis = _basis()
    assert basis.provider_cluster_name == (
        'boltz25-feature-inferenc-f41ggvch-0ae18643')
    assert basis.workload_name == (
        'boltz25-feature-inferenc-f41ggvch-0ae18643-head')
    assert len(basis.provider_cluster_name) == 42
    assert 'provider_cluster_name' not in basis.canonical_value()
    assert 'workload_name' not in basis.canonical_value()


def test_workload_name_basis_canonical_round_trip_and_bytes() -> None:
    basis = resource_actions.ProviderWorkloadNameBasisV1(
        version=1,
        display_name='Cuda_11.8',
        frozen_user_hash='0ae18643',
        max_length=42,
        cluster_name_hash_length=8)
    expected = {
        'version': 1,
        'display_name': 'Cuda_11.8',
        'frozen_user_hash': '0ae18643',
        'max_length': 42,
        'cluster_name_hash_length': 8,
    }
    assert basis.canonical_value() == expected
    assert basis.canonical_bytes == (
        b'{"cluster_name_hash_length":8,"display_name":"Cuda_11.8",'
        b'"frozen_user_hash":"0ae18643","max_length":42,"version":1}')
    assert resource_actions.ProviderWorkloadNameBasisV1.from_value(
        expected) == basis


@pytest.mark.parametrize(('field', 'value'), [
    ('version', 2),
    ('version', True),
    ('max_length', 41),
    ('max_length', True),
    ('cluster_name_hash_length', 2),
    ('cluster_name_hash_length', True),
])
def test_workload_name_basis_rejects_noncanonical_constants(
        field: str, value: object) -> None:
    with pytest.raises(ValueError):
        _basis(**{field: value})


def test_workload_name_basis_rejects_invalid_or_overlong_hash() -> None:
    with pytest.raises(ValueError):
        _basis(frozen_user_hash='invalid_hash')
    with pytest.raises(ValueError, match='1..31 UTF-8 bytes'):
        _basis(frozen_user_hash='a' * 32)
    # The longest bounded hash retains one display character and the full
    # collision-resistant display hash when truncation is required.
    assert len(_basis(frozen_user_hash='a' * 31).provider_cluster_name) == 42
    # Current service-account IDs are longer than legacy local user hashes but
    # remain within the frozen provider-name budget.
    assert _basis(frozen_user_hash='sa-deadbeef00112233').provider_cluster_name


def test_workload_name_basis_rejects_provider_invalid_derived_name() -> None:
    with pytest.raises(ValueError, match='DNS label'):
        _basis(frozen_user_hash='ends-with-hyphen-')
    with pytest.raises(ValueError, match='DNS label'):
        _basis(frozen_user_hash='UPPERCASE')


def test_workload_name_basis_rejects_unknown_or_missing_fields() -> None:
    value = _basis().canonical_value()
    with pytest.raises(ValueError, match='unknown or missing fields'):
        resource_actions.ProviderWorkloadNameBasisV1.from_value({
            **value, 'provider_cluster_name': 'forged'
        })
    missing = dict(value)
    del missing['max_length']
    with pytest.raises(ValueError, match='unknown or missing fields'):
        resource_actions.ProviderWorkloadNameBasisV1.from_value(missing)
