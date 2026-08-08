"""Tests for the persisted placement-normalizer identity contract."""

import uuid

import pytest

from sky.serve import placement_normalization_identity
from sky.serve import serve_state


@pytest.mark.parametrize('protocol', [
    placement_normalization_identity.PROTOCOL_V1,
    placement_normalization_identity.PROTOCOL_V2,
    placement_normalization_identity.PROTOCOL_V3,
    placement_normalization_identity.PROTOCOL_V4,
])
def test_parse_normalizer_identity_accepts_exact_supported_protocols(protocol):
    commit = '0123456789abcdef' * 2 + '01234567'

    identity = placement_normalization_identity.parse_normalizer_identity(
        f'{protocol}:{commit}')

    assert identity == (
        placement_normalization_identity.PlacementNormalizationIdentity(
            protocol=protocol, image_commit=commit))


@pytest.mark.parametrize('value', [
    None,
    True,
    2,
    '',
    '2',
    '2:',
    f'0:{"a" * 40}',
    f'5:{"a" * 40}',
    f'02:{"a" * 40}',
    f'2:{"a" * 39}',
    f'2:{"a" * 41}',
    f'2:{"A" * 40}',
    f' 2:{"a" * 40}',
    f'2:{"a" * 40} ',
    f'2:{"a" * 40}\n',
    f'2:{"a" * 40}-dirty',
])
def test_parse_normalizer_identity_rejects_noncanonical_values(value):
    with pytest.raises(placement_normalization_identity.
                       PlacementNormalizationIdentityError):
        placement_normalization_identity.parse_normalizer_identity(value)


def test_format_normalizer_identity_emits_only_current_protocol():
    commit = 'fedcba9876543210' * 2 + 'fedcba98'

    value = placement_normalization_identity.format_normalizer_identity(commit)

    assert value == f'4:{commit}'
    assert placement_normalization_identity.parse_normalizer_identity(
        value).protocol == placement_normalization_identity.CURRENT_PROTOCOL


@pytest.mark.parametrize('commit', [
    None,
    True,
    '',
    'a' * 39,
    'a' * 41,
    'A' * 40,
    'a' * 40 + '-dirty',
])
def test_format_normalizer_identity_rejects_invalid_commits(commit):
    with pytest.raises(placement_normalization_identity.
                       PlacementNormalizationIdentityError):
        placement_normalization_identity.format_normalizer_identity(commit)


@pytest.mark.parametrize('mode', [
    placement_normalization_identity.APPLY_SUPPORTED_MODE,
    placement_normalization_identity.RETIRE_TERMINAL_HISTORICAL_MODE,
])
def test_parse_manifest_mode_accepts_only_exact_modes(mode):
    assert placement_normalization_identity.parse_manifest_mode(mode) == mode


@pytest.mark.parametrize('mode', [
    None,
    True,
    '',
    'apply',
    ' apply_supported',
    'apply_supported\n',
])
def test_parse_manifest_mode_rejects_aliases_and_coercions(mode):
    with pytest.raises(placement_normalization_identity.
                       PlacementNormalizationIdentityError):
        placement_normalization_identity.parse_manifest_mode(mode)


_FROZEN_APPLY_OUTCOMES = frozenset({
    ('placeholder', 'unchanged'),
    ('explicit_v1', 'changed'),
    ('explicit_v2', 'unchanged'),
    ('fieldless_supported', 'changed'),
    ('historical_physical_per_gpu', 'unchanged'),
    ('retired', 'unchanged'),
})
_FROZEN_RETIREMENT_OUTCOMES = frozenset({
    ('placeholder', 'unchanged'),
    ('explicit_v2', 'unchanged'),
    ('historical_physical_per_gpu', 'retired'),
    ('retired', 'unchanged'),
})


@pytest.mark.parametrize('protocol', [1, 2, 3])
def test_v1_to_v3_manifest_outcome_matrices_remain_frozen(protocol):
    identity = placement_normalization_identity.parse_normalizer_identity(
        f'{protocol}:{"c" * 40}')
    apply_mode = placement_normalization_identity.APPLY_SUPPORTED_MODE
    retirement_mode = (
        placement_normalization_identity.RETIRE_TERMINAL_HISTORICAL_MODE)

    assert placement_normalization_identity.allowed_manifest_outcomes(
        identity, apply_mode) == _FROZEN_APPLY_OUTCOMES
    assert placement_normalization_identity.allowed_manifest_outcomes(
        identity, retirement_mode) == _FROZEN_RETIREMENT_OUTCOMES
    assert placement_normalization_identity.is_loadable_manifest_outcome(
        identity, apply_mode, 'fieldless_supported', 'changed')
    assert not placement_normalization_identity.is_loadable_manifest_outcome(
        identity, retirement_mode, 'fieldless_supported', 'changed')
    assert placement_normalization_identity.is_loadable_manifest_outcome(
        identity, retirement_mode, 'explicit_v2', 'unchanged')
    assert placement_normalization_identity.is_fillable_manifest_outcome(
        identity, apply_mode, 'placeholder', 'unchanged')
    assert placement_normalization_identity.is_fillable_manifest_outcome(
        identity, retirement_mode, 'placeholder', 'unchanged')


def test_v4_retirement_distinguishes_stale_placeholder_from_fillable():
    identity = placement_normalization_identity.parse_normalizer_identity(
        f'4:{"c" * 40}')
    apply_mode = placement_normalization_identity.APPLY_SUPPORTED_MODE
    retirement_mode = (
        placement_normalization_identity.RETIRE_TERMINAL_HISTORICAL_MODE)

    assert placement_normalization_identity.allowed_manifest_outcomes(
        identity, apply_mode) == _FROZEN_APPLY_OUTCOMES
    assert placement_normalization_identity.allowed_manifest_outcomes(
        identity, retirement_mode) == _FROZEN_RETIREMENT_OUTCOMES | {
            ('stale_placeholder', 'unchanged'),
        }
    assert placement_normalization_identity.is_fillable_manifest_outcome(
        identity, apply_mode, 'placeholder', 'unchanged')
    assert placement_normalization_identity.is_fillable_manifest_outcome(
        identity, retirement_mode, 'placeholder', 'unchanged')
    assert not placement_normalization_identity.is_loadable_manifest_outcome(
        identity, retirement_mode, 'stale_placeholder', 'unchanged')
    assert not placement_normalization_identity.is_fillable_manifest_outcome(
        identity, retirement_mode, 'stale_placeholder', 'unchanged')


@pytest.mark.parametrize('protocol', [1, 2, 3])
def test_historical_protocols_reject_stale_placeholder(protocol):
    identity = placement_normalization_identity.parse_normalizer_identity(
        f'{protocol}:{"c" * 40}')

    assert ('stale_placeholder', 'unchanged') not in (
        placement_normalization_identity.allowed_manifest_outcomes(
            identity,
            placement_normalization_identity.RETIRE_TERMINAL_HISTORICAL_MODE))


@pytest.mark.parametrize('protocol', [1, 2, 3, 4])
@pytest.mark.parametrize('mode', [
    placement_normalization_identity.APPLY_SUPPORTED_MODE,
    placement_normalization_identity.RETIRE_TERMINAL_HISTORICAL_MODE,
])
def test_stale_placeholder_is_never_fillable(protocol, mode):
    identity = placement_normalization_identity.parse_normalizer_identity(
        f'{protocol}:{"c" * 40}')

    assert not placement_normalization_identity.is_fillable_manifest_outcome(
        identity, mode, 'stale_placeholder', 'unchanged')


def _manifest_row(normalizer_version):
    digest = 'a' * 64
    return {
        'manifest_run_id': uuid.UUID(int=1),
        'manifest_mode': 'apply_supported',
        'manifest_normalizer_version': normalizer_version,
        'manifest_schema_revision': '037',
        'manifest_release_version': 'test-release',
        'manifest_started_at': 1.0,
        'manifest_completed_at': 2.0,
        'manifest_row_bound': 1,
        'manifest_row_count': 1,
        'manifest_classification_counts': {
            'explicit_v2': 1,
        },
        'manifest_pre_inventory_sha256': digest,
        'manifest_post_inventory_sha256': digest,
        'manifest_freeze_evidence_sha256': digest,
    }


@pytest.mark.parametrize('protocol', [1, 2, 3, 4])
def test_serve_state_manifest_accepts_each_supported_identity(protocol):
    row = _manifest_row(f'{protocol}:{"b" * 40}')

    completed_at = serve_state._validate_placement_normalization_run_manifest(  # pylint: disable=protected-access
        row, row['manifest_run_id'])

    assert completed_at == 2.0


@pytest.mark.parametrize('normalizer_version', [
    '1:test',
    f'5:{"b" * 40}',
    f'2:{"B" * 40}',
    f'2:{"b" * 40}-dirty',
])
def test_serve_state_manifest_rejects_noncanonical_identity(normalizer_version):
    row = _manifest_row(normalizer_version)

    with pytest.raises(RuntimeError, match='invalid release identity'):
        serve_state._validate_placement_normalization_run_manifest(  # pylint: disable=protected-access
            row, row['manifest_run_id'])
