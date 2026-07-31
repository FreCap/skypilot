"""Focused tests for the inert revision-001 capacity foundation."""

from __future__ import annotations

import hashlib
import json
from typing import Any
import uuid

import pytest

from sky.physical_capacity import canonical
from sky.physical_capacity import config
from sky.physical_capacity import models


def _values(enum_type: Any) -> set[str]:
    return {member.value for member in enum_type}


def test_revision_001_row_enums_are_closed() -> None:
    assert _values(models.ProjectionSourceKind) == {
        'serve_service', 'serve_pool', 'managed_job_task'
    }
    assert _values(
        models.ProjectionScanState) == {'running', 'completed', 'failed'}
    assert _values(models.OwnerKind) == {'service', 'pool', 'managed_job_task'}
    assert _values(models.WriterFenceKind) == {
        'serve_lifecycle', 'controller_generation', 'legacy'
    }
    assert _values(
        models.ProjectionConfidence) == {'exact', 'legacy', 'unknown'}
    assert _values(
        models.GroupLifecycleState) == {'active', 'retiring', 'retired'}
    assert _values(models.AllocationSourceKind) == {
        'serve_replica', 'pool_worker', 'managed_job_cluster'
    }
    assert _values(
        models.AllocationIdentityConfidence) == {'exact', 'legacy', 'unknown'}
    assert _values(models.AllocationProjectionState) == {
        'current', 'source_missing', 'quarantined'
    }
    assert _values(models.AllocationObservedState) == {
        'unknown', 'provisioning', 'up', 'stopped', 'absent', 'failed',
        'partial'
    }
    assert _values(
        models.ObservationCertainty) == {'legacy', 'registry', 'provider'}
    assert _values(models.DesiredState) == {'present', 'stopped', 'absent'}
    assert _values(models.ReleaseGate) == {'blocked', 'open'}
    assert _values(models.DesireReasonCode) == {
        'projection', 'carry_forward', 'scale_up', 'replacement', 'recovery',
        'scale_down', 'teardown'
    }
    assert _values(models.ActorType) == {
        'system', 'basic', 'sa', 'sso', 'legacy', 'unknown'
    }


def test_canonical_envelope_is_deterministic_domain_separated_and_utf8(
) -> None:
    payload = {'z': [1, True, None], 'a': 'é'}
    encoded = canonical.canonical_json_bytes(
        payload, domain=canonical.CanonicalDomain.INTENT)
    assert encoded == (
        '{"domain":"intent","payload":{"a":"é","z":[1,true,null]},'
        '"schema_version":1}').encode()
    assert canonical.canonical_hash(
        payload, domain='intent') == hashlib.sha256(encoded).hexdigest()
    assert canonical.canonical_hash(
        payload,
        domain='intent') != canonical.canonical_hash(payload,
                                                     domain='physical_spec')


def test_unhashed_canonical_payload_encoding_is_bounded_and_domain_neutral(
) -> None:
    payload = {'z': [1, True, None], 'a': 'é'}
    assert canonical.canonical_payload_json_bytes(payload) == (
        '{"a":"é","z":[1,true,null]}').encode()

    oversized = {
        f'key-{index}': 'x' * canonical.MAX_CANONICAL_STRING_BYTES
        for index in range(17)
    }
    with pytest.raises(ValueError, match='exceeds 65536 bytes'):
        canonical.canonical_payload_json_bytes(oversized)


@pytest.mark.parametrize('payload', [
    [],
    'value',
    1,
    None,
])
def test_canonical_payload_requires_root_object(payload: object) -> None:
    with pytest.raises(ValueError, match='root object'):
        canonical.validate_payload(payload)


@pytest.mark.parametrize('invalid', [
    0.0,
    1.25,
    float('nan'),
    float('inf'),
])
def test_canonical_payload_rejects_all_floats(invalid: float) -> None:
    with pytest.raises(ValueError, match='floating-point'):
        canonical.validate_payload({'invalid': invalid})


@pytest.mark.parametrize('invalid', [
    -(1 << 63) - 1,
    1 << 63,
])
def test_canonical_payload_rejects_out_of_range_integers(invalid: int) -> None:
    with pytest.raises(ValueError, match='signed 64-bit'):
        canonical.validate_payload({'invalid': invalid})


def test_canonical_payload_accepts_signed_64_bit_boundaries() -> None:
    canonical.validate_payload({
        'minimum': -(1 << 63),
        'maximum': (1 << 63) - 1,
    })


@pytest.mark.parametrize('invalid', [
    b'bytes',
    ('tuple',),
    {
        1: 'non-string-key'
    },
])
def test_canonical_payload_rejects_non_json_types(invalid: object) -> None:
    with pytest.raises(ValueError):
        canonical.validate_payload(
            {'invalid': invalid} if not isinstance(invalid, dict) else invalid)


def test_canonical_payload_enforces_utf8_string_byte_limit() -> None:
    canonical.validate_payload(
        {'value': 'é' * (canonical.MAX_CANONICAL_STRING_BYTES // 2)})
    with pytest.raises(ValueError, match='UTF-8 bytes'):
        canonical.validate_payload(
            {'value': 'é' * (canonical.MAX_CANONICAL_STRING_BYTES // 2 + 1)})
    with pytest.raises(ValueError, match='valid UTF-8'):
        canonical.validate_payload({'value': '\ud800'})


def test_canonical_payload_enforces_nesting_limit() -> None:
    accepted: dict[str, Any] = {}
    for _ in range(canonical.MAX_CANONICAL_DEPTH - 1):
        accepted = {'child': accepted}
    canonical.validate_payload(accepted)

    rejected = {'child': accepted}
    with pytest.raises(ValueError, match='nesting'):
        canonical.validate_payload(rejected)


def test_canonical_payload_enforces_aggregate_item_limit() -> None:
    canonical.validate_payload(
        {'items': [None] * (canonical.MAX_CANONICAL_ITEMS - 1)})
    with pytest.raises(ValueError, match='aggregate'):
        canonical.validate_payload(
            {'items': [None] * canonical.MAX_CANONICAL_ITEMS})


def test_canonical_payload_rejects_reference_cycles() -> None:
    payload: dict[str, Any] = {}
    payload['self'] = payload
    with pytest.raises(ValueError, match='cycles'):
        canonical.validate_payload(payload)


def test_canonical_envelope_enforces_encoded_size_domain_and_version() -> None:
    oversized = {
        f'key-{index}': 'x' * canonical.MAX_CANONICAL_STRING_BYTES
        for index in range(17)
    }
    with pytest.raises(ValueError, match='exceeds 65536 bytes'):
        canonical.canonical_json_bytes(oversized, domain='intent')
    with pytest.raises(ValueError, match='Unknown canonical domain'):
        canonical.canonical_json_bytes({}, domain='unreviewed')
    for invalid_version in (True, 0, 2):
        with pytest.raises(ValueError, match='schema_version'):
            canonical.canonical_json_bytes({},
                                           domain='intent',
                                           schema_version=invalid_version)


def test_validate_bounded_string_counts_utf8_bytes() -> None:
    assert canonical.validate_bounded_string('é', max_bytes=2,
                                             field='value') == 'é'
    with pytest.raises(ValueError, match='UTF-8 bytes'):
        canonical.validate_bounded_string('é', max_bytes=1, field='value')
    with pytest.raises(ValueError, match='must not be empty'):
        canonical.validate_bounded_string('', max_bytes=1, field='value')


def test_load_config_defaults_to_disabled_and_is_uncached(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(config.PHYSICAL_CAPACITY_MODE_ENV_VAR, raising=False)
    monkeypatch.delenv(config.PHYSICAL_CAPACITY_ALLOWLIST_ENV_VAR,
                       raising=False)
    assert config.load_config() == config.CapacityConfig()

    monkeypatch.setenv(config.PHYSICAL_CAPACITY_MODE_ENV_VAR, 'shadow')
    assert config.load_config().mode is config.CapacityMode.SHADOW


@pytest.mark.parametrize('mode', list(config.CapacityMode))
def test_load_config_recognizes_closed_modes(monkeypatch: pytest.MonkeyPatch,
                                             mode: config.CapacityMode) -> None:
    monkeypatch.setenv(config.PHYSICAL_CAPACITY_MODE_ENV_VAR, mode.value)
    assert config.load_config().mode is mode


def test_load_config_parses_strict_bounded_allowlist(
        monkeypatch: pytest.MonkeyPatch) -> None:
    group_id = str(uuid.uuid4())
    value = {
        'providers': ['aws', 'gcp', 'kubernetes'],
        'workspaces': ['default', 'research_1'],
        'owner_kinds': ['service', 'pool', 'managed_job_task'],
        'groups': [group_id],
        'verbs': ['observe', 'launch', 'start', 'stop', 'down', 'occupy'],
    }
    monkeypatch.setenv(config.PHYSICAL_CAPACITY_ALLOWLIST_ENV_VAR,
                       json.dumps(value))
    loaded = config.load_config()
    assert loaded.allowlist == config.CapacityAllowlist(
        providers=tuple(config.CapacityProvider),
        workspaces=('default', 'research_1'),
        owner_kinds=tuple(models.OwnerKind),
        groups=(group_id,),
        verbs=tuple(config.CapacityVerb),
    )


@pytest.mark.parametrize('raw,match', [
    ('[]', 'JSON object'),
    ('{"unknown":[]}', 'Unknown capacity allowlist'),
    ('{"providers":"aws"}', 'JSON array'),
    ('{"providers":["azure"]}', 'Unknown capacity allowlist'),
    ('{"owner_kinds":["user_cluster"]}', 'Unknown capacity allowlist'),
    ('{"verbs":["delete"]}', 'Unknown capacity allowlist'),
    ('{"groups":["not-a-uuid"]}', 'group UUID'),
    ('{"groups":["00000000-0000-0000-0000-00000000000A"]}',
     'canonical lowercase'),
    ('{"workspaces":["Invalid Workspace"]}', 'Invalid capacity'),
    ('{"providers":["aws","aws"]}', 'duplicates'),
    ('{"providers":["aws"],"providers":["gcp"]}', 'Duplicate JSON'),
    ('{"providers":[NaN]}', 'Non-standard JSON'),
])
def test_load_config_rejects_malformed_allowlist(
        monkeypatch: pytest.MonkeyPatch, raw: str, match: str) -> None:
    monkeypatch.setenv(config.PHYSICAL_CAPACITY_ALLOWLIST_ENV_VAR, raw)
    with pytest.raises(ValueError, match=match):
        config.load_config()


def test_load_config_enforces_allowlist_limits(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.PHYSICAL_CAPACITY_ALLOWLIST_ENV_VAR,
                       json.dumps({'workspaces': ['x' * 513]}))
    with pytest.raises(ValueError, match='512 UTF-8 bytes'):
        config.load_config()

    monkeypatch.setenv(
        config.PHYSICAL_CAPACITY_ALLOWLIST_ENV_VAR,
        json.dumps({
            'workspaces': [
                f'w{index}'
                for index in range(config.MAX_ALLOWLIST_ENTRIES_PER_FIELD + 1)
            ]
        }))
    with pytest.raises(ValueError, match='at most 1000 entries'):
        config.load_config()

    monkeypatch.setenv(config.PHYSICAL_CAPACITY_ALLOWLIST_ENV_VAR,
                       ' ' * (config.MAX_ALLOWLIST_JSON_BYTES + 1))
    with pytest.raises(ValueError, match='at most 65536'):
        config.load_config()


def test_load_config_rejects_unknown_mode(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.PHYSICAL_CAPACITY_MODE_ENV_VAR, 'active')
    with pytest.raises(ValueError, match='Unknown physical-capacity mode'):
        config.load_config()


@pytest.mark.parametrize('mode', [
    config.CapacityMode.DISABLED,
    config.CapacityMode.SHADOW,
])
def test_revision_001_runtime_capability_accepts_only_inert_modes(
        mode: config.CapacityMode) -> None:
    config.validate_runtime_capability(config.CapacityConfig(mode=mode))


@pytest.mark.parametrize('mode', [
    config.CapacityMode.OBSERVE,
    config.CapacityMode.TEARDOWN,
    config.CapacityMode.SERVE,
    config.CapacityMode.JOBS,
])
def test_revision_001_runtime_capability_rejects_future_modes(
        mode: config.CapacityMode) -> None:
    with pytest.raises(RuntimeError, match='unavailable'):
        config.validate_runtime_capability(config.CapacityConfig(mode=mode))


def test_runtime_capability_rejects_unimplemented_revision() -> None:
    with pytest.raises(ValueError, match='Unsupported'):
        config.validate_runtime_capability(config.CapacityConfig(),
                                           revision='002')
