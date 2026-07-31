"""Tests for C2's strict, common-initialization configuration contract."""

import json

import pytest

from sky.physical_capacity import config
from sky.physical_capacity import contracts
from sky.physical_capacity import hashing
from sky.physical_capacity import models


def _serve_selector(*,
                    workspace: str = 'default',
                    kind: str = 'serve_service',
                    name: str = 'svc') -> dict[str, object]:
    return {
        'workspace': workspace,
        'source_kind': kind,
        'service_name': name,
    }


def _managed_selector(*,
                      workspace: str = 'default',
                      job_id: object = 7,
                      task_id: object = 0) -> dict[str, object]:
    return {
        'workspace': workspace,
        'source_kind': 'managed_job_task',
        'spot_job_id': job_id,
        'task_id': task_id,
    }


def _set_shadow(monkeypatch: pytest.MonkeyPatch,
                selectors: list[dict[str, object]]) -> None:
    monkeypatch.setenv(config.MODE_ENV_VAR, 'shadow')
    monkeypatch.setenv(config.SOURCES_ENV_VAR, json.dumps(selectors))
    monkeypatch.setenv(config.PILOT_END_ENV_VAR, '2026-08-31T00:00:00Z')


def test_load_config_parses_and_canonically_sorts_typed_selectors(
        monkeypatch: pytest.MonkeyPatch) -> None:
    selectors = [
        _managed_selector(job_id=9, task_id=2),
        _serve_selector(kind='serve_pool', name='pool-a'),
        _serve_selector(name='service-a'),
    ]
    _set_shadow(monkeypatch, list(reversed(selectors)))
    loaded = config.load_config()

    assert loaded.mode is config.CapacityMode.SHADOW
    assert loaded.pilot_end_utc == '2026-08-31T00:00:00Z'
    assert set(loaded.sources) == {
        contracts.ServeSourceSelector(
            workspace='default',
            source_kind=models.ProjectionSourceKind.SERVE_SERVICE,
            service_name='service-a'),
        contracts.ServeSourceSelector(
            workspace='default',
            source_kind=models.ProjectionSourceKind.SERVE_POOL,
            service_name='pool-a'),
        contracts.ManagedJobTaskSelector(workspace='default',
                                         spot_job_id=9,
                                         task_id=2),
    }
    first_order = loaded.sources
    monkeypatch.setenv(config.SOURCES_ENV_VAR, json.dumps(selectors))
    assert config.load_config().sources == first_order
    assert loaded.partitions == (
        contracts.SourcePartition('default',
                                  models.ProjectionSourceKind.MANAGED_JOB_TASK),
        contracts.SourcePartition('default',
                                  models.ProjectionSourceKind.SERVE_POOL),
        contracts.SourcePartition('default',
                                  models.ProjectionSourceKind.SERVE_SERVICE),
    )


@pytest.mark.parametrize('raw,match', [
    ('{}', 'top-level JSON array'),
    ('"selector"', 'top-level JSON array'),
    ('[[{}]]', 'must be a JSON object'),
    ('[{"workspace":"default","source_kind":"serve_service",'
     '"service_name":"a","extra":1}]', 'fields must match'),
    ('[{"workspace":"default","source_kind":"serve_service",'
     '"service_name":"a","service_name":"b"}]', 'Duplicate JSON'),
    ('[{"workspace":"default","source_kind":"unknown",'
     '"service_name":"a"}]', 'Unknown selector'),
    ('[NaN]', 'Non-standard JSON'),
])
def test_sources_require_one_strict_top_level_selector_array(
        monkeypatch: pytest.MonkeyPatch, raw: str, match: str) -> None:
    monkeypatch.setenv(config.SOURCES_ENV_VAR, raw)
    with pytest.raises(ValueError, match=match):
        config.load_config()


@pytest.mark.parametrize('selector,match', [
    (_managed_selector(job_id=True), 'must be an integer'),
    (_managed_selector(job_id=0), 'between 1'),
    (_managed_selector(task_id=-1), 'between 0'),
    (_serve_selector(workspace='Invalid Workspace'), 'Invalid selector'),
    (_serve_selector(name=''), 'must not be empty'),
    (_serve_selector(name='x' * 257), 'at most 256'),
])
def test_sources_reject_invalid_typed_values(monkeypatch: pytest.MonkeyPatch,
                                             selector: dict[str, object],
                                             match: str) -> None:
    monkeypatch.setenv(config.SOURCES_ENV_VAR, json.dumps([selector]))
    with pytest.raises(ValueError, match=match):
        config.load_config()


def test_sources_reject_duplicate_canonical_selector_and_partition_overflow(
        monkeypatch: pytest.MonkeyPatch) -> None:
    selector = _serve_selector()
    monkeypatch.setenv(config.SOURCES_ENV_VAR, json.dumps([selector, selector]))
    with pytest.raises(ValueError, match='must not contain duplicates'):
        config.load_config()

    monkeypatch.setenv(
        config.SOURCES_ENV_VAR,
        json.dumps([
            _serve_selector(workspace=f'w{index}')
            for index in range(config.MAX_SOURCE_PARTITIONS + 1)
        ]))
    with pytest.raises(ValueError, match='at most 16 partitions'):
        config.load_config()


def test_sources_enforce_raw_utf8_bound(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.SOURCES_ENV_VAR,
                       ' ' * (config.MAX_SOURCES_JSON_BYTES + 1))
    with pytest.raises(ValueError, match='at most 65536 UTF-8 bytes'):
        config.load_config()


def test_sources_accept_exact_64_kib_valid_json(
        monkeypatch: pytest.MonkeyPatch) -> None:
    base = json.dumps([_serve_selector(name='boundary')], separators=(',', ':'))
    padding_bytes = config.MAX_SOURCES_JSON_BYTES - len(base.encode('utf-8'))
    assert padding_bytes > 0
    raw = base + ' ' * padding_bytes
    assert len(raw.encode('utf-8')) == config.MAX_SOURCES_JSON_BYTES

    monkeypatch.setenv(config.SOURCES_ENV_VAR, raw)
    assert config.load_config().sources == (contracts.ServeSourceSelector(
        'default', models.ProjectionSourceKind.SERVE_SERVICE, 'boundary'),)


def test_non_ascii_escaped_and_raw_selector_permutations_are_identical(
        monkeypatch: pytest.MonkeyPatch) -> None:
    selectors = [
        _serve_selector(kind='serve_pool', name='pøøl-推理'),
        _managed_selector(job_id=42),
        _serve_selector(name='café-推理'),
    ]
    escaped = json.dumps(selectors, ensure_ascii=True)
    raw_utf8 = json.dumps(list(reversed(selectors)), ensure_ascii=False)
    assert escaped != raw_utf8

    monkeypatch.setenv(config.SOURCES_ENV_VAR, escaped)
    escaped_config = config.load_config()
    monkeypatch.setenv(config.SOURCES_ENV_VAR, raw_utf8)
    raw_config = config.load_config()
    assert escaped_config.sources == raw_config.sources

    partition = contracts.SourcePartition(
        'default', models.ProjectionSourceKind.MANAGED_JOB_TASK)
    escaped_hash = hashing.projection_scope_hash(partition,
                                                 escaped_config.sources,
                                                 '2026-08-31T00:00:00Z')
    raw_hash = hashing.projection_scope_hash(partition, raw_config.sources,
                                             '2026-08-31T00:00:00Z')
    assert escaped_hash == raw_hash


@pytest.mark.parametrize('value', [
    '2026-08-31T00:00:00.000Z',
    '2026-08-31T00:00:00+00:00',
    '2026-08-31 00:00:00Z',
    '2026-02-30T00:00:00Z',
    '٢٠٢٦-08-31T00:00:00Z',
    '２０２６-08-31T00:00:00Z',
    '',
])
def test_pilot_end_requires_exact_valid_utc_form(
        monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv(config.PILOT_END_ENV_VAR, value)
    with pytest.raises(ValueError, match='pilot end'):
        config.load_config()


def test_selectors_must_be_admitted_by_nonempty_allowlists(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config.SOURCES_ENV_VAR, json.dumps([_serve_selector()]))
    monkeypatch.setenv(config.ALLOWLIST_ENV_VAR,
                       json.dumps({'workspaces': ['research']}))
    with pytest.raises(ValueError, match='not admitted.*workspace'):
        config.load_config()

    monkeypatch.setenv(config.ALLOWLIST_ENV_VAR,
                       json.dumps({'owner_kinds': ['pool']}))
    with pytest.raises(ValueError, match='not admitted.*owner-kind'):
        config.load_config()


def test_common_runtime_gate_is_pure_and_shadow_specific(
        monkeypatch: pytest.MonkeyPatch) -> None:
    _set_shadow(monkeypatch, [_serve_selector()])
    loaded = config.load_config()
    config.validate_common_runtime_environment(loaded,
                                               server_role='controller',
                                               request_backend='postgres')

    with pytest.raises(RuntimeError, match='split controller'):
        config.validate_common_runtime_environment(loaded,
                                                   server_role='api',
                                                   request_backend='postgres')
    with pytest.raises(RuntimeError, match='PostgreSQL'):
        config.validate_common_runtime_environment(loaded,
                                                   server_role='controller',
                                                   request_backend='sqlite')


def test_common_runtime_gate_requires_sources_end_and_empty_group_allowlist(
) -> None:
    with pytest.raises(RuntimeError, match='at least one selector'):
        config.validate_common_runtime_environment(
            config.CapacityConfig(mode=config.CapacityMode.SHADOW),
            server_role='controller',
            request_backend='postgres')

    source = contracts.ServeSourceSelector(
        'default', models.ProjectionSourceKind.SERVE_SERVICE, 'svc')
    with pytest.raises(RuntimeError, match='PILOT_END'):
        config.validate_common_runtime_environment(config.CapacityConfig(
            mode=config.CapacityMode.SHADOW, sources=(source,)),
                                                   server_role='controller',
                                                   request_backend='postgres')

    group_allowlist = config.CapacityAllowlist(
        groups=('00000000-0000-0000-0000-000000000001',))
    with pytest.raises(RuntimeError, match='group allowlist'):
        config.validate_common_runtime_environment(config.CapacityConfig(
            mode=config.CapacityMode.SHADOW,
            allowlist=group_allowlist,
            sources=(source,),
            pilot_end_utc='2026-08-31T00:00:00Z'),
                                                   server_role='controller',
                                                   request_backend='postgres')


def test_disabled_common_gate_does_not_require_shadow_variables() -> None:
    config.validate_common_runtime_environment(config.CapacityConfig())
