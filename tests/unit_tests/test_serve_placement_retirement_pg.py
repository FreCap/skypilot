"""PostgreSQL transaction coverage for placement-contract retirement."""

# pylint: disable=protected-access,redefined-outer-name,unused-import
import base64
import concurrent.futures
import copy
import dataclasses
import hashlib
import json
import pathlib
import pickle
import threading
from typing import Any
import uuid
import zlib

import pytest
import sqlalchemy
from sqlalchemy import orm
from test_serve_placement_contract import _explicit_v2_payload
from test_serve_placement_contract import _normalizer_work
from test_serve_placement_contract import (
    _V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64)
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

import sky
from sky.serve import constants as serve_constants
from sky.serve import ephemeral_storage_contract
from sky.serve import placement_contract_normalization
from sky.serve import placement_normalization_identity
from sky.serve import placement_normalization_manifest
from sky.serve import serve_state

_SERVICE_NAME = 'svc-production-shaped-retirement'
_RESOURCE_SCOPE = 'service-incarnation-a'
_PLACEHOLDER_VERSION_IDS = (3, 10)
_COMMITTED_VERSION_IDS = (1, 2, 4, 5, 6, 7, 8, 9, *range(11, 52))
_ALL_VERSION_IDS = tuple(range(1, 52))
_COMMITTED_VERSION_COUNT = len(_COMMITTED_VERSION_IDS)
_TOTAL_VERSION_COUNT = len(_ALL_VERSION_IDS)
_CURRENT_VERSION = max(_COMMITTED_VERSION_IDS)
_HISTORICAL_VERSION = 2
_LEGACY_NULL_VERSION_IDS = frozenset({1, 2, 4, 5, 6, 7})
_LEGACY_NULL_VERSION_COUNT = len(_LEGACY_NULL_VERSION_IDS)
_TIMESTAMP_BOUNDARY_VERSION = 8
_TIMESTAMP_BOUNDARY_CREATED_AT = 100.0 + _TIMESTAMP_BOUNDARY_VERSION
_STALE_PLACEHOLDER_SCHEMA = 'skyserve-stale-placeholder-retirement-v1'
_PLACEHOLDER_NULL_COLUMNS = (
    'yaml_content',
    'submitted_yaml_content',
    'placement_catalog',
    'controller_config',
    'controller_config_digest',
    'controller_config_snapshot_id',
    'controller_applied_at',
    'quarantined_at',
    'quarantine_reason',
    'retired_yaml_content',
    'retired_at',
    'retirement_reason',
    'retirement_run_id',
    'resource_action_spec_identity',
    'resource_action_spec_identity_sha256',
)
_ROW_BOUND = 100
_PREDECESSOR_COMMIT = 'b' * 40
_PRODUCTION_SNAPSHOT_PATH = pathlib.Path(__file__).with_name(
    'fixtures') / 'serve_placement_normalization_run_3bacd32f.json.zlib.b64'
_PRODUCTION_SNAPSHOT_SHA256 = (
    '5067bd30eb5c2b2604ba1302d020e8e609cec81d25afd74702d2917e1af27ef6')


@dataclasses.dataclass(frozen=True)
class _RetirementFixture:
    predecessor_run_id: uuid.UUID
    historical_payload: bytes
    historical_yaml: str
    intent_preimages: tuple[dict[str, Any], ...]


def _api_pod_identity() -> placement_contract_normalization._ApiPodIdentity:
    return placement_contract_normalization._canonical_api_pod_identity(
        'pod-a', uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'))


def _cleanup_yaml(version: int) -> str:
    generation = f'generation-{version:02d}'
    scope_id = ephemeral_storage_contract.canonical_ephemeral_storage_scope_id(
        _RESOURCE_SCOPE, generation)
    metadata_key = serve_constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY
    return f"""\
_metadata:
  {metadata_key}:
    resource_scope: {_RESOURCE_SCOPE}
    scope_id: {scope_id}
    storage_generation: {generation}
    storage_mounts: []
service: {{}}
"""


def _zero_evidence() -> placement_contract_normalization._ExternalEvidence:
    return placement_contract_normalization._ExternalEvidence(count=0,
                                                              digest='0' * 64)


def _canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True,
                   separators=(',', ':')).encode()).hexdigest()


def _expected_timestamp_inventory() -> list[list[Any]]:
    service_name_sha256 = hashlib.sha256(_SERVICE_NAME.encode()).hexdigest()
    inventory = []
    for version in _COMMITTED_VERSION_IDS:
        legacy_null = version in _LEGACY_NULL_VERSION_IDS
        intent_key_sha256 = _canonical_json_sha256(
            [_SERVICE_NAME, _RESOURCE_SCOPE, f'generation-{version:02d}'])
        inventory.append([
            service_name_sha256,
            version,
            'committed',
            'legacy_prefix_null' if legacy_null else 'finite',
            None if legacy_null else 100.0 + version,
            _TIMESTAMP_BOUNDARY_VERSION if legacy_null else None,
            _TIMESTAMP_BOUNDARY_CREATED_AT if legacy_null else None,
            intent_key_sha256,
            float(version),
        ])
    return inventory


def _no_resource_actions(
    _engine: sqlalchemy.engine.Engine,
    targets: frozenset[placement_contract_normalization._ResourceActionTarget],
) -> dict[tuple[str, int], placement_contract_normalization._ExternalEvidence]:
    return {
        (target.service_name, target.version): _zero_evidence()
        for target in targets
    }


def _normalization_kwargs(engine: sqlalchemy.engine.Engine) -> dict[str, Any]:
    return {
        'engine': engine,
        'row_bound': _ROW_BOUND,
        'freeze_evidence_sha256': 'f' * 64,
        'request_evidence_getter': lambda _engine: _zero_evidence(),
        'api_pod_checker': lambda _engine: _api_pod_identity(),
    }


def _retirement_kwargs(engine: sqlalchemy.engine.Engine) -> dict[str, Any]:
    return {
        **_normalization_kwargs(engine),
        'mode': (placement_contract_normalization.ApplyMode.
                 RETIRE_TERMINAL_HISTORICAL),
        'approved_loaded_image_commits': (_PREDECESSOR_COMMIT,),
        'image_evidence_getter': lambda _name, _version: _zero_evidence(),
        'process_evidence_getter': lambda _targets, _pod_uid: _zero_evidence(),
        'resource_action_evidence_getter': _no_resource_actions,
        'controller_hold_checker': lambda: True,
        'consolidation_mode_checker': lambda: True,
        'legacy_controller_evidence_getter': _zero_evidence,
    }


def _read_intents(
    engine: sqlalchemy.engine.Engine,) -> tuple[dict[str, Any], ...]:
    table = serve_state.ephemeral_storage_cleanup_intents_table
    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(table).where(
                table.c.service_name == _SERVICE_NAME).order_by(
                    table.c.resource_scope,
                    table.c.storage_generation)).mappings().all()
    return tuple(dict(row) for row in rows)


def _seed_predecessor_receipt(
    engine: sqlalchemy.engine.Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> uuid.UUID:
    """Seed one valid completed receipt in the real PostgreSQL schema."""
    serve_state.Base.metadata.create_all(engine)
    monkeypatch.setattr(sky, '__commit__', 'a' * 40)
    row = _normalizer_work(_explicit_v2_payload(), 2)
    run_id = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name='svc-receipt-blocker',
            workspace='workspace',
            status=serve_state.ServiceStatus.READY.value,
            current_version=2,
            active_versions='[2]',
            pool=0,
            controller_pid=123,
            controller_ip='10.0.0.1',
            hash='current-hash',
            lifecycle_epoch=7,
            resource_scope='current-hash',
            logical_replica_semantics=0))
        version = dict(row.original)
        version['service_name'] = 'svc-receipt-blocker'
        connection.execute(
            serve_state.version_specs_table.insert().values(**version))
    row.original['service_name'] = 'svc-receipt-blocker'
    row.result['service_name'] = 'svc-receipt-blocker'
    with orm.Session(engine) as session, session.begin():
        placement_contract_normalization._insert_ledger(
            session, [row],
            run_id=run_id,
            mode=placement_contract_normalization.ApplyMode.SUPPORTED,
            row_bound=10,
            started_at=1.0,
            completed_at=2.0,
            freeze_evidence_sha256='f' * 64,
            pre_digest=placement_contract_normalization._fleet_sha256(
                [row], result=False),
            post_digest=placement_contract_normalization._fleet_sha256(
                [row], result=True))
    with engine.begin() as connection:
        connection.execute(serve_state.services_table.update().where(
            serve_state.services_table.c.name == 'svc-receipt-blocker').values(
                placement_normalization_requested_run_id=run_id,
                placement_normalization_loaded_run_id=run_id,
                placement_normalization_loaded_image_commit='b' * 40,
                placement_normalization_loaded_controller_pid=456,
                placement_normalization_loaded_controller_ip='10.0.0.99',
                placement_normalization_loaded_boot_id='c' * 32,
                placement_normalization_loaded_at=3.0))
    return run_id


def _read_normalization_inventory(
    engine: sqlalchemy.engine.Engine,
) -> tuple[list[placement_contract_normalization._RowWork], dict[str, dict[
        str, Any]]]:
    with orm.Session(engine) as session:
        return placement_contract_normalization._scan_inventory(session,
                                                                row_bound=10)


def _seed_retirement_fixture(
    engine: sqlalchemy.engine.Engine,
    monkeypatch: pytest.MonkeyPatch,
    timestamp_case: str = 'production',
) -> _RetirementFixture:
    """Create a production-shaped inventory and completed protocol-1 proof."""
    # Application transaction tests run against ORM-created tables.  Revision
    # 040's exact SQL authority has its own real-migration suite.
    monkeypatch.setattr(placement_contract_normalization,
                        '_assert_database_write_authority',
                        lambda _connection: 'public')
    if timestamp_case not in {
            'production', 'null_after_boundary', 'no_finite_boundary',
            'legacy_intent_after_boundary', 'finite_intent_after_version'
    }:
        raise ValueError(f'Unknown timestamp case: {timestamp_case}')
    serve_state.Base.metadata.create_all(engine)
    monkeypatch.setattr(sky, '__commit__', 'a' * 40)
    historical_payload = zlib.decompress(
        base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64))
    successor_payload = _explicit_v2_payload()
    version_rows = []
    intent_rows = []
    for version in _COMMITTED_VERSION_IDS:
        yaml_content = _cleanup_yaml(version)
        version_created_at = (None if version in _LEGACY_NULL_VERSION_IDS else
                              100.0 + version)
        intent_created_at = float(version)
        if timestamp_case == 'null_after_boundary' and version == 9:
            version_created_at = None
        elif timestamp_case == 'no_finite_boundary':
            version_created_at = None
        elif (timestamp_case == 'legacy_intent_after_boundary' and
              version == 1):
            intent_created_at = _TIMESTAMP_BOUNDARY_CREATED_AT + 1.0
        elif (timestamp_case == 'finite_intent_after_version' and version == 9):
            assert version_created_at is not None
            intent_created_at = version_created_at + 1.0
        version_rows.append({
            'service_name': _SERVICE_NAME,
            'version': version,
            'spec': (historical_payload
                     if version == _HISTORICAL_VERSION else successor_payload),
            'yaml_content': yaml_content,
            'created_at': version_created_at,
            'created_by': 'test',
        })
        intent_rows.append({
            'service_name': _SERVICE_NAME,
            'resource_scope': _RESOURCE_SCOPE,
            'storage_generation': f'generation-{version:02d}',
            'yaml_content': yaml_content,
            'pool': 0,
            'lifecycle_epoch': 7,
            'provisional': 1,
            'created_at': intent_created_at,
        })

    for version in _PLACEHOLDER_VERSION_IDS:
        version_rows.append({
            'service_name': _SERVICE_NAME,
            'version': version,
            'spec': pickle.dumps(None, protocol=4),
            'yaml_content': None,
            'created_at': None,
            'created_by': 'test-placeholder',
        })
    version_rows.sort(key=lambda row: row['version'])

    with engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name=_SERVICE_NAME,
            workspace='workspace',
            status=serve_state.ServiceStatus.READY.value,
            current_version=_CURRENT_VERSION,
            active_versions=f'[{_CURRENT_VERSION}]',
            pool=0,
            controller_pid=123,
            controller_ip='10.0.0.1',
            hash=_RESOURCE_SCOPE,
            lifecycle_epoch=7,
            resource_scope=_RESOURCE_SCOPE,
            logical_replica_semantics=0))
        connection.execute(serve_state.version_specs_table.insert(),
                           version_rows)
        connection.execute(
            serve_state.ephemeral_storage_cleanup_intents_table.insert(),
            intent_rows)

    # Reproduce the immediately preceding writer identity through the same
    # public operator.  Its unchanged explicit-v2 result rows form the exact
    # immutable current/recovery proof consumed by retirement.
    with monkeypatch.context() as predecessor:
        predecessor.setattr(placement_normalization_identity,
                            'CURRENT_PROTOCOL',
                            placement_normalization_identity.PROTOCOL_V1)
        timestamps = iter((100.0, 101.0))
        run = placement_contract_normalization.run_operator(
            **_normalization_kwargs(engine),
            mode=placement_contract_normalization.ApplyMode.SUPPORTED,
            now=lambda: next(timestamps))
    assert run.run_id is not None
    assert run.changed_rows == 0
    predecessor_run_id = uuid.UUID(run.run_id)

    with engine.begin() as connection:
        connection.execute(serve_state.services_table.update(
        ).where(serve_state.services_table.c.name == _SERVICE_NAME).values(
            placement_normalization_requested_run_id=predecessor_run_id,
            placement_normalization_loaded_run_id=predecessor_run_id,
            placement_normalization_loaded_image_commit=(_PREDECESSOR_COMMIT),
            # Completed receipts are immutable historical evidence; they
            # intentionally need not match today's controller process.
            placement_normalization_loaded_controller_pid=456,
            placement_normalization_loaded_controller_ip='10.0.0.99',
            placement_normalization_loaded_boot_id='c' * 32,
            placement_normalization_loaded_at=102.0,
            # A completed receipt belongs to its immutable run/service
            # incarnation proof, not to today's process or epoch owner.
            lifecycle_epoch=8,
            controller_pid=789,
            controller_ip='10.0.0.2'))

    with engine.connect() as connection:
        manifest_identity = connection.execute(
            sqlalchemy.select(
                serve_state.placement_normalization_runs_table.c.
                normalizer_version).where(
                    serve_state.placement_normalization_runs_table.c.run_id ==
                    predecessor_run_id)).scalar_one()
    assert manifest_identity == f'1:{"a" * 40}'
    return _RetirementFixture(predecessor_run_id, historical_payload,
                              _cleanup_yaml(_HISTORICAL_VERSION),
                              _read_intents(engine))


def _complete_terminal_retirement(
    engine: sqlalchemy.engine.Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[_RetirementFixture,
           placement_contract_normalization.OperatorResult]:
    fixture = _seed_retirement_fixture(engine, monkeypatch)
    timestamps = iter((200.0, 201.0, 202.0))
    result = placement_contract_normalization.run_operator(
        **_retirement_kwargs(engine), now=lambda: next(timestamps))
    assert result.run_id is not None
    return fixture, result


def test_current_reader_accepts_exact_production_protocol_v1_snapshot():
    """The current reader validates the complete secret-free production run."""
    compressed = base64.b64decode(_PRODUCTION_SNAPSHOT_PATH.read_text())
    raw_snapshot = zlib.decompress(compressed)
    assert hashlib.sha256(raw_snapshot).hexdigest() == (
        _PRODUCTION_SNAPSHOT_SHA256)
    snapshot = json.loads(raw_snapshot)
    manifest = snapshot['manifest']
    entries = snapshot['rows']

    # This is an exact export of the two normalization-ledger tables only.
    # Their schema contains hashes, typed projections, and dependency facts;
    # it intentionally has no raw spec, YAML, controller configuration, or
    # task-secret column.  Assert that boundary before trusting the fixture.
    assert set(manifest) == {
        column.name
        for column in serve_state.placement_normalization_runs_table.columns
    }
    expected_entry_columns = {
        column.name
        for column in serve_state.placement_normalization_rows_table.columns
    }
    assert len(entries) == 164
    assert all(set(entry) == expected_entry_columns for entry in entries)
    forbidden_raw_fields = {
        'spec',
        'yaml_content',
        'submitted_yaml_content',
        'retired_yaml_content',
        'controller_config',
    }
    assert all(forbidden_raw_fields.isdisjoint(entry) for entry in entries)

    manifest['run_id'] = uuid.UUID(manifest['run_id'])
    for entry in entries:
        entry['run_id'] = uuid.UUID(entry['run_id'])
    assert manifest['run_id'] == uuid.UUID(
        '3bacd32f-888e-4a1f-af87-8f17dd82f168')
    assert manifest['normalizer_version'] == (
        '1:ccdb295a4a6065fc72f67571e87a395d1e6ec2a1')
    assert manifest['release_version'] == '1.1.1143'
    assert manifest['row_bound'] == 256
    assert manifest['row_count'] == 164
    assert manifest['classification_counts'] == {
        'explicit_v1': 1,
        'fieldless_supported': 155,
        'historical_physical_per_gpu': 1,
        'placeholder': 7,
    }
    assert manifest['freeze_evidence_sha256'] == (
        '7da998baa11b4e0defab15ae9b72987cc7b1862c07f39b3797ec56e40baadba7')
    assert manifest['post_inventory_sha256'] == (
        '288cc2d84d8e884806797640d3457436f864a6bc5e6872674e1c225403b37716')
    assert not placement_normalization_manifest.manifest_mismatches(
        manifest, entries)


@pytest.mark.parametrize(('case', 'match'), [
    ('pending', 'pending or belongs to another run'),
    ('partial', 'invalid controller PID'),
    ('mismatch', 'pending or belongs to another run'),
    ('missing_manifest', 'manifest is missing'),
    ('unapproved', 'unapproved image commit'),
])
def test_postgres_predecessor_receipt_inventory_rejects_blockers(
        empty_postgres, monkeypatch, case, match):
    engine = empty_postgres
    run_id = _seed_predecessor_receipt(engine, monkeypatch)
    alternate_run_id = uuid.UUID('dddddddd-dddd-4ddd-8ddd-dddddddddddd')
    updates: dict[str, Any]
    with engine.begin() as connection:
        if case == 'pending':
            updates = {
                'placement_normalization_loaded_run_id': None,
                'placement_normalization_loaded_image_commit': None,
                'placement_normalization_loaded_controller_pid': None,
                'placement_normalization_loaded_controller_ip': None,
                'placement_normalization_loaded_boot_id': None,
                'placement_normalization_loaded_at': None,
            }
        elif case == 'partial':
            updates = {
                'placement_normalization_loaded_controller_pid': None,
            }
        elif case == 'mismatch':
            manifest = dict(
                connection.execute(
                    sqlalchemy.select(
                        serve_state.placement_normalization_runs_table).where(
                            serve_state.placement_normalization_runs_table.c.
                            run_id == run_id)).mappings().one())
            manifest['run_id'] = alternate_run_id
            connection.execute(
                serve_state.placement_normalization_runs_table.insert().values(
                    **manifest))
            updates = {
                'placement_normalization_loaded_run_id': alternate_run_id,
            }
        elif case == 'missing_manifest':
            # The production schema correctly prevents this corruption.  Drop
            # only the two receipt FKs in this disposable test database to
            # prove the reader still fails closed for a restored/orphaned row.
            connection.exec_driver_sql(
                'ALTER TABLE services DROP CONSTRAINT '
                'fk_services_placement_normalization_requested_run')
            connection.exec_driver_sql(
                'ALTER TABLE services DROP CONSTRAINT '
                'fk_services_placement_normalization_loaded_run')
            updates = {
                'placement_normalization_requested_run_id': alternate_run_id,
                'placement_normalization_loaded_run_id': alternate_run_id,
            }
        else:
            assert case == 'unapproved'
            updates = {
                'placement_normalization_loaded_image_commit': 'd' * 40,
            }
        connection.execute(serve_state.services_table.update().where(
            serve_state.services_table.c.name == 'svc-receipt-blocker').values(
                **updates))

    rows, service_rows = _read_normalization_inventory(engine)
    with orm.Session(engine) as session:
        with pytest.raises(
                placement_contract_normalization.NormalizationBlocker,
                match=match):
            placement_contract_normalization._predecessor_receipt_evidence(
                session,
                rows,
                service_rows,
                frozenset({'b' * 40}),
                row_bound=10,
                freeze_evidence_sha256='f' * 64)


def test_postgres_predecessor_receipt_inventory_rejects_bound_overflow(
        empty_postgres, monkeypatch):
    engine = empty_postgres
    run_id = _seed_predecessor_receipt(engine, monkeypatch)
    with engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name='svc-receipt-overflow',
            workspace='workspace',
            status=serve_state.ServiceStatus.READY.value,
            current_version=2,
            active_versions='[2]',
            pool=0,
            controller_pid=789,
            controller_ip='10.0.0.2',
            hash='overflow-hash',
            lifecycle_epoch=1,
            resource_scope='overflow-hash',
            logical_replica_semantics=0,
            placement_normalization_requested_run_id=run_id))
    rows, service_rows = _read_normalization_inventory(engine)
    with orm.Session(engine) as session:
        with pytest.raises(
                placement_contract_normalization.NormalizationBlocker,
                match='inventory exceeds its explicit row bound'):
            placement_contract_normalization._predecessor_receipt_evidence(
                session,
                rows,
                service_rows,
                frozenset({'b' * 40}),
                row_bound=1,
                freeze_evidence_sha256='f' * 64)


def _assert_database_unchanged_after_failed_retirement(
    engine: sqlalchemy.engine.Engine,
    fixture: _RetirementFixture,
) -> None:
    assert _read_intents(engine) == fixture.intent_preimages
    versions = serve_state.version_specs_table
    runs = serve_state.placement_normalization_runs_table
    services = serve_state.services_table
    with engine.connect() as connection:
        candidate = connection.execute(
            sqlalchemy.select(versions).where(
                versions.c.service_name == _SERVICE_NAME,
                versions.c.version == _HISTORICAL_VERSION)).mappings().one()
        manifest_ids = connection.execute(
            sqlalchemy.select(runs.c.run_id).order_by(
                runs.c.started_at)).scalars().all()
        receipt = connection.execute(
            sqlalchemy.select(
                services.c.placement_normalization_requested_run_id,
                services.c.placement_normalization_loaded_run_id,
                services.c.placement_normalization_loaded_image_commit,
            ).where(services.c.name == _SERVICE_NAME)).one()
    assert bytes(candidate['spec']) == fixture.historical_payload
    assert candidate['yaml_content'] == fixture.historical_yaml
    assert candidate['retired_yaml_content'] is None
    assert candidate['retired_at'] is None
    assert candidate['retirement_reason'] is None
    assert candidate['retirement_run_id'] is None
    assert manifest_ids == [fixture.predecessor_run_id]
    assert tuple(receipt) == (fixture.predecessor_run_id,
                              fixture.predecessor_run_id, _PREDECESSOR_COMMIT)


def test_postgres_protocol_v4_retires_production_shaped_cleanup_inventory(
        empty_postgres, monkeypatch):
    engine = empty_postgres
    fixture = _seed_retirement_fixture(engine, monkeypatch)

    timestamps = iter((200.0, 201.0, 202.0))
    result = placement_contract_normalization.run_operator(
        **_retirement_kwargs(engine), now=lambda: next(timestamps))

    assert result.retired_rows == 1
    assert result.changed_rows == 0
    assert result.run_id is not None
    run_id = uuid.UUID(result.run_id)
    intents_after = _read_intents(engine)
    assert len(intents_after) == _COMMITTED_VERSION_COUNT
    for before, after in zip(fixture.intent_preimages, intents_after):
        assert before['provisional'] == 1
        assert after['provisional'] == 0
        assert {
            key: value for key, value in before.items() if key != 'provisional'
        } == {
            key: value for key, value in after.items() if key != 'provisional'
        }

    versions = serve_state.version_specs_table
    runs = serve_state.placement_normalization_runs_table
    ledger = serve_state.placement_normalization_rows_table
    with engine.connect() as connection:
        candidate = dict(
            connection.execute(
                sqlalchemy.select(versions).where(
                    versions.c.service_name == _SERVICE_NAME, versions.c.version
                    == _HISTORICAL_VERSION)).mappings().one())
        manifest = dict(
            connection.execute(
                sqlalchemy.select(runs).where(
                    runs.c.run_id == run_id)).mappings().one())
        entries = [
            dict(row) for row in connection.execute(
                sqlalchemy.select(ledger).
                where(ledger.c.run_id == run_id).order_by(
                    ledger.c.service_name, ledger.c.version)).mappings().all()
        ]
        placeholder_rows = {
            int(row['version']): dict(row) for row in connection.execute(
                sqlalchemy.select(versions).where(
                    versions.c.service_name == _SERVICE_NAME,
                    versions.c.version.in_(_PLACEHOLDER_VERSION_IDS)).order_by(
                        versions.c.version)).mappings().all()
        }

    assert bytes(candidate['spec']) == pickle.dumps(None, protocol=4)
    assert candidate['yaml_content'] is None
    assert candidate['retired_yaml_content'] == fixture.historical_yaml
    assert candidate['retired_at'] == 201.0
    assert candidate['retirement_run_id'] == run_id
    assert manifest['normalizer_version'] == f'4:{"a" * 40}'
    assert manifest['row_count'] == _TOTAL_VERSION_COUNT
    assert manifest['classification_counts'] == {
        'explicit_v2': _COMMITTED_VERSION_COUNT - 1,
        'historical_physical_per_gpu': 1,
        'stale_placeholder': len(_PLACEHOLDER_VERSION_IDS),
    }
    assert not placement_normalization_manifest.manifest_mismatches(
        manifest, entries)
    candidate_ledger = next(
        entry for entry in entries if entry['version'] == _HISTORICAL_VERSION)
    assert placement_normalization_manifest._retirement_ledger_facts_are_complete(
        candidate_ledger, protocol=4)
    facts = candidate_ledger['dependency_facts']
    assert facts['cleanup_intent_inventory_count'] == _COMMITTED_VERSION_COUNT
    assert facts['cleanup_intent_adopted_count'] == _COMMITTED_VERSION_COUNT
    assert facts['cleanup_match_inventory_count'] == _COMMITTED_VERSION_COUNT
    assert facts['cleanup_candidate_match_count'] == 1
    assert facts['cleanup_candidate_deletion_target_count'] == 0
    assert facts['cleanup_intent_deletion_target_count'] == 0
    assert facts['cleanup_version_timestamp_service_count'] == 1
    assert facts['cleanup_version_timestamp_inventory_count'] == (
        _COMMITTED_VERSION_COUNT)
    assert facts['cleanup_version_timestamp_matched_intent_count'] == (
        _COMMITTED_VERSION_COUNT)
    assert facts['cleanup_legacy_null_version_timestamp_count'] == (
        _LEGACY_NULL_VERSION_COUNT)
    assert facts['cleanup_timestamp_boundary_count'] == 1
    timestamp_inventory_sha256 = facts[
        'cleanup_version_timestamp_inventory_sha256']
    expected_timestamp_inventory_sha256 = _canonical_json_sha256(
        _expected_timestamp_inventory())
    assert timestamp_inventory_sha256 == expected_timestamp_inventory_sha256
    assert facts['cleanup_candidate_version_created_at_mode'] == (
        'legacy_prefix_null')
    assert facts['cleanup_candidate_legacy_timestamp_boundary_version'] == (
        _TIMESTAMP_BOUNDARY_VERSION)
    timestamp_proof_sha256 = facts['cleanup_timestamp_proof_sha256']
    assert timestamp_proof_sha256 == _canonical_json_sha256({
        'cleanup_contract_schema': 'skyserve-ephemeral-storage-retirement-v3',
        'cleanup_version_timestamp_service_count': 1,
        'cleanup_version_timestamp_inventory_count': _COMMITTED_VERSION_COUNT,
        'cleanup_version_timestamp_matched_intent_count': _COMMITTED_VERSION_COUNT,
        'cleanup_legacy_null_version_timestamp_count': _LEGACY_NULL_VERSION_COUNT,
        'cleanup_timestamp_boundary_count': 1,
        'cleanup_version_timestamp_inventory_sha256': expected_timestamp_inventory_sha256,
        'cleanup_candidate_service_name_sha256': hashlib.sha256(
            _SERVICE_NAME.encode()).hexdigest(),
        'cleanup_candidate_version': _HISTORICAL_VERSION,
        'cleanup_candidate_version_created_at_mode': 'legacy_prefix_null',
        'cleanup_candidate_legacy_timestamp_boundary_version': _TIMESTAMP_BOUNDARY_VERSION,
        'cleanup_intent_key_sha256': _canonical_json_sha256(
            [_SERVICE_NAME, _RESOURCE_SCOPE, 'generation-02']),
    })
    assert timestamp_proof_sha256 != timestamp_inventory_sha256
    assert facts['predecessor_receipt_inventory_count'] == 1
    assert facts['approved_loaded_image_commit_count'] == 1
    assert 'same_service_placeholder_dependency_absent' not in facts

    service_name_sha256 = hashlib.sha256(_SERVICE_NAME.encode()).hexdigest()
    stale_entries = [
        entry for entry in entries
        if entry['classification'] == 'stale_placeholder'
    ]
    assert [entry['version'] for entry in stale_entries
           ] == list(_PLACEHOLDER_VERSION_IDS)
    expected_stale_evidence = []
    expected_stale_keys = {
        'schema',
        'service_name_sha256',
        'version',
        'original_row_sha256',
        'strictly_newer_committed_version',
        'image_demand_count',
        'image_demand_sha256',
        'resource_action_root_count',
        'resource_action_root_sha256',
        'state_clean',
        'fill_stale_proved',
    }
    for entry in stale_entries:
        version = entry['version']
        evidence = entry['dependency_facts']['stale_placeholder_evidence']
        assert set(evidence) == expected_stale_keys
        assert evidence == {
            'schema': _STALE_PLACEHOLDER_SCHEMA,
            'service_name_sha256': service_name_sha256,
            'version': version,
            'original_row_sha256': entry['original_row_sha256'],
            'strictly_newer_committed_version': _CURRENT_VERSION,
            'image_demand_count': 0,
            'image_demand_sha256': '0' * 64,
            'resource_action_root_count': 0,
            'resource_action_root_sha256': '0' * 64,
            'state_clean': True,
            'fill_stale_proved': True,
        }
        assert entry['outcome'] == 'unchanged'
        assert entry['original_row_sha256'] == entry['result_row_sha256']
        assert (
            entry['original_column_sha256s'] == entry['result_column_sha256s'])
        row = placeholder_rows[version]
        assert bytes(row['spec']) == pickle.dumps(None, protocol=4)
        assert all(row[column] is None for column in _PLACEHOLDER_NULL_COLUMNS)
        expected_stale_evidence.append(evidence)

    placeholder_inventory = {
        'schema': _STALE_PLACEHOLDER_SCHEMA,
        'service_name_sha256': service_name_sha256,
        'current_version': _CURRENT_VERSION,
        'placeholders': expected_stale_evidence,
    }
    assert facts['same_service_stale_placeholder_proof'] == {
        'schema': _STALE_PLACEHOLDER_SCHEMA,
        'service_name_sha256': service_name_sha256,
        'current_version': _CURRENT_VERSION,
        'placeholder_count': len(_PLACEHOLDER_VERSION_IDS),
        'image_demand_count': 0,
        'resource_action_root_count': 0,
        'inventory_sha256': _canonical_json_sha256(placeholder_inventory),
        'fill_stale_proved': True,
    }


@pytest.mark.parametrize('case', [
    'trailing_fillable',
    'current',
    'active',
    'submitted_yaml',
    'placement_catalog',
    'controller_config',
    'controller_applied',
    'quarantined',
    'replica',
    'protocol_5_none',
    'noncanonical_none',
])
def test_postgres_protocol_v4_rejects_unsafe_placeholder_state(
        empty_postgres, monkeypatch, case):
    engine = empty_postgres
    fixture = _seed_retirement_fixture(engine, monkeypatch)
    versions = serve_state.version_specs_table
    services = serve_state.services_table
    replicas = serve_state.replicas_table
    with engine.begin() as connection:
        placeholder = sqlalchemy.and_(
            versions.c.service_name == _SERVICE_NAME,
            versions.c.version == _PLACEHOLDER_VERSION_IDS[0])
        if case == 'trailing_fillable':
            connection.execute(versions.insert().values(
                service_name=_SERVICE_NAME,
                version=_CURRENT_VERSION + 1,
                spec=pickle.dumps(None, protocol=4),
                yaml_content=None,
                created_by='racing-writer'))
        elif case == 'current':
            connection.execute(services.update().where(
                services.c.name == _SERVICE_NAME).values(
                    current_version=_PLACEHOLDER_VERSION_IDS[0]))
        elif case == 'active':
            connection.execute(services.update().where(
                services.c.name == _SERVICE_NAME).values(
                    active_versions=json.dumps(
                        [_PLACEHOLDER_VERSION_IDS[0], _CURRENT_VERSION])))
        elif case == 'submitted_yaml':
            connection.execute(versions.update().where(placeholder).values(
                submitted_yaml_content='service: {}'))
        elif case == 'placement_catalog':
            connection.execute(versions.update().where(placeholder).values(
                placement_catalog={}))
        elif case == 'controller_config':
            config = b'{}'
            connection.execute(versions.update().where(placeholder).values(
                controller_config=config,
                controller_config_digest=hashlib.sha256(config).hexdigest(),
                controller_config_snapshot_id='d' * 64))
        elif case == 'controller_applied':
            connection.execute(versions.update().where(placeholder).values(
                controller_applied_at=123.0))
        elif case == 'quarantined':
            connection.execute(versions.update().where(placeholder).values(
                quarantined_at=123.0, quarantine_reason='test quarantine'))
        elif case == 'replica':
            connection.execute(replicas.insert().values(
                service_name=_SERVICE_NAME,
                replica_id=999,
                version=_PLACEHOLDER_VERSION_IDS[0],
                status='READY'))
        elif case == 'protocol_5_none':
            connection.execute(versions.update().where(placeholder).values(
                spec=pickle.dumps(None, protocol=5)))
        else:
            assert case == 'noncanonical_none'
            noncanonical_none = b'\x80\x04N0N.'
            assert pickle.loads(noncanonical_none) is None
            assert noncanonical_none != pickle.dumps(None, protocol=4)
            connection.execute(versions.update().where(placeholder).values(
                spec=noncanonical_none))

    timestamps = iter((200.0, 201.0, 202.0))
    with pytest.raises(placement_contract_normalization.NormalizationBlocker):
        placement_contract_normalization.run_operator(
            **_retirement_kwargs(engine), now=lambda: next(timestamps))

    _assert_database_unchanged_after_failed_retirement(engine, fixture)


@pytest.mark.parametrize(('evidence_kind', 'checkpoint'), [
    ('image', 2),
    ('image', 3),
    ('resource_action', 2),
    ('resource_action', 3),
])
def test_postgres_protocol_v4_rejects_zero_count_evidence_drift(
        empty_postgres, monkeypatch, evidence_kind, checkpoint):
    engine = empty_postgres
    fixture = _seed_retirement_fixture(engine, monkeypatch)
    target = (_SERVICE_NAME, _PLACEHOLDER_VERSION_IDS[0])
    kwargs = _retirement_kwargs(engine)
    if evidence_kind == 'image':
        calls: dict[tuple[str, int], int] = {}

        def image_evidence(service_name, version):
            identity = (service_name, version)
            calls[identity] = calls.get(identity, 0) + 1
            digest = ('1' * 64 if identity == target and
                      calls[identity] == checkpoint else '0' * 64)
            return placement_contract_normalization._ExternalEvidence(
                count=0, digest=digest)

        kwargs['image_evidence_getter'] = image_evidence
    else:
        calls = {'count': 0}

        def resource_action_evidence(_engine, targets):
            calls['count'] += 1
            return {
                (item.service_name, item.version):
                    placement_contract_normalization._ExternalEvidence(
                        count=0,
                        digest=('1' * 64 if
                                (item.service_name, item.version) == target and
                                calls['count'] == checkpoint else '0' * 64))
                for item in targets
            }

        kwargs['resource_action_evidence_getter'] = resource_action_evidence

    timestamps = iter((200.0, 201.0, 202.0))
    with pytest.raises(placement_contract_normalization.NormalizationBlocker):
        placement_contract_normalization.run_operator(
            **kwargs, now=lambda: next(timestamps))

    _assert_database_unchanged_after_failed_retirement(engine, fixture)


@pytest.mark.parametrize('evidence_kind', ['image', 'resource_action'])
def test_postgres_protocol_v4_rejects_placeholder_external_dependency(
        empty_postgres, monkeypatch, evidence_kind):
    engine = empty_postgres
    fixture = _seed_retirement_fixture(engine, monkeypatch)
    target = (_SERVICE_NAME, _PLACEHOLDER_VERSION_IDS[0])
    kwargs = _retirement_kwargs(engine)
    if evidence_kind == 'image':

        def image_evidence(service_name, version):
            return placement_contract_normalization._ExternalEvidence(
                count=int((service_name, version) == target), digest='0' * 64)

        kwargs['image_evidence_getter'] = image_evidence
    else:

        def resource_action_evidence(_engine, targets):
            return {
                (item.service_name, item.version):
                    placement_contract_normalization._ExternalEvidence(
                        count=int((item.service_name, item.version) == target),
                        digest='0' * 64) for item in targets
            }

        kwargs['resource_action_evidence_getter'] = resource_action_evidence

    timestamps = iter((200.0, 201.0, 202.0))
    with pytest.raises(placement_contract_normalization.NormalizationBlocker):
        placement_contract_normalization.run_operator(
            **kwargs, now=lambda: next(timestamps))

    _assert_database_unchanged_after_failed_retirement(engine, fixture)


def test_postgres_protocol_v4_rejects_resource_action_target_map_drift(
        empty_postgres, monkeypatch):
    engine = empty_postgres
    fixture = _seed_retirement_fixture(engine, monkeypatch)
    calls = {'count': 0}
    omitted = (_SERVICE_NAME, _PLACEHOLDER_VERSION_IDS[0])

    def resource_action_evidence(_engine, targets):
        calls['count'] += 1
        evidence = {
            (item.service_name, item.version): _zero_evidence()
            for item in targets
        }
        if calls['count'] == 2:
            evidence.pop(omitted)
        return evidence

    kwargs = _retirement_kwargs(engine)
    kwargs['resource_action_evidence_getter'] = resource_action_evidence
    timestamps = iter((200.0, 201.0, 202.0))
    with pytest.raises(placement_contract_normalization.NormalizationBlocker):
        placement_contract_normalization.run_operator(
            **kwargs, now=lambda: next(timestamps))

    _assert_database_unchanged_after_failed_retirement(engine, fixture)


def test_writer_session_lock_fails_busy_instead_of_waiting(empty_postgres):
    engine = empty_postgres
    holder = engine.connect()
    contender = engine.connect()
    try:
        with holder.begin():
            holder.execute(
                sqlalchemy.text(
                    'SELECT pg_advisory_xact_lock('
                    'hashtextextended(:name, 0))'), {
                        'name': (placement_contract_normalization.
                                 _ADVISORY_LOCK_NAME),
                    })
            with pytest.raises(
                    placement_contract_normalization.NormalizationBlocker,
                    match='owns the advisory authority'):
                placement_contract_normalization._acquire_writer_session_lock(
                    contender)
    finally:
        contender.close()
        holder.close()


def test_writer_database_authority_rejects_absent_revision_040(empty_postgres):
    connection = empty_postgres.connect()
    try:
        with pytest.raises(
                placement_contract_normalization.NormalizationBlocker,
                match='write authority is absent or invalid'):
            placement_contract_normalization._acquire_writer_session_lock(
                connection)
    finally:
        connection.close()


def test_writer_database_authority_rejects_temporary_relation_shadow(
        empty_postgres):
    connection = empty_postgres.connect()
    try:
        with connection.begin():
            connection.exec_driver_sql(
                'CREATE TEMPORARY TABLE services (spoof integer)')
        with pytest.raises(
                placement_contract_normalization.NormalizationBlocker,
                match='temporary authority-relation shadow'):
            placement_contract_normalization._acquire_writer_session_lock(
                connection)
    finally:
        connection.close()


@pytest.mark.parametrize('race', ['add', 'fill'])
def test_postgres_protocol_v4_rejects_placeholder_inventory_race(
        empty_postgres, monkeypatch, race):
    engine = empty_postgres
    fixture = _seed_retirement_fixture(engine, monkeypatch)
    calls = {'count': 0}
    versions = serve_state.version_specs_table

    def process_evidence(_targets, _pod_uid):
        calls['count'] += 1
        if calls['count'] == 1:
            with engine.begin() as connection:
                if race == 'add':
                    connection.execute(versions.insert().values(
                        service_name=_SERVICE_NAME,
                        version=_CURRENT_VERSION + 1,
                        spec=pickle.dumps(None, protocol=4),
                        yaml_content=None,
                        created_by='concurrent-writer'))
                else:
                    connection.execute(versions.update().where(
                        versions.c.service_name == _SERVICE_NAME,
                        versions.c.version ==
                        _PLACEHOLDER_VERSION_IDS[0]).values(
                            spec=_explicit_v2_payload(),
                            yaml_content='service: {}',
                            created_at=199.0))
        return _zero_evidence()

    kwargs = _retirement_kwargs(engine)
    kwargs['process_evidence_getter'] = process_evidence
    timestamps = iter((200.0, 201.0, 202.0))
    with pytest.raises(placement_contract_normalization.NormalizationBlocker):
        placement_contract_normalization.run_operator(
            **kwargs, now=lambda: next(timestamps))

    _assert_database_unchanged_after_failed_retirement(engine, fixture)


def test_protocol_v4_full_manifest_rejects_stale_placeholder_fact_tampering(
        empty_postgres, monkeypatch):
    engine = empty_postgres
    _seed_retirement_fixture(engine, monkeypatch)
    timestamps = iter((200.0, 201.0, 202.0))
    result = placement_contract_normalization.run_operator(
        **_retirement_kwargs(engine), now=lambda: next(timestamps))
    assert result.run_id is not None
    run_id = uuid.UUID(result.run_id)
    runs = serve_state.placement_normalization_runs_table
    ledger = serve_state.placement_normalization_rows_table
    with engine.connect() as connection:
        manifest = dict(
            connection.execute(
                sqlalchemy.select(runs).where(
                    runs.c.run_id == run_id)).mappings().one())
        entries = [
            dict(row) for row in connection.execute(
                sqlalchemy.select(ledger).
                where(ledger.c.run_id == run_id).order_by(
                    ledger.c.service_name, ledger.c.version)).mappings().all()
        ]
    assert not placement_normalization_manifest.manifest_mismatches(
        manifest, entries)

    def entry(rows, version):
        return next(row for row in rows
                    if row['service_name'] == _SERVICE_NAME and
                    row['version'] == version)

    def stale_evidence(rows, version=3):
        return entry(rows,
                     version)['dependency_facts']['stale_placeholder_evidence']

    def candidate_proof(rows):
        return entry(
            rows, _HISTORICAL_VERSION
        )['dependency_facts']['same_service_stale_placeholder_proof']

    def remove_stale_evidence(_manifest, rows):
        entry(rows, 3)['dependency_facts'].pop('stale_placeholder_evidence')

    def add_stale_evidence_key(_manifest, rows):
        stale_evidence(rows)['extra'] = None

    def substitute_stale_version(_manifest, rows):
        stale_evidence(rows)['version'] = 10

    def substitute_stale_successor(_manifest, rows):
        stale_evidence(rows)['strictly_newer_committed_version'] = 50

    def substitute_stale_digest(_manifest, rows):
        stale_evidence(rows)['image_demand_sha256'] = 'd' * 64

    def substitute_stale_count(_manifest, rows):
        stale_evidence(rows)['resource_action_root_count'] = 1

    def substitute_stale_boolean(_manifest, rows):
        stale_evidence(rows)['state_clean'] = False

    def substitute_stale_row_hash(_manifest, rows):
        stale_evidence(rows)['original_row_sha256'] = 'd' * 64

    def substitute_column_hash(_manifest, rows):
        entry(rows,
              3)['original_column_sha256s']['submitted_yaml_content'] = 'd' * 64

    def swap_stale_evidence(_manifest, rows):
        first = entry(rows, 3)['dependency_facts']
        second = entry(rows, 10)['dependency_facts']
        first['stale_placeholder_evidence'], second[
            'stale_placeholder_evidence'] = (
                second['stale_placeholder_evidence'],
                first['stale_placeholder_evidence'])

    def demote_stale_classification(_manifest, rows):
        entry(rows, 3)['classification'] = 'placeholder'

    def coordinated_relabel(tampered_manifest, rows, classification):
        relabeled = entry(rows, 3)
        relabeled['classification'] = classification
        relabeled['dependency_facts'].pop('stale_placeholder_evidence')
        remaining_evidence = [stale_evidence(rows, version=10)]
        proof = candidate_proof(rows)
        proof['placeholder_count'] = len(remaining_evidence)
        proof['inventory_sha256'] = (
            placement_normalization_manifest.
            stale_placeholder_inventory_sha256(_SERVICE_NAME,
                                                proof['current_version'],
                                                remaining_evidence))
        counts = tampered_manifest['classification_counts']
        counts['stale_placeholder'] -= 1
        counts[classification] = counts.get(classification, 0) + 1

    def relabel_stale_as_explicit_v2(tampered_manifest, rows):
        coordinated_relabel(tampered_manifest, rows, 'explicit_v2')

    def relabel_stale_as_retired(tampered_manifest, rows):
        coordinated_relabel(tampered_manifest, rows, 'retired')

    def remove_candidate_proof(_manifest, rows):
        entry(rows, _HISTORICAL_VERSION)['dependency_facts'].pop(
            'same_service_stale_placeholder_proof')

    def add_candidate_proof_key(_manifest, rows):
        candidate_proof(rows)['extra'] = None

    def add_legacy_placeholder_absence_fact(_manifest, rows):
        entry(rows, _HISTORICAL_VERSION)['dependency_facts'][
            'same_service_placeholder_dependency_absent'] = True

    def substitute_candidate_count(_manifest, rows):
        candidate_proof(rows)['placeholder_count'] = 1

    def substitute_candidate_current(_manifest, rows):
        candidate_proof(rows)['current_version'] = 50

    def substitute_inventory_digest(_manifest, rows):
        candidate_proof(rows)['inventory_sha256'] = 'd' * 64

    def relabel_as_protocol_3(tampered_manifest, _rows):
        tampered_manifest['normalizer_version'] = f'3:{"a" * 40}'

    tamper_cases = {
        'missing stale evidence': remove_stale_evidence,
        'extra stale evidence key': add_stale_evidence_key,
        'stale version substitution': substitute_stale_version,
        'stale successor substitution': substitute_stale_successor,
        'stale digest substitution': substitute_stale_digest,
        'stale count substitution': substitute_stale_count,
        'stale boolean substitution': substitute_stale_boolean,
        'stale row hash substitution': substitute_stale_row_hash,
        'column hash substitution': substitute_column_hash,
        'swapped stale evidence': swap_stale_evidence,
        'stale classification demotion': demote_stale_classification,
        'coordinated stale explicit-v2 relabel': relabel_stale_as_explicit_v2,
        'coordinated stale retired relabel': relabel_stale_as_retired,
        'missing candidate proof': remove_candidate_proof,
        'extra candidate proof key': add_candidate_proof_key,
        'legacy placeholder absence fact': add_legacy_placeholder_absence_fact,
        'candidate count substitution': substitute_candidate_count,
        'candidate current substitution': substitute_candidate_current,
        'candidate inventory substitution': substitute_inventory_digest,
        'v4 relabeled as v3': relabel_as_protocol_3,
    }
    for label, tamper in tamper_cases.items():
        tampered_manifest = copy.deepcopy(manifest)
        tampered_entries = copy.deepcopy(entries)
        tamper(tampered_manifest, tampered_entries)
        assert placement_normalization_manifest.manifest_mismatches(
            tampered_manifest, tampered_entries), label


@pytest.mark.parametrize('mode', [
    placement_contract_normalization.ApplyMode.SUPPORTED,
    placement_contract_normalization.ApplyMode.RETIRE_TERMINAL_HISTORICAL,
])
def test_protocol_v4_terminal_manifest_fences_writers_before_external_evidence(
        empty_postgres, monkeypatch, mode):
    engine = empty_postgres
    _complete_terminal_retirement(engine, monkeypatch)
    external_calls: list[str] = []

    def unexpected_external_call(*_args, **_kwargs):
        external_calls.append('called')
        raise AssertionError('terminal fence must precede external evidence')

    kwargs = {
        'engine': engine,
        'mode': mode,
        'row_bound': _ROW_BOUND,
        'freeze_evidence_sha256': 'e' * 64,
        'image_evidence_getter': unexpected_external_call,
        'request_evidence_getter': unexpected_external_call,
        'process_evidence_getter': unexpected_external_call,
        'resource_action_evidence_getter': unexpected_external_call,
        'api_pod_checker': unexpected_external_call,
        'controller_hold_checker': unexpected_external_call,
        'consolidation_mode_checker': unexpected_external_call,
        'legacy_controller_evidence_getter': unexpected_external_call,
    }
    if mode is (placement_contract_normalization.ApplyMode.
                RETIRE_TERMINAL_HISTORICAL):
        kwargs['approved_loaded_image_commits'] = (_PREDECESSOR_COMMIT,)

    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match='terminal'):
        placement_contract_normalization.run_operator(**kwargs)
    assert external_calls == []


def test_protocol_v4_serializes_two_first_retirement_writers(
        empty_postgres, monkeypatch):
    engine = empty_postgres
    fixture = _seed_retirement_fixture(engine, monkeypatch)
    first_consolidation = threading.Barrier(2)
    calls_by_thread: dict[int, int] = {}
    calls_lock = threading.Lock()

    def consolidation_mode_checker():
        thread_id = threading.get_ident()
        with calls_lock:
            call_number = calls_by_thread.get(thread_id, 0)
            calls_by_thread[thread_id] = call_number + 1
        if call_number == 0:
            first_consolidation.wait(timeout=10)
        return True

    def invoke_writer():
        timestamps = iter((200.0, 201.0, 202.0))
        kwargs = _retirement_kwargs(engine)
        kwargs['consolidation_mode_checker'] = consolidation_mode_checker
        try:
            return placement_contract_normalization.run_operator(
                **kwargs, now=lambda: next(timestamps))
        except Exception as exc:  # Returned so both futures always join.
            return exc

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _index: invoke_writer(), range(2)))

    successes = [
        outcome for outcome in outcomes
        if isinstance(outcome, placement_contract_normalization.OperatorResult)
    ]
    failures = [outcome for outcome in outcomes if isinstance(outcome, Exception)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0],
                      placement_contract_normalization.NormalizationBlocker)
    assert 'terminal' in str(failures[0]).lower()
    with engine.connect() as connection:
        run_ids = connection.execute(
            sqlalchemy.select(
                serve_state.placement_normalization_runs_table.c.run_id).
            order_by(serve_state.placement_normalization_runs_table.c.
                     completed_at)).scalars().all()
    assert run_ids == [fixture.predecessor_run_id,
                       uuid.UUID(successes[0].run_id)]


def test_protocol_v4_terminal_binding_ignores_epoch_but_not_spec_drift(
        empty_postgres, monkeypatch):
    engine = empty_postgres
    _complete_terminal_retirement(engine, monkeypatch)
    with engine.begin() as connection:
        connection.execute(serve_state.services_table.update().where(
            serve_state.services_table.c.name == _SERVICE_NAME).values(
                lifecycle_epoch=9))

    assert placement_contract_normalization.run_operator(
        engine=engine, mode=None,
        row_bound=_ROW_BOUND).prior_ledger_mismatches == ()

    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.update().where(
            serve_state.version_specs_table.c.service_name == _SERVICE_NAME,
            serve_state.version_specs_table.c.version == _CURRENT_VERSION).
                           values(spec=_explicit_v2_payload(
                               'dynamic_fallback')))
    mismatches = placement_contract_normalization.run_operator(
        engine=engine, mode=None,
        row_bound=_ROW_BOUND).prior_ledger_mismatches
    assert any(mismatch['reason'] == 'tracked_terminal_spec_drift'
               for mismatch in mismatches)


def test_protocol_v4_terminal_binding_requires_manifested_current_row(
        empty_postgres, monkeypatch):
    engine = empty_postgres
    _complete_terminal_retirement(engine, monkeypatch)
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.delete().where(
            serve_state.version_specs_table.c.service_name == _SERVICE_NAME,
            serve_state.version_specs_table.c.version == _CURRENT_VERSION))

    mismatches = placement_contract_normalization.run_operator(
        engine=engine, mode=None,
        row_bound=_ROW_BOUND).prior_ledger_mismatches

    assert any(mismatch['reason'] == 'manifested_current_row_missing'
               for mismatch in mismatches)


@pytest.mark.parametrize('service_name', [
    _SERVICE_NAME,
    'svc-receipt-blocker',
], ids=['retired-candidate', 'ordinary-manifest-service'])
@pytest.mark.parametrize('hash_value', [None, 'malformed hash'],
                         ids=['null', 'whitespace'])
def test_protocol_v4_terminal_binding_rejects_invalid_present_parent_hash(
        empty_postgres, monkeypatch, service_name, hash_value):
    engine = empty_postgres
    _complete_terminal_retirement(engine, monkeypatch)
    with engine.begin() as connection:
        if service_name == 'svc-receipt-blocker':
            connection.execute(serve_state.services_table.insert().values(
                name=service_name,
                workspace='workspace',
                status=serve_state.ServiceStatus.READY.value,
                current_version=1,
                active_versions='[1]',
                pool=0,
                hash=hash_value,
                lifecycle_epoch=1,
                resource_scope='ordinary-service',
                logical_replica_semantics=0))
            connection.execute(
                serve_state.version_specs_table.insert().values(
                    service_name=service_name,
                    version=1,
                    spec=_explicit_v2_payload(),
                    yaml_content='service: {}',
                    created_at=203.0,
                    created_by='post-terminal-test'))
        else:
            connection.execute(serve_state.services_table.update().where(
                serve_state.services_table.c.name == service_name).values(
                    hash=hash_value))

    mismatches = placement_contract_normalization.run_operator(
        engine=engine, mode=None,
        row_bound=_ROW_BOUND).prior_ledger_mismatches

    assert any(
        mismatch['reason'] == 'invalid_current_parent_hash_observation' and
        mismatch['service_name'] == service_name for mismatch in mismatches)


@pytest.mark.parametrize('mutation', ['metadata', 'delete'])
def test_protocol_v4_terminal_binding_requires_retired_tombstone(
        empty_postgres, monkeypatch, mutation):
    engine = empty_postgres
    _complete_terminal_retirement(engine, monkeypatch)
    candidate = sqlalchemy.and_(
        serve_state.version_specs_table.c.service_name == _SERVICE_NAME,
        serve_state.version_specs_table.c.version == _HISTORICAL_VERSION)
    with engine.begin() as connection:
        if mutation == 'metadata':
            connection.execute(serve_state.version_specs_table.update().where(
                candidate).values(retired_yaml_content='tampered: true'))
        else:
            connection.execute(
                serve_state.version_specs_table.delete().where(candidate))

    mismatches = placement_contract_normalization.run_operator(
        engine=engine, mode=None,
        row_bound=_ROW_BOUND).prior_ledger_mismatches
    assert mismatches
    assert any('retired' in str(mismatch['reason']) or
               'tombstone' in str(mismatch['reason'])
               for mismatch in mismatches)


def test_protocol_v4_terminal_binding_never_allows_stale_placeholder_fill(
        empty_postgres, monkeypatch):
    engine = empty_postgres
    _complete_terminal_retirement(engine, monkeypatch)
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.update().where(
            serve_state.version_specs_table.c.service_name == _SERVICE_NAME,
            serve_state.version_specs_table.c.version ==
            _PLACEHOLDER_VERSION_IDS[0]).values(
                spec=_explicit_v2_payload(),
                yaml_content='service: {}',
                created_at=300.0))

    mismatches = placement_contract_normalization.run_operator(
        engine=engine, mode=None,
        row_bound=_ROW_BOUND).prior_ledger_mismatches
    assert mismatches
    assert any('stale' in str(mismatch['reason'])
               for mismatch in mismatches)


def test_protocol_v4_terminal_binding_allows_ordinary_manifest_placeholder_fill(
        empty_postgres, monkeypatch):
    engine = empty_postgres
    serve_state.Base.metadata.create_all(engine)
    ordinary_name = 'ordinary-placeholder-service'
    with engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name=ordinary_name,
            workspace='workspace',
            status=serve_state.ServiceStatus.READY.value,
            current_version=2,
            active_versions='[2]',
            pool=0,
            hash='ordinary-incarnation',
            lifecycle_epoch=1,
            resource_scope='ordinary-incarnation',
            logical_replica_semantics=0))
        connection.execute(serve_state.version_specs_table.insert(), [{
            'service_name': ordinary_name,
            'version': 1,
            'spec': pickle.dumps(None, protocol=4),
            'yaml_content': None,
            'created_at': None,
            'created_by': 'ordinary-placeholder-test',
        }, {
            'service_name': ordinary_name,
            'version': 2,
            'spec': _explicit_v2_payload(),
            'yaml_content': 'service: {}',
            'created_at': 99.0,
            'created_by': 'ordinary-placeholder-test',
        }])

    _complete_terminal_retirement(engine, monkeypatch)
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.update().where(
            serve_state.version_specs_table.c.service_name == ordinary_name,
            serve_state.version_specs_table.c.version == 1).values(
                spec=_explicit_v2_payload(),
                yaml_content='service: {}',
                created_at=300.0))

    assert placement_contract_normalization.run_operator(
        engine=engine, mode=None,
        row_bound=_ROW_BOUND).prior_ledger_mismatches == ()


@pytest.mark.parametrize(('row_kind', 'created_at', 'allowed'), [
    ('explicit_v2', 300.0, True),
    ('explicit_v2', 200.0, False),
    ('placeholder', None, True),
])
def test_protocol_v4_terminal_binding_applies_same_incarnation_high_water(
        empty_postgres, monkeypatch, row_kind, created_at, allowed):
    engine = empty_postgres
    _complete_terminal_retirement(engine, monkeypatch)
    if row_kind == 'explicit_v2':
        spec = _explicit_v2_payload()
        yaml_content = 'service: {}'
    else:
        spec = pickle.dumps(None, protocol=4)
        yaml_content = None
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.insert().values(
            service_name=_SERVICE_NAME,
            version=_CURRENT_VERSION + 1,
            spec=spec,
            yaml_content=yaml_content,
            created_at=created_at,
            created_by='post-terminal-writer'))

    mismatches = placement_contract_normalization.run_operator(
        engine=engine, mode=None,
        row_bound=_ROW_BOUND).prior_ledger_mismatches
    assert (mismatches == ()) is allowed


@pytest.mark.parametrize(('row_kind', 'created_at', 'allowed'), [
    ('explicit_v2', 300.0, True),
    ('explicit_v2', 200.0, False),
    ('placeholder', None, False),
])
def test_protocol_v4_terminal_binding_disambiguates_recreated_service(
        empty_postgres, monkeypatch, row_kind, created_at, allowed):
    engine = empty_postgres
    _complete_terminal_retirement(engine, monkeypatch)
    versions = serve_state.version_specs_table
    services = serve_state.services_table
    if row_kind == 'explicit_v2':
        spec = _explicit_v2_payload()
        yaml_content = 'service: {}'
    else:
        spec = pickle.dumps(None, protocol=4)
        yaml_content = None
    # Version 3 deliberately overlaps a manifested stale identity.  The new
    # parent hash plus a post-terminal explicit commit disambiguates reuse.
    with engine.begin() as connection:
        connection.execute(versions.delete().where(
            versions.c.service_name == _SERVICE_NAME))
        connection.execute(services.update().where(
            services.c.name == _SERVICE_NAME).values(
                hash='new-incarnation',
                resource_scope='new-incarnation',
                lifecycle_epoch=9,
                current_version=3,
                active_versions='[3]'))
        connection.execute(versions.insert().values(
            service_name=_SERVICE_NAME,
            version=3,
            spec=spec,
            yaml_content=yaml_content,
            created_at=created_at,
            created_by='recreated-service'))

    mismatches = placement_contract_normalization.run_operator(
        engine=engine, mode=None,
        row_bound=_ROW_BOUND).prior_ledger_mismatches
    assert (mismatches == ()) is allowed


def test_protocol_v4_terminal_binding_allows_complete_service_teardown(
        empty_postgres, monkeypatch):
    engine = empty_postgres
    _complete_terminal_retirement(engine, monkeypatch)
    with engine.begin() as connection:
        connection.execute(
            serve_state.ephemeral_storage_cleanup_intents_table.delete().where(
                serve_state.ephemeral_storage_cleanup_intents_table.c.
                service_name == _SERVICE_NAME))
        connection.execute(serve_state.version_specs_table.delete().where(
            serve_state.version_specs_table.c.service_name == _SERVICE_NAME))
        connection.execute(serve_state.services_table.delete().where(
            serve_state.services_table.c.name == _SERVICE_NAME))

    assert placement_contract_normalization.run_operator(
        engine=engine, mode=None,
        row_bound=_ROW_BOUND).prior_ledger_mismatches == ()


@pytest.mark.parametrize('timestamp_case', [
    'null_after_boundary',
    'no_finite_boundary',
    'legacy_intent_after_boundary',
    'finite_intent_after_version',
])
def test_postgres_protocol_v4_rejects_invalid_cleanup_timestamp_topology(
        empty_postgres, monkeypatch, timestamp_case):
    engine = empty_postgres
    fixture = _seed_retirement_fixture(engine,
                                       monkeypatch,
                                       timestamp_case=timestamp_case)

    timestamps = iter((200.0, 201.0, 202.0))
    with pytest.raises(placement_contract_normalization.NormalizationBlocker):
        placement_contract_normalization.run_operator(
            **_retirement_kwargs(engine), now=lambda: next(timestamps))

    _assert_database_unchanged_after_failed_retirement(engine, fixture)


@pytest.mark.parametrize('failure_point', ['cas', 'postimage'])
def test_postgres_cleanup_failure_rolls_back_intents_and_retirement(
        empty_postgres, monkeypatch, failure_point):
    engine = empty_postgres
    fixture = _seed_retirement_fixture(engine, monkeypatch)
    intents = serve_state.ephemeral_storage_cleanup_intents_table

    if failure_point == 'cas':
        original = placement_contract_normalization._cas_cleanup_intent_results

        def force_cleanup_cas_failure(session, plan):
            first = plan.rows[0]
            session.execute(intents.update().where(
                intents.c.service_name == first.identity[0],
                intents.c.resource_scope == first.identity[1],
                intents.c.storage_generation == first.identity[2]).values(
                    created_at=first.original['created_at'] + 0.25))
            original(session, plan)

        monkeypatch.setattr(placement_contract_normalization,
                            '_cas_cleanup_intent_results',
                            force_cleanup_cas_failure)
        expected_error = 'Cleanup-intent CAS failed'
    else:
        original = (
            placement_contract_normalization._verify_cleanup_intent_postimages)

        def force_cleanup_postimage_failure(session, plan, row_bound):
            first = plan.rows[0]
            session.execute(intents.update().where(
                intents.c.service_name == first.identity[0],
                intents.c.resource_scope == first.identity[1],
                intents.c.storage_generation == first.identity[2]).values(
                    created_at=first.result['created_at'] + 0.25))
            original(session, plan, row_bound)

        monkeypatch.setattr(placement_contract_normalization,
                            '_verify_cleanup_intent_postimages',
                            force_cleanup_postimage_failure)
        expected_error = 'postimages do not match'

    timestamps = iter((200.0, 201.0, 202.0))
    with pytest.raises(placement_contract_normalization.NormalizationBlocker,
                       match=expected_error):
        placement_contract_normalization.run_operator(
            **_retirement_kwargs(engine), now=lambda: next(timestamps))

    _assert_database_unchanged_after_failed_retirement(engine, fixture)
