"""PostgreSQL transaction coverage for placement-contract retirement."""

# pylint: disable=protected-access,redefined-outer-name,unused-import
import base64
import dataclasses
import hashlib
import json
import pathlib
import pickle
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
from sky.serve import serve_state

_SERVICE_NAME = 'svc-production-shaped-retirement'
_RESOURCE_SCOPE = 'service-incarnation-a'
_VERSION_COUNT = 49
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
) -> _RetirementFixture:
    """Create a production-shaped inventory and completed protocol-1 proof."""
    serve_state.Base.metadata.create_all(engine)
    monkeypatch.setattr(sky, '__commit__', 'a' * 40)
    historical_payload = zlib.decompress(
        base64.b64decode(_V1_1_247_PHYSICAL_PER_GPU_SPEC_ZLIB_B64))
    successor_payload = _explicit_v2_payload()
    version_rows = []
    intent_rows = []
    for version in range(1, _VERSION_COUNT + 1):
        yaml_content = _cleanup_yaml(version)
        version_rows.append({
            'service_name': _SERVICE_NAME,
            'version': version,
            'spec': historical_payload if version == 1 else successor_payload,
            'yaml_content': yaml_content,
            'created_at': 10.0 + version,
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
            'created_at': float(version),
        })

    with engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name=_SERVICE_NAME,
            workspace='workspace',
            status=serve_state.ServiceStatus.READY.value,
            current_version=_VERSION_COUNT,
            active_versions=f'[{_VERSION_COUNT}]',
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
                              _cleanup_yaml(1), _read_intents(engine))


def test_protocol_v2_reader_accepts_exact_production_protocol_v1_snapshot():
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
    assert not placement_contract_normalization._ledger_manifest_mismatches(
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
                versions.c.version == 1)).mappings().one()
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


def test_postgres_protocol_v2_retires_production_shaped_cleanup_inventory(
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
    assert len(intents_after) == _VERSION_COUNT
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
                    versions.c.service_name == _SERVICE_NAME,
                    versions.c.version == 1)).mappings().one())
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

    assert bytes(candidate['spec']) == pickle.dumps(None, protocol=4)
    assert candidate['yaml_content'] is None
    assert candidate['retired_yaml_content'] == fixture.historical_yaml
    assert candidate['retired_at'] == 201.0
    assert candidate['retirement_run_id'] == run_id
    assert manifest['normalizer_version'] == f'2:{"a" * 40}'
    assert not placement_contract_normalization._ledger_manifest_mismatches(
        manifest, entries)
    candidate_ledger = next(entry for entry in entries if entry['version'] == 1)
    assert placement_contract_normalization._retirement_ledger_v2_facts_are_complete(
        candidate_ledger)
    facts = candidate_ledger['dependency_facts']
    assert facts['cleanup_intent_inventory_count'] == _VERSION_COUNT
    assert facts['cleanup_intent_adopted_count'] == _VERSION_COUNT
    assert facts['cleanup_match_inventory_count'] == _VERSION_COUNT
    assert facts['cleanup_candidate_match_count'] == 1
    assert facts['cleanup_candidate_deletion_target_count'] == 0
    assert facts['cleanup_intent_deletion_target_count'] == 0
    assert facts['predecessor_receipt_inventory_count'] == 1
    assert facts['approved_loaded_image_commit_count'] == 1


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
