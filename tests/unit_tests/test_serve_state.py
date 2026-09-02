"""Tests for serve_state.

Focused on the new controller_ip column + atomic update introduced for HA
leader-aware routing.
"""
# Pytest fixture name collides with pylint's "private name" rule (leading
# underscore is the standard convention for fixtures injected for side
# effects). Disable for the file.
# pylint: disable=invalid-name,protected-access
import contextlib
import copy
import enum
import hashlib
import json
import pickle
import sqlite3
from unittest import mock
import uuid

import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky import clouds
from sky.schemas.db import legacy_replica_pickle
from sky.serve import constants as serve_constants
from sky.serve import ephemeral_storage_contract
from sky.serve import kubernetes_identity
from sky.serve import paid_capacity
from sky.serve import placement_contract_normalization
from sky.serve import placement_normalization_manifest
from sky.serve import placement_policy
from sky.serve import replica_info as replica_info_lib
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import service as service_lib
from sky.serve import service_spec as service_spec_lib
from sky.serve import spot_placer
from sky.serve import system_oom_recovery
from sky.serve import system_recovery_state as recovery_state
from sky.skylet import constants as skylet_constants
from sky.utils import common_utils
from sky.utils.db import migration_utils


def _replica(replica_id: int,
             cluster_name: str | None = None,
             version: int = 1) -> replica_managers.ReplicaInfo:
    return replica_managers.ReplicaInfo(
        replica_id=replica_id,
        cluster_name=cluster_name or f'svc-{replica_id}',
        replica_port='8080',
        is_spot=False,
        location=None,
        version=version,
        resources_override=None,
    )


def _paid_pool_key(accelerator: str = 'A100-80GB') -> str:
    """Exact provider pool identity accepted by paid GPU attribution."""
    location = spot_placer.Location(cloud=clouds.AWS(),
                                    region='us-east-1',
                                    zone='us-east-1a',
                                    accelerators={accelerator: 1},
                                    use_spot=True,
                                    instance_type='p4d.24xlarge')
    return paid_capacity.pool_key(location,
                                  workspace='default',
                                  num_nodes=1,
                                  aws_account_id='123456789012')


class _TestServiceSpec(service_spec_lib.SkyServiceSpec):
    """Small, fully-formed service spec used by persistence tests."""

    def __init__(self,
                 label: str = 'test-spec',
                 *,
                 policy: str | None = None,
                 load_balancing_policy: str = 'least_load',
                 uses_logical_replicas: bool = False,
                 graceful_drain_async_occupancy: bool | None = None,
                 lb_stream_timeout_seconds: int = 10,
                 graceful_drain_seconds: int | None = None):
        if uses_logical_replicas:
            graceful_drain_async_occupancy = True
        super().__init__(
            readiness_path='/ready',
            initial_delay_seconds=0,
            readiness_timeout_seconds=5,
            endpoint_probe_interval_seconds=1,
            lb_stream_timeout_seconds=lb_stream_timeout_seconds,
            min_replicas=1,
            lb_high_availability=False,
            max_replicas=1 if uses_logical_replicas else None,
            target_concurrency_per_replica=(1
                                            if uses_logical_replicas else None),
            spot_placer=('dynamic_fallback_per_gpu'
                         if uses_logical_replicas else None),
            load_balancing_policy=load_balancing_policy,
            graceful_drain_seconds=graceful_drain_seconds,
            graceful_drain_async_occupancy=(graceful_drain_async_occupancy),
        )
        self.test_label = label
        self._policy = policy

    def autoscaling_policy_str(self):
        if self._policy is not None:
            return self._policy
        return super().autoscaling_policy_str()


def _service_spec(label: str = 'test-spec',
                  **kwargs) -> service_spec_lib.SkyServiceSpec:
    test_spec = _TestServiceSpec(label, **kwargs)
    spec = service_spec_lib.SkyServiceSpec.__new__(
        service_spec_lib.SkyServiceSpec)
    spec.__dict__ = dict(test_spec.__dict__)
    return spec


def _exact_service_spec(label: str = 'test-spec',
                        **kwargs) -> service_spec_lib.SkyServiceSpec:
    """Return the exact persisted class recognized by the raw inspector."""
    return _service_spec(label, **kwargs)


def _v2_service_spec(label: str = 'test-spec',
                     **kwargs) -> service_spec_lib.SkyServiceSpec:
    spec = _exact_service_spec(label, **kwargs)
    contract = spec.placement_contract
    spec.__dict__.update(contract.persisted_fields())
    spec.__dict__.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)
    return spec


def _v1_service_spec(label: str = 'test-spec',
                     **kwargs) -> service_spec_lib.SkyServiceSpec:
    spec = _exact_service_spec(label, **kwargs)
    contract = spec.placement_contract
    spec.__dict__.update(contract._legacy_v1_persisted_fields())
    spec.__dict__[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] = (
        contract.uses_logical_replicas)
    return spec


def _labeled_version_spec(result) -> tuple[int, str] | None:
    if result is None:
        return None
    version, spec = result
    assert type(spec) is service_spec_lib.SkyServiceSpec
    return version, spec.test_label


def _read_row(engine, name):
    """Read raw services row directly (bypassing get_service_from_name which
    does an INNER JOIN with version_specs and would skip rows without a
    version registered)."""
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(serve_state.services_table).where(
                serve_state.services_table.c.name == name)).fetchone()
    return None if result is None else dict(result._mapping)  # pylint: disable=protected-access


def _read_version_row(engine, name, version):
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(serve_state.version_specs_table).where(
                serve_state.version_specs_table.c.service_name == name,
                serve_state.version_specs_table.c.version ==
                version)).fetchone()
    return None if result is None else dict(result._mapping)  # pylint: disable=protected-access


def _config_snapshot(config: bytes,
                     snapshot_character: str = 'a') -> tuple[bytes, str, str]:
    return (config, hashlib.sha256(config).hexdigest(), snapshot_character * 64)


def _placement_projection_args(worker_projection_version: int = (
    kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION
)) -> dict[str, object]:
    """Return a complete valid set of immutable placement projections."""
    worker_role = 'arn:aws:iam::123456789012:role/skyserve-worker-east'
    return {
        'controller_job_projection': {
            'workspace': 'controller',
            'kubernetes_context': 'controller-east',
            'namespace': 'controller-system',
            'service_account_name': 'controller-sa',
            'priority_class_name': None,
            'lb_data_plane_auth': {
                'secret_name': 'skypilot-serve-lb-data-plane-auth',
                'secret_key': 'tokens',
                'mount_path': ('/etc/skypilot/serve-auth/lb-data-plane/tokens'),
            },
        },
        'controller_work_cache': {
            'kind': 'empty_dir',
            'mount_path': '/mnt/controller-work',
            'required_bytes': 100,
            'required_inodes': 10,
            'size_limit_bytes': 200,
        },
        'worker_placement_projections': [{
            'projection_version': worker_projection_version,
            'candidate_id': 'kubernetes-0000',
            'kubernetes_context': 'east',
            'namespace': 'inference',
            'service_account_name': 'worker-sa',
            'priority_class_name': 'preemptible-inference-low',
            'priority_value': -1000,
            'preemption_policy': 'Never',
            'pod_identity_role_arn': worker_role,
            'accelerator_name': 'H200',
            'accelerator_count': 1,
            'accelerator_scheduling': {
                'label_key': 'nvidia.com/gpu.product',
                'label_values': ['NVIDIA-H200'],
                'resource_key': 'nvidia.com/gpu',
            },
            'scheduler_name': 'default-scheduler',
            'kueue_admission': None,
            'provision_timeout': -1,
            'cache': {
                'kind': 'none',
            },
            'scratch': {
                'kind': 'none',
            },
        }],
    }


def _insert_placement_normalization_run(engine,
                                        run_id: uuid.UUID,
                                        *,
                                        row_count: int = 1,
                                        classification_counts: dict[str, int] |
                                        None = None,
                                        schema_revision: str = '037',
                                        mode: str = 'apply_supported',
                                        normalizer_protocol: int = 1) -> None:
    del classification_counts
    empty_inventory_digest = hashlib.sha256(b'[]').hexdigest()
    with orm.Session(engine) as session:
        session.execute(
            serve_state.placement_normalization_runs_table.insert().values(
                run_id=run_id,
                mode=mode,
                normalizer_version=f'{normalizer_protocol}:{"a" * 40}',
                schema_revision=schema_revision,
                release_version='test',
                started_at=1.0,
                completed_at=2.0,
                row_bound=row_count,
                row_count=0,
                classification_counts={},
                pre_inventory_sha256=empty_inventory_digest,
                post_inventory_sha256=empty_inventory_digest,
                freeze_evidence_sha256='a' * 64))
        session.commit()


def _placement_normalization_value_sha256(value) -> str:
    if isinstance(value, memoryview):
        value = value.tobytes()
    elif isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, bytes):
        payload = b'bytes\0' + value
    else:
        payload = b'json\0' + json.dumps(
            value,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
            default=str,
        ).encode()
    return hashlib.sha256(payload).hexdigest()


def _placement_normalization_row_sha256(column_sha256s: dict[str, str]) -> str:
    return hashlib.sha256(
        json.dumps(column_sha256s, sort_keys=True,
                   separators=(',', ':')).encode()).hexdigest()


def _refresh_placement_normalization_manifest(session: orm.Session,
                                              run_id: uuid.UUID) -> None:
    """Keep synthetic manifests complete and cryptographically coherent."""
    rows = session.execute(
        sqlalchemy.select(serve_state.placement_normalization_rows_table).where(
            serve_state.placement_normalization_rows_table.c.run_id ==
            run_id)).mappings().all()
    classification_counts: dict[str, int] = {}
    pre_inventory = []
    post_inventory = []
    for row in rows:
        classification = row['classification']
        classification_counts[classification] = (
            classification_counts.get(classification, 0) + 1)
        identity = (row['service_name'], row['version'])
        pre_inventory.append((*identity, row['original_row_sha256']))
        post_inventory.append((*identity, row['result_row_sha256']))

    def _fleet_digest(inventory) -> str:
        return hashlib.sha256(
            json.dumps(sorted(inventory),
                       separators=(',', ':')).encode()).hexdigest()

    session.execute(
        sqlalchemy.update(serve_state.placement_normalization_runs_table).where(
            serve_state.placement_normalization_runs_table.c.run_id ==
            run_id).values(row_count=len(rows),
                           classification_counts=classification_counts,
                           pre_inventory_sha256=_fleet_digest(pre_inventory),
                           post_inventory_sha256=_fleet_digest(post_inventory)))


def _insert_placement_normalization_row(
        engine,
        run_id: uuid.UUID,
        service_name: str,
        version: int,
        spec_bytes: bytes,
        service_hash: str,
        lifecycle_epoch: int | None,
        *,
        result_spec_sha256: str | None = None,
        classification: str = 'fieldless_supported',
        outcome: str = 'changed') -> None:
    spec_digest = hashlib.sha256(spec_bytes).hexdigest()
    with orm.Session(engine) as session:
        version_row = session.execute(
            sqlalchemy.select(serve_state.version_specs_table).where(
                serve_state.version_specs_table.c.service_name == service_name,
                serve_state.version_specs_table.c.version ==
                version)).mappings().one()
        version_row = placement_contract_normalization._frozen_version_row(
            version_row)
        result_columns = {
            column: _placement_normalization_value_sha256(value)
            for column, value in version_row.items()
        }
        original_columns = dict(result_columns)
        original_spec_sha256 = spec_digest
        if outcome != 'unchanged':
            original_spec_sha256 = hashlib.sha256(b'original-spec\0' +
                                                  spec_bytes).hexdigest()
            original_columns['spec'] = hashlib.sha256(b'original-column\0' +
                                                      spec_bytes).hexdigest()
        session.execute(
            serve_state.placement_normalization_rows_table.insert().values(
                run_id=run_id,
                service_name=service_name,
                version=version,
                classification=classification,
                outcome=outcome,
                original_spec_sha256=original_spec_sha256,
                result_spec_sha256=(result_spec_sha256 or spec_digest),
                original_row_sha256=_placement_normalization_row_sha256(
                    original_columns),
                result_row_sha256=_placement_normalization_row_sha256(
                    result_columns),
                original_column_sha256s=original_columns,
                result_column_sha256s=result_columns,
                contract_projection=(None
                                     if classification == 'placeholder' else {
                                         'version': 1
                                     }),
                service_hash=service_hash,
                service_lifecycle_epoch=lifecycle_epoch,
                dependency_facts={
                    'service_hash': service_hash,
                    'service_lifecycle_epoch': lifecycle_epoch,
                }))
        _refresh_placement_normalization_manifest(session, run_id)
        session.commit()


def _protocol4_historical_payload() -> bytes:
    historical = _exact_service_spec('historical', uses_logical_replicas=True)
    state = dict(historical.__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field, None)
    state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)
    return placement_contract_normalization._serialize_raw_state(state, 4)


def _protocol4_cleanup_yaml(resource_scope: str,
                            storage_generation: str) -> str:
    scope_id = ephemeral_storage_contract.canonical_ephemeral_storage_scope_id(
        resource_scope, storage_generation)
    metadata_key = serve_constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY
    return f"""\
_metadata:
  {metadata_key}:
    resource_scope: {resource_scope}
    scope_id: {scope_id}
    storage_generation: {storage_generation}
    storage_mounts: []
service: {{}}
"""


def _insert_protocol4_terminal_receipt_state(
    engine,
    service_name: str,
    owner: tuple[int, str],
) -> tuple[uuid.UUID, int]:
    """Persist a real protocol-4 terminal manifest for receipt tests."""
    service_hash = 'incarnation-a'
    assert _add_minimal_service(service_name,
                                service_hash=service_hash,
                                controller_pid=owner[0],
                                controller_ip=owner[1],
                                resource_scope=service_hash,
                                spec=_v2_service_spec('initial'))
    lifecycle_epoch = serve_state.claim_service_lifecycle_epoch(service_name)
    assert serve_state.add_version(service_name) == 2
    assert serve_state.add_version(service_name) == 3
    assert serve_state.add_or_update_version(
        service_name, 4, _v2_service_spec('successor'),
        'service: {}') is serve_state.VersionCommitResult.COMMITTED

    cleanup_yaml = _protocol4_cleanup_yaml(service_hash, 'generation-1')
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(serve_state.version_specs_table).where(
                serve_state.version_specs_table.c.service_name == service_name,
                serve_state.version_specs_table.c.version == 1).values(
                    spec=_protocol4_historical_payload(),
                    yaml_content=cleanup_yaml,
                    controller_applied_at=None))
        session.commit()

    rows = []
    for version in range(1, 5):
        persisted = placement_contract_normalization._frozen_version_row(
            _read_version_row(engine, service_name, version))
        analysis, classification = (
            placement_contract_normalization._classify_version_row(persisted))
        facts = {
            'service_present': True,
            'service_current_version': 4,
            'service_hash': service_hash,
            'service_lifecycle_epoch': lifecycle_epoch,
            'service_resource_scope': service_hash,
            'service_status': 'READY',
            'service_active': version == 4,
            'service_pool': 0,
            'service_resource_action_mode': 'legacy',
            'service_resource_action_mode_changed_at': None,
            'replica_count': 0,
            'unknown_version_replica_count': 0,
            'cleanup_intent_count': 1,
            'quarantined': False,
            'controller_applied': False,
            'retired': False,
        }
        rows.append(
            placement_contract_normalization._RowWork(persisted,
                                                      dict(persisted),
                                                      analysis,
                                                      classification,
                                                      dependency_facts=facts))

    intent = {
        'service_name': service_name,
        'resource_scope': service_hash,
        'storage_generation': 'generation-1',
        'yaml_content': cleanup_yaml,
        'pool': 0,
        'lifecycle_epoch': lifecycle_epoch,
        'provisional': 1,
        'created_at': 0.5,
    }
    service_row = {
        'name': service_name,
        'current_version': 4,
        'active_versions': '[4]',
        'hash': service_hash,
        'lifecycle_epoch': lifecycle_epoch,
        'resource_scope': service_hash,
        'workspace': 'workspace',
        'status': 'READY',
        'pool': 0,
        'resource_action_mode': 'legacy',
        'resource_action_mode_changed_at': None,
    }
    cleanup_plan = placement_contract_normalization._build_cleanup_intent_plan(
        [intent], rows, {service_name: service_row}, row_bound=len(rows))

    approved_commit_digest = (
        placement_contract_normalization._canonical_json_sha256(['b' * 40]))
    freeze_input_digest = 'c' * 64
    freeze_binding_digest = (
        placement_contract_normalization._canonical_json_sha256({
            'approved_loaded_image_commit_sha256': approved_commit_digest,
            'operator_freeze_evidence_input_sha256': freeze_input_digest,
        }))
    receipt_facts = {
        'predecessor_receipt_schema':
            placement_contract_normalization._PREDECESSOR_RECEIPT_SCHEMA,
        'predecessor_receipt_inventory_count': 1,
        'predecessor_receipt_inventory_sha256':
            placement_contract_normalization._canonical_json_sha256(
                [service_name]),
        'approved_loaded_image_commit_count': 1,
        'approved_loaded_image_commit_sha256': approved_commit_digest,
        'operator_freeze_evidence_input_sha256': freeze_input_digest,
        'operator_freeze_approved_commit_binding_sha256': freeze_binding_digest,
        'predecessor_receipts_complete': True,
    }
    receipt_evidence = (
        placement_contract_normalization._PredecessorReceiptEvidence(
            frozenset({service_name}), receipt_facts))
    image_evidence = {
        (service_name, version):
            placement_contract_normalization._ExternalEvidence(0, digest * 64)
        for version, digest in zip((1, 2, 3), 'abc')
    }
    action_evidence = {
        (service_name, version):
            placement_contract_normalization._ExternalEvidence(0, digest * 64)
        for version, digest in zip((1, 2, 3), 'def')
    }
    empty_evidence = placement_contract_normalization._ExternalEvidence(
        0, '0' * 64)
    api_pod = placement_contract_normalization._canonical_api_pod_identity(
        'pod-a', uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'))
    run_id = uuid.uuid4()
    placement_contract_normalization._prepare_retirement_rows(
        rows, {service_name: service_row}, run_id, 10.0, image_evidence,
        action_evidence, empty_evidence, empty_evidence, api_pod,
        empty_evidence, cleanup_plan, receipt_evidence)

    ledger_entries = []
    classification_counts: dict[str, int] = {}
    for row in rows:
        classification = row.ledger_classification
        classification_counts[classification] = (
            classification_counts.get(classification, 0) + 1)
        ledger_entries.append({
            'run_id': run_id,
            'service_name': service_name,
            'version': row.identity[1],
            'classification': classification,
            'outcome': row.outcome,
            'original_spec_sha256': row.analysis.source_sha256,
            'result_spec_sha256': placement_contract_normalization._sha256(
                bytes(row.result['spec'])),
            'original_row_sha256': placement_contract_normalization._row_sha256(
                row.original),
            'result_row_sha256': placement_contract_normalization._row_sha256(
                row.result),
            'original_column_sha256s':
                placement_contract_normalization._column_sha256s(row.original),
            'result_column_sha256s':
                placement_contract_normalization._column_sha256s(row.result),
            'contract_projection': row.analysis.contract_projection,
            'service_hash': service_hash,
            'service_lifecycle_epoch': lifecycle_epoch,
            'dependency_facts': row.dependency_facts,
        })
    run_values = {
        'run_id': run_id,
        'mode': 'retire_terminal_historical',
        'normalizer_version': f'4:{"a" * 40}',
        'schema_revision': '037',
        'release_version': 'test',
        'started_at': 1.0,
        'completed_at': 2.0,
        'row_bound': len(rows),
        'row_count': len(rows),
        'classification_counts': classification_counts,
        'pre_inventory_sha256': placement_contract_normalization._fleet_sha256(
            rows, result=False),
        'post_inventory_sha256': placement_contract_normalization._fleet_sha256(
            rows, result=True),
        'freeze_evidence_sha256': freeze_binding_digest,
    }
    with orm.Session(engine) as session:
        session.execute(
            serve_state.placement_normalization_runs_table.insert().values(
                **run_values))
        session.execute(serve_state.placement_normalization_rows_table.insert(),
                        ledger_entries)
        for row in rows:
            session.execute(
                sqlalchemy.update(serve_state.version_specs_table).where(
                    serve_state.version_specs_table.c.service_name ==
                    service_name, serve_state.version_specs_table.c.version ==
                    row.identity[1]).values(**row.result))
        session.execute(
            sqlalchemy.update(serve_state.services_table).where(
                serve_state.services_table.c.name == service_name).values(
                    placement_normalization_requested_run_id=run_id))
        session.commit()
    return run_id, lifecycle_epoch


def _validate_placement_normalization_manifest_directly(
        engine, run_id: uuid.UUID) -> None:
    with orm.Session(engine) as session:
        serve_state._load_and_validate_placement_normalization_manifest(
            session, run_id)


def _recreate_protocol4_service_version(engine, service_name: str, *,
                                        explicit: bool) -> None:
    if explicit:
        spec = pickle.dumps(_v2_service_spec('recreated'), protocol=4)
        yaml_content = 'service: {}'
        created_at = 3.0
    else:
        spec = pickle.dumps(None, protocol=4)
        yaml_content = None
        created_at = None
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(serve_state.version_specs_table).where(
                serve_state.version_specs_table.c.service_name == service_name))
        session.execute(
            sqlalchemy.update(serve_state.services_table).where(
                serve_state.services_table.c.name == service_name).values(
                    hash='incarnation-b', lifecycle_epoch=2, current_version=1))
        session.execute(serve_state.version_specs_table.insert().values(
            service_name=service_name,
            version=1,
            spec=spec,
            yaml_content=yaml_content,
            created_at=created_at))
        session.commit()


def test_placement_normalization_receipt_locks_only_service_row() -> None:
    """The optional run manifest must stay outside PostgreSQL's row lock."""
    query = serve_state._placement_normalization_receipt_query(
        'svc',
        recovery_version=1,
        current_version=1,
        expected_service_hash='incarnation-a',
        expected_controller_owner=(123, '10.0.0.1'),
        require_ledger=True)
    engine = sqlalchemy.create_mock_engine('postgresql+psycopg2://',
                                           lambda *args, **kwargs: None)

    locked = serve_state._lock_placement_normalization_receipt_query(
        query, engine)
    sql = str(locked.compile(dialect=postgresql.dialect()))

    assert 'FOR UPDATE OF services' in sql


def test_protocol4_current_inventory_query_projects_frozen_columns() -> None:
    query = serve_state._placement_normalization_current_inventory_query(
        ['svc'])

    assert tuple(query.selected_columns.keys()) == (
        placement_normalization_manifest.VERSION_SPEC_COLUMNS)
    assert serve_state._PLACEMENT_NORMALIZATION_RECEIPT_MAX_ROWS + 1 in (
        query.compile().params.values())


def test_protocol4_scan_inventory_projects_frozen_columns(
        _mock_serve_db) -> None:
    assert _add_minimal_service('svc-scan-frozen')
    with orm.Session(_mock_serve_db) as session:
        rows, _ = placement_contract_normalization._scan_inventory(session, 10)

    row = next(row for row in rows if row.identity == ('svc-scan-frozen', 1))
    assert tuple(
        row.original) == (placement_normalization_manifest.VERSION_SPEC_COLUMNS)


_VERSIONED_HA_SCRIPT = (
    f'{serve_constants.VERSIONED_HA_CONFIG_RECOVERY_MARKER}\n'
    'export SKYPILOT_CONFIG=/tmp/config.yaml.v2\n'
    'python -m sky.serve.service --service-name svc\n')


@contextlib.contextmanager
def _count_sql_statements(engine):
    counts = {'n': 0}

    def _count(*args, **kwargs):
        del args, kwargs
        counts['n'] += 1

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _count)
    try:
        yield counts
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute', _count)


@pytest.fixture
def _mock_serve_db(tmp_path, monkeypatch):
    """Point serve_state at a fresh sqlite DB for the duration of one test."""
    db_path = tmp_path / 'serve_state_testing.db'
    engine = create_engine(f'sqlite:///{db_path}')

    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    # `metadata.create_all` only creates tables that don't already exist; this
    # is enough since we're starting from a brand-new DB and the alembic
    # upgrade step in `create_table` would otherwise need a full env.
    serve_state.Base.metadata.create_all(engine)
    yield engine


def _add_minimal_service(name: str,
                         controller_ip=None,
                         controller_pid=12345,
                         service_hash=None,
                         lifecycle_epoch=None,
                         resource_scope=None,
                         workspace=None,
                         yaml_content='yaml: v1',
                         pool=False,
                         spec=None,
                         created_by=None,
                         submitted_yaml_content=None,
                         placement_catalog=None,
                         controller_config=None,
                         controller_config_digest=None,
                         controller_config_snapshot_id=None,
                         controller_job_projection=None,
                         controller_work_cache=None,
                         worker_placement_projections=None,
                         owner_user_id=None,
                         owner_user_name=None):
    """Add a service row with all-required-args defaults so individual tests
    only need to specify what they care about."""
    if spec is None:
        spec = _service_spec(policy='policy',
                             load_balancing_policy='round_robin')
    return serve_state.add_service(
        name=name,
        controller_job_id=1,
        policy='policy',
        requested_resources_str='1x[CPU:1+]',
        load_balancing_policy='round_robin',
        status=serve_state.ServiceStatus.CONTROLLER_INIT,
        tls_encrypted=False,
        pool=pool,
        controller_pid=controller_pid,
        entrypoint='entry',
        spec=spec,
        yaml_content=yaml_content,
        workspace=workspace,
        controller_ip=controller_ip,
        service_hash=service_hash,
        lifecycle_epoch=lifecycle_epoch,
        resource_scope=resource_scope,
        created_by=created_by,
        submitted_yaml_content=submitted_yaml_content,
        placement_catalog=placement_catalog,
        controller_config=controller_config,
        controller_config_digest=controller_config_digest,
        controller_config_snapshot_id=controller_config_snapshot_id,
        controller_job_projection=controller_job_projection,
        controller_work_cache=controller_work_cache,
        worker_placement_projections=worker_placement_projections,
        owner_user_id=owner_user_id,
        owner_user_name=owner_user_name,
    )


def _insert_orphan_service_row(engine, name: str, pool: bool = False):
    """Insert a `services` row with no `version_specs` row.

    Simulates the debris stranded by the pre-atomic registration path (or an
    interrupted teardown) on an older controller; `add_service` can no longer
    produce this state."""
    with orm.Session(engine) as session:
        session.execute(serve_state.services_table.insert().values(
            name=name,
            controller_job_id=1,
            status=serve_state.ServiceStatus.CONTROLLER_INIT.value,
            requested_resources_str='1x[CPU:1+]',
            pool=int(pool),
            controller_pid=12345,
            controller_incarnation=uuid.uuid4(),
            hash='orphan',
            entrypoint='entry'))
        session.commit()


def test_system_recovery_persistence_fails_closed_on_sqlite(_mock_serve_db):
    assert not serve_state.system_recovery_persistence_available()
    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='PostgreSQL'):
        serve_state.patch_replica_system_recovery(
            'svc',
            1,
            _replica(1),
            expected_service_hash='hash',
            expected_lifecycle_epoch=1,
            expected_controller_owner=(123, '10.0.0.1'),
            expected_revision=0)


def test_legacy_sqlite_registration_remains_ownerless(_mock_serve_db,
                                                      monkeypatch) -> None:
    """Local controller state must not invent central tenant authority."""
    monkeypatch.setenv(skylet_constants.USER_ID_ENV_VAR, 'ambient-owner')
    monkeypatch.setenv(skylet_constants.USER_ENV_VAR, 'ambient@example.com')

    assert _add_minimal_service('local-ownerless')

    with _mock_serve_db.connect() as connection:
        owner = connection.execute(
            sqlalchemy.select(
                serve_state.services_table.c.owner_user_id,
                serve_state.services_table.c.owner_user_name).where(
                    serve_state.services_table.c.name ==
                    'local-ownerless')).one()
    assert owner == (None, None)


@pytest.mark.parametrize('owner_user_id,owner_user_name', [
    ('explicit-owner', None),
    (None, 'explicit@example.com'),
])
def test_explicit_partial_service_owner_is_rejected(_mock_serve_db,
                                                    owner_user_id,
                                                    owner_user_name) -> None:
    with pytest.raises(ValueError, match='owner_user_'):
        _add_minimal_service('partial-owner',
                             owner_user_id=owner_user_id,
                             owner_user_name=owner_user_name)


def test_system_recovery_transaction_advances_revision_once(
        _mock_serve_db, monkeypatch):
    """Exercise the reducer transaction independent of PostgreSQL syntax."""
    engine = _mock_serve_db
    owner = (123, '10.0.0.1')
    with engine.begin() as connection:
        connection.execute(
            serve_state.service_lifecycle_fences_table.insert().values(
                name='svc', epoch=1))
    assert _add_minimal_service('svc',
                                controller_pid=owner[0],
                                controller_ip=owner[1],
                                service_hash='service-hash',
                                lifecycle_epoch=1,
                                workspace='default')
    ordinary = _replica(7, version=3)
    assert serve_state.add_or_update_replica(
        'svc',
        7,
        ordinary,
        expected_service_hash='service-hash',
        expected_lifecycle_epoch=1,
        expected_controller_owner=owner)

    # PostgreSQL row-lock behavior is covered in the real-PG companion suite.
    # Bypass only the dialect admission guard here to keep transition logic
    # executable on every developer machine.
    monkeypatch.setattr(serve_state, '_require_system_recovery_postgres',
                        lambda: engine)
    digest = 'a' * 64
    intent = recovery_state.SystemRecoveryLaunchIntent(
        version=1,
        controller_contract_version=2,
        recovery_authorization_version=3,
        recovery_authorization_profile_id='boltz-l4-v3',
        recovery_authorization_sha256=digest,
        runtime_profile_version=2,
        expected_runtime_capability=recovery_state.SYSTEM_RECOVERY_CAPABILITY,
        service_hash='service-hash',
        replica_id=7,
        launch_generation=7,
        launch_nonce='b' * 64,
        workspace='default',
        resource_envelope_sha256=digest,
        task_sha256=digest,
        runtime_image_digest=f'sha256:{digest}',
        owned_container_spec_sha256=digest,
        execution_envelope_sha256=digest)
    desired = serve_state.get_replica_info_from_id('svc', 7)
    assert desired is not None
    desired.system_recovery_launch_intent = intent
    desired.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.CANDIDATE)
    candidate = serve_state.create_replica_system_recovery_candidate(
        'svc',
        7,
        desired,
        expected_service_hash='service-hash',
        expected_lifecycle_epoch=1,
        expected_controller_owner=owner,
        expected_revision=0)
    assert candidate.system_recovery_revision == 1

    unbound = system_oom_recovery.create_unbound_launch_context(
        intent,
        service_name='svc',
        service_version=3,
        controller_pid=owner[0],
        controller_ip=owner[1])
    bound = serve_state.bind_replica_system_recovery_launch_request(
        unbound, 'request-1')
    assert bound.launch_request_id == 'request-1'
    assert bound.system_recovery_revision == 2


def test_placement_policy_state_is_separate_and_owner_fenced(_mock_serve_db):
    owner = (123, '10.0.0.1')
    _add_minimal_service('svc',
                         controller_pid=owner[0],
                         controller_ip=owner[1],
                         service_hash='incarnation-a')

    spot_state = {'version': 1, 'benches': [{'reason': 'capacity'}]}
    rebalance_state = {'version': 1, 'candidates': [{'replica_id': 7}]}
    assert serve_state.set_service_spot_placement_state('svc', 'incarnation-a',
                                                        owner, spot_state)
    assert not serve_state.set_service_cost_rebalance_state(
        'svc', 'incarnation-a', (999, '10.0.0.9'), rebalance_state)
    assert serve_state.set_service_cost_rebalance_state('svc', 'incarnation-a',
                                                        owner, rebalance_state)

    assert serve_state.get_service_placement_policy_states('svc') == {
        'spot_placement_state': spot_state,
        'cost_rebalance_state': rebalance_state,
    }
    assert not serve_state.set_service_spot_placement_state(
        'svc', 'stale-incarnation', owner, {
            'version': 1,
            'benches': []
        })
    assert serve_state.get_service_placement_policy_states('missing') is None


def test_launch_budget_counts_share_one_replica_scan(_mock_serve_db,
                                                     monkeypatch):
    """Provisioning and termination occupancy are counted in one SQL query.

    A row can satisfy both predicates while a launched replica is being
    terminated; preserve the historical independent-count semantics.
    """
    provisioning = replica_managers.ReplicaInfo(replica_id=1,
                                                cluster_name='svc-1',
                                                replica_port='8080',
                                                is_spot=False,
                                                location=None,
                                                version=1,
                                                resources_override=None)
    provisioning.status_property.sky_launch_status = (
        common_utils.ProcessStatus.RUNNING)
    terminating = replica_managers.ReplicaInfo(replica_id=2,
                                               cluster_name='svc-2',
                                               replica_port='8080',
                                               is_spot=False,
                                               location=None,
                                               version=1,
                                               resources_override=None)
    terminating.status_property.sky_launch_status = (
        common_utils.ProcessStatus.RUNNING)
    terminating.status_property.sky_down_status = (
        common_utils.ProcessStatus.RUNNING)
    serve_state.add_or_update_replica('svc', 1, provisioning)
    serve_state.add_or_update_replica('svc', 2, terminating)

    original_loads = serve_state.pickle.loads
    unpickles = 0

    def _counting_loads(value):
        nonlocal unpickles
        unpickles += 1
        return original_loads(value)

    monkeypatch.setattr(serve_state.pickle, 'loads', _counting_loads)
    with _count_sql_statements(_mock_serve_db) as counts:
        assert serve_state.get_replica_mutation_counts() == (2, 1)
    assert counts['n'] == 1
    assert unpickles == 0


def test_replica_json_storage_round_trip_preserves_lifecycle_state():
    info = _replica(7, version=3)
    info.planned_capacity = 8
    info.unknown_capacity_replacement = True
    info.logical_bridge_capacity_verified = True
    info.created_at = 123.5
    info.first_not_ready_time = 124.5
    info.first_consecutive_failure_time = 125.5
    info.reserved_fill = True
    info.cost_rebalance_for_replica_id = 4
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    info.status_property.service_ready_now = True
    info.status_property.first_ready_time = 126.5
    info.status_property.sky_down_status = common_utils.ProcessStatus.SCHEDULED
    info.status_property.is_scale_down = True
    info.status_property.drain_cap_seconds = 30
    info.status_property.drain_started_at = 1234.5
    info.status_property.wait_for_idle_before_termination = True
    info.status_property.logical_retirement_version = 3
    info.status_property.logical_retirement_controller_epoch = 'epoch-a'
    info.status_property.logical_retirement_generation = 10
    info.status_property.logical_retirement_target_capacity = 17
    info.status_property.logical_retirement_confirmed_generation = 11
    info.status_property.logical_retirement_bounded_deadline = True
    info.status_property.logical_retirement_committed = True

    restored = replica_managers.ReplicaInfo.from_storage_dict(
        info.to_storage_dict())

    assert restored.to_storage_dict() == info.to_storage_dict()
    assert restored.unknown_capacity_replacement is True
    assert restored.logical_bridge_capacity_verified is True
    assert restored.status == info.status

    malformed_state = info.to_storage_dict()
    malformed_state['status_property'][
        'logical_retirement_bounded_deadline'] = 'true'
    malformed_restored = replica_managers.ReplicaInfo.from_storage_dict(
        malformed_state)
    assert not malformed_restored.status_property.logical_retirement_bounded_deadline

    malformed_commit_state = info.to_storage_dict()
    malformed_commit_state['status_property'][
        'logical_retirement_committed'] = 'true'
    malformed_commit_restored = replica_managers.ReplicaInfo.from_storage_dict(
        malformed_commit_state)
    assert (
        malformed_commit_restored.status_property.logical_retirement_committed
        is None)

    for malformed_started_at in (True, 0, -1, float('inf'), '1234.5'):
        malformed_drain_state = info.to_storage_dict()
        malformed_drain_state['status_property'][
            'drain_started_at'] = malformed_started_at
        malformed_drain_restored = (replica_managers.ReplicaInfo.
                                    from_storage_dict(malformed_drain_state))
        assert malformed_drain_restored.status_property.drain_started_at is None


def test_replica_json_storage_preserves_region_independent_image_id():
    image = 'docker:example.invalid/boltz:model'
    resource_state = {
        'cloud': 'Kubernetes',
        'region': 'prod_research_cluster_eks',
        'zone': None,
        'accelerators': {
            'A100-80GB': 1,
        },
        'use_spot': False,
        'image_id': {
            None: image,
        },
        'disk_tier': None,
        'ephemeral_storage': 20,
    }
    info = _replica(9)
    info.location = dict(resource_state)
    info.resources_override = dict(resource_state)
    info.resources_override['cloud'] = clouds.Kubernetes()

    storage_state = info.to_storage_dict()
    assert storage_state['location']['image_id'] == [[None, image]]
    assert storage_state['resources_override']['image_id'] == [[None, image]]

    # Exercise the same JSON object-key conversion as PostgreSQL JSONB.
    restored = replica_managers.ReplicaInfo.from_storage_dict(
        json.loads(json.dumps(storage_state)))

    assert restored.location['image_id'] == {None: image}
    assert restored.resources_override['image_id'] == {None: image}
    assert restored.resources_override['cloud'] == 'Kubernetes'
    assert restored.get_spot_location().image_id == {None: image}
    assert restored.to_storage_dict() == storage_state


def test_replica_json_storage_reads_legacy_null_image_id_key():
    info = _replica(1)
    state = info.to_storage_dict()
    state['resources_override'] = {
        'cloud': 'Kubernetes',
        'region': 'prod_research_cluster_eks',
        'image_id': {
            'null': 'docker:example.invalid/boltz:model',
        },
    }

    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    assert restored.resources_override['image_id'] == {
        None: 'docker:example.invalid/boltz:model'
    }


@pytest.mark.parametrize('planned_capacity', [0, -1, True, 1.5, '8'])
def test_replica_rejects_invalid_planned_capacity(planned_capacity):
    with pytest.raises(ValueError, match='positive integer'):
        replica_managers.ReplicaInfo(replica_id=1,
                                     cluster_name='svc-1',
                                     replica_port='8080',
                                     is_spot=True,
                                     location=None,
                                     version=1,
                                     resources_override=None,
                                     planned_capacity=planned_capacity)


def test_replica_rejects_invalid_stored_planned_capacity():
    state = _replica(1).to_storage_dict()
    state['planned_capacity'] = 0
    with pytest.raises(ValueError, match='Stored planned_capacity'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


def test_replica_state_uses_jsonb_on_postgres():
    state_type = serve_state.replicas_table.c.replica_state.type
    assert isinstance(state_type.dialect_impl(postgresql.dialect()),
                      postgresql.JSONB)


def test_resource_action_common_columns_default_to_inert(_mock_serve_db):
    _add_minimal_service('svc', service_hash='incarnation-a')
    assert serve_state.add_or_update_replica('svc', 1, _replica(1))

    with orm.Session(_mock_serve_db) as session:
        service = session.execute(
            sqlalchemy.select(serve_state.services_table).where(
                serve_state.services_table.c.name == 'svc')).mappings().one()
        replica = session.execute(
            sqlalchemy.select(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == 'svc',
                serve_state.replicas_table.c.replica_id == 1)).mappings().one()

    assert service['resource_action_mode'] == 'legacy'
    assert service['resource_action_mode_changed_at'] is None
    assert all(replica[name] is None
               for name in serve_state._ACTION_OWNED_REPLICA_COLUMNS)


def test_replica_teardown_identity_snapshot_distinguishes_legacy_and_action(
        _mock_serve_db):
    _add_minimal_service('svc', service_hash='incarnation-a')
    replica = _replica(1)
    assert serve_state.add_or_update_replica('svc', 1, replica)

    assert serve_state.get_replica_resource_action_identities('svc',
                                                              [1, 2]) == {
                                                                  1: None
                                                              }
    snapshot = serve_state.get_replica_info_with_resource_action_identity(
        'svc', 1)
    assert snapshot is not None
    legacy_info, legacy_identity = snapshot
    assert legacy_info.cluster_name == 'svc-1'
    assert legacy_identity is None

    replica_incarnation = uuid.uuid4()
    cluster_record_uuid = uuid.uuid4()
    with orm.Session(_mock_serve_db) as session:
        session.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == 'svc',
                serve_state.replicas_table.c.replica_id == 1).values(
                    replica_incarnation=replica_incarnation,
                    desired_generation=3,
                    sky_cluster_record_uuid=cluster_record_uuid))
        session.commit()

    action_identity = serve_state.get_replica_resource_action_identity('svc', 1)
    assert action_identity == serve_state.ReplicaResourceActionIdentity(
        replica_id=1,
        cluster_name='svc-1',
        replica_incarnation=replica_incarnation,
        desired_generation=3,
        sky_cluster_record_uuid=cluster_record_uuid)
    action_snapshot = (
        serve_state.get_replica_info_with_resource_action_identity('svc', 1))
    assert action_snapshot is not None
    assert action_snapshot[0].cluster_name == 'svc-1'
    assert action_snapshot[1] == action_identity


@pytest.mark.parametrize('invalid_values, message', [
    ({
        'replica_incarnation': uuid.uuid4()
    }, 'partial or invalid'),
    ({
        'launch_action_id': uuid.uuid4()
    }, 'legacy replica row has resource-action links'),
    ({
        'replica_incarnation': uuid.uuid4(),
        'desired_generation': 1,
        'sky_cluster_record_uuid': uuid.uuid4(),
        'launch_action_id': uuid.uuid4(),
        'launch_shadow_coverage_id': uuid.uuid4(),
    }, 'competing launch action owners'),
])
def test_replica_teardown_identity_snapshot_rejects_invalid_rows(
        _mock_serve_db, invalid_values, message):
    _add_minimal_service('svc', service_hash='incarnation-a')
    assert serve_state.add_or_update_replica('svc', 1, _replica(1))
    with orm.Session(_mock_serve_db) as session:
        session.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == 'svc',
                serve_state.replicas_table.c.replica_id == 1).values(
                    **invalid_values))
        session.commit()

    with pytest.raises(serve_state.MalformedReplicaResourceActionIdentityError,
                       match=message):
        serve_state.get_replica_resource_action_identities('svc', [1])


def test_replica_teardown_snapshot_rejects_divergent_physical_column(
        _mock_serve_db):
    _add_minimal_service('svc', service_hash='incarnation-a')
    assert serve_state.add_or_update_replica('svc', 1, _replica(1))
    with orm.Session(_mock_serve_db) as session:
        session.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == 'svc',
                serve_state.replicas_table.c.replica_id == 1).values(
                    cluster_name='replacement-cluster'))
        session.commit()

    with pytest.raises(serve_state.MalformedReplicaResourceActionIdentityError,
                       match='JSON state differs'):
        serve_state.get_replica_info_with_resource_action_identity('svc', 1)


def test_replica_updates_and_insert_conflicts_preserve_action_owned_columns(
        _mock_serve_db):
    service_hash = 'incarnation-a'
    _add_minimal_service('svc', service_hash=service_hash)
    expected_by_replica = {}

    # Fresh paid admission fails closed on any cleanup-unproven row whose
    # relational pool key and zero-cost copy disagree, so the pre-existing
    # rows carry the same exact paid pool the admission below targets.
    pool_key = _paid_pool_key()
    for replica_id in range(1, 5):
        existing = _replica(replica_id)
        existing.paid_capacity_pool_key = pool_key
        assert serve_state.add_or_update_replica('svc', replica_id, existing)
        launch_shadow_coverage_id = (None if replica_id %
                                     2 else uuid.UUID(int=replica_id * 100 + 5))
        down_shadow_coverage_id = (None if replica_id %
                                   2 else uuid.UUID(int=replica_id * 100 + 6))
        action_values = {
            'replica_incarnation': uuid.UUID(int=replica_id * 100 + 1),
            'desired_generation': replica_id,
            'sky_cluster_record_uuid': uuid.UUID(int=replica_id * 100 + 2),
            'launch_action_id':
                (uuid.UUID(int=replica_id * 100 + 3) if replica_id % 2 else None
                ),
            'down_action_id':
                (uuid.UUID(int=replica_id * 100 + 4) if replica_id % 2 else None
                ),
            'launch_shadow_coverage_id': launch_shadow_coverage_id,
            'down_shadow_coverage_id': down_shadow_coverage_id,
            'launch_shadow_sample_id': launch_shadow_coverage_id,
            'down_shadow_sample_id': down_shadow_coverage_id,
            'resource_action_spec_identity_sha256': None,
            'ordinary_launch_association_id': uuid.UUID(int=replica_id * 100 + 7
                                                       ),
            'non_pool_launch_authorization': None,
            'reserved_fill_intent_idempotency_key': None,
        }
        expected_by_replica[replica_id] = action_values
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name == 'svc',
                    serve_state.replicas_table.c.replica_id ==
                    replica_id).values(**action_values))
            session.commit()

    # Ordinary and batch bookkeeping use different UPDATE builders.
    first = serve_state.get_replica_info_from_id('svc', 1)
    second = serve_state.get_replica_info_from_id('svc', 2)
    assert first is not None and second is not None
    first.version = 2
    second.version = 2
    assert serve_state.add_or_update_replica('svc',
                                             1,
                                             first,
                                             expected_replica_exists=True)
    assert serve_state.add_or_update_replicas('svc', [(2, second)],
                                              expected_replica_exists=True)

    # Paid-capacity admission is INSERT-only and cannot adopt an action row.
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            service_hash,
            3,
            _replica(3, version=2),
            pool_key=pool_key,
            priority=1,
            base_limit=1,
            max_limit=2,
            now=100.0,
            success_ttl_seconds=60.0,
            waiter_ttl_seconds=60.0,
            expected_controller_owner=None)

    # Reserved-fill admission is also INSERT-only. This exercises SQLite's
    # conditional INSERT ... SELECT path against a conflicting key.
    fill_pool_key = json.dumps(['test-context', 'a100'])
    with orm.Session(_mock_serve_db) as session:
        session.execute(serve_state.reserved_fill_claims_table.insert().values(
            service_name='svc',
            pool_key=fill_pool_key,
            weight=1,
            floor_replicas=1,
            gpus_per_replica=1,
            holdings_fill=1,
            heartbeat_ts=100.0))
        session.commit()
    fill_replica = _replica(4, version=2)
    fill_replica.reserved_fill = True
    fill_replica.reserved_fill_pool_key = fill_pool_key
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        serve_state.add_replica_if_round_epoch('svc',
                                               4,
                                               fill_replica,
                                               pool_key=fill_pool_key,
                                               expected_epoch=1)

    with orm.Session(_mock_serve_db) as session:
        rows = session.execute(
            sqlalchemy.select(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name ==
                'svc')).mappings().all()
    actual_by_replica = {row['replica_id']: row for row in rows}
    for replica_id, expected in expected_by_replica.items():
        assert {
            name: actual_by_replica[replica_id][name]
            for name in serve_state._ACTION_OWNED_REPLICA_COLUMNS
        } == expected


def test_replica_reads_do_not_use_pickle(_mock_serve_db, monkeypatch):
    info = _replica(1)
    info.resources_override = {
        'cloud': clouds.AWS(),
        'region': 'us-east-1',
        'use_spot': True,
    }
    serve_state.add_or_update_replica('svc', 1, info)
    monkeypatch.setattr(
        serve_state.pickle, 'loads',
        lambda _: pytest.fail('replica read attempted pickle fallback'))

    restored = serve_state.get_replica_info_from_id('svc', 1)

    assert restored is not None
    assert restored.to_storage_dict() == info.to_storage_dict()
    assert restored.resources_override['cloud'] == 'AWS'


def test_replica_status_counts_are_grouped_in_sql(_mock_serve_db):
    ready = _replica(1)
    ready.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    ready.status_property.service_ready_now = True
    serve_state.add_or_update_replicas('svc', [(1, ready), (2, _replica(2))])

    with _count_sql_statements(_mock_serve_db) as counts:
        status_counts = serve_state.get_replica_status_counts('svc')

    assert counts['n'] == 1
    assert status_counts == {'PENDING': 1, 'READY': 1}


def test_ready_replica_infos_exclude_terminal_history(_mock_serve_db):
    ready = _replica(1)
    ready.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    ready.status_property.service_ready_now = True
    pending = _replica(2)
    failed = _replica(3)
    failed.status_property.sky_launch_status = common_utils.ProcessStatus.FAILED
    serve_state.add_or_update_replicas('svc', [(1, ready), (2, pending),
                                               (3, failed)])

    with _count_sql_statements(_mock_serve_db) as counts:
        infos = serve_state.get_ready_replica_infos('svc')

    assert counts['n'] == 1
    assert [info.replica_id for info in infos] == [1]


def test_replica_status_and_capacity_counts_use_compact_json(_mock_serve_db):
    ready = _replica(1)
    ready.planned_capacity = 8
    ready.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    ready.status_property.service_ready_now = True
    pending = _replica(2)
    pending.planned_capacity = 4
    serve_state.add_or_update_replicas('svc', [(1, ready), (2, pending)])

    with _count_sql_statements(_mock_serve_db) as counts:
        status_counts, capacity_counts = (
            serve_state.get_replica_status_and_capacity_counts('svc'))

    assert counts['n'] == 1
    assert status_counts == {'PENDING': 1, 'READY': 1}
    assert capacity_counts == {'PENDING': 4, 'READY': 8}


def test_replica_json_migration_backfills_legacy_pickle(tmp_path, monkeypatch):
    engine = create_engine(f'sqlite:///{tmp_path / "legacy-serve.db"}')
    legacy_metadata = sqlalchemy.MetaData()
    legacy_replicas = sqlalchemy.Table(
        'replicas', legacy_metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('replica_info', sqlalchemy.LargeBinary))
    version_table = sqlalchemy.Table(
        'alembic_version_serve_state_db', legacy_metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True))
    legacy_metadata.create_all(engine)
    historical_location = {
        'cloud': 'Kubernetes',
        'region': 'prod_research_cluster_eks',
        'zone': None,
        'accelerators': {
            'A100-80GB': 1,
        },
        'use_spot': False,
        'image_id': {
            None: 'docker:example.invalid/boltz:model',
        },
        'disk_tier': None,
        'ephemeral_storage': 20,
    }
    legacy_blob = _genuine_pre_json_replica_pickle(
        replica_id=3,
        cluster_name='legacy-cluster',
        version=2,
        ready=True,
        location=historical_location)
    with engine.begin() as connection:
        connection.execute(legacy_replicas.insert().values(
            service_name='svc', replica_id=3, replica_info=legacy_blob))
        connection.execute(version_table.insert().values(version_num='009'))

    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         '010')
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)

    restored = serve_state.get_replica_info_from_id('svc', 3)
    assert restored is not None
    assert restored.to_storage_dict()['replica_info_version'] == 18
    assert restored.cluster_name == 'legacy-cluster'
    assert restored.location == historical_location
    assert restored.resources_override == historical_location
    assert restored.first_consecutive_failure_time == 42.0
    assert serve_state.get_replica_status_counts('svc') == {'READY': 1}


def _genuine_pre_json_replica_pickle(
        *,
        replica_id: int,
        cluster_name: str,
        version: int,
        record_version: int = 6,
        ready: bool = False,
        location: dict | None = None,
        legacy_process_status_identity: bool = False) -> bytes:
    """Build an exact historical state shape from before JSON authority."""
    source = _replica(3, cluster_name='legacy-cluster', version=2)
    source.replica_id = replica_id
    source.cluster_name = cluster_name
    source.version = version
    source._version = record_version
    if ready:
        source.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        source.status_property.service_ready_now = True
    if location is not None:
        source.location = copy.deepcopy(location)
        source.resources_override = copy.deepcopy(location)
    historical_replica_fields = {
        '_version',
        'replica_id',
        'cluster_name',
        'version',
        'replica_port',
        'created_at',
        'first_not_ready_time',
        'status_property',
        'is_spot',
        'location',
        'resources_override',
        'reserved_fill',
        'cost_rebalance_for_replica_id',
    }
    if record_version < 7:
        source.consecutive_failure_times = [42.0, 43.0]
        historical_replica_fields.add('consecutive_failure_times')
    else:
        source.first_consecutive_failure_time = 42.0
        historical_replica_fields.add('first_consecutive_failure_time')
    if record_version >= 8:
        source.planned_capacity = 4
        historical_replica_fields.add('planned_capacity')
    if record_version >= 9:
        source.unknown_capacity_replacement = True
        historical_replica_fields.add('unknown_capacity_replacement')
    if record_version >= 10:
        source.logical_bridge_capacity_verified = True
        historical_replica_fields.add('logical_bridge_capacity_verified')
    if record_version >= 11:
        source.is_zero_cost = True
        source.cost_rebalance_for_replica_id = 9
        source.resources_override = {
            'cloud': clouds.AWS(),
            'image_id': {
                None: 'ami-historical',
            },
        }
        historical_replica_fields.add('is_zero_cost')
    for field in set(vars(source)) - historical_replica_fields:
        delattr(source, field)
    historical_status_fields = {
        'sky_launch_status',
        'user_app_failed',
        'service_ready_now',
        'first_ready_time',
        'sky_down_status',
        'is_scale_down',
        'preempted',
        'purged',
        'failed_spot_availability',
        'drain_cap_seconds',
        'wait_for_idle_before_termination',
    }
    if record_version >= 11:
        source.status_property.drain_started_at = 100.0
        source.status_property.wait_for_idle_before_termination = True
        source.status_property.logical_retirement_version = 3
        source.status_property.logical_retirement_controller_epoch = 'epoch'
        source.status_property.logical_retirement_generation = 4
        source.status_property.logical_retirement_target_capacity = 5
        source.status_property.logical_retirement_confirmed_generation = 6
        source.status_property.logical_retirement_bounded_deadline = True
        source.status_property.logical_retirement_committed = True
        historical_status_fields.update({
            'drain_started_at',
            'logical_retirement_version',
            'logical_retirement_controller_epoch',
            'logical_retirement_generation',
            'logical_retirement_target_capacity',
            'logical_retirement_confirmed_generation',
            'logical_retirement_bounded_deadline',
            'logical_retirement_committed',
        })
    for field in set(vars(source.status_property)) - historical_status_fields:
        delattr(source.status_property, field)
    original_process_status = replica_managers.ProcessStatus
    if legacy_process_status_identity:
        historical_process_status = enum.Enum(
            'ProcessStatus', {
                status.value: status.value
                for status in common_utils.ProcessStatus
            },
            module='sky.serve.replica_managers')
        replica_managers.ProcessStatus = historical_process_status
        launch_status = source.status_property.sky_launch_status
        down_status = source.status_property.sky_down_status
        source.status_property.sky_launch_status = historical_process_status(
            launch_status.value)
        source.status_property.sky_down_status = (historical_process_status(
            down_status.value) if down_status is not None else None)
    try:
        payload = pickle.dumps(source)
    finally:
        replica_managers.ProcessStatus = original_process_status
    assert b'sky.serve.replica_managers' in payload
    assert b'ReplicaInfo' in payload
    assert b'ReplicaStatusProperty' in payload
    if legacy_process_status_identity:
        assert b'ProcessStatus' in payload
        assert b'sky.utils.common_utils' not in payload
    return payload


_FORBIDDEN_PICKLE_CALLS: list[bool] = []


def _mark_forbidden_pickle_call() -> None:
    _FORBIDDEN_PICKLE_CALLS.append(True)


class _ForbiddenReduce:

    def __reduce__(self):
        return _mark_forbidden_pickle_call, ()


def test_replica_json_migration_owns_pre_v17_projection(monkeypatch):
    payload = _genuine_pre_json_replica_pickle(
        replica_id=3,
        cluster_name='legacy-cluster',
        version=2,
        legacy_process_status_identity=True)

    def _fail_live_decoder(*_args, **_kwargs):
        raise AssertionError('live ReplicaInfo decoder was called')

    def _fail_live_serializer(*_args, **_kwargs):
        raise AssertionError('live ReplicaInfo serializer was called')

    monkeypatch.setattr(replica_managers.ReplicaInfo, '__setstate__',
                        _fail_live_decoder)
    monkeypatch.setattr(replica_managers.ReplicaInfo, 'to_storage_dict',
                        _fail_live_serializer)

    legacy = legacy_replica_pickle.load_pre_json_replica(payload)
    values = legacy_replica_pickle.frozen_replica_row_values(legacy,
                                                             maximum_version=7)

    state = values['replica_state']
    assert vars(legacy)['_version'] == 6
    assert state['replica_info_version'] == 18
    assert set(state) == set(replica_info_lib._REPLICA_INFO_STORAGE_FIELDS)
    assert set(state['status_property']) == set(
        replica_info_lib._REPLICA_STATUS_PROPERTY_FIELDS)
    assert state['first_consecutive_failure_time'] == 42.0
    assert state['system_recovery_disposition'] == 'ORDINARY'
    assert state['logical_bridge_capacity_verified'] is False
    assert values['status'] == serve_state.ReplicaStatus.PENDING.value


def test_replica_json_migration_rejects_newer_and_executable_pickles():
    v11_payload = _genuine_pre_json_replica_pickle(replica_id=3,
                                                   cluster_name='preview',
                                                   version=2,
                                                   record_version=11)
    v11 = legacy_replica_pickle.load_pre_json_replica(v11_payload)
    with pytest.raises(legacy_replica_pickle.LegacyReplicaPickleError,
                       match='exceeds this migration boundary'):
        legacy_replica_pickle.frozen_replica_row_values(v11, maximum_version=7)

    _FORBIDDEN_PICKLE_CALLS.clear()
    payload = pickle.dumps(_ForbiddenReduce())
    with pytest.raises(legacy_replica_pickle.LegacyReplicaPickleError,
                       match='forbidden global'):
        legacy_replica_pickle.load_pre_json_replica(payload)
    assert not _FORBIDDEN_PICKLE_CALLS


def test_replica_json_migration_handles_fresh_database(tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "fresh-serve.db"}')

    serve_state.create_table(engine)

    inspector = sqlalchemy.inspect(engine)
    columns = {column['name'] for column in inspector.get_columns('replicas')}
    assert {'replica_state_version', 'status', 'replica_state'} <= columns
    service_columns = {
        column['name'] for column in inspector.get_columns('services')
    }
    assert 'logical_replica_semantics' in service_columns
    assert 'demand_capacity_observations' in inspector.get_table_names()
    indexes = {
        index['name']: index['column_names']
        for index in inspector.get_indexes('replicas')
    }
    assert indexes['replicas_service_version_idx'] == [
        'service_name', 'version'
    ]


def test_replica_json_migration_owns_retired_pickle_column_on_fresh_replay(
        tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "future-fresh.db"}')
    metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'replicas',
        metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
    )
    version_table = sqlalchemy.Table(
        'alembic_version_serve_state_db',
        metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True),
    )
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(version_table.insert().values(version_num='009'))

    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         '010')

    columns = {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('replicas')
    }
    assert {
        'replica_info',
        'replica_state_version',
        'status',
        'replica_state',
    } <= columns
    with engine.connect() as connection:
        assert connection.execute(sqlalchemy.select(
            version_table.c.version_num)).scalar_one() == '010'


def test_replica_index_migration_converges_predecessor_stamped_legacy_state(
        tmp_path):
    """Revision 026 repairs previews stamped past replica JSON revision 010."""
    engine = create_engine(f'sqlite:///{tmp_path / "preview-serve.db"}')
    metadata = sqlalchemy.MetaData()
    legacy_replicas = sqlalchemy.Table(
        'replicas',
        metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('replica_info', sqlalchemy.LargeBinary),
    )
    version_table = sqlalchemy.Table(
        'alembic_version_serve_state_db',
        metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True),
    )
    metadata.create_all(engine)
    legacy = _genuine_pre_json_replica_pickle(replica_id=1,
                                              cluster_name='svc-1',
                                              version=3,
                                              record_version=11)
    with engine.begin() as connection:
        connection.execute(legacy_replicas.insert().values(service_name='svc',
                                                           replica_id=1,
                                                           replica_info=legacy))
        connection.execute(version_table.insert().values(version_num='025'))

    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         '026')

    inspector = sqlalchemy.inspect(engine)
    columns = {column['name'] for column in inspector.get_columns('replicas')}
    assert {
        'replica_state_version',
        'status',
        'sky_down_status',
        'version',
        'cluster_name',
        'created_at',
        'is_spot',
        'replica_state',
    } <= columns
    indexes = {
        index['name']: index['column_names']
        for index in inspector.get_indexes('replicas')
    }
    assert indexes['replicas_service_status_idx'] == ['service_name', 'status']
    assert indexes['replicas_service_version_idx'] == [
        'service_name', 'version'
    ]
    replicas = sqlalchemy.Table('replicas',
                                sqlalchemy.MetaData(),
                                autoload_with=engine)
    with engine.connect() as connection:
        row = connection.execute(sqlalchemy.select(replicas)).one()._mapping
        revision = connection.execute(
            sqlalchemy.select(version_table.c.version_num)).scalar_one()
    assert row['version'] == 3
    assert row['cluster_name'] == 'svc-1'
    assert row['status'] == serve_state.ReplicaStatus.PENDING.value
    assert row['replica_state_version'] == 1
    assert row['replica_state'] is not None
    state = row['replica_state']
    assert state['replica_info_version'] == 18
    assert state['planned_capacity'] == 4
    assert state['unknown_capacity_replacement'] is True
    assert state['logical_bridge_capacity_verified'] is True
    assert state['is_zero_cost'] is True
    assert state['cost_rebalance_for_replica_id'] == 9
    assert state['resources_override'] == {
        'cloud': 'AWS',
        'image_id': [[None, 'ami-historical']],
    }
    assert state['status_property'] == {
        'sky_launch_status': 'SCHEDULED',
        'user_app_failed': False,
        'service_ready_now': False,
        'first_ready_time': None,
        'sky_down_status': None,
        'is_scale_down': False,
        'preempted': False,
        'purged': False,
        'failed_spot_availability': False,
        'drain_cap_seconds': None,
        'drain_started_at': 100.0,
        'wait_for_idle_before_termination': True,
        'logical_retirement_version': 3,
        'logical_retirement_controller_epoch': 'epoch',
        'logical_retirement_generation': 4,
        'logical_retirement_target_capacity': 5,
        'logical_retirement_confirmed_generation': 6,
        'logical_retirement_bounded_deadline': True,
        'logical_retirement_committed': True,
    }
    assert revision == '026'


def test_replica_index_migration_fails_closed_without_reconstructable_state(
        tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "invalid-preview.db"}')
    metadata = sqlalchemy.MetaData()
    replicas = sqlalchemy.Table(
        'replicas',
        metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('version', sqlalchemy.Integer),
    )
    version_table = sqlalchemy.Table(
        'alembic_version_serve_state_db',
        metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True),
    )
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(replicas.insert().values(service_name='svc',
                                                    replica_id=1,
                                                    version=3))
        connection.execute(version_table.insert().values(version_num='025'))

    with pytest.raises(RuntimeError, match='without legacy replica_info'):
        migration_utils.safe_alembic_upgrade(engine,
                                             migration_utils.SERVE_DB_NAME,
                                             '026')

    with engine.connect() as connection:
        revision = connection.execute(
            sqlalchemy.select(version_table.c.version_num)).scalar_one()
    assert revision == '025'


def test_replica_index_migration_rejects_same_name_with_wrong_columns(tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "wrong-index.db"}')
    metadata = sqlalchemy.MetaData()
    replicas = sqlalchemy.Table(
        'replicas',
        metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
    )
    version_table = sqlalchemy.Table(
        'alembic_version_serve_state_db',
        metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True),
    )
    sqlalchemy.Index('replicas_service_version_idx', replicas.c.service_name,
                     replicas.c.replica_id)
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(version_table.insert().values(version_num='025'))

    with pytest.raises(RuntimeError, match='unexpected shape'):
        migration_utils.safe_alembic_upgrade(engine,
                                             migration_utils.SERVE_DB_NAME,
                                             '026')

    with engine.connect() as connection:
        revision = connection.execute(
            sqlalchemy.select(version_table.c.version_num)).scalar_one()
    assert revision == '025'


def test_elected_version_migration_backfills_latest_committed_version(
        tmp_path, monkeypatch):
    engine = create_engine(f'sqlite:///{tmp_path / "old-serve.db"}')
    monkeypatch.setattr(migration_utils, 'SERVE_NON_POSTGRES_VERSION', '013')
    serve_state.create_table(engine)
    legacy_metadata = sqlalchemy.MetaData()
    legacy_services = sqlalchemy.Table('services',
                                       legacy_metadata,
                                       autoload_with=engine)
    legacy_version_specs = sqlalchemy.Table('version_specs',
                                            legacy_metadata,
                                            autoload_with=engine)
    with engine.begin() as connection:
        connection.execute(legacy_services.insert().values(name='svc',
                                                           current_version=1))
        connection.execute(legacy_version_specs.insert(), [{
            'service_name': 'svc',
            'version': 1,
            'spec': pickle.dumps(None),
            'yaml_content': 'yaml: v1',
        }, {
            'service_name': 'svc',
            'version': 2,
            'spec': pickle.dumps(None),
            'yaml_content': 'yaml: v2',
        }, {
            'service_name': 'svc',
            'version': 3,
            'spec': pickle.dumps(None),
            'yaml_content': None,
        }])

    monkeypatch.setattr(migration_utils, 'SERVE_NON_POSTGRES_VERSION', '014')
    serve_state.create_table(engine)

    with engine.connect() as connection:
        current_version = connection.execute(
            sqlalchemy.select(
                serve_state.services_table.c.current_version).where(
                    serve_state.services_table.c.name == 'svc')).scalar_one()
    assert current_version == 2


def test_version_provenance_migration_adds_nullable_columns(
        tmp_path, monkeypatch):
    engine = create_engine(f'sqlite:///{tmp_path / "old-serve.db"}')
    legacy_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'version_specs',
        legacy_metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
        sqlalchemy.Column('yaml_content', sqlalchemy.Text),
    )
    legacy_metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('CREATE TABLE alembic_version_serve_state_db '
                            '(version_num VARCHAR(32) NOT NULL)'))
        connection.execute(
            sqlalchemy.text(
                "INSERT INTO alembic_version_serve_state_db VALUES ('014')"))

    monkeypatch.setattr(migration_utils, 'SERVE_NON_POSTGRES_VERSION', '015')
    serve_state.create_table(engine)

    columns = {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('version_specs')
    }
    assert {'created_at', 'created_by'} <= columns


def test_submitted_version_yaml_migration_adds_nullable_column(
        tmp_path, monkeypatch):
    engine = create_engine(f'sqlite:///{tmp_path / "old-serve.db"}')
    legacy_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'version_specs',
        legacy_metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
        sqlalchemy.Column('yaml_content', sqlalchemy.Text),
        sqlalchemy.Column('created_at', sqlalchemy.Float),
        sqlalchemy.Column('created_by', sqlalchemy.Text),
    )
    legacy_metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('CREATE TABLE alembic_version_serve_state_db '
                            '(version_num VARCHAR(32) NOT NULL)'))
        connection.execute(
            sqlalchemy.text(
                "INSERT INTO alembic_version_serve_state_db VALUES ('016')"))

    monkeypatch.setattr(migration_utils, 'SERVE_NON_POSTGRES_VERSION', '017')
    serve_state.create_table(engine)

    columns = {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('version_specs')
    }
    assert 'submitted_yaml_content' in columns


def test_placement_catalog_migration_adds_nullable_column(
        tmp_path, monkeypatch):
    # Regression guard for PR #906: migration 028 adds the nullable
    # `placement_catalog` column via ALTER TABLE. The durable round-trip tests
    # (test_version_placement_catalog_persists_and_backfills_once) build the
    # schema with `create_all`, which already includes the column, so they
    # never exercise the upgrade path on a database that predates it. This
    # pins that an on-disk service DB stamped at 027 gains the column and that
    # a pre-existing (legacy) row reads back as SQL NULL, matching 016->017.
    engine = create_engine(f'sqlite:///{tmp_path / "old-serve.db"}')
    legacy_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'version_specs',
        legacy_metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
        sqlalchemy.Column('yaml_content', sqlalchemy.Text),
    )
    legacy_metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('CREATE TABLE alembic_version_serve_state_db '
                            '(version_num VARCHAR(32) NOT NULL)'))
        connection.execute(
            sqlalchemy.text(
                "INSERT INTO alembic_version_serve_state_db VALUES ('027')"))
        connection.execute(
            sqlalchemy.text('INSERT INTO version_specs '
                            '(service_name, version, yaml_content) '
                            "VALUES ('legacy-svc', 1, 'yaml: v1')"))

    monkeypatch.setattr(migration_utils, 'SERVE_NON_POSTGRES_VERSION', '028')
    serve_state.create_table(engine)

    columns = {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('version_specs')
    }
    assert 'placement_catalog' in columns
    with engine.begin() as connection:
        legacy_catalog = connection.execute(
            sqlalchemy.text('SELECT placement_catalog FROM version_specs '
                            'WHERE service_name = :name'), {
                                'name': 'legacy-svc'
                            }).scalar()
    assert legacy_catalog is None


def test_controller_config_migration_adds_nullable_applied_receipt(
        tmp_path, monkeypatch):
    engine = create_engine(f'sqlite:///{tmp_path / "old-serve.db"}')
    legacy_metadata = sqlalchemy.MetaData()
    legacy_versions = sqlalchemy.Table(
        'version_specs',
        legacy_metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
        sqlalchemy.Column('yaml_content', sqlalchemy.Text),
    )
    legacy_metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('CREATE TABLE alembic_version_serve_state_db '
                            '(version_num VARCHAR(32) NOT NULL)'))
        connection.execute(
            sqlalchemy.text(
                "INSERT INTO alembic_version_serve_state_db VALUES ('035')"))
        connection.execute(legacy_versions.insert().values(
            service_name='legacy-svc',
            version=7,
            spec=b'opaque',
            yaml_content='yaml: legacy'))

    monkeypatch.setattr(migration_utils, 'SERVE_NON_POSTGRES_VERSION', '036')
    serve_state.create_table(engine)

    columns = {
        column['name']: column
        for column in sqlalchemy.inspect(engine).get_columns('version_specs')
    }
    assert {
        'controller_config', 'controller_config_digest',
        'controller_config_snapshot_id', 'controller_applied_at'
    } <= columns.keys()
    assert all(columns[name]['nullable']
               for name in ('controller_config', 'controller_config_digest',
                            'controller_config_snapshot_id',
                            'controller_applied_at'))
    with engine.connect() as connection:
        migrated = connection.execute(
            sqlalchemy.text('SELECT controller_config, '
                            'controller_config_digest, '
                            'controller_config_snapshot_id, '
                            'controller_applied_at FROM version_specs '
                            "WHERE service_name = 'legacy-svc'")).one()
    assert migrated == (None, None, None, None)


def test_service_version_terminal_lookup_uses_only_exact_history_keys(
        _mock_serve_db):
    engine = _mock_serve_db
    assert _add_minimal_service('svc-terminal',
                                service_hash='incarnation-a') is True
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.insert(), [{
            'service_name': 'svc-terminal',
            'version': version,
            'spec': pickle.dumps(None),
            'yaml_content': f'yaml: v{version}',
        } for version in range(2, 2001)])
        connection.execute(serve_state.replicas_table.insert(), [{
            'service_name': 'svc-terminal',
            'replica_id': replica,
            'replica_info': b'opaque',
            'version': replica + 1,
        } for replica in range(1, 2000)])
    statements = []

    def record(_connection, _cursor, statement, _parameters, _context,
               _executemany):
        statements.append(statement)

    sqlalchemy.event.listen(engine, 'before_cursor_execute', record)
    try:
        result = serve_state.get_service_version_terminal_states([
            ('svc-terminal', 2000, 'incarnation-a'),
            ('missing-service', 1, 'missing-incarnation'),
        ])
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute', record)

    assert result == {
        ('svc-terminal', 2000, 'incarnation-a'): False,
        ('missing-service', 1, 'missing-incarnation'): True,
    }
    assert len(statements) == 1
    probe_statement = next(statement for statement in statements
                           if 'wanted_service_versions' in statement)
    assert 'WITH wanted_service_versions(service_name, version) AS' in (
        probe_statement)
    assert probe_statement.count('EXISTS (SELECT') == 3
    assert 'FROM version_specs' in probe_statement
    assert 'FROM replicas' in probe_statement
    assert 'SELECT replicas.service_name, replicas.version' not in (
        probe_statement)


def test_service_version_terminal_state_retains_scale_zero_applied_fallback(
        _mock_serve_db):
    assert _add_minimal_service('svc-terminal-fallback',
                                service_hash='incarnation-a',
                                spec=_service_spec('spec-v1'))
    serve_state.set_service_status_and_active_versions(
        'svc-terminal-fallback',
        serve_state.ServiceStatus.READY,
        active_versions=[])
    assert serve_state.add_or_update_version(
        'svc-terminal-fallback', 2, _service_spec('spec-v2'),
        'yaml: v2') is serve_state.VersionCommitResult.COMMITTED
    assert serve_state.quarantine_version('svc-terminal-fallback', 2,
                                          'never applied')

    with _count_sql_statements(_mock_serve_db) as counts:
        result = serve_state.get_service_version_terminal_states([
            ('svc-terminal-fallback', 1, 'incarnation-a'),
        ])

    assert counts['n'] == 1
    assert result == {
        ('svc-terminal-fallback', 1, 'incarnation-a'): False,
    }


def test_service_version_terminal_state_retains_all_applied_history(
        _mock_serve_db):
    owner = (12345, None)
    assert _add_minimal_service('svc-applied-history',
                                service_hash='incarnation-a',
                                spec=_service_spec('spec-v1'))
    assert serve_state.add_or_update_version(
        'svc-applied-history', 2, _service_spec('spec-v2'),
        'yaml: v2') is serve_state.VersionCommitResult.COMMITTED
    assert serve_state.mark_version_controller_applied(
        'svc-applied-history',
        2,
        expected_service_hash='incarnation-a',
        expected_controller_owner=owner)
    assert serve_state.add_or_update_version(
        'svc-applied-history', 3, _service_spec('spec-v3'),
        'yaml: v3') is serve_state.VersionCommitResult.COMMITTED
    serve_state.set_service_status_and_active_versions(
        'svc-applied-history',
        serve_state.ServiceStatus.READY,
        active_versions=[])

    result = serve_state.get_service_version_terminal_states([
        ('svc-applied-history', 1, 'incarnation-a'),
        ('svc-applied-history', 2, 'incarnation-a'),
    ])

    assert result == {
        ('svc-applied-history', 1, 'incarnation-a'): False,
        ('svc-applied-history', 2, 'incarnation-a'): False,
    }


def test_get_specs_batches_requested_versions_in_one_query(_mock_serve_db):
    initial_spec = _service_spec(graceful_drain_async_occupancy=False)
    assert _add_minimal_service('svc-specs', spec=initial_spec) is True
    serve_state.add_or_update_version(
        'svc-specs',
        1,
        _service_spec(graceful_drain_async_occupancy=False),
        'yaml: v1',
    )
    serve_state.add_or_update_version(
        'svc-specs',
        2,
        _service_spec(graceful_drain_async_occupancy=True),
        'yaml: v2',
    )

    with _count_sql_statements(_mock_serve_db) as counts:
        specs = serve_state.get_specs('svc-specs', [2, 1, 2, 3])

    assert counts['n'] == 1, counts
    assert set(specs) == {1, 2}
    assert specs[1].graceful_drain_async_occupancy is False
    assert specs[2].graceful_drain_async_occupancy is True


def test_committed_version_content_is_immutable_and_retryable(_mock_serve_db):
    assert _add_minimal_service('svc-immutable') is True
    assert serve_state.add_version('svc-immutable') == 2
    original_spec = _service_spec('original')
    result = serve_state.add_or_update_version('svc-immutable', 2,
                                               original_spec, 'value: original')
    assert result is serve_state.VersionCommitResult.COMMITTED
    assert _read_row(_mock_serve_db, 'svc-immutable')['current_version'] == 2
    assert serve_state.get_version_yaml_contents('svc-immutable') == {
        1: 'yaml: v1',
        2: 'value: original',
    }
    original_row = _read_version_row(_mock_serve_db, 'svc-immutable', 2)

    # A lost-response retry is idempotent and does not rewrite either stored
    # payload, even if the caller reconstructed a fresh spec object.
    retry_result = serve_state.add_or_update_version('svc-immutable', 2,
                                                     _service_spec('original'),
                                                     'value: original')
    assert retry_result is serve_state.VersionCommitResult.IDEMPOTENT_RETRY
    assert _read_version_row(_mock_serve_db, 'svc-immutable', 2) == original_row
    conflict_result = serve_state.add_or_update_version(
        'svc-immutable', 2, _service_spec('different'), 'value: different')
    assert conflict_result is serve_state.VersionCommitResult.CONTENT_CONFLICT
    assert _read_version_row(_mock_serve_db, 'svc-immutable', 2) == original_row


def test_identical_projection_retry_is_idempotent_at_db_boundary(
        _mock_serve_db):
    service_name = 'svc-projection-retry'
    yaml_content = 'value: projected'
    projections = _placement_projection_args()
    assert _add_minimal_service(service_name, spec=_v2_service_spec('initial'))
    assert serve_state.add_version(service_name) == 2
    assert (serve_state.add_or_update_version(service_name, 2,
                                              _v2_service_spec('projected'),
                                              yaml_content, **projections)
            is serve_state.VersionCommitResult.COMMITTED)
    row_before = _read_version_row(_mock_serve_db, service_name, 2)

    assert (serve_state.add_or_update_version(
        service_name, 2, _v2_service_spec('rebuilt-on-retry'), yaml_content,
        **copy.deepcopy(projections))
            is serve_state.VersionCommitResult.IDEMPOTENT_RETRY)
    assert _read_version_row(_mock_serve_db, service_name, 2) == row_before


@pytest.mark.parametrize(
    'historical_protocol_version',
    range(kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION - 2,
          kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION))
def test_fresh_writes_reject_historical_worker_projections(
        _mock_serve_db, historical_protocol_version):
    projections = _placement_projection_args(
        worker_projection_version=historical_protocol_version)
    error = (f'protocol version {historical_protocol_version} does not '
             'satisfy required version '
             f'{kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION}')

    with pytest.raises(ValueError, match=error):
        _add_minimal_service(f'svc-v{historical_protocol_version}-registration',
                             spec=_v2_service_spec('initial'),
                             **projections)
    registration_name = f'svc-v{historical_protocol_version}-registration'
    assert _read_row(_mock_serve_db, registration_name) is None
    assert _read_version_row(_mock_serve_db, registration_name, 1) is None

    service_name = f'svc-v{historical_protocol_version}-version'
    assert _add_minimal_service(service_name, spec=_v2_service_spec('initial'))
    assert serve_state.add_version(service_name) == 2
    placeholder_before = _read_version_row(_mock_serve_db, service_name, 2)
    with pytest.raises(ValueError, match=error):
        serve_state.add_or_update_version(service_name, 2,
                                          _v2_service_spec('placeholder-fill'),
                                          'yaml: v2', **projections)
    assert _read_version_row(_mock_serve_db, service_name,
                             2) == placeholder_before

    with pytest.raises(ValueError, match=error):
        serve_state.add_or_update_version(service_name, 3,
                                          _v2_service_spec('direct-insert'),
                                          'yaml: v3', **projections)
    assert _read_version_row(_mock_serve_db, service_name, 3) is None


@pytest.mark.parametrize(
    'historical_protocol_version',
    range(kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION - 2,
          kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION))
def test_identical_historical_projection_retry_remains_idempotent(
        _mock_serve_db, historical_protocol_version):
    service_name = f'svc-v{historical_protocol_version}-projection-retry'
    yaml_content = 'value: projected'
    assert _add_minimal_service(service_name, spec=_v2_service_spec('initial'))
    assert serve_state.add_version(service_name) == 2
    current_projections = _placement_projection_args()
    assert (serve_state.add_or_update_version(service_name, 2,
                                              _v2_service_spec('projected'),
                                              yaml_content,
                                              **current_projections)
            is serve_state.VersionCommitResult.COMMITTED)

    # Simulate a version committed by one of the two previous protocols.  The
    # current controller may settle an exact lost-response retry, but must not
    # grant these historical bytes fresh write or provider authority.
    historical_projections = _placement_projection_args(
        worker_projection_version=historical_protocol_version)
    with orm.Session(_mock_serve_db) as session:
        session.execute(
            sqlalchemy.update(serve_state.version_specs_table).where(
                serve_state.version_specs_table.c.service_name == service_name,
                serve_state.version_specs_table.c.version == 2).values(
                    worker_placement_projections=historical_projections[
                        'worker_placement_projections']))
        session.commit()
    row_before = _read_version_row(_mock_serve_db, service_name, 2)

    assert (serve_state.add_or_update_version(
        service_name, 2, _v2_service_spec('rebuilt-on-retry'), yaml_content,
        **historical_projections)
            is serve_state.VersionCommitResult.IDEMPOTENT_RETRY)
    assert _read_version_row(_mock_serve_db, service_name, 2) == row_before


@pytest.mark.parametrize('projection_name', [
    'controller_job_projection',
    'controller_work_cache',
    'worker_placement_projections',
])
def test_projection_drift_conflicts_and_preserves_committed_row(
        _mock_serve_db, projection_name):
    service_name = f'svc-projection-conflict-{projection_name}'
    yaml_content = 'value: projected'
    projections = _placement_projection_args()
    assert _add_minimal_service(service_name, spec=_v2_service_spec('initial'))
    assert serve_state.add_version(service_name) == 2
    assert (serve_state.add_or_update_version(service_name, 2,
                                              _v2_service_spec('projected'),
                                              yaml_content, **projections)
            is serve_state.VersionCommitResult.COMMITTED)
    row_before = _read_version_row(_mock_serve_db, service_name, 2)
    changed = copy.deepcopy(projections)
    if projection_name == 'controller_job_projection':
        changed[projection_name]['namespace'] = 'different-controller-system'
    elif projection_name == 'controller_work_cache':
        changed[projection_name]['size_limit_bytes'] = 300
    else:
        changed[projection_name][0]['accelerator_name'] = 'H100'

    assert (serve_state.add_or_update_version(
        service_name, 2, _v2_service_spec('rebuilt-on-retry'), yaml_content,
        **changed) is serve_state.VersionCommitResult.CONTENT_CONFLICT)
    assert _read_version_row(_mock_serve_db, service_name, 2) == row_before


def test_identical_yaml_retry_cannot_backfill_legacy_null_projections(
        _mock_serve_db):
    service_name = 'svc-legacy-null-projections'
    yaml_content = 'value: legacy'
    assert _add_minimal_service(service_name, spec=_v2_service_spec('initial'))
    assert serve_state.add_version(service_name) == 2
    assert (serve_state.add_or_update_version(service_name, 2,
                                              _v2_service_spec('legacy'),
                                              yaml_content)
            is serve_state.VersionCommitResult.COMMITTED)
    row_before = _read_version_row(_mock_serve_db, service_name, 2)
    assert all(row_before[column] is None for column in (
        'controller_job_projection',
        'controller_work_cache',
        'worker_placement_projections',
    ))

    assert (serve_state.add_or_update_version(service_name, 2,
                                              _v2_service_spec('legacy-retry'),
                                              yaml_content,
                                              **_placement_projection_args())
            is serve_state.VersionCommitResult.CONTENT_CONFLICT)
    assert _read_version_row(_mock_serve_db, service_name, 2) == row_before


def test_new_service_and_version_writes_require_raw_v2_state(_mock_serve_db):
    with pytest.raises(ValueError, match='mirror-free v2'):
        _add_minimal_service('svc-v1-registration',
                             spec=_v1_service_spec('legacy'))
    assert _read_row(_mock_serve_db, 'svc-v1-registration') is None
    assert _read_version_row(_mock_serve_db, 'svc-v1-registration', 1) is None

    assert _add_minimal_service('svc-v2-boundary',
                                spec=_v2_service_spec('initial'))
    initial_payload = _read_version_row(_mock_serve_db, 'svc-v2-boundary',
                                        1)['spec']
    assert placement_contract_normalization.analyze_spec_pickle(
        initial_payload).classification is (
            placement_contract_normalization.Classification.EXPLICIT_V2)

    assert serve_state.add_version('svc-v2-boundary') == 2
    placeholder_before = _read_version_row(_mock_serve_db, 'svc-v2-boundary', 2)
    with pytest.raises(ValueError, match='mirror-free v2'):
        serve_state.add_or_update_version('svc-v2-boundary', 2,
                                          _v1_service_spec('legacy-fill'),
                                          'yaml: legacy-fill')
    assert _read_version_row(_mock_serve_db, 'svc-v2-boundary',
                             2) == placeholder_before

    with pytest.raises(ValueError, match='mirror-free v2'):
        serve_state.add_or_update_version('svc-v2-boundary', 3,
                                          _v1_service_spec('legacy-insert'),
                                          'yaml: legacy-insert')
    assert _read_version_row(_mock_serve_db, 'svc-v2-boundary', 3) is None

    assert serve_state.add_or_update_version(
        'svc-v2-boundary', 2, _v2_service_spec('current'),
        'yaml: current') is serve_state.VersionCommitResult.COMMITTED
    committed_payload = _read_version_row(_mock_serve_db, 'svc-v2-boundary',
                                          2)['spec']
    assert placement_contract_normalization.analyze_spec_pickle(
        committed_payload).classification is (
            placement_contract_normalization.Classification.EXPLICIT_V2)


def test_identical_retry_preserves_existing_v1_bytes(_mock_serve_db):
    service_name = 'svc-v1-retry'
    yaml_content = 'yaml: legacy'
    assert _add_minimal_service(service_name, spec=_v2_service_spec('initial'))
    assert serve_state.add_or_update_version(
        service_name, 2, _v2_service_spec('committed'),
        yaml_content) is serve_state.VersionCommitResult.COMMITTED

    legacy_spec = _v1_service_spec('legacy-retry')
    legacy_bytes = placement_contract_normalization._serialize_raw_state(
        dict(legacy_spec.__dict__), 4)
    with orm.Session(_mock_serve_db) as session:
        session.execute(
            sqlalchemy.update(serve_state.version_specs_table).where(
                serve_state.version_specs_table.c.service_name == service_name,
                serve_state.version_specs_table.c.version == 2).values(
                    spec=legacy_bytes))
        session.commit()
    row_before = _read_version_row(_mock_serve_db, service_name, 2)

    assert serve_state.add_or_update_version(
        service_name, 2, legacy_spec,
        yaml_content) is serve_state.VersionCommitResult.IDEMPOTENT_RETRY
    row_after = _read_version_row(_mock_serve_db, service_name, 2)
    assert row_after == row_before
    assert row_after['spec'] == legacy_bytes


def test_retired_version_cannot_be_refilled_and_remains_cleanup_inventory(
        _mock_serve_db):
    service_name = 'svc-retired'
    retired_yaml = 'service:\n  pool: false\n'
    assert _add_minimal_service(service_name, spec=_service_spec('v1'))
    assert serve_state.add_or_update_version(
        service_name, 2, _service_spec('v2'),
        retired_yaml) is serve_state.VersionCommitResult.COMMITTED
    assert serve_state.add_or_update_version(
        service_name, 3, _service_spec('v3'),
        'yaml: v3') is serve_state.VersionCommitResult.COMMITTED
    run_id = uuid.uuid4()
    _insert_placement_normalization_run(_mock_serve_db, run_id)
    with orm.Session(_mock_serve_db) as session:
        session.execute(
            sqlalchemy.update(serve_state.version_specs_table).where(
                serve_state.version_specs_table.c.service_name == service_name,
                serve_state.version_specs_table.c.version == 2).values(
                    spec=pickle.dumps(None, protocol=4),
                    yaml_content=None,
                    retired_yaml_content=retired_yaml,
                    retired_at=10.0,
                    retirement_reason='test retirement',
                    retirement_run_id=run_id))
        session.commit()

    retired_before = _read_version_row(_mock_serve_db, service_name, 2)
    result = serve_state.add_or_update_version(service_name, 2,
                                               _service_spec('replacement'),
                                               'yaml: replacement')

    assert result is serve_state.VersionCommitResult.CONTENT_CONFLICT
    assert _read_version_row(_mock_serve_db, service_name, 2) == retired_before
    assert serve_state.get_yaml_contents(service_name, [2]) == {2: None}
    assert serve_state.get_version_yaml_contents(service_name) == {
        1: 'yaml: v1',
        2: retired_yaml,
        3: 'yaml: v3',
    }


def test_orphan_mode_uses_retired_yaml_without_reviving_live_yaml(
        _mock_serve_db):
    run_id = uuid.uuid4()
    _insert_placement_normalization_run(_mock_serve_db, run_id)
    with orm.Session(_mock_serve_db) as session:
        session.execute(serve_state.version_specs_table.insert().values(
            service_name='orphan-retired-pool',
            version=1,
            spec=pickle.dumps(None, protocol=4),
            yaml_content=None,
            retired_yaml_content='service:\n  pool: true\n',
            retired_at=10.0,
            retirement_reason='test retirement',
            retirement_run_id=run_id))
        session.commit()

    assert serve_state.get_orphaned_service_child_mode(
        'orphan-retired-pool') is True
    assert serve_state.get_yaml_contents('orphan-retired-pool', [1]) == {
        1: None,
    }


def test_initial_version_controller_config_persists_and_verifies(
        _mock_serve_db):
    snapshot = _config_snapshot(b'active_workspace: research\n')
    assert _add_minimal_service('svc-initial-config',
                                controller_config=snapshot[0],
                                controller_config_digest=snapshot[1],
                                controller_config_snapshot_id=snapshot[2])
    assert (serve_state.get_version_controller_config('svc-initial-config',
                                                      1) == snapshot)


def test_version_controller_config_retry_requires_exact_snapshot(
        _mock_serve_db):
    assert _add_minimal_service('svc-config-retry')
    assert serve_state.add_version('svc-config-retry') == 2
    snapshot = _config_snapshot(b'active_workspace: research\n')
    result = serve_state.add_or_update_version(
        'svc-config-retry',
        2,
        _service_spec('v2'),
        'value: v2',
        ha_recovery_script=_VERSIONED_HA_SCRIPT,
        controller_config=snapshot[0],
        controller_config_digest=snapshot[1],
        controller_config_snapshot_id=snapshot[2])
    assert result is serve_state.VersionCommitResult.COMMITTED
    original_row = _read_version_row(_mock_serve_db, 'svc-config-retry', 2)

    assert (serve_state.add_or_update_version(
        'svc-config-retry',
        2,
        _service_spec('v2'),
        'value: v2',
        controller_config=snapshot[0],
        controller_config_digest=snapshot[1],
        controller_config_snapshot_id=snapshot[2])
            is serve_state.VersionCommitResult.IDEMPOTENT_RETRY)
    assert (serve_state.add_or_update_version('svc-config-retry', 2,
                                              _service_spec('v2'), 'value: v2')
            is serve_state.VersionCommitResult.CONTENT_CONFLICT)
    different_snapshot = _config_snapshot(b'active_workspace: other\n', 'b')
    assert (serve_state.add_or_update_version(
        'svc-config-retry',
        2,
        _service_spec('v2'),
        'value: v2',
        controller_config=different_snapshot[0],
        controller_config_digest=different_snapshot[1],
        controller_config_snapshot_id=different_snapshot[2])
            is serve_state.VersionCommitResult.CONTENT_CONFLICT)
    assert _read_version_row(_mock_serve_db, 'svc-config-retry',
                             2) == (original_row)


def test_config_aware_commit_backfills_only_null_prior_versions(_mock_serve_db):
    initial_snapshot = _config_snapshot(b'active_workspace: initial\n', '1')
    assert _add_minimal_service(
        'svc-config-backfill',
        service_hash='incarnation-a',
        controller_config=initial_snapshot[0],
        controller_config_digest=initial_snapshot[1],
        controller_config_snapshot_id=initial_snapshot[2])
    assert serve_state.add_version('svc-config-backfill') == 2
    assert (serve_state.add_or_update_version('svc-config-backfill', 2,
                                              _service_spec('legacy'),
                                              'value: legacy')
            is serve_state.VersionCommitResult.COMMITTED)
    assert serve_state.add_version('svc-config-backfill') == 3

    current_snapshot = _config_snapshot(b'active_workspace: current\n', '3')
    legacy_snapshot = _config_snapshot(b'active_workspace: legacy\n', '2')
    assert (serve_state.add_or_update_version(
        'svc-config-backfill',
        3,
        _service_spec('current'),
        'value: current',
        ha_recovery_script=_VERSIONED_HA_SCRIPT,
        controller_config=current_snapshot[0],
        controller_config_digest=current_snapshot[1],
        controller_config_snapshot_id=current_snapshot[2],
        legacy_controller_config_snapshot=legacy_snapshot,
        legacy_controller_applied_version=1,
        expected_service_hash='incarnation-a',
        expected_controller_owner=(12345, None))
            is serve_state.VersionCommitResult.COMMITTED)
    assert serve_state.get_version_controller_config('svc-config-backfill',
                                                     1) == initial_snapshot
    assert serve_state.get_version_controller_config('svc-config-backfill',
                                                     2) == legacy_snapshot
    assert serve_state.get_version_controller_config('svc-config-backfill',
                                                     3) == current_snapshot


def test_get_version_controller_config_rejects_digest_corruption(
        _mock_serve_db):
    snapshot = _config_snapshot(b'active_workspace: research\n')
    assert _add_minimal_service('svc-corrupt-config',
                                controller_config=snapshot[0],
                                controller_config_digest=snapshot[1],
                                controller_config_snapshot_id=snapshot[2])
    with orm.Session(_mock_serve_db) as session:
        session.execute(
            sqlalchemy.update(serve_state.version_specs_table).where(
                serve_state.version_specs_table.c.service_name ==
                'svc-corrupt-config').values(controller_config_digest='0' * 64))
        session.commit()
    with pytest.raises(serve_state.ControllerConfigCorruptionError,
                       match='failed integrity validation'):
        serve_state.get_version_controller_config('svc-corrupt-config', 1)


def test_invalid_controller_config_snapshot_is_rejected_before_write(
        _mock_serve_db):
    assert _add_minimal_service('svc-invalid-config')
    assert serve_state.add_version('svc-invalid-config') == 2
    with pytest.raises(ValueError, match='provide config bytes'):
        serve_state.add_or_update_version(
            'svc-invalid-config',
            2,
            _service_spec('v2'),
            'value: v2',
            controller_config=b'config',
            controller_config_digest=hashlib.sha256(b'config').hexdigest())
    row = _read_version_row(_mock_serve_db, 'svc-invalid-config', 2)
    assert row['yaml_content'] is None
    assert row['controller_config'] is None


def test_version_and_ha_recovery_script_commit_atomically(_mock_serve_db):
    assert _add_minimal_service('svc-config') is True
    assert serve_state.set_ha_recovery_script('svc-config', 'legacy-script')
    assert serve_state.add_version('svc-config') == 2
    result = serve_state.add_or_update_version(
        'svc-config',
        2,
        _service_spec('v2'),
        'value: v2',
        ha_recovery_script='config-free-recovery-script-v2')
    assert result is serve_state.VersionCommitResult.COMMITTED
    assert (serve_state.get_ha_recovery_script('svc-config') ==
            'config-free-recovery-script-v2')
    retry = serve_state.add_or_update_version(
        'svc-config',
        2,
        _service_spec('v2'),
        'value: v2',
        ha_recovery_script='config-free-recovery-script-v2')
    assert retry is serve_state.VersionCommitResult.IDEMPOTENT_RETRY
    stale_retry = serve_state.add_or_update_version(
        'svc-config',
        2,
        _service_spec('v2'),
        'value: v2',
        ha_recovery_script='different-script-for-same-version')
    assert stale_retry is serve_state.VersionCommitResult.CONTENT_CONFLICT
    assert (serve_state.get_ha_recovery_script('svc-config') ==
            'config-free-recovery-script-v2')


def test_ha_config_upsert_failure_rolls_back_version_transaction(
        _mock_serve_db):
    assert _add_minimal_service('svc-config-rollback',
                                service_hash='incarnation-a') is True
    with orm.Session(_mock_serve_db) as session:
        session.execute(
            sqlalchemy.update(serve_state.version_specs_table).where(
                serve_state.version_specs_table.c.service_name ==
                'svc-config-rollback',
                serve_state.version_specs_table.c.version == 1).values(
                    controller_applied_at=None))
        session.commit()
    assert serve_state.set_ha_recovery_script('svc-config-rollback',
                                              'legacy-script')
    assert serve_state.add_version('svc-config-rollback') == 2
    current_snapshot = _config_snapshot(b'active_workspace: current\n', 'c')
    legacy_snapshot = _config_snapshot(b'active_workspace: legacy\n', 'd')

    def _fail_recovery_upsert(_connection, _cursor, statement, _parameters,
                              _context, _executemany):
        if 'INSERT INTO serve_ha_recovery_script' in statement:
            raise RuntimeError('injected recovery upsert failure')

    sqlalchemy.event.listen(_mock_serve_db, 'before_cursor_execute',
                            _fail_recovery_upsert)
    try:
        with pytest.raises(RuntimeError, match='injected recovery'):
            serve_state.add_or_update_version(
                'svc-config-rollback',
                2,
                _service_spec('v2'),
                'value: v2',
                ha_recovery_script=_VERSIONED_HA_SCRIPT,
                controller_config=current_snapshot[0],
                controller_config_digest=current_snapshot[1],
                controller_config_snapshot_id=current_snapshot[2],
                legacy_controller_config_snapshot=legacy_snapshot,
                legacy_controller_applied_version=1,
                expected_service_hash='incarnation-a',
                expected_controller_owner=(12345, None))
    finally:
        sqlalchemy.event.remove(_mock_serve_db, 'before_cursor_execute',
                                _fail_recovery_upsert)

    version_row = _read_version_row(_mock_serve_db, 'svc-config-rollback', 2)
    assert version_row is not None
    assert version_row['yaml_content'] is None
    assert version_row['controller_config'] is None
    assert serve_state.get_version_controller_config('svc-config-rollback',
                                                     1) is None
    assert _read_version_row(_mock_serve_db, 'svc-config-rollback',
                             1)['controller_applied_at'] is None
    assert _read_row(_mock_serve_db,
                     'svc-config-rollback')['current_version'] == 1
    assert (serve_state.get_ha_recovery_script('svc-config-rollback') ==
            'legacy-script')


def test_version_placement_catalog_persists_and_backfills_once(_mock_serve_db):
    initial_catalog = {'schema_version': 1, 'entries': []}
    assert _add_minimal_service('svc-catalog',
                                placement_catalog=initial_catalog) is True
    assert serve_state.get_placement_catalog('svc-catalog',
                                             1) == (initial_catalog)

    assert serve_state.add_version('svc-catalog') == 2
    update_catalog = {
        'schema_version': 1,
        'entries': [{
            'location': {
                'cloud': 'AWS',
                'region': 'us-east-1',
            },
            'hourly_cost': 0.25,
        }],
    }
    assert (serve_state.add_or_update_version('svc-catalog',
                                              2,
                                              _service_spec('v2'),
                                              'value: v2',
                                              placement_catalog=update_catalog)
            is serve_state.VersionCommitResult.COMMITTED)
    assert serve_state.get_placement_catalog('svc-catalog', 2) == update_catalog

    assert serve_state.add_version('svc-catalog') == 3
    assert (serve_state.add_or_update_version('svc-catalog', 3,
                                              _service_spec('legacy'),
                                              'value: legacy')
            is serve_state.VersionCommitResult.COMMITTED)
    winner = {'schema_version': 1, 'entries': [{'winner': True}]}
    loser = {'schema_version': 1, 'entries': [{'winner': False}]}
    assert serve_state.set_placement_catalog_if_missing('svc-catalog', 3,
                                                        winner)
    assert not serve_state.set_placement_catalog_if_missing(
        'svc-catalog', 3, loser)
    assert serve_state.get_placement_catalog('svc-catalog', 3) == winner


def test_identical_version_retry_only_backfills_missing_catalog(_mock_serve_db):
    assert _add_minimal_service('svc-catalog-retry') is True
    catalog = {'schema_version': 1, 'entries': []}
    assert (serve_state.add_or_update_version('svc-catalog-retry',
                                              1,
                                              _service_spec('ignored'),
                                              'yaml: v1',
                                              placement_catalog=catalog)
            is serve_state.VersionCommitResult.IDEMPOTENT_RETRY)
    row = _read_version_row(_mock_serve_db, 'svc-catalog-retry', 1)
    assert row['placement_catalog'] == catalog
    original_spec = row['spec']
    assert (serve_state.add_or_update_version('svc-catalog-retry',
                                              1,
                                              _service_spec('different'),
                                              'yaml: v1',
                                              placement_catalog={
                                                  'schema_version': 1,
                                                  'entries': [{
                                                      'other': True
                                                  }]
                                              })
            is serve_state.VersionCommitResult.IDEMPOTENT_RETRY)
    final_row = _read_version_row(_mock_serve_db, 'svc-catalog-retry', 1)
    assert final_row['placement_catalog'] == catalog
    assert final_row['spec'] == original_spec


def test_logical_replica_activation_is_durable_and_one_way(_mock_serve_db):
    physical = _service_spec('physical')
    logical = _service_spec('logical', uses_logical_replicas=True)
    assert _add_minimal_service('svc-logical', spec=physical) is True
    assert not serve_state.service_uses_logical_replica_semantics('svc-logical')

    assert serve_state.add_version('svc-logical') == 2
    assert (serve_state.add_or_update_version('svc-logical', 2, logical,
                                              'yaml: logical')
            is serve_state.VersionCommitResult.COMMITTED)
    assert serve_state.service_uses_logical_replica_semantics('svc-logical')

    assert serve_state.add_version('svc-logical') == 3
    assert (serve_state.add_or_update_version('svc-logical', 3, physical,
                                              'yaml: physical')
            is serve_state.VersionCommitResult.SEMANTIC_CONFLICT)
    assert serve_state.get_spec('svc-logical', 3) is None

    # A lost-response retry of a physical version committed before activation
    # remains idempotent. The fence only rejects new physical commits.
    assert (serve_state.add_or_update_version('svc-logical', 1, physical,
                                              'yaml: v1')
            is serve_state.VersionCommitResult.IDEMPOTENT_RETRY)


def test_lower_logical_commit_cannot_flip_newer_physical_semantics(
        _mock_serve_db):
    physical = _service_spec('physical')
    logical = _service_spec('logical', uses_logical_replicas=True)
    assert _add_minimal_service('svc-out-of-order', spec=physical) is True
    assert serve_state.add_version('svc-out-of-order') == 2
    assert serve_state.add_version('svc-out-of-order') == 3

    assert (serve_state.add_or_update_version('svc-out-of-order', 3, physical,
                                              'yaml: physical-v3')
            is serve_state.VersionCommitResult.COMMITTED)
    assert (serve_state.add_or_update_version('svc-out-of-order', 2, logical,
                                              'yaml: logical-v2')
            is serve_state.VersionCommitResult.STALE_VERSION)
    assert not serve_state.service_uses_logical_replica_semantics(
        'svc-out-of-order')
    assert serve_state.get_spec('svc-out-of-order', 2) is None
    assert serve_state.get_spec('svc-out-of-order',
                                3).uses_logical_replicas is False


def test_initial_logical_service_sets_activation_fence(_mock_serve_db):
    logical = _service_spec('logical', uses_logical_replicas=True)
    assert _add_minimal_service('svc-initial-logical', spec=logical) is True
    assert serve_state.service_uses_logical_replica_semantics(
        'svc-initial-logical')


def test_demand_capacity_observation_round_trip(_mock_serve_db):
    serve_state.upsert_demand_capacity_observation('research', 123.0, 123.5, {
        'a100': 223,
        'l4': 12,
    })

    observations = serve_state.get_demand_capacity_observations(
        ['research', 'missing'])
    assert set(observations) == {'research'}
    assert observations['research']['snapshot_time'] == 123.0
    assert observations['research']['completed_at'] == 123.5
    assert json.loads(observations['research']['availability']) == {
        'a100': 223,
        'l4': 12,
    }

    # Query failures are durable observations too. They rate-limit provider
    # retries while remaining distinguishable from a successful empty result.
    serve_state.upsert_demand_capacity_observation('research', 124.0, 125.0,
                                                   None)
    observation = serve_state.get_demand_capacity_observations(['research'
                                                               ])['research']
    assert observation['snapshot_time'] == 124.0
    assert observation['completed_at'] == 125.0
    assert observation['availability'] is None


def test_get_replica_infos_from_ids_batches_in_one_query(_mock_serve_db):
    for rid in (1, 2, 3):
        info = replica_managers.ReplicaInfo(replica_id=rid,
                                            cluster_name=f'svc-{rid}',
                                            replica_port='8080',
                                            is_spot=False,
                                            location=None,
                                            version=1,
                                            resources_override=None)
        serve_state.add_or_update_replica('svc', rid, info)

    with _count_sql_statements(_mock_serve_db) as counts:
        infos = serve_state.get_replica_infos_from_ids('svc', [2, 1, 2, 4])

    assert counts['n'] == 1, counts
    assert set(infos) == {1, 2}
    assert infos[1].replica_id == 1
    assert infos[2].replica_id == 2


def test_get_replica_infos_from_ids_empty_skips_query(_mock_serve_db):
    with _count_sql_statements(_mock_serve_db) as counts:
        assert serve_state.get_replica_infos_from_ids('svc', []) == {}

    assert counts['n'] == 0, counts


def test_get_yaml_contents_batches_requested_versions_in_one_query(
        _mock_serve_db):
    assert _add_minimal_service('svc-yamls') is True
    serve_state.add_or_update_version(
        'svc-yamls',
        1,
        _service_spec(graceful_drain_async_occupancy=False),
        'yaml: v1',
    )
    serve_state.add_or_update_version(
        'svc-yamls',
        2,
        _service_spec(graceful_drain_async_occupancy=True),
        'yaml: v2',
    )

    with _count_sql_statements(_mock_serve_db) as counts:
        yamls = serve_state.get_yaml_contents('svc-yamls', [2, 1, 2, 3])

    assert counts['n'] == 1, counts
    assert yamls == {
        1: 'yaml: v1',
        2: 'yaml: v2',
    }


def test_get_yaml_contents_empty_versions_skips_query(_mock_serve_db):
    with _count_sql_statements(_mock_serve_db) as counts:
        yamls = serve_state.get_yaml_contents('svc-yamls', [])

    assert counts['n'] == 0, counts
    assert not yamls


def test_get_version_yaml_contents_fetches_all_versions_in_one_query(
        _mock_serve_db):
    assert _add_minimal_service('svc-all-yamls') is True
    serve_state.add_or_update_version(
        'svc-all-yamls',
        2,
        _service_spec(graceful_drain_async_occupancy=True),
        'yaml: v2',
    )
    serve_state.add_or_update_version(
        'svc-all-yamls',
        1,
        _service_spec(graceful_drain_async_occupancy=False),
        'yaml: v1',
    )
    # Interrupted updates leave a NULL-yaml placeholder.  The batched cleanup
    # snapshot must preserve the old per-version reader's skip semantics.
    assert serve_state.add_version('svc-all-yamls') == 3

    with _count_sql_statements(_mock_serve_db) as counts:
        yamls = serve_state.get_version_yaml_contents('svc-all-yamls')

    assert counts['n'] == 1, counts
    assert yamls == {
        1: 'yaml: v1',
        2: 'yaml: v2',
    }
    assert list(yamls) == [1, 2]


def test_get_version_yaml_contents_unknown_service_returns_empty(
        _mock_serve_db):
    assert serve_state.get_version_yaml_contents('svc-missing') == {}


def test_get_service_from_name_uses_joined_spec_in_single_query(_mock_serve_db):
    spec = _service_spec(policy='qps=2', load_balancing_policy='least_load')
    assert _add_minimal_service('svc-read', spec=spec) is True

    with _count_sql_statements(_mock_serve_db) as counts:
        record = serve_state.get_service_from_name('svc-read')

    assert counts['n'] == 1, counts
    assert record is not None
    assert record['policy'] == 'Fixed 1 replica'
    assert record['load_balancing_policy'] == 'least_load'


def test_service_workspace_survives_durable_round_trip(_mock_serve_db):
    assert _add_minimal_service('svc-workspace', workspace='research') is True
    record = serve_state.get_service_from_name('svc-workspace')
    assert record is not None
    assert record['workspace'] == 'research'


def test_get_services_uses_single_query_for_multiple_rows(_mock_serve_db):
    assert _add_minimal_service('svc-a') is True
    assert _add_minimal_service('svc-b') is True
    assert _add_minimal_service('svc-c') is True

    with _count_sql_statements(_mock_serve_db) as counts:
        records = serve_state.get_services()

    assert counts['n'] == 1, counts
    assert sorted(record['name'] for record in records) == [
        'svc-a',
        'svc-b',
        'svc-c',
    ]


def test_get_num_services_filters_raw_modes_without_deserializing(
        _mock_serve_db, monkeypatch):
    assert _add_minimal_service('serve-versioned') is True
    assert _add_minimal_service('pool-versioned', pool=True) is True
    _insert_orphan_service_row(_mock_serve_db, 'serve-orphan')
    _insert_orphan_service_row(_mock_serve_db, 'pool-orphan', pool=True)

    def _unexpected_spec_unpickle(_):
        raise AssertionError('service counts must not deserialize specs')

    monkeypatch.setattr(serve_state.pickle, 'loads', _unexpected_spec_unpickle)
    with _count_sql_statements(_mock_serve_db) as counts:
        assert serve_state.get_num_services() == 4
        assert serve_state.get_num_services(pool=False) == 2
        assert serve_state.get_num_services(pool=True) == 2

    assert counts['n'] == 3, counts


def test_get_service_liveness_snapshots_is_one_slim_version_backed_query(
        _mock_serve_db, monkeypatch):
    assert _add_minimal_service('serve-b',
                                controller_ip='10.0.0.2',
                                controller_pid=22,
                                service_hash='hash-b',
                                resource_scope='scope-b',
                                workspace='workspace-b') is True
    assert _add_minimal_service('serve-a',
                                controller_ip='10.0.0.1',
                                controller_pid=11,
                                service_hash='hash-a',
                                resource_scope='scope-a',
                                workspace='workspace-a') is True
    assert _add_minimal_service('pool-a', pool=True) is True
    _insert_orphan_service_row(_mock_serve_db, 'serve-orphan')
    serve_state.set_service_status_and_active_versions(
        'serve-b', serve_state.ServiceStatus.FAILED_CLEANUP)

    def _unexpected_spec_unpickle(_):
        raise AssertionError('liveness snapshots must not deserialize specs')

    monkeypatch.setattr(serve_state.pickle, 'loads', _unexpected_spec_unpickle)
    with _count_sql_statements(_mock_serve_db) as counts:
        records = serve_state.get_service_liveness_snapshots(pool=False)

    assert counts['n'] == 1, counts
    assert records == [{
        'name': 'serve-a',
        'status': serve_state.ServiceStatus.CONTROLLER_INIT,
        'controller_job_id': 1,
        'controller_pid': 11,
        'controller_ip': '10.0.0.1',
        'hash': 'hash-a',
        'resource_scope': 'scope-a',
        'workspace': 'workspace-a',
        'yaml_content': 'yaml: v1',
        'recovery_version': 1,
        'config_protocol_active': False,
        'recovery_config_present': False,
    }, {
        'name': 'serve-b',
        'status': serve_state.ServiceStatus.FAILED_CLEANUP,
        'controller_job_id': 1,
        'controller_pid': 22,
        'controller_ip': '10.0.0.2',
        'hash': 'hash-b',
        'resource_scope': 'scope-b',
        'workspace': 'workspace-b',
        'yaml_content': 'yaml: v1',
        'recovery_version': 1,
        'config_protocol_active': False,
        'recovery_config_present': False,
    }]


def test_get_service_liveness_snapshots_reports_latest_version_yaml(
        _mock_serve_db):
    """The snapshot carries the LATEST version's yaml, including NULL for a
    placeholder version row, so liveness sweeps can retire unbootable rows
    without a per-service joined read."""
    assert _add_minimal_service('svc', yaml_content='yaml: v1') is True
    serve_state.add_or_update_version(
        'svc', 2, _service_spec(graceful_drain_async_occupancy=False),
        'yaml: v2')
    assert _add_minimal_service('placeholder', yaml_content=None) is True

    records = serve_state.get_service_liveness_snapshots(pool=False)

    by_name = {record['name']: record for record in records}
    assert by_name['svc']['yaml_content'] == 'yaml: v2'
    assert by_name['placeholder']['yaml_content'] is None


def test_get_service_liveness_snapshots_elects_applied_quarantine_fallback(
        _mock_serve_db, monkeypatch):
    """HA gets one workspace-bound recovery election without loading specs."""
    config = (b'active_workspace: research\n'
              b'workspaces: {research: {}}\n')
    digest = hashlib.sha256(config).hexdigest()
    assert _add_minimal_service('svc',
                                workspace='research',
                                spec=_service_spec('spec-1'),
                                controller_config=config,
                                controller_config_digest=digest,
                                controller_config_snapshot_id='a' * 64)
    for version, snapshot_id in ((2, 'b' * 64), (3, 'c' * 64)):
        assert serve_state.add_or_update_version(
            'svc',
            version,
            _service_spec(f'spec-{version}'),
            f'yaml: v{version}',
            controller_config=config,
            controller_config_digest=digest,
            controller_config_snapshot_id=snapshot_id,
            ha_recovery_script=_VERSIONED_HA_SCRIPT,
        ) is serve_state.VersionCommitResult.COMMITTED
    assert serve_state.quarantine_version('svc', 3, 'never applied')

    def _unexpected_spec_unpickle(_):
        raise AssertionError('liveness snapshots must not deserialize specs')

    monkeypatch.setattr(serve_state.pickle, 'loads', _unexpected_spec_unpickle)
    with _count_sql_statements(_mock_serve_db) as counts:
        records = serve_state.get_service_liveness_snapshots(pool=False)

    assert counts['n'] == 1, counts
    assert len(records) == 1
    assert records[0]['workspace'] == 'research'
    assert records[0]['yaml_content'] == 'yaml: v3'
    assert records[0]['recovery_version'] == 1
    assert records[0]['config_protocol_active'] is True
    assert records[0]['recovery_config_present'] is True


class TestAddServiceWritesControllerIp:
    """`add_service` should persist the new controller_ip column when caller
    provides POD_IP. Older callers that don't pass it must still work
    (column defaults to NULL)."""

    def test_with_controller_ip(self, _mock_serve_db):
        success = _add_minimal_service('svc1', controller_ip='10.0.0.7')
        assert success is True
        record = _read_row(_mock_serve_db, 'svc1')
        assert record is not None
        assert record['controller_ip'] == '10.0.0.7'

    def test_without_controller_ip(self, _mock_serve_db):
        # No controller_ip arg → column stays NULL (used by single-pod /
        # non-K8s deployments where the routing layer falls back to localhost).
        success = _add_minimal_service('svc2')
        assert success is True
        record = _read_row(_mock_serve_db, 'svc2')
        assert record is not None
        assert record['controller_ip'] is None

    def test_returns_false_on_duplicate(self, _mock_serve_db):
        assert _add_minimal_service('svc3', controller_ip='10.0.0.7') is True
        # Adding the same service again must not corrupt — uniqueness violation
        # is converted to False so up() can short-circuit.
        assert _add_minimal_service('svc3', controller_ip='10.0.0.8') is False
        # And the row is unchanged.
        record = _read_row(_mock_serve_db, 'svc3')
        assert record['controller_ip'] == '10.0.0.7'

    def test_persists_caller_generated_incarnation(self, _mock_serve_db):
        assert _add_minimal_service(
            'svc-known-hash', service_hash='caller-generated-hash') is True
        assert _read_row(_mock_serve_db,
                         'svc-known-hash')['hash'] == 'caller-generated-hash'


class TestAddServiceAtomicRegistration:
    """`add_service` must write the `services` row and its initial
    `version_specs` row atomically. The two-write path (add_service then
    add_or_update_version) had a crash window that stranded a `services` row
    with no version row -- invisible to the latest-version INNER JOIN, so it
    could never be recovered, removed, or have its name reused."""

    def test_registration_is_visible_via_join(self, _mock_serve_db):
        # The whole point: after the atomic write, the service is reachable
        # through get_service_from_name (which INNER-JOINs version_specs),
        # with its initial version row in place.
        assert _add_minimal_service('svc-atomic') is True
        assert serve_state.get_service_from_name('svc-atomic') is not None
        assert (serve_state.get_latest_version('svc-atomic') ==
                serve_constants.INITIAL_VERSION)
        assert serve_state.get_yaml_content(
            'svc-atomic', serve_constants.INITIAL_VERSION) == 'yaml: v1'
        version_row = _read_version_row(_mock_serve_db, 'svc-atomic', 1)
        assert version_row is not None
        assert version_row['controller_applied_at'] is not None
        assert (
            version_row['controller_applied_at'] == version_row['created_at'])

    def test_duplicate_does_not_write_second_version_row(self, _mock_serve_db):
        assert _add_minimal_service('svc-dup') is True
        # A duplicate name returns False so up() can short-circuit, and must
        # not write a second version row.
        assert _add_minimal_service('svc-dup') is False
        with orm.Session(_mock_serve_db) as session:
            versions = session.execute(
                sqlalchemy.select(
                    serve_state.version_specs_table.c.version).where(
                        serve_state.version_specs_table.c.service_name ==
                        'svc-dup')).fetchall()
        assert len(versions) == 1

    def test_legacy_registration_preserves_stale_version_inventory(
            self, _mock_serve_db):
        # A non-consolidated controller has no API-local lifecycle epoch, but
        # its own Serve DB is still authoritative. Never overwrite the last
        # cleanup inventory merely because this registration is legacy-shaped.
        serve_state.add_or_update_version('svc-stale',
                                          serve_constants.INITIAL_VERSION,
                                          _service_spec('stale'), 'yaml: stale')
        assert _read_row(_mock_serve_db, 'svc-stale') is None  # no svc row

        with pytest.raises(serve_state.OrphanedVersionRecordsError):
            _add_minimal_service('svc-stale')
        assert serve_state.get_service_from_name('svc-stale') is None
        assert serve_state.get_yaml_content(
            'svc-stale', serve_constants.INITIAL_VERSION) == 'yaml: stale'

    def test_fenced_registration_preserves_orphan_version_inventory(
            self, _mock_serve_db):
        # An older interrupted teardown can leave v1 AND v2+ without the
        # parent services row. Replacing v1 alone makes MAX(version)=v2 and
        # leaks the predecessor's spec/yaml into the new incarnation.
        serve_state.add_or_update_version('svc-stale', 1,
                                          _service_spec('old-spec-1'),
                                          'yaml: old-v1')
        serve_state.add_or_update_version('svc-stale', 2,
                                          _service_spec('old-spec-2'),
                                          'yaml: old-v2')
        assert serve_state.set_ha_recovery_script('svc-stale', 'old-script')
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                serve_state.reserved_fill_claims_table.insert().values(
                    service_name='svc-stale',
                    pool_key='old-pool',
                    weight=1,
                    floor_replicas=1,
                    gpus_per_replica=1,
                    holdings_fill=1,
                    heartbeat_ts=1))
            session.commit()

        epoch = serve_state.claim_service_lifecycle_epoch('svc-stale')
        with pytest.raises(serve_state.OrphanedVersionRecordsError,
                           match=r'versions \[1, 2\]'):
            _add_minimal_service('svc-stale',
                                 service_hash='new-incarnation',
                                 lifecycle_epoch=epoch)

        assert _read_row(_mock_serve_db, 'svc-stale') is None
        assert serve_state.get_service_versions('svc-stale') == [1, 2]
        assert serve_state.get_ha_recovery_script('svc-stale') == 'old-script'
        with orm.Session(_mock_serve_db) as session:
            claim = session.execute(
                sqlalchemy.select(
                    serve_state.reserved_fill_claims_table.c.service_name).
                where(serve_state.reserved_fill_claims_table.c.service_name ==
                      'svc-stale')).fetchone()
        assert claim is not None

    def test_fenced_registration_preserves_orphan_replica_inventory(
            self, _mock_serve_db):
        orphan = _replica(9, cluster_name='billable-cluster-9')
        serve_state.add_or_update_replica('svc-stale', 9, orphan)
        epoch = serve_state.claim_service_lifecycle_epoch('svc-stale')

        with pytest.raises(serve_state.OrphanedReplicaRecordsError,
                           match='billable-cluster-9'):
            _add_minimal_service('svc-stale',
                                 service_hash='new-incarnation',
                                 lifecycle_epoch=epoch)

        assert _read_row(_mock_serve_db, 'svc-stale') is None
        replicas = serve_state.get_replica_infos('svc-stale')
        assert len(replicas) == 1
        assert replicas[0].cluster_name == 'billable-cluster-9'

    def test_current_recovery_script_survives_fenced_registration(
            self, _mock_serve_db):
        # Model the consolidation-mode launch ordering: an old script may
        # remain after predecessor cleanup, then the API parent claims the
        # next lifecycle and atomically replaces it before spawning the new
        # controller.  Initial registration must preserve that current script
        # while still clearing stale reserved-fill claims.
        assert serve_state.set_ha_recovery_script('svc-stale', 'old-script')
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                serve_state.reserved_fill_claims_table.insert().values(
                    service_name='svc-stale',
                    pool_key='old-pool',
                    weight=1,
                    floor_replicas=1,
                    gpus_per_replica=1,
                    holdings_fill=1,
                    heartbeat_ts=1))
            session.commit()

        assert 'svc-stale' not in (serve_state.get_orphaned_service_child_names(
            ['svc-stale']))
        epoch = serve_state.claim_service_lifecycle_epoch('svc-stale')
        assert serve_state.set_ha_recovery_script('svc-stale', 'current-script',
                                                  epoch)
        assert _add_minimal_service('svc-stale',
                                    service_hash='new-incarnation',
                                    lifecycle_epoch=epoch)
        assert (
            serve_state.get_ha_recovery_script('svc-stale') == 'current-script')
        with orm.Session(_mock_serve_db) as session:
            claim = session.execute(
                sqlalchemy.select(
                    serve_state.reserved_fill_claims_table.c.service_name).
                where(serve_state.reserved_fill_claims_table.c.service_name ==
                      'svc-stale')).fetchone()
        assert claim is None

    def test_get_service_pool_from_db_sees_orphan_row(self, _mock_serve_db):
        # The raw-pool accessor must read a version-less row (the orphan case)
        # that get_service_from_name's inner join hides -- this is what gates
        # the mode-scoped `down --purge` cleanup of such an orphan.
        _insert_orphan_service_row(_mock_serve_db, 'svc-orphan')
        assert serve_state.get_service_from_name('svc-orphan') is None
        assert serve_state.get_service_pool_from_db('svc-orphan') is False
        assert serve_state.get_service_pool_from_db('never-existed') is None


class TestServiceLifecycleEpoch:
    """A durable name epoch fences work after advisory-session loss."""

    def test_epoch_survives_delete_and_recreate(self, _mock_serve_db):
        epoch_a = serve_state.claim_service_lifecycle_epoch('svc')
        assert epoch_a == 1
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch_a,
                                    resource_scope='incarnation-a')

        epoch_teardown = serve_state.claim_service_lifecycle_epoch('svc')
        assert epoch_teardown == 2
        assert not serve_state.remove_service_completely(
            'svc', 'incarnation-a', expected_lifecycle_epoch=epoch_a)
        assert serve_state.remove_service_completely(
            'svc', 'incarnation-a', expected_lifecycle_epoch=epoch_teardown)

        epoch_b = serve_state.claim_service_lifecycle_epoch('svc')
        assert epoch_b == 3
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-b',
                                    lifecycle_epoch=epoch_b,
                                    resource_scope='incarnation-b')
        row = _read_row(_mock_serve_db, 'svc')
        assert row['hash'] == 'incarnation-b'
        assert row['lifecycle_epoch'] == epoch_b
        assert row['resource_scope'] == 'incarnation-b'

    def test_stale_child_row_mutations_cannot_touch_successor(
            self, _mock_serve_db):
        epoch_a = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch_a,
                                    resource_scope='incarnation-a')
        replica_a = _replica(1, cluster_name='replica-a')
        assert serve_state.add_or_update_replica(
            'svc',
            1,
            replica_a,
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch_a)

        epoch_delete = serve_state.claim_service_lifecycle_epoch('svc')
        assert serve_state.remove_service_completely(
            'svc', 'incarnation-a', expected_lifecycle_epoch=epoch_delete)
        epoch_b = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-b',
                                    lifecycle_epoch=epoch_b,
                                    resource_scope='incarnation-b')
        assert serve_state.add_or_update_replica(
            'svc',
            1,
            _replica(1, cluster_name='replica-b'),
            expected_service_hash='incarnation-b',
            expected_lifecycle_epoch=epoch_b)

        assert not serve_state.remove_replica(
            'svc',
            1,
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch_delete,
            expected_replica_record_id=replica_a.replica_record_id)
        assert not serve_state.remove_replicas(
            'svc', [1],
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch_delete,
            expected_replica_record_ids={1: replica_a.replica_record_id})
        replica = serve_state.get_replica_info_from_id('svc', 1)
        assert replica is not None
        assert replica.cluster_name == 'replica-b'

    def test_exact_owner_replica_cleanup_is_idempotent_when_child_is_absent(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch,
                                    resource_scope='incarnation-a')

        assert serve_state.remove_replica('svc',
                                          99,
                                          expected_service_hash='incarnation-a',
                                          expected_lifecycle_epoch=epoch,
                                          expected_replica_record_id=str(
                                              uuid.uuid4()))

    def test_exact_owner_bulk_replica_cleanup_is_atomic_and_idempotent(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        owner = (123, '10.0.0.1')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch,
                                    resource_scope='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1])
        expected_record_ids = {}
        for replica_id in range(1200):
            info = _replica(replica_id)
            expected_record_ids[replica_id] = info.replica_record_id
            assert serve_state.add_or_update_replica(
                'svc',
                replica_id,
                info,
                expected_service_hash='incarnation-a',
                expected_lifecycle_epoch=epoch)

        assert not serve_state.remove_replicas(
            'svc',
            list(range(1200)),
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch,
            expected_controller_owner=(456, '10.0.0.2'),
            expected_replica_record_ids=expected_record_ids)
        assert len(serve_state.get_replica_infos('svc')) == 1200
        assert serve_state.remove_replicas(
            'svc',
            list(range(1200)),
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch,
            expected_controller_owner=owner,
            expected_replica_record_ids=expected_record_ids)
        assert serve_state.get_replica_infos('svc') == []
        assert _read_row(_mock_serve_db, 'svc') is not None
        assert _read_version_row(_mock_serve_db, 'svc', 1) is not None
        assert serve_state.remove_replicas(
            'svc',
            list(range(1200)),
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch,
            expected_controller_owner=owner,
            expected_replica_record_ids=expected_record_ids)

    def test_stale_recovery_script_and_version_claims_fail(
            self, _mock_serve_db):
        epoch_a = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch_a,
                                    resource_scope='incarnation-a')
        assert serve_state.set_ha_recovery_script('svc', 'script-a', epoch_a)

        epoch_b = serve_state.claim_service_lifecycle_epoch('svc')
        assert not serve_state.set_ha_recovery_script('svc', 'stale-script',
                                                      epoch_a)
        assert serve_state.get_ha_recovery_script('svc') == 'script-a'
        with pytest.raises(RuntimeError, match='lifecycle ownership'):
            serve_state.add_version('svc',
                                    expected_service_hash='incarnation-a',
                                    expected_lifecycle_epoch=epoch_a)
        assert serve_state.add_version('svc',
                                       expected_service_hash='incarnation-a',
                                       expected_lifecycle_epoch=epoch_b) == 2

        assert not serve_state.add_or_update_version(
            'svc',
            2,
            _service_spec('stale'),
            'stale: yaml',
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch_a)
        assert serve_state.get_yaml_content('svc', 2) is None

    def test_version_cas_checks_authoritative_fence_before_service_row(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch)
        # Model a new lifecycle claimant that advanced the authoritative fence
        # but has not yet stamped services.lifecycle_epoch (it may be blocked
        # on that row behind this update transaction in PostgreSQL).
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(
                    serve_state.service_lifecycle_fences_table).where(
                        serve_state.service_lifecycle_fences_table.c.name ==
                        'svc').values(epoch=epoch + 1))
            session.commit()
        assert _read_row(_mock_serve_db, 'svc')['lifecycle_epoch'] == epoch

        with pytest.raises(RuntimeError, match='lifecycle ownership'):
            serve_state.add_version('svc',
                                    expected_service_hash='incarnation-a',
                                    expected_lifecycle_epoch=epoch)
        assert not serve_state.add_or_update_version(
            'svc',
            2,
            _service_spec('stale'),
            'yaml: stale',
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch)
        assert serve_state.get_latest_version('svc') == 1

    def test_status_and_teardown_claims_check_authoritative_fence(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch)
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.SHUTTING_DOWN)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(
                    serve_state.service_lifecycle_fences_table).where(
                        serve_state.service_lifecycle_fences_table.c.name ==
                        'svc').values(epoch=epoch + 1))
            session.commit()

        assert not serve_state.set_service_status_and_active_versions_if_hash(
            'svc',
            'incarnation-a',
            serve_state.ServiceStatus.FAILED_CLEANUP,
            expected_lifecycle_epoch=epoch)
        assert not serve_state.claim_orphaned_service_teardown(
            'svc',
            'incarnation-a',
            200,
            '10.0.0.2',
            999,
            '10.0.0.9',
            expected_lifecycle_epoch=epoch)
        assert not serve_state.claim_unrecoverable_service_teardown(
            'svc',
            'incarnation-a',
            200,
            '10.0.0.2',
            999,
            '10.0.0.9',
            expected_lifecycle_epoch=epoch)
        row = _read_row(_mock_serve_db, 'svc')
        assert row['status'] == serve_state.ServiceStatus.SHUTTING_DOWN.value
        assert row['controller_pid'] == 200

    def test_stale_controller_cannot_write_current_incarnation_children(
            self, _mock_serve_db):
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a')
        stale_owner = (100, '10.0.0.1')
        assert not serve_state.add_or_update_replica(
            'svc',
            1,
            'stale-single',
            expected_service_hash='incarnation-a',
            expected_controller_owner=stale_owner)
        assert not serve_state.add_or_update_replicas(
            'svc', [(1, 'stale-batch')],
            expected_service_hash='incarnation-a',
            expected_controller_owner=stale_owner)
        assert serve_state.get_replica_info_from_id('svc', 1) is None


class TestEphemeralStorageCleanupIntents:
    """Storage ownership is durable before upload and adopted atomically."""

    @staticmethod
    def _yaml(resource_scope: str, generation: str) -> str:
        scope_id = (
            ephemeral_storage_contract.canonical_ephemeral_storage_scope_id(
                resource_scope, generation))
        return f"""\
_metadata:
  {serve_constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY}:
    resource_scope: {resource_scope}
    scope_id: {scope_id}
    storage_generation: {generation}
    storage_mounts: []
service:
  readiness_probe: /
"""

    def test_initial_registration_adopts_intent_in_same_transaction(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-a', 'generation-1')
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-1', yaml_content, False, epoch,
            True)
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch,
                                    resource_scope='incarnation-a',
                                    yaml_content=yaml_content)

        intents = serve_state.get_ephemeral_storage_cleanup_intents('svc')
        assert len(intents) == 1
        assert intents[0]['provisional'] == 0

    def test_exact_handoff_rejects_cas_rowcount_miss(self):
        yaml_content = self._yaml('incarnation-a', 'generation-1')
        intent = {
            'service_name': 'svc',
            'resource_scope': 'incarnation-a',
            'storage_generation': 'generation-1',
            'yaml_content': yaml_content,
            'pool': 0,
            'lifecycle_epoch': 1,
            'provisional': 1,
            'created_at': 1.0,
        }
        select_result = mock.Mock()
        select_result.mappings.return_value.one_or_none.return_value = intent
        update_result = mock.Mock(rowcount=0)
        session = mock.Mock(spec=orm.Session)
        session.execute.side_effect = [select_result, update_result]

        with pytest.raises(
                ephemeral_storage_contract.EphemeralStorageContractError):
            serve_state._adopt_exact_ephemeral_storage_cleanup_intent(
                session, 'svc', 'incarnation-a', 'generation-1', yaml_content,
                False, 1, 2.0)

        assert session.execute.call_count == 2

    def test_initial_registration_accepts_already_committed_exact_intent(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-a', 'generation-1')
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-1', yaml_content, False, epoch,
            False)

        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch,
                                    resource_scope='incarnation-a',
                                    yaml_content=yaml_content)
        intent = serve_state.get_ephemeral_storage_cleanup_intents('svc')[0]
        assert intent['provisional'] == 0

    def test_initial_registration_requires_matching_intent(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-a', 'generation-1')

        with pytest.raises(
                ephemeral_storage_contract.EphemeralStorageContractError):
            _add_minimal_service('svc',
                                 service_hash='incarnation-a',
                                 lifecycle_epoch=epoch,
                                 resource_scope='incarnation-a',
                                 yaml_content=yaml_content)

        assert serve_state.get_service_from_name('svc') is None

    @pytest.mark.parametrize('mismatch', ['yaml', 'pool', 'stale_provisional'])
    def test_initial_registration_requires_exact_intent_preimage(
            self, _mock_serve_db, mismatch):
        intent_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        intent_yaml = self._yaml('incarnation-a', 'generation-1')
        committed_yaml = intent_yaml
        intent_pool = False
        if mismatch == 'yaml':
            committed_yaml += 'envs: {}\n'
        elif mismatch == 'pool':
            intent_pool = True
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-1', intent_yaml, intent_pool,
            intent_epoch, True)
        commit_epoch = intent_epoch
        if mismatch == 'stale_provisional':
            commit_epoch = serve_state.claim_service_lifecycle_epoch('svc')

        with pytest.raises(
                ephemeral_storage_contract.EphemeralStorageContractError):
            _add_minimal_service('svc',
                                 service_hash='incarnation-a',
                                 lifecycle_epoch=commit_epoch,
                                 resource_scope='incarnation-a',
                                 yaml_content=committed_yaml)

        assert serve_state.get_service_from_name('svc') is None
        intent = serve_state.get_ephemeral_storage_cleanup_intents('svc')[0]
        assert intent['provisional'] == 1

    def test_version_commit_adopts_new_generation_atomically(
            self, _mock_serve_db):
        first_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=first_epoch,
                                    resource_scope='incarnation-a')
        update_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-a', 'generation-2')
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-2', yaml_content, False,
            update_epoch, True)

        assert serve_state.add_or_update_version(
            'svc',
            2,
            _service_spec('generation-2'),
            yaml_content,
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=update_epoch,
            expected_controller_owner=(200, '10.0.0.2'))
        intent = serve_state.get_ephemeral_storage_cleanup_intents('svc')[0]
        assert intent['storage_generation'] == 'generation-2'
        assert intent['provisional'] == 0

    def test_version_commit_accepts_reused_committed_intent(
            self, _mock_serve_db):
        first_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        initial_yaml = self._yaml('incarnation-a', 'generation-1')
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-1', initial_yaml, False,
            first_epoch, False)
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=first_epoch,
                                    resource_scope='incarnation-a',
                                    yaml_content=initial_yaml)

        update_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        update_yaml = f'{initial_yaml}envs: {{}}\n'
        # Reusing one already-committed generation refreshes its exact YAML but
        # intentionally retains the original lifecycle/provisional ownership.
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-1', update_yaml, False,
            update_epoch, False)
        before = serve_state.get_ephemeral_storage_cleanup_intents('svc')[0]
        assert before['lifecycle_epoch'] == first_epoch
        assert before['provisional'] == 0
        assert before['yaml_content'] == update_yaml

        assert serve_state.add_or_update_version(
            'svc',
            2,
            _service_spec('generation-2'),
            update_yaml,
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=update_epoch,
            expected_controller_owner=(200, '10.0.0.2'))
        after = serve_state.get_ephemeral_storage_cleanup_intents('svc')[0]
        assert after['lifecycle_epoch'] == first_epoch
        assert after['provisional'] == 0

    def test_version_commit_rejects_future_committed_intent(
            self, _mock_serve_db):
        first_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=first_epoch,
                                    resource_scope='incarnation-a')
        update_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-a', 'generation-2')
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-2', yaml_content, False,
            update_epoch, False)
        intent_table = serve_state.ephemeral_storage_cleanup_intents_table
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(intent_table).where(
                    intent_table.c.service_name == 'svc').values(
                        lifecycle_epoch=update_epoch + 1))
            session.commit()

        with pytest.raises(
                ephemeral_storage_contract.EphemeralStorageContractError):
            serve_state.add_or_update_version(
                'svc',
                2,
                _service_spec('generation-2'),
                yaml_content,
                expected_service_hash='incarnation-a',
                expected_lifecycle_epoch=update_epoch,
                expected_controller_owner=(200, '10.0.0.2'))

        assert serve_state.get_yaml_content('svc', 2) is None

    def test_version_commit_rejects_intent_created_after_version(
            self, _mock_serve_db):
        first_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=first_epoch,
                                    resource_scope='incarnation-a')
        update_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-a', 'generation-2')
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-2', yaml_content, False,
            update_epoch, True)
        intent_table = serve_state.ephemeral_storage_cleanup_intents_table
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(intent_table).where(
                    intent_table.c.service_name == 'svc').values(
                        created_at=10**12))
            session.commit()

        with pytest.raises(
                ephemeral_storage_contract.EphemeralStorageContractError):
            serve_state.add_or_update_version(
                'svc',
                2,
                _service_spec('generation-2'),
                yaml_content,
                expected_service_hash='incarnation-a',
                expected_lifecycle_epoch=update_epoch,
                expected_controller_owner=(200, '10.0.0.2'))

        assert serve_state.get_yaml_content('svc', 2) is None

    def test_version_commit_rejects_malformed_parent_pool_bit(
            self, _mock_serve_db):
        first_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=first_epoch,
                                    resource_scope='incarnation-a')
        update_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-a', 'generation-2')
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-2', yaml_content, False,
            update_epoch, True)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == 'svc').values(pool=2))
            session.commit()

        with pytest.raises(
                ephemeral_storage_contract.EphemeralStorageContractError):
            serve_state.add_or_update_version(
                'svc',
                2,
                _service_spec('generation-2'),
                yaml_content,
                expected_service_hash='incarnation-a',
                expected_lifecycle_epoch=update_epoch,
                expected_controller_owner=(200, '10.0.0.2'))

        assert serve_state.get_yaml_content('svc', 2) is None

    def test_legacy_metadata_alias_blocks_initial_commit(self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-a', 'generation-1').replace(
            '_metadata:', 'metadata:', 1)
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-1', yaml_content, False, epoch,
            True)
        with pytest.raises(
                ephemeral_storage_contract.EphemeralStorageContractError):
            _add_minimal_service('svc',
                                 service_hash='incarnation-a',
                                 lifecycle_epoch=epoch,
                                 resource_scope='incarnation-a',
                                 yaml_content=yaml_content)

        intent = serve_state.get_ephemeral_storage_cleanup_intents('svc')[0]
        assert intent['provisional'] == 1

    def test_malformed_internal_metadata_blocks_initial_commit(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-a', 'generation-1').replace(
            '    storage_mounts: []\n', '')
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-1', yaml_content, False, epoch,
            True)

        with pytest.raises(
                ephemeral_storage_contract.EphemeralStorageContractError):
            _add_minimal_service('svc',
                                 service_hash='incarnation-a',
                                 lifecycle_epoch=epoch,
                                 resource_scope='incarnation-a',
                                 yaml_content=yaml_content)

        assert serve_state.get_service_from_name('svc') is None
        intent = serve_state.get_ephemeral_storage_cleanup_intents('svc')[0]
        assert intent['provisional'] == 1

    def test_scope_owner_mismatch_blocks_version_commit(self, _mock_serve_db):
        first_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=first_epoch,
                                    resource_scope='incarnation-a')
        update_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-b', 'generation-2')

        with pytest.raises(
                ephemeral_storage_contract.EphemeralStorageContractError):
            serve_state.add_or_update_version(
                'svc',
                2,
                _service_spec('generation-2'),
                yaml_content,
                expected_service_hash='incarnation-a',
                expected_lifecycle_epoch=update_epoch,
                expected_controller_owner=(200, '10.0.0.2'))

        assert serve_state.get_yaml_content('svc', 2) is None

    def test_version_commit_requires_matching_intent(self, _mock_serve_db):
        first_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=first_epoch,
                                    resource_scope='incarnation-a')
        update_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-a', 'generation-2')

        with pytest.raises(
                ephemeral_storage_contract.EphemeralStorageContractError):
            serve_state.add_or_update_version(
                'svc',
                2,
                _service_spec('generation-2'),
                yaml_content,
                expected_service_hash='incarnation-a',
                expected_lifecycle_epoch=update_epoch,
                expected_controller_owner=(200, '10.0.0.2'))

        assert serve_state.get_yaml_content('svc', 2) is None
        service = serve_state.get_service_from_name('svc')
        assert service is not None
        assert service['version'] == serve_constants.INITIAL_VERSION


class TestGetServiceFromNameReturnsControllerIp:
    """Joined service reads must surface the persisted controller IP."""

    def _add_with_version(self, service_name, controller_ip):
        # Reading via get_service_from_name requires a version_specs row
        # (it's an INNER JOIN); `add_service` writes the initial version row
        # in the same transaction, so this is enough for the JOIN to fire.
        _add_minimal_service(service_name, controller_ip=controller_ip)

    def test_round_trips_controller_ip(self, _mock_serve_db):
        self._add_with_version('svc-rt', controller_ip='10.4.10.8')
        record = serve_state.get_service_from_name('svc-rt')
        assert record is not None
        # The whole point: the dict KEY must exist with the persisted value.
        assert 'controller_ip' in record, (
            'controller_ip key missing from get_service_from_name() record — '
            'callers will silently fall back to localhost routing')
        assert record['controller_ip'] == '10.4.10.8'

    def test_round_trips_none_controller_ip(self, _mock_serve_db):
        # Even when the row was written without controller_ip (single-pod
        # mode), the dict must contain the key with value None — otherwise
        # `record.get('controller_ip')` and `record['controller_ip']` give
        # inconsistent answers.
        self._add_with_version('svc-rt-none', controller_ip=None)
        record = serve_state.get_service_from_name('svc-rt-none')
        assert record is not None
        assert 'controller_ip' in record
        assert record['controller_ip'] is None

    def test_record_includes_all_persisted_columns(self, _mock_serve_db):
        """Belt and braces: snapshot the keys we expect from the read path.
        If a future PR adds a column to services_table but forgets to update
        `_get_service_from_row`, this test fails loudly."""
        self._add_with_version('svc-rt-keys', controller_ip='10.0.0.1')
        record = serve_state.get_service_from_name('svc-rt-keys')
        assert record is not None
        for key in (
                'name',
                'status',
                'controller_pid',
                'controller_ip',
                'controller_port',
                'pool',
        ):
            assert key in record, f'missing key: {key}'


class TestGetServiceControllerOwner:
    """The proxy hot path reads one narrow services-table record."""

    def test_returns_only_routing_identity_without_loading_spec(
            self, _mock_serve_db, monkeypatch):
        _add_minimal_service('svc-owner', controller_ip='10.4.10.8')
        serve_state.set_service_controller_port('svc-owner', 20007)

        def fail_if_spec_loaded(*args, **kwargs):
            del args, kwargs
            raise AssertionError('owner lookup must not deserialize a spec')

        monkeypatch.setattr(serve_state, 'get_spec', fail_if_spec_loaded)
        record = serve_state.get_service_controller_owner('svc-owner')

        assert record is not None
        assert set(record) == {
            'hash',
            'status',
            'controller_pid',
            'controller_ip',
            'controller_port',
            'lifecycle_epoch',
            'pool',
            'resource_scope',
        }
        assert record['hash']
        assert record['status'] == serve_state.ServiceStatus.CONTROLLER_INIT
        assert record['controller_pid'] == 12345
        assert record['controller_ip'] == '10.4.10.8'
        assert record['controller_port'] == 20007
        assert record['pool'] is False

    def test_missing_row_returns_none(self, _mock_serve_db):
        assert serve_state.get_service_controller_owner('missing') is None

    def test_route_owner_state_requires_lb_state(self):
        with pytest.raises(ValueError,
                           match='include_route_owner_state requires'):
            serve_state.get_service_controller_owner(
                'svc-owner', include_route_owner_state=True)

    def test_lb_state_extension_includes_complete_cutover_row_in_one_query(
            self, _mock_serve_db):
        _add_minimal_service('svc-role-state', controller_ip='10.4.10.8')

        with _count_sql_statements(_mock_serve_db) as counts:
            record = serve_state.get_service_controller_owner(
                'svc-role-state', include_lb_state=True)

        assert record is not None
        assert record['lb_ha_enabled'] is False
        assert record['lb_active_slot'] is None
        assert record['lb_cutover_generation'] == 0
        assert record['lb_pending_slot'] is None
        assert record['lb_cutover_phase'] == 'STABLE'
        assert record['lb_drain_started_at'] is None
        assert counts['n'] == 1

    def test_require_version_rejects_orphan_service_row(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc-orphan')

        with _count_sql_statements(_mock_serve_db) as counts:
            record = serve_state.get_service_controller_owner(
                'svc-orphan', require_version=True)

        assert record is None
        assert counts['n'] == 1


class TestServiceReplicaLaunchAuthorization:
    """Launch generations follow the quarantine-aware recovery election."""

    def test_scale_zero_quarantine_uses_applied_not_intermediate_version(
            self, _mock_serve_db):
        owner = (321, '10.4.10.8')
        assert _add_minimal_service('svc-launch-fence',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    service_hash='incarnation-a',
                                    spec=_service_spec('spec-v1'))
        serve_state.set_service_status_and_active_versions(
            'svc-launch-fence',
            serve_state.ServiceStatus.READY,
            # A target of zero produces no READY replica routing versions.
            active_versions=[])
        legacy_authorization = (
            serve_state.get_service_replica_launch_authorization(
                'svc-launch-fence'))
        assert legacy_authorization is not None
        assert legacy_authorization['launch_version_required'] is False
        config = b'active_workspace: research\n'
        config_digest = hashlib.sha256(config).hexdigest()
        config_snapshot = (config, config_digest, 'a' * 64)
        assert serve_state.add_or_update_version(
            'svc-launch-fence',
            2,
            _service_spec('spec-v2'),
            'yaml: v2',
            ha_recovery_script=(
                f'{serve_constants.VERSIONED_HA_CONFIG_RECOVERY_MARKER}\n'
                'python -m sky.serve.service\n'),
            controller_config=config,
            controller_config_digest=config_digest,
            controller_config_snapshot_id='b' * 64,
            legacy_controller_config_snapshot=config_snapshot,
            legacy_controller_applied_version=1,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
        ) is serve_state.VersionCommitResult.COMMITTED
        # v2 is committed, but the controller never completed its runtime
        # transition and therefore has no applied receipt.
        assert _read_version_row(_mock_serve_db, 'svc-launch-fence',
                                 2)['controller_applied_at'] is None

        config_v3 = _config_snapshot(b'active_workspace: research-v3\n', 'c')
        assert serve_state.add_or_update_version(
            'svc-launch-fence',
            3,
            _service_spec('spec-v3'),
            'yaml: v3',
            ha_recovery_script=_VERSIONED_HA_SCRIPT,
            controller_config=config_v3[0],
            controller_config_digest=config_v3[1],
            controller_config_snapshot_id=config_v3[2]
        ) is serve_state.VersionCommitResult.COMMITTED
        assert _read_row(_mock_serve_db,
                         'svc-launch-fence')['current_version'] == 3
        committed_authorization = (
            serve_state.get_service_replica_launch_authorization(
                'svc-launch-fence'))
        assert committed_authorization is not None
        assert committed_authorization['launch_authorized_version'] == 3
        assert committed_authorization['launch_version_required'] is True
        assert serve_state.quarantine_version('svc-launch-fence', 3,
                                              'deterministic update failure')

        with _count_sql_statements(_mock_serve_db) as counts:
            authorization = (
                serve_state.get_service_replica_launch_authorization(
                    'svc-launch-fence'))

        assert counts['n'] == 1
        assert authorization is not None
        assert authorization['hash'] == 'incarnation-a'
        assert (authorization['controller_pid'],
                authorization['controller_ip']) == owner
        assert authorization['status'] == serve_state.ServiceStatus.READY
        # v2 is the newest non-quarantined commit, but v1 is the newest
        # generation actually applied by this controller. This remains exact
        # with no READY replicas and an empty active_versions routing set.
        assert authorization['launch_authorized_version'] == 1
        assert authorization['launch_version_required'] is True
        recovery = serve_state.get_recovery_version_spec('svc-launch-fence')
        assert _labeled_version_spec(recovery) == (1, 'spec-v1')

        config_v4 = _config_snapshot(b'active_workspace: research-v4\n', 'd')
        assert serve_state.add_or_update_version(
            'svc-launch-fence',
            4,
            _service_spec('spec-v4'),
            'yaml: v4',
            ha_recovery_script=_VERSIONED_HA_SCRIPT,
            controller_config=config_v4[0],
            controller_config_digest=config_v4[1],
            controller_config_snapshot_id=config_v4[2]
        ) is serve_state.VersionCommitResult.COMMITTED
        superseding_authorization = (
            serve_state.get_service_replica_launch_authorization(
                'svc-launch-fence'))
        assert superseding_authorization is not None
        assert superseding_authorization['launch_authorized_version'] == 4
        assert _labeled_version_spec(
            serve_state.get_recovery_version_spec('svc-launch-fence')) == (
                4, 'spec-v4')

    def test_first_protocol_commit_seeds_legacy_scale_zero_fallback(
            self, _mock_serve_db):
        owner = (654, '10.4.10.9')
        assert _add_minimal_service('svc-legacy-activation',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    service_hash='incarnation-legacy',
                                    spec=_service_spec('spec-v1'))
        # Revision 036 is additive and intentionally does not guess which
        # historical version an existing controller had applied.
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.version_specs_table).where(
                    serve_state.version_specs_table.c.service_name ==
                    'svc-legacy-activation',
                    serve_state.version_specs_table.c.version == 1).values(
                        controller_applied_at=None))
            session.commit()
        serve_state.set_service_status_and_active_versions(
            'svc-legacy-activation',
            serve_state.ServiceStatus.READY,
            active_versions=[])

        current_config = _config_snapshot(b'active_workspace: current\n', 'e')
        legacy_config = _config_snapshot(b'active_workspace: legacy\n', 'f')
        assert serve_state.add_or_update_version(
            'svc-legacy-activation',
            2,
            _service_spec('spec-v2'),
            'yaml: v2',
            ha_recovery_script=_VERSIONED_HA_SCRIPT,
            controller_config=current_config[0],
            controller_config_digest=current_config[1],
            controller_config_snapshot_id=current_config[2],
            legacy_controller_config_snapshot=legacy_config,
            legacy_controller_applied_version=1,
            expected_service_hash='incarnation-legacy',
            expected_controller_owner=owner,
        ) is serve_state.VersionCommitResult.COMMITTED
        assert _read_version_row(_mock_serve_db, 'svc-legacy-activation',
                                 1)['controller_applied_at'] is not None
        assert _read_version_row(_mock_serve_db, 'svc-legacy-activation',
                                 2)['controller_applied_at'] is None
        assert serve_state.quarantine_version(
            'svc-legacy-activation',
            2,
            'deterministic preflight failure',
            expected_service_hash='incarnation-legacy',
            expected_controller_owner=owner)

        authorization = serve_state.get_service_replica_launch_authorization(
            'svc-legacy-activation')
        assert authorization is not None
        assert authorization['launch_authorized_version'] == 1
        assert _labeled_version_spec(
            serve_state.get_recovery_version_spec('svc-legacy-activation')) == (
                1, 'spec-v1')

    def test_missing_service_has_no_launch_authorization(self, _mock_serve_db):
        assert serve_state.get_service_replica_launch_authorization(
            'missing') is None

    def test_partial_config_tuple_still_requires_versioned_launch(
            self, _mock_serve_db):
        assert _add_minimal_service('svc-partial-config',
                                    service_hash='incarnation-partial')
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.version_specs_table).where(
                    serve_state.version_specs_table.c.service_name ==
                    'svc-partial-config').values(controller_config_digest='a' *
                                                 64))
            session.commit()

        authorization = serve_state.get_service_replica_launch_authorization(
            'svc-partial-config')

        assert authorization is not None
        assert authorization['launch_authorized_version'] == 1
        assert authorization['launch_version_required'] is True


def test_system_recovery_snapshot_elects_applied_scale_zero_fallback(
        _mock_serve_db):
    assert _add_minimal_service('svc-system-recovery',
                                service_hash='incarnation-a',
                                workspace='research',
                                spec=_service_spec('spec-v1'))
    serve_state.set_service_status_and_active_versions(
        'svc-system-recovery',
        serve_state.ServiceStatus.NO_REPLICA,
        active_versions=[])
    assert serve_state.add_or_update_version(
        'svc-system-recovery', 2, _service_spec('spec-v2'),
        'yaml: v2') is serve_state.VersionCommitResult.COMMITTED
    assert serve_state.quarantine_version('svc-system-recovery', 2,
                                          'never applied')

    with _count_sql_statements(_mock_serve_db) as counts:
        snapshot = serve_state.get_system_recovery_authorization_snapshot(
            'svc-system-recovery')

    assert counts['n'] == 1
    assert snapshot is not None
    assert snapshot['version'] == 1
    assert snapshot['spec'].test_label == 'spec-v1'
    assert snapshot['yaml_content'] == 'yaml: v1'
    assert snapshot['quarantined_at'] is None
    assert snapshot['replica_count'] == 0


def test_ha_recovery_snapshot_is_one_fenced_quarantine_aware_read(
        _mock_serve_db, monkeypatch):
    owner = (9876, '10.5.0.7')
    lifecycle_epoch = serve_state.claim_service_lifecycle_epoch(
        'svc-ha-snapshot')
    v1_config = _config_snapshot(
        b'active_workspace: research\nworkspaces:\n  research: {}\n', '1')
    assert _add_minimal_service('svc-ha-snapshot',
                                controller_pid=owner[0],
                                controller_ip=owner[1],
                                service_hash='incarnation-a',
                                lifecycle_epoch=lifecycle_epoch,
                                resource_scope='scope-a',
                                workspace='research',
                                spec=_service_spec('spec-v1'),
                                controller_config=v1_config[0],
                                controller_config_digest=v1_config[1],
                                controller_config_snapshot_id=v1_config[2])
    serve_state.set_service_status_and_active_versions(
        'svc-ha-snapshot',
        serve_state.ServiceStatus.NO_REPLICA,
        active_versions=[])

    v2_config = _config_snapshot(b'active_workspace: research\n', '2')
    v3_config = _config_snapshot(b'active_workspace: research\n', '3')
    assert serve_state.add_or_update_version(
        'svc-ha-snapshot',
        2,
        _service_spec('spec-v2'),
        'yaml: v2',
        ha_recovery_script=_VERSIONED_HA_SCRIPT,
        controller_config=v2_config[0],
        controller_config_digest=v2_config[1],
        controller_config_snapshot_id=v2_config[2]
    ) is serve_state.VersionCommitResult.COMMITTED
    assert serve_state.add_or_update_version(
        'svc-ha-snapshot',
        3,
        _service_spec('spec-v3'),
        'yaml: v3',
        ha_recovery_script=_VERSIONED_HA_SCRIPT,
        controller_config=v3_config[0],
        controller_config_digest=v3_config[1],
        controller_config_snapshot_id=v3_config[2]
    ) is serve_state.VersionCommitResult.COMMITTED
    assert serve_state.quarantine_version('svc-ha-snapshot', 3, 'never applied')

    def _fail_if_spec_deserialized(*args, **kwargs):
        del args, kwargs
        raise AssertionError('HA authorization must not deserialize a spec')

    monkeypatch.setattr(serve_state.pickle, 'loads', _fail_if_spec_deserialized)
    with _count_sql_statements(_mock_serve_db) as counts:
        snapshot = serve_state.get_service_ha_recovery_snapshot(
            'svc-ha-snapshot', expected_service_hash='incarnation-a')

    assert counts['n'] == 1
    assert snapshot == {
        'service_name': 'svc-ha-snapshot',
        'hash': 'incarnation-a',
        'lifecycle_epoch': lifecycle_epoch,
        'controller_pid': owner[0],
        'controller_ip': owner[1],
        'workspace': 'research',
        'resource_scope': 'scope-a',
        'status': serve_state.ServiceStatus.NO_REPLICA,
        # v2 is newer but unproven; the dominant v3 quarantine falls back to
        # the initial controller-applied generation and its exact config tuple.
        'recovery_version': 1,
        'config_protocol_active': True,
        'controller_config_snapshot': v1_config,
        'ha_recovery_script': _VERSIONED_HA_SCRIPT,
    }


def test_ha_recovery_snapshot_fences_incarnation_and_legacy_protocol(
        _mock_serve_db):
    lifecycle_epoch = serve_state.claim_service_lifecycle_epoch('svc-ha-legacy')
    assert _add_minimal_service('svc-ha-legacy',
                                controller_pid=4321,
                                controller_ip='10.6.0.8',
                                service_hash='incarnation-current',
                                lifecycle_epoch=lifecycle_epoch,
                                resource_scope='scope-current',
                                workspace='research')

    with _count_sql_statements(_mock_serve_db) as counts:
        stale = serve_state.get_service_ha_recovery_snapshot(
            'svc-ha-legacy', expected_service_hash='incarnation-stale')
    assert counts['n'] == 1
    assert stale is None

    with _count_sql_statements(_mock_serve_db) as counts:
        current = serve_state.get_service_ha_recovery_snapshot(
            'svc-ha-legacy', expected_service_hash='incarnation-current')
    assert counts['n'] == 1
    assert current is not None
    assert current['recovery_version'] == 1
    assert current['config_protocol_active'] is False
    assert current['controller_config_snapshot'] is None
    assert current['ha_recovery_script'] is None


def test_ha_recovery_snapshot_rejects_selected_config_corruption(
        _mock_serve_db):
    config = _config_snapshot(b'active_workspace: research\n', '4')
    assert _add_minimal_service('svc-ha-corrupt',
                                service_hash='incarnation-corrupt',
                                workspace='research',
                                controller_config=config[0],
                                controller_config_digest=config[1],
                                controller_config_snapshot_id=config[2])
    with orm.Session(_mock_serve_db) as session:
        session.execute(
            sqlalchemy.update(serve_state.version_specs_table).where(
                serve_state.version_specs_table.c.service_name ==
                'svc-ha-corrupt').values(controller_config_digest='0' * 64))
        session.commit()

    with _count_sql_statements(_mock_serve_db) as counts, pytest.raises(
            serve_state.ControllerConfigCorruptionError,
            match='failed integrity validation'):
        serve_state.get_service_ha_recovery_snapshot(
            'svc-ha-corrupt', expected_service_hash='incarnation-corrupt')
    assert counts['n'] == 1


class TestGetServiceRuntimeSnapshot:
    """The controller hot paths should avoid the joined latest-spec read."""

    def test_returns_runtime_fields_without_loading_spec(
            self, _mock_serve_db, monkeypatch):
        spec = _service_spec(policy='qps=3', load_balancing_policy='least_load')
        _add_minimal_service('svc-runtime',
                             controller_ip='10.4.10.9',
                             spec=spec)
        serve_state.set_service_status_and_active_versions(
            'svc-runtime',
            serve_state.ServiceStatus.CONTROLLER_INIT,
            active_versions=[2])

        def fail_if_spec_loaded(*args, **kwargs):
            del args, kwargs
            raise AssertionError(
                'runtime snapshot must not deserialize the latest spec')

        monkeypatch.setattr(serve_state.pickle, 'loads', fail_if_spec_loaded)
        with _count_sql_statements(_mock_serve_db) as counts:
            record = serve_state.get_service_runtime_snapshot(
                'svc-runtime', require_version=True)

        assert counts['n'] == 1, counts
        assert record == {
            'hash': _read_row(_mock_serve_db, 'svc-runtime')['hash'],
            'controller_pid': 12345,
            'controller_ip': '10.4.10.9',
            'active_versions': [2],
        }

    def test_require_version_rejects_orphan_service_row(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc-orphan')

        with _count_sql_statements(_mock_serve_db) as counts:
            record = serve_state.get_service_runtime_snapshot(
                'svc-orphan', require_version=True)

        assert record is None
        assert counts['n'] == 1


def test_scale_planning_fingerprint_tracks_replica_and_runtime_mutations(
        _mock_serve_db):
    _add_minimal_service('svc-planning-fingerprint')
    replica = _replica(1)
    assert serve_state.add_or_update_replica('svc-planning-fingerprint', 1,
                                             replica)

    initial = serve_state.get_scale_planning_state_fingerprint(
        'svc-planning-fingerprint', require_version=True)
    assert initial is not None

    replica.resources_override = {'accelerators': {'A100': 1}}
    assert serve_state.add_or_update_replica('svc-planning-fingerprint',
                                             1,
                                             replica,
                                             expected_replica_exists=True)
    changed_replica = serve_state.get_scale_planning_state_fingerprint(
        'svc-planning-fingerprint', require_version=True)
    assert changed_replica is not None
    assert changed_replica != initial

    serve_state.set_service_status_and_active_versions(
        'svc-planning-fingerprint',
        serve_state.ServiceStatus.CONTROLLER_INIT,
        active_versions=[2])
    changed_runtime = serve_state.get_scale_planning_state_fingerprint(
        'svc-planning-fingerprint', require_version=True)
    assert changed_runtime is not None
    assert changed_runtime != changed_replica


class TestGetServiceStatusSnapshot:
    """Control paths read one slim, version-backed service row."""

    def test_returns_status_fields_without_loading_spec(self, _mock_serve_db,
                                                        monkeypatch):
        _add_minimal_service('svc-status',
                             controller_ip='10.4.10.10',
                             resource_scope='scope-a')
        serve_state.set_service_controller_port('svc-status', 20008)

        def fail_if_spec_loaded(*args, **kwargs):
            del args, kwargs
            raise AssertionError(
                'status snapshot must not deserialize the latest spec')

        monkeypatch.setattr(serve_state.pickle, 'loads', fail_if_spec_loaded)
        with _count_sql_statements(_mock_serve_db) as counts:
            record = serve_state.get_service_status_snapshot(
                'svc-status', require_version=True)

        row = _read_row(_mock_serve_db, 'svc-status')
        assert counts['n'] == 1, counts
        assert record == {
            'name': 'svc-status',
            'controller_job_id': 1,
            'controller_port': 20008,
            'load_balancer_port': None,
            'status': serve_state.ServiceStatus.CONTROLLER_INIT,
            'pool': False,
            'controller_pid': 12345,
            'controller_ip': '10.4.10.10',
            'hash': row['hash'],
            'lifecycle_epoch': row['lifecycle_epoch'],
            'resource_scope': 'scope-a',
            'workspace': None,
            'uptime': row['uptime'],
            'policy': 'policy',
            'requested_resources_str': '1x[CPU:1+]',
            'load_balancing_policy': 'round_robin',
            'tls_encrypted': False,
            'version': row['current_version'],
            'elected_version': row['current_version'],
            'active_versions': [],
            'logical_replica_semantics': False,
            'replica_unit': 'physical_backend',
        }

    def test_require_version_rejects_orphan_service_row(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc-orphan')

        with _count_sql_statements(_mock_serve_db) as counts:
            record = serve_state.get_service_status_snapshot(
                'svc-orphan', require_version=True)

        assert record is None
        assert counts['n'] == 1


class TestUpdateServiceControllerPidIpAndPort:
    """The atomic update is the core of the HA-recovery DB flip — it must
    write pid, ip, AND port in a single transaction so clients never
    observe a half-flipped row (e.g. new pid + old ip + stale port that
    points at a different service's listener on the new pod).

    Recovery picks port locally (find_free_port on the recovery pod);
    that new port has to land in DB together with pid/ip, otherwise
    a `_get_controller_url` consumer reading DB between writes could
    route to the new pod with the old port and hit the wrong listener.
    """

    def test_updates_all_three_fields(self, _mock_serve_db):
        _add_minimal_service('svc', controller_ip='10.0.0.7')
        serve_state.set_service_controller_port('svc', 20001)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']

        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=99999,
            controller_ip='10.0.0.8',
            controller_port=20007,
            expected_service_hash=service_hash,
            expected_controller_pid=12345,
            expected_controller_ip='10.0.0.7') is True

        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_pid'] == 99999
        assert record['controller_ip'] == '10.0.0.8'
        assert record['controller_port'] == 20007

    def test_can_clear_controller_ip(self, _mock_serve_db):
        # If we ever need to write None (e.g. controller is on a non-K8s pod
        # in a hybrid deploy), the column must accept NULL. Port is still
        # required (it's an int column, no NULL).
        _add_minimal_service('svc', controller_ip='10.0.0.7')
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=99999,
            controller_ip=None,
            controller_port=20007,
            expected_service_hash=service_hash,
            expected_controller_pid=12345,
            expected_controller_ip='10.0.0.7') is True
        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_pid'] == 99999
        assert record['controller_ip'] is None
        assert record['controller_port'] == 20007

    def test_no_op_when_service_missing(self, _mock_serve_db):
        # Should not raise if the row was deleted between read and write
        # (e.g. a `down` raced our recovery).
        assert serve_state.update_service_controller_pid_ip_and_port(
            'never-existed',
            controller_pid=1,
            controller_ip='10.0.0.7',
            controller_port=20001,
            expected_service_hash='missing-incarnation',
            expected_controller_pid=12345,
            expected_controller_ip='10.0.0.7') is False
        assert _read_row(_mock_serve_db, 'never-existed') is None

    def test_does_not_touch_other_fields(self, _mock_serve_db):
        # The atomic update must only touch the three specified columns —
        # don't want to clobber e.g. status, load_balancer_port from a
        # concurrent writer.
        _add_minimal_service('svc', controller_ip='10.0.0.7')
        serve_state.set_service_controller_port('svc', 20001)
        serve_state.set_service_load_balancer_port('svc', 30000)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']

        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=99999,
            controller_ip='10.0.0.8',
            controller_port=20007,
            expected_service_hash=service_hash,
            expected_controller_pid=12345,
            expected_controller_ip='10.0.0.7') is True

        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_pid'] == 99999
        assert record['controller_ip'] == '10.0.0.8'
        assert record['controller_port'] == 20007
        assert record['load_balancer_port'] == 30000  # untouched

    def test_rejects_purge_and_same_name_successor_with_same_pid(
            self, _mock_serve_db):
        lifecycle_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        _add_minimal_service('svc',
                             controller_ip='10.0.0.7',
                             controller_pid=777,
                             lifecycle_epoch=lifecycle_epoch)
        old_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', old_hash, 777, '10.0.0.7', 888, '10.0.0.8') is True

        teardown_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert serve_state.remove_service_completely(
            'svc', old_hash, expected_lifecycle_epoch=teardown_epoch)
        successor_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        _add_minimal_service('svc',
                             controller_ip='10.9.0.1',
                             controller_pid=888,
                             lifecycle_epoch=successor_epoch)
        successor = _read_row(_mock_serve_db, 'svc')
        assert successor['hash'] != old_hash

        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=888,
            controller_ip='10.0.0.8',
            controller_port=20007,
            expected_service_hash=old_hash,
            expected_controller_pid=888,
            expected_controller_ip='10.0.0.8') is False
        record = _read_row(_mock_serve_db, 'svc')
        assert record['hash'] == successor['hash']
        assert record['controller_ip'] == '10.9.0.1'
        assert record['controller_port'] is None

    def test_publish_requires_preclaimed_ip_when_pid_collides(
            self, _mock_serve_db):
        _add_minimal_service('svc',
                             controller_ip='10.0.0.1',
                             controller_pid=777,
                             service_hash='same-incarnation')
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', 'same-incarnation', 777, '10.0.0.1', 777, '10.0.0.2') is True

        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=777,
            controller_ip='10.0.0.1',
            controller_port=20001,
            expected_service_hash='same-incarnation',
            expected_controller_pid=777,
            expected_controller_ip='10.0.0.1') is False
        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=777,
            controller_ip='10.0.0.2',
            controller_port=20002,
            expected_service_hash='same-incarnation',
            expected_controller_pid=777,
            expected_controller_ip='10.0.0.2') is True
        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_ip'] == '10.0.0.2'
        assert record['controller_port'] == 20002


class TestUpdateServiceControllerPidIfOwner:
    """Recovery preclaim must fence by incarnation, pid, and controller IP."""

    def test_preclaim_requires_original_hash_and_pid(self, _mock_serve_db):
        _add_minimal_service('svc', controller_pid=111)
        serve_state.set_service_controller_port('svc', 20001)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', service_hash, 111, None, 222, '10.0.0.2') is True
        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_pid'] == 222
        assert record['controller_ip'] == '10.0.0.2'
        assert record['controller_port'] is None
        # A second recovery that read owner 111 before the first claim loses.
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', service_hash, 111, None, 333, '10.0.0.3') is False
        assert _read_row(_mock_serve_db, 'svc')['controller_pid'] == 222

    def test_preclaim_rejects_same_name_successor(self, _mock_serve_db):
        lifecycle_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        _add_minimal_service('svc',
                             controller_pid=111,
                             lifecycle_epoch=lifecycle_epoch)
        old_hash = _read_row(_mock_serve_db, 'svc')['hash']
        teardown_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert serve_state.remove_service_completely(
            'svc', old_hash, expected_lifecycle_epoch=teardown_epoch)
        # Deliberately reuse the same PID to model distinct Kubernetes pods.
        successor_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        _add_minimal_service('svc',
                             controller_pid=111,
                             lifecycle_epoch=successor_epoch)
        successor_hash = _read_row(_mock_serve_db, 'svc')['hash']

        assert serve_state.update_service_controller_pid_if_owner(
            'svc', old_hash, 111, None, 222, '10.0.0.2') is False
        record = _read_row(_mock_serve_db, 'svc')
        assert record['hash'] == successor_hash
        assert record['controller_pid'] == 111

    def test_ip_fences_equal_pids_within_same_incarnation(self, _mock_serve_db):
        _add_minimal_service('svc',
                             controller_pid=111,
                             controller_ip='10.0.0.1',
                             service_hash='same-incarnation')
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', 'same-incarnation', 111, '10.0.0.1', 111, '10.0.0.2') is True
        # A stale parent on the old pod can have the same namespace-local PID,
        # but its old IP is no longer authoritative.
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', 'same-incarnation', 111, '10.0.0.1', 111,
            '10.0.0.3') is False
        assert _read_row(_mock_serve_db, 'svc')['controller_ip'] == '10.0.0.2'

    def test_preclaim_atomically_fences_lifecycle_status_and_version(
            self, _mock_serve_db):
        lifecycle_epoch = serve_state.claim_service_lifecycle_epoch(
            'svc-jit-fence')
        assert _add_minimal_service('svc-jit-fence',
                                    controller_pid=111,
                                    controller_ip='10.0.0.1',
                                    service_hash='same-incarnation',
                                    lifecycle_epoch=lifecycle_epoch)
        kwargs = {
            'service_name': 'svc-jit-fence',
            'expected_service_hash': 'same-incarnation',
            'expected_controller_pid': 111,
            'expected_controller_ip': '10.0.0.1',
            'controller_pid': 222,
            'controller_ip': '10.0.0.2',
        }
        assert not serve_state.update_service_controller_pid_if_owner(
            **kwargs,
            expected_lifecycle_epoch=lifecycle_epoch + 1,
            expected_status=serve_state.ServiceStatus.CONTROLLER_INIT,
            expected_recovery_version=1)
        assert not serve_state.update_service_controller_pid_if_owner(
            **kwargs,
            expected_lifecycle_epoch=lifecycle_epoch,
            expected_status=serve_state.ServiceStatus.READY,
            expected_recovery_version=1)
        assert not serve_state.update_service_controller_pid_if_owner(
            **kwargs,
            expected_lifecycle_epoch=lifecycle_epoch,
            expected_status=serve_state.ServiceStatus.CONTROLLER_INIT,
            expected_recovery_version=2)
        assert serve_state.update_service_controller_pid_if_owner(
            **kwargs,
            expected_lifecycle_epoch=lifecycle_epoch,
            expected_status=serve_state.ServiceStatus.CONTROLLER_INIT,
            expected_recovery_version=1)


class TestSetServiceControllerPortIfOwner:
    """Compare-and-swap port write for the in-place controller respawn: the
    UPDATE must be filtered on hash/PID/IP so a parent whose row was taken over
    by HA recovery cannot clobber the new owner's port."""

    def test_owner_updates_port(self, _mock_serve_db):
        _add_minimal_service('svc')  # seeds controller_pid=12345
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_controller_port_if_owner(
            'svc', service_hash, 12345, None, 20123) is True
        assert _read_row(_mock_serve_db, 'svc')['controller_port'] == 20123

    def test_non_owner_is_rejected(self, _mock_serve_db):
        _add_minimal_service('svc')
        serve_state.set_service_controller_port('svc', 20123)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_controller_port_if_owner(
            'svc', service_hash, 99999, None, 20999) is False
        assert _read_row(_mock_serve_db, 'svc')['controller_port'] == 20123

    def test_missing_service_returns_false(self, _mock_serve_db):
        assert serve_state.set_service_controller_port_if_owner(
            'never-existed', 'missing-hash', 12345, None, 20123) is False

    def test_same_pid_successor_is_rejected_by_hash(self, _mock_serve_db):
        lifecycle_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        _add_minimal_service('svc',
                             controller_pid=12345,
                             lifecycle_epoch=lifecycle_epoch)
        old_hash = _read_row(_mock_serve_db, 'svc')['hash']
        teardown_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert serve_state.remove_service_completely(
            'svc', old_hash, expected_lifecycle_epoch=teardown_epoch)
        successor_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        _add_minimal_service('svc',
                             controller_pid=12345,
                             lifecycle_epoch=successor_epoch)
        assert serve_state.set_service_controller_port_if_owner(
            'svc', old_hash, 12345, None, 20123) is False
        assert _read_row(_mock_serve_db, 'svc')['controller_port'] is None


class TestAcknowledgeControllerTeardown:
    """Owner-only teardown ack must publish the terminal controller port."""

    def test_owner_atomically_publishes_terminal_status_and_ack(
            self, _mock_serve_db):
        _add_minimal_service('svc', service_hash='incarnation-a')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.READY)

        assert serve_state.acknowledge_service_controller_teardown_if_owner(
            'svc', 'incarnation-a', 12345, None)

        row = _read_row(_mock_serve_db, 'svc')
        assert row['status'] == serve_state.ServiceStatus.SHUTTING_DOWN.value
        assert (row['controller_port'] ==
                serve_constants.CONTROLLER_TEARDOWN_ACK_PORT)

    def test_stale_owner_cannot_change_successor_status(self, _mock_serve_db):
        _add_minimal_service('svc', service_hash='incarnation-b')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.READY)

        assert not serve_state.acknowledge_service_controller_teardown_if_owner(
            'svc', 'incarnation-a', 12345, None)
        assert (_read_row(
            _mock_serve_db,
            'svc')['status'] == serve_state.ServiceStatus.READY.value)


class TestSetServiceLoadBalancerPortIfOwner:
    """CAS port write for recovery's external-LB republish: the UPDATE is
    filtered on hash/PID/IP so a stale recovery cannot write to a
    same-name successor's row."""

    def test_owner_updates_port(self, _mock_serve_db):
        _add_minimal_service('svc')  # seeds controller_pid=12345
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_load_balancer_port_if_owner(
            'svc', service_hash, 12345, None, 30001) is True
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] == 30001

    def test_owner_updates_null_port(self, _mock_serve_db):
        # NULL -> port must work for the owner: recovery of an up() that
        # crashed before registration is the case this setter exists for.
        _add_minimal_service('svc')
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] is None
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_load_balancer_port_if_owner(
            'svc', service_hash, 12345, None, 30001) is True
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] == 30001

    def test_non_owner_is_rejected(self, _mock_serve_db):
        _add_minimal_service('svc')
        serve_state.set_service_load_balancer_port('svc', 30002)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_load_balancer_port_if_owner(
            'svc', service_hash, 99999, None, 30001) is False
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] == 30002

    def test_missing_service_returns_false(self, _mock_serve_db):
        assert serve_state.set_service_load_balancer_port_if_owner(
            'never-existed', 'missing-hash', 12345, None, 30001) is False

    def test_same_pid_successor_is_rejected_by_hash(self, _mock_serve_db):
        lifecycle_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        _add_minimal_service('svc',
                             controller_pid=12345,
                             lifecycle_epoch=lifecycle_epoch)
        old_hash = _read_row(_mock_serve_db, 'svc')['hash']
        teardown_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert serve_state.remove_service_completely(
            'svc', old_hash, expected_lifecycle_epoch=teardown_epoch)
        successor_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        _add_minimal_service('svc',
                             controller_pid=12345,
                             lifecycle_epoch=successor_epoch)
        assert serve_state.set_service_load_balancer_port_if_owner(
            'svc', old_hash, 12345, None, 30001) is False
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] is None


class TestSetServiceControllerIp:
    """Standalone IP setter is rarely called (the atomic version is preferred
    for recovery), but kept for symmetry with set_service_controller_port."""

    def test_basic(self, _mock_serve_db):
        _add_minimal_service('svc', controller_ip=None)
        serve_state.set_service_controller_ip('svc', '10.0.0.9')
        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_ip'] == '10.0.0.9'
        # pid unchanged
        assert record['controller_pid'] == 12345


class TestRemoveServiceCompletely:
    """`remove_service_completely` deletes services / version_specs /
    serve_ha_recovery_script in one transaction. Sequential deletes had
    a real-world failure mode where the last call
    was the one most likely to be skipped
    when the subprocess died mid-cleanup, leaving the row orphaned across
    many test runs while the other tables stayed clean. This guarantees
    all-or-nothing teardown for service metadata.
    """

    def _populate(self, engine, name):
        # Seed all service tables plus a replica row so the exact incarnation
        # can be removed atomically.
        lifecycle_epoch = serve_state.claim_service_lifecycle_epoch(name)
        _add_minimal_service(name,
                             controller_ip='10.0.0.1',
                             lifecycle_epoch=lifecycle_epoch)
        serve_state.add_version(name)
        serve_state.set_ha_recovery_script(name, 'dummy script')
        # replicas: a minimal pickled ReplicaInfo proxy is hard to
        # construct here, so just write a row directly to the table.
        with orm.Session(engine) as session:
            session.execute(serve_state.replicas_table.insert().values(
                service_name=name, replica_id=1, replica_info=b'fake-pickle'))
            session.commit()

    def test_failed_cleanup_status_retains_metadata_and_reserves_name(
            self, _mock_serve_db):
        self._populate(_mock_serve_db, 'svc-retained')
        service_hash = _read_row(_mock_serve_db, 'svc-retained')['hash']

        assert serve_state.set_service_status_and_active_versions_if_owner(
            'svc-retained',
            service_hash,
            12345,
            '10.0.0.1',
            serve_state.ServiceStatus.FAILED_CLEANUP,
            expected_status=serve_state.ServiceStatus.CONTROLLER_INIT)

        with orm.Session(_mock_serve_db) as session:
            for _, column in [
                (serve_state.services_table, serve_state.services_table.c.name),
                (serve_state.replicas_table,
                 serve_state.replicas_table.c.service_name),
                (serve_state.version_specs_table,
                 serve_state.version_specs_table.c.service_name),
                (serve_state.serve_ha_recovery_script_table,
                 serve_state.serve_ha_recovery_script_table.c.service_name),
            ]:
                assert session.execute(
                    sqlalchemy.select(column).where(
                        column == 'svc-retained')).first() is not None
        # The durable services row keeps same-name H_new from registering.
        assert not _add_minimal_service('svc-retained')

    def test_removes_service_metadata_tables(self, _mock_serve_db):
        self._populate(_mock_serve_db, 'svc-rsc')
        # Sanity: rows are present.
        with orm.Session(_mock_serve_db) as session:
            assert session.execute(
                sqlalchemy.select(serve_state.services_table.c.name).where(
                    serve_state.services_table.c.name ==
                    'svc-rsc')).first() is not None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.serve_ha_recovery_script_table.c.service_name).
                where(serve_state.serve_ha_recovery_script_table.c.service_name
                      == 'svc-rsc')).first() is not None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.service_name).where(
                        serve_state.replicas_table.c.service_name ==
                        'svc-rsc')).first() is not None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.version_specs_table.c.service_name).where(
                        serve_state.version_specs_table.c.service_name ==
                        'svc-rsc')).first() is not None

        service_hash = _read_row(_mock_serve_db, 'svc-rsc')['hash']
        teardown_epoch = serve_state.claim_service_lifecycle_epoch('svc-rsc')
        assert serve_state.remove_service_completely(
            'svc-rsc', service_hash, expected_lifecycle_epoch=teardown_epoch)

        # The three metadata tables must be gone.
        with orm.Session(_mock_serve_db) as session:
            assert session.execute(
                sqlalchemy.select(serve_state.services_table.c.name).where(
                    serve_state.services_table.c.name ==
                    'svc-rsc')).first() is None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.serve_ha_recovery_script_table.c.service_name).
                where(serve_state.serve_ha_recovery_script_table.c.service_name
                      == 'svc-rsc')).first() is None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.version_specs_table.c.service_name).where(
                        serve_state.version_specs_table.c.service_name ==
                        'svc-rsc')).first() is None
            # Replica rows are children of the incarnation and must be gone in
            # the same transaction, or a stale purge can leave rows that a
            # same-name successor reads as its own.
            assert session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.service_name).where(
                        serve_state.replicas_table.c.service_name ==
                        'svc-rsc')).first() is None

    def test_does_not_touch_other_services(self, _mock_serve_db):
        """Make sure deletion is scoped to the named service."""
        self._populate(_mock_serve_db, 'svc-keep')
        self._populate(_mock_serve_db, 'svc-drop')

        drop_hash = _read_row(_mock_serve_db, 'svc-drop')['hash']
        teardown_epoch = serve_state.claim_service_lifecycle_epoch('svc-drop')
        assert serve_state.remove_service_completely(
            'svc-drop', drop_hash, expected_lifecycle_epoch=teardown_epoch)

        # svc-keep's rows must all survive.
        with orm.Session(_mock_serve_db) as session:
            for tbl_name, tbl, col_name in [
                ('services', serve_state.services_table, 'name'),
                ('version_specs', serve_state.version_specs_table,
                 'service_name'),
                ('serve_ha_recovery_script',
                 serve_state.serve_ha_recovery_script_table, 'service_name'),
                ('replicas', serve_state.replicas_table, 'service_name'),
            ]:
                col = getattr(tbl.c, col_name)
                assert session.execute(
                    sqlalchemy.select(col).where(
                        col == 'svc-keep')).first() is not None, (
                            f'svc-keep was wrongly removed from {tbl_name}')

    def test_no_op_when_nothing_to_delete(self, _mock_serve_db):
        # Should be a silent no-op when no rows exist.
        assert not serve_state.remove_service_completely(
            'never-existed', 'missing-hash')

    def test_stale_a_delete_spares_successor_b_and_all_children(
            self, _mock_serve_db):
        self._populate(_mock_serve_db, 'svc')
        hash_a = _read_row(_mock_serve_db, 'svc')['hash']
        teardown_epoch_a = serve_state.claim_service_lifecycle_epoch('svc')
        assert serve_state.remove_service_completely(
            'svc', hash_a, expected_lifecycle_epoch=teardown_epoch_a)

        self._populate(_mock_serve_db, 'svc')
        hash_b = _read_row(_mock_serve_db, 'svc')['hash']
        assert hash_b != hash_a

        assert not serve_state.remove_service_completely(
            'svc', hash_a, expected_lifecycle_epoch=teardown_epoch_a)
        assert _read_row(_mock_serve_db, 'svc')['hash'] == hash_b
        with orm.Session(_mock_serve_db) as session:
            for table, column in [
                (serve_state.replicas_table,
                 serve_state.replicas_table.c.service_name),
                (serve_state.version_specs_table,
                 serve_state.version_specs_table.c.service_name),
                (serve_state.serve_ha_recovery_script_table,
                 serve_state.serve_ha_recovery_script_table.c.service_name),
            ]:
                assert session.execute(
                    sqlalchemy.select(column).where(
                        column == 'svc')).first() is not None, table.name


class TestTerminalVersionFences:
    """A down/purge winner must prevent both phases of update commit."""

    def test_terminal_status_rejects_placeholder_and_yaml_commit(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch)
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.SHUTTING_DOWN)

        with pytest.raises(RuntimeError, match='terminal status'):
            serve_state.add_version('svc',
                                    expected_service_hash='incarnation-a',
                                    expected_lifecycle_epoch=epoch)
        assert not serve_state.add_or_update_version(
            'svc',
            2,
            _service_spec('terminal'),
            'yaml: v2',
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch,
            expected_controller_owner=(200, '10.0.0.2'))
        assert serve_state.get_latest_version('svc') == 1


class TestUnrecoverableServiceRows:
    """No-yaml rows make HA recovery impossible and must stay purgeable."""

    def test_ha_retirement_marks_terminal_and_removes_script(
            self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        assert serve_state.set_ha_recovery_script('svc', 'impossible script')

        assert serve_state.mark_unrecoverable_service_for_cleanup('svc',
                                                                  'orphan',
                                                                  pool=False)
        assert _read_row(
            _mock_serve_db,
            'svc')['status'] == serve_state.ServiceStatus.FAILED_CLEANUP.value
        assert serve_state.get_ha_recovery_script('svc') is None

    def test_purge_claim_consumes_impossible_script(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.SHUTTING_DOWN)
        assert serve_state.set_ha_recovery_script('svc', 'impossible script')

        assert serve_state.claim_unrecoverable_service_teardown(
            'svc', 'orphan', 12345, None, 999, '10.0.0.9')
        owner = serve_state.get_service_controller_owner('svc')
        assert owner is not None
        assert owner['controller_pid'] == 999
        assert owner['controller_ip'] == '10.0.0.9'
        assert (owner['controller_port'] ==
                serve_constants.CONTROLLER_TEARDOWN_ACK_PORT)
        assert serve_state.get_ha_recovery_script('svc') is None

    def test_ha_retirement_cannot_overwrite_active_purge(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.SHUTTING_DOWN)
        assert serve_state.set_ha_recovery_script('svc', 'impossible script')

        assert not serve_state.mark_unrecoverable_service_for_cleanup(
            'svc', 'orphan', pool=False)
        assert _read_row(
            _mock_serve_db,
            'svc')['status'] == serve_state.ServiceStatus.SHUTTING_DOWN.value
        assert (
            serve_state.get_ha_recovery_script('svc') == 'impossible script')

    def test_ha_retirement_rechecks_concurrent_committed_version(
            self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        assert serve_state.set_ha_recovery_script('svc', 'bootable script')
        serve_state.add_or_update_version('svc', 1, _service_spec('spec-1'),
                                          'yaml: v1')

        assert not serve_state.mark_unrecoverable_service_for_cleanup(
            'svc', 'orphan', pool=False)
        assert serve_state.get_ha_recovery_script('svc') == 'bootable script'

    def test_purge_claim_refuses_committed_version(self, _mock_serve_db):
        assert _add_minimal_service('svc', service_hash='incarnation-a')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.SHUTTING_DOWN)
        assert serve_state.set_ha_recovery_script('svc', 'valid script')

        assert not serve_state.claim_unrecoverable_service_teardown(
            'svc', 'incarnation-a', 12345, None, 999, '10.0.0.9')
        assert serve_state.get_ha_recovery_script('svc') == 'valid script'


class TestRecoveryVersionSelection:
    """An interrupted `sky serve update` leaves a NULL-yaml placeholder
    version (written by `add_version`) as MAX(version). Recovery must resume
    the latest version whose yaml was actually committed, otherwise the
    controller crash-loops asserting on the NULL yaml. These tests pin that
    `get_latest_committed_version` skips the placeholder."""

    def test_committed_version_skips_placeholder(self, _mock_serve_db):
        # Two committed versions, then an interrupted update creates a
        # NULL-yaml placeholder v3 as the new MAX.
        serve_state.add_or_update_version('svc', 1, _service_spec('spec-1'),
                                          'yaml: v1')
        serve_state.add_or_update_version('svc', 2, _service_spec('spec-2'),
                                          'yaml: v2')
        assert serve_state.add_version('svc') == 3  # placeholder
        # Raw MAX points at the placeholder (the bug)...
        assert serve_state.get_latest_version('svc') == 3
        # ...but recovery must pick the newest committed version, not the
        # placeholder and not the original.
        assert serve_state.get_latest_committed_version('svc') == 2

    def test_committed_version_none_when_only_placeholder(self, _mock_serve_db):
        serve_state.add_version('svc')  # placeholder v1, no committed yaml
        assert serve_state.get_latest_committed_version('svc') is None

    def test_batch_committed_versions_match_single_row_answers(
            self, _mock_serve_db):
        serve_state.add_or_update_version('svc-a', 1, _service_spec('spec-a1'),
                                          'yaml: a1')
        serve_state.add_or_update_version('svc-a', 2, _service_spec('spec-a2'),
                                          'yaml: a2')
        serve_state.add_version('svc-a')  # placeholder v3
        serve_state.add_version('svc-b')  # placeholder v1, never committed
        serve_state.add_or_update_version('svc-c', 1, _service_spec('spec-c1'),
                                          'yaml: c1')

        with _count_sql_statements(_mock_serve_db) as counts:
            committed_versions = serve_state.get_latest_committed_versions(
                ['svc-a', 'svc-b', 'svc-c', 'missing', 'svc-a'])

        assert counts['n'] == 1
        assert committed_versions == {
            'svc-a': 2,
            'svc-c': 1,
        }
        assert committed_versions.get('svc-a') == (
            serve_state.get_latest_committed_version('svc-a'))
        assert committed_versions.get('svc-c') == (
            serve_state.get_latest_committed_version('svc-c'))
        assert 'svc-b' not in committed_versions
        assert 'missing' not in committed_versions
        assert serve_state.get_latest_committed_version('svc-b') is None

    def test_batch_service_mode_and_hashes_match_single_row_answers(
            self, _mock_serve_db):
        assert _add_minimal_service('svc-a', service_hash='hash-a', pool=False)
        assert _add_minimal_service('svc-b', service_hash='hash-b', pool=True)

        with _count_sql_statements(_mock_serve_db) as counts:
            identities = serve_state.get_service_mode_and_hashes(
                ['svc-a', 'svc-b', 'missing', 'svc-a'])

        assert counts['n'] == 1
        assert identities == {
            'svc-a': (False, 'hash-a'),
            'svc-b': (True, 'hash-b'),
        }
        assert identities.get('svc-a') == serve_state.get_service_mode_and_hash(
            'svc-a')
        assert identities.get('svc-b') == serve_state.get_service_mode_and_hash(
            'svc-b')
        assert 'missing' not in identities
        assert serve_state.get_service_mode_and_hash('missing') is None

    @pytest.mark.parametrize('getter_name', [
        'get_latest_committed_versions',
        'get_service_mode_and_hashes',
    ])
    def test_batch_recovery_fallbacks_bound_empty_and_chunked_queries(
            self, _mock_serve_db, getter_name):
        getter = getattr(serve_state, getter_name)
        with _count_sql_statements(_mock_serve_db) as empty_counts:
            assert getter([]) == {}
        assert empty_counts['n'] == 0

        batch_size = serve_state._TERMINAL_IDENTITY_QUERY_BATCH_SIZE
        missing_names = [f'missing-{i}' for i in range(batch_size + 1)]
        with _count_sql_statements(_mock_serve_db) as chunked_counts:
            assert getter(missing_names + [missing_names[0]]) == {}
        assert chunked_counts['n'] == 2

    @pytest.mark.parametrize('getter_name', [
        'get_latest_committed_versions',
        'get_service_mode_and_hashes',
    ])
    def test_batch_recovery_fallbacks_return_second_chunk_values(
            self, _mock_serve_db, getter_name):
        # Regression guard for PR #911: the batched HA-recovery reads
        # accumulate results across `_TERMINAL_IDENTITY_QUERY_BATCH_SIZE`
        # chunks via `rows.extend(...)`. The sibling ...bound_empty_and_chunked
        # test requests only all-missing names, so it asserts the statement
        # count but never that a real row landing in the SECOND chunk survives
        # accumulation. A refactor that dropped later chunks (e.g. `rows =`
        # instead of `rows.extend`, or an early `break`) would still issue two
        # statements yet silently lose the second-chunk service. This pins that
        # a real second-chunk value is returned and equals the single-row read.
        batch_size = serve_state._TERMINAL_IDENTITY_QUERY_BATCH_SIZE
        # The getters read sorted(set(names)); a 'zzz-' name sorts after the
        # `batch_size` fillers, landing at index == batch_size (first row of
        # the second chunk).
        filler_names = [f'chunk-miss-{i:04d}' for i in range(batch_size)]
        real_name = 'zzz-second-chunk-svc'
        getter = getattr(serve_state, getter_name)
        if getter_name == 'get_latest_committed_versions':
            serve_state.add_or_update_version(real_name, 1,
                                              _service_spec('spec-1'),
                                              'yaml: v1')
            serve_state.add_or_update_version(real_name, 2,
                                              _service_spec('spec-2'),
                                              'yaml: v2')
            serve_state.add_version(real_name)  # uncommitted placeholder v3
            expected_value = 2
            single_row_value = serve_state.get_latest_committed_version(
                real_name)
        else:
            assert _add_minimal_service(real_name,
                                        service_hash='hash-z',
                                        pool=True)
            expected_value = (True, 'hash-z')
            single_row_value = serve_state.get_service_mode_and_hash(real_name)

        with _count_sql_statements(_mock_serve_db) as counts:
            result = getter(filler_names + [real_name])

        assert counts['n'] == 2, counts
        assert result == {real_name: expected_value}
        assert result.get(real_name) == single_row_value

    def test_version_records_include_commit_provenance(self, _mock_serve_db,
                                                       monkeypatch):
        timestamps = iter([1000.0, 1001.0])
        monkeypatch.setattr(serve_state.time, 'time', lambda: next(timestamps))
        assert _add_minimal_service('svc',
                                    spec=_service_spec('spec-1'),
                                    created_by='alice',
                                    submitted_yaml_content='submitted: v1')
        assert serve_state.add_version('svc', created_by='bob') == 2
        serve_state.add_or_update_version(
            'svc',
            2,
            _service_spec('spec-2'),
            'yaml: v2',
            submitted_yaml_content='submitted: v2')

        records = serve_state.get_version_records('svc')
        for record in records:
            record['spec'] = record['spec'].test_label
        assert records == [{
            'version': 1,
            'spec': 'spec-1',
            'yaml_content': 'yaml: v1',
            'submitted_yaml_content': 'submitted: v1',
            'created_at': 1000.0,
            'created_by': 'alice',
            'quarantined_at': None,
            'quarantine_reason': None,
            'controller_job_projection': None,
            'controller_work_cache': None,
            'worker_placement_projections': None,
        }, {
            'version': 2,
            'spec': 'spec-2',
            'yaml_content': 'yaml: v2',
            'submitted_yaml_content': 'submitted: v2',
            'created_at': 1001.0,
            'created_by': 'bob',
            'quarantined_at': None,
            'quarantine_reason': None,
            'controller_job_projection': None,
            'controller_work_cache': None,
            'worker_placement_projections': None,
        }]

        version_two = serve_state.get_version_record('svc', 2)
        assert version_two is not None
        assert version_two['version'] == 2
        assert version_two['spec'].test_label == 'spec-2'
        assert version_two['yaml_content'] == 'yaml: v2'
        assert serve_state.get_version_record('svc', 3) is None
        with pytest.raises(ValueError, match='positive integer'):
            serve_state.get_version_record('svc', True)

        metadata = serve_state.get_version_records('svc', include_yaml=False)
        assert [record['version'] for record in metadata] == [1, 2]
        assert all(record['yaml_content'] is None for record in metadata)
        assert all(
            record['submitted_yaml_content'] is None for record in metadata)

    def test_quarantine_is_durable_and_applicable_snapshot_skips_it(
            self, _mock_serve_db):
        serve_state.add_or_update_version('svc', 1, _service_spec('spec-1'),
                                          'yaml: v1')
        serve_state.add_or_update_version('svc', 2, _service_spec('spec-2'),
                                          'yaml: v2')

        assert serve_state.quarantine_version('svc',
                                              2,
                                              'deterministic port failure',
                                              quarantined_at=123.0)
        assert serve_state.get_latest_committed_version('svc') == 2
        assert _labeled_version_spec(
            serve_state.get_latest_committed_version_spec('svc')) == (2,
                                                                      'spec-2')
        assert _labeled_version_spec(
            serve_state.get_latest_applicable_version_spec('svc')) == (1,
                                                                       'spec-1')
        assert serve_state.get_latest_quarantined_version('svc') == {
            'version': 2,
            'quarantined_at': 123.0,
            'quarantine_reason': 'deterministic port failure',
        }

        serve_state.add_or_update_version('svc', 3, _service_spec('spec-3'),
                                          'yaml: v3')
        assert _labeled_version_spec(
            serve_state.get_latest_applicable_version_spec('svc')) == (3,
                                                                       'spec-3')

    def test_recovery_prefers_proven_applied_version_below_quarantine(
            self, _mock_serve_db):
        assert _add_minimal_service('svc', spec=_service_spec('spec-1'))
        serve_state.add_or_update_version('svc', 2, _service_spec('spec-2'),
                                          'yaml: v2')
        serve_state.add_or_update_version('svc', 3, _service_spec('spec-3'),
                                          'yaml: v3')
        # At target zero there are no routing versions to consult. v1 remains
        # a safe fallback because registration durably recorded it as applied.
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.READY, active_versions=[])

        assert serve_state.quarantine_version('svc', 3, 'never ready')
        # Version 2 is committed but its runtime transition never completed.
        assert _labeled_version_spec(
            serve_state.get_latest_applicable_version_spec('svc')) == (2,
                                                                       'spec-2')
        assert _labeled_version_spec(
            serve_state.get_recovery_version_spec('svc')) == (1, 'spec-1')

        # A later commit supersedes the quarantine and remains eligible for a
        # fresh rollout on recovery.
        serve_state.add_or_update_version('svc', 4, _service_spec('spec-4'),
                                          'yaml: v4')
        assert _labeled_version_spec(
            serve_state.get_recovery_version_spec('svc')) == (4, 'spec-4')

    def test_lb_grace_uses_applied_version_below_dominant_quarantine(
            self, _mock_serve_db, monkeypatch):
        v1 = _service_spec('v1',
                           lb_stream_timeout_seconds=11,
                           graceful_drain_seconds=21)
        v2 = _service_spec('v2',
                           lb_stream_timeout_seconds=12,
                           graceful_drain_seconds=22)
        v3 = _service_spec('v3',
                           lb_stream_timeout_seconds=13,
                           graceful_drain_seconds=23)
        assert _add_minimal_service('svc-grace', spec=v1)
        assert serve_state.add_or_update_version(
            'svc-grace', 2, v2,
            'yaml: v2') is serve_state.VersionCommitResult.COMMITTED
        assert serve_state.add_or_update_version(
            'svc-grace', 3, v3,
            'yaml: v3') is serve_state.VersionCommitResult.COMMITTED
        assert serve_state.quarantine_version('svc-grace', 3, 'never applied')
        observed = []

        def _grace(stream_timeout, drain_timeout):
            observed.append((stream_timeout, drain_timeout))
            return 123

        monkeypatch.setattr(service_lib.lb_k8s,
                            'lb_termination_grace_period_seconds', _grace)

        assert (service_lib._get_latest_committed_lb_termination_grace_seconds(
            'svc-grace') == 123)
        assert observed == [(11, 21)]

    def test_controller_applied_receipt_is_owner_fenced_and_idempotent(
            self, _mock_serve_db):
        owner = (123, '10.0.0.1')
        assert _add_minimal_service('svc-applied',
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=_service_spec('spec-1'))
        assert serve_state.add_or_update_version(
            'svc-applied', 2, _service_spec('spec-2'),
            'yaml: v2') is serve_state.VersionCommitResult.COMMITTED

        assert not serve_state.mark_version_controller_applied(
            'svc-applied',
            2,
            expected_service_hash='incarnation-b',
            expected_controller_owner=owner,
            applied_at=100.0)
        assert not serve_state.mark_version_controller_applied(
            'svc-applied',
            2,
            expected_service_hash='incarnation-a',
            applied_at=100.0)
        assert not serve_state.mark_version_controller_applied(
            'svc-applied',
            2,
            expected_service_hash='incarnation-a',
            expected_controller_owner=(456, '10.0.0.2'),
            applied_at=100.0)
        assert not serve_state.mark_version_controller_applied(
            'svc-applied',
            99,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
            applied_at=100.0)

        assert serve_state.mark_version_controller_applied(
            'svc-applied',
            2,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
            applied_at=100.0)
        assert serve_state.mark_version_controller_applied(
            'svc-applied',
            2,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
            applied_at=200.0)
        assert _read_version_row(_mock_serve_db, 'svc-applied',
                                 2)['controller_applied_at'] == 100.0

        # A later commit may race ahead while v3 is still applying. The exact
        # owner may record v3 after v4 commits; reconciliation is serialized,
        # and rejecting this would lose the only accurate fallback receipt.
        assert serve_state.add_or_update_version(
            'svc-applied', 3, _service_spec('spec-3'),
            'yaml: v3') is serve_state.VersionCommitResult.COMMITTED
        assert serve_state.add_or_update_version(
            'svc-applied', 4, _service_spec('spec-4'),
            'yaml: v4') is serve_state.VersionCommitResult.COMMITTED
        assert serve_state.mark_version_controller_applied(
            'svc-applied',
            3,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
            applied_at=300.0)
        assert _read_version_row(_mock_serve_db, 'svc-applied',
                                 3)['controller_applied_at'] == 300.0

        assert serve_state.add_version('svc-applied') == 5
        assert not serve_state.mark_version_controller_applied(
            'svc-applied',
            5,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
            applied_at=400.0)
        assert serve_state.add_or_update_version(
            'svc-applied', 5, _service_spec('spec-5'),
            'yaml: v5') is serve_state.VersionCommitResult.COMMITTED
        assert serve_state.quarantine_version('svc-applied', 5, 'bad update')
        assert not serve_state.mark_version_controller_applied(
            'svc-applied',
            5,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
            applied_at=400.0)

    def test_placement_normalization_receipt_is_exact_and_owner_fenced(
            self, _mock_serve_db):
        service_name = 'svc-normalized'
        owner = (123, '10.0.0.1')
        assert _add_minimal_service(service_name,
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=_v2_service_spec('spec-1'))
        lifecycle_epoch = serve_state.claim_service_lifecycle_epoch(
            service_name)
        run_id = uuid.uuid4()
        _insert_placement_normalization_run(_mock_serve_db, run_id)
        spec_bytes = _read_version_row(_mock_serve_db, service_name, 1)['spec']
        _insert_placement_normalization_row(_mock_serve_db, run_id,
                                            service_name, 1, spec_bytes,
                                            'incarnation-a', lifecycle_epoch)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name).values(
                        placement_normalization_requested_run_id=run_id))
            session.commit()

        request = serve_state.get_placement_normalization_request(
            service_name,
            recovery_version=1,
            current_version=1,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner)
        assert request == serve_state.PlacementNormalizationRequest(
            run_id=run_id,
            recovery_version=1,
            current_version=1,
            lifecycle_epoch=lifecycle_epoch)
        with pytest.raises(RuntimeError, match='predates its run completion'):
            serve_state.acknowledge_placement_normalization_loaded(
                service_name,
                request,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner,
                image_commit='commit-a',
                child_controller_pid=456,
                boot_id='a' * 32,
                loaded_at=1.5)
        assert _read_row(
            _mock_serve_db,
            service_name)['placement_normalization_loaded_run_id'] is None
        assert serve_state.acknowledge_placement_normalization_loaded(
            service_name,
            request,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
            image_commit='commit-a',
            child_controller_pid=456,
            boot_id='a' * 32,
            loaded_at=123.0)

        service_row = _read_row(_mock_serve_db, service_name)
        assert service_row['placement_normalization_requested_run_id'] == run_id
        assert service_row['placement_normalization_loaded_run_id'] == run_id
        assert (service_row['placement_normalization_loaded_image_commit'] ==
                'commit-a')
        assert (
            service_row['placement_normalization_loaded_controller_pid'] == 456)
        assert (service_row['placement_normalization_loaded_controller_ip'] ==
                owner[1])
        assert (service_row['placement_normalization_loaded_boot_id'] == 'a' *
                32)
        assert service_row['placement_normalization_loaded_at'] == 123.0

        assert not serve_state.acknowledge_placement_normalization_loaded(
            service_name,
            request,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
            image_commit='commit-b',
            child_controller_pid=789,
            boot_id='b' * 32,
            loaded_at=456.0)
        immutable_receipt = _read_row(_mock_serve_db, service_name)
        assert immutable_receipt[
            'placement_normalization_loaded_run_id'] == run_id
        assert immutable_receipt[
            'placement_normalization_loaded_image_commit'] == 'commit-a'
        assert immutable_receipt[
            'placement_normalization_loaded_controller_pid'] == 456
        assert immutable_receipt[
            'placement_normalization_loaded_controller_ip'] == owner[1]
        assert immutable_receipt[
            'placement_normalization_loaded_boot_id'] == 'a' * 32
        assert immutable_receipt['placement_normalization_loaded_at'] == 123.0

        # The receipt is a one-time normalization proof, not a permanent
        # requirement that future ordinary versions appear in the old run.
        assert serve_state.add_or_update_version(
            service_name, 2, _v2_service_spec('spec-2'),
            'yaml: v2') is serve_state.VersionCommitResult.COMMITTED
        assert serve_state.get_placement_normalization_request(
            service_name,
            recovery_version=2,
            current_version=2,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner) is None

        # Both selected versions are now later than the completed run.  The
        # immutable v1 ledger row is still the incarnation anchor; copying the
        # receipt to a different same-name incarnation must fail closed.
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(
                    serve_state.placement_normalization_rows_table).where(
                        serve_state.placement_normalization_rows_table.c.run_id
                        == run_id,
                        serve_state.placement_normalization_rows_table.c.
                        service_name == service_name).values(
                            service_hash='incarnation-b'))
            session.commit()
        with pytest.raises(RuntimeError,
                           match='service incarnation|owner_facts'):
            serve_state.get_placement_normalization_request(
                service_name,
                recovery_version=2,
                current_version=2,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

    def test_protocol4_receipt_read_rejects_coherent_stale_relabel(
            self, _mock_serve_db):
        service_name = 'svc-normalization-p4-relabel'
        owner = (123, '10.0.0.1')
        run_id, _ = _insert_protocol4_terminal_receipt_state(
            _mock_serve_db, service_name, owner)
        with orm.Session(_mock_serve_db) as session:
            entries = session.execute(
                sqlalchemy.select(
                    serve_state.placement_normalization_rows_table).where(
                        serve_state.placement_normalization_rows_table.c.run_id
                        == run_id)).mappings().all()
            retired = next(
                entry for entry in entries if entry['outcome'] == 'retired')
            stale = sorted((entry for entry in entries
                            if entry['classification'] == 'stale_placeholder'),
                           key=lambda entry: entry['version'])
            relabeled_facts = dict(stale[0]['dependency_facts'])
            relabeled_facts.pop('stale_placeholder_evidence')
            retired_facts = dict(retired['dependency_facts'])
            summary = dict(
                retired_facts['same_service_stale_placeholder_proof'])
            remaining_evidence = [
                entry['dependency_facts']['stale_placeholder_evidence']
                for entry in stale[1:]
            ]
            summary['placeholder_count'] = len(remaining_evidence)
            summary['inventory_sha256'] = (placement_normalization_manifest.
                                           stale_placeholder_inventory_sha256(
                                               service_name,
                                               summary['current_version'],
                                               remaining_evidence))
            retired_facts['same_service_stale_placeholder_proof'] = summary
            session.execute(
                sqlalchemy.update(
                    serve_state.placement_normalization_rows_table).where(
                        serve_state.placement_normalization_rows_table.c.run_id
                        == run_id,
                        serve_state.placement_normalization_rows_table.c.
                        service_name == service_name,
                        serve_state.placement_normalization_rows_table.c.version
                        == stale[0]['version']).values(
                            classification='explicit_v2',
                            dependency_facts=relabeled_facts))
            session.execute(
                sqlalchemy.update(
                    serve_state.placement_normalization_rows_table).where(
                        serve_state.placement_normalization_rows_table.c.run_id
                        == run_id,
                        serve_state.placement_normalization_rows_table.c.
                        service_name == service_name,
                        serve_state.placement_normalization_rows_table.c.version
                        == retired['version']).values(
                            dependency_facts=retired_facts))
            _refresh_placement_normalization_manifest(session, run_id)
            session.commit()

        with pytest.raises(RuntimeError, match='stale_placeholder|manifest'):
            serve_state.get_placement_normalization_request(
                service_name,
                recovery_version=4,
                current_version=4,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

    def test_protocol4_ack_rejects_coherent_stale_ledger_omission(
            self, _mock_serve_db):
        service_name = 'svc-normalization-p4-omission'
        owner = (123, '10.0.0.1')
        run_id, _ = _insert_protocol4_terminal_receipt_state(
            _mock_serve_db, service_name, owner)
        request = serve_state.get_placement_normalization_request(
            service_name,
            recovery_version=4,
            current_version=4,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner)
        assert request is not None

        with orm.Session(_mock_serve_db) as session:
            entries = session.execute(
                sqlalchemy.select(
                    serve_state.placement_normalization_rows_table).where(
                        serve_state.placement_normalization_rows_table.c.run_id
                        == run_id)).mappings().all()
            retired = next(
                entry for entry in entries if entry['outcome'] == 'retired')
            stale = sorted((entry for entry in entries
                            if entry['classification'] == 'stale_placeholder'),
                           key=lambda entry: entry['version'])
            retired_facts = dict(retired['dependency_facts'])
            summary = dict(
                retired_facts['same_service_stale_placeholder_proof'])
            remaining_evidence = [
                entry['dependency_facts']['stale_placeholder_evidence']
                for entry in stale[1:]
            ]
            summary['placeholder_count'] = len(remaining_evidence)
            summary['inventory_sha256'] = (placement_normalization_manifest.
                                           stale_placeholder_inventory_sha256(
                                               service_name,
                                               summary['current_version'],
                                               remaining_evidence))
            retired_facts['same_service_stale_placeholder_proof'] = summary
            session.execute(
                sqlalchemy.update(
                    serve_state.placement_normalization_rows_table).where(
                        serve_state.placement_normalization_rows_table.c.run_id
                        == run_id,
                        serve_state.placement_normalization_rows_table.c.
                        service_name == service_name,
                        serve_state.placement_normalization_rows_table.c.version
                        == retired['version']).values(
                            dependency_facts=retired_facts))
            session.execute(
                sqlalchemy.delete(
                    serve_state.placement_normalization_rows_table).where(
                        serve_state.placement_normalization_rows_table.c.run_id
                        == run_id,
                        serve_state.placement_normalization_rows_table.c.
                        service_name == service_name,
                        serve_state.placement_normalization_rows_table.c.version
                        == stale[0]['version']))
            _refresh_placement_normalization_manifest(session, run_id)
            session.commit()

        with pytest.raises(RuntimeError,
                           match='terminal current inventory|post_terminal'):
            serve_state.acknowledge_placement_normalization_loaded(
                service_name,
                request,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner,
                image_commit='commit-a',
                child_controller_pid=456,
                boot_id='a' * 32,
                loaded_at=123.0)
        assert _read_row(
            _mock_serve_db,
            service_name)['placement_normalization_loaded_run_id'] is None

    def test_protocol4_receipt_live_binding_is_candidate_scoped(
            self, _mock_serve_db):
        service_name = 'svc-normalization-p4-candidate'
        unrelated_name = 'svc-normalization-p4-unrelated'
        owner = (123, '10.0.0.1')
        run_id, lifecycle_epoch = (_insert_protocol4_terminal_receipt_state(
            _mock_serve_db, service_name, owner))
        assert _add_minimal_service(unrelated_name,
                                    service_hash='unrelated-incarnation',
                                    spec=_v2_service_spec('unrelated'))
        unrelated_lifecycle = serve_state.claim_service_lifecycle_epoch(
            unrelated_name)
        unrelated_spec = _read_version_row(_mock_serve_db, unrelated_name,
                                           1)['spec']
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(
                    serve_state.placement_normalization_runs_table).where(
                        serve_state.placement_normalization_runs_table.c.run_id
                        == run_id).values(row_bound=5))
            session.commit()
        _insert_placement_normalization_row(_mock_serve_db,
                                            run_id,
                                            unrelated_name,
                                            1,
                                            unrelated_spec,
                                            'unrelated-incarnation',
                                            unrelated_lifecycle,
                                            classification='explicit_v2',
                                            outcome='unchanged')
        unrelated_projection = (
            placement_contract_normalization.analyze_spec_pickle(
                unrelated_spec).contract_projection)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(
                    serve_state.placement_normalization_rows_table).where(
                        serve_state.placement_normalization_rows_table.c.run_id
                        == run_id,
                        serve_state.placement_normalization_rows_table.c.
                        service_name == unrelated_name).values(
                            contract_projection=unrelated_projection))
            session.commit()

        assert serve_state.get_placement_normalization_request(
            service_name,
            recovery_version=4,
            current_version=4,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner) == (
                serve_state.PlacementNormalizationRequest(
                    run_id=run_id,
                    recovery_version=4,
                    current_version=4,
                    lifecycle_epoch=lifecycle_epoch))

    def test_protocol4_receipt_ack_and_later_lifecycle_are_valid(
            self, _mock_serve_db):
        service_name = 'svc-normalization-p4-valid-ack'
        owner = (123, '10.0.0.1')
        run_id, lifecycle_epoch = (_insert_protocol4_terminal_receipt_state(
            _mock_serve_db, service_name, owner))
        request = serve_state.get_placement_normalization_request(
            service_name,
            recovery_version=4,
            current_version=4,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner)
        assert request == serve_state.PlacementNormalizationRequest(
            run_id=run_id,
            recovery_version=4,
            current_version=4,
            lifecycle_epoch=lifecycle_epoch)
        assert serve_state.acknowledge_placement_normalization_loaded(
            service_name,
            request,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
            image_commit='commit-a',
            child_controller_pid=456,
            boot_id='a' * 32,
            loaded_at=123.0)

        assert serve_state.claim_service_lifecycle_epoch(
            service_name) == lifecycle_epoch + 1
        assert serve_state.get_placement_normalization_request(
            service_name,
            recovery_version=4,
            current_version=4,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner) is None

    def test_protocol4_receipt_rejects_deleted_retired_candidate(
            self, _mock_serve_db):
        service_name = 'svc-normalization-p4-candidate-deleted'
        owner = (123, '10.0.0.1')
        _insert_protocol4_terminal_receipt_state(_mock_serve_db, service_name,
                                                 owner)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.delete(serve_state.version_specs_table).where(
                    serve_state.version_specs_table.c.service_name ==
                    service_name,
                    serve_state.version_specs_table.c.version == 1))
            session.commit()

        with pytest.raises(RuntimeError, match='retired_candidate_row_missing'):
            serve_state.get_placement_normalization_request(
                service_name,
                recovery_version=4,
                current_version=4,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

    def test_protocol4_receipt_rejects_retirement_column_drift(
            self, _mock_serve_db):
        service_name = 'svc-normalization-p4-candidate-drift'
        owner = (123, '10.0.0.1')
        _insert_protocol4_terminal_receipt_state(_mock_serve_db, service_name,
                                                 owner)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.version_specs_table).where(
                    serve_state.version_specs_table.c.service_name ==
                    service_name,
                    serve_state.version_specs_table.c.version == 1).values(
                        retired_yaml_content='tampered: true'))
            session.commit()

        with pytest.raises(RuntimeError, match='retired_terminal_row_drift'):
            serve_state.get_placement_normalization_request(
                service_name,
                recovery_version=4,
                current_version=4,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

    def test_protocol4_manifest_rejects_present_parent_without_hash(
            self, _mock_serve_db):
        service_name = 'svc-normalization-p4-parent-without-hash'
        owner = (123, '10.0.0.1')
        run_id, _ = _insert_protocol4_terminal_receipt_state(
            _mock_serve_db, service_name, owner)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name).values(
                        hash=None))
            session.commit()

        with pytest.raises(RuntimeError,
                           match='invalid_current_parent_hash_observation'):
            _validate_placement_normalization_manifest_directly(
                _mock_serve_db, run_id)

    def test_protocol4_manifest_allows_complete_parent_teardown(
            self, _mock_serve_db):
        service_name = 'svc-normalization-p4-teardown'
        owner = (123, '10.0.0.1')
        run_id, _ = _insert_protocol4_terminal_receipt_state(
            _mock_serve_db, service_name, owner)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.delete(serve_state.version_specs_table).where(
                    serve_state.version_specs_table.c.service_name ==
                    service_name))
            session.execute(
                sqlalchemy.delete(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name))
            session.commit()

        _validate_placement_normalization_manifest_directly(
            _mock_serve_db, run_id)

    def test_protocol4_manifest_allows_recreated_explicit_overlap(
            self, _mock_serve_db):
        service_name = 'svc-normalization-p4-recreated-explicit'
        owner = (123, '10.0.0.1')
        run_id, _ = _insert_protocol4_terminal_receipt_state(
            _mock_serve_db, service_name, owner)
        _recreate_protocol4_service_version(_mock_serve_db,
                                            service_name,
                                            explicit=True)

        _validate_placement_normalization_manifest_directly(
            _mock_serve_db, run_id)

    def test_protocol4_manifest_rejects_recreated_placeholder_overlap(
            self, _mock_serve_db):
        service_name = 'svc-normalization-p4-recreated-placeholder'
        owner = (123, '10.0.0.1')
        run_id, _ = _insert_protocol4_terminal_receipt_state(
            _mock_serve_db, service_name, owner)
        _recreate_protocol4_service_version(_mock_serve_db,
                                            service_name,
                                            explicit=False)

        with pytest.raises(RuntimeError,
                           match='old_stale|invalid_new_incarnation'):
            _validate_placement_normalization_manifest_directly(
                _mock_serve_db, run_id)

    def test_placement_normalization_receipt_rejects_stale_lifecycle(
            self, _mock_serve_db):
        service_name = 'svc-normalization-stale'
        owner = (123, '10.0.0.1')
        assert _add_minimal_service(service_name,
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=_v2_service_spec('spec-1'))
        lifecycle_epoch = serve_state.claim_service_lifecycle_epoch(
            service_name)
        run_id = uuid.uuid4()
        _insert_placement_normalization_run(_mock_serve_db, run_id)
        spec_bytes = _read_version_row(_mock_serve_db, service_name, 1)['spec']
        _insert_placement_normalization_row(_mock_serve_db, run_id,
                                            service_name, 1, spec_bytes,
                                            'incarnation-a', lifecycle_epoch)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name).values(
                        placement_normalization_requested_run_id=run_id))
            session.commit()
        request = serve_state.get_placement_normalization_request(
            service_name,
            recovery_version=1,
            current_version=1,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner)
        assert request is not None

        serve_state.claim_service_lifecycle_epoch(service_name)
        assert not serve_state.acknowledge_placement_normalization_loaded(
            service_name,
            request,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
            image_commit='commit-a',
            child_controller_pid=456,
            boot_id='a' * 32)
        service_row = _read_row(_mock_serve_db, service_name)
        assert service_row['placement_normalization_loaded_run_id'] is None

    def test_placement_normalization_read_distinguishes_no_request_from_stale(
            self, _mock_serve_db):
        owner = (123, '10.0.0.1')
        current_spec = _exact_service_spec('spec-1')
        assert _add_minimal_service('svc-normalization-read',
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=current_spec)

        v2_state = dict(current_spec.__dict__)
        v1_state = dict(v2_state)
        v1_state.update(
            current_spec.placement_contract._legacy_v1_persisted_fields())
        v1_state[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] = False
        fieldless_state = dict(v2_state)
        for field in placement_policy.CONTRACT_FIELDS:
            fieldless_state.pop(field)
        historical_state = dict(
            _exact_service_spec('historical',
                                uses_logical_replicas=True).__dict__)
        for field in placement_policy.CONTRACT_FIELDS:
            historical_state.pop(field)
        payloads = (
            placement_contract_normalization._serialize_raw_state(
                fieldless_state, 4),
            placement_contract_normalization._serialize_raw_state(v1_state, 4),
            placement_contract_normalization._serialize_raw_state(v2_state, 4),
            placement_contract_normalization._serialize_raw_state(
                historical_state, 4),
        )
        for payload in payloads:
            with orm.Session(_mock_serve_db) as session:
                session.execute(
                    sqlalchemy.update(serve_state.version_specs_table).where(
                        serve_state.version_specs_table.c.service_name ==
                        'svc-normalization-read').values(spec=payload))
                session.commit()
            assert serve_state.get_placement_normalization_request(
                'svc-normalization-read',
                recovery_version=1,
                current_version=1,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner) is None
        with pytest.raises(RuntimeError, match='current-version fence'):
            serve_state.get_placement_normalization_request(
                'svc-normalization-read',
                recovery_version=1,
                current_version=2,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

    def test_completed_receipt_rejects_fieldless_raw_spec_despite_materializer(
            self, _mock_serve_db):
        service_name = 'svc-normalization-fieldless'
        owner = (123, '10.0.0.1')
        assert _add_minimal_service(service_name,
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=_exact_service_spec('spec-1'))
        fieldless_spec = _exact_service_spec('fieldless')
        fieldless_state = dict(fieldless_spec.__dict__)
        for field in placement_policy.CONTRACT_FIELDS:
            fieldless_state.pop(field)
        fieldless_bytes = (
            placement_contract_normalization._serialize_raw_state(
                fieldless_state, 4))
        materialized = pickle.loads(fieldless_bytes)
        _, materialized_version = placement_policy.decode_contract_state(
            materialized.__dict__)
        assert materialized_version == (
            placement_policy.PLACEMENT_CONTRACT_VERSION_V2)

        run_id = uuid.uuid4()
        _insert_placement_normalization_run(_mock_serve_db, run_id)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.version_specs_table).where(
                    serve_state.version_specs_table.c.service_name ==
                    service_name,
                    serve_state.version_specs_table.c.version == 1).values(
                        spec=fieldless_bytes))
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name).values(
                        placement_normalization_requested_run_id=run_id,
                        placement_normalization_loaded_run_id=run_id,
                        placement_normalization_loaded_image_commit='commit-a',
                        placement_normalization_loaded_controller_pid=456,
                        placement_normalization_loaded_controller_ip=owner[1],
                        placement_normalization_loaded_boot_id='a' * 32,
                        placement_normalization_loaded_at=123.0))
            session.commit()

        with pytest.raises(RuntimeError, match='mirror-free v2'):
            serve_state.get_placement_normalization_request(
                service_name,
                recovery_version=1,
                current_version=1,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

    def test_pending_receipt_rejects_mismatched_v2_ledger_result(
            self, _mock_serve_db):
        service_name = 'svc-normalization-ledger-mismatch'
        owner = (123, '10.0.0.1')
        assert _add_minimal_service(service_name,
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=_v2_service_spec('spec-1'))
        run_id = uuid.uuid4()
        _insert_placement_normalization_run(_mock_serve_db, run_id)
        spec_bytes = _read_version_row(_mock_serve_db, service_name, 1)['spec']
        _insert_placement_normalization_row(_mock_serve_db,
                                            run_id,
                                            service_name,
                                            1,
                                            spec_bytes,
                                            'incarnation-a',
                                            None,
                                            result_spec_sha256='d' * 64)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name).values(
                        placement_normalization_requested_run_id=run_id))
            session.commit()

        with pytest.raises(RuntimeError, match='persisted spec does not match'):
            serve_state.get_placement_normalization_request(
                service_name,
                recovery_version=1,
                current_version=1,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

    def test_pending_receipt_rejects_outcome_disallowed_by_manifest_mode(
            self, _mock_serve_db):
        service_name = 'svc-normalization-mode-mismatch'
        owner = (123, '10.0.0.1')
        assert _add_minimal_service(service_name,
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=_v2_service_spec('spec-1'))
        lifecycle_epoch = serve_state.claim_service_lifecycle_epoch(
            service_name)
        run_id = uuid.uuid4()
        _insert_placement_normalization_run(_mock_serve_db,
                                            run_id,
                                            mode='retire_terminal_historical',
                                            normalizer_protocol=2)
        spec_bytes = _read_version_row(_mock_serve_db, service_name, 1)['spec']
        _insert_placement_normalization_row(_mock_serve_db, run_id,
                                            service_name, 1, spec_bytes,
                                            'incarnation-a', lifecycle_epoch)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name).values(
                        placement_normalization_requested_run_id=run_id))
            session.commit()

        with pytest.raises(RuntimeError, match='mode|outcome|ledger'):
            serve_state.get_placement_normalization_request(
                service_name,
                recovery_version=1,
                current_version=1,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

    def test_completed_receipt_rejects_substituted_inventoried_v2_spec(
            self, _mock_serve_db):
        service_name = 'svc-normalization-completed-mismatch'
        owner = (123, '10.0.0.1')
        assert _add_minimal_service(service_name,
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=_v2_service_spec('spec-1'))
        lifecycle_epoch = serve_state.claim_service_lifecycle_epoch(
            service_name)
        run_id = uuid.uuid4()
        _insert_placement_normalization_run(_mock_serve_db, run_id)
        inventoried_spec = _read_version_row(_mock_serve_db, service_name,
                                             1)['spec']
        _insert_placement_normalization_row(_mock_serve_db, run_id,
                                            service_name, 1, inventoried_spec,
                                            'incarnation-a', lifecycle_epoch)
        substituted_spec = pickle.dumps(_v2_service_spec('spec-2'), protocol=4)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.version_specs_table).where(
                    serve_state.version_specs_table.c.service_name ==
                    service_name,
                    serve_state.version_specs_table.c.version == 1).values(
                        spec=substituted_spec))
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name).values(
                        placement_normalization_requested_run_id=run_id,
                        placement_normalization_loaded_run_id=run_id,
                        placement_normalization_loaded_image_commit='commit-a',
                        placement_normalization_loaded_controller_pid=456,
                        placement_normalization_loaded_controller_ip=owner[1],
                        placement_normalization_loaded_boot_id='a' * 32,
                        placement_normalization_loaded_at=123.0))
            session.commit()

        with pytest.raises(RuntimeError, match='persisted spec does not match'):
            serve_state.get_placement_normalization_request(
                service_name,
                recovery_version=1,
                current_version=1,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

    def test_completed_receipt_survives_later_lifecycle_epoch(
            self, _mock_serve_db):
        service_name = 'svc-normalization-later-lifecycle'
        owner = (123, '10.0.0.1')
        assert _add_minimal_service(service_name,
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=_v2_service_spec('spec-1'))
        lifecycle_epoch = serve_state.claim_service_lifecycle_epoch(
            service_name)
        run_id = uuid.uuid4()
        _insert_placement_normalization_run(_mock_serve_db, run_id)
        spec_bytes = _read_version_row(_mock_serve_db, service_name, 1)['spec']
        _insert_placement_normalization_row(_mock_serve_db, run_id,
                                            service_name, 1, spec_bytes,
                                            'incarnation-a', lifecycle_epoch)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name).values(
                        placement_normalization_requested_run_id=run_id,
                        placement_normalization_loaded_run_id=run_id,
                        placement_normalization_loaded_image_commit='commit-a',
                        placement_normalization_loaded_controller_pid=456,
                        placement_normalization_loaded_controller_ip=owner[1],
                        placement_normalization_loaded_boot_id='a' * 32,
                        placement_normalization_loaded_at=123.0))
            session.commit()

        assert serve_state.claim_service_lifecycle_epoch(
            service_name) == lifecycle_epoch + 1
        assert serve_state.get_placement_normalization_request(
            service_name,
            recovery_version=1,
            current_version=1,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner) is None

    def test_completed_receipt_allows_inventoried_placeholder_fill(
            self, _mock_serve_db):
        service_name = 'svc-normalization-placeholder-fill'
        owner = (123, '10.0.0.1')
        assert _add_minimal_service(service_name,
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=_v2_service_spec('spec-1'))
        assert serve_state.add_version(service_name) == 2
        lifecycle_epoch = serve_state.claim_service_lifecycle_epoch(
            service_name)
        run_id = uuid.uuid4()
        _insert_placement_normalization_run(_mock_serve_db,
                                            run_id,
                                            row_count=2,
                                            classification_counts={
                                                'fieldless_supported': 1,
                                                'placeholder': 1,
                                            })
        first_spec = _read_version_row(_mock_serve_db, service_name, 1)['spec']
        placeholder_spec = _read_version_row(_mock_serve_db, service_name,
                                             2)['spec']
        _insert_placement_normalization_row(_mock_serve_db, run_id,
                                            service_name, 1, first_spec,
                                            'incarnation-a', lifecycle_epoch)
        _insert_placement_normalization_row(_mock_serve_db,
                                            run_id,
                                            service_name,
                                            2,
                                            placeholder_spec,
                                            'incarnation-a',
                                            lifecycle_epoch,
                                            classification='placeholder',
                                            outcome='unchanged')
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name).values(
                        placement_normalization_requested_run_id=run_id,
                        placement_normalization_loaded_run_id=run_id,
                        placement_normalization_loaded_image_commit='commit-a',
                        placement_normalization_loaded_controller_pid=456,
                        placement_normalization_loaded_controller_ip=owner[1],
                        placement_normalization_loaded_boot_id='a' * 32,
                        placement_normalization_loaded_at=123.0))
            session.commit()

        assert serve_state.add_or_update_version(
            service_name, 2, _v2_service_spec('spec-2'),
            'yaml: v2') is serve_state.VersionCommitResult.COMMITTED
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(
                    serve_state.placement_normalization_rows_table).where(
                        serve_state.placement_normalization_rows_table.c.run_id
                        == run_id,
                        serve_state.placement_normalization_rows_table.c.
                        service_name == service_name,
                        serve_state.placement_normalization_rows_table.c.version
                        == 2).values(service_hash='incarnation-b'))
            session.commit()
        with pytest.raises(RuntimeError,
                           match='service incarnation|owner_facts'):
            serve_state.get_placement_normalization_request(
                service_name,
                recovery_version=2,
                current_version=2,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(
                    serve_state.placement_normalization_rows_table).where(
                        serve_state.placement_normalization_rows_table.c.run_id
                        == run_id,
                        serve_state.placement_normalization_rows_table.c.
                        service_name == service_name,
                        serve_state.placement_normalization_rows_table.c.version
                        == 2).values(service_hash='incarnation-a'))
            session.commit()
        assert serve_state.get_placement_normalization_request(
            service_name,
            recovery_version=2,
            current_version=2,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner) is None

    def test_completed_receipt_rejects_missing_requested_run(
            self, _mock_serve_db):
        service_name = 'svc-normalization-missing-run'
        owner = (123, '10.0.0.1')
        assert _add_minimal_service(service_name,
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=_v2_service_spec('spec-1'))
        missing_run_id = uuid.uuid4()
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name).values(
                        placement_normalization_requested_run_id=missing_run_id,
                        placement_normalization_loaded_run_id=missing_run_id,
                        placement_normalization_loaded_image_commit='commit-a',
                        placement_normalization_loaded_controller_pid=456,
                        placement_normalization_loaded_controller_ip=owner[1],
                        placement_normalization_loaded_boot_id='a' * 32,
                        placement_normalization_loaded_at=123.0))
            session.commit()

        with pytest.raises(RuntimeError, match='manifest is missing'):
            serve_state.get_placement_normalization_request(
                service_name,
                recovery_version=1,
                current_version=1,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

    def test_completed_receipt_rejects_corrupt_run_manifest(
            self, _mock_serve_db):
        service_name = 'svc-normalization-corrupt-run'
        owner = (123, '10.0.0.1')
        assert _add_minimal_service(service_name,
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=_v2_service_spec('spec-1'))
        run_id = uuid.uuid4()
        _insert_placement_normalization_run(_mock_serve_db,
                                            run_id,
                                            schema_revision='corrupt')
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name).values(
                        placement_normalization_requested_run_id=run_id,
                        placement_normalization_loaded_run_id=run_id,
                        placement_normalization_loaded_image_commit='commit-a',
                        placement_normalization_loaded_controller_pid=456,
                        placement_normalization_loaded_controller_ip=owner[1],
                        placement_normalization_loaded_boot_id='a' * 32,
                        placement_normalization_loaded_at=123.0))
            session.commit()

        with pytest.raises(RuntimeError, match='invalid release identity'):
            serve_state.get_placement_normalization_request(
                service_name,
                recovery_version=1,
                current_version=1,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

    def test_completed_receipt_rejects_deleted_pre_run_ledger_row(
            self, _mock_serve_db):
        service_name = 'svc-normalization-deleted-ledger'
        owner = (123, '10.0.0.1')
        assert _add_minimal_service(service_name,
                                    service_hash='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    spec=_v2_service_spec('spec-1'))
        run_id = uuid.uuid4()
        _insert_placement_normalization_run(_mock_serve_db, run_id)
        spec_bytes = _read_version_row(_mock_serve_db, service_name, 1)['spec']
        _insert_placement_normalization_row(_mock_serve_db, run_id,
                                            service_name, 1, spec_bytes,
                                            'incarnation-a', None)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.version_specs_table).where(
                    serve_state.version_specs_table.c.service_name ==
                    service_name,
                    serve_state.version_specs_table.c.version == 1).values(
                        created_at=1.5))
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == service_name).values(
                        placement_normalization_requested_run_id=run_id,
                        placement_normalization_loaded_run_id=run_id,
                        placement_normalization_loaded_image_commit='commit-a',
                        placement_normalization_loaded_controller_pid=456,
                        placement_normalization_loaded_controller_ip=owner[1],
                        placement_normalization_loaded_boot_id='a' * 32,
                        placement_normalization_loaded_at=123.0))
            session.execute(
                sqlalchemy.delete(
                    serve_state.placement_normalization_rows_table).where(
                        serve_state.placement_normalization_rows_table.c.run_id
                        == run_id,
                        serve_state.placement_normalization_rows_table.c.
                        service_name == service_name,
                        serve_state.placement_normalization_rows_table.c.version
                        == 1))
            session.commit()

        with pytest.raises(RuntimeError,
                           match='incomplete_run_inventory|ledger anchor'):
            serve_state.get_placement_normalization_request(
                service_name,
                recovery_version=1,
                current_version=1,
                expected_service_hash='incarnation-a',
                expected_controller_owner=owner)

    def test_quarantine_rejects_placeholder_and_is_idempotent(
            self, _mock_serve_db):
        assert serve_state.add_version('svc') == 1
        assert not serve_state.quarantine_version('svc', 1, 'not committed')
        serve_state.add_or_update_version('svc', 1, _service_spec('spec-1'),
                                          'yaml: v1')
        assert serve_state.quarantine_version('svc',
                                              1,
                                              'first reason',
                                              quarantined_at=123.0)
        assert serve_state.quarantine_version('svc',
                                              1,
                                              'second reason',
                                              quarantined_at=456.0)
        assert serve_state.get_latest_quarantined_version('svc') == {
            'version': 1,
            'quarantined_at': 123.0,
            'quarantine_reason': 'first reason',
        }

    def test_quarantine_is_fenced_by_controller_ownership(self, _mock_serve_db):
        assert _add_minimal_service('svc-owner',
                                    service_hash='incarnation-a',
                                    controller_pid=123,
                                    controller_ip='10.0.0.1')
        serve_state.add_or_update_version('svc-owner', 2,
                                          _service_spec('spec-2'), 'yaml: v2')

        assert not serve_state.quarantine_version(
            'svc-owner',
            2,
            'stale controller',
            expected_service_hash='incarnation-a',
            expected_controller_owner=(456, '10.0.0.2'))
        assert serve_state.get_latest_quarantined_version('svc-owner') is None

        assert serve_state.quarantine_version(
            'svc-owner',
            2,
            'current controller',
            quarantined_at=123.0,
            expected_service_hash='incarnation-a',
            expected_controller_owner=(123, '10.0.0.1'))
        assert serve_state.get_latest_quarantined_version('svc-owner') == {
            'version': 2,
            'quarantined_at': 123.0,
            'quarantine_reason': 'current controller',
        }

    def test_committed_version_spec_is_one_row_snapshot(self, _mock_serve_db):
        serve_state.add_or_update_version('svc', 1, _service_spec('spec-1'),
                                          'yaml: v1')
        serve_state.add_or_update_version('svc', 2, _service_spec('spec-2'),
                                          'yaml: v2')
        serve_state.add_version('svc')  # placeholder v3

        statements = []

        def _record_statement(*args):
            statements.append(args[2])

        sqlalchemy.event.listen(_mock_serve_db, 'before_cursor_execute',
                                _record_statement)
        try:
            snapshot = serve_state.get_latest_committed_version_spec('svc')
        finally:
            sqlalchemy.event.remove(_mock_serve_db, 'before_cursor_execute',
                                    _record_statement)

        assert _labeled_version_spec(snapshot) == (2, 'spec-2')
        assert len(statements) == 1, statements

    def test_committed_version_spec_none_without_committed_row(
            self, _mock_serve_db):
        serve_state.add_version('svc')
        assert serve_state.get_latest_committed_version_spec('svc') is None

    def test_committed_version_spec_none_for_unusable_spec(
            self, _mock_serve_db):
        with orm.Session(_mock_serve_db) as session:
            session.execute(serve_state.version_specs_table.insert().values(
                service_name='svc',
                version=1,
                spec=pickle.dumps(None),
                yaml_content='yaml: v1'))
            session.commit()
        assert serve_state.get_latest_committed_version_spec('svc') is None


class TestUnrecoverableServiceCleanup:
    """Rows without committed YAML must not wait on impossible recovery."""

    def test_ha_retirement_marks_failed_and_removes_script(
            self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        serve_state.set_ha_recovery_script('svc', 'unbootable script')

        assert serve_state.mark_unrecoverable_service_for_cleanup('svc',
                                                                  'orphan',
                                                                  pool=False)
        assert (_read_row(
            _mock_serve_db,
            'svc')['status'] == serve_state.ServiceStatus.FAILED_CLEANUP.value)
        assert serve_state.get_ha_recovery_script('svc') is None

    def test_purge_claim_consumes_unbootable_script(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        assert serve_state.set_service_status_and_active_versions_if_hash(
            'svc', 'orphan', serve_state.ServiceStatus.SHUTTING_DOWN)
        serve_state.set_ha_recovery_script('svc', 'unbootable script')

        assert serve_state.claim_unrecoverable_service_teardown(
            'svc', 'orphan', 12345, None, 67890, '10.0.0.2')
        row = _read_row(_mock_serve_db, 'svc')
        assert row['controller_pid'] == 67890
        assert row['controller_ip'] == '10.0.0.2'
        assert (row['controller_port'] ==
                serve_constants.CONTROLLER_TEARDOWN_ACK_PORT)
        assert serve_state.get_ha_recovery_script('svc') is None

    def test_committed_version_prevents_unrecoverable_claim(
            self, _mock_serve_db):
        assert _add_minimal_service('svc',
                                    controller_pid=12345,
                                    service_hash='incarnation-a')
        assert serve_state.set_service_status_and_active_versions_if_hash(
            'svc', 'incarnation-a', serve_state.ServiceStatus.SHUTTING_DOWN)
        serve_state.set_ha_recovery_script('svc', 'bootable script')

        assert not serve_state.claim_unrecoverable_service_teardown(
            'svc', 'incarnation-a', 12345, None, 67890, '10.0.0.2')
        assert serve_state.get_ha_recovery_script('svc') == 'bootable script'


class TestTerminalServiceRejectsVersionWrites:
    """Terminal services must reject later version placeholder/YAML writes."""

    def test_down_winner_blocks_placeholder_and_yaml_commit(
            self, _mock_serve_db):
        assert _add_minimal_service('svc',
                                    controller_pid=12345,
                                    service_hash='incarnation-a')
        assert serve_state.set_service_status_and_active_versions_if_hash(
            'svc', 'incarnation-a', serve_state.ServiceStatus.SHUTTING_DOWN)

        with pytest.raises(RuntimeError, match='terminal status'):
            serve_state.add_version('svc')
        assert not serve_state.add_or_update_version(
            'svc', 2, _service_spec('spec-2'), 'yaml: v2')
        assert serve_state.get_latest_version('svc') == 1
        assert serve_state.get_yaml_content('svc', 1) == 'yaml: v1'


class TestBatchReplicaUpsert:
    """add_or_update_replicas: one statement for a probe round's writes."""

    def test_batch_insert_then_batch_update(self, _mock_serve_db):
        infos = [(i, _replica(i, version=1)) for i in range(1, 4)]
        serve_state.add_or_update_replicas('svc', infos)
        rows = {
            i: serve_state.get_replica_info_from_id('svc', i)
            for i in range(1, 4)
        }
        assert all(rows[i].version == 1 for i in range(1, 4))

        # Bookkeeping carries each immutable record identity and updates in
        # place without an insert-capable conflict path.
        updated = []
        for replica_id, info in rows.items():
            info.version = 2
            updated.append((replica_id, info))
        serve_state.add_or_update_replicas('svc',
                                           updated,
                                           expected_replica_exists=True)
        assert all(
            serve_state.get_replica_info_from_id('svc', i).version == 2
            for i in range(1, 4))
        assert len(serve_state.get_replica_infos('svc')) == 3

    def test_empty_batch_is_noop(self, _mock_serve_db):
        serve_state.add_or_update_replicas('svc', [])
        assert not serve_state.get_replica_infos('svc')

    def test_empty_batch_fence_requires_and_validates_owner_identity(
            self, _mock_serve_db):
        incomplete_fences = ({}, {
            'expected_service_hash': 'incarnation-a'
        }, {
            'expected_controller_owner': (200, '10.0.0.2')
        }, {
            'expected_service_hash': 'incarnation-a',
            'expected_controller_owner': (None, None)
        })
        for fence in incomplete_fences:
            assert not serve_state.add_or_update_replicas(
                'svc', [], validate_fence_on_empty=True, **fence)
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a')
        assert serve_state.add_or_update_replicas(
            'svc', [],
            expected_service_hash='incarnation-a',
            expected_controller_owner=(200, '10.0.0.2'),
            expected_replica_exists=True,
            validate_fence_on_empty=True)
        assert not serve_state.add_or_update_replicas(
            'svc', [],
            expected_service_hash='incarnation-a',
            expected_controller_owner=(100, '10.0.0.1'),
            expected_replica_exists=True,
            validate_fence_on_empty=True)

    def test_batch_larger_than_chunk_size(self, _mock_serve_db):
        chunk_size = (serve_state._SQLITE_MAX_BIND_PARAMS //
                      len(serve_state.replicas_table.c))
        n = chunk_size * 2 + 17
        infos = [(i, _replica(i)) for i in range(1, n + 1)]
        serve_state.add_or_update_replicas('svc', infos)
        assert len(serve_state.get_replica_infos('svc')) == n

    def test_batch_respects_legacy_sqlite_bind_limit(self, _mock_serve_db):
        raw_connection = _mock_serve_db.raw_connection()
        try:
            raw_connection.driver_connection.setlimit(
                sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER,
                serve_state._SQLITE_MAX_BIND_PARAMS)
        finally:
            raw_connection.close()

        infos = [(i, _replica(i)) for i in range(1, 91)]
        serve_state.add_or_update_replicas('svc', infos)
        assert len(serve_state.get_replica_infos('svc')) == 90


class TestExpectedExistingReplicaPersistence:
    """Terminal deletes are absorbing for stale bookkeeping snapshots."""

    def test_delete_first_rejects_stale_single_update(self, _mock_serve_db):
        assert serve_state.add_or_update_replica('svc', 1, _replica(1))
        stale = serve_state.get_replica_info_from_id('svc', 1)
        assert stale is not None
        stale.version = 2

        assert serve_state.remove_replica(
            'svc', 1, expected_replica_record_id=stale.replica_record_id)
        assert not serve_state.add_or_update_replica(
            'svc', 1, stale, expected_replica_exists=True)
        assert serve_state.get_replica_info_from_id('svc', 1) is None

    def test_delete_first_rejects_entire_stale_batch(self, _mock_serve_db):
        assert serve_state.add_or_update_replicas('svc', [(1, _replica(1)),
                                                          (2, _replica(2))])
        stale = serve_state.get_replica_info_from_id('svc', 1)
        survivor_update = serve_state.get_replica_info_from_id('svc', 2)
        assert stale is not None and survivor_update is not None
        stale.version = 2
        survivor_update.version = 2
        assert serve_state.remove_replica(
            'svc', 1, expected_replica_record_id=stale.replica_record_id)

        assert not serve_state.add_or_update_replicas(
            'svc', [(1, stale), (2, survivor_update)],
            expected_replica_exists=True)
        assert serve_state.get_replica_info_from_id('svc', 1) is None
        survivor = serve_state.get_replica_info_from_id('svc', 2)
        assert survivor is not None
        assert survivor.version == 1

    def test_write_first_then_delete_leaves_no_row(self, _mock_serve_db):
        assert serve_state.add_or_update_replica('svc', 1, _replica(1))
        update = serve_state.get_replica_info_from_id('svc', 1)
        assert update is not None
        update.version = 2
        assert serve_state.add_or_update_replica('svc',
                                                 1,
                                                 update,
                                                 expected_replica_exists=True)
        assert serve_state.remove_replica(
            'svc', 1, expected_replica_record_id=update.replica_record_id)
        assert serve_state.get_replica_info_from_id('svc', 1) is None

    def test_delete_fence_rejects_malformed_and_inexact_batch_inputs(
            self, _mock_serve_db):
        info = _replica(1)
        assert serve_state.add_or_update_replica('svc', 1, info)

        with pytest.raises(ValueError, match='canonical UUID'):
            serve_state.remove_replica('svc',
                                       1,
                                       expected_replica_record_id='INVALID')
        with pytest.raises(ValueError, match='duplicates'):
            serve_state.remove_replicas(
                'svc', [1, 1],
                expected_service_hash='incarnation-a',
                expected_replica_record_ids={1: info.replica_record_id})
        with pytest.raises(ValueError, match='cover every replica'):
            serve_state.remove_replicas(
                'svc', [],
                expected_service_hash='incarnation-a',
                expected_replica_record_ids={1: info.replica_record_id})
        with pytest.raises(ValueError, match='canonical UUID'):
            serve_state.remove_replicas(
                'svc', [1],
                expected_service_hash='incarnation-a',
                expected_replica_record_ids={1: 'INVALID'})

        persisted = serve_state.get_replica_info_from_id('svc', 1)
        assert persisted is not None
        assert persisted.replica_record_id == info.replica_record_id

    def test_physical_replica_key_must_match_payload_before_writes(
            self, _mock_serve_db):
        with pytest.raises(ValueError, match='row key must match'):
            serve_state.add_or_update_replica('svc', 1, _replica(2))
        with pytest.raises(ValueError, match='row key must match'):
            serve_state.add_or_update_replicas('svc', [(1, _replica(1)),
                                                       (2, _replica(3))])
        assert serve_state.get_replica_infos('svc') == []

        assert _add_minimal_service('svc', service_hash='incarnation-a')
        with pytest.raises(ValueError, match='row key must match'):
            serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'incarnation-a',
                1,
                _replica(2),
                pool_key='paid-pool',
                priority=1,
                base_limit=1,
                max_limit=2,
                now=1.0,
                success_ttl_seconds=60.0,
                waiter_ttl_seconds=60.0,
                expected_controller_owner=None)
        with orm.Session(_mock_serve_db) as session:
            assert session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_pools_table)).first() is None
        assert serve_state.get_replica_infos('svc') == []

    def test_paid_outcomes_cannot_mutate_unfenced_extra_replica(
            self, _mock_serve_db):
        assert _add_minimal_service('svc', service_hash='incarnation-a')
        first = _replica(1)
        second = _replica(2)
        second.paid_capacity_pool_key = 'paid-pool'
        assert serve_state.add_or_update_replicas('svc', [(1, first),
                                                          (2, second)])
        update = serve_state.get_replica_info_from_id('svc', 1)
        assert update is not None
        update.version = 9
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                serve_state.paid_capacity_pools_table.insert().values(
                    pool_key='paid-pool',
                    current_limit=1,
                    successes_since_resize=0,
                    updated_at=1.0))
            session.execute(
                serve_state.paid_capacity_claims_table.insert().values(
                    service_name='svc',
                    service_hash='incarnation-a',
                    replica_id=2,
                    pool_key='paid-pool',
                    priority=1,
                    claimed_at=1.0))
            session.commit()

        with pytest.raises(ValueError, match='outcomes must identify'):
            serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
                'svc',
                'incarnation-a', [(1, update)],
                {2: paid_capacity.LaunchOutcome.SUCCESS},
                base_limit=1,
                max_limit=2,
                now=2.0,
                success_ttl_seconds=60.0,
                expected_controller_owner=None)

        persisted = serve_state.get_replica_info_from_id('svc', 1)
        assert persisted is not None
        assert persisted.version == 1
        with orm.Session(_mock_serve_db) as session:
            claim = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table)).first()
        assert claim is not None

    def test_pre_v17_json_cannot_enter_terminal_delete_path(
            self, _mock_serve_db):
        info = _replica(1)
        info.created_at = 123.5
        assert serve_state.add_or_update_replica('svc', 1, info)
        with orm.Session(_mock_serve_db) as session:
            row = session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.replica_state).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id ==
                        1)).scalar_one()
            row['replica_info_version'] = 12
            for field_name in replica_info_lib.V13_ADDITIVE_STORAGE_FIELDS:
                row.pop(field_name)
            session.execute(
                sqlalchemy.update(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name == 'svc',
                    serve_state.replicas_table.c.replica_id == 1).values(
                        replica_state=row))
            session.commit()

        with pytest.raises(ValueError, match='invalid top-level shape'):
            serve_state.get_replica_info_from_id('svc', 1)

    def test_recreated_numeric_id_rejects_stale_updates_and_deletes(
            self, _mock_serve_db):
        assert _add_minimal_service('svc', service_hash='incarnation-a')
        assert serve_state.add_or_update_replicas('svc', [(1, _replica(1)),
                                                          (2, _replica(2))])
        stale = serve_state.get_replica_info_from_id('svc', 1)
        survivor_update = serve_state.get_replica_info_from_id('svc', 2)
        assert stale is not None and survivor_update is not None
        assert serve_state.remove_replica(
            'svc', 1, expected_replica_record_id=stale.replica_record_id)

        replacement = _replica(1, version=10)
        assert replacement.replica_record_id != stale.replica_record_id
        assert serve_state.add_or_update_replica('svc', 1, replacement)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                serve_state.paid_capacity_pools_table.insert().values(
                    pool_key='replacement-pool',
                    current_limit=1,
                    successes_since_resize=0,
                    updated_at=1.0))
            session.execute(
                serve_state.paid_capacity_claims_table.insert().values(
                    service_name='svc',
                    service_hash='incarnation-a',
                    replica_id=1,
                    pool_key='replacement-pool',
                    priority=1,
                    claimed_at=1.0))
            session.commit()
        with orm.Session(_mock_serve_db) as session:
            claim_before = dict(
                session.execute(
                    sqlalchemy.select(serve_state.paid_capacity_claims_table)).
                mappings().one())
        stale.version = 99
        survivor_update.version = 99

        assert not serve_state.add_or_update_replica(
            'svc', 1, stale, expected_replica_exists=True)
        assert not serve_state.add_or_update_replicas(
            'svc', [(1, stale), (2, survivor_update)],
            expected_replica_exists=True)
        assert not serve_state.remove_replica(
            'svc',
            1,
            expected_service_hash='incarnation-a',
            expected_replica_record_id=stale.replica_record_id)
        assert not serve_state.remove_replicas(
            'svc', [1, 2],
            expected_service_hash='incarnation-a',
            expected_replica_record_ids={
                1: stale.replica_record_id,
                2: survivor_update.replica_record_id,
            })
        persisted = serve_state.get_replica_info_from_id('svc', 1)
        survivor = serve_state.get_replica_info_from_id('svc', 2)
        assert persisted is not None and survivor is not None
        assert persisted.replica_record_id == replacement.replica_record_id
        assert persisted.version == 10
        assert survivor.version == 1
        with orm.Session(_mock_serve_db) as session:
            claim_after = dict(
                session.execute(
                    sqlalchemy.select(serve_state.paid_capacity_claims_table)).
                mappings().one())
        assert claim_after == claim_before

        with orm.Session(_mock_serve_db) as session:
            row = session.execute(
                sqlalchemy.select(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name == 'svc',
                    serve_state.replicas_table.c.replica_id ==
                    1)).mappings().one()
        from_json = replica_managers.ReplicaInfo.from_storage_dict(
            row['replica_state'])
        assert from_json.replica_record_id == replacement.replica_record_id
        assert row['replica_info'] is None

    def test_initial_single_and_batch_insert_conflicts_are_atomic(
            self, _mock_serve_db):
        initial = _replica(1)
        assert serve_state.add_or_update_replica('svc', 1, initial)

        with pytest.raises(sqlalchemy.exc.IntegrityError):
            serve_state.add_or_update_replica('svc', 1, _replica(1, version=2))
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            serve_state.add_or_update_replicas('svc',
                                               [(2, _replica(2)),
                                                (1, _replica(1, version=3))])

        persisted = serve_state.get_replica_info_from_id('svc', 1)
        assert persisted is not None
        assert persisted.replica_record_id == initial.replica_record_id
        assert persisted.version == 1
        assert serve_state.get_replica_info_from_id('svc', 2) is None

    def test_recreated_numeric_id_rejects_paid_capacity_completion(
            self, _mock_serve_db):
        del _mock_serve_db
        owner = (123, '10.0.0.1')
        assert _add_minimal_service('svc',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    service_hash='incarnation-a')
        assert serve_state.add_or_update_replica(
            'svc',
            1,
            _replica(1),
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner)
        stale = serve_state.get_replica_info_from_id('svc', 1)
        assert stale is not None
        stale.version = 2
        assert serve_state.remove_replica(
            'svc',
            1,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
            expected_replica_record_id=(stale.replica_record_id))
        replacement = _replica(1, version=10)
        assert replacement.replica_record_id != stale.replica_record_id
        assert serve_state.add_or_update_replica(
            'svc',
            1,
            replacement,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner)

        assert not serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'incarnation-a', [(1, stale)],
            {1: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=1,
            max_limit=1,
            now=1.0,
            success_ttl_seconds=60.0,
            expected_controller_owner=owner)
        persisted = serve_state.get_replica_info_from_id('svc', 1)
        assert persisted is not None
        assert persisted.replica_record_id == replacement.replica_record_id
        assert persisted.version == 10


class TestGroupedReplicaSnapshot:
    """get_replica_infos_grouped reads the global replica table once."""

    def test_groups_all_services_in_one_statement(self, _mock_serve_db):
        for service_id in range(20):
            service_name = f'svc-{service_id}'
            infos = [(replica_id,
                      _replica(replica_id,
                               cluster_name=f'{service_name}-{replica_id}'))
                     for replica_id in range(3)]
            serve_state.add_or_update_replicas(service_name, infos)

        with _count_sql_statements(_mock_serve_db) as counts:
            grouped = serve_state.get_replica_infos_grouped()

        assert counts['n'] == 1
        assert set(grouped) == {f'svc-{i}' for i in range(20)}
        assert all(len(infos) == 3 for infos in grouped.values())
        assert all(
            info.cluster_name.startswith(f'{service_name}-')
            for service_name, infos in grouped.items()
            for info in infos)

    def test_empty_table_returns_empty_mapping(self, _mock_serve_db):
        assert not serve_state.get_replica_infos_grouped()


class TestReplicaClusterNameSnapshot:
    """Exact owner discovery reads only the scalar cluster-name column."""

    def test_returns_all_exact_cluster_names_in_one_statement(
            self, _mock_serve_db):
        serve_state.add_or_update_replicas(
            'svc-a', [(1, _replica(1, cluster_name='svc-a-r1')),
                      (2, _replica(2, cluster_name='svc-a-r2'))])
        serve_state.add_or_update_replica('svc-b', 1,
                                          _replica(1, cluster_name='svc-b-r1'))

        with _count_sql_statements(_mock_serve_db) as counts:
            cluster_names = serve_state.get_replica_cluster_names()

        assert counts['n'] == 1
        assert cluster_names == {'svc-a-r1', 'svc-a-r2', 'svc-b-r1'}

    def test_empty_table_returns_empty_set(self, _mock_serve_db):
        assert not serve_state.get_replica_cluster_names()
