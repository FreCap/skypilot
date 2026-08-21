"""PostgreSQL contracts for process-safe reserved-fill provider proofs."""

# pylint: disable=protected-access,redefined-outer-name,unused-import
# pylint: disable=wrong-import-position,not-callable,unnecessary-lambda
from collections.abc import Mapping
import concurrent.futures
import contextlib
import dataclasses
import datetime
import json
import multiprocessing
import os
import pathlib
import socket
import sys
import threading
import time
from typing import Any
from unittest import mock

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import reserved_fill_reclaim_pressure_worker as pressure_worker
import sqlalchemy
from test_pool_capacity_observation_pg import _DOCKER_AVAILABLE
from test_pool_capacity_observation_pg import _LOCAL_POSTGRES_AVAILABLE
from test_pool_capacity_observation_pg import _POSTGRES_REQUIRED
from test_pool_capacity_observation_pg import observation_engine  # noqa: F401
from test_pool_capacity_observation_pg import pg_server  # noqa: F401

from sky.serve import placement_normalization_authority
from sky.serve import reserved_fill_reclaim_attestation as reclaim
from sky.serve import reserved_fill_reclaim_proof_schema as proof_schema
from sky.serve import reserved_fill_reclaim_proofs as proofs
from sky.server.requests import process as request_process
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

_REPO_ROOT = pathlib.Path(__file__).parents[2]
_POLICY_PROJECT = _REPO_ROOT / 'boltz' / 'reserved_fill_reclaim_policy'
sys.path.insert(0, str(_POLICY_PROJECT / 'src'))

from boltz_reserved_fill_reclaim_policy import aws_attestation  # noqa: E402
from boltz_reserved_fill_reclaim_policy import (  # noqa: E402
    kubernetes_attestation)
from boltz_reserved_fill_reclaim_policy import policy as policy_lib

pytestmark = pytest.mark.skipif(
    not _LOCAL_POSTGRES_AVAILABLE and not _DOCKER_AVAILABLE and
    not _POSTGRES_REQUIRED,
    reason='Docker unavailable; skipping real-PostgreSQL proof tests')

_CONTEXT = 'phx_research_cluster_eks'
_GATE_GENERATION = 17
_CGROUP_MEMORY_MAX_ENV_VAR = 'SKYPILOT_SERVE054_CGROUP_MEMORY_MAX_BYTES'


def _read_process_cgroup_memory_bytes() -> tuple[str, int]:
    """Read the process cgroup's absolute peak, or current v2 usage.

    A cgroup namespace may expose the process cgroup at the controller mount
    root, while a host cgroup mount requires appending ``/proc/self/cgroup``'s
    relative path. Try both representations.
    """
    cgroup_entries = pathlib.Path('/proc/self/cgroup').read_text(
        encoding='utf-8').splitlines()
    v2_relative = ''
    v1_memory_relative = ''
    for entry in cgroup_entries:
        hierarchy_id, controllers, relative_path = entry.split(':', 2)
        if hierarchy_id == '0' and not controllers:
            v2_relative = relative_path.lstrip('/')
        elif 'memory' in controllers.split(','):
            v1_memory_relative = relative_path.lstrip('/')

    candidates = []
    v2_root = pathlib.Path('/sys/fs/cgroup')
    if v2_relative:
        candidates.extend((v2_root / v2_relative / 'memory.peak',
                           v2_root / v2_relative / 'memory.current'))
    candidates.extend((v2_root / 'memory.peak', v2_root / 'memory.current'))
    v1_root = v2_root / 'memory'
    if v1_memory_relative:
        candidates.append(v1_root / v1_memory_relative /
                          'memory.max_usage_in_bytes')
    candidates.append(v1_root / 'memory.max_usage_in_bytes')

    for candidate in candidates:
        try:
            value = candidate.read_text(encoding='utf-8').strip()
        except (FileNotFoundError, PermissionError):
            continue
        if value == 'max':
            continue
        try:
            return str(candidate), int(value)
        except ValueError as exc:
            raise AssertionError(
                f'Invalid cgroup memory value at {candidate}: {value!r}'
            ) from exc
    raise AssertionError(
        'Serve054 pressure qualification requires readable cgroup v2 '
        'memory.peak/current or v1 memory.max_usage_in_bytes.')


def _pressure_cgroup_memory_limit_bytes() -> int | None:
    raw_limit = os.environ.get(_CGROUP_MEMORY_MAX_ENV_VAR)
    if not raw_limit:
        return None
    try:
        limit = int(raw_limit)
    except ValueError as exc:
        raise AssertionError(
            f'{_CGROUP_MEMORY_MAX_ENV_VAR} must be an integer, got '
            f'{raw_limit!r}.') from exc
    assert limit > 0, f'{_CGROUP_MEMORY_MAX_ENV_VAR} must be positive.'
    return limit


def test_pressure_cgroup_memory_limit_empty_is_disabled(monkeypatch):
    monkeypatch.setenv(_CGROUP_MEMORY_MAX_ENV_VAR, '')
    assert _pressure_cgroup_memory_limit_bytes() is None


def _identity() -> reclaim.ReclaimPolicyIdentity:
    return reclaim.ReclaimPolicyIdentity(fleet_bundle_sha256='a' * 64,
                                         policy_revision='test-policy-v1',
                                         provider_inventory_sha256='b' * 64)


def _proof_payload() -> dict[str, Any]:
    return {
        'aws': {
            'safe': True,
            'count': 1,
        },
        'kubernetes': {
            'safe': True,
            'count': 1,
            'physical_cluster_uid': 'physical-cluster-uid',
        },
    }


def _proof_candidate(
    payload: Mapping[str, Any] | None = None,
    *,
    oldest_completed_monotonic: float | None = None,
) -> proofs.ReclaimProviderProofCandidate:
    if payload is None:
        payload = _proof_payload()
    if oldest_completed_monotonic is None:
        oldest_completed_monotonic = time.monotonic()
    return proofs.ReclaimProviderProofCandidate(
        proof_payload=payload,
        oldest_completed_monotonic=oldest_completed_monotonic)


def _accept_payload(_payload: Mapping[str, Any]) -> bool:
    return True


@pytest.fixture
def proof_engine(observation_engine):  # noqa: F811
    config = migration_utils.get_alembic_config(observation_engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, migration_utils.SERVE_VERSION)
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                CREATE TABLE serve054_test_provider_calls (
                    domain text PRIMARY KEY,
                    call_count bigint NOT NULL
                )
            """))
    yield observation_engine


def _admission(
    policy: policy_lib.BoltzReservedFillReclaimPolicy
) -> reclaim.ReclaimProjectedAdmission:
    context = policy._bundle.fleet_context(_CONTEXT)
    accelerator = context['accelerators']['h200']
    admission = context['kueue_admission']
    assert admission is not None
    priority = context['priority_class']
    return reclaim.ReclaimProjectedAdmission(
        worker_projection_sha256='c' * 64,
        kubernetes_context=_CONTEXT,
        namespace=context['namespace'],
        service_account_name=context['service_account_name'],
        pod_identity_role_arn=context['pod_identity_role_arn'],
        scheduler_name=context['scheduler_name'],
        priority_class_name=priority['name'],
        priority_value=priority['value'],
        preemption_policy=priority['preemption_policy'],
        admission_mode=reclaim.ReclaimAdmissionMode.KUEUE,
        local_queue_name=admission['local_queue_name'],
        workload_priority_class_name=(
            admission['workload_priority_class_name']),
        accelerator='h200',
        accelerator_count=accelerator['count'],
        accelerator_scheduling=reclaim.ReclaimAcceleratorScheduling(
            label_key=accelerator['product_label_key'],
            label_values=tuple(sorted(accelerator['product_label_values'])),
            resource_key=accelerator['resource_name']))


def _launch_scope(
    policy: policy_lib.BoltzReservedFillReclaimPolicy,
) -> reclaim.ReclaimLaunchScope:
    context = policy._bundle.fleet_context(_CONTEXT)
    return reclaim.ReclaimLaunchScope(
        service_name='boltz-l4-fleet',
        service_version=64,
        pool_key=json.dumps(['v2', context['physical_cluster_uid'], 'h200']),
        service_generation=21,
        physical_cluster_uid=context['physical_cluster_uid'],
        kubernetes_context=_CONTEXT,
        accelerator='h200',
        accelerator_count=1,
        projected_admission=_admission(policy))


def _provider_proof(
    policy: policy_lib.BoltzReservedFillReclaimPolicy,
    domain: str,
) -> object:
    context = policy._bundle.fleet_context(_CONTEXT)
    provider = policy._bundle.provider_context(_CONTEXT)
    if domain == 'aws':
        return aws_attestation.PodIdentityProof(
            kubernetes_context=_CONTEXT,
            cluster_arn=provider['eks']['cluster_arn'],
            namespace=context['namespace'],
            service_account_name=context['service_account_name'],
            expected_role_arn=context['pod_identity_role_arn'],
            association_count=0,
            identity_absence_proven=True)
    admission = context['kueue_admission']
    assert admission is not None
    return kubernetes_attestation.KubernetesContextProof(
        kubernetes_context=_CONTEXT,
        physical_cluster_uid=context['physical_cluster_uid'],
        namespace_uid=provider['namespace_uid'],
        kueue_managed=True,
        local_queue_name=admission['local_queue_name'],
        cluster_queue_name=admission['queues']['inference_cluster_queue'],
        pod_identity_irsa_annotation_absent=True,
        assign_queue_labels_for_pods=True,
        topology_aware_scheduling=True,
        custom_scheduler_deployment_proven=False,
        resource_flavor_topology_names=tuple(
            sorted((flavor['name'], flavor['topology_name'])
                   for flavor in provider['resource_flavors'])),
        node_flavors=tuple(
            kubernetes_attestation.NodeFlavorProof(
                flavor=node['flavor'],
                non_deleting_node_count=1,
                product_label_value=node['product_label_value'],
                resource_name=node['resource_name'],
                capacity_per_node=node['capacity_per_node'])
            for node in provider['node_inventory']))


@contextlib.contextmanager
def _tcp_accept_blackhole():
    """Accept every TCP connection without answering libpq's handshake."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.settimeout(0.05)
    accepted: list[socket.socket] = []
    stopped = threading.Event()

    def _accept():
        while not stopped.is_set():
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            accepted.append(connection)

    server = threading.Thread(target=_accept,
                              name='serve054-test-blackhole',
                              daemon=True)
    server.start()
    try:
        yield listener.getsockname()[1], accepted
    finally:
        stopped.set()
        listener.close()
        server.join(timeout=1)
        assert not server.is_alive()
        for connection in accepted:
            connection.close()


def _proof_session_count(engine: sqlalchemy.engine.Engine) -> int:
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.text("""
                SELECT count(*)
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND application_name IN (
                      'skypilot-reclaim-proof',
                      'skypilot-reclaim-proof-owner')
                  AND pid <> pg_backend_pid()
            """)).scalar_one()


def _wait_for_no_thread(prefix: str, timeout: float = 1) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not any(
                thread.name.startswith(prefix)
                for thread in threading.enumerate()):
            return True
        time.sleep(0.01)
    return False


def _disposable_stalled_launch(database_url: str) -> None:
    """Stall both providers while their exact proof locks are held."""
    engine = sqlalchemy.create_engine(database_url)
    counter_engine = sqlalchemy.create_engine(database_url,
                                              poolclass=sqlalchemy.NullPool)
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    repository = proofs.ReclaimProviderProofRepository(engine)

    def _provider_job(_context_name, domain, _deadline, _cancellation):
        with counter_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO serve054_test_provider_calls
                        (domain, call_count)
                    VALUES (:domain, 1)
                    ON CONFLICT (domain) DO UPDATE
                    SET call_count =
                        serve054_test_provider_calls.call_count + 1
                """), {'domain': domain})
        time.sleep(60)
        return _provider_proof(policy, domain)

    policy._provider_job = _provider_job
    policy._emit_proof = lambda _payload: None
    try:
        with mock.patch.object(policy_lib.reserved_fill_reclaim_proofs,
                               'ReclaimProviderProofRepository',
                               return_value=repository):
            policy.authorize_launch(_launch_scope(policy),
                                    expected_identity=policy.policy_identity(),
                                    expected_gate_generation=_GATE_GENERATION,
                                    deadline_monotonic=time.monotonic() + 5)
    finally:
        repository._proof_engine.dispose()
        counter_engine.dispose()
        engine.dispose()


def test_serve054_schema_is_postgresql_only_and_bounded(proof_engine,
                                                        monkeypatch):
    config = migration_utils.get_alembic_config(proof_engine,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)
    assert scripts.get_heads() == ['056']
    assert scripts.get_revision('054').down_revision == '053'
    assert migration_utils.SERVE_VERSION == '056'
    inspector = sqlalchemy.inspect(proof_engine)
    assert (proof_schema.serve_reserved_fill_reclaim_provider_proofs_table.name
            in inspector.get_table_names())
    columns = {
        column['name']: (str(column['type']), column['nullable']) for column in
        inspector.get_columns('serve_reserved_fill_reclaim_provider_proofs')
    }
    assert columns == {
        'receipt_nonce': ('TEXT', False),
        'reconciliation_gate_generation': ('BIGINT', False),
        'reclaim_fleet_bundle_sha256': ('TEXT', False),
        'reclaim_policy_revision': ('TEXT', False),
        'reclaim_provider_inventory_sha256': ('TEXT', False),
        'kubernetes_context': ('TEXT', False),
        'proof_schema_version': ('INTEGER', False),
        'proof_payload': ('JSONB', False),
        'proof_sha256': ('TEXT', False),
        'completed_at': ('TIMESTAMP', False),
    }
    reflected_columns = {
        column['name']: column for column in inspector.get_columns(
            'serve_reserved_fill_reclaim_provider_proofs')
    }
    assert reflected_columns['completed_at']['type'].timezone is True
    primary_key = inspector.get_pk_constraint(
        'serve_reserved_fill_reclaim_provider_proofs')
    assert primary_key['constrained_columns'] == ['receipt_nonce']
    assert primary_key['name'] == (
        'serve_reserved_fill_reclaim_provider_proofs_pkey')
    unique_constraints = inspector.get_unique_constraints(
        'serve_reserved_fill_reclaim_provider_proofs')
    assert [(constraint['name'], constraint['column_names'])
            for constraint in unique_constraints
           ] == [('serve054_reclaim_proof_authority_uq', [
               'reconciliation_gate_generation',
               'reclaim_fleet_bundle_sha256',
               'reclaim_policy_revision',
               'reclaim_provider_inventory_sha256',
               'kubernetes_context',
           ])]
    assert inspector.get_foreign_keys(
        'serve_reserved_fill_reclaim_provider_proofs') == []
    check_names = {
        constraint['name'] for constraint in inspector.get_check_constraints(
            'serve_reserved_fill_reclaim_provider_proofs')
    }
    assert check_names == {
        'serve054_reclaim_proof_digest_ck',
        'serve054_reclaim_proof_payload_ck',
        'serve054_reclaim_proof_positive_ck',
        'serve054_reclaim_proof_text_ck',
    }

    sqlite = sqlalchemy.create_engine('sqlite://')
    with pytest.raises(ValueError, match='PostgreSQL-only'):
        proofs.ReclaimProviderProofRepository(sqlite)
    migration = __import__(
        'sky.schemas.db.serve_state.054_reserved_fill_reclaim_provider_proofs',
        fromlist=['upgrade'])
    with sqlite.connect() as connection:
        monkeypatch.setattr(migration.op, 'get_bind', lambda: connection)
        with pytest.raises(RuntimeError, match='PostgreSQL-only'):
            migration.upgrade()
        with pytest.raises(RuntimeError, match='PostgreSQL-only'):
            migration.downgrade()


def test_serve054_is_forward_only(proof_engine):
    config = migration_utils.get_alembic_config(proof_engine,
                                                migration_utils.SERVE_DB_NAME)
    with pytest.raises(RuntimeError, match='forward-only'):
        alembic_command.downgrade(config, '053')
    assert migration_utils.get_current_alembic_revision(
        proof_engine, migration_utils.SERVE_DB_NAME) == '056'
    assert (proof_schema.serve_reserved_fill_reclaim_provider_proofs_table.name
            in sqlalchemy.inspect(proof_engine).get_table_names())


def test_application_context_payload_and_depth_bounds_are_exact():
    identity = _identity()
    exact_context = 'x' * reclaim.RECLAIM_PROVIDER_CONTEXT_MAX_BYTES
    assert len(
        reclaim.reclaim_provider_proof_lock_id(identity, _GATE_GENERATION,
                                               exact_context)) == 64
    with pytest.raises(ValueError, match='at most 1024 UTF-8 bytes'):
        reclaim.reclaim_provider_proof_lock_id(identity, _GATE_GENERATION,
                                               exact_context + 'x')

    empty_size = len(
        json.dumps({
            'blob': ''
        }, sort_keys=True, separators=(',', ':')).encode('utf-8'))
    exact_payload = {
        'blob': 'x' * (proofs.PROVIDER_PROOF_PAYLOAD_MAX_BYTES - empty_size)
    }
    normalized, digest = proofs.canonical_proof_payload(exact_payload)
    assert normalized == exact_payload
    assert len(digest) == 64
    with pytest.raises(proofs.ReclaimProviderProofError,
                       match='canonical JSON byte limit'):
        proofs.canonical_proof_payload({'blob': exact_payload['blob'] + 'x'})

    allowed_depth: Any = 'leaf'
    for _ in range(proofs._PROVIDER_PROOF_MAX_JSON_DEPTH - 1):
        allowed_depth = [allowed_depth]
    proofs.canonical_proof_payload({'nested': allowed_depth})
    rejected_depth: Any = [allowed_depth]
    with pytest.raises(proofs.ReclaimProviderProofError,
                       match='maximum JSON nesting depth'):
        proofs.canonical_proof_payload({'nested': rejected_depth})


def test_database_context_and_jsonb_storage_bounds_are_exact(proof_engine):
    table = proof_schema.serve_reserved_fill_reclaim_provider_proofs_table
    identity = _identity()

    def values(*, nonce: str, context: str, payload: dict[str, Any]):
        return {
            'receipt_nonce': nonce,
            'reconciliation_gate_generation': _GATE_GENERATION,
            'reclaim_fleet_bundle_sha256': identity.fleet_bundle_sha256,
            'reclaim_policy_revision': identity.policy_revision,
            'reclaim_provider_inventory_sha256':
                identity.provider_inventory_sha256,
            'kubernetes_context': context,
            'proof_schema_version': proofs.PROVIDER_PROOF_SCHEMA_VERSION,
            'proof_payload': payload,
            'proof_sha256': 'd' * 64,
            'completed_at': datetime.datetime.now(datetime.timezone.utc),
        }

    with proof_engine.connect() as connection:
        jsonb_overhead = connection.execute(
            sqlalchemy.text("""
                SELECT octet_length(jsonb_build_object('blob', '')::text)
            """)).scalar_one()
    exact_payload = {
        'blob': 'x' * (proof_schema.PROVIDER_PROOF_PAYLOAD_STORAGE_MAX_BYTES -
                       jsonb_overhead)
    }
    with proof_engine.begin() as connection:
        assert connection.execute(
            sqlalchemy.text("""
                SELECT octet_length(CAST(:payload AS jsonb)::text)
            """), {
                'payload': json.dumps(exact_payload)
            }).scalar_one() == (
                proof_schema.PROVIDER_PROOF_PAYLOAD_STORAGE_MAX_BYTES)
        connection.execute(
            sqlalchemy.insert(table).values(
                **values(nonce='1' * 64,
                         context='c' *
                         reclaim.RECLAIM_PROVIDER_CONTEXT_MAX_BYTES,
                         payload=exact_payload)))

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with proof_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(table).values(
                    **values(nonce='2' * 64,
                             context='oversize-payload',
                             payload={'blob': exact_payload['blob'] + 'x'})))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with proof_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(table).values(
                    **values(nonce='3' * 64,
                             context='c' *
                             (reclaim.RECLAIM_PROVIDER_CONTEXT_MAX_BYTES + 1),
                             payload={'safe': True})))


def test_pre_serve054_controller_cannot_adopt_head(proof_engine, monkeypatch):
    with proof_engine.connect() as connection:
        placement_normalization_authority.assert_reader_database_authority(
            connection)
    prior_revisions = frozenset(
        revision for revision in
        placement_normalization_authority.RECOGNIZED_ADDITIVE_REVISIONS
        if revision not in ('054', '055'))
    monkeypatch.setattr(placement_normalization_authority,
                        'RECOGNIZED_ADDITIVE_REVISIONS', prior_revisions)
    with proof_engine.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='recognized additive revision'):
            placement_normalization_authority.assert_reader_database_authority(
                connection)


def test_repository_preserves_url_options_and_installs_named_metrics(
        proof_engine, monkeypatch):
    configured_url = proof_engine.url.update_query_dict(
        {'options': '-c geqo=off'})
    configured_engine = sqlalchemy.create_engine(configured_url)
    captured = {}

    def _derive(engine, *, connect_args, engine_namespace,
                pool_reset_on_return):
        captured.update(engine=engine,
                        connect_args=connect_args,
                        engine_namespace=engine_namespace,
                        pool_reset_on_return=pool_reset_on_return)
        return sqlalchemy.create_engine(
            engine.url,
            poolclass=sqlalchemy.NullPool,
            connect_args=connect_args,
            pool_reset_on_return=(pool_reset_on_return))

    monkeypatch.setattr(db_utils, 'create_postgres_nullpool_engine', _derive)
    repository = proofs.ReclaimProviderProofRepository(configured_engine)
    try:
        assert captured['engine'] is configured_engine
        assert captured['engine_namespace'] == 'reserved-fill-reclaim-proof'
        assert captured['pool_reset_on_return'] is None
        assert captured['connect_args'] == {
            'connect_timeout': 1,
            'application_name': 'skypilot-reclaim-proof',
            'options': ('-c geqo=off -c statement_timeout=200ms '
                        '-c lock_timeout=200ms '
                        '-c idle_in_transaction_session_timeout=6000ms'),
        }
    finally:
        repository._proof_engine.dispose()
        configured_engine.dispose()


def test_slow_database_read_is_bounded_and_reaps_launch_workers(
        proof_engine, monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    provider_job = mock.Mock()
    monkeypatch.setattr(policy, '_provider_job', provider_job)
    monkeypatch.setattr(policy, '_emit_proof', lambda _payload: None)
    monkeypatch.setattr(
        repository, '_read_statement',
        lambda *_args, **_kwargs: sqlalchemy.select(
            sqlalchemy.func.pg_sleep(10).label('_blocked'),
            sqlalchemy.func.clock_timestamp().label('_database_now')))
    started = time.monotonic()

    with mock.patch.object(policy_lib.reserved_fill_reclaim_proofs,
                           'ReclaimProviderProofRepository',
                           return_value=repository):
        with pytest.raises(reclaim.ReclaimAttestationError,
                           match='before its deadline'):
            policy.authorize_launch(_launch_scope(policy),
                                    expected_identity=policy.policy_identity(),
                                    expected_gate_generation=_GATE_GENERATION,
                                    deadline_monotonic=started + 5)

    assert time.monotonic() - started < 2
    provider_job.assert_not_called()
    assert _wait_for_no_thread('boltz-reclaim-launch')
    assert _proof_session_count(proof_engine) == 0
    with proof_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        ).scalar_one() == 0


def test_blackholed_connect_is_single_attempt_reaps_workers_and_recovers(
        proof_engine, monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    provider_job = mock.Mock()
    monkeypatch.setattr(policy, '_provider_job', provider_job)
    monkeypatch.setattr(policy, '_emit_proof', lambda _payload: None)
    with _tcp_accept_blackhole() as (port, accepted):
        unavailable_url = proof_engine.url.set(host='127.0.0.1', port=port)
        unavailable_engine = sqlalchemy.create_engine(unavailable_url)
        repository = proofs.ReclaimProviderProofRepository(unavailable_engine)
        started = time.monotonic()
        with mock.patch.object(policy_lib.reserved_fill_reclaim_proofs,
                               'ReclaimProviderProofRepository',
                               return_value=repository):
            with pytest.raises(reclaim.ReclaimAttestationError,
                               match='before its deadline'):
                policy.authorize_launch(
                    _launch_scope(policy),
                    expected_identity=policy.policy_identity(),
                    expected_gate_generation=_GATE_GENERATION,
                    deadline_monotonic=started + 5)
        elapsed = time.monotonic() - started
        repository._proof_engine.dispose()
        unavailable_engine.dispose()
        assert len(accepted) == 1

    assert elapsed < 3
    provider_job.assert_not_called()
    assert _wait_for_no_thread('boltz-reclaim-launch')
    assert _proof_session_count(proof_engine) == 0
    with proof_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        ).scalar_one() == 0

    healthy_calls = []
    healthy_deadlines = []

    def _healthy_provider(context_name, domain, provider_deadline,
                          _cancellation):
        assert context_name == _CONTEXT
        healthy_calls.append(domain)
        healthy_deadlines.append(provider_deadline)
        return _provider_proof(policy, domain)

    monkeypatch.setattr(policy, '_provider_job', _healthy_provider)
    healthy_repository = proofs.ReclaimProviderProofRepository(proof_engine)
    outer_deadline = time.monotonic() + 5
    with mock.patch.object(policy_lib.reserved_fill_reclaim_proofs,
                           'ReclaimProviderProofRepository',
                           return_value=healthy_repository):
        authorization = policy.authorize_launch(
            _launch_scope(policy),
            expected_identity=policy.policy_identity(),
            expected_gate_generation=_GATE_GENERATION,
            deadline_monotonic=outer_deadline)
    assert len(authorization.provider_proof_reference.receipt_nonce) == 64
    assert sorted(healthy_calls) == ['aws', 'kubernetes']
    assert healthy_deadlines == pytest.approx((outer_deadline - 0.5,) * 2)
    with proof_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        ).scalar_one() == 1


def test_blackholed_election_transaction_has_no_retry_or_provider_call(
        proof_engine, monkeypatch):
    with _tcp_accept_blackhole() as (port, accepted):
        unavailable_url = proof_engine.url.set(host='127.0.0.1', port=port)
        unavailable_engine = sqlalchemy.create_engine(unavailable_url)
        repository = proofs.ReclaimProviderProofRepository(unavailable_engine)
        database_anchor = datetime.datetime.now(datetime.timezone.utc)
        monkeypatch.setattr(
            repository, '_read',
            mock.Mock(return_value=(None, database_anchor, time.monotonic())))
        provider = mock.Mock()
        started = time.monotonic()
        with pytest.raises(Exception):
            repository.get_or_prove(identity=_identity(),
                                    gate_generation=_GATE_GENERATION,
                                    kubernetes_context=_CONTEXT,
                                    deadline_monotonic=started + 5,
                                    prove=provider,
                                    validate=_accept_payload)
        elapsed = time.monotonic() - started
        repository._proof_engine.dispose()
        unavailable_engine.dispose()
        assert len(accepted) == 1

    assert elapsed < 3
    provider.assert_not_called()


def test_slow_leader_reread_is_bounded_before_provider(proof_engine,
                                                       monkeypatch):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    original_statement = repository._read_statement
    statement_count = 0

    def _statement(*args, **kwargs):
        nonlocal statement_count
        statement_count += 1
        if statement_count == 2:
            return sqlalchemy.select(
                sqlalchemy.func.pg_sleep(10).label('_blocked'),
                sqlalchemy.func.clock_timestamp().label('_database_now'))
        return original_statement(*args, **kwargs)

    monkeypatch.setattr(repository, '_read_statement', _statement)
    provider = mock.Mock()
    started = time.monotonic()
    with pytest.raises(Exception):
        repository.get_or_prove(identity=_identity(),
                                gate_generation=_GATE_GENERATION,
                                kubernetes_context=_CONTEXT,
                                deadline_monotonic=started + 5,
                                prove=provider,
                                validate=_accept_payload)

    assert time.monotonic() - started < 2
    assert statement_count == 2
    provider.assert_not_called()
    assert _proof_session_count(proof_engine) == 0


def test_slow_publication_rolls_back_and_closes_transaction(proof_engine):
    with proof_engine.begin() as connection:
        connection.exec_driver_sql("""
            CREATE FUNCTION serve054_test_slow_publication()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM pg_sleep(10);
                RETURN NEW;
            END
            $$
        """)
        connection.exec_driver_sql("""
            CREATE TRIGGER serve054_test_slow_publication
            BEFORE INSERT ON serve_reserved_fill_reclaim_provider_proofs
            FOR EACH ROW EXECUTE FUNCTION serve054_test_slow_publication()
        """)
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    provider = mock.Mock(side_effect=_proof_candidate)
    started = time.monotonic()
    with pytest.raises(Exception):
        repository.get_or_prove(identity=_identity(),
                                gate_generation=_GATE_GENERATION,
                                kubernetes_context=_CONTEXT,
                                deadline_monotonic=started + 5,
                                prove=provider,
                                validate=_accept_payload)

    assert time.monotonic() - started < 2
    provider.assert_called_once()
    assert _proof_session_count(proof_engine) == 0
    with proof_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        ).scalar_one() == 0


def test_provider_does_not_begin_without_publication_reserve(
        proof_engine, monkeypatch):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    read_count = 0

    def _consume_horizon(**_kwargs):
        nonlocal read_count
        read_count += 1
        if read_count == 2:
            time.sleep(1.35)
        return (None, datetime.datetime.now(datetime.timezone.utc),
                time.monotonic())

    monkeypatch.setattr(repository, '_read', _consume_horizon)
    provider = mock.Mock()
    started = time.monotonic()
    with pytest.raises(proofs.ReclaimProviderProofError,
                       match='deadline is invalid or expired'):
        repository.get_or_prove(identity=_identity(),
                                gate_generation=_GATE_GENERATION,
                                kubernetes_context=_CONTEXT,
                                deadline_monotonic=started + 1.8,
                                prove=provider,
                                validate=_accept_payload)
    provider.assert_not_called()
    assert read_count == 2
    assert time.monotonic() - started < 1.8
    assert _proof_session_count(proof_engine) == 0


def test_disposable_boundary_kills_stalled_proof_family(proof_engine):
    database_url = proof_engine.url.render_as_string(hide_password=False)
    executor = request_process.DisposableExecutor(max_workers=1)
    try:
        future = executor.submit(_disposable_stalled_launch, database_url)
        wait_deadline = time.monotonic() + 10
        calls = {}
        while time.monotonic() < wait_deadline:
            with proof_engine.connect() as connection:
                calls = dict(
                    connection.execute(
                        sqlalchemy.text("""
                            SELECT domain, call_count
                            FROM serve054_test_provider_calls
                            ORDER BY domain
                        """)).all())
            if calls == {'aws': 1, 'kubernetes': 1}:
                break
            time.sleep(0.01)
        assert calls == {'aws': 1, 'kubernetes': 1}
        with proof_engine.connect() as connection:
            active_sessions, advisory_holders = connection.execute(
                sqlalchemy.text("""
                    SELECT
                        count(DISTINCT activity.pid),
                        count(DISTINCT activity.pid) FILTER (
                            WHERE locks.locktype = 'advisory'
                              AND locks.granted)
                    FROM pg_stat_activity AS activity
                    LEFT JOIN pg_locks AS locks ON locks.pid = activity.pid
                    WHERE activity.datname = current_database()
                      AND activity.application_name IN (
                          'skypilot-reclaim-proof',
                          'skypilot-reclaim-proof-owner')
                """)).one()
        assert active_sessions == 1
        assert advisory_holders == 1
        started = time.monotonic()
        with pytest.raises(reclaim.ReclaimAttestationError,
                           match='before its deadline'):
            future.result(timeout=12)
        elapsed = time.monotonic() - started
        assert future.boundary_result is not None
        assert future.boundary_result.family_drained
        assert (future.boundary_result.outcome.kind
                is request_process.InvocationOutcomeKind.FAILED)
    finally:
        executor.shutdown()

    assert elapsed < 8
    assert _proof_session_count(proof_engine) == 0
    with proof_engine.connect() as connection:
        calls = dict(
            connection.execute(
                sqlalchemy.text("""
                SELECT domain, call_count
                FROM serve054_test_provider_calls
                ORDER BY domain
            """)).all())
        rows = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        ).scalar_one()
        proof_locks = connection.execute(
            sqlalchemy.text("""
                SELECT count(*)
                FROM pg_locks AS locks
                JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
                WHERE activity.datname = current_database()
                  AND activity.application_name IN (
                      'skypilot-reclaim-proof',
                      'skypilot-reclaim-proof-owner')
                  AND locks.locktype = 'advisory'
            """)).scalar_one()
    assert calls == {'aws': 1, 'kubernetes': 1}
    assert rows == 0
    assert proof_locks == 0

    recovered = proofs.ReclaimProviderProofRepository(
        proof_engine).get_or_prove(identity=_identity(),
                                   gate_generation=_GATE_GENERATION,
                                   kubernetes_context=_CONTEXT,
                                   deadline_monotonic=time.monotonic() + 5,
                                   prove=lambda: _proof_candidate(),
                                   validate=_accept_payload)
    assert len(recovered.reference.receipt_nonce) == 64


def test_physical_close_crossing_deadline_denies_but_receipt_is_reusable(
        proof_engine, monkeypatch):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    original_require_deadline = proofs._require_deadline
    physically_closed = False

    def _closed(_dbapi_connection, _connection_record):
        nonlocal physically_closed
        physically_closed = True

    def _require_before_close(deadline):
        if physically_closed:
            raise proofs.ReclaimProviderProofError(
                'forced deadline crossing after physical close')
        return original_require_deadline(deadline)

    sqlalchemy.event.listen(repository._proof_engine, 'close', _closed)
    monkeypatch.setattr(proofs, '_require_deadline', _require_before_close)
    provider = mock.Mock(side_effect=_proof_candidate)
    kwargs = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
        'validate': _accept_payload,
    }
    with pytest.raises(proofs.ReclaimProviderProofError,
                       match='physical close'):
        repository.get_or_prove(**kwargs,
                                deadline_monotonic=time.monotonic() + 5,
                                prove=provider)
    provider.assert_called_once()
    sqlalchemy.event.remove(repository._proof_engine, 'close', _closed)
    monkeypatch.setattr(proofs, '_require_deadline', original_require_deadline)
    with proof_engine.connect() as connection:
        persisted_nonce = connection.execute(
            sqlalchemy.select(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table.
                c.receipt_nonce)).scalar_one()

    # A result crossing the boundary after commit/physical close denies this
    # caller but does not make the exact server-committed row unsafe to reuse.
    unexpected_provider = mock.Mock()
    reused = repository.get_or_prove(**kwargs,
                                     deadline_monotonic=time.monotonic() + 5,
                                     prove=unexpected_provider)
    unexpected_provider.assert_not_called()
    assert reused.reference.receipt_nonce == persisted_nonce


def test_lost_commit_ack_denies_current_but_later_reuses_receipt(
        proof_engine, monkeypatch):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    original_commit = repository._commit

    def _commit_then_lose_ack(transaction):
        original_commit(transaction)
        raise proofs.ReclaimProviderProofError(
            'forced lost commit acknowledgement')

    monkeypatch.setattr(repository, '_commit', _commit_then_lose_ack)
    provider = mock.Mock(side_effect=_proof_candidate)
    kwargs = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
        'validate': _accept_payload,
    }
    with pytest.raises(proofs.ReclaimProviderProofError,
                       match='lost commit acknowledgement'):
        repository.get_or_prove(**kwargs,
                                deadline_monotonic=time.monotonic() + 5,
                                prove=provider)
    provider.assert_called_once()
    monkeypatch.setattr(repository, '_commit', original_commit)
    unexpected_provider = mock.Mock()
    reused = repository.get_or_prove(**kwargs,
                                     deadline_monotonic=time.monotonic() + 5,
                                     prove=unexpected_provider)
    unexpected_provider.assert_not_called()
    with proof_engine.connect() as connection:
        assert reused.reference.receipt_nonce == connection.execute(
            sqlalchemy.select(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table.
                c.receipt_nonce)).scalar_one()


def test_physical_close_crossing_deadline_denies_existing_receipt_under_lock(
        proof_engine, monkeypatch):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    kwargs = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
        'validate': _accept_payload,
    }
    seeded = repository.get_or_prove(**kwargs,
                                     deadline_monotonic=time.monotonic() + 5,
                                     prove=lambda: _proof_candidate())
    original_read = repository._read
    read_count = 0

    def _miss_once(**read_kwargs):
        nonlocal read_count
        read_count += 1
        if read_count == 1:
            return (None, datetime.datetime.now(datetime.timezone.utc),
                    time.monotonic())
        return original_read(**read_kwargs)

    physically_closed = False
    original_require_deadline = proofs._require_deadline

    def _closed(_dbapi_connection, _connection_record):
        nonlocal physically_closed
        physically_closed = True

    def _require_before_close(deadline):
        if physically_closed:
            raise proofs.ReclaimProviderProofError(
                'forced deadline crossing after physical close')
        return original_require_deadline(deadline)

    monkeypatch.setattr(repository, '_read', _miss_once)
    sqlalchemy.event.listen(repository._proof_engine, 'close', _closed)
    monkeypatch.setattr(proofs, '_require_deadline', _require_before_close)
    provider = mock.Mock()
    with pytest.raises(proofs.ReclaimProviderProofError,
                       match='physical close'):
        repository.get_or_prove(**kwargs,
                                deadline_monotonic=time.monotonic() + 5,
                                prove=provider)
    provider.assert_not_called()
    assert read_count == 2
    sqlalchemy.event.remove(repository._proof_engine, 'close', _closed)
    with proof_engine.connect() as connection:
        nonces = connection.execute(
            sqlalchemy.select(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table.
                c.receipt_nonce)).scalars().all()
    assert nonces == [seeded.reference.receipt_nonce]


def test_launch_database_and_provider_io_run_only_on_proof_workers(
        proof_engine, monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    original_get_or_prove = repository.get_or_prove
    construction_threads = []
    repository_threads = []
    provider_threads = []

    def _repository_factory():
        construction_threads.append(threading.current_thread().name)
        assert threading.current_thread().name.startswith(
            'boltz-reclaim-launch')
        return repository

    def _get_or_prove(**kwargs):
        repository_threads.append(threading.current_thread().name)
        assert threading.current_thread().name.startswith(
            'boltz-reclaim-launch')
        return original_get_or_prove(**kwargs)

    def _provider_job(context_name, domain, _deadline, _cancellation):
        assert context_name == _CONTEXT
        provider_threads.append(threading.current_thread().name)
        assert threading.current_thread().name.startswith(
            'boltz-reclaim-attest')
        return _provider_proof(policy, domain)

    monkeypatch.setattr(repository, 'get_or_prove', _get_or_prove)
    monkeypatch.setattr(proofs, 'ReclaimProviderProofRepository',
                        _repository_factory)
    monkeypatch.setattr(policy, '_provider_job', _provider_job)
    monkeypatch.setattr(policy, '_emit_proof', lambda _payload: None)
    policy.authorize_launch(_launch_scope(policy),
                            expected_identity=policy.policy_identity(),
                            expected_gate_generation=_GATE_GENERATION,
                            deadline_monotonic=time.monotonic() + 5)

    assert len(construction_threads) == 1
    assert len(repository_threads) == 1
    assert len(provider_threads) == 2
    assert _wait_for_no_thread('boltz-reclaim-launch')


def test_context_receipt_uses_oldest_asymmetric_domain_completion(
        proof_engine, monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    starts = {}
    completions = {}

    def _provider_job(context_name, domain, _deadline, _cancellation):
        assert context_name == _CONTEXT
        starts[domain] = time.monotonic()
        time.sleep(0.45 if domain == 'kubernetes' else 0.2)
        value = _provider_proof(policy, domain)
        completions[domain] = time.monotonic()
        return value

    monkeypatch.setattr(policy, '_provider_job', _provider_job)
    monkeypatch.setattr(policy, '_emit_proof', lambda _payload: None)
    with mock.patch.object(policy_lib.reserved_fill_reclaim_proofs,
                           'ReclaimProviderProofRepository',
                           return_value=repository):
        authorization = policy.authorize_launch(
            _launch_scope(policy),
            expected_identity=policy.policy_identity(),
            expected_gate_generation=_GATE_GENERATION,
            deadline_monotonic=time.monotonic() + 5)

    completed = authorization.provider_proof_reference.completed_monotonic
    assert completed - min(starts.values()) >= 0.15
    assert completed <= completions['aws']
    assert completions['aws'] - completed < 0.15
    assert completions['kubernetes'] - completed >= 0.2
    with proof_engine.begin() as connection:
        assert proofs.provider_proof_reference_holds_in_connection(
            connection,
            authorization.provider_proof_reference,
            expected_physical_cluster_uid=(
                authorization.scope.physical_cluster_uid))


def test_repository_rejects_untyped_or_out_of_interval_candidate(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    cases = (
        (lambda: _proof_payload(), 'candidate is untyped'),
        (lambda: _proof_candidate(oldest_completed_monotonic=0),
         'outside its exact execution interval'),
        (lambda: _proof_candidate(oldest_completed_monotonic=time.monotonic() +
                                  10), 'outside its exact execution interval'),
    )
    for prove, message in cases:
        with pytest.raises(proofs.ReclaimProviderProofError, match=message):
            repository.get_or_prove(identity=_identity(),
                                    gate_generation=_GATE_GENERATION,
                                    kubernetes_context=_CONTEXT,
                                    deadline_monotonic=time.monotonic() + 5,
                                    prove=prove,
                                    validate=_accept_payload)
    with proof_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        ).scalar_one() == 0


@pytest.mark.parametrize(
    'worker_count',
    (15, pytest.param(90, marks=pytest.mark.serve054_process_pressure)))
def test_multiprocess_launches_share_fresh_provider_receipt(
        proof_engine, worker_count):
    # The ordinary pytest process keeps the normal package initializers. Only
    # each clean spawned worker takes the pressure helper's narrow import path.
    assert not pressure_worker.MINIMAL_SKY_PACKAGE_BOOTSTRAP
    cgroup_memory_limit = _pressure_cgroup_memory_limit_bytes()
    cgroup_memory_source = None
    cgroup_memory_samples = []

    def _sample_cgroup_memory() -> None:
        nonlocal cgroup_memory_source
        if cgroup_memory_limit is None:
            return
        source, usage_bytes = _read_process_cgroup_memory_bytes()
        if cgroup_memory_source is None:
            cgroup_memory_source = source
        else:
            assert source == cgroup_memory_source
        cgroup_memory_samples.append(usage_bytes)

    def _collect_barrier(queue_proxy: Any, count: int) -> tuple[Any, ...]:
        return tuple(queue_proxy.get(timeout=10) for _ in range(count))

    # With memory.peak this includes setup in the fresh dedicated job. With a
    # v2 memory.current-only controller, the ready and active-wave samples keep
    # the qualification conservative for the 90 resident workers.
    _sample_cgroup_memory()
    database_url = proof_engine.url.render_as_string(hide_password=False)
    context = multiprocessing.get_context('spawn')
    provider_delay_seconds = 3.2
    peak_total_proof_sessions = 0
    peak_owner_sessions = 0
    peak_advisory_holders = 0
    peak_advisory_waiters = 0
    peak_ordinary_worker_sessions = 0

    def _sample_database_activity(
            monitor: sqlalchemy.engine.Connection) -> None:
        nonlocal peak_total_proof_sessions
        nonlocal peak_owner_sessions
        nonlocal peak_advisory_holders
        nonlocal peak_advisory_waiters
        nonlocal peak_ordinary_worker_sessions
        (total_proof_sessions, owner_sessions, advisory_holders,
         advisory_waiters, ordinary_worker_sessions) = monitor.execute(
             sqlalchemy.text("""
                SELECT
                    count(*) FILTER (
                        WHERE application_name IN (
                            'skypilot-reclaim-proof',
                            'skypilot-reclaim-proof-owner',
                            'skypilot-reclaim-proof-worker',
                            'skypilot-reclaim-proof-counter')),
                    count(*) FILTER (
                        WHERE application_name =
                                  'skypilot-reclaim-proof-owner'),
                    (SELECT count(*)
                     FROM pg_locks AS holder_locks
                     JOIN pg_stat_activity AS holder_activity
                       ON holder_activity.pid = holder_locks.pid
                     WHERE holder_activity.datname = current_database()
                       AND holder_activity.application_name IN (
                           'skypilot-reclaim-proof',
                           'skypilot-reclaim-proof-owner')
                       AND holder_locks.locktype = 'advisory'
                       AND holder_locks.granted),
                    (SELECT count(*)
                     FROM pg_locks AS waiter_locks
                     JOIN pg_stat_activity AS waiter_activity
                       ON waiter_activity.pid = waiter_locks.pid
                     WHERE waiter_activity.datname = current_database()
                       AND waiter_activity.application_name IN (
                           'skypilot-reclaim-proof',
                           'skypilot-reclaim-proof-owner')
                       AND waiter_locks.locktype = 'advisory'
                       AND NOT waiter_locks.granted),
                    count(*) FILTER (
                        WHERE application_name =
                              'skypilot-reclaim-proof-worker')
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
            """)).one()
        peak_total_proof_sessions = max(peak_total_proof_sessions,
                                        total_proof_sessions)
        peak_owner_sessions = max(peak_owner_sessions, owner_sessions)
        peak_advisory_holders = max(peak_advisory_holders, advisory_holders)
        peak_advisory_waiters = max(peak_advisory_waiters, advisory_waiters)
        peak_ordinary_worker_sessions = max(peak_ordinary_worker_sessions,
                                            ordinary_worker_sessions)
        _sample_cgroup_memory()

    with context.Manager() as manager:
        ready_queue = manager.Queue()
        start_event = manager.Event()
        deadline_value = manager.Value('d', 0.0)
        provider_started_queue = manager.Queue()
        provider_release_event = manager.Event()
        loser_parked_queue = manager.Queue()
        loser_release_event = manager.Event()
        try:
            with concurrent.futures.ProcessPoolExecutor(
                    max_workers=worker_count, mp_context=context
            ) as executor, contextlib.ExitStack() as barrier_cleanup:
                # This stack exits before the executor. Assertions therefore
                # release every parked worker before executor.__exit__ waits.
                barrier_cleanup.callback(loser_release_event.set)
                barrier_cleanup.callback(provider_release_event.set)
                # A setup assertion can fire before the normal cold-wave
                # release. Publishing the unset deadline wakes those workers
                # into their immediate fail-fast validation instead of a
                # 120-second test-only wait.
                barrier_cleanup.callback(start_event.set)
                futures = tuple(
                    executor.submit(
                        pressure_worker.multiprocess_launch_authorization,
                        database_url, ready_queue, start_event, deadline_value,
                        provider_started_queue, provider_release_event,
                        loser_parked_queue, loser_release_event)
                    for _ in range(worker_count))
                ready_pids = {
                    ready_queue.get(timeout=180) for _ in range(worker_count)
                }
                assert len(ready_pids) == worker_count
                _sample_cgroup_memory()
                with proof_engine.connect().execution_options(
                        isolation_level='AUTOCOMMIT') as monitor:
                    session_baseline = monitor.execute(
                        sqlalchemy.text("""
                            SELECT sessions
                            FROM pg_stat_database
                            WHERE datname = current_database()
                        """)).scalar_one()
                    deadline_value.value = (
                        time.monotonic() +
                        reclaim.POLICY_OPERATION_TIMEOUT_SECONDS)
                    wave_started = (deadline_value.value -
                                    reclaim.POLICY_OPERATION_TIMEOUT_SECONDS)
                    start_event.set()
                    with concurrent.futures.ThreadPoolExecutor(
                            max_workers=2) as barrier_executor:
                        providers_future = barrier_executor.submit(
                            _collect_barrier, provider_started_queue, 2)
                        losers_future = barrier_executor.submit(
                            _collect_barrier, loser_parked_queue,
                            worker_count - 1)
                        while not (providers_future.done() and
                                   losers_future.done()):
                            _sample_database_activity(monitor)
                            time.sleep(0.01)
                        provider_starts = providers_future.result()
                        parked_losers = set(losers_future.result())

                    provider_pids = {record[0] for record in provider_starts}
                    provider_domains = {record[1] for record in provider_starts}
                    assert len(provider_pids) == 1
                    assert provider_domains == {'aws', 'kubernetes'}
                    assert provider_pids.issubset(ready_pids)
                    assert parked_losers == ready_pids - provider_pids
                    parked_state = monitor.execute(
                        sqlalchemy.text("""
                            SELECT
                                count(*) FILTER (
                                    WHERE application_name =
                                      'skypilot-reclaim-proof-owner'),
                                count(*) FILTER (
                                    WHERE application_name =
                                      'skypilot-reclaim-proof-owner'
                                      AND state = 'idle in transaction'),
                                count(*) FILTER (
                                    WHERE application_name =
                                      'skypilot-reclaim-proof'),
                                (SELECT count(*)
                                 FROM pg_locks AS holder_locks
                                 JOIN pg_stat_activity AS holder_activity
                                   ON holder_activity.pid = holder_locks.pid
                                 WHERE holder_activity.datname =
                                       current_database()
                                   AND holder_activity.application_name IN (
                                     'skypilot-reclaim-proof',
                                     'skypilot-reclaim-proof-owner')
                                   AND holder_locks.locktype = 'advisory'
                                   AND holder_locks.granted),
                                (SELECT count(*)
                                 FROM pg_locks AS waiter_locks
                                 JOIN pg_stat_activity AS waiter_activity
                                   ON waiter_activity.pid = waiter_locks.pid
                                 WHERE waiter_activity.datname =
                                       current_database()
                                   AND waiter_activity.application_name IN (
                                     'skypilot-reclaim-proof',
                                     'skypilot-reclaim-proof-owner')
                                   AND waiter_locks.locktype = 'advisory'
                                   AND NOT waiter_locks.granted)
                            FROM pg_stat_activity
                            WHERE datname = current_database()
                              AND pid <> pg_backend_pid()
                        """)).one()
                    assert parked_state == (1, 1, 0, 1, 0)
                    _sample_database_activity(monitor)
                    # The deterministic snapshot above proves that every
                    # loser closed its election transaction. Release only the
                    # losers now so the real receipt-polling pressure runs
                    # while the single provider owner remains parked.
                    loser_release_event.set()

                    release_at = wave_started + provider_delay_seconds
                    while time.monotonic() < release_at:
                        _sample_database_activity(monitor)
                        time.sleep(
                            min(0.01, max(0, release_at - time.monotonic())))
                    provider_release_event.set()
                    while not all(future.done() for future in futures):
                        _sample_database_activity(monitor)
                        time.sleep(0.01)
                    session_total = monitor.execute(
                        sqlalchemy.text("""
                            SELECT sessions
                            FROM pg_stat_database
                            WHERE datname = current_database()
                        """)).scalar_one()
                results = tuple(future.result() for future in futures)
        finally:
            # Assertions must never strand the 90 spawned workers behind a
            # test-only barrier.
            provider_release_event.set()
            loser_release_event.set()
    _sample_cgroup_memory()
    assert len({result[0] for result in results}) == 1
    assert all(result[1] for result in results)
    assert all(result[4] for result in results)
    assert max(result[2] for result in results) < 5
    pressure_metrics = {
        'workers': worker_count,
        'max_elapsed_seconds': max(result[2] for result in results),
        'max_start_skew_seconds': max(result[3] for result in results),
        'peak_total_proof_sessions': peak_total_proof_sessions,
        'peak_owner_sessions': peak_owner_sessions,
        'peak_advisory_holders': peak_advisory_holders,
        'peak_advisory_waiters': peak_advisory_waiters,
        'peak_ordinary_worker_sessions': peak_ordinary_worker_sessions,
        'physical_session_opens': session_total - session_baseline,
    }
    if cgroup_memory_limit is not None:
        assert cgroup_memory_source is not None
        assert cgroup_memory_samples
        pressure_metrics.update({
            'cgroup_memory_source': cgroup_memory_source,
            'cgroup_memory_peak_bytes': max(cgroup_memory_samples),
            'cgroup_memory_limit_bytes': cgroup_memory_limit,
        })
    print('serve054_pressure_metrics=' +
          json.dumps(pressure_metrics, sort_keys=True))
    # The one context leader retains one dedicated session. Every losing
    # probe and receipt read must close before its local wait instead of
    # pinning a PostgreSQL backend for the duration of provider work.
    assert peak_owner_sessions == 1
    assert peak_advisory_holders == 1
    assert peak_advisory_waiters == 0
    assert peak_ordinary_worker_sessions <= worker_count
    # Initial jitter and exponential receipt polling keep instantaneous
    # sessions below PostgreSQL's default 100-session envelope. The complete
    # empirical wave, including every terminal guard, must stay within eight
    # physical opens per caller plus a small fixed monitor/provider allowance.
    assert peak_total_proof_sessions < 100
    assert session_total - session_baseline < worker_count * 8 + 20
    with proof_engine.connect() as connection:
        calls = dict(
            connection.execute(
                sqlalchemy.text("""
                    SELECT domain, call_count
                    FROM serve054_test_provider_calls
                    ORDER BY domain
                """)).all())
        rows = connection.execute(
            sqlalchemy.select(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        ).mappings().all()
        surviving_sessions = connection.execute(
            sqlalchemy.text("""
                SELECT count(*)
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND application_name IN (
                      'skypilot-reclaim-proof',
                      'skypilot-reclaim-proof-owner',
                      'skypilot-reclaim-proof-worker',
                      'skypilot-reclaim-proof-counter')
            """)).scalar_one()
    assert calls == {'aws': 1, 'kubernetes': 1}
    assert len(rows) == 1
    assert len(rows[0]['receipt_nonce']) == 64
    assert surviving_sessions == 0
    if cgroup_memory_limit is not None:
        cgroup_memory_peak = max(cgroup_memory_samples)
        assert cgroup_memory_peak <= cgroup_memory_limit, (
            f'Serve054 pressure cgroup peak {cgroup_memory_peak} exceeds '
            f'the dedicated-runner limit {cgroup_memory_limit}.')


def test_expired_identical_receipt_refreshes_once_without_revoking_reference(
        proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    calls = 0
    calls_lock = threading.Lock()

    def prove():
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.1)
        return _proof_candidate()

    kwargs = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
        'deadline_monotonic': time.monotonic() + 5,
        'prove': prove,
        'validate': _accept_payload,
    }
    first = repository.get_or_prove(**kwargs)
    with proof_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE serve_reserved_fill_reclaim_provider_proofs
                SET completed_at = CURRENT_TIMESTAMP - INTERVAL '10 seconds'
            """))

    def refresh():
        return repository.get_or_prove(**{
            **kwargs,
            'deadline_monotonic': time.monotonic() + 5,
        })

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        refreshed = tuple(executor.map(lambda _: refresh(), range(8)))
    assert calls == 2
    refreshed_nonces = {item.reference.receipt_nonce for item in refreshed}
    assert len(refreshed_nonces) == 1
    assert refreshed_nonces == {first.reference.receipt_nonce}
    with proof_engine.begin() as connection:
        assert proofs.provider_proof_reference_holds_in_connection(
            connection,
            first.reference,
            expected_physical_cluster_uid='physical-cluster-uid')


def test_receipt_without_terminal_guard_reserve_is_refreshed(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    calls = 0

    def prove():
        nonlocal calls
        calls += 1
        return _proof_candidate()

    kwargs = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
        'prove': prove,
        'validate': _accept_payload,
    }
    first = repository.get_or_prove(**kwargs,
                                    deadline_monotonic=time.monotonic() + 5)
    near_expiry_age = (reclaim.AUTHORIZATION_MAX_AGE_SECONDS -
                       reclaim.LAUNCH_AUTHORIZATION_MIN_REMAINING_SECONDS / 2)
    with proof_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE serve_reserved_fill_reclaim_provider_proofs
                SET completed_at = clock_timestamp()
                    - make_interval(secs => :age)
            """), {'age': near_expiry_age})

    refreshed = repository.get_or_prove(**kwargs,
                                        deadline_monotonic=time.monotonic() + 5)

    assert calls == 2
    assert refreshed.reference.receipt_nonce == first.reference.receipt_nonce
    assert refreshed.has_terminal_guard_reserve
    with proof_engine.begin() as connection:
        assert proofs.provider_proof_reference_holds_in_connection(
            connection,
            first.reference,
            expected_physical_cluster_uid='physical-cluster-uid')


def test_concurrent_launches_refresh_before_delayed_terminal_guard(
        proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    calls = 0
    calls_lock = threading.Lock()

    def prove():
        nonlocal calls
        with calls_lock:
            calls += 1
        return _proof_candidate()

    common = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
        'prove': prove,
        'validate': _accept_payload,
    }
    seeded = repository.get_or_prove(**common,
                                     deadline_monotonic=time.monotonic() + 5)
    old_handoff_remaining = 1.0
    assert old_handoff_remaining < (
        reclaim.LAUNCH_AUTHORIZATION_MIN_REMAINING_SECONDS)
    with proof_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE serve_reserved_fill_reclaim_provider_proofs
                SET completed_at = clock_timestamp()
                    - make_interval(secs => :age)
            """), {
                'age': (reclaim.AUTHORIZATION_MAX_AGE_SECONDS -
                        old_handoff_remaining)
            })

    launch_count = 10
    handoff_barrier = threading.Barrier(launch_count)
    terminal_entry_delay = old_handoff_remaining + 0.1

    def launch():
        receipt = repository.get_or_prove(**common,
                                          deadline_monotonic=time.monotonic() +
                                          5)
        handoff_barrier.wait(timeout=5)
        time.sleep(terminal_entry_delay)
        with proof_engine.begin() as connection:
            holds = proofs.provider_proof_reference_holds_in_connection(
                connection,
                receipt.reference,
                expected_physical_cluster_uid='physical-cluster-uid')
        return receipt.reference.receipt_nonce, holds

    with concurrent.futures.ThreadPoolExecutor(
            max_workers=launch_count) as executor:
        results = tuple(executor.map(lambda _: launch(), range(launch_count)))

    # The old 0.5-second handoff reserve reused the aged receipt and every
    # delayed terminal guard failed.  The shared minimum-remaining contract
    # elects one refresh before any launch receives its reference.
    assert calls == 2
    assert {nonce for nonce, _ in results} == {seeded.reference.receipt_nonce}
    assert all(holds for _, holds in results)


def test_slow_validation_cannot_consume_terminal_guard_reserve(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    common = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
    }
    repository.get_or_prove(**common,
                            deadline_monotonic=time.monotonic() + 5,
                            prove=_proof_candidate,
                            validate=_accept_payload)
    age_before_validation = (
        reclaim.AUTHORIZATION_MAX_AGE_SECONDS -
        reclaim.LAUNCH_AUTHORIZATION_MIN_REMAINING_SECONDS - 0.2)
    with proof_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE serve_reserved_fill_reclaim_provider_proofs
                SET completed_at = clock_timestamp()
                    - make_interval(secs => :age)
            """), {'age': age_before_validation})
    validation_calls = 0
    prove_calls = 0

    def slow_validate(_payload):
        nonlocal validation_calls
        validation_calls += 1
        time.sleep(0.3)
        return True

    def prove():
        nonlocal prove_calls
        prove_calls += 1
        return _proof_candidate()

    refreshed = repository.get_or_prove(**common,
                                        deadline_monotonic=time.monotonic() + 5,
                                        prove=prove,
                                        validate=slow_validate)

    assert validation_calls >= 2
    assert prove_calls == 1
    assert refreshed.has_terminal_guard_reserve


def test_connection_close_cannot_consume_terminal_guard_reserve(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    common = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
    }
    repository.get_or_prove(**common,
                            deadline_monotonic=time.monotonic() + 5,
                            prove=_proof_candidate,
                            validate=_accept_payload)
    age_before_close = (reclaim.AUTHORIZATION_MAX_AGE_SECONDS -
                        reclaim.LAUNCH_AUTHORIZATION_MIN_REMAINING_SECONDS -
                        0.4)
    with proof_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE serve_reserved_fill_reclaim_provider_proofs
                SET completed_at = clock_timestamp()
                    - make_interval(secs => :age)
            """), {'age': age_before_close})
    prove_calls = 0
    proof_calls_at_close = []

    def prove():
        nonlocal prove_calls
        prove_calls += 1
        return _proof_candidate()

    def delay_first_physical_close(_dbapi_connection, _connection_record):
        proof_calls_at_close.append(prove_calls)
        if len(proof_calls_at_close) == 1:
            time.sleep(0.55)

    sqlalchemy.event.listen(repository._proof_engine, 'close',
                            delay_first_physical_close)
    try:
        refreshed = repository.get_or_prove(**common,
                                            deadline_monotonic=time.monotonic()
                                            + 5,
                                            prove=prove,
                                            validate=_accept_payload)
    finally:
        sqlalchemy.event.remove(repository._proof_engine, 'close',
                                delay_first_physical_close)

    assert proof_calls_at_close[0] == 0
    assert prove_calls == 1
    assert refreshed.has_terminal_guard_reserve


def test_changed_proof_refresh_rotates_nonce_and_revokes_old_reference(
        proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    old_payload = {
        **_proof_payload(),
        'inventory_revision': 'old',
    }
    first = repository.get_or_prove(identity=_identity(),
                                    gate_generation=_GATE_GENERATION,
                                    kubernetes_context=_CONTEXT,
                                    deadline_monotonic=time.monotonic() + 5,
                                    prove=lambda: _proof_candidate(old_payload),
                                    validate=_accept_payload)
    new_payload = {
        **_proof_payload(),
        'inventory_revision': 'new',
    }

    refreshed = repository.get_or_prove(
        identity=_identity(),
        gate_generation=_GATE_GENERATION,
        kubernetes_context=_CONTEXT,
        deadline_monotonic=time.monotonic() + 5,
        prove=lambda: _proof_candidate(new_payload),
        validate=lambda payload: payload.get('inventory_revision') == 'new')

    assert refreshed.reference.receipt_nonce != first.reference.receipt_nonce
    with proof_engine.begin() as connection:
        assert not proofs.provider_proof_reference_holds_in_connection(
            connection,
            first.reference,
            expected_physical_cluster_uid='physical-cluster-uid')
        assert proofs.provider_proof_reference_holds_in_connection(
            connection,
            refreshed.reference,
            expected_physical_cluster_uid='physical-cluster-uid')


def test_staggered_launches_span_two_identical_renewals(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    proof_calls = 0
    proof_calls_lock = threading.Lock()
    worker_count = 90
    wave_horizon_seconds = 10.0
    wave_started = time.monotonic() + 0.1

    def prove():
        nonlocal proof_calls
        with proof_calls_lock:
            proof_calls += 1
        time.sleep(0.03)
        return _proof_candidate()

    def launch(index):
        scheduled = (wave_started + wave_horizon_seconds * index /
                     (worker_count - 1))
        remaining = scheduled - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        receipt = repository.get_or_prove(identity=_identity(),
                                          gate_generation=_GATE_GENERATION,
                                          kubernetes_context=_CONTEXT,
                                          deadline_monotonic=time.monotonic() +
                                          5,
                                          prove=prove,
                                          validate=_accept_payload)
        with proof_engine.begin() as connection:
            holds = proofs.provider_proof_reference_holds_in_connection(
                connection,
                receipt.reference,
                expected_physical_cluster_uid='physical-cluster-uid')
        return receipt.reference.receipt_nonce, holds

    with concurrent.futures.ThreadPoolExecutor(
            max_workers=worker_count) as executor:
        results = tuple(executor.map(launch, range(worker_count)))

    assert proof_calls >= 3
    assert len({nonce for nonce, _ in results}) == 1
    assert all(holds for _, holds in results)


def test_distinct_context_authorities_prove_in_parallel(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    provider_barrier = threading.Barrier(2)

    def prove():
        provider_barrier.wait(timeout=2)
        return _proof_candidate()

    def authorize(context_name):
        return repository.get_or_prove(identity=_identity(),
                                       gate_generation=_GATE_GENERATION,
                                       kubernetes_context=context_name,
                                       deadline_monotonic=time.monotonic() + 5,
                                       prove=prove,
                                       validate=_accept_payload)

    contexts = ('context-a', 'context-b')
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        receipts = tuple(executor.map(authorize, contexts))

    assert tuple(receipt.reference.kubernetes_context
                 for receipt in receipts) == contexts


def test_leader_transaction_uses_live_idle_timeout_and_holds_xact_lock(
        proof_engine, monkeypatch):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    original_read = repository._read
    provider_entered = threading.Event()
    release_provider = threading.Event()
    leader = {}
    transaction_read_count = 0

    def observed_read(**kwargs):
        nonlocal transaction_read_count
        result = original_read(**kwargs)
        connection = kwargs.get('connection')
        if connection is not None:
            transaction_read_count += 1
            if transaction_read_count == 2:
                leader['idle_timeout'] = connection.exec_driver_sql(
                    'SHOW idle_in_transaction_session_timeout').scalar_one()
                leader['pid'] = connection.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.pg_backend_pid())).scalar_one()
        return result

    def prove():
        provider_entered.set()
        assert release_provider.wait(timeout=2)
        return _proof_candidate()

    monkeypatch.setattr(repository, '_read', observed_read)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(repository.get_or_prove,
                                 identity=_identity(),
                                 gate_generation=_GATE_GENERATION,
                                 kubernetes_context=_CONTEXT,
                                 deadline_monotonic=time.monotonic() + 5,
                                 prove=prove,
                                 validate=_accept_payload)
        assert provider_entered.wait(timeout=2)
        application_name = None
        state = None
        advisory_locks = None
        monitor_deadline = time.monotonic() + 1
        while time.monotonic() < monitor_deadline:
            with proof_engine.connect() as connection:
                row = connection.execute(
                    sqlalchemy.text("""
                        SELECT activity.application_name,
                               activity.state,
                               count(*) FILTER (
                                   WHERE locks.locktype = 'advisory'
                                     AND locks.granted)
                        FROM pg_stat_activity AS activity
                        LEFT JOIN pg_locks AS locks
                          ON locks.pid = activity.pid
                        WHERE activity.pid = :pid
                        GROUP BY activity.application_name, activity.state
                    """), {
                        'pid': leader['pid']
                    }).one_or_none()
            if row is not None:
                application_name, state, advisory_locks = row
                if (application_name == 'skypilot-reclaim-proof-owner' and
                        state == 'idle in transaction' and advisory_locks == 1):
                    break
            time.sleep(0.01)
        assert leader['idle_timeout'] == '6s'
        assert application_name == 'skypilot-reclaim-proof-owner'
        assert state == 'idle in transaction'
        assert advisory_locks == 1
        release_provider.set()
        receipt = future.result(timeout=2)

    assert transaction_read_count == 2
    assert len(receipt.reference.receipt_nonce) == 64
    assert _proof_session_count(proof_engine) == 0


def test_waiter_timeout_does_not_cancel_leader(proof_engine):
    leader_repository = proofs.ReclaimProviderProofRepository(proof_engine)
    waiter_repository = proofs.ReclaimProviderProofRepository(proof_engine)
    leader_entered = threading.Event()
    release_leader = threading.Event()
    provider_calls = []

    def leader_proof():
        provider_calls.append('leader')
        leader_entered.set()
        assert release_leader.wait(timeout=2)
        return _proof_candidate()

    def waiter_proof():
        provider_calls.append('waiter')
        return _proof_candidate()

    common = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
    }
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        leader = executor.submit(leader_repository.get_or_prove,
                                 **common,
                                 deadline_monotonic=time.monotonic() + 5,
                                 prove=leader_proof,
                                 validate=_accept_payload)
        assert leader_entered.wait(timeout=2)
        waiter = executor.submit(waiter_repository.get_or_prove,
                                 **common,
                                 deadline_monotonic=time.monotonic() + 1.8,
                                 prove=waiter_proof,
                                 validate=_accept_payload)
        with pytest.raises(proofs.ReclaimProviderProofError,
                           match='was not published before its deadline'):
            waiter.result(timeout=2.5)
        release_leader.set()
        receipt = leader.result(timeout=2)

    assert provider_calls == ['leader']
    assert len(receipt.reference.receipt_nonce) == 64


def test_malformed_receipt_is_reproved_under_exact_lock(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    calls = 0

    def prove():
        nonlocal calls
        calls += 1
        return _proof_candidate()

    kwargs = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
        'prove': prove,
        'validate': _accept_payload,
    }
    first = repository.get_or_prove(**kwargs,
                                    deadline_monotonic=time.monotonic() + 5)
    with proof_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE serve_reserved_fill_reclaim_provider_proofs
                SET proof_sha256 = :wrong_digest
            """), {'wrong_digest': 'f' * 64})

    refreshed = repository.get_or_prove(**kwargs,
                                        deadline_monotonic=time.monotonic() + 5)

    assert calls == 2
    assert refreshed.reference.receipt_nonce != first.reference.receipt_nonce
    assert refreshed.reference.proof_sha256 != 'f' * 64


@pytest.mark.parametrize('bad_domain', ('aws', 'kubernetes'))
def test_semantically_wrong_cached_summary_is_reproved_by_policy(
        proof_engine, monkeypatch, bad_domain):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    bad_payload = {
        domain: dataclasses.asdict(_provider_proof(policy, domain))
        for domain in ('aws', 'kubernetes')
    }
    if bad_domain == 'aws':
        bad_payload['aws'][
            'cluster_arn'] = 'arn:aws:eks:us-west-2:1:cluster/wrong'
    else:
        bad_payload['kubernetes'][
            'physical_cluster_uid'] = 'wrong-physical-cluster'
    seeded = repository.get_or_prove(
        identity=policy.policy_identity(),
        gate_generation=_GATE_GENERATION,
        kubernetes_context=_CONTEXT,
        deadline_monotonic=time.monotonic() + 5,
        prove=lambda: _proof_candidate(bad_payload),
        validate=_accept_payload)
    provider_calls = []

    def provider_job(context_name, domain, _deadline, _cancellation):
        assert context_name == _CONTEXT
        provider_calls.append(domain)
        return _provider_proof(policy, domain)

    monkeypatch.setattr(policy, '_provider_job', provider_job)
    monkeypatch.setattr(policy, '_emit_proof', lambda _payload: None)
    with mock.patch.object(policy_lib.reserved_fill_reclaim_proofs,
                           'ReclaimProviderProofRepository',
                           return_value=repository):
        authorization = policy.authorize_launch(
            _launch_scope(policy),
            expected_identity=policy.policy_identity(),
            expected_gate_generation=_GATE_GENERATION,
            deadline_monotonic=time.monotonic() + 5)

    assert sorted(provider_calls) == ['aws', 'kubernetes']
    assert authorization.provider_proof_reference.receipt_nonce != (
        seeded.reference.receipt_nonce)


def test_cached_receipt_cannot_cross_caller_deadline_during_validation(
        proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    kwargs = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
        'prove': lambda: _proof_candidate(),
    }
    repository.get_or_prove(**kwargs,
                            deadline_monotonic=time.monotonic() + 5,
                            validate=_accept_payload)

    def slow_validation(_payload):
        time.sleep(0.08)
        return True

    with pytest.raises(proofs.ReclaimProviderProofError,
                       match='deadline is invalid or expired'):
        repository.get_or_prove(**kwargs,
                                deadline_monotonic=time.monotonic() + 0.05,
                                validate=slow_validation)


def test_future_receipt_clock_fails_before_provider_io(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    calls = 0

    def prove():
        nonlocal calls
        calls += 1
        return _proof_candidate()

    kwargs = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
        'prove': prove,
        'validate': _accept_payload,
    }
    first = repository.get_or_prove(**kwargs,
                                    deadline_monotonic=time.monotonic() + 5)
    with proof_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE serve_reserved_fill_reclaim_provider_proofs
                SET completed_at = CURRENT_TIMESTAMP + INTERVAL '30 seconds'
            """))

    with pytest.raises(proofs.ReclaimProviderProofError,
                       match='database clock is indeterminate'):
        repository.get_or_prove(**kwargs,
                                deadline_monotonic=time.monotonic() + 5)
    with proof_engine.connect() as connection:
        nonce, digest = connection.execute(
            sqlalchemy.select(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table.
                c.receipt_nonce,
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table.
                c.proof_sha256)).one()

    assert calls == 1
    assert nonce == first.reference.receipt_nonce
    assert digest == first.reference.proof_sha256


def test_uncertain_returning_row_rolls_back_publication(proof_engine,
                                                        monkeypatch):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    original_decode = repository._decode_query_row

    def reject_inserted_row(row, **kwargs):
        if row is not None and row.get('receipt_nonce') is not None:
            raise proofs.ReclaimProviderProofError(
                'forced post-insert clock uncertainty')
        return original_decode(row, **kwargs)

    monkeypatch.setattr(repository, '_decode_query_row', reject_inserted_row)
    with pytest.raises(proofs.ReclaimProviderProofError,
                       match='forced post-insert clock uncertainty'):
        repository.get_or_prove(identity=_identity(),
                                gate_generation=_GATE_GENERATION,
                                kubernetes_context=_CONTEXT,
                                deadline_monotonic=time.monotonic() + 5,
                                prove=lambda: _proof_candidate(),
                                validate=_accept_payload)

    with proof_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        ).scalar_one() == 0


def test_failed_provider_proof_is_not_persisted(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)

    def fail():
        raise RuntimeError('provider failed')

    with pytest.raises(RuntimeError, match='provider failed'):
        repository.get_or_prove(identity=_identity(),
                                gate_generation=_GATE_GENERATION,
                                kubernetes_context=_CONTEXT,
                                deadline_monotonic=time.monotonic() + 5,
                                prove=fail,
                                validate=_accept_payload)
    with proof_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        ).scalar_one() == 0


def test_lost_advisory_session_cannot_publish(proof_engine, monkeypatch):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    original_publish = repository._publish

    def lose_session(**kwargs):
        transaction_connection = kwargs['connection']
        pid = transaction_connection.execute(
            sqlalchemy.select(sqlalchemy.func.pg_backend_pid())).scalar_one()
        with proof_engine.connect() as connection:
            assert connection.execute(
                sqlalchemy.text('SELECT pg_terminate_backend(:pid)'), {
                    'pid': pid
                }).scalar_one()
            connection.commit()
        return original_publish(**kwargs)

    monkeypatch.setattr(repository, '_publish', lose_session)
    with pytest.raises(Exception):
        repository.get_or_prove(identity=_identity(),
                                gate_generation=_GATE_GENERATION,
                                kubernetes_context=_CONTEXT,
                                deadline_monotonic=time.monotonic() + 5,
                                prove=lambda: _proof_candidate(),
                                validate=_accept_payload)
    with proof_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        ).scalar_one() == 0


def test_lost_leader_fails_wave_and_later_call_recovers(proof_engine,
                                                        monkeypatch):
    leader_repository = proofs.ReclaimProviderProofRepository(proof_engine)
    waiter_repositories = tuple(
        proofs.ReclaimProviderProofRepository(proof_engine) for _ in range(4))
    waiter_polling = tuple(threading.Event() for _ in waiter_repositories)
    waiter_elections = tuple(
        mock.Mock(wraps=repository._authority_lock_id)
        for repository in waiter_repositories)
    leader_entered = threading.Event()
    release_leader = threading.Event()
    provider_calls = []
    original_publish = leader_repository._publish

    def lose_session(**kwargs):
        transaction_connection = kwargs['connection']
        pid = transaction_connection.execute(
            sqlalchemy.select(sqlalchemy.func.pg_backend_pid())).scalar_one()
        with proof_engine.connect() as connection:
            assert connection.execute(
                sqlalchemy.text('SELECT pg_terminate_backend(:pid)'), {
                    'pid': pid
                }).scalar_one()
            connection.commit()
        return original_publish(**kwargs)

    monkeypatch.setattr(leader_repository, '_publish', lose_session)
    for repository, entered, election in zip(waiter_repositories,
                                             waiter_polling, waiter_elections):
        original_wait = repository._wait_for_published_receipt

        def observed_wait(*, _entered=entered, _wait=original_wait, **kwargs):
            _entered.set()
            return _wait(**kwargs)

        monkeypatch.setattr(repository, '_wait_for_published_receipt',
                            observed_wait)
        monkeypatch.setattr(repository, '_authority_lock_id', election)

    def leader_proof():
        provider_calls.append('lost-leader')
        leader_entered.set()
        assert release_leader.wait(timeout=2)
        return _proof_candidate()

    def waiter_proof():
        provider_calls.append('unexpected-waiter')
        return _proof_candidate()

    common = {
        'identity': _identity(),
        'gate_generation': _GATE_GENERATION,
        'kubernetes_context': _CONTEXT,
        'validate': _accept_payload,
    }
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        leader = executor.submit(leader_repository.get_or_prove,
                                 **common,
                                 deadline_monotonic=time.monotonic() + 5,
                                 prove=leader_proof)
        assert leader_entered.wait(timeout=2)
        waiter_deadline = time.monotonic() + 1.8
        waiters = tuple(
            executor.submit(repository.get_or_prove,
                            **common,
                            deadline_monotonic=waiter_deadline,
                            prove=waiter_proof)
            for repository in waiter_repositories)
        assert all(entered.wait(timeout=2) for entered in waiter_polling)
        release_leader.set()
        with pytest.raises(Exception):
            leader.result(timeout=2)
        for waiter in waiters:
            with pytest.raises(proofs.ReclaimProviderProofError,
                               match='deadline'):
                waiter.result(timeout=2)

    with proof_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        ).scalar_one() == 0

    recovery_calls = 0

    def recovery_proof():
        nonlocal recovery_calls
        recovery_calls += 1
        return _proof_candidate()

    recovered = proofs.ReclaimProviderProofRepository(
        proof_engine).get_or_prove(**common,
                                   deadline_monotonic=time.monotonic() + 5,
                                   prove=recovery_proof)
    assert provider_calls == ['lost-leader']
    assert all(election.call_count == 1 for election in waiter_elections)
    assert recovery_calls == 1
    assert len(recovered.reference.receipt_nonce) == 64


def test_final_guard_rejects_stale_or_mismatched_receipt(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    receipt = repository.get_or_prove(identity=_identity(),
                                      gate_generation=_GATE_GENERATION,
                                      kubernetes_context=_CONTEXT,
                                      deadline_monotonic=time.monotonic() + 5,
                                      prove=_proof_candidate,
                                      validate=_accept_payload)
    reference = receipt.reference
    with proof_engine.begin() as connection:
        assert proofs.provider_proof_reference_holds_in_connection(
            connection,
            reference,
            expected_physical_cluster_uid='physical-cluster-uid')
        wrong_nonce = dataclasses.replace(reference, receipt_nonce='f' * 64)
        wrong_digest = dataclasses.replace(reference, proof_sha256='f' * 64)
        assert not proofs.provider_proof_reference_holds_in_connection(
            connection,
            wrong_nonce,
            expected_physical_cluster_uid='physical-cluster-uid')
        assert not proofs.provider_proof_reference_holds_in_connection(
            connection,
            wrong_digest,
            expected_physical_cluster_uid='physical-cluster-uid')
        locally_stale = dataclasses.replace(
            reference,
            completed_monotonic=(reference.completed_monotonic -
                                 reclaim.AUTHORIZATION_MAX_AGE_SECONDS - 1))
        assert not proofs.provider_proof_reference_holds_in_connection(
            connection,
            locally_stale,
            expected_physical_cluster_uid='physical-cluster-uid')
        assert not proofs.provider_proof_reference_holds_in_connection(
            connection,
            reference,
            expected_physical_cluster_uid='wrong-physical-cluster')
        wrong_identity = reclaim.ReclaimPolicyIdentity(
            fleet_bundle_sha256='c' * 64,
            policy_revision=reference.identity.policy_revision,
            provider_inventory_sha256=(
                reference.identity.provider_inventory_sha256))

        wrong_authorities = (
            dataclasses.replace(reference, gate_generation=18),
            dataclasses.replace(reference, identity=wrong_identity),
            dataclasses.replace(reference, kubernetes_context='wrong-context'),
        )
        for wrong_authority in wrong_authorities:
            assert not proofs.provider_proof_reference_holds_in_connection(
                connection,
                wrong_authority,
                expected_physical_cluster_uid='physical-cluster-uid')
    with proof_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE serve_reserved_fill_reclaim_provider_proofs
                SET completed_at = CURRENT_TIMESTAMP - INTERVAL '10 seconds'
            """))
    with proof_engine.begin() as connection:
        assert not proofs.provider_proof_reference_holds_in_connection(
            connection,
            reference,
            expected_physical_cluster_uid='physical-cluster-uid')


def test_final_guard_requires_read_committed_isolation(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    receipt = repository.get_or_prove(identity=_identity(),
                                      gate_generation=_GATE_GENERATION,
                                      kubernetes_context=_CONTEXT,
                                      deadline_monotonic=time.monotonic() + 5,
                                      prove=_proof_candidate,
                                      validate=_accept_payload)
    with proof_engine.connect().execution_options(
            isolation_level='REPEATABLE READ') as connection:
        with connection.begin():
            assert not proofs.provider_proof_reference_holds_in_connection(
                connection,
                receipt.reference,
                expected_physical_cluster_uid='physical-cluster-uid')


@pytest.mark.parametrize(('column', 'value'), (
    ('reconciliation_gate_generation', _GATE_GENERATION + 1),
    ('reclaim_fleet_bundle_sha256', 'c' * 64),
    ('reclaim_policy_revision', 'wrong-policy'),
    ('reclaim_provider_inventory_sha256', 'd' * 64),
    ('kubernetes_context', 'wrong-context'),
    ('proof_schema_version', 2),
))
def test_final_guard_rejects_malformed_authority_row(proof_engine, column,
                                                     value):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    receipt = repository.get_or_prove(identity=_identity(),
                                      gate_generation=_GATE_GENERATION,
                                      kubernetes_context=_CONTEXT,
                                      deadline_monotonic=time.monotonic() + 5,
                                      prove=_proof_candidate,
                                      validate=_accept_payload)
    reference = receipt.reference
    table = proof_schema.serve_reserved_fill_reclaim_provider_proofs_table
    with proof_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(table).where(
                table.c.receipt_nonce == reference.receipt_nonce).values(
                    **{column: value}))
    with proof_engine.begin() as connection:
        assert not proofs.provider_proof_reference_holds_in_connection(
            connection,
            reference,
            expected_physical_cluster_uid='physical-cluster-uid')


def test_final_guard_rejects_missing_row(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    receipt = repository.get_or_prove(identity=_identity(),
                                      gate_generation=_GATE_GENERATION,
                                      kubernetes_context=_CONTEXT,
                                      deadline_monotonic=time.monotonic() + 5,
                                      prove=_proof_candidate,
                                      validate=_accept_payload)
    reference = receipt.reference
    table = proof_schema.serve_reserved_fill_reclaim_provider_proofs_table
    with proof_engine.begin() as connection:
        connection.execute(
            sqlalchemy.delete(table).where(
                table.c.receipt_nonce == reference.receipt_nonce))
    with proof_engine.begin() as connection:
        assert not proofs.provider_proof_reference_holds_in_connection(
            connection,
            reference,
            expected_physical_cluster_uid='physical-cluster-uid')


def test_final_guard_rejects_delete_reinsert_aba(proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    receipt = repository.get_or_prove(identity=_identity(),
                                      gate_generation=_GATE_GENERATION,
                                      kubernetes_context=_CONTEXT,
                                      deadline_monotonic=time.monotonic() + 5,
                                      prove=_proof_candidate,
                                      validate=_accept_payload)
    old_reference = receipt.reference
    table = proof_schema.serve_reserved_fill_reclaim_provider_proofs_table
    with proof_engine.begin() as connection:
        connection.execute(
            sqlalchemy.delete(table).where(
                table.c.receipt_nonce == old_reference.receipt_nonce))

    replacement = repository.get_or_prove(identity=_identity(),
                                          gate_generation=_GATE_GENERATION,
                                          kubernetes_context=_CONTEXT,
                                          deadline_monotonic=time.monotonic() +
                                          5,
                                          prove=lambda: _proof_candidate(),
                                          validate=_accept_payload)
    assert replacement.reference.proof_sha256 == (old_reference.proof_sha256)
    assert replacement.reference.receipt_nonce != (old_reference.receipt_nonce)
    with proof_engine.begin() as connection:
        assert not proofs.provider_proof_reference_holds_in_connection(
            connection,
            old_reference,
            expected_physical_cluster_uid='physical-cluster-uid')
        assert proofs.provider_proof_reference_holds_in_connection(
            connection,
            replacement.reference,
            expected_physical_cluster_uid='physical-cluster-uid')


def test_mvcc_guard_orders_before_changed_refresh_commit_without_blocking(
        proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    old_payload = {
        **_proof_payload(),
        'inventory_revision': 'old',
    }
    receipt = repository.get_or_prove(
        identity=_identity(),
        gate_generation=_GATE_GENERATION,
        kubernetes_context=_CONTEXT,
        deadline_monotonic=time.monotonic() + 5,
        prove=lambda: _proof_candidate(old_payload),
        validate=_accept_payload)
    old_reference = receipt.reference
    refresh_calls = 0

    def prove_refresh():
        nonlocal refresh_calls
        refresh_calls += 1
        return _proof_candidate({
            **_proof_payload(),
            'inventory_revision': 'new',
        })

    def accept_new(payload):
        return payload.get('inventory_revision') == 'new'

    guard_connection = proof_engine.connect()
    guard_transaction = guard_connection.begin()
    try:
        assert proofs.provider_proof_reference_holds_in_connection(
            guard_connection,
            old_reference,
            expected_physical_cluster_uid='physical-cluster-uid')
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            refresh = executor.submit(repository.get_or_prove,
                                      identity=_identity(),
                                      gate_generation=_GATE_GENERATION,
                                      kubernetes_context=_CONTEXT,
                                      deadline_monotonic=time.monotonic() + 5,
                                      prove=prove_refresh,
                                      validate=accept_new)
            replacement = refresh.result(timeout=2)
        assert refresh_calls == 1
        assert replacement.reference.receipt_nonce != (
            old_reference.receipt_nonce)
        assert not proofs.provider_proof_reference_holds_in_connection(
            guard_connection,
            old_reference,
            expected_physical_cluster_uid='physical-cluster-uid')
        assert proofs.provider_proof_reference_holds_in_connection(
            guard_connection,
            replacement.reference,
            expected_physical_cluster_uid='physical-cluster-uid')
    finally:
        guard_transaction.rollback()
        guard_connection.close()

    with proof_engine.begin() as connection:
        assert not proofs.provider_proof_reference_holds_in_connection(
            connection,
            old_reference,
            expected_physical_cluster_uid='physical-cluster-uid')
        assert proofs.provider_proof_reference_holds_in_connection(
            connection,
            replacement.reference,
            expected_physical_cluster_uid='physical-cluster-uid')


def test_uncommitted_identical_refresh_does_not_block_or_reject_final_guard(
        proof_engine):
    repository = proofs.ReclaimProviderProofRepository(proof_engine)
    receipt = repository.get_or_prove(identity=_identity(),
                                      gate_generation=_GATE_GENERATION,
                                      kubernetes_context=_CONTEXT,
                                      deadline_monotonic=time.monotonic() + 5,
                                      prove=_proof_candidate,
                                      validate=_accept_payload)
    table = proof_schema.serve_reserved_fill_reclaim_provider_proofs_table
    provider_effect = mock.Mock()
    refresh_connection = proof_engine.connect()
    refresh_transaction = refresh_connection.begin()
    try:
        refresh_connection.execute(
            sqlalchemy.update(table).where(
                table.c.receipt_nonce == receipt.reference.receipt_nonce).
            values(completed_at=table.c.completed_at))
        started = time.monotonic()
        with proof_engine.begin() as guard_connection:
            if proofs.provider_proof_reference_holds_in_connection(
                    guard_connection,
                    receipt.reference,
                    expected_physical_cluster_uid='physical-cluster-uid'):
                provider_effect()
        assert time.monotonic() - started < 1
        provider_effect.assert_called_once()
    finally:
        refresh_transaction.rollback()
        refresh_connection.close()

    with proof_engine.begin() as connection:
        assert proofs.provider_proof_reference_holds_in_connection(
            connection,
            receipt.reference,
            expected_physical_cluster_uid='physical-cluster-uid')
    provider_effect.assert_called_once()


def test_conservative_round_trip_mapping_never_understates_age():
    identity = _identity()
    now = datetime.datetime.now(datetime.timezone.utc)
    payload, digest = proofs.canonical_proof_payload(_proof_payload())
    row = {
        'receipt_nonce': 'e' * 64,
        'reconciliation_gate_generation': _GATE_GENERATION,
        'reclaim_fleet_bundle_sha256': identity.fleet_bundle_sha256,
        'reclaim_policy_revision': identity.policy_revision,
        'reclaim_provider_inventory_sha256':
            (identity.provider_inventory_sha256),
        'kubernetes_context': _CONTEXT,
        'proof_schema_version': proofs.PROVIDER_PROOF_SCHEMA_VERSION,
        'proof_payload': payload,
        'proof_sha256': digest,
        'completed_at': now - datetime.timedelta(seconds=1),
    }
    receipt = proofs._decode_receipt(row,
                                     expected_identity=identity,
                                     expected_gate_generation=_GATE_GENERATION,
                                     expected_kubernetes_context=_CONTEXT,
                                     database_now=now,
                                     local_read_started=100.0,
                                     local_read_finished=100.25)
    assert receipt.reference.completed_monotonic == pytest.approx(99.0)
