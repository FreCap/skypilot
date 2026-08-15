"""One-shot retained ReplicaInfo v18 normalization before Serve047."""

import argparse
import copy
import json
import math
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm

from sky.serve import constants
from sky.serve import replica_info
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.utils import locks
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

_CONTRACT = 'skyserve.replica-info-v18-normalization/v1'
_CONSTRAINT = 'ck_replicas_replica_info_version_18'
_CURRENT_VERSION = 18
_ATTRIBUTION_FIELDS = replica_info.V17_COLLISION_OPTIONAL_STORAGE_FIELDS
_LEGACY_VERSIONS = frozenset((3, 6, 7, 12, 13, 14))
_NORMALIZABLE_VERSIONS = _LEGACY_VERSIONS | {17, _CURRENT_VERSION}
_TRANSITION_RECORD_ID_NAMESPACE = uuid.UUID(
    '3b448973-9e2f-58aa-a640-27fb7c6a8884')
_LEGACY_TOP_LEVEL_DEFAULTS = {
    'planned_capacity': 1,
    'unknown_capacity_replacement': False,
    'logical_bridge_capacity_verified': False,
    'reserved_fill_pool_key': None,
    'reserved_fill_service_generation': None,
    'reserved_fill_physical_cluster_uid': None,
    'reserved_fill_kubernetes_context': None,
    'reserved_fill_allocation_generation': None,
    'reserved_fill_allocation_input_sha256': None,
    'reserved_fill_allocation_claim_generation': None,
    'reserved_fill_reconciliation_gate_generation': None,
    'reserved_fill_reclaim_fleet_bundle_sha256': None,
    'reserved_fill_reclaim_policy_revision': None,
    'reserved_fill_reclaim_provider_inventory_sha256': None,
    'reserved_fill_worker_projection_sha256': None,
    'reserved_fill_observation_generation': None,
    'reserved_fill_observation_sequence': None,
    'reserved_fill_intent_idempotency_key': None,
    'zero_cost_admission_sequence': None,
    'zero_cost_materialization_sequence': None,
    'is_zero_cost': False,
    'paid_capacity_pool_key': None,
}
_LEGACY_STATUS_DEFAULTS = {
    'drain_started_at': None,
    'logical_retirement_version': None,
    'logical_retirement_controller_epoch': None,
    'logical_retirement_generation': None,
    'logical_retirement_target_capacity': None,
    'logical_retirement_confirmed_generation': None,
    'logical_retirement_bounded_deadline': False,
    'logical_retirement_committed': None,
}
_LEGACY_SYSTEM_RECOVERY_DEFAULTS = {
    'system_recovery_launch_intent': None,
    'system_recovery_disposition': 'ORDINARY',
    'launch_request_id': None,
    'service_job_id': None,
    'candidate_ready_observed_at': None,
    'ordinary_release_not_before': None,
    'system_recovery_revision': 0,
    'system_recovery': None,
    'system_recovery_quarantine': None,
}
_CONSTRAINT_EXPRESSION = (
    "replica_state_version IS NOT NULL AND replica_state_version = 1 AND "
    "replica_state IS NOT NULL AND replica_state @> "
    "'{\"replica_info_version\": 18}'::jsonb AND replica_info IS NULL")
_POSTGRES_CONSTRAINT_EXPRESSION = (
    "((replica_state_version IS NOT NULL) AND "
    "(replica_state_version = 1) AND (replica_state IS NOT NULL) AND "
    "(replica_state @> '{\"replica_info_version\": 18}'::jsonb) AND "
    "(replica_info IS NULL))")
_REQUIRED_SERVE_DATABASE_REVISION = '046'
_SPLIT_WRITER_ROLES = ('api', 'controller', 'executor')


class ReplicaRecordNormalizationError(RuntimeError):
    """The retained replica inventory cannot cross the v18 boundary."""


def _exact_json_equal(left: Any, right: Any) -> bool:
    """Compare JSON values without Python's bool/int equality coercion."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        if set(left) != set(right):
            return False
        return all(_exact_json_equal(left[key], right[key]) for key in left)
    if isinstance(left, list):
        return (len(left) == len(right) and all(
            _exact_json_equal(left_item, right_item)
            for left_item, right_item in zip(left, right)))
    return bool(left == right)


def _record_key(row: sqlalchemy.engine.Row) -> tuple[str, int]:
    service_name = row.service_name
    replica_id = row.replica_id
    if (not isinstance(service_name, str) or not service_name or
            type(replica_id) is not int or replica_id < 0):
        raise ReplicaRecordNormalizationError(
            'A retained replica row has an invalid physical key.')
    return service_name, replica_id


def _expected_transition_record_id(state: dict[str, Any]) -> str:
    """Pin v1.1.1276's deterministic identity for pre-v13 rows."""
    replica_id = state['replica_id']
    cluster_name = state['cluster_name']
    created_at = state['created_at']
    if created_at is None:
        created_at_token = 'none'
    else:
        timestamp = float(created_at)
        if not math.isfinite(timestamp):
            raise ValueError('Invalid legacy creation timestamp.')
        created_at_token = timestamp.hex()
    identity_material = (f'{replica_id}:{len(cluster_name)}:{cluster_name}:'
                         f'{created_at_token}')
    return str(uuid.uuid5(_TRANSITION_RECORD_ID_NAMESPACE, identity_material))


def _expected_legacy_canonical_state(
        original: dict[str, Any]) -> dict[str, Any]:
    """Materialize only the bounded v1.1.1276-to-v18 deltas."""
    raw_version = original['replica_info_version']
    assert raw_version in _LEGACY_VERSIONS
    expected = copy.deepcopy(original)
    expected['replica_info_version'] = _CURRENT_VERSION
    for field, default in _LEGACY_TOP_LEVEL_DEFAULTS.items():
        expected.setdefault(field, copy.deepcopy(default))
    # Normalization is stricter than the bounded runtime reader: the reader
    # retains v1.1.1276's conversions, but this atomic rewrite refuses to
    # persist any present-field value or JSON-type change. The archived live
    # preflight proved recursive present-field parity for every retained row.
    if raw_version < 13:
        expected['replica_record_id'] = _expected_transition_record_id(expected)
        expected.update(_LEGACY_SYSTEM_RECOVERY_DEFAULTS)

    status = expected['status_property']
    assert isinstance(status, dict)
    for field, default in _LEGACY_STATUS_DEFAULTS.items():
        status.setdefault(field, copy.deepcopy(default))
    return expected


def _canonical_state(row: sqlalchemy.engine.Row,
                     row_ordinal: int) -> dict[str, Any]:
    _record_key(row)
    row_label = f'retained replica row {row_ordinal}'
    if row.replica_state_version != 1 or not isinstance(row.replica_state,
                                                        dict):
        raise ReplicaRecordNormalizationError(
            f'{row_label} has invalid JSON state.')
    original = copy.deepcopy(row.replica_state)
    raw_version = original.get('replica_info_version')
    if type(raw_version
           ) is not int or raw_version not in _NORMALIZABLE_VERSIONS:
        raise ReplicaRecordNormalizationError(
            f'{row_label} has an unsupported ReplicaInfo version.')
    try:
        info = replica_info.ReplicaInfo.from_storage_dict(original)
        canonical = info.to_storage_dict()
        repeated = replica_info.ReplicaInfo.from_storage_dict(
            copy.deepcopy(original)).to_storage_dict()
        verified = replica_info.ReplicaInfo.from_storage_dict(
            copy.deepcopy(canonical)).to_storage_dict()
    except Exception as error:  # pylint: disable=broad-except
        raise ReplicaRecordNormalizationError(
            f'{row_label} cannot be canonically normalized '
            f'({type(error).__name__}).') from None
    if (not _exact_json_equal(canonical, repeated) or
            not _exact_json_equal(canonical, verified) or
            canonical.get('replica_info_version') != _CURRENT_VERSION):
        raise ReplicaRecordNormalizationError(
            f'{row_label} did not reach stable v18 state.')
    try:
        if raw_version in _LEGACY_VERSIONS:
            expected = _expected_legacy_canonical_state(original)
            expected_delta = 'bounded legacy-to-v18 materialization'
        else:
            expected = copy.deepcopy(original)
            expected['replica_info_version'] = _CURRENT_VERSION
            for field in _ATTRIBUTION_FIELDS:
                if field not in expected:
                    expected[field] = None
            expected_delta = 'exact v17-to-v18 version/null expansion'
    except Exception as error:  # pylint: disable=broad-except
        raise ReplicaRecordNormalizationError(
            f'{row_label} cannot verify its canonical delta '
            f'({type(error).__name__}).') from None
    if not _exact_json_equal(canonical, expected):
        raise ReplicaRecordNormalizationError(
            f'{row_label} would change state outside the {expected_delta}.')
    if canonical.get('replica_id') != row.replica_id:
        raise ReplicaRecordNormalizationError(
            f'{row_label} disagrees with its physical key.')
    sky_down_status = info.status_property.sky_down_status
    derived_columns = {
        'replica_state_version': 1,
        'status': info.status.value,
        'sky_down_status':
            (sky_down_status.value if sky_down_status is not None else None),
        'version': info.version,
        'cluster_name': info.cluster_name,
        'created_at': info.created_at,
        'is_spot': info.is_spot,
        'paid_capacity_pool_key': info.paid_capacity_pool_key,
    }
    mismatches = [
        field for field, expected_value in derived_columns.items()
        if not _exact_json_equal(getattr(row, field), expected_value)
    ]
    if mismatches:
        raise ReplicaRecordNormalizationError(
            f'{row_label} has denormalized scalar columns: '
            f'{", ".join(mismatches)}.')
    return canonical


def _require_split_writer_rollout(rollout: Any) -> Any:
    """Reject the temporary one-pod/all topology at the v18 boundary."""
    deployments = getattr(rollout, 'deployments', None)
    writer_instances = getattr(rollout, 'writer_instances', None)
    if not isinstance(deployments, tuple) or not isinstance(
            writer_instances, tuple):
        raise ReplicaRecordNormalizationError(
            'ReplicaInfo v18 normalization requires an exact split '
            'API/controller/executor writer rollout.')
    deployment_roles = tuple(
        getattr(deployment, 'role', None) for deployment in deployments)
    writer_role_counts = {
        role: sum(
            getattr(instance, 'role', None) == role
            for instance in writer_instances) for role in _SPLIT_WRITER_ROLES
    }
    if (len(deployment_roles) != len(_SPLIT_WRITER_ROLES) or
            set(deployment_roles) != set(_SPLIT_WRITER_ROLES) or any(
                len(getattr(deployment, 'pod_cohort', ())) != 2
                for deployment in deployments) or len(writer_instances) != 6 or
            any(count != 2 for count in writer_role_counts.values())):
        raise ReplicaRecordNormalizationError(
            'ReplicaInfo v18 normalization requires an exact split '
            'API/controller/executor writer rollout; the one-pod/all '
            'topology is not eligible.')
    return rollout


def _constraint_state(
        session: orm.Session) -> tuple[bool, bool, str | None, str | None]:
    row = session.execute(
        sqlalchemy.text(
            'SELECT convalidated, pg_get_constraintdef(oid) AS definition, '
            'pg_get_expr(conbin, conrelid) AS expression '
            'FROM pg_constraint WHERE conrelid = '
            "'replicas'::regclass AND conname = :name AND contype = 'c'"), {
                'name': _CONSTRAINT,
            }).mappings().one_or_none()
    if row is None:
        return False, False, None, None
    return (True, row['convalidated']
            is True, str(row['definition']), str(row['expression']))


def _require_exact_constraint(session: orm.Session, *, validated: bool) -> None:
    exists, observed_validated, definition, expression = _constraint_state(
        session)
    if not exists or definition is None or expression is None:
        raise ReplicaRecordNormalizationError(
            f'{_CONSTRAINT} was not installed.')
    if observed_validated is not validated:
        expected = 'validated' if validated else 'not validated'
        raise ReplicaRecordNormalizationError(
            f'{_CONSTRAINT} is not {expected}.')
    if ' '.join(expression.split()) != _POSTGRES_CONSTRAINT_EXPRESSION:
        raise ReplicaRecordNormalizationError(
            f'{_CONSTRAINT} has an unexpected definition: {definition}')


def _remaining_noncurrent_records(session: orm.Session) -> int:
    remaining = session.execute(
        sqlalchemy.text("SELECT count(*) FROM replicas WHERE "
                        "replica_state_version IS DISTINCT FROM 1 OR "
                        "replica_state IS NULL OR "
                        "NOT (replica_state @> "
                        "'{\"replica_info_version\": 18}'::jsonb)"))
    return int(remaining.scalar_one())


def _remaining_pickle_records(session: orm.Session) -> int:
    remaining = session.execute(
        sqlalchemy.text(
            'SELECT count(*) FROM replicas WHERE replica_info IS NOT NULL'))
    return int(remaining.scalar_one())


def _normalize_retained_replica_records() -> dict[str, Any]:
    """Rewrite every retained row once all writers run the v18 image."""
    lock = locks.get_lock(constants.RESERVED_FILL_BROKER_LOCK_ID)
    with lock.acquire(blocking=True):
        # This private proof is deliberately not exposed as an operator input.
        # The token-bound API pod must belong to a stable API/controller/
        # executor rollout whose live writer leases all use one image digest.
        rollout = _require_split_writer_rollout(reserved_capacity_broker  # pylint: disable=protected-access
                                                ._read_stable_writer_rollout())
        engine = serve_state.get_database_engine()
        if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise ReplicaRecordNormalizationError(
                'ReplicaInfo v18 normalization requires PostgreSQL.')
        database_revision = migration_utils.get_current_alembic_revision(
            engine, migration_utils.SERVE_DB_NAME)
        if database_revision != _REQUIRED_SERVE_DATABASE_REVISION:
            raise ReplicaRecordNormalizationError(
                'ReplicaInfo v18 normalization requires exact Serve database '
                f'revision {_REQUIRED_SERVE_DATABASE_REVISION}.')
        with orm.Session(engine) as session:
            session.execute(
                sqlalchemy.text('LOCK TABLE replicas IN ACCESS EXCLUSIVE MODE'))
            rows = session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.service_name,
                    serve_state.replicas_table.c.replica_id,
                    serve_state.replicas_table.c.cluster_name,
                    serve_state.replicas_table.c.replica_info,
                    serve_state.replicas_table.c.replica_state_version,
                    serve_state.replicas_table.c.status,
                    serve_state.replicas_table.c.sky_down_status,
                    serve_state.replicas_table.c.version,
                    serve_state.replicas_table.c.created_at,
                    serve_state.replicas_table.c.is_spot,
                    serve_state.replicas_table.c.paid_capacity_pool_key,
                    serve_state.replicas_table.c.replica_state,
                ).order_by(serve_state.replicas_table.c.service_name,
                           serve_state.replicas_table.c.replica_id)).fetchall()
            canonical_records = []
            services = set()
            for row_ordinal, row in enumerate(rows, start=1):
                key = _record_key(row)
                services.add(key[0])
                canonical = _canonical_state(row, row_ordinal)
                canonical_records.append((row, key, canonical))

            exists, validated, _, _ = _constraint_state(session)
            if exists:
                _require_exact_constraint(session, validated=validated)
            else:
                # NOT VALID avoids reading legacy rows during DDL. PostgreSQL
                # nevertheless enforces the check for every subsequent write,
                # so the old writer is fenced before normalization starts.
                session.execute(
                    sqlalchemy.text(
                        f'ALTER TABLE replicas ADD CONSTRAINT {_CONSTRAINT} '
                        f'CHECK ({_CONSTRAINT_EXPRESSION}) NOT VALID'))
                _require_exact_constraint(session, validated=False)

            rewritten = 0
            already_current = 0
            for row, key, canonical in canonical_records:
                if (_exact_json_equal(row.replica_state, canonical) and
                        row.replica_info is None):
                    already_current += 1
                    continue
                result = session.execute(
                    sqlalchemy.update(serve_state.replicas_table).where(
                        serve_state.replicas_table.c.service_name == key[0],
                        serve_state.replicas_table.c.replica_id ==
                        key[1]).values(replica_state=canonical,
                                       replica_info=None))
                if result.rowcount != 1:
                    raise ReplicaRecordNormalizationError(
                        'Retained replica row changed during normalization.')
                rewritten += 1

            remaining_noncurrent = _remaining_noncurrent_records(session)
            if remaining_noncurrent != 0:
                raise ReplicaRecordNormalizationError(
                    f'{remaining_noncurrent} retained replicas remain outside '
                    'the exact v18 contract.')
            remaining_pickle = _remaining_pickle_records(session)
            if remaining_pickle != 0:
                raise ReplicaRecordNormalizationError(
                    f'{remaining_pickle} retained legacy pickle records remain.'
                )
            # A second token-bound proof while the table remains write-locked
            # rejects a rollout change during the transaction. The check
            # constraint permanently rejects a future v17 writer after commit.
            final_rollout = _require_split_writer_rollout(
                reserved_capacity_broker  # pylint: disable=protected-access
                ._read_stable_writer_rollout())
            if final_rollout != rollout:
                raise ReplicaRecordNormalizationError(
                    'The writer rollout changed during v18 normalization.')
            session.commit()

        # Updating replicas can leave deferred foreign-key trigger events, so
        # validation is a second transaction. The first commit is still the
        # atomic expand-and-normalize boundary: it installs an enforced check
        # and rewrites every retained row while holding ACCESS EXCLUSIVE. A
        # crash here is safely resumable; no v17 writer can cross the check.
        with orm.Session(engine) as session:
            session.execute(
                sqlalchemy.text('LOCK TABLE replicas IN ACCESS EXCLUSIVE MODE'))
            exists, validated, _, _ = _constraint_state(session)
            if not exists:
                raise ReplicaRecordNormalizationError(
                    f'{_CONSTRAINT} disappeared after normalization.')
            _require_exact_constraint(session, validated=validated)
            if not validated:
                session.execute(
                    sqlalchemy.text(f'ALTER TABLE replicas VALIDATE CONSTRAINT '
                                    f'{_CONSTRAINT}'))
            _require_exact_constraint(session, validated=True)
            remaining_noncurrent = _remaining_noncurrent_records(session)
            if remaining_noncurrent != 0:
                raise ReplicaRecordNormalizationError(
                    f'{remaining_noncurrent} retained replicas remain outside '
                    'the exact v18 contract after validation.')
            remaining_pickle = _remaining_pickle_records(session)
            if remaining_pickle != 0:
                raise ReplicaRecordNormalizationError(
                    f'{remaining_pickle} retained legacy pickle records remain '
                    'after validation.')
            final_rollout = _require_split_writer_rollout(
                reserved_capacity_broker  # pylint: disable=protected-access
                ._read_stable_writer_rollout())
            if final_rollout != rollout:
                raise ReplicaRecordNormalizationError(
                    'The writer rollout changed before v18 validation.')
            session.commit()

        return {
            'already_current_records': already_current,
            'constraint': _CONSTRAINT,
            'contract': _CONTRACT,
            'invalid_records': 0,
            'remaining_legacy_pickle_records': remaining_pickle,
            'remaining_noncurrent_records': remaining_noncurrent,
            'rewritten_records': rewritten,
            'scanned_records': len(rows),
            'scanned_services': len(services),
            'schema_version': _CURRENT_VERSION,
            'serve_database_revision': database_revision,
            'writer_deployment_roles': list(_SPLIT_WRITER_ROLES),
            'writer_image_digest': rollout.image_digest,
            'writer_pod_inventory_count': rollout.pod_inventory_count,
            'writer_pod_inventory_sha256': rollout.pod_inventory_sha256,
            'writer_process_count': len(rollout.writer_instances),
        }


def normalize_retained_replica_records() -> dict[str, Any]:
    """Run normalization without exposing unexpected operational failures."""
    try:
        return _normalize_retained_replica_records()
    except ReplicaRecordNormalizationError:
        raise
    except Exception:  # pylint: disable=broad-except
        # Keep this boundary outside the lock and both session contexts so they
        # finish rollback/cleanup before an operational or database failure is
        # reduced to the secret-safe public contract. In particular, raw
        # SQLAlchemy exceptions can render bound replica identifiers or JSON.
        raise ReplicaRecordNormalizationError(
            'ReplicaInfo v18 normalization failed unexpectedly.') from None


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Normalize retained ReplicaInfo rows to strict v18.')
    parser.add_argument('--json', action='store_true', required=True)
    parser.parse_args()
    receipt = normalize_retained_replica_records()
    print(json.dumps(receipt, sort_keys=True, separators=(',', ':')))


if __name__ == '__main__':
    main()
