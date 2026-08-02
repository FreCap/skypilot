"""PostgreSQL store for the dark resource-action kernel.

Only API requests own execution leases.  This store adds stable logical
identity and attempt evidence above that existing queue; it intentionally has
no dispatcher, provider handler, or SkyServe dependency.
"""

from __future__ import annotations

from collections.abc import Mapping
import datetime
import json
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.server.requests import postgres as request_postgres
from sky.server.requests import requests as requests_lib
from sky.server.requests import resource_actions as actions
from sky.server.requests import storage as request_storage

_TERMINAL_REQUEST_STATES = tuple(
    status.value for status in requests_lib.RequestStatus.finished_status())


def _mapping_value(mapping: Mapping[str, Any], key: str) -> Any:
    try:
        return mapping[key]
    except KeyError as e:
        raise actions.InvariantViolation(
            f'Durable row is missing required column {key!r}.') from e


def _json_object(value: Any, *, name: str) -> actions.JsonObject:
    if not isinstance(value, Mapping):
        raise actions.InvariantViolation(f'{name} is not a JSON object.')
    try:
        normalized = actions._canonical_object(  # pylint: disable=protected-access
            value, name=name)
        raw_bytes = json.dumps(value,
                               sort_keys=True,
                               separators=(',', ':'),
                               ensure_ascii=False,
                               allow_nan=False).encode('utf-8')
        if raw_bytes != actions.canonical_json_bytes(normalized):
            raise ValueError('stored value is not already canonical')
        return normalized
    except (TypeError, ValueError) as e:
        raise actions.InvariantViolation(
            f'{name} violates canonical object bounds: {e}') from e


def _sha256_text(value: Any, *, name: str) -> str:
    if (not isinstance(value, str) or len(value) != 64 or
            any(character not in '0123456789abcdef' for character in value)):
        raise actions.InvariantViolation(
            f'{name} is not lowercase SHA-256 text.')
    return value


def _timestamp(value: Any, *, name: str) -> datetime.datetime:
    if (not isinstance(value, datetime.datetime) or value.tzinfo is None or
            value.utcoffset() is None):
        raise actions.InvariantViolation(
            f'{name} is not a timezone-aware timestamp.')
    return value


def _action_record(row: Mapping[str, Any]) -> actions.ActionRecord:
    """Decode and fully revalidate one durable action row."""
    try:
        resource_identity_value = json.loads(
            str(_mapping_value(row, 'resource_identity')))
        if not isinstance(resource_identity_value, dict):
            raise ValueError('resource identity is not an object')
        expected_keys = {
            'version', 'service_hash', 'service_incarnation', 'replica_id',
            'replica_incarnation'
        }
        if set(resource_identity_value) != expected_keys:
            raise ValueError('resource identity has unknown or missing fields')
        if resource_identity_value['version'] != 1:
            raise ValueError('unsupported resource identity version')
        if _mapping_value(row, 'domain') != 'serve':
            raise ValueError('unsupported action domain')
        if _mapping_value(row, 'resource_type') != 'replica':
            raise ValueError('unsupported resource type')
        identity = actions.ResourceActionIdentity(
            service_hash=resource_identity_value['service_hash'],
            service_incarnation=resource_identity_value['service_incarnation'],
            replica_id=resource_identity_value['replica_id'],
            replica_incarnation=resource_identity_value['replica_incarnation'],
            desired_generation=int(_mapping_value(row, 'desired_generation')),
            action_kind=actions.ActionKind(
                str(_mapping_value(row, 'action_type'))),
        )
        action_id = uuid.UUID(str(_mapping_value(row, 'action_id')))
        if action_id != identity.action_id:
            raise ValueError('action UUID does not match its identity preimage')
        if str(_mapping_value(
                row, 'resource_identity')) != identity.resource_identity:
            raise ValueError('resource identity bytes are not canonical')
        immutable_spec = _json_object(_mapping_value(row, 'immutable_spec'),
                                      name='immutable_spec')
        immutable_spec_sha256 = str(_mapping_value(row,
                                                   'immutable_spec_sha256'))
        if actions.canonical_sha256(immutable_spec) != immutable_spec_sha256:
            raise ValueError('immutable spec hash mismatch')
        kernel_state = actions.KernelState(
            str(_mapping_value(row, 'kernel_state')))
        last_result_raw = row.get('last_result')
        last_result = (_json_object(last_result_raw, name='last_result')
                       if last_result_raw is not None else None)
        last_result_sha256 = row.get('last_result_sha256')
        if ((last_result is None) != (last_result_sha256 is None) or
            (last_result is not None and
             actions.canonical_sha256(last_result) != last_result_sha256)):
            raise ValueError('last result/hash shape mismatch')
        return actions.ActionRecord(
            action_id=action_id,
            domain='serve',
            resource_type='replica',
            resource_identity=identity.resource_identity,
            desired_generation=identity.desired_generation,
            action_type=identity.action_kind.value,
            immutable_spec=immutable_spec,
            immutable_spec_sha256=immutable_spec_sha256,
            kernel_state=kernel_state,
            current_attempt=int(_mapping_value(row, 'current_attempt')),
            next_attempt_at=row.get('next_attempt_at'),
            last_result=last_result,
            last_result_sha256=(str(last_result_sha256)
                                if last_result_sha256 is not None else None),
            terminal_disposition=row.get('terminal_disposition'),
            revision=int(_mapping_value(row, 'revision')),
            created_at=_mapping_value(row, 'created_at'),
            updated_at=_mapping_value(row, 'updated_at'),
            terminal_at=row.get('terminal_at'),
        )
    except (KeyError, TypeError, ValueError) as e:
        raise actions.InvariantViolation(
            f'Invalid durable resource-action row: {e}') from e


def _attempt_record(row: Mapping[str, Any]) -> actions.AttemptRecord:
    """Decode and revalidate one durable attempt row."""
    try:
        action_id = uuid.UUID(str(_mapping_value(row, 'action_id')))
        attempt_value = _mapping_value(row, 'attempt')
        if (not isinstance(attempt_value, int) or
                isinstance(attempt_value, bool) or attempt_value <= 0):
            raise ValueError('attempt is not a positive integer')
        request_id_value = _mapping_value(row, 'request_id')
        if not isinstance(request_id_value, str):
            raise ValueError('request ID is not text')
        if request_id_value != actions.request_id_for_attempt(
                action_id, attempt_value):
            raise ValueError('request ID does not match the attempt preimage')
        request_input_sha256 = _sha256_text(_mapping_value(
            row, 'request_input_sha256'),
                                            name='request_input_sha256')
        provider_operation_id = row.get('provider_operation_id')
        if provider_operation_id is not None:
            if not isinstance(provider_operation_id, str):
                raise ValueError('provider operation ID is not text')
            normalized_provider_operation_id = actions._bounded_text(  # pylint: disable=protected-access
                provider_operation_id,
                name='provider_operation_id',
                maximum_bytes=1024)
            if provider_operation_id != normalized_provider_operation_id:
                raise ValueError('provider operation ID is not canonical')
        typed_outcome_raw = row.get('typed_outcome')
        typed_outcome = (_json_object(typed_outcome_raw, name='typed_outcome')
                         if typed_outcome_raw is not None else None)
        typed_outcome_sha256 = row.get('typed_outcome_sha256')
        if typed_outcome_sha256 is not None:
            typed_outcome_sha256 = _sha256_text(typed_outcome_sha256,
                                                name='typed_outcome_sha256')
        if ((typed_outcome is None) != (typed_outcome_sha256 is None) or
            (typed_outcome is not None and
             actions.canonical_sha256(typed_outcome) != typed_outcome_sha256)):
            raise ValueError('typed outcome/hash shape mismatch')
        boundary = actions.MutationBoundary(
            str(_mapping_value(row, 'mutation_boundary')))
        provider_io_boundary = actions.ProviderIOBoundary(
            str(_mapping_value(row, 'provider_io_boundary')))
        if (boundary is not actions.MutationBoundary.SETTLED and
                boundary.value != provider_io_boundary.value):
            raise ValueError('unsettled request/provider boundaries differ')
        if (provider_io_boundary
                is not actions.ProviderIOBoundary.SUBMITTED_OR_AMBIGUOUS and
                provider_operation_id is not None):
            raise ValueError('provider operation ID precedes submission')
        provider_progress_raw = row.get('provider_progress')
        provider_progress = (_json_object(provider_progress_raw,
                                          name='provider_progress')
                             if provider_progress_raw is not None else None)
        provider_progress_sha256 = row.get('provider_progress_sha256')
        if provider_progress_sha256 is not None:
            provider_progress_sha256 = _sha256_text(
                provider_progress_sha256, name='provider_progress_sha256')
        progress_revision = _mapping_value(row, 'provider_progress_revision')
        if (not isinstance(progress_revision, int) or
                isinstance(progress_revision, bool) or progress_revision < 0):
            raise ValueError('provider progress revision is negative')
        if ((provider_progress is None) != (provider_progress_sha256 is None) or
            (provider_progress is None) != (progress_revision == 0) or
            (provider_progress is not None and
             actions.canonical_sha256(provider_progress)
             != provider_progress_sha256)):
            raise ValueError('provider progress/hash/revision shape mismatch')
        terminal_state = row.get('request_terminal_state')
        admitted_at = _timestamp(_mapping_value(row, 'admitted_at'),
                                 name='admitted_at')
        updated_at = _timestamp(_mapping_value(row, 'updated_at'),
                                name='updated_at')
        settled_at = row.get('settled_at')
        if settled_at is not None:
            settled_at = _timestamp(settled_at, name='settled_at')
        if updated_at < admitted_at or (settled_at is not None and
                                        settled_at < admitted_at):
            raise ValueError('attempt timestamps are out of order')
        if boundary is actions.MutationBoundary.SETTLED:
            if (typed_outcome is None or
                    terminal_state not in _TERMINAL_REQUEST_STATES or
                    settled_at is None):
                raise ValueError('settled attempt has incomplete evidence')
        elif (typed_outcome is not None or terminal_state is not None or
              settled_at is not None):
            raise ValueError('unsettled attempt has terminal evidence')
        return actions.AttemptRecord(
            action_id=action_id,
            attempt=attempt_value,
            request_id=request_id_value,
            request_input_sha256=request_input_sha256,
            provider_operation_id=provider_operation_id,
            mutation_boundary=boundary,
            provider_io_boundary=provider_io_boundary,
            provider_progress=provider_progress,
            provider_progress_sha256=(str(provider_progress_sha256)
                                      if provider_progress_sha256 is not None
                                      else None),
            provider_progress_revision=progress_revision,
            typed_outcome=typed_outcome,
            typed_outcome_sha256=(str(typed_outcome_sha256) if
                                  typed_outcome_sha256 is not None else None),
            request_terminal_state=terminal_state,
            admitted_at=admitted_at,
            updated_at=updated_at,
            settled_at=settled_at,
        )
    except (TypeError, ValueError) as e:
        raise actions.InvariantViolation(
            f'Invalid durable resource-action attempt row: {e}') from e


def _same_canonical_value(left: Any, right: Any) -> bool:
    try:
        return actions.canonical_json_bytes(
            left) == actions.canonical_json_bytes(right)
    except (TypeError, ValueError):
        return False


def _deadline_text(value: datetime.datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise actions.InvariantViolation(
            'Durable precondition deadline has no timezone.')
    return value.astimezone(
        datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def _request_input_mismatches(
    request_row: Mapping[str, Any],
    queue_row: Mapping[str, Any] | None,
    request_input: actions.ActionRequestInput,
    *,
    require_queue: bool,
) -> list[str]:
    """Compare every surviving immutable request/queue input."""
    expected = request_input.value
    mismatches: list[str] = []
    correlation = {
        'request_id': request_row.get('request_id'),
        'action_id':
            (str(request_row['resource_action_id'])
             if request_row.get('resource_action_id') is not None else None),
        'attempt': request_row.get('resource_action_attempt'),
    }
    if correlation != {
            'request_id': request_input.request_id,
            'action_id': str(request_input.action_id),
            'attempt': request_input.attempt,
    }:
        mismatches.append('correlation')
    request_columns = {
        'name': 'name',
        'handler_name': 'handler_name',
        'payload_type': 'payload_type',
        'payload_format': 'payload_format',
        'payload_version': 'payload_version',
        'producer_version': 'producer_version',
        'payload_json': 'payload_json',
        'execution_class': 'execution_class',
        'cluster_name': 'cluster_name',
        'schedule_type': 'schedule_type',
        'user_id': 'user_id',
        'file_mounts_blob_id': 'file_mounts_blob_id',
        'ignore_return_value': 'ignore_return_value',
        'retryable': 'retryable',
    }
    for input_name, column_name in request_columns.items():
        if not _same_canonical_value(request_row.get(column_name),
                                     expected[input_name]):
            mismatches.append(input_name)

    if queue_row is None:
        if require_queue:
            mismatches.append('queue_missing')
        return mismatches
    queue_values = {
        'schedule_type': queue_row.get('schedule_type'),
        'queue_priority': queue_row.get('priority'),
        'ignore_return_value': queue_row.get('ignore_return_value'),
        'retryable': queue_row.get('retryable'),
        'precondition_type': queue_row.get('precondition_type'),
        'precondition_payload': queue_row.get('precondition_payload'),
        'precondition_deadline': _deadline_text(
            queue_row.get('precondition_deadline')),
    }
    for input_name, actual in queue_values.items():
        if not _same_canonical_value(actual, expected[input_name]):
            mismatches.append(f'queue.{input_name}')
    return mismatches


def _bounded_conflict_result(code: str, request_id: str) -> actions.JsonObject:
    return {
        'version': 1,
        'kind': 'materialization_conflict',
        'code': code[:128],
        'request_id': request_id[:128],
    }


class PostgresResourceActionStore:
    """Typed PostgreSQL store reusing the existing request queue and lease."""

    def __init__(
        self,
        engine: sqlalchemy.engine.Engine | None = None,
        *,
        provider_progress_contract: actions.ProviderProgressContract,
    ) -> None:
        self._engine = engine
        self._instance_id = request_postgres.ensure_server_instance_id()
        self._provider_progress_contract = provider_progress_contract
        for method_name in ('retry_seed', 'validate_attempt_snapshot',
                            'validate_progress_transition',
                            'validate_reduction'):
            if not callable(
                    getattr(provider_progress_contract, method_name, None)):
                raise TypeError('provider_progress_contract is missing '
                                f'{method_name}().')

    def _database(self) -> sqlalchemy.engine.Engine:
        return self._engine or request_postgres.initialize_and_get_db()

    def admit_in_transaction(
        self,
        connection: sqlalchemy.engine.Connection,
        new_action: actions.NewResourceAction,
    ) -> actions.ActionRecord:
        """Idempotently admit an action using the caller's transaction.

        The caller acquires any domain/leadership rows before this method.  It
        neither commits nor opens a nested transaction.
        """
        identity = new_action.identity
        now = sqlalchemy.func.clock_timestamp()
        values = {
            'action_id': new_action.action_id,
            'domain': 'serve',
            'resource_type': 'replica',
            'resource_identity': identity.resource_identity,
            'desired_generation': identity.desired_generation,
            'action_type': identity.action_kind.value,
            'immutable_spec': dict(new_action.immutable_spec),
            'immutable_spec_sha256': new_action.immutable_spec_sha256,
            'kernel_state': actions.KernelState.READY.value,
            'current_attempt': 0,
            'next_attempt_at': now,
            'last_result': None,
            'last_result_sha256': None,
            'terminal_disposition': None,
            'revision': 1,
            'created_at': now,
            'updated_at': now,
            'terminal_at': None,
        }
        connection.execute(
            postgresql.insert(request_postgres.RESOURCE_ACTIONS).values(
                **values).on_conflict_do_nothing().returning(
                    request_postgres.RESOURCE_ACTIONS.c.action_id)
        ).scalar_one_or_none()
        action_table = request_postgres.RESOURCE_ACTIONS
        # Lock both possible conflict targets in canonical UUID order.  Two
        # rows means the action-ID and natural-key commitments diverged.
        rows = connection.execute(
            sqlalchemy.select(action_table).where(
                sqlalchemy.or_(
                    action_table.c.action_id == new_action.action_id,
                    sqlalchemy.and_(
                        action_table.c.domain == 'serve',
                        action_table.c.resource_type == 'replica',
                        action_table.c.resource_identity ==
                        identity.resource_identity,
                        action_table.c.desired_generation ==
                        identity.desired_generation, action_table.c.action_type
                        == identity.action_kind.value))).order_by(
                            action_table.c.action_id).with_for_update()
        ).mappings().all()
        if len(rows) != 1:
            if not rows:
                raise actions.InvariantViolation(
                    'Action insert reported a conflict without a durable row.')
            raise actions.ActionConflict(
                'Action UUID and natural identity resolve to different rows.')
        row = rows[0]
        record = _action_record(row)
        expected_immutable_bytes = actions.canonical_json_bytes(
            new_action.immutable_spec)
        if (record.action_id != new_action.action_id or
                record.resource_identity != identity.resource_identity or
                record.desired_generation != identity.desired_generation or
                record.action_type != identity.action_kind.value or
                actions.canonical_json_bytes(record.immutable_spec)
                != expected_immutable_bytes or record.immutable_spec_sha256
                != new_action.immutable_spec_sha256):
            raise actions.ActionConflict(
                f'Action identity {new_action.action_id} already exists with '
                'different immutable bytes.')
        return record

    def list_due(self, limit: int = 100) -> list[actions.ActionCandidate]:
        """Discover due READY actions without taking row locks."""
        if (not isinstance(limit, int) or isinstance(limit, bool) or
                limit <= 0):
            raise ValueError('limit must be a positive integer.')
        statement = sqlalchemy.select(request_postgres.RESOURCE_ACTIONS).where(
            request_postgres.RESOURCE_ACTIONS.c.kernel_state ==
            actions.KernelState.READY.value,
            request_postgres.RESOURCE_ACTIONS.c.next_attempt_at
            <= sqlalchemy.func.clock_timestamp()).order_by(
                request_postgres.RESOURCE_ACTIONS.c.next_attempt_at,
                request_postgres.RESOURCE_ACTIONS.c.action_id).limit(limit)
        with self._database().connect() as connection:
            rows = connection.execute(statement).mappings().all()
        records = [_action_record(row) for row in rows]
        return [
            actions.ActionCandidate(record.action_id, record.revision,
                                    record.current_attempt + 1,
                                    record.next_attempt_at)
            for record in records
        ]

    def _locked_action(
        self,
        connection: sqlalchemy.engine.Connection,
        action_id: uuid.UUID,
        *,
        skip_locked: bool = False,
    ) -> Mapping[str, Any] | None:
        return connection.execute(
            sqlalchemy.select(request_postgres.RESOURCE_ACTIONS).where(
                request_postgres.RESOURCE_ACTIONS.c.action_id == action_id).
            with_for_update(skip_locked=skip_locked)).mappings().first()

    def _locked_attempt(
        self,
        connection: sqlalchemy.engine.Connection,
        action_id: uuid.UUID,
        attempt: int,
    ) -> Mapping[str, Any] | None:
        return connection.execute(
            sqlalchemy.select(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                action_id, request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt
                == attempt).with_for_update()).mappings().first()

    def _block_locked_action(
        self,
        connection: sqlalchemy.engine.Connection,
        action: actions.ActionRecord,
        *,
        attempt: int,
        request_id: str,
        code: str,
    ) -> actions.ActionRecord:
        result_value = _bounded_conflict_result(code, request_id)
        result = connection.execute(
            sqlalchemy.update(request_postgres.RESOURCE_ACTIONS).where(
                request_postgres.RESOURCE_ACTIONS.c.action_id ==
                action.action_id, request_postgres.RESOURCE_ACTIONS.c.revision
                == action.revision).values(
                    kernel_state=actions.KernelState.BLOCKED.value,
                    current_attempt=max(action.current_attempt, attempt),
                    next_attempt_at=None,
                    last_result=result_value,
                    last_result_sha256=actions.canonical_sha256(result_value),
                    terminal_disposition=None,
                    terminal_at=None,
                    revision=action.revision + 1,
                    updated_at=sqlalchemy.func.clock_timestamp()))
        if result.rowcount != 1:
            raise actions.StaleRevision(
                f'Action {action.action_id} changed while being blocked.')
        row = self._locked_action(connection, action.action_id)
        assert row is not None
        return _action_record(row)

    def _locked_binding(
        self,
        connection: sqlalchemy.engine.Connection,
        request_input: actions.ActionRequestInput,
    ) -> tuple[actions.AttemptRecord | None, Mapping[str, Any] | None,
               Mapping[str, Any] | None]:
        attempt_row = self._locked_attempt(connection, request_input.action_id,
                                           request_input.attempt)
        request_row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request_input.request_id).with_for_update()).mappings().first()
        queue_row = connection.execute(
            sqlalchemy.select(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id ==
                request_input.request_id).with_for_update()).mappings().first()
        return ((_attempt_record(attempt_row)
                 if attempt_row is not None else None), request_row, queue_row)

    def _lock_attempt_insert_conflicts(
        self,
        connection: sqlalchemy.engine.Connection,
        request_input: actions.ActionRequestInput,
    ) -> list[Mapping[str, Any]]:
        """Stabilize every unique target that rejected an attempt insert."""
        attempt_table = request_postgres.RESOURCE_ACTION_ATTEMPTS
        rows = connection.execute(
            sqlalchemy.select(attempt_table).where(
                sqlalchemy.or_(
                    sqlalchemy.and_(
                        attempt_table.c.action_id == request_input.action_id,
                        attempt_table.c.attempt == request_input.attempt),
                    attempt_table.c.request_id == request_input.request_id)).
            order_by(
                attempt_table.c.action_id,
                attempt_table.c.attempt).with_for_update()).mappings().all()
        request_ids = sorted({
            request_input.request_id,
            *(str(row['request_id']) for row in rows),
        })
        connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id.in_(
                    request_ids)).order_by(request_postgres.REQUESTS.c.
                                           request_id).with_for_update()).all()
        connection.execute(
            sqlalchemy.select(request_postgres.QUEUE).where(
                request_postgres.QUEUE.c.request_id.in_(request_ids)).order_by(
                    request_postgres.QUEUE.c.request_id).with_for_update()).all(
                    )
        return rows

    def _derive_retry_seed(
        self,
        action: actions.ActionRecord,
        predecessor: actions.AttemptRecord,
    ) -> actions.JsonObject | None:
        """Validate a settled predecessor and derive its sole retry seed."""
        if predecessor.mutation_boundary is not actions.MutationBoundary.SETTLED:
            raise actions.ActionConflict('Retry predecessor is not settled.')
        if predecessor.typed_outcome is None:
            raise actions.InvariantViolation(
                'Settled retry predecessor has no typed outcome.')
        if predecessor.provider_progress is None:
            if (predecessor.provider_io_boundary
                    is not actions.ProviderIOBoundary.NOT_STARTED or
                    predecessor.provider_progress_sha256 is not None or
                    predecessor.provider_progress_revision != 0 or
                    predecessor.provider_operation_id is not None):
                raise actions.ActionConflict(
                    'A fresh retry cursor requires the exact predecessor '
                    'pre-I/O shape.')
        try:
            seed_value = self._provider_progress_contract.retry_seed(
                action, predecessor)
            seed = (
                None if seed_value is None else actions._canonical_object(  # pylint: disable=protected-access
                    seed_value,
                    name='derived_provider_progress_seed'))
        except (TypeError, ValueError) as e:
            raise actions.ActionConflict(
                f'Typed retry seed derivation rejected the predecessor: {e}'
            ) from e
        if ((predecessor.provider_progress is None) != (seed is None)):
            raise actions.ActionConflict(
                'Typed retry seed presence differs from predecessor progress.')
        return seed

    def _validate_progress_snapshot(
        self,
        action: actions.ActionRecord,
        predecessor: actions.AttemptRecord | None,
        attempt: actions.AttemptRecord,
        execution_fence: actions.AttemptExecutionFence | None,
    ) -> None:
        try:
            self._provider_progress_contract.validate_attempt_snapshot(
                action, predecessor, attempt, execution_fence)
        except (TypeError, ValueError) as e:
            raise actions.InvariantViolation(
                f'Typed provider progress snapshot is invalid: {e}') from e

    def _validate_progress_transition(
        self,
        action: actions.ActionRecord,
        predecessor: actions.AttemptRecord | None,
        attempt: actions.AttemptRecord,
        execution_fence: actions.AttemptExecutionFence,
        proposed_progress: actions.JsonObject,
    ) -> None:
        self._validate_progress_snapshot(action, predecessor, attempt,
                                         execution_fence)
        try:
            self._provider_progress_contract.validate_progress_transition(
                action, predecessor, attempt, execution_fence,
                proposed_progress)
        except (TypeError, ValueError) as e:
            raise actions.ActionConflict(
                f'Typed provider progress transition is invalid: {e}') from e

    def _validate_reduction_contract(
        self,
        action: actions.ActionRecord,
        predecessor: actions.AttemptRecord | None,
        attempt: actions.AttemptRecord,
        reduction: actions.ActionReduction,
        context: actions.ReductionContext,
    ) -> None:
        if reduction.kernel_state is actions.KernelState.READY:
            if attempt.provider_progress is None:
                if (attempt.provider_io_boundary
                        is not actions.ProviderIOBoundary.NOT_STARTED or
                        attempt.provider_progress_sha256 is not None or
                        attempt.provider_progress_revision != 0 or
                        attempt.provider_operation_id is not None):
                    raise actions.ActionConflict(
                        'Retry reduction requires an exact pre-I/O null cursor '
                        'or a retained typed cursor.')
            elif attempt.provider_io_boundary is (
                    actions.ProviderIOBoundary.NOT_STARTED):
                if (attempt.attempt <= 1 or predecessor is None or
                        attempt.provider_progress_revision != 1 or
                        attempt.provider_operation_id is not None):
                    raise actions.ActionConflict(
                        'A NOT_STARTED retry cursor must be the exact inherited '
                        'revision-one seed.')
        try:
            self._provider_progress_contract.validate_reduction(
                action, predecessor, attempt, reduction, context)
        except (TypeError, ValueError) as e:
            raise actions.ActionConflict(
                f'Typed action reduction is invalid: {e}') from e

    def materialize(
        self,
        action_id: uuid.UUID,
        expected_revision: int,
        expected_attempt: int,
        request: requests_lib.Request,
    ) -> actions.MaterializationResult | None:
        """Materialize READY or adopt the exact QUEUED lost-ACK binding.

        This method owns one short transaction.  ``None`` means another
        materializer currently owns the action row selected with SKIP LOCKED.
        """
        request_input = actions.ActionRequestInput.from_request(
            action_id, expected_attempt, request)
        request_input.validate()
        with self._database().begin() as connection:
            action_row = self._locked_action(connection,
                                             request_input.action_id,
                                             skip_locked=True)
            if action_row is None:
                exists = connection.execute(
                    sqlalchemy.select(
                        request_postgres.RESOURCE_ACTIONS.c.action_id).where(
                            request_postgres.RESOURCE_ACTIONS.c.action_id ==
                            request_input.action_id)).scalar_one_or_none()
                if exists is not None:
                    return None
                raise actions.InvariantViolation(
                    f'Unknown resource action {request_input.action_id}.')
            action = _action_record(action_row)

            if action.kernel_state is actions.KernelState.READY:
                if (action.revision != expected_revision or
                        expected_attempt != action.current_attempt + 1):
                    raise actions.StaleRevision(
                        f'Action {action.action_id} is revision '
                        f'{action.revision}/attempt {action.current_attempt}.')
                database_now = connection.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
                if (action.next_attempt_at is None or
                        action.next_attempt_at > database_now):
                    raise actions.StaleRevision(
                        f'Action {action.action_id} is not due.')
                predecessor: actions.AttemptRecord | None = None
                progress_seed: actions.JsonObject | None = None
                if expected_attempt > 1:
                    predecessor_row = self._locked_attempt(
                        connection, action.action_id, expected_attempt - 1)
                    if predecessor_row is None:
                        raise actions.InvariantViolation(
                            'Retry materialization is missing its predecessor.')
                    predecessor = _attempt_record(predecessor_row)
                    progress_seed = self._derive_retry_seed(action, predecessor)
                inserted_attempt = connection.execute(
                    postgresql.insert(
                        request_postgres.RESOURCE_ACTION_ATTEMPTS).values(
                            action_id=action.action_id,
                            attempt=expected_attempt,
                            request_id=request_input.request_id,
                            request_input_sha256=request_input.sha256,
                            provider_operation_id=None,
                            mutation_boundary=(
                                actions.MutationBoundary.NOT_STARTED.value),
                            provider_io_boundary=(
                                actions.ProviderIOBoundary.NOT_STARTED.value),
                            provider_progress=progress_seed,
                            provider_progress_sha256=(
                                actions.canonical_sha256(progress_seed)
                                if progress_seed is not None else None),
                            provider_progress_revision=(1 if progress_seed
                                                        is not None else 0),
                            typed_outcome=None,
                            typed_outcome_sha256=None,
                            request_terminal_state=None,
                            admitted_at=sqlalchemy.func.clock_timestamp(),
                            updated_at=sqlalchemy.func.clock_timestamp(),
                            settled_at=None).on_conflict_do_nothing().returning(
                                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.
                                request_id)).scalar_one_or_none()
                if inserted_attempt is None:
                    conflicts = self._lock_attempt_insert_conflicts(
                        connection, request_input)
                    if not conflicts:
                        raise actions.InvariantViolation(
                            'Attempt insert conflicted without a durable owner.'
                        )
                    foreign_owner = any(
                        uuid.UUID(str(row['action_id'])) != action.action_id or
                        int(row['attempt']) != expected_attempt
                        for row in conflicts)
                    conflict_code = ('ready_request_id_conflict' if
                                     foreign_owner else 'ready_attempt_exists')
                    if len(conflicts) > 1:
                        conflict_code = 'ready_multiple_attempt_conflicts'
                    blocked = self._block_locked_action(
                        connection,
                        action,
                        attempt=expected_attempt,
                        request_id=request_input.request_id,
                        code=conflict_code)
                    return actions.MaterializationResult(blocked,
                                                         None,
                                                         blocked=True)
                inserted_attempt_row = self._locked_attempt(
                    connection, action.action_id, expected_attempt)
                if inserted_attempt_row is None:
                    raise actions.InvariantViolation(
                        'Attempt insert succeeded without a durable row.')
                inserted_attempt_record = _attempt_record(inserted_attempt_row)
                try:
                    self._validate_progress_snapshot(action, predecessor,
                                                     inserted_attempt_record,
                                                     None)
                except actions.ResourceActionError:
                    blocked = self._block_locked_action(
                        connection,
                        action,
                        attempt=expected_attempt,
                        request_id=request_input.request_id,
                        code='inserted_attempt_progress_contract')
                    return actions.MaterializationResult(
                        blocked, inserted_attempt_record, blocked=True)
                inserted_request = request_postgres._insert_request_and_queue(  # pylint: disable=protected-access
                    connection,
                    request,
                    resource_action_id=action.action_id,
                    resource_action_attempt=expected_attempt)
                attempt, request_row, queue_row = self._locked_binding(
                    connection, request_input)
                if not inserted_request:
                    blocked = self._block_locked_action(
                        connection,
                        action,
                        attempt=expected_attempt,
                        request_id=request_input.request_id,
                        code='ready_request_exists')
                    return actions.MaterializationResult(blocked,
                                                         attempt,
                                                         blocked=True)
                if attempt is None or request_row is None:
                    blocked = self._block_locked_action(
                        connection,
                        action,
                        attempt=expected_attempt,
                        request_id=request_input.request_id,
                        code='inserted_binding_missing')
                    return actions.MaterializationResult(blocked,
                                                         attempt,
                                                         blocked=True)
                mismatches = _request_input_mismatches(request_row,
                                                       queue_row,
                                                       request_input,
                                                       require_queue=True)
                if (attempt.request_id != request_input.request_id or
                        attempt.request_input_sha256 != request_input.sha256):
                    mismatches.append('attempt_commitment')
                if (not _same_canonical_value(attempt.provider_progress,
                                              progress_seed) or
                        attempt.provider_progress_revision
                        != (1 if progress_seed is not None else 0)):
                    mismatches.append('attempt_progress_seed')
                try:
                    self._validate_progress_snapshot(action, predecessor,
                                                     attempt, None)
                except actions.ResourceActionError:
                    mismatches.append('attempt_progress_contract')
                if mismatches:
                    blocked = self._block_locked_action(
                        connection,
                        action,
                        attempt=expected_attempt,
                        request_id=request_input.request_id,
                        code='inserted_' + mismatches[0])
                    return actions.MaterializationResult(blocked,
                                                         attempt,
                                                         blocked=True)
                result = connection.execute(
                    sqlalchemy.update(request_postgres.RESOURCE_ACTIONS).where(
                        request_postgres.RESOURCE_ACTIONS.c.action_id ==
                        action.action_id,
                        request_postgres.RESOURCE_ACTIONS.c.revision ==
                        expected_revision,
                        request_postgres.RESOURCE_ACTIONS.c.kernel_state ==
                        actions.KernelState.READY.value).values(
                            kernel_state=actions.KernelState.QUEUED.value,
                            current_attempt=expected_attempt,
                            next_attempt_at=None,
                            revision=expected_revision + 1,
                            updated_at=sqlalchemy.func.clock_timestamp()))
                if result.rowcount != 1:
                    raise actions.StaleRevision(
                        f'Action {action.action_id} changed during '
                        'materialization.')
                committed_row = self._locked_action(connection,
                                                    action.action_id)
                assert committed_row is not None
                return actions.MaterializationResult(
                    _action_record(committed_row), attempt, created=True)

            if action.kernel_state is actions.KernelState.QUEUED:
                if (action.revision != expected_revision + 1 or
                        action.current_attempt != expected_attempt):
                    raise actions.StaleRevision(
                        f'Action {action.action_id} is not the expected '
                        'lost-ack materialization.')
                predecessor = None
                if expected_attempt > 1:
                    predecessor_row = self._locked_attempt(
                        connection, action.action_id, expected_attempt - 1)
                    if predecessor_row is None:
                        raise actions.InvariantViolation(
                            'Lost-ACK retry adoption is missing its predecessor.'
                        )
                    predecessor = _attempt_record(predecessor_row)
                    # Re-derivation validates the settled predecessor's typed
                    # outcome/cursor without accepting any caller cursor.
                    self._derive_retry_seed(action, predecessor)
                attempt, request_row, queue_row = self._locked_binding(
                    connection, request_input)
                if attempt is None or request_row is None:
                    blocked = self._block_locked_action(
                        connection,
                        action,
                        attempt=expected_attempt,
                        request_id=request_input.request_id,
                        code='adoption_binding_missing')
                    return actions.MaterializationResult(blocked,
                                                         attempt,
                                                         blocked=True)
                terminal = request_row['status'] in _TERMINAL_REQUEST_STATES
                mismatches = _request_input_mismatches(
                    request_row,
                    queue_row,
                    request_input,
                    require_queue=not terminal)
                if (attempt.request_id != request_input.request_id or
                        attempt.request_input_sha256 != request_input.sha256):
                    mismatches.append('attempt_commitment')
                try:
                    self._validate_progress_snapshot(action, predecessor,
                                                     attempt, None)
                except actions.ResourceActionError:
                    mismatches.append('attempt_progress_contract')
                if terminal and queue_row is not None:
                    mismatches.append('terminal_queue_present')
                if mismatches:
                    blocked = self._block_locked_action(
                        connection,
                        action,
                        attempt=expected_attempt,
                        request_id=request_input.request_id,
                        code='adoption_' + mismatches[0])
                    return actions.MaterializationResult(blocked,
                                                         attempt,
                                                         blocked=True)
                return actions.MaterializationResult(action,
                                                     attempt,
                                                     adopted=True)

            raise actions.StaleRevision(
                f'Action {action.action_id} is {action.kernel_state.value}, '
                'not materializable.')

    def _lock_claimed_attempt(
        self,
        connection: sqlalchemy.engine.Connection,
        request_id: str,
    ) -> tuple[actions.ActionRecord, actions.AttemptRecord | None,
               actions.AttemptRecord, actions.AttemptExecutionFence]:
        claim = request_storage.current_execution_claim(request_id)
        if claim is None:
            raise actions.ClaimLost(
                f'Request {request_id} has no active execution claim.')
        correlation = connection.execute(
            sqlalchemy.select(
                request_postgres.REQUESTS.c.resource_action_id,
                request_postgres.REQUESTS.c.resource_action_attempt).where(
                    request_postgres.REQUESTS.c.request_id ==
                    request_id)).mappings().first()
        if (correlation is None or correlation['resource_action_id'] is None or
                correlation['resource_action_attempt'] is None):
            raise actions.ClaimLost(
                f'Request {request_id} is not action-correlated.')
        action_id = uuid.UUID(str(correlation['resource_action_id']))
        attempt_number = int(correlation['resource_action_attempt'])
        action_row = self._locked_action(connection, action_id)
        if action_row is None:
            raise actions.InvariantViolation(
                f'Correlated action {action_id} is missing.')
        action = _action_record(action_row)
        predecessor: actions.AttemptRecord | None = None
        if attempt_number > 1:
            predecessor_row = self._locked_attempt(connection, action_id,
                                                   attempt_number - 1)
            if predecessor_row is None:
                raise actions.InvariantViolation(
                    f'Correlated attempt {action_id}/{attempt_number} has no '
                    'predecessor.')
            predecessor = _attempt_record(predecessor_row)
            if predecessor.mutation_boundary is not (
                    actions.MutationBoundary.SETTLED):
                raise actions.InvariantViolation(
                    f'Correlated attempt {action_id}/{attempt_number} has an '
                    'unsettled predecessor.')
        attempt_row = self._locked_attempt(connection, action_id,
                                           attempt_number)
        if attempt_row is None:
            raise actions.InvariantViolation(
                f'Correlated attempt {action_id}/{attempt_number} is missing.')
        attempt = _attempt_record(attempt_row)
        request_row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request_id).with_for_update()).mappings().first()
        if request_row is None:
            raise actions.ClaimLost(
                f'Request {request_id} no longer owns a live claim.')
        # Read a fresh non-transaction timestamp only after FOR UPDATE returns.
        # The lease may have expired while this statement waited for a
        # terminal writer, even when no row value changed.
        fence = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.clock_timestamp().label('database_now'),
                request_postgres._controller_claim_is_current(  # pylint: disable=protected-access
                ).label('controller_is_current')).where(
                    request_postgres.REQUESTS.c.request_id ==
                    request_id)).mappings().one()
        expected_token = uuid.UUID(claim.claim_token)
        expected_worker = uuid.UUID(self._instance_id)
        lease_expires_at = request_row['lease_expires_at']
        request_fence_matches = (
            request_row['resource_action_id'] == action_id and
            request_row['resource_action_attempt'] == attempt_number and
            request_row['status'] == requests_lib.RequestStatus.RUNNING.value
            and request_row['execution_generation']
            == claim.execution_generation and
            request_row['claim_token'] == expected_token and
            request_row['worker_instance_id'] == expected_worker and
            lease_expires_at is not None and
            lease_expires_at > fence['database_now'] and
            bool(fence['controller_is_current']))
        if not request_fence_matches:
            raise actions.ClaimLost(
                f'Request {request_id} no longer owns a live claim.')
        if (action.kernel_state is not actions.KernelState.QUEUED or
                action.current_attempt != attempt_number or
                attempt.request_id != request_id or
                attempt.mutation_boundary is actions.MutationBoundary.SETTLED):
            raise actions.ClaimLost(
                f'Request {request_id} is no longer the active action attempt.')
        fence_identity = actions.AttemptExecutionFence(
            request_id=request_id,
            execution_generation=claim.execution_generation,
            claim_token=expected_token,
            worker_instance_id=expected_worker,
            controller_generation=request_row['controller_generation'])
        return action, predecessor, attempt, fence_identity

    def commit_intent_with_progress(
        self,
        request_id: str,
        provider_progress: Mapping[str, Any],
        expected_progress_revision: int,
    ) -> actions.AttemptRecord:
        """Atomically cross intent and commit the first typed cursor.

        The configured domain contract owns the closed cursor transition. This
        store owns canonical bytes, revision monotonicity, claim fencing, and
        the two boundary fields.
        """
        if (not isinstance(expected_progress_revision, int) or
                isinstance(expected_progress_revision, bool) or
                expected_progress_revision < 0):
            raise ValueError('expected_progress_revision must be nonnegative.')
        normalized = actions._canonical_object(  # pylint: disable=protected-access
            provider_progress,
            name='provider_progress')
        progress_sha256 = actions.canonical_sha256(normalized)
        with self._database().begin() as connection:
            action, predecessor, attempt, fence = self._lock_claimed_attempt(
                connection, request_id)
            self._validate_progress_transition(action, predecessor, attempt,
                                               fence, normalized)
            if (attempt.mutation_boundary
                    is actions.MutationBoundary.INTENT_COMMITTED):
                if (_same_canonical_value(attempt.provider_progress,
                                          normalized) and
                        attempt.provider_progress_sha256 == progress_sha256 and
                        attempt.provider_progress_revision
                        == expected_progress_revision + 1):
                    return attempt
                raise actions.InvariantViolation(
                    'Typed intent replay differs from the committed cursor.')
            if attempt.mutation_boundary is not (
                    actions.MutationBoundary.NOT_STARTED):
                raise actions.InvariantViolation(
                    'Typed intent can only start from NOT_STARTED.')
            if (attempt.provider_progress_revision
                    != expected_progress_revision):
                raise actions.StaleRevision(
                    'Provider progress changed before intent commit.')
            updated = connection.execute(
                sqlalchemy.update(
                    request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                        attempt.action_id,
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                        attempt.attempt, request_postgres.
                        RESOURCE_ACTION_ATTEMPTS.c.mutation_boundary ==
                        actions.MutationBoundary.NOT_STARTED.value,
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.
                        provider_progress_revision == expected_progress_revision
                    ).values(
                        mutation_boundary=(actions.MutationBoundary.
                                           INTENT_COMMITTED.value),
                        provider_io_boundary=(
                            actions.ProviderIOBoundary.INTENT_COMMITTED.value),
                        provider_progress=normalized,
                        provider_progress_sha256=progress_sha256,
                        provider_progress_revision=(expected_progress_revision +
                                                    1),
                        updated_at=sqlalchemy.func.clock_timestamp()))
            if updated.rowcount != 1:
                raise actions.ClaimLost(
                    f'Request {request_id} lost its typed intent fence.')
            row = self._locked_attempt(connection, attempt.action_id,
                                       attempt.attempt)
            assert row is not None
            return _attempt_record(row)

    def write_provider_progress(
        self,
        request_id: str,
        provider_progress: Mapping[str, Any],
        expected_progress_revision: int,
    ) -> actions.AttemptRecord:
        """Claim-fenced monotonic typed provider-progress checkpoint."""
        if (not isinstance(expected_progress_revision, int) or
                isinstance(expected_progress_revision, bool) or
                expected_progress_revision <= 0):
            raise ValueError('expected_progress_revision must be positive.')
        normalized = actions._canonical_object(  # pylint: disable=protected-access
            provider_progress,
            name='provider_progress')
        progress_sha256 = actions.canonical_sha256(normalized)
        with self._database().begin() as connection:
            action, predecessor, attempt, fence = self._lock_claimed_attempt(
                connection, request_id)
            if attempt.mutation_boundary not in (
                    actions.MutationBoundary.INTENT_COMMITTED,
                    actions.MutationBoundary.SUBMITTED_OR_AMBIGUOUS):
                raise actions.InvariantViolation(
                    'Provider progress requires a crossed active intent.')
            if attempt.provider_progress is None:
                raise actions.InvariantViolation(
                    'Later provider progress is missing its first cursor.')
            self._validate_progress_transition(action, predecessor, attempt,
                                               fence, normalized)
            if (_same_canonical_value(attempt.provider_progress, normalized) and
                    attempt.provider_progress_sha256 == progress_sha256 and
                    attempt.provider_progress_revision
                    in (expected_progress_revision,
                        expected_progress_revision + 1)):
                return attempt
            if (attempt.provider_progress_revision
                    != expected_progress_revision):
                raise actions.StaleRevision(
                    'Provider progress revision changed before checkpoint.')
            updated = connection.execute(
                sqlalchemy.update(
                    request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                        attempt.action_id,
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                        attempt.attempt,
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.
                        provider_progress_revision == expected_progress_revision
                    ).values(
                        provider_progress=normalized,
                        provider_progress_sha256=progress_sha256,
                        provider_progress_revision=(expected_progress_revision +
                                                    1),
                        updated_at=sqlalchemy.func.clock_timestamp()))
            if updated.rowcount != 1:
                raise actions.ClaimLost(
                    f'Request {request_id} lost its progress fence.')
            row = self._locked_attempt(connection, attempt.action_id,
                                       attempt.attempt)
            assert row is not None
            return _attempt_record(row)

    def record_submission(
        self,
        request_id: str,
        provider_operation_id: str | None,
    ) -> actions.AttemptRecord:
        """Claim-fenced provider submission/ambiguity evidence write."""
        normalized_operation_id: str | None = None
        if provider_operation_id is not None:
            normalized_operation_id = actions._bounded_text(  # pylint: disable=protected-access
                provider_operation_id,
                name='provider_operation_id',
                maximum_bytes=1024)
        with self._database().begin() as connection:
            action, predecessor, attempt, fence = self._lock_claimed_attempt(
                connection, request_id)
            self._validate_progress_snapshot(action, predecessor, attempt,
                                             fence)
            if attempt.mutation_boundary is actions.MutationBoundary.NOT_STARTED:
                raise actions.InvariantViolation(
                    'Provider submission cannot precede INTENT_COMMITTED.')
            if (normalized_operation_id is not None and
                    attempt.provider_operation_id is not None and
                    attempt.provider_operation_id != normalized_operation_id):
                raise actions.ActionConflict(
                    f'Attempt {attempt.action_id}/{attempt.attempt} already '
                    'has a different provider operation ID.')
            if attempt.mutation_boundary not in (
                    actions.MutationBoundary.INTENT_COMMITTED,
                    actions.MutationBoundary.SUBMITTED_OR_AMBIGUOUS):
                raise actions.InvariantViolation(
                    f'Cannot record submission from '
                    f'{attempt.mutation_boundary.value}.')
            updated = connection.execute(
                sqlalchemy.update(
                    request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                        attempt.action_id,
                        request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt ==
                        attempt.attempt).values(
                            mutation_boundary=(actions.MutationBoundary.
                                               SUBMITTED_OR_AMBIGUOUS.value),
                            provider_io_boundary=(actions.ProviderIOBoundary.
                                                  SUBMITTED_OR_AMBIGUOUS.value),
                            provider_operation_id=(
                                normalized_operation_id
                                if normalized_operation_id is not None else
                                attempt.provider_operation_id),
                            updated_at=sqlalchemy.func.clock_timestamp()))
            if updated.rowcount != 1:
                raise actions.ClaimLost(
                    f'Request {request_id} lost its submission journal fence.')
            row = self._locked_attempt(connection, attempt.action_id,
                                       attempt.attempt)
            assert row is not None
            return _attempt_record(row)

    def list_reducible(self, limit: int = 100) -> list[actions.ActionCandidate]:
        """Discover QUEUED actions whose correlated request is terminal."""
        if (not isinstance(limit, int) or isinstance(limit, bool) or
                limit <= 0):
            raise ValueError('limit must be a positive integer.')
        action_table = request_postgres.RESOURCE_ACTIONS
        attempt_table = request_postgres.RESOURCE_ACTION_ATTEMPTS
        request_table = request_postgres.REQUESTS
        statement = sqlalchemy.select(
            action_table.c.action_id, action_table.c.revision,
            action_table.c.current_attempt, attempt_table.c.request_id).join(
                attempt_table,
                sqlalchemy.and_(
                    attempt_table.c.action_id == action_table.c.action_id,
                    attempt_table.c.attempt == action_table.c.current_attempt)
            ).join(
                request_table,
                sqlalchemy.and_(
                    request_table.c.resource_action_id ==
                    attempt_table.c.action_id,
                    request_table.c.resource_action_attempt ==
                    attempt_table.c.attempt,
                    request_table.c.request_id == attempt_table.c.request_id)
            ).where(
                action_table.c.kernel_state == actions.KernelState.QUEUED.value,
                request_table.c.status.in_(_TERMINAL_REQUEST_STATES)).order_by(
                    action_table.c.updated_at,
                    action_table.c.action_id).limit(limit)
        with self._database().connect() as connection:
            rows = connection.execute(statement).mappings().all()
        return [
            actions.ActionCandidate(action_id=uuid.UUID(str(row['action_id'])),
                                    revision=int(row['revision']),
                                    attempt=int(row['current_attempt']),
                                    request_id=str(row['request_id']))
            for row in rows
        ]

    def _validate_reduction_request(
        self,
        request_row: Mapping[str, Any],
        request_input: actions.ActionRequestInput,
    ) -> None:
        if request_row['status'] not in _TERMINAL_REQUEST_STATES:
            raise actions.StaleRevision(
                f'Request {request_input.request_id} is not terminal.')
        mismatches = _request_input_mismatches(request_row,
                                               None,
                                               request_input,
                                               require_queue=False)
        if mismatches:
            raise actions.ActionConflict(
                f'Terminal request input mismatch: {mismatches[0]}.')

    def reduce_in_transaction(
        self,
        connection: sqlalchemy.engine.Connection,
        action_id: uuid.UUID,
        attempt: int,
        expected_revision: int,
        request_input: actions.ActionRequestInput,
        reducer: actions.Reducer,
    ) -> actions.ReductionResult:
        """Snapshot terminal evidence and reduce in the caller transaction.

        The caller must lock leadership and all matching domain rows first.
        The callback runs only for the first reduction and may update those
        already-locked domain rows with ``connection``.  This method never
        commits and never opens a nested transaction.
        """
        request_input.validate()
        parsed_action_id = uuid.UUID(str(action_id))
        if (request_input.action_id != parsed_action_id or
                request_input.attempt != attempt):
            raise actions.ActionConflict(
                'Reducer input does not match the requested action attempt.')
        action_row = self._locked_action(connection, parsed_action_id)
        if action_row is None:
            raise actions.InvariantViolation(
                f'Unknown resource action {parsed_action_id}.')
        action = _action_record(action_row)
        predecessor: actions.AttemptRecord | None = None
        if attempt > 1:
            predecessor_row = self._locked_attempt(connection, parsed_action_id,
                                                   attempt - 1)
            if predecessor_row is None:
                raise actions.InvariantViolation(
                    f'Action attempt {parsed_action_id}/{attempt} has no '
                    'predecessor.')
            predecessor = _attempt_record(predecessor_row)
            if predecessor.mutation_boundary is not (
                    actions.MutationBoundary.SETTLED):
                raise actions.InvariantViolation(
                    f'Action attempt {parsed_action_id}/{attempt} has an '
                    'unsettled predecessor.')
        attempt_row = self._locked_attempt(connection, parsed_action_id,
                                           attempt)
        if attempt_row is None:
            raise actions.InvariantViolation(
                f'Unknown resource action attempt {parsed_action_id}/{attempt}.'
            )
        attempt_record = _attempt_record(attempt_row)
        if (attempt_record.request_id != request_input.request_id or
                attempt_record.request_input_sha256 != request_input.sha256):
            raise actions.ActionConflict(
                'Attempt request/input commitment does not match reducer input.'
            )

        if attempt_record.mutation_boundary is actions.MutationBoundary.SETTLED:
            if (action.revision != expected_revision + 1 or
                    action.current_attempt != attempt or
                    action.kernel_state is actions.KernelState.QUEUED):
                raise actions.StaleRevision(
                    f'Settled action {action.action_id} has advanced beyond '
                    'this reduction replay.')
            self._validate_progress_snapshot(action, predecessor,
                                             attempt_record, None)
            return actions.ReductionResult(action,
                                           attempt_record,
                                           replayed=True)

        if (action.kernel_state is not actions.KernelState.QUEUED or
                action.revision != expected_revision or
                action.current_attempt != attempt):
            raise actions.StaleRevision(
                f'Action {action.action_id} is not the expected reducible '
                'revision/attempt.')
        request_row = connection.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request_input.request_id)).mappings().first()
        if request_row is None:
            raise actions.InvariantViolation(
                f'Unsettled attempt request {request_input.request_id} is '
                'missing.')
        self._validate_reduction_request(request_row, request_input)
        # This single fresh value is read after all action/attempt lock waits.
        # It is reused for the retry deadline and terminal snapshot.
        database_now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        terminal_request = request_postgres._request_from_mapping(  # pylint: disable=protected-access
            request_row)
        reduction_context = actions.ReductionContext(
            terminal_request=terminal_request, database_now=database_now)
        reduction = reducer(connection, action, attempt_record,
                            reduction_context).normalized()
        typed_outcome = dict(reduction.typed_outcome)
        result_value = dict(reduction.result)
        provider_operation_id = attempt_record.provider_operation_id
        self._validate_reduction_contract(action, predecessor, attempt_record,
                                          reduction, reduction_context)

        settled = connection.execute(
            sqlalchemy.update(request_postgres.RESOURCE_ACTION_ATTEMPTS).where(
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.action_id ==
                parsed_action_id,
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.attempt == attempt,
                request_postgres.RESOURCE_ACTION_ATTEMPTS.c.mutation_boundary
                != actions.MutationBoundary.SETTLED.value).values(
                    provider_operation_id=provider_operation_id,
                    mutation_boundary=actions.MutationBoundary.SETTLED.value,
                    typed_outcome=typed_outcome,
                    typed_outcome_sha256=actions.canonical_sha256(
                        typed_outcome),
                    request_terminal_state=request_row['status'],
                    settled_at=database_now,
                    updated_at=database_now))
        if settled.rowcount != 1:
            raise actions.StaleRevision(
                f'Attempt {parsed_action_id}/{attempt} was already settled.')
        action_values: dict[str, Any] = {
            'kernel_state': reduction.kernel_state.value,
            'next_attempt_at': None,
            'last_result': result_value,
            'last_result_sha256': actions.canonical_sha256(result_value),
            'terminal_disposition': None,
            'terminal_at': None,
            'revision': expected_revision + 1,
            'updated_at': database_now,
        }
        if reduction.kernel_state is actions.KernelState.READY:
            assert reduction.retry_after_seconds is not None
            action_values['next_attempt_at'] = (
                database_now +
                datetime.timedelta(seconds=reduction.retry_after_seconds))
        elif reduction.kernel_state is actions.KernelState.TERMINAL:
            action_values['terminal_disposition'] = (
                reduction.terminal_disposition)
            action_values['terminal_at'] = database_now
        updated = connection.execute(
            sqlalchemy.update(request_postgres.RESOURCE_ACTIONS).where(
                request_postgres.RESOURCE_ACTIONS.c.action_id ==
                parsed_action_id, request_postgres.RESOURCE_ACTIONS.c.revision
                == expected_revision,
                request_postgres.RESOURCE_ACTIONS.c.kernel_state ==
                actions.KernelState.QUEUED.value,
                request_postgres.RESOURCE_ACTIONS.c.current_attempt ==
                attempt).values(**action_values))
        if updated.rowcount != 1:
            raise actions.StaleRevision(
                f'Action {parsed_action_id} changed during reduction.')
        committed_action_row = self._locked_action(connection, parsed_action_id)
        committed_attempt_row = self._locked_attempt(connection,
                                                     parsed_action_id, attempt)
        assert committed_action_row is not None
        assert committed_attempt_row is not None
        committed_action = _action_record(committed_action_row)
        committed_attempt = _attempt_record(committed_attempt_row)
        self._validate_progress_snapshot(committed_action, predecessor,
                                         committed_attempt, None)
        return actions.ReductionResult(committed_action,
                                       committed_attempt,
                                       replayed=False)
