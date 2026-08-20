"""Process-safe PostgreSQL receipts for reserved-fill provider proofs.

Only a completed, context-wide provider fact is shared.  Exact service,
replica, pool, accelerator, and worker-projection authority remains in the
caller-owned launch scope and is revalidated at the terminal effect boundary.
"""

from collections.abc import Callable
from collections.abc import Mapping
import dataclasses
import datetime
import hashlib
import json
import math
import secrets
import socket
import struct
import time
from typing import Any

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.adaptors import common as adaptors_common
from sky.serve import reserved_fill_reclaim_attestation as reclaim
from sky.serve import reserved_fill_reclaim_proof_schema as proof_schema
from sky.utils.db import db_utils
from sky.utils.db import postgres_lock

serve_state_schema = adaptors_common.LazyImport('sky.serve.serve_state_schema')

PROVIDER_PROOF_SCHEMA_VERSION = 1
PROVIDER_PROOF_PAYLOAD_MAX_BYTES = 32 * 1024
_PROVIDER_PROOF_LOCK_PREFIX = 'skyserve-reserved-fill-reclaim-proof'
_INITIAL_JITTER_MAX_SECONDS = 0.1
_RECEIPT_POLL_INITIAL_SECONDS = 0.4
_RECEIPT_POLL_MAX_SECONDS = 1.0
_DATABASE_CONNECT_TIMEOUT_SECONDS = 1
_DATABASE_STATEMENT_TIMEOUT_MILLISECONDS = 200
_DATABASE_SOCKET_TIMEOUT_MILLISECONDS = 200
_DATABASE_IDLE_TRANSACTION_TIMEOUT_MILLISECONDS = 6000
_PROVIDER_PUBLICATION_RESERVE_SECONDS = 0.5
# A receipt handed back to the launch policy must leave time for the caller to
# enter the terminal PostgreSQL authority transaction.  The terminal guard
# still checks the full five-second freshness bound on the database clock; this
# reserve is a liveness qualification, not an extension of proof authority.
_TERMINAL_GUARD_RESERVE_SECONDS = 0.5
_DATABASE_APPLICATION_NAME = 'skypilot-reclaim-proof'
_DATABASE_OWNER_APPLICATION_NAME = 'skypilot-reclaim-proof-owner'
_PROVIDER_PROOF_MAX_JSON_DEPTH = 32

if (_DATABASE_IDLE_TRANSACTION_TIMEOUT_MILLISECONDS
        <= reclaim.POLICY_OPERATION_TIMEOUT_SECONDS * 1000):
    raise RuntimeError('The provider-proof idle transaction timeout must cover '
                       'the complete outer policy horizon.')


class ReclaimProviderProofError(RuntimeError):
    """A completed provider proof could not be read or published safely."""


class _ReclaimProviderProofClockError(ReclaimProviderProofError):
    """A database/local clock relationship cannot authorize a refresh."""


@dataclasses.dataclass(frozen=True)
class ReclaimProviderProofCandidate:
    """One complete context proof and its oldest domain completion."""

    proof_payload: Mapping[str, Any]
    oldest_completed_monotonic: float

    def __post_init__(self) -> None:
        if not isinstance(self.proof_payload, Mapping):
            raise ValueError('proof_payload must be a mapping.')
        completed = self.oldest_completed_monotonic
        if (isinstance(completed, bool) or
                not isinstance(completed, (int, float)) or
                not math.isfinite(float(completed)) or completed < 0):
            raise ValueError(
                'oldest_completed_monotonic must be finite and nonnegative.')


@dataclasses.dataclass(frozen=True)
class ReclaimProviderProofReceipt:
    """One validated receipt and its safe machine-readable summary."""

    reference: reclaim.ReclaimProviderProofReference
    proof_payload: dict[str, Any]
    completed_at: datetime.datetime
    database_now: datetime.datetime

    @property
    def is_fresh(self) -> bool:
        age = (self.database_now - self.completed_at).total_seconds()
        return age < reclaim.AUTHORIZATION_MAX_AGE_SECONDS

    @property
    def has_terminal_guard_reserve(self) -> bool:
        """Whether a caller can still enter the terminal authority guard."""
        # completed_monotonic is conservatively anchored to the beginning of
        # the database read that produced this receipt.  Unlike database_now,
        # this live comparison includes payload validation, transaction close,
        # physical connection close, and every other local handoff delay.
        age = time.monotonic() - self.reference.completed_monotonic
        return 0 <= age < (reclaim.AUTHORIZATION_MAX_AGE_SECONDS -
                           _TERMINAL_GUARD_RESERVE_SECONDS)


def _require_deadline(deadline_monotonic: float) -> float:
    if (isinstance(deadline_monotonic, bool) or
            not isinstance(deadline_monotonic, (int, float)) or
            not math.isfinite(float(deadline_monotonic)) or
            time.monotonic() >= float(deadline_monotonic)):
        raise ReclaimProviderProofError(
            'The provider-proof receipt deadline is invalid or expired.')
    return float(deadline_monotonic)


def provider_proof_deadline(deadline_monotonic: float) -> float:
    """Reserve transaction publication and physical-close time."""
    deadline = _require_deadline(deadline_monotonic)
    provider_deadline = deadline - _PROVIDER_PUBLICATION_RESERVE_SECONDS
    return _require_deadline(provider_deadline)


def _bound_database_socket(dbapi_connection: Any,
                           _connection_record: Any) -> None:
    """Put a hard client-side send/receive bound on one libpq session."""
    timeout_seconds, timeout_milliseconds = divmod(
        _DATABASE_SOCKET_TIMEOUT_MILLISECONDS, 1000)
    timeout = struct.pack('@ll', timeout_seconds, timeout_milliseconds * 1000)
    database_socket = socket.socket(fileno=dbapi_connection.fileno())
    try:
        database_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO,
                                   timeout)
        database_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO,
                                   timeout)
        if (database_socket.family in (socket.AF_INET, socket.AF_INET6) and
                hasattr(socket, 'TCP_USER_TIMEOUT')):
            database_socket.setsockopt(socket.IPPROTO_TCP,
                                       socket.TCP_USER_TIMEOUT,
                                       _DATABASE_SOCKET_TIMEOUT_MILLISECONDS)
    finally:
        # The wrapper borrows libpq's descriptor; libpq remains its sole owner.
        database_socket.detach()


def _require_json_value(value: Any, subject: str, *, depth: int = 0) -> None:
    if depth > _PROVIDER_PROOF_MAX_JSON_DEPTH:
        raise ReclaimProviderProofError(
            f'{subject} exceeds the maximum JSON nesting depth.')
    if value is None or type(value) in (bool, str, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ReclaimProviderProofError(
                f'{subject} contains non-finite numeric data.')
        return
    if type(value) in (list, tuple):
        for item in value:
            _require_json_value(item, subject, depth=depth + 1)
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise ReclaimProviderProofError(
                    f'{subject} contains a non-text object key.')
            _require_json_value(item, subject, depth=depth + 1)
        return
    raise ReclaimProviderProofError(f'{subject} contains a non-JSON value.')


def canonical_proof_payload(
        value: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    """Return the exact JSON-safe proof object and canonical digest."""
    if type(value) is not dict:
        raise ReclaimProviderProofError(
            'The provider proof summary must be an exact JSON object.')
    _require_json_value(value, 'The provider proof summary')
    try:
        encoded = json.dumps(value,
                             sort_keys=True,
                             separators=(',', ':'),
                             ensure_ascii=False,
                             allow_nan=False).encode('utf-8')
        if len(encoded) > PROVIDER_PROOF_PAYLOAD_MAX_BYTES:
            raise ReclaimProviderProofError(
                'The provider proof summary exceeds the canonical JSON byte '
                'limit.')
        normalized = json.loads(encoded)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ReclaimProviderProofError(
            'The provider proof summary is not canonical JSON.') from error
    if type(normalized) is not dict:
        raise ReclaimProviderProofError(
            'The provider proof summary must be a JSON object.')
    return normalized, hashlib.sha256(encoded).hexdigest()


def _require_database_time(value: Any, subject: str) -> datetime.datetime:
    if (not isinstance(value, datetime.datetime) or value.tzinfo is None or
            value.utcoffset() is None):
        raise _ReclaimProviderProofClockError(
            f'{subject} is not a timezone-aware database timestamp.')
    return value


def _decode_receipt(
    row: Mapping[str, Any],
    *,
    expected_identity: reclaim.ReclaimPolicyIdentity,
    expected_gate_generation: int,
    expected_kubernetes_context: str,
    database_now: datetime.datetime,
    local_read_started: float,
    local_read_finished: float,
) -> ReclaimProviderProofReceipt:
    """Validate one durable row and conservatively map its completion time."""
    if not isinstance(row, Mapping):
        raise ReclaimProviderProofError('The provider proof row is malformed.')
    database_now = _require_database_time(database_now, 'Database clock')
    completed_at = _require_database_time(row.get('completed_at'),
                                          'Proof completion')
    database_age = (database_now - completed_at).total_seconds()
    round_trip = local_read_finished - local_read_started
    if not math.isfinite(database_age) or database_age < 0:
        raise _ReclaimProviderProofClockError(
            'The provider proof database clock is indeterminate.')
    if not math.isfinite(round_trip) or round_trip < 0:
        raise _ReclaimProviderProofClockError(
            'The provider proof local clock is indeterminate.')
    completed_monotonic = local_read_finished - database_age - round_trip

    try:
        identity = reclaim.ReclaimPolicyIdentity(
            fleet_bundle_sha256=row['reclaim_fleet_bundle_sha256'],
            policy_revision=row['reclaim_policy_revision'],
            provider_inventory_sha256=(
                row['reclaim_provider_inventory_sha256']))
        gate_generation = row['reconciliation_gate_generation']
        kubernetes_context = row['kubernetes_context']
        proof_schema_version = row['proof_schema_version']
        proof_payload, proof_sha256 = canonical_proof_payload(
            row['proof_payload'])
        reference = reclaim.ReclaimProviderProofReference(
            receipt_nonce=row['receipt_nonce'],
            proof_sha256=row['proof_sha256'],
            identity=identity,
            gate_generation=gate_generation,
            kubernetes_context=kubernetes_context,
            completed_monotonic=completed_monotonic)
    except (KeyError, TypeError, ValueError) as error:
        raise ReclaimProviderProofError(
            'The provider proof row has an invalid authority shape.') from error
    if (identity != expected_identity or
            gate_generation != expected_gate_generation or
            kubernetes_context != expected_kubernetes_context or
            proof_schema_version != PROVIDER_PROOF_SCHEMA_VERSION or
            proof_sha256 != reference.proof_sha256):
        raise ReclaimProviderProofError(
            'The provider proof row does not match exact authority.')
    return ReclaimProviderProofReceipt(reference=reference,
                                       proof_payload=proof_payload,
                                       completed_at=completed_at,
                                       database_now=database_now)


class ReclaimProviderProofRepository:
    """PostgreSQL-only completed-proof read and publication boundary."""

    def __init__(
        self,
        engine: sqlalchemy.engine.Engine | None = None,
    ) -> None:
        base_engine = (serve_state_schema.get_database_engine()
                       if engine is None else engine)
        if base_engine.dialect.name != 'postgresql':
            raise ValueError(
                'ReclaimProviderProofRepository is PostgreSQL-only.')
        existing_options = base_engine.url.query.get('options')
        if existing_options is not None and type(existing_options) is not str:
            raise ValueError(
                'ReclaimProviderProofRepository requires scalar PostgreSQL '
                'URL options.')
        timeout_options = (
            '-c statement_timeout='
            f'{_DATABASE_STATEMENT_TIMEOUT_MILLISECONDS}ms '
            '-c lock_timeout='
            f'{_DATABASE_STATEMENT_TIMEOUT_MILLISECONDS}ms '
            '-c idle_in_transaction_session_timeout='
            f'{_DATABASE_IDLE_TRANSACTION_TIMEOUT_MILLISECONDS}ms')
        if existing_options:
            timeout_options = f'{existing_options} {timeout_options}'
        self._proof_engine = db_utils.create_postgres_nullpool_engine(
            base_engine,
            engine_namespace='reserved-fill-reclaim-proof',
            pool_reset_on_return=None,
            connect_args={
                'connect_timeout': _DATABASE_CONNECT_TIMEOUT_SECONDS,
                'application_name': _DATABASE_APPLICATION_NAME,
                'options': timeout_options,
            })
        sqlalchemy.event.listen(self._proof_engine, 'connect',
                                _bound_database_socket)

    @staticmethod
    def _authority_lock_id(
        identity: reclaim.ReclaimPolicyIdentity,
        gate_generation: int,
        kubernetes_context: str,
    ) -> str:
        try:
            authority_hash = reclaim.reclaim_provider_proof_lock_id(
                identity, gate_generation, kubernetes_context)
        except ValueError as error:
            raise ReclaimProviderProofError(
                'The provider proof authority is invalid.') from error
        return f'{_PROVIDER_PROOF_LOCK_PREFIX}-{authority_hash}'

    @staticmethod
    def _payload_is_accepted(
        proof_payload: Mapping[str, Any],
        validate: Callable[[Mapping[str, Any]], bool],
    ) -> bool:
        try:
            accepted = validate(proof_payload)
        except Exception as error:  # pylint: disable=broad-except
            raise ReclaimProviderProofError(
                'The provider proof payload validator failed.') from error
        if type(accepted) is not bool:
            raise ReclaimProviderProofError(
                'The provider proof payload validator returned a non-boolean.')
        return accepted

    @staticmethod
    def _read_statement(
        identity: reclaim.ReclaimPolicyIdentity,
        gate_generation: int,
        kubernetes_context: str,
    ) -> sqlalchemy.sql.Select:
        table = (proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        candidates = sqlalchemy.select(table).where(
            table.c.reconciliation_gate_generation == gate_generation,
            table.c.reclaim_fleet_bundle_sha256 == identity.fleet_bundle_sha256,
            table.c.reclaim_policy_revision == identity.policy_revision,
            table.c.reclaim_provider_inventory_sha256 ==
            identity.provider_inventory_sha256,
            table.c.kubernetes_context == kubernetes_context).subquery()
        anchor = sqlalchemy.select(
            sqlalchemy.literal(1).label('_anchor')).subquery()
        # The anchor preserves one database-clock row on a miss; multiple
        # exact-authority rows remain multiple results and fail closed.
        return sqlalchemy.select(
            *(candidates.c[column.name] for column in table.columns),
            sqlalchemy.func.clock_timestamp().label(
                '_database_now')).select_from(
                    anchor.outerjoin(candidates, sqlalchemy.true()))

    def _decode_query_row(
        self,
        row: Mapping[str, Any] | None,
        *,
        identity: reclaim.ReclaimPolicyIdentity,
        gate_generation: int,
        kubernetes_context: str,
        local_read_started: float,
        local_read_finished: float,
    ) -> tuple[ReclaimProviderProofReceipt | None, datetime.datetime]:
        if row is None:
            raise ReclaimProviderProofError(
                'The provider proof read returned no database clock.')
        database_now = _require_database_time(row.get('_database_now'),
                                              'Database clock')
        round_trip = local_read_finished - local_read_started
        if not math.isfinite(round_trip) or round_trip < 0:
            raise _ReclaimProviderProofClockError(
                'The provider proof local clock is indeterminate.')
        if row.get('receipt_nonce') is None:
            return None, database_now
        try:
            receipt = _decode_receipt(
                row,
                expected_identity=identity,
                expected_gate_generation=gate_generation,
                expected_kubernetes_context=kubernetes_context,
                database_now=database_now,
                local_read_started=local_read_started,
                local_read_finished=local_read_finished)
        except _ReclaimProviderProofClockError:
            raise
        except ReclaimProviderProofError:
            return None, database_now
        return receipt, database_now

    def _read(
        self,
        *,
        identity: reclaim.ReclaimPolicyIdentity,
        gate_generation: int,
        kubernetes_context: str,
        deadline: float,
        connection: sqlalchemy.engine.Connection | None = None,
    ) -> tuple[ReclaimProviderProofReceipt | None, datetime.datetime, float]:
        statement = self._read_statement(identity, gate_generation,
                                         kubernetes_context)

        def _read_and_anchor(
            connection: sqlalchemy.engine.Connection,) -> Mapping[str, Any]:
            rows = connection.execute(statement).mappings().all()
            if len(rows) > 1:
                raise ReclaimProviderProofError(
                    'The provider proof authority has multiple rows.')
            if rows:
                return rows[0]
            raise ReclaimProviderProofError(
                'The provider proof read returned no database clock.')

        # A caller may use its remaining outer horizon for this bounded
        # operation. DisposableExecutor and its warden, rather than a
        # pessimistic libpq reserve, own the hard survivor boundary.
        _require_deadline(deadline)
        local_started = time.monotonic()
        if connection is None:
            # The receipt-owned NullPool engine creates one physical backend
            # for this read and closes it on every outcome. This avoids
            # retaining one ordinary QueuePool checkout per waiting process.
            with self._proof_engine.connect() as owned_connection:
                raw_row = _read_and_anchor(owned_connection)
        else:
            raw_row = _read_and_anchor(connection)
        local_finished = time.monotonic()
        receipt, database_now = self._decode_query_row(
            raw_row,
            identity=identity,
            gate_generation=gate_generation,
            kubernetes_context=kubernetes_context,
            local_read_started=local_started,
            local_read_finished=local_finished)
        _require_deadline(deadline)
        return receipt, database_now, local_finished

    def _publish(
        self,
        *,
        connection: sqlalchemy.engine.Connection,
        identity: reclaim.ReclaimPolicyIdentity,
        gate_generation: int,
        kubernetes_context: str,
        proof_payload: dict[str, Any],
        proof_sha256: str,
        completed_at: datetime.datetime,
        deadline: float,
    ) -> ReclaimProviderProofReceipt:
        table = (proof_schema.serve_reserved_fill_reclaim_provider_proofs_table)
        receipt_nonce = secrets.token_hex(32)
        values = {
            'receipt_nonce': receipt_nonce,
            'reconciliation_gate_generation': gate_generation,
            'reclaim_fleet_bundle_sha256': identity.fleet_bundle_sha256,
            'reclaim_policy_revision': identity.policy_revision,
            'reclaim_provider_inventory_sha256':
                (identity.provider_inventory_sha256),
            'kubernetes_context': kubernetes_context,
            'proof_schema_version': PROVIDER_PROOF_SCHEMA_VERSION,
            'proof_payload': proof_payload,
            'proof_sha256': proof_sha256,
            'completed_at': completed_at,
        }
        insert = postgresql.insert(table).values(**values)
        same_proof = sqlalchemy.and_(
            table.c.proof_schema_version ==
            insert.excluded.proof_schema_version,
            table.c.proof_sha256 == insert.excluded.proof_sha256,
            table.c.proof_payload == insert.excluded.proof_payload)
        statement = insert.on_conflict_do_update(
            constraint='serve054_reclaim_proof_authority_uq',
            set_={
                # An identical completed fact is a monotonic freshness renewal,
                # not a new authority.  Keeping its nonce prevents one launch's
                # refresh from revoking concurrently minted exact references.
                # Any schema or proof-content change still rotates the nonce and
                # makes every older reference fail closed.
                'receipt_nonce': sqlalchemy.case(
                    (same_proof, table.c.receipt_nonce),
                    else_=insert.excluded.receipt_nonce),
                'proof_schema_version': insert.excluded.proof_schema_version,
                'proof_payload': insert.excluded.proof_payload,
                'proof_sha256': insert.excluded.proof_sha256,
                'completed_at': insert.excluded.completed_at,
            }).returning(
                table,
                sqlalchemy.func.clock_timestamp().label('_database_now'))

        # Decode and verify RETURNING before commit. Any clock, digest, payload,
        # nonce, or authority uncertainty rolls back together with the exact
        # transaction advisory lock.
        _require_deadline(deadline)
        local_started = time.monotonic()
        raw_row = connection.execute(statement).mappings().one_or_none()
        local_finished = time.monotonic()
        if raw_row is None:
            raise ReclaimProviderProofError(
                'The provider proof publication returned no receipt.')
        receipt, _ = self._decode_query_row(
            raw_row,
            identity=identity,
            gate_generation=gate_generation,
            kubernetes_context=kubernetes_context,
            local_read_started=local_started,
            local_read_finished=local_finished)
        if (receipt is None or receipt.proof_payload != proof_payload or
                receipt.reference.proof_sha256 != proof_sha256 or
                receipt.completed_at != completed_at or
                not receipt.has_terminal_guard_reserve):
            raise ReclaimProviderProofError(
                'The published provider proof receipt is indeterminate.')
        _require_deadline(deadline)
        return receipt

    @staticmethod
    def _commit(transaction: sqlalchemy.engine.Transaction) -> None:
        """Commit through a narrow acknowledgement-ambiguity test seam."""
        transaction.commit()

    def _wait_for_published_receipt(
        self,
        *,
        identity: reclaim.ReclaimPolicyIdentity,
        gate_generation: int,
        kubernetes_context: str,
        deadline: float,
        validate: Callable[[Mapping[str, Any]], bool],
    ) -> ReclaimProviderProofReceipt:
        """Wait locally for one owner; never enter a lock-handoff convoy."""

        def _remaining_wait() -> float:
            remaining = deadline - time.monotonic()
            if not math.isfinite(remaining) or remaining <= 0:
                raise ReclaimProviderProofError(
                    'The provider-proof receipt was not published before its '
                    'deadline.')
            return remaining

        poll_seconds = _RECEIPT_POLL_INITIAL_SECONDS
        while True:
            remaining = _remaining_wait()
            # A full-to-1.25x exponential interval prevents independent
            # processes from synchronizing their short PostgreSQL reads.
            upper = min(poll_seconds * 1.25, remaining)
            lower = min(poll_seconds, upper)
            delay = lower + (upper - lower) * secrets.randbelow(1024) / 1024
            time.sleep(delay)
            _remaining_wait()
            try:
                waiting, _, _ = self._read(
                    identity=identity,
                    gate_generation=gate_generation,
                    kubernetes_context=kubernetes_context,
                    deadline=deadline)
            except _ReclaimProviderProofClockError:
                raise
            except ReclaimProviderProofError:
                waiting = None
            _remaining_wait()
            if (waiting is not None and self._payload_is_accepted(
                    waiting.proof_payload, validate) and
                    waiting.has_terminal_guard_reserve):
                _remaining_wait()
                return waiting
            poll_seconds = min(_RECEIPT_POLL_MAX_SECONDS, poll_seconds * 2)

    def _read_elect_and_maybe_publish(
        self,
        *,
        identity: reclaim.ReclaimPolicyIdentity,
        gate_generation: int,
        kubernetes_context: str,
        deadline: float,
        prove: Callable[[], ReclaimProviderProofCandidate],
        validate: Callable[[Mapping[str, Any]], bool],
    ) -> ReclaimProviderProofReceipt | None:
        """Use one transaction for exact read, election, and publication."""
        _require_deadline(deadline)
        selected: ReclaimProviderProofReceipt | None = None
        publish = False
        with self._proof_engine.connect() as connection:
            transaction = connection.begin()
            try:
                receipt, _, _ = self._read(
                    identity=identity,
                    gate_generation=gate_generation,
                    kubernetes_context=kubernetes_context,
                    deadline=deadline,
                    connection=connection)
                if (receipt is not None and self._payload_is_accepted(
                        receipt.proof_payload, validate) and
                        receipt.has_terminal_guard_reserve):
                    selected = receipt
                else:
                    _require_deadline(deadline)
                    acquired = connection.execute(
                        sqlalchemy.text(
                            'SELECT pg_catalog.pg_try_advisory_xact_lock('
                            ':lock_key)'), {
                                'lock_key': postgres_lock.postgres_lock_key(
                                    self._authority_lock_id(
                                        identity, gate_generation,
                                        kubernetes_context))
                            }).scalar_one()
                    if type(acquired) is not bool:
                        raise ReclaimProviderProofError(
                            'The provider proof election result is malformed.')
                    _require_deadline(deadline)
                    if acquired:
                        owner_application_name = connection.execute(
                            sqlalchemy.text(
                                'SELECT pg_catalog.set_config('
                                "'application_name', :application_name, true)"),
                            {
                                'application_name': _DATABASE_OWNER_APPLICATION_NAME
                            }).scalar_one()
                        if owner_application_name != (
                                _DATABASE_OWNER_APPLICATION_NAME):
                            raise ReclaimProviderProofError(
                                'The provider proof owner phase is '
                                'indeterminate.')
                        _require_deadline(deadline)
                        # READ COMMITTED reread closes a prior-owner commit
                        # between the first read and the nonblocking election.
                        existing, database_anchor, local_anchor = self._read(
                            identity=identity,
                            gate_generation=gate_generation,
                            kubernetes_context=kubernetes_context,
                            deadline=deadline,
                            connection=connection)
                        if (existing is not None and self._payload_is_accepted(
                                existing.proof_payload, validate) and
                                existing.has_terminal_guard_reserve):
                            selected = existing
                        else:
                            provider_deadline = provider_proof_deadline(
                                deadline)
                            candidate = prove()
                            proof_returned = time.monotonic()
                            if proof_returned >= provider_deadline:
                                raise ReclaimProviderProofError(
                                    'The provider proof consumed its reserved '
                                    'publication horizon.')
                            if not isinstance(candidate,
                                              ReclaimProviderProofCandidate):
                                raise ReclaimProviderProofError(
                                    'The provider proof candidate is untyped.')
                            oldest_completed = float(
                                candidate.oldest_completed_monotonic)
                            if (oldest_completed < local_anchor or
                                    oldest_completed > proof_returned):
                                raise ReclaimProviderProofError(
                                    'The provider proof completion time is '
                                    'outside its exact execution interval.')
                            proof_payload, proof_sha256 = canonical_proof_payload(
                                candidate.proof_payload)
                            if not self._payload_is_accepted(
                                    proof_payload, validate):
                                raise ReclaimProviderProofError(
                                    'The fresh provider proof payload is not '
                                    'exact.')
                            completed_at = database_anchor + datetime.timedelta(
                                seconds=oldest_completed - local_anchor)
                            selected = self._publish(
                                connection=connection,
                                identity=identity,
                                gate_generation=gate_generation,
                                kubernetes_context=kubernetes_context,
                                proof_payload=proof_payload,
                                proof_sha256=proof_sha256,
                                completed_at=completed_at,
                                deadline=deadline)
                            publish = True
                            _require_deadline(deadline)
                if publish:
                    self._commit(transaction)
                else:
                    transaction.rollback()
            except BaseException:
                if transaction.is_active:
                    try:
                        transaction.rollback()
                    except Exception:  # pylint: disable=broad-except
                        connection.invalidate()
                raise
        # Authorization is possible only after commit/rollback released the
        # transaction lock and NullPool physically closed the backend.
        _require_deadline(deadline)
        return selected

    def get_or_prove(
        self,
        *,
        identity: reclaim.ReclaimPolicyIdentity,
        gate_generation: int,
        kubernetes_context: str,
        deadline_monotonic: float,
        prove: Callable[[], ReclaimProviderProofCandidate],
        validate: Callable[[Mapping[str, Any]], bool],
    ) -> ReclaimProviderProofReceipt:
        """Read a fresh receipt or publish one proof under exact authority."""
        deadline = _require_deadline(deadline_monotonic)
        initial_remaining = deadline - time.monotonic()
        initial_jitter = min(_INITIAL_JITTER_MAX_SECONDS,
                             initial_remaining / 10)
        if initial_jitter > 0:
            time.sleep(initial_jitter * secrets.randbelow(1024) / 1024)
        while True:
            selected = self._read_elect_and_maybe_publish(
                identity=identity,
                gate_generation=gate_generation,
                kubernetes_context=kubernetes_context,
                deadline=deadline,
                prove=prove,
                validate=validate)
            _require_deadline(deadline)
            if selected is None:
                # A loser does not reacquire while the elected owner is still
                # proving. Its election transaction is physically gone before
                # these bounded, independently closed receipt reads begin.
                selected = self._wait_for_published_receipt(
                    identity=identity,
                    gate_generation=gate_generation,
                    kubernetes_context=kubernetes_context,
                    deadline=deadline,
                    validate=validate)
                _require_deadline(deadline)
            # This is the actual handoff boundary: validation and every
            # commit/rollback/physical-close delay have already elapsed. If
            # they consumed the reserve, re-enter election under the original
            # deadline and never expose the near-expiry receipt.
            if selected.has_terminal_guard_reserve:
                return selected


def provider_proof_reference_holds_in_connection(
    connection: sqlalchemy.engine.Connection,
    reference: reclaim.ReclaimProviderProofReference,
    *,
    expected_physical_cluster_uid: str,
) -> bool:
    """Validate one exact context receipt before a provider effect."""
    if connection.dialect.name != 'postgresql':
        return False
    if (type(expected_physical_cluster_uid) is not str or
            not expected_physical_cluster_uid):
        return False
    if not isinstance(reference, reclaim.ReclaimProviderProofReference):
        return False
    # READ COMMITTED gives each terminal statement a current committed MVCC
    # snapshot. A transaction-wide stale snapshot would not observe a proof
    # replacement that committed after an earlier Serve fence read.
    if connection.get_isolation_level().upper() != 'READ COMMITTED':
        return False
    table = proof_schema.serve_reserved_fill_reclaim_provider_proofs_table
    # One READ COMMITTED MVCC statement is the terminal proof linearization
    # point. A committed replacement is visible and rejects the old exact
    # nonce; an uncommitted replacement leaves the prior committed proof
    # visible and therefore orders this guard before that transition. A row
    # lock would add false failures without protecting the later provider
    # effect, because this transaction ends before that effect starts.
    row = connection.execute(
        sqlalchemy.select(
            table,
            sqlalchemy.func.clock_timestamp().label('_database_now')).where(
                table.c.receipt_nonce ==
                reference.receipt_nonce)).mappings().one_or_none()
    if row is None:
        return False
    database_now = row['_database_now']
    local_now = time.monotonic()
    reference_age = local_now - reference.completed_monotonic
    if (not math.isfinite(reference_age) or reference_age < 0 or
            reference_age >= reclaim.AUTHORIZATION_MAX_AGE_SECONDS):
        return False
    try:
        receipt = _decode_receipt(
            row,
            expected_identity=reference.identity,
            expected_gate_generation=reference.gate_generation,
            expected_kubernetes_context=reference.kubernetes_context,
            database_now=database_now,
            local_read_started=local_now,
            local_read_finished=local_now)
        kubernetes_summary = receipt.proof_payload.get('kubernetes')
        if (receipt.reference.receipt_nonce != reference.receipt_nonce or
                receipt.reference.proof_sha256 != reference.proof_sha256 or
                not receipt.is_fresh or
                not isinstance(kubernetes_summary, Mapping) or
                kubernetes_summary.get('physical_cluster_uid')
                != expected_physical_cluster_uid):
            return False
    except ReclaimProviderProofError:
        return False
    return True
