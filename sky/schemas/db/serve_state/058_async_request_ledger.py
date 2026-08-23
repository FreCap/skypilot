"""Add normalized PostgreSQL asynchronous request dispatch receipts.

Revision ID: 058
Revises: 057
Create Date: 2026-08-23

Logical request identity is separate from append-only attempts.  A retry adds a
new attempt row only after an exact pre-dispatch rejection.  Immutable S3
results and completion markers remain the result authority.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '058'
down_revision: str | Sequence[str] | None = '057'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REQUESTS = 'serve_async_requests'
_ATTEMPTS = 'serve_async_request_attempts'
_REQUEST_GUARD_FUNCTION = 'skyserve058_guard_async_request'
_REQUEST_GUARD_TRIGGER = 'skyserve058_async_request_guard'
_ATTEMPT_GUARD_FUNCTION = 'skyserve058_guard_async_attempt'
_ATTEMPT_GUARD_TRIGGER = 'skyserve058_async_attempt_guard'
_DELETE_GUARD_FUNCTION = 'skyserve058_forbid_async_ledger_delete'
_REQUEST_DELETE_TRIGGER = 'skyserve058_async_request_delete_guard'
_ATTEMPT_DELETE_TRIGGER = 'skyserve058_async_attempt_delete_guard'
_CONSISTENCY_FUNCTION = 'skyserve058_check_async_ledger_consistency'
_REQUEST_CONSISTENCY_TRIGGER = 'skyserve058_async_request_consistency'
_ATTEMPT_CONSISTENCY_TRIGGER = 'skyserve058_async_attempt_consistency'
_TERMINAL_STATES = "'SUCCEEDED', 'FAILED', 'CANCELLED', 'EXPIRED'"


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'The asynchronous request ledger is PostgreSQL-only.')


def _install_request_guard() -> None:
    op.execute(f'''
        CREATE FUNCTION {_REQUEST_GUARD_FUNCTION}() RETURNS trigger
        AS $function$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.current_attempt_no <> 1 THEN
                    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                        'asynchronous request must start at attempt one';
                END IF;
                RETURN NEW;
            END IF;
            IF NEW.service_name IS DISTINCT FROM OLD.service_name OR
               NEW.service_hash IS DISTINCT FROM OLD.service_hash OR
               NEW.request_key_sha256 IS DISTINCT FROM
                    OLD.request_key_sha256 OR
               NEW.intent_sha256 IS DISTINCT FROM OLD.intent_sha256 OR
               NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'asynchronous request identity is immutable';
            END IF;
            IF NEW.current_attempt_id IS NOT DISTINCT FROM
                    OLD.current_attempt_id OR
               NEW.current_attempt_no <> OLD.current_attempt_no + 1 OR
               NEW.updated_at < OLD.updated_at THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'asynchronous current attempt did not advance exactly';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM {_ATTEMPTS}
                 WHERE service_name = OLD.service_name
                   AND service_hash = OLD.service_hash
                   AND request_key_sha256 = OLD.request_key_sha256
                   AND attempt_id = OLD.current_attempt_id
                   AND state = 'REJECTED_PRE_DISPATCH'
            ) THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'asynchronous prior attempt does not authorize replay';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM {_ATTEMPTS}
                 WHERE service_name = NEW.service_name
                   AND service_hash = NEW.service_hash
                   AND request_key_sha256 = NEW.request_key_sha256
                   AND attempt_id = NEW.current_attempt_id
                   AND attempt_no = NEW.current_attempt_no
                   AND state = 'DISPATCH_MAY_HAVE_OCCURRED'
                   AND revision = 1
            ) THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'asynchronous successor attempt is not an initial bind';
            END IF;
            RETURN NEW;
        END;
        $function$
        LANGUAGE plpgsql
    ''')
    op.execute(f'''
        CREATE TRIGGER {_REQUEST_GUARD_TRIGGER}
        BEFORE INSERT OR UPDATE ON {_REQUESTS}
        FOR EACH ROW EXECUTE FUNCTION {_REQUEST_GUARD_FUNCTION}()
    ''')


def _install_attempt_guard() -> None:
    op.execute(f'''
        CREATE FUNCTION {_ATTEMPT_GUARD_FUNCTION}() RETURNS trigger
        AS $function$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.revision <> 1 OR
                   NEW.accepted_at IS NOT NULL OR
                   NEW.terminal_at IS NOT NULL OR
                   NEW.terminal_status IS NOT NULL OR
                   NEW.processing_time_us IS NOT NULL OR
                   NOT (
                       (NEW.state = 'REJECTED_PRE_DISPATCH' AND
                        NEW.dispatch_binding IS NULL) OR
                       (NEW.state = 'DISPATCH_MAY_HAVE_OCCURRED' AND
                        NEW.dispatch_binding IS NOT NULL)
                   ) THEN
                    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                        'asynchronous attempt has an invalid initial state';
                END IF;
                RETURN NEW;
            END IF;
            IF NEW.service_name IS DISTINCT FROM OLD.service_name OR
               NEW.service_hash IS DISTINCT FROM OLD.service_hash OR
               NEW.request_key_sha256 IS DISTINCT FROM
                    OLD.request_key_sha256 OR
               NEW.attempt_id IS DISTINCT FROM OLD.attempt_id OR
               NEW.attempt_no IS DISTINCT FROM OLD.attempt_no OR
               NEW.dispatch_binding IS DISTINCT FROM OLD.dispatch_binding OR
               NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'asynchronous attempt identity is immutable';
            END IF;
            IF NEW.revision <> OLD.revision + 1 OR
               NEW.updated_at < OLD.updated_at THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'asynchronous attempt revision is not monotonic';
            END IF;
            IF OLD.accepted_at IS NOT NULL AND
               NEW.accepted_at IS DISTINCT FROM OLD.accepted_at THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'asynchronous acceptance is immutable';
            END IF;
            IF OLD.terminal_at IS NOT NULL AND
               (NEW.terminal_at IS DISTINCT FROM OLD.terminal_at OR
                NEW.terminal_status IS DISTINCT FROM OLD.terminal_status OR
                NEW.processing_time_us IS DISTINCT FROM
                    OLD.processing_time_us) THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'asynchronous terminal receipt is immutable';
            END IF;

            IF OLD.state = 'DISPATCH_MAY_HAVE_OCCURRED' AND
               NEW.state = 'ACCEPTED' THEN
                IF NEW.accepted_at IS NULL OR
                   NEW.terminal_at IS NOT NULL THEN
                    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                        'invalid asynchronous acceptance';
                END IF;
            ELSIF OLD.state = 'DISPATCH_MAY_HAVE_OCCURRED' AND
                  NEW.state IN ('AMBIGUOUS', 'REJECTED_PRE_DISPATCH') THEN
                IF NEW.accepted_at IS DISTINCT FROM OLD.accepted_at OR
                   NEW.terminal_at IS NOT NULL THEN
                    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                        'invalid asynchronous pre-dispatch transition';
                END IF;
            ELSIF OLD.state = 'ACCEPTED' AND NEW.state = 'AMBIGUOUS' THEN
                IF NEW.accepted_at IS DISTINCT FROM OLD.accepted_at OR
                   NEW.terminal_at IS NOT NULL THEN
                    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                        'invalid asynchronous ambiguous transition';
                END IF;
            ELSIF OLD.state IN ('DISPATCH_MAY_HAVE_OCCURRED', 'ACCEPTED',
                                'AMBIGUOUS') AND
                  NEW.state IN ({_TERMINAL_STATES}) THEN
                IF NEW.accepted_at IS NULL OR NEW.terminal_at IS NULL OR
                   NEW.terminal_status IS DISTINCT FROM NEW.state OR
                   NEW.processing_time_us IS NULL THEN
                    RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                        'invalid asynchronous terminal transition';
                END IF;
            ELSE
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'invalid asynchronous attempt state transition';
            END IF;
            RETURN NEW;
        END;
        $function$
        LANGUAGE plpgsql
    ''')
    op.execute(f'''
        CREATE TRIGGER {_ATTEMPT_GUARD_TRIGGER}
        BEFORE INSERT OR UPDATE ON {_ATTEMPTS}
        FOR EACH ROW EXECUTE FUNCTION {_ATTEMPT_GUARD_FUNCTION}()
    ''')


def _install_append_only_guards() -> None:
    op.execute(f'''
        CREATE FUNCTION {_DELETE_GUARD_FUNCTION}() RETURNS trigger
        AS $function$
        BEGIN
            RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                'asynchronous ledger rows are append-only';
            RETURN OLD;
        END;
        $function$
        LANGUAGE plpgsql
    ''')
    op.execute(f'''
        CREATE TRIGGER {_REQUEST_DELETE_TRIGGER}
        BEFORE DELETE ON {_REQUESTS}
        FOR EACH ROW EXECUTE FUNCTION {_DELETE_GUARD_FUNCTION}()
    ''')
    op.execute(f'''
        CREATE TRIGGER {_ATTEMPT_DELETE_TRIGGER}
        BEFORE DELETE ON {_ATTEMPTS}
        FOR EACH ROW EXECUTE FUNCTION {_DELETE_GUARD_FUNCTION}()
    ''')


def _install_deferred_consistency_guards() -> None:
    op.execute(f'''
        CREATE FUNCTION {_CONSISTENCY_FUNCTION}() RETURNS trigger
        AS $function$
        DECLARE
            request_row {_REQUESTS}%ROWTYPE;
            attempt_count bigint;
            invalid_attempt_count bigint;
            current_match_count bigint;
        BEGIN
            SELECT * INTO request_row
              FROM {_REQUESTS}
             WHERE service_name = NEW.service_name
               AND service_hash = NEW.service_hash
               AND request_key_sha256 = NEW.request_key_sha256;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'asynchronous attempt has no logical request';
            END IF;
            SELECT count(*),
                   count(*) FILTER (
                       WHERE attempt_no > request_row.current_attempt_no),
                   count(*) FILTER (
                       WHERE attempt_id = request_row.current_attempt_id AND
                             attempt_no = request_row.current_attempt_no)
              INTO attempt_count, invalid_attempt_count, current_match_count
              FROM {_ATTEMPTS}
             WHERE service_name = NEW.service_name
               AND service_hash = NEW.service_hash
               AND request_key_sha256 = NEW.request_key_sha256;
            IF attempt_count <> request_row.current_attempt_no OR
               invalid_attempt_count <> 0 OR current_match_count <> 1 THEN
                RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
                    'asynchronous attempt sequence or pointer is inconsistent';
            END IF;
            RETURN NULL;
        END;
        $function$
        LANGUAGE plpgsql
    ''')
    op.execute(f'''
        CREATE CONSTRAINT TRIGGER {_REQUEST_CONSISTENCY_TRIGGER}
        AFTER INSERT OR UPDATE ON {_REQUESTS}
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION {_CONSISTENCY_FUNCTION}()
    ''')
    op.execute(f'''
        CREATE CONSTRAINT TRIGGER {_ATTEMPT_CONSISTENCY_TRIGGER}
        AFTER INSERT OR UPDATE ON {_ATTEMPTS}
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION {_CONSISTENCY_FUNCTION}()
    ''')


def upgrade() -> None:
    """Create the normalized dispatch/replay authority."""
    _require_postgresql()
    op.create_table(
        _REQUESTS, sa.Column('service_name', sa.Text(), primary_key=True),
        sa.Column('service_hash', sa.Text(), primary_key=True),
        sa.Column('request_key_sha256', sa.Text(), primary_key=True),
        sa.Column('intent_sha256', sa.Text(), nullable=False),
        sa.Column('current_attempt_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('current_attempt_no', sa.Integer(), nullable=False),
        sa.Column('created_at',
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.text('clock_timestamp()')),
        sa.Column('updated_at',
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.text('clock_timestamp()')),
        sa.CheckConstraint(
            "request_key_sha256 ~ '^[0-9a-f]{64}$' AND "
            "intent_sha256 ~ '^[0-9a-f]{64}$'",
            name='serve058_async_request_digest_ck'),
        sa.CheckConstraint(
            'current_attempt_no > 0 AND updated_at >= created_at AND '
            'octet_length(service_name) BETWEEN 1 AND 512 AND '
            'octet_length(service_hash) BETWEEN 1 AND 512',
            name='serve058_async_request_identity_ck'))
    op.create_table(
        _ATTEMPTS, sa.Column('service_name', sa.Text(), primary_key=True),
        sa.Column('service_hash', sa.Text(), primary_key=True),
        sa.Column('request_key_sha256', sa.Text(), primary_key=True),
        sa.Column('attempt_id', sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column('attempt_no', sa.Integer(), nullable=False),
        sa.Column('state', sa.Text(), nullable=False),
        sa.Column('revision', sa.BigInteger(), nullable=False),
        sa.Column('dispatch_binding', postgresql.JSONB(none_as_null=True)),
        sa.Column('accepted_at', sa.DateTime(timezone=True)),
        sa.Column('terminal_at', sa.DateTime(timezone=True)),
        sa.Column('terminal_status', sa.Text()),
        sa.Column('processing_time_us', sa.BigInteger()),
        sa.Column('created_at',
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.text('clock_timestamp()')),
        sa.Column('updated_at',
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.text('clock_timestamp()')),
        sa.ForeignKeyConstraint(
            ['service_name', 'service_hash', 'request_key_sha256'], [
                f'{_REQUESTS}.service_name', f'{_REQUESTS}.service_hash',
                f'{_REQUESTS}.request_key_sha256'
            ],
            name='serve058_async_attempt_request_fk',
            ondelete='RESTRICT',
            deferrable=True,
            initially='DEFERRED'),
        sa.UniqueConstraint('service_name',
                            'service_hash',
                            'request_key_sha256',
                            'attempt_no',
                            name='serve058_async_attempt_number_uq'),
        sa.CheckConstraint(
            'attempt_no > 0 AND revision > 0 AND updated_at >= created_at',
            name='serve058_async_attempt_positive_ck'),
        sa.CheckConstraint(
            "state IN ('REJECTED_PRE_DISPATCH', "
            "'DISPATCH_MAY_HAVE_OCCURRED', 'ACCEPTED', 'AMBIGUOUS', "
            "'SUCCEEDED', 'FAILED', 'CANCELLED', 'EXPIRED')",
            name='serve058_async_attempt_state_ck'),
        sa.CheckConstraint(
            "state = 'REJECTED_PRE_DISPATCH' OR dispatch_binding IS NOT NULL",
            name='serve058_async_attempt_binding_ck'),
        sa.CheckConstraint(
            f"(state IN ({_TERMINAL_STATES}) AND accepted_at IS NOT NULL AND "
            'terminal_at IS NOT NULL AND terminal_status = state AND '
            'processing_time_us IS NOT NULL AND processing_time_us >= 0) OR '
            f"(state NOT IN ({_TERMINAL_STATES}) AND terminal_at IS NULL AND "
            'terminal_status IS NULL AND processing_time_us IS NULL)',
            name='serve058_async_attempt_terminal_ck'),
        sa.CheckConstraint(
            'dispatch_binding IS NULL OR '
            "(jsonb_typeof(dispatch_binding) = 'object' AND "
            'octet_length(dispatch_binding::text) <= 8192 AND '
            "dispatch_binding ?& ARRAY['schema_version', "
            "'route_contract_service_version', "
            "'selected_worker_service_version', "
            "'route_projection_generation', 'route_projection_sha256', "
            "'route_source_epoch', 'replica_id', 'replica_record_id', "
            "'projected_accelerator', 'projected_accelerator_count', "
            "'is_zero_cost', 'location', 'worker_admission'] AND "
            "dispatch_binding - ARRAY['schema_version', "
            "'route_contract_service_version', "
            "'selected_worker_service_version', "
            "'route_projection_generation', 'route_projection_sha256', "
            "'route_source_epoch', 'replica_id', 'replica_record_id', "
            "'projected_accelerator', 'projected_accelerator_count', "
            "'is_zero_cost', 'location', "
            "'worker_admission'] = '{}'::jsonb)",
            name='serve058_async_attempt_dispatch_json_ck'))
    op.create_foreign_key(
        'serve058_async_request_current_attempt_fk',
        _REQUESTS,
        _ATTEMPTS, [
            'service_name', 'service_hash', 'request_key_sha256',
            'current_attempt_id'
        ], ['service_name', 'service_hash', 'request_key_sha256', 'attempt_id'],
        deferrable=True,
        initially='DEFERRED')
    op.create_index('serve058_async_attempt_state_idx', _ATTEMPTS,
                    ['service_name', 'service_hash', 'state', 'updated_at'])
    op.create_index('serve058_async_attempt_terminal_idx', _ATTEMPTS,
                    ['service_name', 'service_hash', 'terminal_at'])
    _install_request_guard()
    _install_attempt_guard()
    _install_append_only_guards()
    _install_deferred_consistency_guards()


def downgrade() -> None:
    """Preserve dispatch/replay receipts across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve058 is forward-only; request dispatch receipts are durable.')
