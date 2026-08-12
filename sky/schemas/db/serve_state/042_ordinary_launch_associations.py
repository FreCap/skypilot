"""Add the closed durable ordinary-launch association state machine.

Revision ID: 042
Revises: 041
Create Date: 2026-08-11

This migration intentionally owns its complete DDL.  Importing runtime Serve
modules here would make a historical migration change whenever application
metadata changes and would couple the independent Serve/API schema lineages.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

revision: str = '042'
down_revision: str | Sequence[str] | None = '041'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = 'serve_ordinary_launch_associations'
_SERVICES = 'services'
_REPLICAS = 'replicas'
_UNSETTLED = "'BOUND', 'CANCEL_REQUESTED', 'RESULT_RECORDED', 'AMBIGUOUS'"
_SETTLED = "'PROJECTED', 'PRE_EFFECT_TERMINAL'"
_DIGEST_VERSION = 'serve-bound-launch.v1'

_SERVICE_GUARD_FUNCTION = 'skyserve042_guard_service_binding'
_ASSOCIATION_GUARD_FUNCTION = 'skyserve042_guard_ordinary_association'
_REPLICA_GUARD_FUNCTION = 'skyserve042_guard_replica_binding'
_ASSOCIATION_CONSISTENCY_FUNCTION = (
    'skyserve042_check_association_consistency')
_REPLICA_CONSISTENCY_FUNCTION = 'skyserve042_check_replica_consistency'
_SERVICE_CONSISTENCY_FUNCTION = 'skyserve042_check_service_consistency'

_SERVICE_GUARD_TRIGGER = 'skyserve042_service_binding_guard'
_ASSOCIATION_GUARD_TRIGGER = 'skyserve042_association_guard'
_REPLICA_GUARD_TRIGGER = 'skyserve042_replica_binding_guard'
_ASSOCIATION_CONSISTENCY_TRIGGER = ('skyserve042_association_consistency')
_REPLICA_CONSISTENCY_TRIGGER = 'skyserve042_replica_consistency'
_SERVICE_CONSISTENCY_TRIGGER = 'skyserve042_service_consistency'


def _column_names(bind: sa.engine.Connection, table: str) -> set[str]:
    return {
        str(column['name']) for column in sa.inspect(bind).get_columns(table)
    }


def _constraint_names(bind: sa.engine.Connection, table: str) -> set[str]:
    inspector = sa.inspect(bind)
    names = {
        str(item['name'])
        for item in inspector.get_check_constraints(table)
        if item.get('name') is not None
    }
    names.update(
        str(item['name'])
        for item in inspector.get_foreign_keys(table)
        if item.get('name') is not None)
    return names


def _index_names(bind: sa.engine.Connection, table: str) -> set[str]:
    return {
        str(item['name'])
        for item in sa.inspect(bind).get_indexes(table)
        if item.get('name') is not None
    }


def _add_columns(bind: sa.engine.Connection) -> None:
    service_columns = {
        'controller_incarnation': sa.Column(
            'controller_incarnation',
            sa.Uuid(as_uuid=True),
            nullable=False,
            server_default=sa.text('gen_random_uuid()')),
        'controller_owner_epoch': sa.Column('controller_owner_epoch',
                                            sa.BigInteger(),
                                            nullable=False,
                                            server_default='1'),
        'ordinary_launch_binding_capable': sa.Column(
            'ordinary_launch_binding_capable',
            sa.Boolean(),
            nullable=False,
            server_default=sa.false()),
        'ordinary_launch_binding_mode': sa.Column(
            'ordinary_launch_binding_mode',
            sa.Text(),
            nullable=False,
            server_default='legacy'),
        'ordinary_launch_binding_epoch': sa.Column(
            'ordinary_launch_binding_epoch',
            sa.BigInteger(),
            nullable=False,
            server_default='0'),
    }
    existing = _column_names(bind, _SERVICES)
    for name, column in service_columns.items():
        if name not in existing:
            op.add_column(_SERVICES, column)
    if 'ordinary_launch_association_id' not in _column_names(bind, _REPLICAS):
        op.add_column(
            _REPLICAS,
            sa.Column('ordinary_launch_association_id',
                      sa.Uuid(as_uuid=True),
                      nullable=True))


def _create_association_table(bind: sa.engine.Connection) -> None:
    if sa.inspect(bind).has_table(_TABLE):
        return
    op.create_table(
        _TABLE,
        sa.Column('association_id', sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column('submission_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('tenant_scope', sa.Text(), nullable=False),
        sa.Column('service_name', sa.Text(), nullable=False),
        sa.Column('service_hash', sa.Text(), nullable=False),
        sa.Column('service_workspace', sa.Text(), nullable=False),
        sa.Column('service_lifecycle_epoch', sa.BigInteger(), nullable=False),
        sa.Column('service_binding_epoch', sa.BigInteger(), nullable=False),
        sa.Column('service_version', sa.Integer(), nullable=False),
        sa.Column('replica_id', sa.Integer(), nullable=False),
        sa.Column('replica_record_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('paid_capacity_pool_key', sa.Text()),
        sa.Column('launch_generation', sa.BigInteger(), nullable=False),
        sa.Column('cluster_name', sa.Text(), nullable=False),
        sa.Column('request_id', sa.Text(), nullable=False),
        sa.Column('input_digest', sa.Text(), nullable=False),
        sa.Column('digest_version',
                  sa.Text(),
                  nullable=False,
                  server_default=_DIGEST_VERSION),
        sa.Column('owner_controller_incarnation',
                  sa.Uuid(as_uuid=True),
                  nullable=False),
        sa.Column('owner_controller_epoch', sa.BigInteger(), nullable=False),
        sa.Column('owner_revision',
                  sa.BigInteger(),
                  nullable=False,
                  server_default='1'),
        sa.Column('owner_transferred_at', sa.DateTime(timezone=True)),
        sa.Column('effect_phase',
                  sa.Text(),
                  nullable=False,
                  server_default='NOT_STARTED'),
        sa.Column('effect_phase_changed_at',
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.text('clock_timestamp()')),
        sa.Column('resolution',
                  sa.Text(),
                  nullable=False,
                  server_default='BOUND'),
        sa.Column('cancel_reason', sa.Text()),
        sa.Column('cancel_requested_at', sa.DateTime(timezone=True)),
        sa.Column('terminal_status', sa.Text()),
        sa.Column('terminal_cause', sa.Text()),
        sa.Column('terminal_execution_generation', sa.BigInteger()),
        sa.Column('execution_quiescence_required', sa.Boolean()),
        sa.Column('execution_quiesced_generation', sa.BigInteger()),
        sa.Column('execution_quiesced_at', sa.DateTime(timezone=True)),
        sa.Column('service_job_id', sa.BigInteger()),
        sa.Column('result_recorded_at', sa.DateTime(timezone=True)),
        sa.Column('ambiguity_code', sa.Text()),
        sa.Column('projected_at', sa.DateTime(timezone=True)),
        sa.Column('pin_released_at', sa.DateTime(timezone=True)),
        sa.Column('tombstone_not_before', sa.DateTime(timezone=True)),
        sa.Column('created_at',
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.text('clock_timestamp()')),
        sa.Column('updated_at',
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.text('clock_timestamp()')),
        sa.CheckConstraint('length(tenant_scope) > 0',
                           name='serve_ordinary_binding_tenant_scope'),
        sa.CheckConstraint('length(service_name) > 0',
                           name='serve_ordinary_binding_service_name'),
        sa.CheckConstraint('length(service_hash) > 0',
                           name='serve_ordinary_binding_service_hash'),
        sa.CheckConstraint('length(service_workspace) > 0',
                           name='serve_ordinary_binding_workspace'),
        sa.CheckConstraint('service_lifecycle_epoch > 0',
                           name='serve_ordinary_binding_lifecycle_epoch'),
        sa.CheckConstraint('service_binding_epoch > 0',
                           name='serve_ordinary_binding_binding_epoch'),
        sa.CheckConstraint('service_version > 0',
                           name='serve_ordinary_binding_service_version'),
        sa.CheckConstraint('replica_id > 0',
                           name='serve_ordinary_binding_replica_id'),
        sa.CheckConstraint(
            'paid_capacity_pool_key IS NULL OR '
            'length(paid_capacity_pool_key) > 0',
            name='serve_ordinary_binding_paid_pool'),
        sa.CheckConstraint('launch_generation > 0',
                           name='serve_ordinary_binding_generation'),
        sa.CheckConstraint('length(cluster_name) > 0',
                           name='serve_ordinary_binding_cluster_name'),
        sa.CheckConstraint('length(request_id) > 0',
                           name='serve_ordinary_binding_request_id'),
        sa.CheckConstraint("input_digest ~ '^[0-9a-f]{64}$'",
                           name='serve_ordinary_binding_input_digest'),
        sa.CheckConstraint(f"digest_version = '{_DIGEST_VERSION}'",
                           name='serve_ordinary_binding_digest_version'),
        sa.CheckConstraint('owner_controller_epoch > 0',
                           name='serve_ordinary_binding_owner_epoch'),
        sa.CheckConstraint('owner_revision > 0',
                           name='serve_ordinary_binding_owner_revision'),
        sa.CheckConstraint(
            "effect_phase IN ('NOT_STARTED', 'PROVIDER_IO', "
            "'SERVICE_JOB_IO', 'SERVICE_JOB_RECORDED')",
            name='serve_ordinary_binding_effect_phase'),
        sa.CheckConstraint(
            "resolution IN ('BOUND', 'CANCEL_REQUESTED', "
            "'RESULT_RECORDED', 'PROJECTED', 'PRE_EFFECT_TERMINAL', "
            "'AMBIGUOUS')",
            name='serve_ordinary_binding_resolution'),
        sa.CheckConstraint(
            "terminal_status IS NULL OR terminal_status IN "
            "('SUCCEEDED', 'FAILED', 'CANCELLED')",
            name='serve_ordinary_binding_terminal_status'),
        sa.CheckConstraint(
            'terminal_execution_generation IS NULL OR '
            'terminal_execution_generation >= 0',
            name='serve_ordinary_binding_terminal_generation'),
        sa.CheckConstraint(
            'execution_quiesced_generation IS NULL OR '
            'execution_quiesced_generation >= 0',
            name='serve_ordinary_binding_quiesced_generation'),
        sa.CheckConstraint('service_job_id IS NULL OR service_job_id > 0',
                           name='serve_ordinary_binding_service_job_id'),
        sa.CheckConstraint(
            "(resolution = 'AMBIGUOUS') = (ambiguity_code IS NOT NULL)",
            name='serve_ordinary_binding_ambiguity'),
        sa.CheckConstraint(
            "resolution <> 'CANCEL_REQUESTED' OR "
            '(cancel_reason IS NOT NULL AND cancel_requested_at IS NOT NULL)',
            name='serve_ordinary_binding_cancel'),
        sa.CheckConstraint(
            '(cancel_reason IS NULL) = (cancel_requested_at IS NULL)',
            name='serve_ordinary_binding_cancel_pair'),
        sa.CheckConstraint(
            "(effect_phase = 'SERVICE_JOB_RECORDED') = "
            '(service_job_id IS NOT NULL)',
            name='serve_ordinary_binding_service_job'),
        sa.CheckConstraint(
            "resolution NOT IN ('RESULT_RECORDED', 'PROJECTED') OR "
            "effect_phase = 'SERVICE_JOB_RECORDED'",
            name='serve_ordinary_binding_result_effect'),
        sa.CheckConstraint(
            "resolution NOT IN ('RESULT_RECORDED', 'PROJECTED', "
            "'PRE_EFFECT_TERMINAL') OR "
            '(terminal_status IS NOT NULL AND '
            'terminal_execution_generation IS NOT NULL AND '
            'execution_quiescence_required IS NOT NULL)',
            name='serve_ordinary_binding_terminal_evidence'),
        sa.CheckConstraint(
            '(execution_quiescence_required IS DISTINCT FROM TRUE) OR '
            '(execution_quiesced_generation = terminal_execution_generation '
            'AND execution_quiesced_at IS NOT NULL)',
            name='serve_ordinary_binding_quiescence'),
        sa.CheckConstraint(
            "resolution <> 'PRE_EFFECT_TERMINAL' OR "
            "effect_phase = 'NOT_STARTED'",
            name='serve_ordinary_binding_pre_effect'),
        sa.CheckConstraint(
            "resolution NOT IN ('PROJECTED', 'PRE_EFFECT_TERMINAL') OR "
            '(projected_at IS NOT NULL AND pin_released_at IS NOT NULL AND '
            'tombstone_not_before IS NOT NULL)',
            name='serve_ordinary_binding_projection'),
        sa.CheckConstraint(
            "pin_released_at IS NULL OR resolution IN "
            "('PROJECTED', 'PRE_EFFECT_TERMINAL')",
            name='serve_ordinary_binding_pin_release'),
    )


def _create_constraints_and_indexes(bind: sa.engine.Connection) -> None:
    constraints = _constraint_names(bind, _SERVICES)
    service_checks = {
        'serve042_controller_owner_epoch_ck': 'controller_owner_epoch > 0',
        'serve042_binding_mode_ck': "ordinary_launch_binding_mode IN ('legacy', 'bound')",
        'serve042_binding_epoch_ck': 'ordinary_launch_binding_epoch >= 0',
        'serve042_bound_service_ck':
            "ordinary_launch_binding_mode <> 'bound' OR "
            '(pool = 0 AND ordinary_launch_binding_capable AND '
            'ordinary_launch_binding_epoch > 0 AND workspace IS NOT NULL '
            "AND length(workspace) > 0)",
    }
    for name, expression in service_checks.items():
        if name not in constraints:
            op.create_check_constraint(name, _SERVICES, expression)

    constraints = _constraint_names(bind, _REPLICAS)
    if 'fk_replicas_ordinary_launch_association' not in constraints:
        op.create_foreign_key('fk_replicas_ordinary_launch_association',
                              _REPLICAS,
                              _TABLE, ['ordinary_launch_association_id'],
                              ['association_id'],
                              ondelete='RESTRICT')

    indexes = _index_names(bind, _TABLE)
    definitions = (
        ('uq_serve_ordinary_binding_submission',
         ['tenant_scope', 'service_workspace', 'submission_id'], True, None),
        ('uq_serve_ordinary_binding_request', ['request_id'], True, None),
        ('uq_serve_ordinary_binding_generation',
         ['service_name', 'replica_record_id',
          'launch_generation'], True, None),
        ('uq_serve_ordinary_binding_unsettled',
         ['service_name',
          'replica_record_id'], True, sa.text(f'resolution IN ({_UNSETTLED})')),
        ('ix_serve_ordinary_binding_replica',
         ['service_name', 'replica_id', 'created_at'], False, None),
        ('ix_serve_ordinary_binding_gc', ['tombstone_not_before'], False,
         sa.text(f'resolution IN ({_SETTLED})')),
    )
    for name, columns, unique, predicate in definitions:
        if name not in indexes:
            op.create_index(name,
                            _TABLE,
                            columns,
                            unique=unique,
                            postgresql_where=predicate)


def _install_guards() -> None:
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_SERVICE_GUARD_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF EXISTS (
                    SELECT 1 FROM {_TABLE} AS association
                    WHERE association.service_name = OLD.name
                      AND association.resolution IN ({_UNSETTLED})
                ) THEN
                    RAISE EXCEPTION
                        'unresolved ordinary-launch associations block service deletion';
                END IF;
                RETURN OLD;
            END IF;

            IF NEW.controller_owner_epoch < OLD.controller_owner_epoch THEN
                RAISE EXCEPTION 'ordinary-launch controller owner epoch regressed';
            END IF;
            IF NEW.controller_incarnation IS DISTINCT FROM
                    OLD.controller_incarnation THEN
                IF NEW.controller_owner_epoch <> OLD.controller_owner_epoch + 1 THEN
                    RAISE EXCEPTION
                        'controller incarnation change requires one owner-epoch advance';
                END IF;
            ELSIF NEW.controller_owner_epoch <> OLD.controller_owner_epoch THEN
                RAISE EXCEPTION
                    'controller owner epoch requires a fresh incarnation';
            END IF;
            IF NEW.ordinary_launch_binding_capable IS DISTINCT FROM
                    OLD.ordinary_launch_binding_capable
               AND NEW.controller_incarnation IS NOT DISTINCT FROM
                    OLD.controller_incarnation THEN
                RAISE EXCEPTION
                    'ordinary-launch capability is bound to controller incarnation';
            END IF;
            IF NEW.ordinary_launch_binding_mode IS DISTINCT FROM
                    OLD.ordinary_launch_binding_mode THEN
                IF NEW.ordinary_launch_binding_epoch <>
                        OLD.ordinary_launch_binding_epoch + 1 THEN
                    RAISE EXCEPTION
                        'binding mode change requires one binding-epoch advance';
                END IF;
            ELSIF NEW.ordinary_launch_binding_epoch <>
                    OLD.ordinary_launch_binding_epoch THEN
                RAISE EXCEPTION
                    'binding epoch may advance only with a mode transition';
            END IF;
            IF NEW.ordinary_launch_binding_mode = 'bound' AND
                    (NOT NEW.ordinary_launch_binding_capable OR NEW.pool <> 0 OR
                     NEW.workspace IS NULL OR length(NEW.workspace) = 0) THEN
                RAISE EXCEPTION 'bound ordinary-launch service is incapable';
            END IF;
            IF OLD.ordinary_launch_binding_mode = 'bound' AND
                    (NEW.controller_pid IS DISTINCT FROM OLD.controller_pid OR
                     NEW.controller_ip IS DISTINCT FROM OLD.controller_ip) AND
                    NEW.controller_incarnation IS NOT DISTINCT FROM
                        OLD.controller_incarnation THEN
                RAISE EXCEPTION
                    'bound routing owner change requires a fresh incarnation';
            END IF;
            RETURN NEW;
        END;
        $function$
    """)
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_ASSOCIATION_GUARD_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        DECLARE
            service_owner RECORD;
            old_effect_rank integer;
            new_effect_rank integer;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF OLD.resolution IN ({_UNSETTLED}) THEN
                    RAISE EXCEPTION
                        'unresolved ordinary-launch association cannot be deleted';
                END IF;
                IF OLD.pin_released_at IS NULL OR
                   OLD.tombstone_not_before IS NULL OR
                   OLD.tombstone_not_before > clock_timestamp() OR EXISTS (
                       SELECT 1 FROM {_REPLICAS} AS replica
                       WHERE replica.ordinary_launch_association_id =
                             OLD.association_id
                   ) THEN
                    RAISE EXCEPTION
                        'ordinary-launch tombstone retention is not satisfied';
                END IF;
                RETURN OLD;
            END IF;

            SELECT service.* INTO service_owner
            FROM {_SERVICES} AS service
            WHERE service.name = NEW.service_name;
            IF NOT FOUND OR service_owner.hash IS DISTINCT FROM NEW.service_hash
               OR service_owner.workspace IS DISTINCT FROM
                    NEW.service_workspace
               OR service_owner.lifecycle_epoch IS DISTINCT FROM
                    NEW.service_lifecycle_epoch
               OR service_owner.ordinary_launch_binding_mode <> 'bound'
               OR service_owner.ordinary_launch_binding_epoch IS DISTINCT FROM
                    NEW.service_binding_epoch
               OR NOT service_owner.ordinary_launch_binding_capable
               OR service_owner.controller_incarnation IS DISTINCT FROM
                    NEW.owner_controller_incarnation
               OR service_owner.controller_owner_epoch IS DISTINCT FROM
                    NEW.owner_controller_epoch THEN
                RAISE EXCEPTION
                    'ordinary-launch association does not match current service authority';
            END IF;

            IF TG_OP = 'INSERT' THEN
                IF NEW.owner_revision <> 1 OR NEW.effect_phase <> 'NOT_STARTED'
                   OR NEW.resolution <> 'BOUND' THEN
                    RAISE EXCEPTION
                        'ordinary-launch association has invalid initial state';
                END IF;
                RETURN NEW;
            END IF;

            IF NEW.association_id IS DISTINCT FROM OLD.association_id
               OR NEW.submission_id IS DISTINCT FROM OLD.submission_id
               OR NEW.tenant_scope IS DISTINCT FROM OLD.tenant_scope
               OR NEW.service_name IS DISTINCT FROM OLD.service_name
               OR NEW.service_hash IS DISTINCT FROM OLD.service_hash
               OR NEW.service_workspace IS DISTINCT FROM OLD.service_workspace
               OR NEW.service_lifecycle_epoch IS DISTINCT FROM
                    OLD.service_lifecycle_epoch
               OR NEW.service_binding_epoch IS DISTINCT FROM
                    OLD.service_binding_epoch
               OR NEW.service_version IS DISTINCT FROM OLD.service_version
               OR NEW.replica_id IS DISTINCT FROM OLD.replica_id
               OR NEW.replica_record_id IS DISTINCT FROM OLD.replica_record_id
               OR NEW.paid_capacity_pool_key IS DISTINCT FROM
                    OLD.paid_capacity_pool_key
               OR NEW.launch_generation IS DISTINCT FROM OLD.launch_generation
               OR NEW.cluster_name IS DISTINCT FROM OLD.cluster_name
               OR NEW.request_id IS DISTINCT FROM OLD.request_id
               OR NEW.input_digest IS DISTINCT FROM OLD.input_digest
               OR NEW.digest_version IS DISTINCT FROM OLD.digest_version
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION
                    'ordinary-launch association identity is immutable';
            END IF;
            IF NEW.owner_revision < OLD.owner_revision OR
                    NEW.owner_revision > OLD.owner_revision + 1 THEN
                RAISE EXCEPTION
                    'ordinary-launch owner revision is not monotonic';
            END IF;
            IF NEW IS DISTINCT FROM OLD AND
                    NEW.owner_revision = OLD.owner_revision THEN
                RAISE EXCEPTION
                    'ordinary-launch mutation requires owner revision advance';
            END IF;
            IF (NEW.owner_controller_incarnation IS DISTINCT FROM
                    OLD.owner_controller_incarnation OR
                NEW.owner_controller_epoch IS DISTINCT FROM
                    OLD.owner_controller_epoch) AND
                    NEW.owner_revision <> OLD.owner_revision + 1 THEN
                RAISE EXCEPTION
                    'ordinary-launch owner transfer requires revision advance';
            END IF;

            old_effect_rank := CASE OLD.effect_phase
                WHEN 'NOT_STARTED' THEN 0 WHEN 'PROVIDER_IO' THEN 1
                WHEN 'SERVICE_JOB_IO' THEN 2
                WHEN 'SERVICE_JOB_RECORDED' THEN 3 END;
            new_effect_rank := CASE NEW.effect_phase
                WHEN 'NOT_STARTED' THEN 0 WHEN 'PROVIDER_IO' THEN 1
                WHEN 'SERVICE_JOB_IO' THEN 2
                WHEN 'SERVICE_JOB_RECORDED' THEN 3 END;
            IF new_effect_rank < old_effect_rank OR
                    new_effect_rank > old_effect_rank + 1 THEN
                RAISE EXCEPTION
                    'ordinary-launch effect phase transition is illegal';
            END IF;
            IF NEW.effect_phase IS DISTINCT FROM OLD.effect_phase AND
                    NEW.effect_phase_changed_at IS NOT DISTINCT FROM
                        OLD.effect_phase_changed_at THEN
                RAISE EXCEPTION
                    'effect phase transition requires a database timestamp';
            END IF;
            IF OLD.service_job_id IS NOT NULL AND
                    NEW.service_job_id IS DISTINCT FROM OLD.service_job_id THEN
                RAISE EXCEPTION 'ordinary-launch service-job ID is immutable';
            END IF;
            IF OLD.cancel_reason IS NOT NULL AND
                    (NEW.cancel_reason IS DISTINCT FROM OLD.cancel_reason OR
                     NEW.cancel_requested_at IS DISTINCT FROM
                        OLD.cancel_requested_at) THEN
                RAISE EXCEPTION
                    'ordinary-launch cancellation intent is immutable';
            END IF;
            IF OLD.cancel_reason IS NULL AND NEW.cancel_reason IS NOT NULL AND
                    NOT (OLD.resolution = 'BOUND' AND
                         NEW.resolution = 'CANCEL_REQUESTED') THEN
                RAISE EXCEPTION
                    'ordinary-launch cancellation requires exact transition';
            END IF;
            IF OLD.terminal_status IS NOT NULL AND
                    (NEW.terminal_status IS DISTINCT FROM OLD.terminal_status OR
                     NEW.terminal_cause IS DISTINCT FROM OLD.terminal_cause OR
                     NEW.terminal_execution_generation IS DISTINCT FROM
                        OLD.terminal_execution_generation OR
                     NEW.execution_quiescence_required IS DISTINCT FROM
                        OLD.execution_quiescence_required OR
                     NEW.execution_quiesced_generation IS DISTINCT FROM
                        OLD.execution_quiesced_generation OR
                     NEW.execution_quiesced_at IS DISTINCT FROM
                        OLD.execution_quiesced_at) THEN
                RAISE EXCEPTION
                    'ordinary-launch terminal evidence is immutable';
            END IF;
            IF OLD.pin_released_at IS NOT NULL AND
                    NEW.pin_released_at IS DISTINCT FROM OLD.pin_released_at THEN
                RAISE EXCEPTION
                    'ordinary-launch pin-release evidence is immutable';
            END IF;
            IF OLD.tombstone_not_before IS NOT NULL AND
                    NEW.tombstone_not_before IS DISTINCT FROM
                        OLD.tombstone_not_before THEN
                RAISE EXCEPTION
                    'ordinary-launch tombstone deadline is immutable';
            END IF;

            IF NEW.resolution IS DISTINCT FROM OLD.resolution AND NOT (
                (OLD.resolution = 'BOUND' AND NEW.resolution IN
                    ('CANCEL_REQUESTED', 'RESULT_RECORDED',
                     'PRE_EFFECT_TERMINAL', 'AMBIGUOUS')) OR
                (OLD.resolution = 'CANCEL_REQUESTED' AND NEW.resolution IN
                    ('RESULT_RECORDED', 'PRE_EFFECT_TERMINAL', 'AMBIGUOUS')) OR
                (OLD.resolution = 'RESULT_RECORDED' AND NEW.resolution IN
                    ('PROJECTED', 'AMBIGUOUS'))
            ) THEN
                RAISE EXCEPTION
                    'ordinary-launch resolution transition is illegal';
            END IF;
            IF NEW.resolution IN ({_SETTLED}) AND
                    OLD.resolution NOT IN ({_SETTLED}) AND
                    NEW.tombstone_not_before <
                        transaction_timestamp() + INTERVAL '60 days' THEN
                RAISE EXCEPTION
                    'ordinary-launch tombstone retention is shorter than 60 days';
            END IF;
            RETURN NEW;
        END;
        $function$
    """)
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_REPLICA_GUARD_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        DECLARE association RECORD;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF OLD.ordinary_launch_association_id IS NOT NULL THEN
                    RAISE EXCEPTION
                        'bound ordinary-launch replica cannot be deleted';
                END IF;
                RETURN OLD;
            END IF;
            IF TG_OP = 'UPDATE' AND
               OLD.ordinary_launch_association_id IS NOT NULL AND
               NEW.ordinary_launch_association_id IS NOT NULL AND
               OLD.ordinary_launch_association_id IS DISTINCT FROM
                    NEW.ordinary_launch_association_id THEN
                RAISE EXCEPTION
                    'ordinary-launch replica pointer cannot be swapped';
            END IF;
            IF NEW.ordinary_launch_association_id IS NOT NULL THEN
                SELECT bound.* INTO association FROM {_TABLE} AS bound
                WHERE bound.association_id =
                      NEW.ordinary_launch_association_id;
                IF NOT FOUND OR association.service_name <> NEW.service_name
                   OR association.replica_id <> NEW.replica_id
                   OR association.replica_record_id::text IS DISTINCT FROM
                        (NEW.replica_state ->> 'replica_record_id')
                   OR association.resolution NOT IN ({_UNSETTLED}) THEN
                    RAISE EXCEPTION
                        'ordinary-launch replica pointer is not exact';
                END IF;
            END IF;
            IF TG_OP = 'UPDATE' AND
               OLD.ordinary_launch_association_id IS NOT NULL AND
               NEW.ordinary_launch_association_id IS NULL THEN
                SELECT bound.* INTO association FROM {_TABLE} AS bound
                WHERE bound.association_id =
                      OLD.ordinary_launch_association_id;
                IF NOT FOUND OR NOT (
                    association.resolution = 'RESULT_RECORDED' OR
                    (association.effect_phase = 'NOT_STARTED' AND
                     association.terminal_status IS NOT NULL)
                ) THEN
                    RAISE EXCEPTION
                        'ordinary-launch pointer clear lacks terminal evidence';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $function$
    """)

    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_ASSOCIATION_CONSISTENCY_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        DECLARE current_row RECORD;
        BEGIN
            IF TG_OP = 'DELETE' THEN RETURN NULL; END IF;
            SELECT bound.* INTO current_row FROM {_TABLE} AS bound
            WHERE bound.association_id = NEW.association_id;
            IF NOT FOUND THEN RETURN NULL; END IF;
            IF current_row.resolution IN ({_UNSETTLED}) AND NOT EXISTS (
                SELECT 1 FROM {_REPLICAS} AS replica
                WHERE replica.service_name = current_row.service_name
                  AND replica.replica_id = current_row.replica_id
                  AND replica.ordinary_launch_association_id =
                        current_row.association_id
                  AND replica.replica_state ->> 'replica_record_id' =
                        current_row.replica_record_id::text
            ) THEN
                RAISE EXCEPTION
                    'unsettled ordinary-launch association lacks exact replica pointer';
            END IF;
            IF current_row.resolution IN ({_SETTLED}) AND (
                current_row.pin_released_at IS NULL OR EXISTS (
                    SELECT 1 FROM {_REPLICAS} AS replica
                    WHERE replica.ordinary_launch_association_id =
                          current_row.association_id
                )
            ) THEN
                RAISE EXCEPTION
                    'settled ordinary-launch association retains active state';
            END IF;
            RETURN NULL;
        END;
        $function$
    """)
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_REPLICA_CONSISTENCY_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            IF TG_OP = 'UPDATE' AND
               OLD.ordinary_launch_association_id IS NOT NULL AND
               NEW.ordinary_launch_association_id IS NULL AND EXISTS (
                   SELECT 1 FROM {_TABLE} AS association
                   WHERE association.association_id =
                         OLD.ordinary_launch_association_id
                     AND association.resolution IN ({_UNSETTLED})
               ) THEN
                RAISE EXCEPTION
                    'unresolved ordinary-launch pointer clear cannot commit';
            END IF;
            RETURN NULL;
        END;
        $function$
    """)
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_SERVICE_CONSISTENCY_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            IF TG_OP = 'DELETE' THEN RETURN NULL; END IF;
            IF EXISTS (
                SELECT 1 FROM {_TABLE} AS association
                WHERE association.service_name = NEW.name
                  AND association.resolution IN ({_UNSETTLED})
                  AND (association.owner_controller_incarnation IS DISTINCT
                           FROM NEW.controller_incarnation OR
                       association.owner_controller_epoch IS DISTINCT FROM
                           NEW.controller_owner_epoch OR
                       association.service_lifecycle_epoch IS DISTINCT FROM
                           NEW.lifecycle_epoch OR
                       association.service_binding_epoch IS DISTINCT FROM
                           NEW.ordinary_launch_binding_epoch)
            ) THEN
                RAISE EXCEPTION
                    'service owner change did not transfer unresolved associations';
            END IF;
            RETURN NULL;
        END;
        $function$
    """)

    triggers = (
        (_SERVICE_GUARD_TRIGGER, _SERVICES, 'BEFORE UPDATE OR DELETE',
         _SERVICE_GUARD_FUNCTION, False),
        (_ASSOCIATION_GUARD_TRIGGER, _TABLE,
         'BEFORE INSERT OR UPDATE OR DELETE', _ASSOCIATION_GUARD_FUNCTION,
         False),
        (_REPLICA_GUARD_TRIGGER, _REPLICAS, 'BEFORE INSERT OR UPDATE OR DELETE',
         _REPLICA_GUARD_FUNCTION, False),
        (_ASSOCIATION_CONSISTENCY_TRIGGER, _TABLE,
         'AFTER INSERT OR UPDATE OR DELETE', _ASSOCIATION_CONSISTENCY_FUNCTION,
         True),
        (_REPLICA_CONSISTENCY_TRIGGER, _REPLICAS,
         'AFTER INSERT OR UPDATE OR DELETE', _REPLICA_CONSISTENCY_FUNCTION,
         True),
        (_SERVICE_CONSISTENCY_TRIGGER, _SERVICES, 'AFTER UPDATE OR DELETE',
         _SERVICE_CONSISTENCY_FUNCTION, True),
    )
    for trigger, table, timing, function, deferred in triggers:
        op.execute(f'DROP TRIGGER IF EXISTS {trigger} ON {table}')
        kind = 'CONSTRAINT ' if deferred else ''
        deferral = ' DEFERRABLE INITIALLY DEFERRED' if deferred else ''
        op.execute(f"""
            CREATE {kind}TRIGGER {trigger}
            {timing} ON {table}{deferral}
            FOR EACH ROW EXECUTE FUNCTION {function}()
        """)


def upgrade() -> None:
    """Install Serve042 only on the central PostgreSQL Serve database."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    _add_columns(bind)
    _create_association_table(bind)
    _create_constraints_and_indexes(bind)
    _install_guards()


def downgrade() -> None:
    # An older binary can ignore these PostgreSQL-only columns, while deleting
    # the evidence or guards could permit a duplicate provider effect.  A
    # rollback must first use the explicit fenced demotion protocol.
    raise RuntimeError(
        'Serve042 is forward-only. Demote every ordinary-launch binding '
        'through the fenced rollback protocol, retain the association '
        'evidence, and roll back application images without downgrading the '
        'database schema.')
