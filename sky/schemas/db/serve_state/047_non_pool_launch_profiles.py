"""Add generic non-pool launch profiles and legacy incident evidence.

Revision ID: 047
Revises: 046
Create Date: 2026-08-16

Serve047 is additive and dark. Existing Serve042 associations retain a NULL
generic envelope and therefore gain no generic effect authority. The legacy
ledger records evidence for explicitly reviewed bounded reconciliation scopes;
it never creates an association, request receipt, quiescence receipt, or
successor authority.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

revision: str = '047'
down_revision: str | Sequence[str] | None = '046'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ASSOCIATIONS = 'serve_ordinary_launch_associations'
_REPLICAS = 'replicas'
_SERVICES = 'services'
_LEGACY_RECONCILIATIONS = 'serve_legacy_launch_reconciliations'
_LEGACY_SCOPES = 'serve_legacy_launch_reconciliation_scopes'

_PROFILE_KINDS = (
    'ORDINARY_PAID',
    'ORDINARY_ZERO_COST',
    'RESERVED_FILL',
    'UNKNOWN_CAPACITY_REPLACEMENT',
    'COST_REBALANCE',
    'SYSTEM_OOM_RECOVERY',
)
_AUTHORIZATION_KINDS = (
    'PAID_CAPACITY_CLAIM',
    'ZERO_COST_ADMISSION',
    'RESERVED_FILL_ALLOCATION',
    'UNKNOWN_CAPACITY_REPLACEMENT',
    'COST_REBALANCE_DECISION',
    'SYSTEM_OOM_RECOVERY',
)
_RECONCILIATION_OUTCOMES = (
    'ACTIVE_ADOPT',
    'RESULT_RECORDED',
    'PROJECTED',
    'PRE_EFFECT_TERMINAL',
    'POST_EFFECT_AMBIGUOUS',
)
_PROVIDER_EVIDENCE = (
    'NOT_QUERIED',
    'PRESENT',
    'ABSENT',
    'UNKNOWN',
    'REPLACED',
)
_LEGACY_RESOLUTIONS = (
    'LEGACY_EFFECT_AMBIGUOUS',
    'CLEANUP_AUTHORIZED',
    'PROJECTED',
)

_PROFILE_GUARD_FUNCTION = 'skyserve047_guard_non_pool_profile'
_PROFILE_GUARD_TRIGGER = 'skyserve047_non_pool_profile_guard'
_SERVICE_GUARD_FUNCTION = 'skyserve047_guard_non_pool_service_capability'
_SERVICE_GUARD_TRIGGER = 'skyserve047_non_pool_service_capability_guard'
_LEGACY_GUARD_FUNCTION = 'skyserve047_guard_legacy_reconciliation'
_LEGACY_GUARD_TRIGGER = 'skyserve047_legacy_reconciliation_guard'
_LEGACY_SCOPE_GUARD_FUNCTION = 'skyserve047_guard_legacy_scope'
_LEGACY_SCOPE_GUARD_TRIGGER = 'skyserve047_legacy_scope_guard'
_REPLICA_AUTHORIZATION_GUARD_FUNCTION = (
    'skyserve047_guard_replica_non_pool_authorization')
_REPLICA_AUTHORIZATION_GUARD_TRIGGER = (
    'skyserve047_replica_non_pool_authorization_guard')
_ORDINARY_SERVICE_GUARD_FUNCTION = 'skyserve042_guard_service_binding'
_UNSETTLED_ASSOCIATIONS = (
    "'BOUND', 'CANCEL_REQUESTED', 'RESULT_RECORDED', 'AMBIGUOUS'")


def _sql_values(values: tuple[str, ...]) -> str:
    return ', '.join(f"'{value}'" for value in values)


def _column_names(bind: sa.engine.Connection, table: str) -> set[str]:
    return {
        str(column['name']) for column in sa.inspect(bind).get_columns(table)
    }


def _constraint_names(bind: sa.engine.Connection, table: str) -> set[str]:
    return {
        str(check['name'])
        for check in sa.inspect(bind).get_check_constraints(table)
        if check.get('name') is not None
    }


def _index_names(bind: sa.engine.Connection, table: str) -> set[str]:
    return {
        str(index['name'])
        for index in sa.inspect(bind).get_indexes(table)
        if index.get('name') is not None
    }


def _add_profile_columns(bind: sa.engine.Connection) -> None:
    association_columns = {
        'binding_protocol_version': sa.Column('binding_protocol_version',
                                              sa.Integer()),
        'profile_kind': sa.Column('profile_kind', sa.Text()),
        'profile_version': sa.Column('profile_version', sa.Integer()),
        'profile_digest': sa.Column('profile_digest', sa.Text()),
        'capability_cohort_epoch': sa.Column('capability_cohort_epoch',
                                             sa.BigInteger()),
        'capability_profile_set_digest': sa.Column(
            'capability_profile_set_digest', sa.Text()),
        'receipt_protocol_version': sa.Column('receipt_protocol_version',
                                              sa.Integer()),
        'authorization_kind': sa.Column('authorization_kind', sa.Text()),
        'authorization_reference': sa.Column('authorization_reference',
                                             sa.Text()),
        'authorization_generation': sa.Column('authorization_generation',
                                              sa.BigInteger()),
        'authorization_digest': sa.Column('authorization_digest', sa.Text()),
        'reconciliation_outcome': sa.Column('reconciliation_outcome',
                                            sa.Text()),
        'provider_evidence': sa.Column('provider_evidence', sa.Text()),
        'provider_evidence_observed_at': sa.Column(
            'provider_evidence_observed_at', sa.DateTime(timezone=True)),
        'provider_evidence_payload': sa.Column(
            'provider_evidence_payload', postgresql.JSONB(none_as_null=True)),
        'provider_evidence_digest': sa.Column('provider_evidence_digest',
                                              sa.Text()),
    }
    existing = _column_names(bind, _ASSOCIATIONS)
    for name, column in association_columns.items():
        if name not in existing:
            op.add_column(_ASSOCIATIONS, column)

    service_columns = {
        'non_pool_launch_binding_capable': sa.Column(
            'non_pool_launch_binding_capable',
            sa.Boolean(),
            nullable=False,
            server_default=sa.false()),
        'non_pool_launch_controller_incarnation': sa.Column(
            'non_pool_launch_controller_incarnation', sa.Uuid(as_uuid=True)),
        'non_pool_launch_binding_protocol_version': sa.Column(
            'non_pool_launch_binding_protocol_version', sa.Integer()),
        'non_pool_launch_capability_profile_set_digest': sa.Column(
            'non_pool_launch_capability_profile_set_digest', sa.Text()),
        'non_pool_launch_capability_cohort_epoch': sa.Column(
            'non_pool_launch_capability_cohort_epoch', sa.BigInteger()),
        'non_pool_launch_receipt_protocol_version': sa.Column(
            'non_pool_launch_receipt_protocol_version', sa.Integer()),
    }
    existing = _column_names(bind, _SERVICES)
    for name, column in service_columns.items():
        if name not in existing:
            op.add_column(_SERVICES, column)

    replica_columns = _column_names(bind, _REPLICAS)
    if 'non_pool_launch_authorization' not in replica_columns:
        op.add_column(
            _REPLICAS,
            sa.Column('non_pool_launch_authorization',
                      postgresql.JSONB(none_as_null=True)))


def _create_profile_constraints(bind: sa.engine.Connection) -> None:
    existing = _constraint_names(bind, _ASSOCIATIONS)
    checks = {
        'serve047_profile_complete_ck':
            'num_nonnulls(binding_protocol_version, profile_kind, '
            'profile_version, profile_digest, capability_cohort_epoch, '
            'capability_profile_set_digest, receipt_protocol_version, '
            'authorization_kind, authorization_reference, '
            'authorization_generation, authorization_digest) IN (0, 11)',
        'serve047_profile_values_ck':
            '(binding_protocol_version IS NULL OR '
            '(binding_protocol_version = 2 AND profile_version = 1 AND '
            'capability_cohort_epoch > 0 AND '
            'authorization_generation >= 0 AND '
            'length(authorization_reference) > 0 AND '
            'receipt_protocol_version = 1 AND '
            f'profile_kind IN ({_sql_values(_PROFILE_KINDS)}) AND '
            f'authorization_kind IN ({_sql_values(_AUTHORIZATION_KINDS)})))',
        'serve047_profile_digests_ck':
            '(binding_protocol_version IS NULL OR '
            "(profile_digest ~ '^[0-9a-f]{64}$' AND "
            "capability_profile_set_digest ~ '^[0-9a-f]{64}$' AND "
            "authorization_digest ~ '^[0-9a-f]{64}$'))",
        'serve047_profile_authorization_ck':
            "(profile_kind IS NULL OR "
            "(profile_kind = 'ORDINARY_PAID' AND "
            "authorization_kind = 'PAID_CAPACITY_CLAIM') OR "
            "(profile_kind = 'ORDINARY_ZERO_COST' AND "
            "authorization_kind = 'ZERO_COST_ADMISSION') OR "
            "(profile_kind = 'RESERVED_FILL' AND "
            "authorization_kind = 'RESERVED_FILL_ALLOCATION') OR "
            "(profile_kind = 'UNKNOWN_CAPACITY_REPLACEMENT' AND "
            "authorization_kind = 'UNKNOWN_CAPACITY_REPLACEMENT') OR "
            "(profile_kind = 'COST_REBALANCE' AND "
            "authorization_kind = 'COST_REBALANCE_DECISION') OR "
            "(profile_kind = 'SYSTEM_OOM_RECOVERY' AND "
            "authorization_kind = 'SYSTEM_OOM_RECOVERY'))",
        'serve047_reconciliation_ck':
            '(reconciliation_outcome IS NULL OR '
            f'reconciliation_outcome IN '
            f'({_sql_values(_RECONCILIATION_OUTCOMES)}))',
        'serve047_reconciliation_complete_ck':
            '((binding_protocol_version IS NULL AND '
            'reconciliation_outcome IS NULL AND provider_evidence IS NULL) OR '
            '(binding_protocol_version = 2 AND '
            'reconciliation_outcome IS NOT NULL AND '
            'provider_evidence IS NOT NULL))',
        'serve047_reconciliation_resolution_ck':
            '(binding_protocol_version IS NULL OR '
            "(reconciliation_outcome = 'ACTIVE_ADOPT' AND "
            "resolution IN ('BOUND', 'CANCEL_REQUESTED')) OR "
            "(reconciliation_outcome = 'RESULT_RECORDED' AND "
            "resolution = 'RESULT_RECORDED') OR "
            "(reconciliation_outcome = 'PROJECTED' AND "
            "resolution = 'PROJECTED') OR "
            "(reconciliation_outcome = 'PRE_EFFECT_TERMINAL' AND "
            "resolution = 'PRE_EFFECT_TERMINAL') OR "
            "(reconciliation_outcome = 'POST_EFFECT_AMBIGUOUS' AND "
            "resolution = 'AMBIGUOUS'))",
        'serve047_provider_evidence_ck':
            '(provider_evidence IS NULL OR '
            f'provider_evidence IN ({_sql_values(_PROVIDER_EVIDENCE)}))',
        'serve047_provider_evidence_shape_ck':
            '(provider_evidence IS NULL AND '
            'provider_evidence_observed_at IS NULL AND '
            'provider_evidence_payload IS NULL AND '
            'provider_evidence_digest IS NULL) OR '
            "(provider_evidence = 'NOT_QUERIED' AND "
            'provider_evidence_observed_at IS NULL AND '
            'provider_evidence_payload IS NULL AND '
            'provider_evidence_digest IS NULL) OR '
            "(provider_evidence IN ('PRESENT', 'ABSENT', 'UNKNOWN', "
            "'REPLACED') AND provider_evidence_observed_at IS NOT NULL AND "
            "jsonb_typeof(provider_evidence_payload) = 'object' AND "
            "provider_evidence_digest ~ '^[0-9a-f]{64}$')",
    }
    for name, expression in checks.items():
        if name not in existing:
            op.create_check_constraint(name, _ASSOCIATIONS, expression)

    existing = _constraint_names(bind, _SERVICES)
    service_checks = {
        'serve047_service_capability_complete_ck':
            '((NOT non_pool_launch_binding_capable AND '
            'num_nonnulls(non_pool_launch_controller_incarnation, '
            'non_pool_launch_binding_protocol_version, '
            'non_pool_launch_capability_profile_set_digest, '
            'non_pool_launch_capability_cohort_epoch, '
            'non_pool_launch_receipt_protocol_version) = 0) OR '
            '(non_pool_launch_binding_capable AND '
            'num_nonnulls(non_pool_launch_controller_incarnation, '
            'non_pool_launch_binding_protocol_version, '
            'non_pool_launch_capability_profile_set_digest, '
            'non_pool_launch_capability_cohort_epoch, '
            'non_pool_launch_receipt_protocol_version) = 5 AND '
            'non_pool_launch_controller_incarnation = '
            'controller_incarnation))',
        'serve047_service_capability_values_ck':
            '(non_pool_launch_binding_protocol_version IS NULL OR '
            'non_pool_launch_binding_protocol_version = 2) AND '
            '(non_pool_launch_capability_profile_set_digest IS NULL OR '
            "non_pool_launch_capability_profile_set_digest ~ "
            "'^[0-9a-f]{64}$') AND "
            '(non_pool_launch_capability_cohort_epoch IS NULL OR '
            'non_pool_launch_capability_cohort_epoch > 0) AND '
            '(non_pool_launch_receipt_protocol_version IS NULL OR '
            'non_pool_launch_receipt_protocol_version = 1)',
    }
    for name, expression in service_checks.items():
        if name not in existing:
            op.create_check_constraint(name, _SERVICES, expression)

    existing = _constraint_names(bind, _REPLICAS)
    if 'serve047_replica_non_pool_authorization_shape_ck' not in existing:
        op.create_check_constraint(
            'serve047_replica_non_pool_authorization_shape_ck', _REPLICAS,
            'non_pool_launch_authorization IS NULL OR '
            "jsonb_typeof(non_pool_launch_authorization) = 'object'")


def _create_legacy_tables(bind: sa.engine.Connection) -> None:
    if not sa.inspect(bind).has_table(_LEGACY_SCOPES):
        op.create_table(
            _LEGACY_SCOPES,
            sa.Column('scope_id', sa.Uuid(as_uuid=True), primary_key=True),
            sa.Column('scope_version', sa.Integer(), nullable=False),
            sa.Column('service_name', sa.Text(), nullable=False),
            sa.Column('service_hash', sa.Text(), nullable=False),
            sa.Column('service_lifecycle_epoch',
                      sa.BigInteger(),
                      nullable=False),
            sa.Column('identity_count', sa.Integer(), nullable=False),
            sa.Column('identities', postgresql.JSONB, nullable=False),
            sa.Column('identities_sha256', sa.Text(), nullable=False),
            sa.Column('reviewed_by', sa.Text(), nullable=False),
            sa.Column('review_reason', sa.Text(), nullable=False),
            sa.Column('reviewed_at',
                      sa.DateTime(timezone=True),
                      nullable=False,
                      server_default=sa.text('clock_timestamp()')),
            sa.CheckConstraint(
                'scope_version = 1 AND service_lifecycle_epoch > 0 AND '
                'identity_count > 0 AND identity_count <= 1000',
                name='serve047_legacy_scope_shape_ck'),
            sa.CheckConstraint(
                "jsonb_typeof(identities) = 'array' AND "
                'jsonb_array_length(identities) = identity_count',
                name='serve047_legacy_scope_identities_ck'),
            sa.CheckConstraint("identities_sha256 ~ '^[0-9a-f]{64}$'",
                               name='serve047_legacy_scope_digest_ck'),
            sa.CheckConstraint(
                'length(service_name) > 0 AND length(service_hash) > 0 AND '
                'length(reviewed_by) > 0 AND length(review_reason) > 0',
                name='serve047_legacy_scope_text_ck'))

    if not sa.inspect(bind).has_table(_LEGACY_RECONCILIATIONS):
        op.create_table(
            _LEGACY_RECONCILIATIONS,
            sa.Column('event_id', sa.Uuid(as_uuid=True), primary_key=True),
            sa.Column('scope_id',
                      sa.Uuid(as_uuid=True),
                      sa.ForeignKey(
                          f'{_LEGACY_SCOPES}.scope_id',
                          name='fk_serve047_legacy_reconciliation_scope',
                          ondelete='RESTRICT'),
                      nullable=False),
            sa.Column('service_name', sa.Text(), nullable=False),
            sa.Column('service_hash', sa.Text(), nullable=False),
            sa.Column('service_lifecycle_epoch',
                      sa.BigInteger(),
                      nullable=False),
            sa.Column('replica_id', sa.Integer(), nullable=False),
            sa.Column('replica_record_id',
                      sa.Uuid(as_uuid=True),
                      nullable=False),
            sa.Column('replica_version', sa.Integer(), nullable=False),
            sa.Column('cluster_name', sa.Text(), nullable=False),
            sa.Column('request_id', sa.Text(), nullable=False),
            sa.Column('provider_context', sa.Text(), nullable=False),
            sa.Column('provider_physical_resource_uid',
                      sa.Text(),
                      nullable=False),
            sa.Column('reconciliation_sequence',
                      sa.BigInteger(),
                      nullable=False),
            sa.Column('observed_request_status', sa.Text(), nullable=False),
            sa.Column('observed_request_execution_generation', sa.BigInteger()),
            sa.Column('observed_request_queue_present',
                      sa.Boolean(),
                      nullable=False),
            sa.Column('observed_request_claim_present',
                      sa.Boolean(),
                      nullable=False),
            sa.Column('observed_request_result_digest', sa.Text()),
            sa.Column('observed_request_at',
                      sa.DateTime(timezone=True),
                      nullable=False),
            sa.Column('observed_request_evidence',
                      postgresql.JSONB,
                      nullable=False),
            sa.Column('observed_request_evidence_digest',
                      sa.Text(),
                      nullable=False),
            sa.Column('executor_terminated_at', sa.DateTime(timezone=True)),
            sa.Column('executor_termination_evidence',
                      postgresql.JSONB(none_as_null=True)),
            sa.Column('executor_termination_evidence_digest', sa.Text()),
            sa.Column('provider_evidence', sa.Text(), nullable=False),
            sa.Column('provider_evidence_observed_at',
                      sa.DateTime(timezone=True)),
            sa.Column('provider_evidence_payload',
                      postgresql.JSONB(none_as_null=True)),
            sa.Column('provider_evidence_digest', sa.Text()),
            sa.Column('cleanup_completed_at', sa.DateTime(timezone=True)),
            sa.Column('cleanup_completion_evidence',
                      postgresql.JSONB(none_as_null=True)),
            sa.Column('cleanup_completion_evidence_digest', sa.Text()),
            sa.Column('resolution', sa.Text(), nullable=False),
            sa.Column('actor', sa.Text(), nullable=False),
            sa.Column('reason', sa.Text(), nullable=False),
            sa.Column('created_at',
                      sa.DateTime(timezone=True),
                      nullable=False,
                      server_default=sa.text('clock_timestamp()')),
            sa.CheckConstraint(
                'service_lifecycle_epoch > 0 AND replica_id > 0 AND '
                'replica_version > 0 AND reconciliation_sequence > 0',
                name='serve047_legacy_positive_identity_ck'),
            sa.CheckConstraint(
                'length(service_name) > 0 AND length(service_hash) > 0 AND '
                'length(cluster_name) > 0 AND length(request_id) > 0 AND '
                'length(provider_context) > 0 AND '
                'length(provider_physical_resource_uid) > 0 AND '
                'length(observed_request_status) > 0 AND '
                'length(actor) > 0 AND length(reason) > 0',
                name='serve047_legacy_text_ck'),
            sa.CheckConstraint(
                "jsonb_typeof(observed_request_evidence) = 'object' AND "
                "observed_request_evidence_digest ~ '^[0-9a-f]{64}$' AND "
                '(observed_request_result_digest IS NULL OR '
                "observed_request_result_digest ~ '^[0-9a-f]{64}$')",
                name='serve047_legacy_request_evidence_ck'),
            sa.CheckConstraint(
                'observed_request_execution_generation IS NULL OR '
                'observed_request_execution_generation >= 0',
                name='serve047_legacy_request_generation_ck'),
            sa.CheckConstraint(
                '(executor_terminated_at IS NULL AND '
                'executor_termination_evidence IS NULL AND '
                'executor_termination_evidence_digest IS NULL) OR '
                '(executor_terminated_at IS NOT NULL AND '
                "jsonb_typeof(executor_termination_evidence) = 'object' AND "
                "executor_termination_evidence_digest ~ '^[0-9a-f]{64}$')",
                name='serve047_legacy_executor_evidence_ck'),
            sa.CheckConstraint(
                f'provider_evidence IN ({_sql_values(_PROVIDER_EVIDENCE)})',
                name='serve047_legacy_provider_evidence_ck'),
            sa.CheckConstraint(
                "(provider_evidence = 'NOT_QUERIED' AND "
                'provider_evidence_observed_at IS NULL AND '
                'provider_evidence_payload IS NULL AND '
                'provider_evidence_digest IS NULL) OR '
                "(provider_evidence <> 'NOT_QUERIED' AND "
                'provider_evidence_observed_at IS NOT NULL AND '
                "jsonb_typeof(provider_evidence_payload) = 'object' AND "
                "provider_evidence_digest ~ '^[0-9a-f]{64}$')",
                name='serve047_legacy_provider_shape_ck'),
            sa.CheckConstraint(
                f'resolution IN ({_sql_values(_LEGACY_RESOLUTIONS)})',
                name='serve047_legacy_resolution_ck'),
            sa.CheckConstraint(
                "resolution = 'LEGACY_EFFECT_AMBIGUOUS' OR "
                "(observed_request_status IN "
                "('SUCCEEDED', 'FAILED', 'CANCELLED') AND "
                'executor_terminated_at IS NOT NULL AND '
                "provider_evidence = 'ABSENT' AND "
                'provider_evidence_observed_at >= executor_terminated_at)',
                name='serve047_legacy_cleanup_authority_ck'),
            sa.CheckConstraint(
                "(resolution <> 'PROJECTED' AND "
                'cleanup_completed_at IS NULL AND '
                'cleanup_completion_evidence IS NULL AND '
                'cleanup_completion_evidence_digest IS NULL) OR '
                "(resolution = 'PROJECTED' AND "
                'cleanup_completed_at >= provider_evidence_observed_at AND '
                "jsonb_typeof(cleanup_completion_evidence) = 'object' AND "
                "cleanup_completion_evidence_digest ~ '^[0-9a-f]{64}$')",
                name='serve047_legacy_cleanup_completion_ck'))

    indexes = _index_names(bind, _LEGACY_RECONCILIATIONS)
    if 'uq_serve047_legacy_identity_sequence' not in indexes:
        op.create_index(
            'uq_serve047_legacy_identity_sequence',
            _LEGACY_RECONCILIATIONS, [
                'scope_id', 'service_name', 'service_hash', 'replica_record_id',
                'cluster_name', 'replica_id', 'request_id', 'provider_context',
                'provider_physical_resource_uid', 'reconciliation_sequence'
            ],
            unique=True)
    if 'ix_serve047_legacy_resolution_created' not in indexes:
        op.create_index('ix_serve047_legacy_resolution_created',
                        _LEGACY_RECONCILIATIONS, ['resolution', 'created_at'])


def _install_guards() -> None:
    immutable_profile_columns = (
        'binding_protocol_version',
        'profile_kind',
        'profile_version',
        'profile_digest',
        'capability_cohort_epoch',
        'capability_profile_set_digest',
        'receipt_protocol_version',
        'authorization_kind',
        'authorization_reference',
        'authorization_generation',
        'authorization_digest',
    )
    immutable_checks = '\n               OR '.join(
        f'(OLD.{column} IS NOT NULL AND NEW.{column} IS DISTINCT FROM '
        f'OLD.{column})' for column in immutable_profile_columns)
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_PROFILE_GUARD_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            IF {immutable_checks} THEN
                RAISE EXCEPTION
                    'non-pool launch profile identity is immutable';
            END IF;
            RETURN NEW;
        END;
        $function$
    """)
    op.execute(
        f'DROP TRIGGER IF EXISTS {_PROFILE_GUARD_TRIGGER} ON {_ASSOCIATIONS}')
    op.execute(f"""
        CREATE TRIGGER {_PROFILE_GUARD_TRIGGER}
        BEFORE UPDATE ON {_ASSOCIATIONS}
        FOR EACH ROW EXECUTE FUNCTION {_PROFILE_GUARD_FUNCTION}()
    """)

    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_REPLICA_AUTHORIZATION_GUARD_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            IF NEW.non_pool_launch_authorization IS DISTINCT FROM
                    OLD.non_pool_launch_authorization THEN
                RAISE EXCEPTION
                    'replica non-pool launch authorization is initial-insert-only';
            END IF;
            RETURN NEW;
        END;
        $function$
    """)
    op.execute(f'DROP TRIGGER IF EXISTS {_REPLICA_AUTHORIZATION_GUARD_TRIGGER} '
               f'ON {_REPLICAS}')
    op.execute(f"""
        CREATE TRIGGER {_REPLICA_AUTHORIZATION_GUARD_TRIGGER}
        BEFORE UPDATE ON {_REPLICAS}
        FOR EACH ROW EXECUTE FUNCTION {_REPLICA_AUTHORIZATION_GUARD_FUNCTION}()
    """)

    capability_columns = (
        'non_pool_launch_binding_capable',
        'non_pool_launch_controller_incarnation',
        'non_pool_launch_binding_protocol_version',
        'non_pool_launch_capability_profile_set_digest',
        'non_pool_launch_capability_cohort_epoch',
        'non_pool_launch_receipt_protocol_version',
    )
    capability_changed = '\n               OR '.join(
        f'NEW.{column} IS DISTINCT FROM OLD.{column}'
        for column in capability_columns)

    # Serve042 owns the canonical service authority and binding-epoch guard.
    # Generic capability activation is a second transition within bound mode,
    # so extend that guard in place instead of introducing a competing epoch.
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_ORDINARY_SERVICE_GUARD_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF EXISTS (
                    SELECT 1 FROM {_ASSOCIATIONS} AS association
                    WHERE association.service_name = OLD.name
                      AND association.resolution IN (
                          {_UNSETTLED_ASSOCIATIONS})
                ) THEN
                    RAISE EXCEPTION
                        'unresolved ordinary-launch associations block service deletion';
                END IF;
                RETURN OLD;
            END IF;

            IF NEW.controller_owner_epoch < OLD.controller_owner_epoch THEN
                RAISE EXCEPTION
                    'ordinary-launch controller owner epoch regressed';
            END IF;
            IF NEW.controller_incarnation IS DISTINCT FROM
                    OLD.controller_incarnation THEN
                IF NEW.controller_owner_epoch <>
                        OLD.controller_owner_epoch + 1 THEN
                    RAISE EXCEPTION
                        'controller incarnation change requires one owner-epoch advance';
                END IF;
            ELSIF NEW.controller_owner_epoch <>
                    OLD.controller_owner_epoch THEN
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
            ELSIF NEW.ordinary_launch_binding_epoch IS DISTINCT FROM
                    OLD.ordinary_launch_binding_epoch AND NOT (
                OLD.ordinary_launch_binding_mode = 'bound' AND
                NEW.ordinary_launch_binding_mode = 'bound' AND
                NEW.ordinary_launch_binding_epoch =
                    OLD.ordinary_launch_binding_epoch + 1 AND
                NEW.controller_incarnation IS NOT DISTINCT FROM
                    OLD.controller_incarnation AND
                ({capability_changed})
            ) THEN
                RAISE EXCEPTION
                    'binding epoch advance requires a mode or non-pool capability transition';
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
        CREATE OR REPLACE FUNCTION {_SERVICE_GUARD_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.non_pool_launch_binding_capable OR
                   num_nonnulls(
                       NEW.non_pool_launch_controller_incarnation,
                       NEW.non_pool_launch_binding_protocol_version,
                       NEW.non_pool_launch_capability_profile_set_digest,
                       NEW.non_pool_launch_capability_cohort_epoch,
                       NEW.non_pool_launch_receipt_protocol_version) <> 0 THEN
                    RAISE EXCEPTION
                        'new service cannot bypass non-pool capability CAS';
                END IF;
                RETURN NEW;
            END IF;
            IF ({capability_changed}) AND
               NEW.controller_incarnation IS NOT DISTINCT FROM
                   OLD.controller_incarnation AND NOT (
                       NEW.ordinary_launch_binding_mode = 'bound' AND
                       NEW.ordinary_launch_binding_epoch =
                           OLD.ordinary_launch_binding_epoch + 1) THEN
                RAISE EXCEPTION
                    'non-pool capability change requires fresh controller incarnation or adjacent bound epoch';
            END IF;
            RETURN NEW;
        END;
        $function$
    """)
    op.execute(
        f'DROP TRIGGER IF EXISTS {_SERVICE_GUARD_TRIGGER} ON {_SERVICES}')
    op.execute(f"""
        CREATE TRIGGER {_SERVICE_GUARD_TRIGGER}
        BEFORE INSERT OR UPDATE ON {_SERVICES}
        FOR EACH ROW EXECUTE FUNCTION {_SERVICE_GUARD_FUNCTION}()
    """)

    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_LEGACY_SCOPE_GUARD_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION
                    'legacy launch reconciliation scope is append-only';
            END IF;
            IF EXISTS (
                SELECT 1
                  FROM jsonb_array_elements(NEW.identities) AS item(identity)
                 WHERE jsonb_typeof(item.identity) <> 'object'
                    OR NOT item.identity ?& ARRAY[
                        'replica_id', 'replica_record_id', 'replica_version',
                        'cluster_name', 'request_id', 'provider_context',
                        'provider_physical_resource_uid']
                    OR (SELECT count(*)
                          FROM jsonb_object_keys(item.identity)) <> 7
                    OR item.identity->>'replica_id' !~ '^[1-9][0-9]*$'
                    OR item.identity->>'replica_version' !~ '^[1-9][0-9]*$'
                    OR item.identity->>'replica_record_id' !~
                        '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[1-5][0-9a-f]{{3}}-'
                        '[89ab][0-9a-f]{{3}}-[0-9a-f]{{12}}$'
                    OR length(item.identity->>'cluster_name') = 0
                    OR length(item.identity->>'request_id') = 0
                    OR length(item.identity->>'provider_context') = 0
                    OR length(
                        item.identity->>'provider_physical_resource_uid') = 0
            ) OR (
                SELECT count(DISTINCT item.identity)
                  FROM jsonb_array_elements(NEW.identities) AS item(identity)
            ) <> NEW.identity_count THEN
                RAISE EXCEPTION
                    'legacy reconciliation scope identities are malformed';
            END IF;
            RETURN NEW;
        END;
        $function$
    """)
    op.execute(f'DROP TRIGGER IF EXISTS {_LEGACY_SCOPE_GUARD_TRIGGER} '
               f'ON {_LEGACY_SCOPES}')
    op.execute(f"""
        CREATE TRIGGER {_LEGACY_SCOPE_GUARD_TRIGGER}
        BEFORE INSERT OR UPDATE OR DELETE ON {_LEGACY_SCOPES}
        FOR EACH ROW EXECUTE FUNCTION {_LEGACY_SCOPE_GUARD_FUNCTION}()
    """)

    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_LEGACY_GUARD_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        DECLARE
            scope_record record;
            previous_record record;
            previous_rank integer;
            new_rank integer;
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION
                    'legacy launch reconciliation evidence is append-only';
            END IF;
            SELECT * INTO scope_record
              FROM {_LEGACY_SCOPES}
             WHERE scope_id = NEW.scope_id
             FOR UPDATE;
            IF NOT FOUND OR
               scope_record.service_name <> NEW.service_name OR
               scope_record.service_hash <> NEW.service_hash OR
               scope_record.service_lifecycle_epoch <>
                   NEW.service_lifecycle_epoch OR
               NOT scope_record.identities @> jsonb_build_array(
                   jsonb_build_object(
                       'replica_id', NEW.replica_id,
                       'replica_record_id', NEW.replica_record_id::text,
                       'replica_version', NEW.replica_version,
                       'cluster_name', NEW.cluster_name,
                       'request_id', NEW.request_id,
                       'provider_context', NEW.provider_context,
                       'provider_physical_resource_uid',
                           NEW.provider_physical_resource_uid)) THEN
                RAISE EXCEPTION
                    'legacy launch evidence is outside its sealed scope';
            END IF;

            SELECT * INTO previous_record
              FROM {_LEGACY_RECONCILIATIONS}
             WHERE scope_id = NEW.scope_id
               AND service_name = NEW.service_name
               AND service_hash = NEW.service_hash
               AND replica_record_id = NEW.replica_record_id
               AND cluster_name = NEW.cluster_name
               AND replica_id = NEW.replica_id
               AND request_id = NEW.request_id
               AND provider_context = NEW.provider_context
               AND provider_physical_resource_uid =
                   NEW.provider_physical_resource_uid
             ORDER BY reconciliation_sequence DESC
             LIMIT 1
             FOR UPDATE;
            IF NOT FOUND THEN
                IF NEW.reconciliation_sequence <> 1 OR
                   NEW.resolution <> 'LEGACY_EFFECT_AMBIGUOUS' THEN
                    RAISE EXCEPTION
                        'legacy reconciliation must begin ambiguous';
                END IF;
                RETURN NEW;
            END IF;
            IF previous_record.resolution = 'PROJECTED' OR
               NEW.reconciliation_sequence <>
                   previous_record.reconciliation_sequence + 1 THEN
                RAISE EXCEPTION
                    'legacy reconciliation sequence is closed or noncontiguous';
            END IF;
            previous_rank := CASE previous_record.resolution
                WHEN 'LEGACY_EFFECT_AMBIGUOUS' THEN 1
                WHEN 'CLEANUP_AUTHORIZED' THEN 2
                WHEN 'PROJECTED' THEN 3
            END;
            new_rank := CASE NEW.resolution
                WHEN 'LEGACY_EFFECT_AMBIGUOUS' THEN 1
                WHEN 'CLEANUP_AUTHORIZED' THEN 2
                WHEN 'PROJECTED' THEN 3
            END;
            IF new_rank < previous_rank OR new_rank > previous_rank + 1 THEN
                RAISE EXCEPTION
                    'legacy reconciliation state transition is invalid';
            END IF;
            RETURN NEW;
        END;
        $function$
    """)
    op.execute(f'DROP TRIGGER IF EXISTS {_LEGACY_GUARD_TRIGGER} '
               f'ON {_LEGACY_RECONCILIATIONS}')
    op.execute(f"""
        CREATE TRIGGER {_LEGACY_GUARD_TRIGGER}
        BEFORE INSERT OR UPDATE OR DELETE ON {_LEGACY_RECONCILIATIONS}
        FOR EACH ROW EXECUTE FUNCTION {_LEGACY_GUARD_FUNCTION}()
    """)


def upgrade() -> None:
    """Install an inert generic envelope and legacy evidence ledger."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    _add_profile_columns(bind)
    _create_profile_constraints(bind)
    _create_legacy_tables(bind)
    _install_guards()


def downgrade() -> None:
    """Preserve profile and legacy evidence across application rollback."""
    raise RuntimeError(
        'Serve047 is forward-only. Preserve generic profile and legacy '
        'reconciliation evidence while rolling application code back.')
