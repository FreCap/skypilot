"""PostgreSQL schema catalog for SkyServe resource-action shadow evidence.

This metadata is deliberately separate from ``serve_state_schema.Base``.
Revision 001 uses that legacy metadata for both supported Serve dialects;
keeping the shadow tables here prevents a fresh SQLite bootstrap from trying
to create PostgreSQL-only JSONB evidence tables.
"""

import sqlalchemy
from sqlalchemy.dialects import postgresql

metadata = sqlalchemy.MetaData()

_SHA256_PATTERN = "^[0-9a-f]{64}$"
_UUID_PATTERN = ('^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-'
                 '[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
_PARENT_PHASES = (
    'PENDING',
    'RUNNING',
    'COMPLETE',
    'ABANDONED_PRE_SUBMIT',
    'AMBIGUOUS',
)
_PARITY_CLASSES = (
    'PENDING',
    'MATCH',
    'IDENTITY_MISMATCH',
    'PLACEMENT_MISMATCH',
    'SUBMISSION_CERTAINTY_MISMATCH',
    'OPERATION_ID_MISMATCH',
    'RETRY_MISMATCH',
    'OBSERVATION_MISMATCH',
    'TERMINAL_MISMATCH',
    'UNSUPPORTED_PROVIDER_PROFILE',
    'ABANDONED',
    'AMBIGUOUS',
)
_DIVERGENCE_CLASSES = tuple(value for value in _PARITY_CLASSES
                            if value not in ('PENDING', 'MATCH', 'ABANDONED',
                                             'AMBIGUOUS'))
_CHILD_PHASES = (
    'PRE_SUBMIT',
    'REQUEST_BOUND',
    'COMPLETE',
    'ABANDONED_PRE_SUBMIT',
    'REQUEST_ASSOCIATION_UNKNOWN',
)


def _sql_values(values: tuple[str, ...]) -> str:
    return ', '.join(f"'{value}'" for value in values)


def _json_object_shape(column: str) -> str:
    return (f"jsonb_typeof({column}) IS NOT DISTINCT FROM 'object' AND "
            f'octet_length(CAST({column} AS TEXT)) <= 65536')


def _optional_json_hash_shape(column: str, hash_column: str) -> str:
    return (f'(({column} IS NULL AND {hash_column} IS NULL) OR '
            f'({column} IS NOT NULL AND {hash_column} IS NOT NULL AND '
            f'{_json_object_shape(column)} AND '
            f"{hash_column} ~ '{_SHA256_PATTERN}'))")


shadow_samples_table = sqlalchemy.Table(
    'serve_resource_action_shadow_samples',
    metadata,
    sqlalchemy.Column('would_be_action_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('service_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_incarnation',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('replica_id', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('replica_incarnation',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('desired_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('action_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('resource_identity', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('immutable_spec',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('immutable_spec_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('provider_plan',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('provider_plan_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('profile_eligibility', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('phase', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('legacy_projection', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('legacy_projection_sha256', sqlalchemy.Text),
    sqlalchemy.Column('proposed_projection',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('proposed_projection_sha256', sqlalchemy.Text),
    sqlalchemy.Column('parity_class', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('revision',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='1'),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('completed_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.PrimaryKeyConstraint('would_be_action_id',
                                    name='pk_serve_ra_shadow_samples'),
    sqlalchemy.UniqueConstraint('service_hash',
                                'service_incarnation',
                                'replica_id',
                                'replica_incarnation',
                                'desired_generation',
                                'action_type',
                                name='uq_serve_ra_shadow_samples_identity'),
    sqlalchemy.CheckConstraint(
        'octet_length(service_name) BETWEEN 1 AND 256 AND '
        'octet_length(service_hash) = 36 AND '
        'octet_length(resource_identity) BETWEEN 1 AND 1024',
        name='ck_serve_ra_shadow_samples_identity_bounds'),
    sqlalchemy.CheckConstraint(
        f"service_hash ~ '{_UUID_PATTERN}' AND "
        'service_hash = CAST(service_incarnation AS TEXT)',
        name='ck_serve_ra_shadow_samples_service_incarnation'),
    sqlalchemy.CheckConstraint('replica_id >= 0',
                               name='ck_serve_ra_shadow_samples_replica_id'),
    sqlalchemy.CheckConstraint('desired_generation > 0',
                               name='ck_serve_ra_shadow_samples_generation'),
    sqlalchemy.CheckConstraint("action_type IN ('launch', 'down')",
                               name='ck_serve_ra_shadow_samples_action_type'),
    sqlalchemy.CheckConstraint(
        f'{_json_object_shape("immutable_spec")} AND '
        f"immutable_spec_sha256 ~ '{_SHA256_PATTERN}'",
        name='ck_serve_ra_shadow_samples_spec_shape'),
    sqlalchemy.CheckConstraint(
        f'{_json_object_shape("provider_plan")} AND '
        f"provider_plan_sha256 ~ '{_SHA256_PATTERN}'",
        name='ck_serve_ra_shadow_samples_plan_shape'),
    sqlalchemy.CheckConstraint(
        "profile_eligibility IN ('ELIGIBLE', 'UNSUPPORTED')",
        name='ck_serve_ra_shadow_samples_eligibility'),
    sqlalchemy.CheckConstraint(f'phase IN ({_sql_values(_PARENT_PHASES)})',
                               name='ck_serve_ra_shadow_samples_phase'),
    sqlalchemy.CheckConstraint(
        _optional_json_hash_shape('legacy_projection',
                                  'legacy_projection_sha256'),
        name='ck_serve_ra_shadow_samples_legacy_projection'),
    sqlalchemy.CheckConstraint(
        _optional_json_hash_shape('proposed_projection',
                                  'proposed_projection_sha256'),
        name='ck_serve_ra_shadow_samples_proposed_projection'),
    sqlalchemy.CheckConstraint(
        f'parity_class IN ({_sql_values(_PARITY_CLASSES)})',
        name='ck_serve_ra_shadow_samples_parity'),
    sqlalchemy.CheckConstraint(
        "(phase IN ('PENDING', 'RUNNING') AND parity_class = 'PENDING' "
        'AND completed_at IS NULL) OR '
        "(phase = 'COMPLETE' AND parity_class NOT IN "
        "('PENDING', 'ABANDONED', 'AMBIGUOUS') AND "
        'legacy_projection IS NOT NULL AND proposed_projection IS NOT NULL '
        'AND completed_at IS NOT NULL) OR '
        "(phase = 'ABANDONED_PRE_SUBMIT' AND parity_class = 'ABANDONED' "
        'AND completed_at IS NOT NULL) OR '
        "(phase = 'AMBIGUOUS' AND parity_class = 'AMBIGUOUS' "
        'AND completed_at IS NOT NULL)',
        name='ck_serve_ra_shadow_samples_phase_shape'),
    sqlalchemy.CheckConstraint('revision > 0',
                               name='ck_serve_ra_shadow_samples_revision'),
    sqlalchemy.CheckConstraint(
        'updated_at >= created_at AND '
        '(completed_at IS NULL OR completed_at >= created_at)',
        name='ck_serve_ra_shadow_samples_timestamps'),
)

sqlalchemy.Index(
    'ix_serve_ra_shadow_samples_promotion',
    shadow_samples_table.c.service_name,
    shadow_samples_table.c.service_hash,
    shadow_samples_table.c.created_at,
    shadow_samples_table.c.would_be_action_id,
)
sqlalchemy.Index(
    'ix_serve_ra_shadow_samples_blockers',
    shadow_samples_table.c.service_name,
    shadow_samples_table.c.service_hash,
    shadow_samples_table.c.updated_at,
    postgresql_where=sqlalchemy.text(
        "phase <> 'COMPLETE' OR parity_class <> 'MATCH' OR "
        "profile_eligibility <> 'ELIGIBLE'"),
)
sqlalchemy.Index(
    'ix_serve_ra_shadow_samples_retention',
    shadow_samples_table.c.completed_at,
    shadow_samples_table.c.would_be_action_id,
    postgresql_where=sqlalchemy.text('completed_at IS NOT NULL'),
)

shadow_attempts_table = sqlalchemy.Table(
    'serve_resource_action_shadow_attempts',
    metadata,
    sqlalchemy.Column('would_be_action_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('request_sequence', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('logical_attempt', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('request_role', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('planned_execution_kind', sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('phase', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('legacy_request_id', sqlalchemy.Text),
    sqlalchemy.Column('invocation',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('invocation_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('provider_operation_id', sqlalchemy.Text),
    sqlalchemy.Column('actual_outcome', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('actual_outcome_sha256', sqlalchemy.Text),
    sqlalchemy.Column('proposed_outcome', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('proposed_outcome_sha256', sqlalchemy.Text),
    sqlalchemy.Column('retry_decision', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('retry_decision_sha256', sqlalchemy.Text),
    sqlalchemy.Column('pre_observation', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('pre_observation_sha256', sqlalchemy.Text),
    sqlalchemy.Column('post_observation', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('post_observation_sha256', sqlalchemy.Text),
    sqlalchemy.Column('divergence_class', sqlalchemy.Text),
    sqlalchemy.Column('admitted_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('request_bound_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('completed_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.PrimaryKeyConstraint('would_be_action_id',
                                    'request_sequence',
                                    name='pk_serve_ra_shadow_attempts'),
    sqlalchemy.ForeignKeyConstraint(
        ['would_be_action_id'],
        ['serve_resource_action_shadow_samples.would_be_action_id'],
        ondelete='CASCADE',
        name='fk_serve_ra_shadow_attempts_sample'),
    sqlalchemy.CheckConstraint('request_sequence > 0 AND logical_attempt > 0',
                               name='ck_serve_ra_shadow_attempts_counters'),
    sqlalchemy.CheckConstraint(
        "request_role IN ('PRIMARY_LAUNCH', 'PRIMARY_DOWN', "
        "'LAUNCH_CLEANUP_DOWN')",
        name='ck_serve_ra_shadow_attempts_role'),
    sqlalchemy.CheckConstraint(
        "planned_execution_kind IN ('api_request', 'legacy_direct_down')",
        name='ck_serve_ra_shadow_attempts_execution'),
    sqlalchemy.CheckConstraint(f'phase IN ({_sql_values(_CHILD_PHASES)})',
                               name='ck_serve_ra_shadow_attempts_phase'),
    sqlalchemy.CheckConstraint(
        '(legacy_request_id IS NULL OR '
        'octet_length(legacy_request_id) BETWEEN 1 AND 128) AND '
        '(provider_operation_id IS NULL OR '
        'octet_length(provider_operation_id) BETWEEN 1 AND 1024)',
        name='ck_serve_ra_shadow_attempts_text_bounds'),
    sqlalchemy.CheckConstraint(
        f'{_json_object_shape("invocation")} AND '
        f"invocation_sha256 ~ '{_SHA256_PATTERN}'",
        name='ck_serve_ra_shadow_attempts_invocation'),
    sqlalchemy.CheckConstraint(
        _optional_json_hash_shape('actual_outcome', 'actual_outcome_sha256'),
        name='ck_serve_ra_shadow_attempts_actual_outcome'),
    sqlalchemy.CheckConstraint(
        _optional_json_hash_shape('proposed_outcome',
                                  'proposed_outcome_sha256'),
        name='ck_serve_ra_shadow_attempts_proposed_outcome'),
    sqlalchemy.CheckConstraint(
        _optional_json_hash_shape('retry_decision', 'retry_decision_sha256'),
        name='ck_serve_ra_shadow_attempts_retry_decision'),
    sqlalchemy.CheckConstraint(
        _optional_json_hash_shape('pre_observation', 'pre_observation_sha256'),
        name='ck_serve_ra_shadow_attempts_pre_observation'),
    sqlalchemy.CheckConstraint(
        _optional_json_hash_shape('post_observation',
                                  'post_observation_sha256'),
        name='ck_serve_ra_shadow_attempts_post_observation'),
    sqlalchemy.CheckConstraint(
        'divergence_class IS NULL OR '
        f'divergence_class IN ({_sql_values(_DIVERGENCE_CLASSES)})',
        name='ck_serve_ra_shadow_attempts_divergence'),
    sqlalchemy.CheckConstraint(
        "(phase = 'PRE_SUBMIT' AND legacy_request_id IS NULL AND "
        'request_bound_at IS NULL AND completed_at IS NULL) OR '
        "(phase = 'REQUEST_BOUND' AND "
        "planned_execution_kind = 'api_request' AND "
        'legacy_request_id IS NOT NULL AND request_bound_at IS NOT NULL AND '
        'completed_at IS NULL) OR '
        "(phase = 'COMPLETE' AND completed_at IS NOT NULL AND "
        "((planned_execution_kind = 'api_request' AND "
        'legacy_request_id IS NOT NULL AND request_bound_at IS NOT NULL) OR '
        "(planned_execution_kind = 'legacy_direct_down' AND "
        'legacy_request_id IS NULL AND request_bound_at IS NULL))) OR '
        "(phase = 'ABANDONED_PRE_SUBMIT' AND completed_at IS NOT NULL AND "
        'legacy_request_id IS NULL AND request_bound_at IS NULL AND '
        'provider_operation_id IS NULL AND actual_outcome IS NULL AND '
        'post_observation IS NULL) OR '
        "(phase = 'REQUEST_ASSOCIATION_UNKNOWN' AND "
        "planned_execution_kind = 'api_request' AND completed_at IS NOT NULL "
        'AND legacy_request_id IS NULL AND request_bound_at IS NULL)',
        name='ck_serve_ra_shadow_attempts_phase_shape'),
    sqlalchemy.CheckConstraint(
        "phase <> 'COMPLETE' OR "
        "planned_execution_kind <> 'legacy_direct_down' OR "
        'divergence_class IS NOT NULL',
        name='ck_serve_ra_shadow_attempts_direct_divergence'),
    sqlalchemy.CheckConstraint(
        'updated_at >= admitted_at AND '
        '(request_bound_at IS NULL OR request_bound_at >= admitted_at) AND '
        '(completed_at IS NULL OR completed_at >= admitted_at)',
        name='ck_serve_ra_shadow_attempts_timestamps'),
)

sqlalchemy.Index(
    'uq_serve_ra_shadow_attempts_request',
    shadow_attempts_table.c.legacy_request_id,
    unique=True,
    postgresql_where=sqlalchemy.text('legacy_request_id IS NOT NULL'),
)
sqlalchemy.Index(
    'ix_serve_ra_shadow_attempts_stale',
    shadow_attempts_table.c.phase,
    shadow_attempts_table.c.admitted_at,
    shadow_attempts_table.c.would_be_action_id,
    shadow_attempts_table.c.request_sequence,
    postgresql_where=sqlalchemy.text(
        "phase IN ('PRE_SUBMIT', 'REQUEST_BOUND')"),
)

# Uppercase aliases match the central request-store schema catalog style.
SHADOW_SAMPLES = shadow_samples_table
SHADOW_ATTEMPTS = shadow_attempts_table
RESOURCE_ACTION_STATE_METADATA = metadata


def service_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return fresh revision-032 columns for the existing services table."""
    return (
        sqlalchemy.Column('resource_action_mode',
                          sqlalchemy.Text,
                          nullable=False,
                          server_default='legacy'),
        sqlalchemy.Column('resource_action_mode_changed_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=True),
    )


def replica_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return fresh action-owned columns for the existing replicas table."""
    uuid_type = sqlalchemy.Uuid(as_uuid=True)
    return (
        sqlalchemy.Column('replica_incarnation', uuid_type, nullable=True),
        sqlalchemy.Column('desired_generation',
                          sqlalchemy.BigInteger,
                          nullable=True),
        sqlalchemy.Column('sky_cluster_record_uuid',
                          sqlalchemy.Uuid(as_uuid=True),
                          nullable=True),
        sqlalchemy.Column('launch_action_id',
                          sqlalchemy.Uuid(as_uuid=True),
                          nullable=True),
        sqlalchemy.Column('down_action_id',
                          sqlalchemy.Uuid(as_uuid=True),
                          nullable=True),
        sqlalchemy.Column('launch_shadow_sample_id',
                          sqlalchemy.Uuid(as_uuid=True),
                          nullable=True),
        sqlalchemy.Column('down_shadow_sample_id',
                          sqlalchemy.Uuid(as_uuid=True),
                          nullable=True),
    )
