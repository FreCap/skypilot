"""PostgreSQL schema catalogs for SkyServe resource-action evidence.

The staged tables are separate from ``serve_state_schema.Base`` so a fresh
SQLite bootstrap never tries to create PostgreSQL-only JSONB evidence tables.
Serve revision 033 may encounter the staged sample/attempt pair from an
unshipped, empty migration draft, then converges that pair into the complete
head graph in the same guarded transaction.
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
_COHORT_STATES = (
    'REGISTERING',
    'ACCEPTING',
    'DRAINING',
    'REMOVAL_AUTHORIZED',
    'RETIRED',
)
_COHORT_REFERENCE_STATES = (
    'PREPARING',
    'SHADOW_ACTIVE',
    'ACTION_ACTIVE',
    'RELEASED',
)
_NORMALIZATION_OUTCOMES = (
    'REPRESENTABLE',
    'NOT_REPRESENTABLE',
)
_LAUNCH_NOT_REPRESENTABLE_REASONS = (
    'request_contract',
    'secret_or_tls_material',
    'source_mismatch',
    'policy_configured_or_mutated',
    'managed_secrets',
    'multi_task',
    'multi_node',
    'multi_resource',
    'mount_or_storage',
    'non_kubernetes',
    'spot',
    'non_direct_pod_topology',
    'port_contract',
    'reserved_label_collision',
    'mutable_image',
    'custom_provider_implementation',
    'preflight_unavailable_or_invalid',
    'authority_worker_attestation',
    'authorization_or_principal_drift',
    'prerequisite_or_network_drift',
    'admitted_object_contract',
    'runtime_or_job_contract',
    'unrepresented_execution_config',
    'unrepresented_resource',
    'unfrozen_placement',
    'unfrozen_identity',
    'unfrozen_kubernetes_scope',
    'target_mismatch',
)
_DOWN_NOT_REPRESENTABLE_REASONS = (
    'request_contract',
    'prior_launch_basis',
    'target_mismatch',
    'preflight_unavailable_or_invalid',
    'authority_worker_attestation',
    'authorization_or_principal_drift',
    'prerequisite_or_network_drift',
    'policy_configured_or_mutated',
    'unrepresented_execution_config',
    'unfrozen_kubernetes_scope',
)
_TERMINAL_REQUEST_STATUSES = (
    'SUCCEEDED',
    'FAILED',
    'CANCELLED',
)
_RETRY_DISPOSITIONS = (
    'RETRY_SAME_DECISION',
    'TERMINAL',
    'REPLAN_NEW_GENERATION',
    'BLOCK',
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


def shadow_attempt_effect_trace_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return fresh revision-033 effect-trace columns."""
    return (
        sqlalchemy.Column('legacy_effect_trace',
                          postgresql.JSONB(none_as_null=True),
                          nullable=True),
        sqlalchemy.Column('legacy_effect_trace_sha256',
                          sqlalchemy.Text,
                          nullable=True),
    )


# Clone the abandoned feature draft's staged tables into the revision-033 head
# graph before extending their physical catalog. The guarded migration may
# encounter that empty pair, but upstream revision 032 never imports it.
head_metadata = sqlalchemy.MetaData()
shadow_samples_head_table = shadow_samples_table.to_metadata(head_metadata)
shadow_attempts_head_table = shadow_attempts_table.to_metadata(head_metadata)
for _column in shadow_attempt_effect_trace_columns():
    shadow_attempts_head_table.append_column(_column)
shadow_attempts_head_table.append_constraint(
    sqlalchemy.CheckConstraint(_optional_json_hash_shape(
        'legacy_effect_trace', 'legacy_effect_trace_sha256'),
                               name='ck_serve_ra_shadow_attempts_effect_trace'))

worker_cohorts_table = sqlalchemy.Table(
    'serve_resource_action_worker_cohorts',
    head_metadata,
    sqlalchemy.Column('cohort_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('deployment_uid', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('cohort_identity',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('cohort_identity_sha256', sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('registration_attestations',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('registration_attestations_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('lifecycle_state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('revision',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='1'),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('state_changed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('retired_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.PrimaryKeyConstraint('cohort_id',
                                    name='pk_serve_ra_worker_cohorts'),
    sqlalchemy.UniqueConstraint('deployment_uid',
                                name='uq_serve_ra_worker_cohorts_deployment'),
    sqlalchemy.CheckConstraint(
        'octet_length(cohort_id) BETWEEN 1 AND 1024 AND '
        'octet_length(deployment_uid) BETWEEN 1 AND 1024',
        name='ck_serve_ra_worker_cohorts_text_bounds'),
    sqlalchemy.CheckConstraint(
        f'{_json_object_shape("cohort_identity")} AND '
        f"cohort_identity_sha256 ~ '{_SHA256_PATTERN}'",
        name='ck_serve_ra_worker_cohorts_identity'),
    sqlalchemy.CheckConstraint(
        f'{_json_object_shape("registration_attestations")} AND '
        f"registration_attestations_sha256 ~ '{_SHA256_PATTERN}'",
        name='ck_serve_ra_worker_cohorts_attestations'),
    sqlalchemy.CheckConstraint(
        f'lifecycle_state IN ({_sql_values(_COHORT_STATES)})',
        name='ck_serve_ra_worker_cohorts_state'),
    sqlalchemy.CheckConstraint('revision > 0',
                               name='ck_serve_ra_worker_cohorts_revision'),
    sqlalchemy.CheckConstraint(
        'state_changed_at >= created_at AND '
        "((lifecycle_state = 'RETIRED' AND retired_at IS NOT NULL) OR "
        "(lifecycle_state <> 'RETIRED' AND retired_at IS NULL)) AND "
        '(retired_at IS NULL OR retired_at >= state_changed_at)',
        name='ck_serve_ra_worker_cohorts_timestamps'),
)
sqlalchemy.Index('ix_serve_ra_worker_cohorts_state',
                 worker_cohorts_table.c.lifecycle_state,
                 worker_cohorts_table.c.state_changed_at,
                 worker_cohorts_table.c.cohort_id)

worker_cohort_refs_table = sqlalchemy.Table(
    'serve_resource_action_worker_cohort_refs',
    head_metadata,
    sqlalchemy.Column('decision_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('cohort_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('replica_incarnation',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('desired_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('action_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('controller_owner_fence', sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('lifecycle_epoch', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('preparation_capability_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('reference_state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('revision',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='1'),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('bound_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('released_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.PrimaryKeyConstraint('decision_id',
                                    name='pk_serve_ra_worker_cohort_refs'),
    sqlalchemy.ForeignKeyConstraint(
        ['cohort_id'], ['serve_resource_action_worker_cohorts.cohort_id'],
        ondelete='RESTRICT',
        name='fk_serve_ra_worker_cohort_refs_cohort'),
    sqlalchemy.CheckConstraint(
        f"service_hash ~ '{_UUID_PATTERN}' AND "
        'octet_length(cohort_id) BETWEEN 1 AND 1024 AND '
        'octet_length(controller_owner_fence) BETWEEN 1 AND 1024',
        name='ck_serve_ra_worker_cohort_refs_identity'),
    sqlalchemy.CheckConstraint(
        'desired_generation > 0 AND lifecycle_epoch > 0 AND revision > 0',
        name='ck_serve_ra_worker_cohort_refs_counters'),
    sqlalchemy.CheckConstraint(
        f"preparation_capability_sha256 ~ '{_SHA256_PATTERN}'",
        name='ck_serve_ra_worker_cohort_refs_capability'),
    sqlalchemy.CheckConstraint("action_type IN ('launch', 'down')",
                               name='ck_serve_ra_worker_cohort_refs_action'),
    sqlalchemy.CheckConstraint(
        f'reference_state IN ({_sql_values(_COHORT_REFERENCE_STATES)})',
        name='ck_serve_ra_worker_cohort_refs_state'),
    sqlalchemy.CheckConstraint(
        "(reference_state = 'PREPARING' AND bound_at IS NULL AND "
        'released_at IS NULL) OR '
        "(reference_state IN ('SHADOW_ACTIVE', 'ACTION_ACTIVE') AND "
        'bound_at IS NOT NULL AND released_at IS NULL) OR '
        "(reference_state = 'RELEASED' AND released_at IS NOT NULL)",
        name='ck_serve_ra_worker_cohort_refs_state_shape'),
    sqlalchemy.CheckConstraint(
        '(bound_at IS NULL OR bound_at >= created_at) AND '
        '(released_at IS NULL OR '
        'released_at >= COALESCE(bound_at, created_at))',
        name='ck_serve_ra_worker_cohort_refs_timestamps'),
)
sqlalchemy.Index(
    'ix_serve_ra_worker_cohort_refs_active',
    worker_cohort_refs_table.c.cohort_id,
    worker_cohort_refs_table.c.decision_id,
    postgresql_where=sqlalchemy.text("reference_state <> 'RELEASED'"))

shadow_coverage_table = sqlalchemy.Table(
    'serve_resource_action_shadow_coverage',
    head_metadata,
    sqlalchemy.Column('decision_id',
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
    sqlalchemy.Column('normalizer_contract_version',
                      sqlalchemy.SmallInteger,
                      nullable=False),
    sqlalchemy.Column('normalization_outcome', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('not_representable_reason', sqlalchemy.Text),
    sqlalchemy.Column('worker_cohort_ref_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('admitted_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.PrimaryKeyConstraint('decision_id',
                                    name='pk_serve_ra_shadow_coverage'),
    sqlalchemy.ForeignKeyConstraint(
        ['worker_cohort_ref_id'],
        ['serve_resource_action_worker_cohort_refs.decision_id'],
        ondelete='RESTRICT',
        name='fk_serve_ra_shadow_coverage_cohort_ref'),
    sqlalchemy.UniqueConstraint('service_hash',
                                'service_incarnation',
                                'replica_id',
                                'replica_incarnation',
                                'desired_generation',
                                'action_type',
                                name='uq_serve_ra_shadow_coverage_identity'),
    sqlalchemy.CheckConstraint(
        'octet_length(service_name) BETWEEN 1 AND 256 AND '
        'octet_length(service_hash) = 36',
        name='ck_serve_ra_shadow_coverage_identity_bounds'),
    sqlalchemy.CheckConstraint(
        f"service_hash ~ '{_UUID_PATTERN}' AND "
        'service_hash = CAST(service_incarnation AS TEXT)',
        name='ck_serve_ra_shadow_coverage_incarnation'),
    sqlalchemy.CheckConstraint(
        'replica_id >= 0 AND desired_generation > 0 AND '
        'normalizer_contract_version = 1',
        name='ck_serve_ra_shadow_coverage_counters'),
    sqlalchemy.CheckConstraint("action_type IN ('launch', 'down')",
                               name='ck_serve_ra_shadow_coverage_action'),
    sqlalchemy.CheckConstraint(
        f'normalization_outcome IN '
        f'({_sql_values(_NORMALIZATION_OUTCOMES)})',
        name='ck_serve_ra_shadow_coverage_outcome'),
    sqlalchemy.CheckConstraint(
        "(normalization_outcome = 'REPRESENTABLE' AND "
        'not_representable_reason IS NULL) OR '
        "(normalization_outcome = 'NOT_REPRESENTABLE' AND "
        'not_representable_reason IS NOT NULL AND '
        "((action_type = 'launch' AND not_representable_reason IN "
        f'({_sql_values(_LAUNCH_NOT_REPRESENTABLE_REASONS)})) OR '
        "(action_type = 'down' AND not_representable_reason IN "
        f'({_sql_values(_DOWN_NOT_REPRESENTABLE_REASONS)}))))',
        name='ck_serve_ra_shadow_coverage_reason'),
    sqlalchemy.CheckConstraint(
        'worker_cohort_ref_id IS NULL OR worker_cohort_ref_id = decision_id',
        name='ck_serve_ra_shadow_coverage_cohort_ref'),
)
sqlalchemy.Index('ix_serve_ra_shadow_coverage_promotion',
                 shadow_coverage_table.c.service_name,
                 shadow_coverage_table.c.service_hash,
                 shadow_coverage_table.c.admitted_at,
                 shadow_coverage_table.c.decision_id)
sqlalchemy.Index('ix_serve_ra_shadow_coverage_blockers',
                 shadow_coverage_table.c.service_name,
                 shadow_coverage_table.c.service_hash,
                 shadow_coverage_table.c.admitted_at,
                 shadow_coverage_table.c.decision_id,
                 postgresql_where=sqlalchemy.text(
                     "normalization_outcome = 'NOT_REPRESENTABLE'"))
sqlalchemy.Index(
    'ix_serve_ra_shadow_coverage_unlinked',
    shadow_coverage_table.c.admitted_at,
    shadow_coverage_table.c.decision_id,
    postgresql_where=sqlalchemy.text('worker_cohort_ref_id IS NULL'))

shadow_coverage_attempts_table = sqlalchemy.Table(
    'serve_resource_action_shadow_coverage_attempts',
    head_metadata,
    sqlalchemy.Column('decision_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('request_sequence', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('logical_attempt', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('request_role', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('phase', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('legacy_request_id', sqlalchemy.Text),
    sqlalchemy.Column('terminal_request_status', sqlalchemy.Text),
    sqlalchemy.Column('retry_disposition', sqlalchemy.Text),
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
    sqlalchemy.PrimaryKeyConstraint(
        'decision_id',
        'request_sequence',
        name='pk_serve_ra_shadow_coverage_attempts'),
    sqlalchemy.ForeignKeyConstraint(
        ['decision_id'], ['serve_resource_action_shadow_coverage.decision_id'],
        ondelete='CASCADE',
        name='fk_serve_ra_shadow_coverage_attempts_coverage'),
    sqlalchemy.CheckConstraint('request_sequence > 0 AND logical_attempt > 0',
                               name='ck_serve_ra_shadow_cov_attempts_counters'),
    sqlalchemy.CheckConstraint(
        "request_role IN ('PRIMARY_LAUNCH', 'PRIMARY_DOWN', "
        "'LAUNCH_CLEANUP_DOWN')",
        name='ck_serve_ra_shadow_cov_attempts_role'),
    sqlalchemy.CheckConstraint(f'phase IN ({_sql_values(_CHILD_PHASES)})',
                               name='ck_serve_ra_shadow_cov_attempts_phase'),
    sqlalchemy.CheckConstraint(
        '(legacy_request_id IS NULL OR '
        'octet_length(legacy_request_id) BETWEEN 1 AND 128)',
        name='ck_serve_ra_shadow_cov_attempts_request_id'),
    sqlalchemy.CheckConstraint(
        'terminal_request_status IS NULL OR '
        f'terminal_request_status IN '
        f'({_sql_values(_TERMINAL_REQUEST_STATUSES)})',
        name='ck_serve_ra_shadow_cov_attempts_terminal_status'),
    sqlalchemy.CheckConstraint(
        'retry_disposition IS NULL OR '
        f'retry_disposition IN ({_sql_values(_RETRY_DISPOSITIONS)})',
        name='ck_serve_ra_shadow_cov_attempts_retry'),
    sqlalchemy.CheckConstraint(
        "(phase = 'PRE_SUBMIT' AND legacy_request_id IS NULL AND "
        'request_bound_at IS NULL AND completed_at IS NULL AND '
        'terminal_request_status IS NULL AND retry_disposition IS NULL) OR '
        "(phase = 'REQUEST_BOUND' AND legacy_request_id IS NOT NULL AND "
        'request_bound_at IS NOT NULL AND completed_at IS NULL AND '
        'terminal_request_status IS NULL AND retry_disposition IS NULL) OR '
        "(phase = 'COMPLETE' AND legacy_request_id IS NOT NULL AND "
        'request_bound_at IS NOT NULL AND completed_at IS NOT NULL AND '
        'terminal_request_status IS NOT NULL AND '
        'retry_disposition IS NOT NULL) OR '
        "(phase IN ('ABANDONED_PRE_SUBMIT', "
        "'REQUEST_ASSOCIATION_UNKNOWN') AND legacy_request_id IS NULL AND "
        'request_bound_at IS NULL AND completed_at IS NOT NULL AND '
        'terminal_request_status IS NULL AND retry_disposition IS NULL)',
        name='ck_serve_ra_shadow_cov_attempts_phase_shape'),
    sqlalchemy.CheckConstraint(
        'updated_at >= admitted_at AND '
        '(request_bound_at IS NULL OR request_bound_at >= admitted_at) AND '
        '(completed_at IS NULL OR completed_at >= admitted_at)',
        name='ck_serve_ra_shadow_cov_attempts_timestamps'),
)
sqlalchemy.Index(
    'uq_serve_ra_shadow_cov_attempts_request',
    shadow_coverage_attempts_table.c.legacy_request_id,
    unique=True,
    postgresql_where=sqlalchemy.text('legacy_request_id IS NOT NULL'))
sqlalchemy.Index('ix_serve_ra_shadow_cov_attempts_stale',
                 shadow_coverage_attempts_table.c.phase,
                 shadow_coverage_attempts_table.c.admitted_at,
                 shadow_coverage_attempts_table.c.decision_id,
                 shadow_coverage_attempts_table.c.request_sequence,
                 postgresql_where=sqlalchemy.text(
                     "phase IN ('PRE_SUBMIT', 'REQUEST_BOUND')"))

# A represented parent is retained until its same-ID decision coverage row can
# be deleted by the typed retention transaction.
shadow_samples_head_table.append_constraint(
    sqlalchemy.ForeignKeyConstraint(
        ['would_be_action_id'],
        ['serve_resource_action_shadow_coverage.decision_id'],
        ondelete='RESTRICT',
        name='fk_serve_ra_shadow_samples_coverage'))

# Uppercase aliases match the central request-store schema catalog style.  The
# staged graph lets migration 033 adopt an empty feature-draft sample/attempt
# pair before it installs the complete head graph.
STAGED_SERVE033_METADATA = metadata
STAGED_SHADOW_SAMPLES = shadow_samples_table
STAGED_SHADOW_ATTEMPTS = shadow_attempts_table
SHADOW_SAMPLES = shadow_samples_head_table
SHADOW_ATTEMPTS = shadow_attempts_head_table
WORKER_COHORTS = worker_cohorts_table
WORKER_COHORT_REFS = worker_cohort_refs_table
SHADOW_COVERAGE = shadow_coverage_table
SHADOW_COVERAGE_ATTEMPTS = shadow_coverage_attempts_table
RESOURCE_ACTION_STATE_METADATA = head_metadata


def service_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return fresh revision-033 columns for the existing services table."""
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


def replica_coverage_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return fresh inert revision-033 coverage-link columns."""
    return (
        sqlalchemy.Column('launch_shadow_coverage_id',
                          sqlalchemy.Uuid(as_uuid=True),
                          nullable=True),
        sqlalchemy.Column('down_shadow_coverage_id',
                          sqlalchemy.Uuid(as_uuid=True),
                          nullable=True),
    )
