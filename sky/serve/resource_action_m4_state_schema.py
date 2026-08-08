"""PostgreSQL schema catalog for SkyServe resource-action M4 authority.

Revision 038 deliberately keeps this metadata separate from the frozen
revision-033 evidence graph.  Historical migration 033 enumerates its metadata
at runtime, so appending any of the objects below to that graph would change a
fresh database's historical revision-033 catalog.
"""

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.serve import resource_action_state_schema

_SHA256_PATTERN = '^[0-9a-f]{64}$'
_UUID_PATTERN = ('^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-'
                 '[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
_JSON_MAX_BYTES = 65536
_WORKER_FENCE_MAX_BYTES = 30720
_WORKER_COLD_FENCES_MAX_BYTES = 65536


def _sql_values(values: tuple[str, ...]) -> str:
    return ', '.join(f"'{value}'" for value in values)


def _json_shape(column: str,
                root: str = 'object',
                maximum_bytes: int = _JSON_MAX_BYTES) -> str:
    # CASE keeps the predicate two-valued and avoids applying JSON functions
    # to a value of the wrong root type.
    return ('(CASE WHEN jsonb_typeof('
            f'{column}) = \'{root}\' THEN '
            f'octet_length(CAST({column} AS TEXT)) <= {maximum_bytes} '
            'ELSE FALSE END) IS TRUE')


def _json_array_shape(column: str,
                      length: int,
                      maximum_bytes: int = _JSON_MAX_BYTES) -> str:
    return (
        '(CASE WHEN jsonb_typeof('
        f'{column}) = \'array\' THEN jsonb_array_length({column}) = {length} '
        f'AND octet_length(CAST({column} AS TEXT)) <= {maximum_bytes} '
        'ELSE FALSE END) IS TRUE')


def _nested_json_array_length_shape(column: str, field: str,
                                    length: int) -> str:
    return ('(CASE WHEN jsonb_typeof('
            f"{column} -> '{field}') = 'array' THEN "
            f"jsonb_array_length({column} -> '{field}') = {length} "
            'ELSE FALSE END) IS TRUE')


def _sha256_shape(column: str) -> str:
    return f"{column} ~ '{_SHA256_PATTERN}'"


def _required_json_hash_shape(column: str,
                              hash_column: str,
                              *,
                              root: str = 'object',
                              maximum_bytes: int = _JSON_MAX_BYTES) -> str:
    return (f'{_json_shape(column, root, maximum_bytes)} AND '
            f'{_sha256_shape(hash_column)}')


def _optional_json_hash_shape(column: str,
                              hash_column: str,
                              *,
                              root: str = 'object',
                              maximum_bytes: int = _JSON_MAX_BYTES) -> str:
    return (
        f'(({column} IS NULL AND {hash_column} IS NULL) OR '
        f'({column} IS NOT NULL AND {hash_column} IS NOT NULL AND '
        f'{_required_json_hash_shape(column, hash_column, root=root, maximum_bytes=maximum_bytes)}))'
    )


def service_candidate_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return fresh portable candidate-binding columns for ``services``."""
    return (
        sqlalchemy.Column('resource_action_candidate_epoch',
                          sqlalchemy.Uuid(as_uuid=True),
                          nullable=True),
        sqlalchemy.Column('resource_action_candidate_policy_sha256',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('resource_action_candidate_binding_sha256',
                          sqlalchemy.Text,
                          nullable=True),
    )


def version_spec_identity_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return fresh immutable resource-action identity columns."""
    return (
        sqlalchemy.Column('resource_action_spec_identity',
                          sqlalchemy.JSON(none_as_null=True).with_variant(
                              postgresql.JSONB(none_as_null=True),
                              'postgresql'),
                          nullable=True),
        sqlalchemy.Column('resource_action_spec_identity_sha256',
                          sqlalchemy.Text,
                          nullable=True),
    )


def replica_spec_identity_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return the fresh replica-to-version identity commitment column."""
    return (sqlalchemy.Column('resource_action_spec_identity_sha256',
                              sqlalchemy.Text,
                              nullable=True),)


def cohort_candidate_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return the fresh terminal-lifecycle timestamp column."""
    return (sqlalchemy.Column('removal_authorized_at',
                              sqlalchemy.DateTime(timezone=True),
                              nullable=True),)


def cohort_ref_authority_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return the nullable action-policy binding carried by references."""
    return (
        sqlalchemy.Column('authority_policy_epoch',
                          postgresql.UUID(as_uuid=True),
                          nullable=True),
        sqlalchemy.Column('authority_policy_sha256',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('authority_binding_sha256',
                          sqlalchemy.Text,
                          nullable=True),
    )


def coverage_candidate_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return the non-null qualification binding carried by coverage."""
    return (
        sqlalchemy.Column('candidate_epoch',
                          postgresql.UUID(as_uuid=True),
                          nullable=False),
        sqlalchemy.Column('qualification_policy_sha256',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('qualification_binding_sha256',
                          sqlalchemy.Text,
                          nullable=False),
    )


def service_candidate_check_constraints(
) -> tuple[sqlalchemy.CheckConstraint, ...]:
    """Return the closed service mode/candidate binding constraint."""
    return (sqlalchemy.CheckConstraint(
        "((resource_action_mode = 'legacy' AND "
        'resource_action_candidate_epoch IS NULL AND '
        'resource_action_candidate_policy_sha256 IS NULL AND '
        'resource_action_candidate_binding_sha256 IS NULL) OR '
        "(resource_action_mode IN ('shadow', 'authoritative') AND "
        'resource_action_mode_changed_at IS NOT NULL AND '
        'resource_action_candidate_epoch IS NOT NULL AND '
        f"resource_action_candidate_policy_sha256 ~ '{_SHA256_PATTERN}' AND "
        f"resource_action_candidate_binding_sha256 ~ '{_SHA256_PATTERN}')) "
        'IS TRUE',
        name='ck_services_resource_action_candidate_mode'),)


def version_spec_identity_check_constraints(
) -> tuple[sqlalchemy.CheckConstraint, ...]:
    """Return the pair-null immutable version-identity constraint."""
    return (sqlalchemy.CheckConstraint(
        _optional_json_hash_shape('resource_action_spec_identity',
                                  'resource_action_spec_identity_sha256'),
        name='ck_version_specs_resource_action_identity'),)


def replica_spec_identity_check_constraints(
) -> tuple[sqlalchemy.CheckConstraint, ...]:
    """Return the replica identity/link commitment constraint."""
    return (sqlalchemy.CheckConstraint(
        '((resource_action_spec_identity_sha256 IS NULL AND '
        'launch_action_id IS NULL AND down_action_id IS NULL AND '
        'launch_shadow_coverage_id IS NULL AND '
        'down_shadow_coverage_id IS NULL AND '
        'launch_shadow_sample_id IS NULL AND '
        'down_shadow_sample_id IS NULL) OR '
        '(resource_action_spec_identity_sha256 IS NOT NULL AND '
        f"resource_action_spec_identity_sha256 ~ '{_SHA256_PATTERN}')) "
        'IS TRUE',
        name='ck_replicas_resource_action_spec_identity'),)


def coverage_candidate_check_constraints(
) -> tuple[sqlalchemy.CheckConstraint, ...]:
    """Return the qualification hash constraint for coverage rows."""
    return (sqlalchemy.CheckConstraint(
        f"qualification_policy_sha256 ~ '{_SHA256_PATTERN}' AND "
        f"qualification_binding_sha256 ~ '{_SHA256_PATTERN}'",
        name='ck_serve_ra_shadow_coverage_candidate_binding'),)


def cohort_lifecycle_check_constraints(
) -> tuple[sqlalchemy.CheckConstraint, ...]:
    """Return the V2 lifecycle / retired-V1-history shape."""
    registration_version = (
        "(jsonb_typeof(registration_attestations) = 'object') IS TRUE")
    exact_v2 = (
        "((registration_attestations -> 'version')::text = '2') IS TRUE")
    return (sqlalchemy.CheckConstraint(
        'state_changed_at >= created_at AND '
        f"((lifecycle_state IN ('REGISTERING', 'ACCEPTING', 'DRAINING') "
        f'AND {registration_version} AND {exact_v2} AND '
        'removal_authorized_at IS NULL AND retired_at IS NULL) OR '
        "(lifecycle_state = 'REMOVAL_AUTHORIZED' AND "
        f'{registration_version} AND {exact_v2} AND '
        'removal_authorized_at IS NOT NULL AND '
        'state_changed_at = removal_authorized_at AND retired_at IS NULL) OR '
        "(lifecycle_state = 'RETIRED' AND "
        f'{registration_version} AND {exact_v2} AND '
        'removal_authorized_at IS NOT NULL AND retired_at IS NOT NULL AND '
        'state_changed_at = retired_at AND '
        'retired_at >= removal_authorized_at) OR '
        "(lifecycle_state = 'RETIRED' AND "
        f'{registration_version} AND '
        "((registration_attestations -> 'version')::text = '1') IS TRUE AND "
        'removal_authorized_at IS NULL AND retired_at IS NOT NULL AND '
        'state_changed_at = retired_at))',
        name='ck_serve_ra_worker_cohorts_timestamps'),)


def cohort_ref_authority_check_constraints(
) -> tuple[sqlalchemy.CheckConstraint, ...]:
    """Return the complete state/policy-triple shape for references."""
    return (sqlalchemy.CheckConstraint(
        "((reference_state IN ('PREPARING', 'SHADOW_ACTIVE') AND "
        'authority_policy_epoch IS NULL AND '
        'authority_policy_sha256 IS NULL AND '
        'authority_binding_sha256 IS NULL) OR '
        "(reference_state = 'ACTION_ACTIVE' AND "
        'authority_policy_epoch IS NOT NULL AND '
        f"authority_policy_sha256 ~ '{_SHA256_PATTERN}' AND "
        f"authority_binding_sha256 ~ '{_SHA256_PATTERN}') OR "
        "(reference_state = 'RELEASED' AND (("
        'authority_policy_epoch IS NULL AND '
        'authority_policy_sha256 IS NULL AND '
        'authority_binding_sha256 IS NULL) OR ('
        'authority_policy_epoch IS NOT NULL AND '
        f"authority_policy_sha256 ~ '{_SHA256_PATTERN}' AND "
        f"authority_binding_sha256 ~ '{_SHA256_PATTERN}')))) IS TRUE",
        name='ck_serve_ra_worker_cohort_refs_authority_binding'),)


def cohort_ref_authority_foreign_key() -> sqlalchemy.ForeignKeyConstraint:
    """Return the full immutable action-policy tuple foreign key."""
    return sqlalchemy.ForeignKeyConstraint(
        [
            'service_hash', 'authority_policy_epoch', 'authority_policy_sha256',
            'authority_binding_sha256'
        ], [
            authority_policy_epochs_table.c.service_hash,
            authority_policy_epochs_table.c.policy_epoch,
            authority_policy_epochs_table.c.policy_sha256,
            authority_policy_epochs_table.c.authority_binding_sha256,
        ],
        ondelete='RESTRICT',
        name='fk_serve_ra_worker_cohort_refs_authority_policy')


def serve038_worker_state_check_constraints(
) -> dict[str, tuple[sqlalchemy.CheckConstraint, ...]]:
    """Return fresh CHECKs for the three durable worker-state tables."""
    lease = sqlalchemy.CheckConstraint(
        '(worker_instance_id = pod_uid AND generation > 0 AND revision > 0 '
        "AND state IN ('ACTIVE', 'REVOKED') AND "
        f'{_required_json_hash_shape("renewal_registration", "renewal_registration_sha256")} '
        "AND expires_at = renewed_at + INTERVAL '60 seconds' AND "
        "((state = 'ACTIVE' AND revision = generation AND "
        'revoked_at IS NULL AND revocation_reason IS NULL AND '
        'revocation_owner_id IS NULL AND '
        "((generation = 1 AND last_operation_kind = 'INSERT') OR "
        "(generation > 1 AND last_operation_kind = 'RENEW'))) OR "
        "(state = 'REVOKED' AND revision = generation + 1 AND "
        'revoked_at IS NOT NULL AND revoked_at >= renewed_at AND '
        "revocation_reason IN ('STALE_HANDOFF', 'CANDIDATE_ABANDONED', "
        "'COHORT_COLD_RECOVERY', 'COHORT_REMOVAL') AND "
        "last_operation_kind = 'REVOKE' AND "
        "((revocation_reason = 'COHORT_REMOVAL' AND "
        'revocation_owner_id IS NULL) OR '
        "(revocation_reason IN ('STALE_HANDOFF', 'CANDIDATE_ABANDONED', "
        "'COHORT_COLD_RECOVERY') AND revocation_owner_id IS NOT NULL))))) "
        'IS TRUE',
        name='serve038_worker_lease_closed_shape_ck')

    handoff_scalar = sqlalchemy.CheckConstraint(
        '(chain_sequence > 0 AND source_cohort_revision > 0 AND '
        'source_registration_set_revision = source_cohort_revision AND '
        "((source_registration_set -> 'revision')::text = "
        'CAST(source_cohort_revision AS TEXT)) IS TRUE AND '
        "source_cohort_state IN ('ACCEPTING', 'DRAINING') AND "
        'stale_worker_instance_id = stale_pod_uid AND '
        'survivor_worker_instance_id = survivor_pod_uid AND '
        'candidate_worker_instance_id = candidate_pod_uid AND '
        'stale_worker_instance_id <> survivor_worker_instance_id AND '
        'stale_worker_instance_id <> candidate_worker_instance_id AND '
        'survivor_worker_instance_id <> candidate_worker_instance_id AND '
        'opened_at = fenced_at AND predecessor_handoff_id IS DISTINCT FROM '
        'handoff_id AND '
        "((stale_fence_disposition = 'NEWLY_REVOKED' AND "
        'predecessor_handoff_id IS NULL AND chain_sequence = 1) OR '
        "(stale_fence_disposition = 'ADOPTED_ABANDONED_PREDECESSOR' AND "
        'predecessor_handoff_id IS NOT NULL AND chain_sequence > 1))) '
        'IS TRUE',
        name='serve038_worker_handoff_scalar_lineage_ck')

    required_handoff_json = ' AND '.join((
        _required_json_hash_shape('source_registration_set',
                                  'source_registration_set_sha256'),
        _nested_json_array_length_shape('source_registration_set', 'workers',
                                        2),
        _required_json_hash_shape('stale_authority_fence',
                                  'stale_authority_fence_sha256',
                                  maximum_bytes=_WORKER_FENCE_MAX_BYTES),
        _required_json_hash_shape('stale_uid_absence_proof',
                                  'stale_uid_absence_proof_sha256'),
        _required_json_hash_shape('candidate_registration',
                                  'candidate_registration_sha256'),
    ))
    optional_handoff_json = ' AND '.join((
        _optional_json_hash_shape('survivor_registration',
                                  'survivor_registration_sha256'),
        _optional_json_hash_shape('final_registration_set',
                                  'final_registration_set_sha256'),
        _optional_json_hash_shape('final_deployment_snapshot',
                                  'final_deployment_snapshot_sha256'),
        _optional_json_hash_shape('candidate_absence_proof',
                                  'candidate_absence_proof_sha256'),
        _optional_json_hash_shape('survivor_absence_proof',
                                  'survivor_absence_proof_sha256'),
        _optional_json_hash_shape('candidate_zero_effect_proof',
                                  'candidate_zero_effect_proof_sha256'),
        '(final_registration_set IS NULL OR ' + _nested_json_array_length_shape(
            'final_registration_set', 'workers', 2) + ')',
    ))
    open_shape = (
        "(handoff_state = 'OPEN' AND revision = 1 AND "
        'survivor_registration IS NULL AND survivor_acknowledged_at IS NULL '
        'AND final_registration_set IS NULL AND '
        'final_registration_set_revision IS NULL AND '
        'final_deployment_snapshot IS NULL AND '
        'committed_cohort_revision IS NULL AND '
        'candidate_absence_proof IS NULL AND '
        'survivor_absence_proof IS NULL AND '
        'candidate_zero_effect_proof IS NULL AND '
        'abandonment_reason IS NULL AND terminal_at IS NULL)')
    ready_shape = ("(handoff_state = 'READY' AND revision = 2 AND "
                   'survivor_registration IS NOT NULL AND '
                   'survivor_acknowledged_at IS NOT NULL AND '
                   'final_registration_set IS NULL AND '
                   'final_registration_set_revision IS NULL AND '
                   'final_deployment_snapshot IS NULL AND '
                   'committed_cohort_revision IS NULL AND '
                   'candidate_absence_proof IS NULL AND '
                   'survivor_absence_proof IS NULL AND '
                   'candidate_zero_effect_proof IS NULL AND '
                   'abandonment_reason IS NULL AND terminal_at IS NULL)')
    completed_shape = (
        "(handoff_state = 'COMPLETED' AND revision = 3 AND "
        'survivor_registration IS NOT NULL AND '
        'survivor_acknowledged_at IS NOT NULL AND '
        'final_registration_set IS NOT NULL AND '
        'final_registration_set_revision IS NOT NULL AND '
        'final_deployment_snapshot IS NOT NULL AND '
        'committed_cohort_revision IS NOT NULL AND '
        'candidate_absence_proof IS NULL AND '
        'survivor_absence_proof IS NULL AND '
        'candidate_zero_effect_proof IS NULL AND '
        'abandonment_reason IS NULL AND terminal_at IS NOT NULL)')
    abandoned_shape = (
        "(handoff_state = 'ABANDONED' AND "
        '((revision = 2 AND survivor_registration IS NULL AND '
        'survivor_acknowledged_at IS NULL) OR '
        '(revision = 3 AND survivor_registration IS NOT NULL AND '
        'survivor_acknowledged_at IS NOT NULL)) AND '
        'final_registration_set IS NULL AND '
        'final_registration_set_revision IS NULL AND '
        'final_deployment_snapshot IS NULL AND '
        'committed_cohort_revision IS NULL AND '
        'candidate_absence_proof IS NOT NULL AND '
        'candidate_zero_effect_proof IS NOT NULL AND '
        "abandonment_reason IN ('candidate_absent_zero_effect', "
        "'both_members_lost_cold_recovery_required') AND "
        "((abandonment_reason = 'candidate_absent_zero_effect' AND "
        'survivor_absence_proof IS NULL) OR '
        "(abandonment_reason = 'both_members_lost_cold_recovery_required' "
        'AND survivor_absence_proof IS NOT NULL)) AND terminal_at IS NOT NULL)')
    handoff_pairing = sqlalchemy.CheckConstraint(
        f'{required_handoff_json} AND {optional_handoff_json} AND '
        f'(({open_shape} OR {ready_shape} OR {completed_shape} OR '
        f'{abandoned_shape})) IS TRUE',
        name='serve038_worker_handoff_pairing_state_ck')

    handoff_terminal = sqlalchemy.CheckConstraint(
        "(((handoff_state = 'COMPLETED' AND "
        'final_registration_set_revision = source_cohort_revision + 1 AND '
        'committed_cohort_revision = source_cohort_revision + 1 AND '
        "((final_registration_set -> 'revision')::text = "
        'CAST(final_registration_set_revision AS TEXT)) IS TRUE) OR '
        "(handoff_state <> 'COMPLETED')) AND "
        "((handoff_state IN ('OPEN', 'READY') AND terminal_at IS NULL) OR "
        "(handoff_state IN ('COMPLETED', 'ABANDONED') AND "
        'terminal_at IS NOT NULL))) IS TRUE',
        name='serve038_worker_handoff_terminal_revision_ck')

    cold_required = sqlalchemy.CheckConstraint(
        f'{_required_json_hash_shape("source_registration_set", "source_registration_set_sha256")} AND '
        f'{_nested_json_array_length_shape("source_registration_set", "workers", 2)} AND '
        f'{_json_array_shape("old_uid_absence_proofs", 2, _WORKER_COLD_FENCES_MAX_BYTES)} AND '
        f'{_sha256_shape("old_uid_absence_proofs_sha256")} AND '
        f'{_json_array_shape("old_authority_fences", 2, _WORKER_COLD_FENCES_MAX_BYTES)} AND '
        f'{_sha256_shape("old_authority_fences_sha256")} AND '
        f'{_required_json_hash_shape("final_registration_set", "final_registration_set_sha256")} AND '
        f'{_nested_json_array_length_shape("final_registration_set", "workers", 2)} AND '
        f'{_required_json_hash_shape("final_deployment_snapshot", "final_deployment_snapshot_sha256")}',
        name='serve038_worker_cold_required_json_ck')
    cold_revision = sqlalchemy.CheckConstraint(
        'source_cohort_revision > 0 AND '
        'source_registration_set_revision = source_cohort_revision AND '
        "source_cohort_state IN ('ACCEPTING', 'DRAINING') AND "
        'final_registration_set_revision = source_cohort_revision + 1 AND '
        'committed_cohort_revision = source_cohort_revision + 1 AND '
        "((source_registration_set -> 'revision')::text = "
        'CAST(source_cohort_revision AS TEXT)) IS TRUE AND '
        "((final_registration_set -> 'revision')::text = "
        'CAST(final_registration_set_revision AS TEXT)) IS TRUE',
        name='serve038_worker_cold_revision_shape_ck')
    return {
        'serve_resource_action_worker_registration_leases': (lease,),
        'serve_resource_action_worker_registration_handoffs':
            (handoff_scalar, handoff_pairing, handoff_terminal),
        'serve_resource_action_worker_registration_cold_recoveries':
            (cold_required, cold_revision),
    }


SERVE038_METADATA = sqlalchemy.MetaData()

authority_policy_epochs_table = sqlalchemy.Table(
    'serve_resource_action_authority_policy_epochs',
    SERVE038_METADATA,
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('policy_epoch',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('predecessor_policy_epoch',
                      postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('policy',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('policy_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('authority_binding_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('rotation_proof',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('rotation_proof_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('nonterminal_inventory',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('nonterminal_inventory_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('reason', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('policy_state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('admission_state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('admission_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('last_operation_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('last_operation_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('admission_changed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('activated_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('superseded_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.PrimaryKeyConstraint('service_hash',
                                    'policy_epoch',
                                    name='pk_serve_ra_authority_policy_epochs'),
    sqlalchemy.UniqueConstraint(
        'service_hash',
        'policy_epoch',
        'policy_sha256',
        'authority_binding_sha256',
        name='uq_serve_ra_authority_policy_epochs_binding'),
    sqlalchemy.ForeignKeyConstraint(
        ['service_hash', 'predecessor_policy_epoch'], [
            'serve_resource_action_authority_policy_epochs.service_hash',
            'serve_resource_action_authority_policy_epochs.policy_epoch'
        ],
        name='fk_serve_ra_authority_policy_epochs_predecessor'),
    sqlalchemy.CheckConstraint(
        f"(service_hash ~ '{_UUID_PATTERN}' AND "
        f'{_required_json_hash_shape("policy", "policy_sha256")} AND '
        f'{_sha256_shape("authority_binding_sha256")} AND '
        f'{_required_json_hash_shape("rotation_proof", "rotation_proof_sha256")} AND '
        f'{_required_json_hash_shape("nonterminal_inventory", "nonterminal_inventory_sha256")}) IS TRUE',
        name='ck_serve_ra_authority_policy_epochs_payloads'),
    sqlalchemy.CheckConstraint(
        'predecessor_policy_epoch IS NULL OR '
        'predecessor_policy_epoch <> policy_epoch',
        name='ck_serve_ra_authority_policy_epochs_predecessor'),
    sqlalchemy.CheckConstraint(
        "((reason = 'INITIAL_PROMOTION' AND "
        'predecessor_policy_epoch IS NULL) OR '
        "(reason = 'COMPATIBLE_IMAGE_ROTATION' AND "
        'predecessor_policy_epoch IS NOT NULL)) IS TRUE',
        name='ck_serve_ra_authority_policy_epochs_reason'),
    sqlalchemy.CheckConstraint(
        '(admission_revision > 0 AND '
        "((policy_state = 'ACTIVE' AND admission_state = 'OPEN' AND "
        "((admission_revision = 1 AND last_operation_kind = 'ACTIVATE') OR "
        "(admission_revision > 1 AND last_operation_kind = 'REOPEN'))) OR "
        "(policy_state = 'ACTIVE' AND admission_state = 'DRAINING' AND "
        "admission_revision > 1 AND last_operation_kind = 'DRAIN') OR "
        "(policy_state = 'ACTIVE' AND admission_state = 'CLOSED' AND "
        "admission_revision > 1 AND last_operation_kind = 'CLOSE') OR "
        "(policy_state = 'SUPERSEDED' AND admission_state = 'CLOSED' AND "
        "admission_revision > 1 AND last_operation_kind = 'SUPERSEDE'))) "
        'IS TRUE',
        name='ck_serve_ra_authority_policy_epochs_admission'),
    sqlalchemy.CheckConstraint(
        "((policy_state = 'ACTIVE' AND "
        "admission_state IN ('OPEN', 'DRAINING', 'CLOSED') AND "
        'activated_at IS NOT NULL AND activated_at = created_at AND '
        'admission_changed_at >= activated_at AND superseded_at IS NULL) OR '
        "(policy_state = 'SUPERSEDED' AND admission_state = 'CLOSED' AND "
        'activated_at IS NOT NULL AND activated_at = created_at AND '
        'admission_changed_at >= activated_at AND '
        'superseded_at IS NOT NULL AND '
        'superseded_at >= admission_changed_at)) IS TRUE',
        name='ck_serve_ra_authority_policy_epochs_timestamps'),
)
sqlalchemy.Index(
    'uq_serve_ra_authority_policy_epochs_predecessor',
    authority_policy_epochs_table.c.service_hash,
    authority_policy_epochs_table.c.predecessor_policy_epoch,
    unique=True,
    postgresql_where=sqlalchemy.text('predecessor_policy_epoch IS NOT NULL'))
sqlalchemy.Index(
    'uq_serve_ra_authority_policy_epochs_root',
    authority_policy_epochs_table.c.service_hash,
    unique=True,
    postgresql_where=sqlalchemy.text('predecessor_policy_epoch IS NULL'))
sqlalchemy.Index('uq_serve_ra_authority_policy_epochs_active',
                 authority_policy_epochs_table.c.service_hash,
                 unique=True,
                 postgresql_where=sqlalchemy.text("policy_state = 'ACTIVE'"))

_worker_checks = serve038_worker_state_check_constraints()

worker_registration_leases_table = sqlalchemy.Table(
    'serve_resource_action_worker_registration_leases',
    SERVE038_METADATA,
    sqlalchemy.Column('cohort_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('worker_instance_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('pod_uid', postgresql.UUID(as_uuid=True), nullable=False),
    sqlalchemy.Column('generation', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('renewal_registration',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('renewal_registration_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('renewed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('expires_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('revoked_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('revocation_reason', sqlalchemy.Text),
    sqlalchemy.Column('revocation_owner_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('last_operation_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('last_operation_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('revision', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.PrimaryKeyConstraint(
        'cohort_id',
        'worker_instance_id',
        name='pk_serve_ra_worker_registration_leases'),
    sqlalchemy.UniqueConstraint(
        'cohort_id',
        'pod_uid',
        name='uq_serve_ra_worker_registration_leases_pod'),
    sqlalchemy.ForeignKeyConstraint(
        ['cohort_id'],
        [resource_action_state_schema.WORKER_COHORTS.c.cohort_id],
        ondelete='RESTRICT',
        name='fk_serve_ra_worker_registration_leases_cohort'),
    *_worker_checks['serve_resource_action_worker_registration_leases'],
)
sqlalchemy.Index('ix_serve_ra_worker_registration_leases_active_expiry',
                 worker_registration_leases_table.c.cohort_id,
                 worker_registration_leases_table.c.expires_at,
                 postgresql_where=sqlalchemy.text("state = 'ACTIVE'"))

worker_registration_handoffs_table = sqlalchemy.Table(
    'serve_resource_action_worker_registration_handoffs',
    SERVE038_METADATA,
    sqlalchemy.Column('cohort_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('handoff_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('predecessor_handoff_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('chain_sequence', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('stale_fence_disposition',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('source_cohort_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('source_cohort_state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_registration_set_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('source_registration_set',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('source_registration_set_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('stale_worker_instance_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('stale_pod_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('stale_pod_uid',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('survivor_worker_instance_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('survivor_pod_uid',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('candidate_worker_instance_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('candidate_pod_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('candidate_pod_uid',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('stale_authority_fence',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('stale_authority_fence_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('stale_uid_absence_proof',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('stale_uid_absence_proof_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('candidate_registration',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('candidate_registration_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('survivor_registration',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('survivor_registration_sha256', sqlalchemy.Text),
    sqlalchemy.Column('handoff_state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('final_registration_set',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('final_registration_set_sha256', sqlalchemy.Text),
    sqlalchemy.Column('final_registration_set_revision', sqlalchemy.BigInteger),
    sqlalchemy.Column('final_deployment_snapshot',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('final_deployment_snapshot_sha256', sqlalchemy.Text),
    sqlalchemy.Column('committed_cohort_revision', sqlalchemy.BigInteger),
    sqlalchemy.Column('candidate_absence_proof',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('candidate_absence_proof_sha256', sqlalchemy.Text),
    sqlalchemy.Column('survivor_absence_proof',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('survivor_absence_proof_sha256', sqlalchemy.Text),
    sqlalchemy.Column('candidate_zero_effect_proof',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('candidate_zero_effect_proof_sha256', sqlalchemy.Text),
    sqlalchemy.Column('abandonment_reason', sqlalchemy.Text),
    sqlalchemy.Column('revision', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('opened_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('fenced_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('survivor_acknowledged_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('terminal_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.PrimaryKeyConstraint(
        'cohort_id',
        'handoff_id',
        name='pk_serve_ra_worker_registration_handoffs'),
    sqlalchemy.UniqueConstraint(
        'cohort_id',
        'source_cohort_revision',
        'chain_sequence',
        name='uq_serve_ra_worker_handoffs_source_sequence'),
    sqlalchemy.UniqueConstraint('cohort_id',
                                'candidate_pod_uid',
                                name='uq_serve_ra_worker_handoffs_candidate'),
    sqlalchemy.ForeignKeyConstraint(
        ['cohort_id'],
        [resource_action_state_schema.WORKER_COHORTS.c.cohort_id],
        ondelete='RESTRICT',
        name='fk_serve_ra_worker_handoffs_cohort'),
    sqlalchemy.ForeignKeyConstraint(
        ['cohort_id', 'predecessor_handoff_id'], [
            'serve_resource_action_worker_registration_handoffs.cohort_id',
            'serve_resource_action_worker_registration_handoffs.handoff_id'
        ],
        ondelete='RESTRICT',
        name='fk_serve_ra_worker_handoffs_predecessor'),
    *_worker_checks['serve_resource_action_worker_registration_handoffs'],
)
sqlalchemy.Index(
    'uq_serve_ra_worker_handoffs_predecessor',
    worker_registration_handoffs_table.c.cohort_id,
    worker_registration_handoffs_table.c.predecessor_handoff_id,
    unique=True,
    postgresql_where=sqlalchemy.text('predecessor_handoff_id IS NOT NULL'))
sqlalchemy.Index(
    'uq_serve_ra_worker_handoffs_nonterminal',
    worker_registration_handoffs_table.c.cohort_id,
    unique=True,
    postgresql_where=sqlalchemy.text("handoff_state IN ('OPEN', 'READY')"))

worker_registration_cold_recoveries_table = sqlalchemy.Table(
    'serve_resource_action_worker_registration_cold_recoveries',
    SERVE038_METADATA,
    sqlalchemy.Column('cohort_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('recovery_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('source_cohort_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('source_cohort_state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_registration_set_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('source_registration_set',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('source_registration_set_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('old_uid_absence_proofs',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('old_uid_absence_proofs_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('old_authority_fences',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('old_authority_fences_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('final_registration_set',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('final_registration_set_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('final_registration_set_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('final_deployment_snapshot',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('final_deployment_snapshot_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('committed_cohort_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('completed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.PrimaryKeyConstraint(
        'cohort_id',
        'recovery_id',
        name='pk_serve_ra_worker_registration_cold_recoveries'),
    sqlalchemy.UniqueConstraint(
        'cohort_id',
        'source_cohort_revision',
        name='uq_serve_ra_worker_cold_recoveries_source'),
    sqlalchemy.ForeignKeyConstraint(
        ['cohort_id'],
        [resource_action_state_schema.WORKER_COHORTS.c.cohort_id],
        ondelete='RESTRICT',
        name='fk_serve_ra_worker_cold_recoveries_cohort'),
    *_worker_checks[
        'serve_resource_action_worker_registration_cold_recoveries'],
)

crash_canary_runs_table = sqlalchemy.Table(
    'serve_resource_action_crash_canary_runs',
    SERVE038_METADATA,
    sqlalchemy.Column('service_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_incarnation',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('candidate_epoch',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('boundary_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('run_id', postgresql.UUID(as_uuid=True), nullable=False),
    sqlalchemy.Column('subject_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('action_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('action_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('attempt', sqlalchemy.Integer),
    sqlalchemy.Column('request_id', sqlalchemy.Text),
    sqlalchemy.Column('qualification_policy_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('qualification_binding_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('injection_nonce_sha256', sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('run_state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('injection_receipt', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('injection_receipt_sha256', sqlalchemy.Text),
    sqlalchemy.Column('verification_evidence',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('verification_evidence_sha256', sqlalchemy.Text),
    sqlalchemy.Column('outcome', sqlalchemy.Text),
    sqlalchemy.Column('revision', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('started_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('completed_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.PrimaryKeyConstraint('service_hash',
                                    'candidate_epoch',
                                    'boundary_id',
                                    'run_id',
                                    name='pk_serve_ra_crash_canary_runs'),
    sqlalchemy.UniqueConstraint('run_id',
                                name='uq_serve_ra_crash_canary_runs_id'),
    sqlalchemy.CheckConstraint(
        'octet_length(service_name) BETWEEN 1 AND 256 AND '
        'octet_length(boundary_id) BETWEEN 1 AND 256 AND '
        f"service_hash ~ '{_UUID_PATTERN}' AND "
        'service_hash = CAST(service_incarnation AS TEXT) AND '
        f"action_kind IN ('launch', 'down') AND "
        f"qualification_policy_sha256 ~ '{_SHA256_PATTERN}' AND "
        f"qualification_binding_sha256 ~ '{_SHA256_PATTERN}' AND "
        f"injection_nonce_sha256 ~ '{_SHA256_PATTERN}'",
        name='ck_serve_ra_crash_canary_runs_identity'),
    sqlalchemy.CheckConstraint(
        "((subject_kind = 'service' AND action_id IS NULL AND "
        'attempt IS NULL AND request_id IS NULL) OR '
        "(subject_kind = 'action' AND action_id IS NOT NULL AND "
        'attempt IS NULL AND request_id IS NULL) OR '
        "(subject_kind = 'request' AND action_id IS NOT NULL AND "
        'attempt > 0 AND request_id IS NOT NULL AND '
        f"request_id ~ '{_UUID_PATTERN}')) IS TRUE",
        name='ck_serve_ra_crash_canary_runs_subject'),
    sqlalchemy.CheckConstraint(
        _optional_json_hash_shape('injection_receipt',
                                  'injection_receipt_sha256') + ' AND ' +
        _optional_json_hash_shape('verification_evidence',
                                  'verification_evidence_sha256'),
        name='ck_serve_ra_crash_canary_runs_payloads'),
    sqlalchemy.CheckConstraint(
        "((run_state = 'STARTED' AND revision = 1 AND "
        'injection_receipt IS NULL AND verification_evidence IS NULL AND '
        'outcome IS NULL AND completed_at IS NULL) OR '
        "(run_state = 'COMPLETED' AND revision = 2 AND "
        'verification_evidence IS NOT NULL AND '
        "outcome IN ('PASS', 'FAIL', 'ABANDONED') AND "
        'completed_at IS NOT NULL AND completed_at >= started_at AND '
        "((outcome = 'PASS' AND injection_receipt IS NOT NULL) OR "
        "(outcome IN ('FAIL', 'ABANDONED'))))) IS TRUE",
        name='ck_serve_ra_crash_canary_runs_state'),
)
sqlalchemy.Index('uq_serve_ra_crash_canary_runs_started',
                 crash_canary_runs_table.c.service_hash,
                 crash_canary_runs_table.c.candidate_epoch,
                 crash_canary_runs_table.c.boundary_id,
                 unique=True,
                 postgresql_where=sqlalchemy.text("run_state = 'STARTED'"))
sqlalchemy.Index('ix_serve_ra_crash_canary_runs_promotion',
                 crash_canary_runs_table.c.service_hash,
                 crash_canary_runs_table.c.candidate_epoch,
                 crash_canary_runs_table.c.run_state,
                 crash_canary_runs_table.c.boundary_id)

attempt_exhaustions_table = sqlalchemy.Table(
    'serve_resource_action_attempt_exhaustions',
    SERVE038_METADATA,
    sqlalchemy.Column('action_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('event_code', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('attempt', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('request_id', sqlalchemy.Text, nullable=False),
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
    sqlalchemy.Column('reduction_basis', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_input_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('typed_outcome_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('result_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('settled_action_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('occurred_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.PrimaryKeyConstraint('action_id',
                                    'event_code',
                                    name='pk_serve_ra_attempt_exhaustions'),
    sqlalchemy.UniqueConstraint('request_id',
                                'event_code',
                                name='uq_serve_ra_attempt_exhaustions_request'),
    sqlalchemy.CheckConstraint(
        "event_code = 'attempt_domain_exhausted' AND "
        'attempt = 2147483647 AND replica_id >= 0 AND '
        'desired_generation > 0 AND settled_action_revision > 0 AND '
        'octet_length(service_name) BETWEEN 1 AND 256 AND '
        f"service_hash ~ '{_UUID_PATTERN}' AND "
        'service_hash = CAST(service_incarnation AS TEXT) AND '
        f"request_id ~ '{_UUID_PATTERN}' AND "
        "action_type IN ('launch', 'down') AND "
        "reduction_basis IN ('handler_retryable', 'handler_uncertain', "
        "'request_not_started', 'request_observation_required') AND "
        f"request_input_sha256 ~ '{_SHA256_PATTERN}' AND "
        f"typed_outcome_sha256 ~ '{_SHA256_PATTERN}' AND "
        f"result_sha256 ~ '{_SHA256_PATTERN}'",
        name='ck_serve_ra_attempt_exhaustions_shape'),
)
sqlalchemy.Index('ix_serve_ra_attempt_exhaustions_occurred',
                 attempt_exhaustions_table.c.occurred_at,
                 attempt_exhaustions_table.c.action_id)

AUTHORITY_POLICY_EPOCHS = authority_policy_epochs_table
WORKER_REGISTRATION_LEASES = worker_registration_leases_table
WORKER_REGISTRATION_HANDOFFS = worker_registration_handoffs_table
WORKER_REGISTRATION_COLD_RECOVERIES = (
    worker_registration_cold_recoveries_table)
CRASH_CANARY_RUNS = crash_canary_runs_table
ATTEMPT_EXHAUSTIONS = attempt_exhaustions_table

# Complete runtime reflections for the three altered Serve033 evidence
# relations live outside both the frozen Serve033 graph and the six-table 038
# creation metadata.  Runtime stores use these aliases after the 038 head.
SERVE038_ALTERED_RELATION_METADATA = sqlalchemy.MetaData()
worker_cohorts_v2_table = resource_action_state_schema.WORKER_COHORTS.to_metadata(
    SERVE038_ALTERED_RELATION_METADATA)
for _constraint in tuple(worker_cohorts_v2_table.constraints):
    if _constraint.name == 'ck_serve_ra_worker_cohorts_timestamps':
        worker_cohorts_v2_table.constraints.remove(_constraint)
worker_cohorts_v2_table.append_column(cohort_candidate_columns()[0])
for _constraint in cohort_lifecycle_check_constraints():
    worker_cohorts_v2_table.append_constraint(_constraint)

worker_cohort_refs_v2_table = (resource_action_state_schema.WORKER_COHORT_REFS.
                               to_metadata(SERVE038_ALTERED_RELATION_METADATA))
for _column in cohort_ref_authority_columns():
    worker_cohort_refs_v2_table.append_column(_column)
for _constraint in cohort_ref_authority_check_constraints():
    worker_cohort_refs_v2_table.append_constraint(_constraint)
worker_cohort_refs_v2_table.append_constraint(
    cohort_ref_authority_foreign_key())

shadow_coverage_v2_table = resource_action_state_schema.SHADOW_COVERAGE.to_metadata(
    SERVE038_ALTERED_RELATION_METADATA)
for _column in coverage_candidate_columns():
    shadow_coverage_v2_table.append_column(_column)
for _constraint in coverage_candidate_check_constraints():
    shadow_coverage_v2_table.append_constraint(_constraint)

WORKER_COHORTS_V2 = worker_cohorts_v2_table
WORKER_COHORT_REFS_V2 = worker_cohort_refs_v2_table
SHADOW_COVERAGE_V2 = shadow_coverage_v2_table

# Revision 036 owns a separate catalog.  Do not append these relations to
# SERVE038_METADATA: migration 035 imports and enumerates that historical
# metadata when a fresh database walks through the old revision.
SERVE039_METADATA = sqlalchemy.MetaData()


def worker_registration_lease_execution_owner_columns(
) -> tuple[sqlalchemy.Column, ...]:
    """Return the nullable Serve039 execution-owner commitment."""
    return (
        sqlalchemy.Column('execution_owner',
                          postgresql.JSONB(none_as_null=True),
                          nullable=True),
        sqlalchemy.Column('execution_owner_sha256',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('execution_owner_api_instance_id',
                          postgresql.UUID(as_uuid=True),
                          nullable=True),
    )


def worker_registration_lease_execution_owner_check_constraints(
) -> tuple[sqlalchemy.CheckConstraint, ...]:
    """Return the closed Serve039 lease/owner state constraint."""
    owner_shape = (
        '((execution_owner IS NULL AND execution_owner_sha256 IS NULL AND '
        'execution_owner_api_instance_id IS NULL) OR '
        '(execution_owner IS NOT NULL AND execution_owner_sha256 IS NOT NULL '
        'AND execution_owner_api_instance_id IS NOT NULL AND '
        f'{_json_shape("execution_owner")} AND '
        f'{_sha256_shape("execution_owner_sha256")} AND '
        '(CASE WHEN jsonb_typeof(execution_owner) = \'object\' THEN '
        'execution_owner_api_instance_id::text = '
        "execution_owner ->> 'api_instance_id' AND "
        'worker_instance_id::text = '
        "execution_owner ->> 'authority_worker_instance_id' AND "
        "pod_uid::text = execution_owner ->> 'pod_uid' AND "
        'execution_owner_api_instance_id <> worker_instance_id '
        'ELSE FALSE END) IS TRUE))')
    return (sqlalchemy.CheckConstraint(
        'worker_instance_id = pod_uid AND generation > 0 AND revision > 0 AND '
        f'{_required_json_hash_shape("renewal_registration", "renewal_registration_sha256")} AND '
        "expires_at = renewed_at + interval '60 seconds' AND "
        f'{owner_shape} AND '
        "(last_operation_kind NOT IN ('BIND_EXECUTION_OWNER', "
        "'SUPERSEDE_EXECUTION_OWNER') OR execution_owner IS NOT NULL) AND "
        "((state = 'ACTIVE' AND revision = generation AND revoked_at IS NULL "
        'AND revocation_reason IS NULL AND revocation_owner_id IS NULL AND '
        "((generation = 1 AND last_operation_kind = 'INSERT') OR "
        "(generation > 1 AND last_operation_kind IN ('RENEW', "
        "'BIND_EXECUTION_OWNER', 'SUPERSEDE_EXECUTION_OWNER')))) OR "
        "(state = 'REVOKED' AND revision = generation + 1 AND "
        'revoked_at IS NOT NULL AND revoked_at >= renewed_at AND '
        "revocation_reason IN ('STALE_HANDOFF', 'CANDIDATE_ABANDONED', "
        "'COHORT_COLD_RECOVERY', 'COHORT_REMOVAL') AND "
        "last_operation_kind = 'REVOKE' AND "
        "((revocation_reason = 'COHORT_REMOVAL' AND "
        'revocation_owner_id IS NULL) OR '
        "(revocation_reason <> 'COHORT_REMOVAL' AND "
        'revocation_owner_id IS NOT NULL))))',
        name='serve039_worker_lease_execution_owner_ck'),)


def shadow_parent_execution_route_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return the final default-bearing Serve039 shadow-parent columns."""
    return (
        sqlalchemy.Column('execution_route',
                          sqlalchemy.Text,
                          nullable=False,
                          server_default='LEGACY_CONTROLLER'),
        sqlalchemy.Column('private_fallback_reason',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('private_fallback_evidence',
                          postgresql.JSONB(none_as_null=True),
                          nullable=True),
        sqlalchemy.Column('private_fallback_evidence_sha256',
                          sqlalchemy.Text,
                          nullable=True),
    )


def shadow_parent_execution_route_check_constraints(
) -> tuple[sqlalchemy.CheckConstraint, ...]:
    """Return the row-local route/fallback ownership constraint."""
    fallback_pair = _optional_json_hash_shape(
        'private_fallback_evidence', 'private_fallback_evidence_sha256')
    return (sqlalchemy.CheckConstraint(
        "((execution_route = 'PENDING_SELECTION' AND phase = 'PENDING' AND "
        'private_fallback_reason IS NULL AND '
        'private_fallback_evidence IS NULL AND '
        'private_fallback_evidence_sha256 IS NULL) OR '
        "(execution_route = 'PRIVATE_API_REQUEST' AND "
        "phase IN ('RUNNING', 'COMPLETE') AND "
        'private_fallback_reason IS NULL AND '
        'private_fallback_evidence IS NULL AND '
        'private_fallback_evidence_sha256 IS NULL) OR '
        "(execution_route = 'LEGACY_CONTROLLER' AND "
        '((private_fallback_reason IS NULL AND '
        'private_fallback_evidence IS NULL AND '
        'private_fallback_evidence_sha256 IS NULL) OR '
        "(private_fallback_reason = 'linked_admission_not_representable' AND "
        f'{fallback_pair} AND private_fallback_evidence IS NOT NULL)))) IS TRUE',
        name='serve039_shadow_parent_execution_route_ck'),)


def shadow_child_execution_check_constraints(
) -> tuple[sqlalchemy.CheckConstraint, ...]:
    """Return Serve039 private/legacy child execution constraints."""
    return (
        sqlalchemy.CheckConstraint(
            "((planned_execution_kind IN ('api_request', "
            "'legacy_direct_down')) OR "
            "(planned_execution_kind = 'private_api_request' AND "
            "request_role IN ('PRIMARY_LAUNCH', 'PRIMARY_DOWN') AND "
            "phase IN ('REQUEST_BOUND', 'COMPLETE') AND "
            'legacy_request_id IS NOT NULL AND '
            'request_bound_at IS NOT NULL)) IS TRUE',
            name='serve039_shadow_child_execution_kind_ck'),
        sqlalchemy.CheckConstraint(
            "(phase = 'PRE_SUBMIT' AND legacy_request_id IS NULL AND "
            'request_bound_at IS NULL AND completed_at IS NULL) OR '
            "(phase = 'REQUEST_BOUND' AND planned_execution_kind IN "
            "('api_request', 'private_api_request') AND "
            'legacy_request_id IS NOT NULL AND request_bound_at IS NOT NULL '
            'AND completed_at IS NULL) OR '
            "(phase = 'COMPLETE' AND completed_at IS NOT NULL AND "
            "((planned_execution_kind IN ('api_request', "
            "'private_api_request') AND legacy_request_id IS NOT NULL AND "
            'request_bound_at IS NOT NULL) OR '
            "(planned_execution_kind = 'legacy_direct_down' AND "
            'legacy_request_id IS NULL AND request_bound_at IS NULL))) OR '
            "(phase = 'ABANDONED_PRE_SUBMIT' AND completed_at IS NOT NULL "
            'AND legacy_request_id IS NULL AND request_bound_at IS NULL AND '
            'provider_operation_id IS NULL AND actual_outcome IS NULL AND '
            'post_observation IS NULL) OR '
            "(phase = 'REQUEST_ASSOCIATION_UNKNOWN' AND "
            "planned_execution_kind = 'api_request' AND "
            'completed_at IS NOT NULL AND legacy_request_id IS NULL AND '
            'request_bound_at IS NULL)',
            name='serve039_shadow_child_phase_shape_ck'),
    )


execution_authority_lineage_table = sqlalchemy.Table(
    'serve_resource_action_execution_authority_lineage',
    SERVE039_METADATA,
    sqlalchemy.Column('action_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('attempt', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('request_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_input_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_execution_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('authority_worker_instance_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('worker_instance_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('claim_token_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('controller_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('policy_epoch',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('policy_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('authority_binding_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('policy_admission_state', sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('policy_admission_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('cohort_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('cohort_revision', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('registration_set_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('worker_lease_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('reference_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('api_instance_started_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('api_instance_heartbeat_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('dispatch_membership',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('dispatch_membership_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('execution_authority',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('execution_authority_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('authorized_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.PrimaryKeyConstraint(
        'action_id',
        'attempt',
        'request_execution_generation',
        name='pk_serve_ra_execution_authority_lineage'),
    sqlalchemy.UniqueConstraint(
        'request_id',
        'request_execution_generation',
        name='uq_serve_ra_execution_authority_lineage_request'),
    sqlalchemy.ForeignKeyConstraint(
        ['action_id'],
        [resource_action_state_schema.WORKER_COHORT_REFS.c.decision_id],
        ondelete='RESTRICT',
        name='fk_serve_ra_execution_authority_lineage_reference'),
    sqlalchemy.ForeignKeyConstraint(
        ['cohort_id', 'authority_worker_instance_id'], [
            worker_registration_leases_table.c.cohort_id,
            worker_registration_leases_table.c.worker_instance_id,
        ],
        ondelete='RESTRICT',
        name='fk_serve_ra_execution_authority_lineage_lease'),
    sqlalchemy.ForeignKeyConstraint(
        [
            'service_hash', 'policy_epoch', 'policy_sha256',
            'authority_binding_sha256'
        ], [
            authority_policy_epochs_table.c.service_hash,
            authority_policy_epochs_table.c.policy_epoch,
            authority_policy_epochs_table.c.policy_sha256,
            authority_policy_epochs_table.c.authority_binding_sha256,
        ],
        ondelete='RESTRICT',
        name='fk_serve_ra_execution_authority_lineage_policy'),
    sqlalchemy.CheckConstraint(
        'attempt > 0 AND request_execution_generation = 1 AND '
        'controller_generation IS NULL AND policy_admission_revision > 0 AND '
        'cohort_revision > 0 AND '
        'registration_set_revision = cohort_revision AND '
        'worker_lease_revision > 0 AND reference_revision > 0 AND '
        'authority_worker_instance_id <> worker_instance_id AND '
        "policy_admission_state IN ('OPEN', 'DRAINING') AND "
        f"request_id ~ '{_UUID_PATTERN}' AND service_hash ~ '{_UUID_PATTERN}' AND "
        f'{_sha256_shape("request_input_sha256")} AND '
        f'{_sha256_shape("claim_token_sha256")} AND '
        f'{_sha256_shape("policy_sha256")} AND '
        f'{_sha256_shape("authority_binding_sha256")} AND '
        f'{_required_json_hash_shape("dispatch_membership", "dispatch_membership_sha256")} AND '
        f'{_required_json_hash_shape("execution_authority", "execution_authority_sha256")} AND '
        'api_instance_heartbeat_at >= api_instance_started_at AND '
        'authorized_at >= api_instance_started_at',
        name='ck_serve_ra_execution_authority_lineage_shape'),
)
sqlalchemy.Index(
    'ix_serve_ra_execution_lineage_authority_worker',
    execution_authority_lineage_table.c.cohort_id,
    execution_authority_lineage_table.c.authority_worker_instance_id)
sqlalchemy.Index('ix_serve_ra_execution_lineage_process',
                 execution_authority_lineage_table.c.worker_instance_id,
                 execution_authority_lineage_table.c.cohort_id)
sqlalchemy.Index('ix_serve_ra_execution_lineage_policy',
                 execution_authority_lineage_table.c.service_hash,
                 execution_authority_lineage_table.c.policy_epoch,
                 execution_authority_lineage_table.c.policy_sha256,
                 execution_authority_lineage_table.c.authority_binding_sha256)

attempt_terminal_authority_table = sqlalchemy.Table(
    'serve_resource_action_attempt_terminal_authority',
    SERVE039_METADATA,
    sqlalchemy.Column('action_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('attempt', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('request_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_input_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_terminal_state', sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('request_execution_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('authority_worker_instance_id',
                      postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('worker_instance_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('handler_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('authority_disposition', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('lineage_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('terminal_cause', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_finished_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.PrimaryKeyConstraint(
        'action_id', 'attempt', name='pk_serve_ra_attempt_terminal_authority'),
    sqlalchemy.UniqueConstraint(
        'request_id', name='uq_serve_ra_attempt_terminal_authority_request'),
    sqlalchemy.ForeignKeyConstraint(
        ['action_id', 'attempt', 'lineage_generation'], [
            execution_authority_lineage_table.c.action_id,
            execution_authority_lineage_table.c.attempt,
            execution_authority_lineage_table.c.request_execution_generation
        ],
        ondelete='RESTRICT',
        name='fk_serve_ra_attempt_terminal_authority_lineage'),
    sqlalchemy.CheckConstraint(
        'attempt > 0 AND request_execution_generation IN (0, 1) AND '
        f"request_id ~ '{_UUID_PATTERN}' AND "
        f'{_sha256_shape("request_input_sha256")} AND '
        "handler_name IN ('serve_resource_action_launch', "
        "'serve_resource_action_down') AND "
        "((authority_disposition = 'NO_SUCCESSFUL_CLAIM_START' AND "
        'lineage_generation IS NULL AND '
        '((request_execution_generation = 0 AND '
        'authority_worker_instance_id IS NULL AND worker_instance_id IS NULL) '
        'OR (request_execution_generation = 1 AND '
        'authority_worker_instance_id IS NOT NULL AND '
        'worker_instance_id IS NOT NULL AND '
        'authority_worker_instance_id <> worker_instance_id)) AND '
        "((terminal_cause = 'CLAIM_START_NOT_REPRESENTABLE' AND "
        "request_terminal_state = 'FAILED' AND "
        'request_execution_generation = 1) OR '
        "(terminal_cause = 'TERMINAL_BEFORE_CLAIM_START' AND "
        "request_terminal_state IN ('FAILED', 'CANCELLED')))) OR "
        "(authority_disposition = 'LINEAGE' AND "
        'request_execution_generation = 1 AND '
        'authority_worker_instance_id IS NOT NULL AND '
        'worker_instance_id IS NOT NULL AND '
        'authority_worker_instance_id <> worker_instance_id AND '
        'lineage_generation = request_execution_generation AND '
        "((terminal_cause = 'HANDLER_RETURN' AND "
        "request_terminal_state = 'SUCCEEDED') OR "
        "(terminal_cause = 'REQUEST_FAILED' AND "
        "request_terminal_state = 'FAILED') OR "
        "(terminal_cause = 'REQUEST_CANCELLED' AND "
        "request_terminal_state = 'CANCELLED') OR "
        "(terminal_cause = 'CLAIM_REAUTHORIZATION_FAILED' AND "
        "request_terminal_state = 'FAILED')))) IS TRUE",
        name='ck_serve_ra_attempt_terminal_authority_shape'),
)
sqlalchemy.Index(
    'ix_serve_ra_attempt_terminal_authority_worker',
    attempt_terminal_authority_table.c.authority_worker_instance_id,
    postgresql_where=sqlalchemy.text(
        'authority_worker_instance_id IS NOT NULL'))
sqlalchemy.Index(
    'ix_serve_ra_attempt_terminal_process',
    attempt_terminal_authority_table.c.worker_instance_id,
    postgresql_where=sqlalchemy.text('worker_instance_id IS NOT NULL'))

shadow_request_terminal_history_table = sqlalchemy.Table(
    'serve_resource_action_shadow_request_terminal_history',
    SERVE039_METADATA,
    sqlalchemy.Column('decision_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('request_sequence', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('request_role', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('immutable_payload_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('request_input_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('handler_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_terminal_state', sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('request_execution_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('authority_worker_instance_id',
                      postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('worker_instance_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('authority_disposition', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('execution_authority_lineage_sha256', sqlalchemy.Text),
    sqlalchemy.Column('terminal_cause', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('terminal_winner',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('terminal_winner_sha256', sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('request_return_sha256', sqlalchemy.Text),
    sqlalchemy.Column('request_finished_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.PrimaryKeyConstraint(
        'decision_id',
        'request_sequence',
        name='pk_serve_ra_shadow_request_terminal_history'),
    sqlalchemy.UniqueConstraint(
        'request_id',
        name='uq_serve_ra_shadow_request_terminal_history_request'),
    sqlalchemy.CheckConstraint(
        'request_sequence > 0 AND '
        'request_execution_generation IN (0, 1) AND '
        "((request_role = 'PRIMARY_LAUNCH' AND "
        "handler_name = 'serve_shadow_candidate_launch') OR "
        "(request_role = 'PRIMARY_DOWN' AND "
        "handler_name = 'serve_shadow_candidate_down')) AND "
        f"request_id ~ '{_UUID_PATTERN}' AND "
        f'{_sha256_shape("immutable_payload_sha256")} AND '
        f'{_sha256_shape("request_input_sha256")} AND '
        f'{_required_json_hash_shape("terminal_winner", "terminal_winner_sha256")} AND '
        f'({_sha256_shape("request_return_sha256")} OR request_return_sha256 IS NULL) AND '
        "((authority_disposition = 'NO_SUCCESSFUL_CLAIM_START' AND "
        'execution_authority_lineage_sha256 IS NULL AND '
        'request_return_sha256 IS NULL AND '
        "terminal_cause = 'TERMINAL_BEFORE_CLAIM_START' AND "
        "request_terminal_state IN ('FAILED', 'CANCELLED') AND "
        '((request_execution_generation = 0 AND '
        'authority_worker_instance_id IS NULL AND worker_instance_id IS NULL) '
        'OR (request_execution_generation = 1 AND '
        'authority_worker_instance_id IS NOT NULL AND '
        'worker_instance_id IS NOT NULL AND '
        'authority_worker_instance_id <> worker_instance_id))) OR '
        "(authority_disposition = 'SHADOW_EXECUTION' AND "
        'request_execution_generation = 1 AND '
        'authority_worker_instance_id IS NOT NULL AND '
        'worker_instance_id IS NOT NULL AND '
        'authority_worker_instance_id <> worker_instance_id AND '
        f'{_sha256_shape("execution_authority_lineage_sha256")} AND '
        "((terminal_cause = 'HANDLER_RETURN' AND "
        "request_terminal_state = 'SUCCEEDED' AND "
        'request_return_sha256 IS NOT NULL) OR '
        "(terminal_cause = 'REQUEST_FAILED' AND "
        "request_terminal_state = 'FAILED' AND "
        'request_return_sha256 IS NULL) OR '
        "(terminal_cause = 'REQUEST_CANCELLED' AND "
        "request_terminal_state = 'CANCELLED' AND "
        'request_return_sha256 IS NULL)))) IS TRUE',
        name='ck_serve_ra_shadow_request_terminal_history_shape'),
)
sqlalchemy.Index(
    'ix_serve_ra_shadow_terminal_authority_worker',
    shadow_request_terminal_history_table.c.authority_worker_instance_id,
    postgresql_where=sqlalchemy.text(
        'authority_worker_instance_id IS NOT NULL'))
sqlalchemy.Index(
    'ix_serve_ra_shadow_terminal_process',
    shadow_request_terminal_history_table.c.worker_instance_id,
    postgresql_where=sqlalchemy.text('worker_instance_id IS NOT NULL'))

shadow_admission_fallback_history_table = sqlalchemy.Table(
    'serve_resource_action_shadow_admission_fallback_history',
    SERVE039_METADATA,
    sqlalchemy.Column('decision_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('operation_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('deterministic_request_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('fallback_commitment',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('fallback_commitment_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('committed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.PrimaryKeyConstraint(
        'decision_id', name='pk_serve_ra_shadow_admission_fallback_history'),
    sqlalchemy.UniqueConstraint('operation_id',
                                name='uq_serve_ra_shadow_fallback_operation'),
    sqlalchemy.CheckConstraint(_required_json_hash_shape(
        'fallback_commitment', 'fallback_commitment_sha256'),
                               name='ck_serve_ra_shadow_fallback_payload'),
)

shadow_admission_fallback_progress_history_table = sqlalchemy.Table(
    'serve_resource_action_shadow_admission_fallback_progress_log',
    SERVE039_METADATA,
    sqlalchemy.Column('decision_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('fallback_operation_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('progress_operation_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('fallback_commitment_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('progress_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('first_request_sequence', sqlalchemy.Integer),
    sqlalchemy.Column('progress_commitment',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('progress_commitment_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('progressed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.PrimaryKeyConstraint(
        'decision_id',
        name='pk_serve_ra_shadow_admission_fallback_progress_history'),
    sqlalchemy.UniqueConstraint(
        'progress_operation_id',
        name='uq_serve_ra_shadow_fallback_progress_operation'),
    sqlalchemy.CheckConstraint(
        f'{_sha256_shape("fallback_commitment_sha256")} AND '
        f'{_required_json_hash_shape("progress_commitment", "progress_commitment_sha256")} AND '
        "((progress_kind = 'LEGACY_PRE_SUBMIT' AND "
        'first_request_sequence = 1) OR '
        "(progress_kind = 'TERMINAL_NO_CALL_RELEASE' AND "
        'first_request_sequence IS NULL))',
        name='ck_serve_ra_shadow_fallback_progress_shape'),
)

shadow_settlement_history_table = sqlalchemy.Table(
    'serve_resource_action_shadow_settlement_history',
    SERVE039_METADATA,
    sqlalchemy.Column('decision_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('request_sequence', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('request_role', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('operation_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('terminal_history_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('successor_kind', sqlalchemy.Text),
    sqlalchemy.Column('successor_decision_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('successor_request_sequence', sqlalchemy.Integer),
    sqlalchemy.Column('settlement_commitment',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('settlement_commitment_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('settled_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.PrimaryKeyConstraint(
        'decision_id',
        'request_sequence',
        name='pk_serve_ra_shadow_settlement_history'),
    sqlalchemy.UniqueConstraint('operation_id',
                                name='uq_serve_ra_shadow_settlement_operation'),
    sqlalchemy.CheckConstraint(
        'request_sequence > 0 AND '
        "request_role IN ('PRIMARY_LAUNCH', 'PRIMARY_DOWN') AND "
        f'{_sha256_shape("terminal_history_sha256")} AND '
        f'{_required_json_hash_shape("settlement_commitment", "settlement_commitment_sha256")} AND '
        "(successor_kind IS NULL OR successor_kind IN ('retry_same_plan', "
        "'observe_same_plan', 'partial_down')) AND "
        '((successor_kind IS NULL AND successor_decision_id IS NULL AND '
        'successor_request_sequence IS NULL) OR '
        '(successor_kind IS NOT NULL AND successor_decision_id IS NOT NULL '
        'AND successor_request_sequence > 0))',
        name='ck_serve_ra_shadow_settlement_shape'),
)
sqlalchemy.Index(
    'uq_serve_ra_shadow_settlement_partial_source',
    shadow_settlement_history_table.c.decision_id,
    unique=True,
    postgresql_where=sqlalchemy.text("successor_kind = 'partial_down'"))
sqlalchemy.Index(
    'uq_serve_ra_shadow_settlement_partial_target',
    shadow_settlement_history_table.c.successor_decision_id,
    shadow_settlement_history_table.c.successor_request_sequence,
    unique=True,
    postgresql_where=sqlalchemy.text("successor_kind = 'partial_down'"))
sqlalchemy.Index(
    'ix_serve_ra_shadow_settlement_partial_target',
    shadow_settlement_history_table.c.successor_decision_id,
    shadow_settlement_history_table.c.successor_request_sequence,
    postgresql_where=sqlalchemy.text("successor_kind = 'partial_down'"))

shadow_execution_history_table = sqlalchemy.Table(
    'serve_resource_action_shadow_execution_history',
    SERVE039_METADATA,
    sqlalchemy.Column('decision_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('request_sequence', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('request_role', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('handler_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('immutable_payload_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('request_input_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('preflight_request',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('preflight_request_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('preflight_response',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('preflight_response_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('phase', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_execution_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('authority_worker_instance_id',
                      postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('worker_instance_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('claim_token_sha256', sqlalchemy.Text),
    sqlalchemy.Column('dispatch_membership',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('dispatch_membership_sha256', sqlalchemy.Text),
    sqlalchemy.Column('execution_authority',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('execution_authority_sha256', sqlalchemy.Text),
    sqlalchemy.Column('execution_authority_lineage_sha256', sqlalchemy.Text),
    sqlalchemy.Column('authorized_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('provider_io_boundary', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('provider_progress_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('provider_progress', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('provider_progress_sha256', sqlalchemy.Text),
    sqlalchemy.Column('provider_operation_id', sqlalchemy.Text),
    sqlalchemy.Column('provider_effect_trace',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('provider_effect_trace_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('request_return', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('request_return_sha256', sqlalchemy.Text),
    sqlalchemy.Column('terminal_history_sha256', sqlalchemy.Text),
    sqlalchemy.Column('settlement_basis', sqlalchemy.Text),
    sqlalchemy.Column('reduction_disposition', sqlalchemy.Text),
    sqlalchemy.Column('partial_down_decision_id',
                      postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('partial_down_request_sequence', sqlalchemy.Integer),
    sqlalchemy.Column('partial_down_basis_sha256', sqlalchemy.Text),
    sqlalchemy.Column('revision', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('settled_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.PrimaryKeyConstraint(
        'decision_id',
        'request_sequence',
        name='pk_serve_ra_shadow_execution_history'),
    sqlalchemy.UniqueConstraint(
        'request_id', name='uq_serve_ra_shadow_execution_history_request'),
    sqlalchemy.ForeignKeyConstraint(
        ['decision_id', 'request_sequence'], [
            resource_action_state_schema.SHADOW_ATTEMPTS.c.would_be_action_id,
            resource_action_state_schema.SHADOW_ATTEMPTS.c.request_sequence
        ],
        ondelete='CASCADE',
        name='fk_serve_ra_shadow_execution_history_attempt'),
    sqlalchemy.CheckConstraint(
        'request_sequence > 0 AND '
        "request_role IN ('PRIMARY_LAUNCH', 'PRIMARY_DOWN') AND "
        f"request_id ~ '{_UUID_PATTERN}' AND "
        "handler_name IN ('serve_shadow_candidate_launch', "
        "'serve_shadow_candidate_down') AND "
        f'{_sha256_shape("immutable_payload_sha256")} AND '
        f'{_sha256_shape("request_input_sha256")} AND '
        f'{_required_json_hash_shape("preflight_request", "preflight_request_sha256")} AND '
        f'{_required_json_hash_shape("preflight_response", "preflight_response_sha256")} AND '
        f'{_required_json_hash_shape("provider_effect_trace", "provider_effect_trace_sha256")} AND '
        "phase IN ('BOUND', 'AUTHORIZED', 'SETTLED') AND "
        '(request_execution_generation IS NULL OR '
        'request_execution_generation = 1) AND '
        "provider_io_boundary IN ('NOT_STARTED', 'INTENT_COMMITTED', "
        "'SUBMITTED_OR_AMBIGUOUS') AND "
        'provider_progress_revision >= 0 AND revision > 0 AND '
        'updated_at >= created_at AND '
        '(settled_at IS NULL OR settled_at >= created_at)',
        name='ck_serve_ra_shadow_execution_history_shape'),
    sqlalchemy.CheckConstraint(
        _optional_json_hash_shape('provider_progress',
                                  'provider_progress_sha256') + ' AND ' +
        _optional_json_hash_shape('dispatch_membership',
                                  'dispatch_membership_sha256') + ' AND ' +
        _optional_json_hash_shape('execution_authority',
                                  'execution_authority_sha256') + ' AND ' +
        _optional_json_hash_shape('request_return', 'request_return_sha256'),
        name='ck_serve_ra_shadow_execution_history_pairs'),
    sqlalchemy.CheckConstraint(
        '((provider_progress_revision = 0 AND provider_progress IS NULL AND '
        'provider_progress_sha256 IS NULL) OR '
        '(provider_progress_revision > 0 AND provider_progress IS NOT NULL '
        'AND provider_progress_sha256 IS NOT NULL)) AND '
        "(provider_io_boundary = 'NOT_STARTED' OR "
        '(provider_progress_revision > 0 AND provider_progress IS NOT NULL))',
        name='ck_serve_ra_shadow_execution_history_progress'),
    sqlalchemy.CheckConstraint(
        "((phase = 'BOUND' AND request_execution_generation IS NULL AND "
        'authority_worker_instance_id IS NULL AND worker_instance_id IS NULL '
        'AND claim_token_sha256 IS NULL AND dispatch_membership IS NULL AND '
        'dispatch_membership_sha256 IS NULL AND execution_authority IS NULL '
        'AND execution_authority_sha256 IS NULL AND '
        'execution_authority_lineage_sha256 IS NULL AND authorized_at IS NULL '
        'AND request_return IS NULL AND request_return_sha256 IS NULL AND '
        'terminal_history_sha256 IS NULL AND settlement_basis IS NULL AND '
        'reduction_disposition IS NULL AND settled_at IS NULL) OR '
        "(phase = 'AUTHORIZED' AND request_execution_generation = 1 AND "
        'authority_worker_instance_id IS NOT NULL AND worker_instance_id IS '
        'NOT NULL AND authority_worker_instance_id <> worker_instance_id AND '
        f'{_sha256_shape("claim_token_sha256")} AND '
        'dispatch_membership IS NOT NULL AND '
        'dispatch_membership_sha256 IS NOT NULL AND '
        'execution_authority IS NOT NULL AND '
        'execution_authority_sha256 IS NOT NULL AND '
        f'{_sha256_shape("execution_authority_lineage_sha256")} AND '
        'authorized_at IS NOT NULL AND authorized_at >= created_at AND '
        'request_return IS NULL AND request_return_sha256 IS NULL AND '
        'terminal_history_sha256 IS NULL AND settlement_basis IS NULL AND '
        'reduction_disposition IS NULL AND settled_at IS NULL) OR '
        "(phase = 'SETTLED' AND "
        '((request_execution_generation IS NULL AND '
        'authority_worker_instance_id IS NULL AND worker_instance_id IS NULL '
        'AND claim_token_sha256 IS NULL AND dispatch_membership IS NULL AND '
        'dispatch_membership_sha256 IS NULL AND execution_authority IS NULL '
        'AND execution_authority_sha256 IS NULL AND '
        'execution_authority_lineage_sha256 IS NULL AND authorized_at IS NULL) '
        'OR (request_execution_generation = 1 AND '
        'authority_worker_instance_id IS NOT NULL AND worker_instance_id IS '
        'NOT NULL AND authority_worker_instance_id <> worker_instance_id AND '
        f'{_sha256_shape("claim_token_sha256")} AND '
        'dispatch_membership IS NOT NULL AND '
        'dispatch_membership_sha256 IS NOT NULL AND '
        'execution_authority IS NOT NULL AND '
        'execution_authority_sha256 IS NOT NULL AND '
        f'{_sha256_shape("execution_authority_lineage_sha256")} AND '
        'authorized_at IS NOT NULL AND authorized_at >= created_at)) AND '
        f'{_sha256_shape("terminal_history_sha256")} AND '
        "settlement_basis IN ('HANDLER_RETURN', 'REQUEST_FALLBACK') AND "
        "reduction_disposition IN ('S', 'R', 'U', 'B', 'Q', 'P0', 'O', "
        "'X') AND settled_at IS NOT NULL AND "
        "((settlement_basis = 'HANDLER_RETURN' AND request_return IS NOT NULL "
        'AND request_return_sha256 IS NOT NULL) OR '
        "(settlement_basis = 'REQUEST_FALLBACK' AND request_return IS NULL "
        'AND request_return_sha256 IS NULL)))) IS TRUE',
        name='ck_serve_ra_shadow_execution_history_phase'),
    sqlalchemy.CheckConstraint(
        "((reduction_disposition = 'Q' AND request_role = 'PRIMARY_LAUNCH' "
        "AND settlement_basis = 'HANDLER_RETURN' AND "
        'partial_down_decision_id IS NOT NULL AND '
        'partial_down_request_sequence > 0 AND '
        f'{_sha256_shape("partial_down_basis_sha256")}) OR '
        "((reduction_disposition IS NULL OR reduction_disposition <> 'Q') "
        'AND partial_down_decision_id IS NULL AND '
        'partial_down_request_sequence IS NULL AND '
        'partial_down_basis_sha256 IS NULL)) IS TRUE',
        name='ck_serve_ra_shadow_execution_history_partial_down'),
)
sqlalchemy.Index('ix_serve_ra_shadow_execution_authority_worker',
                 shadow_execution_history_table.c.authority_worker_instance_id,
                 postgresql_where=sqlalchemy.text(
                     'authority_worker_instance_id IS NOT NULL'))
sqlalchemy.Index(
    'ix_serve_ra_shadow_execution_process',
    shadow_execution_history_table.c.worker_instance_id,
    postgresql_where=sqlalchemy.text('worker_instance_id IS NOT NULL'))
sqlalchemy.Index(
    'uq_serve_ra_shadow_execution_partial_down',
    shadow_execution_history_table.c.partial_down_decision_id,
    shadow_execution_history_table.c.partial_down_request_sequence,
    unique=True,
    postgresql_where=sqlalchemy.text('partial_down_decision_id IS NOT NULL'))

worker_process_supersessions_table = sqlalchemy.Table(
    'serve_resource_action_worker_process_supersessions',
    SERVE039_METADATA,
    sqlalchemy.Column('cohort_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('supersession_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('authority_worker_instance_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('operation_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('source_lease_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('source_lease_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('committed_lease_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('committed_lease_revision',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('prior_api_instance_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('current_api_instance_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('prior_execution_owner',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('prior_execution_owner_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('current_execution_owner',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('current_execution_owner_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('container_supersession_proof',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('container_supersession_proof_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('request_claims',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('request_claims_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('completed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.PrimaryKeyConstraint(
        'cohort_id',
        'supersession_id',
        name='pk_serve_ra_worker_process_supersessions'),
    sqlalchemy.UniqueConstraint(
        'cohort_id',
        'operation_id',
        name='uq_serve_ra_worker_process_supersessions_operation'),
    sqlalchemy.UniqueConstraint(
        'prior_api_instance_id',
        name='uq_serve_ra_worker_process_supersessions_prior'),
    sqlalchemy.UniqueConstraint(
        'current_api_instance_id',
        name='uq_serve_ra_worker_process_supersessions_current'),
    sqlalchemy.ForeignKeyConstraint(
        ['cohort_id'],
        [resource_action_state_schema.WORKER_COHORTS.c.cohort_id],
        ondelete='RESTRICT',
        name='fk_serve_ra_worker_process_supersessions_cohort'),
    sqlalchemy.CheckConstraint(
        'supersession_id = operation_id AND source_lease_generation > 0 AND '
        'source_lease_revision > 0 AND '
        'source_lease_generation = source_lease_revision AND '
        'committed_lease_generation = source_lease_generation + 1 AND '
        'committed_lease_revision = source_lease_revision + 1 AND '
        'prior_api_instance_id <> current_api_instance_id AND '
        'prior_api_instance_id <> authority_worker_instance_id AND '
        'current_api_instance_id <> authority_worker_instance_id AND '
        f'{_required_json_hash_shape("prior_execution_owner", "prior_execution_owner_sha256")} AND '
        f'{_required_json_hash_shape("current_execution_owner", "current_execution_owner_sha256")} AND '
        f'{_required_json_hash_shape("container_supersession_proof", "container_supersession_proof_sha256")} AND '
        f'{_required_json_hash_shape("request_claims", "request_claims_sha256", root="array")} AND '
        "(CASE WHEN jsonb_typeof(request_claims) = 'array' THEN "
        'jsonb_array_length(request_claims) <= 16 ELSE FALSE END) IS TRUE AND '
        "(CASE WHEN jsonb_typeof(prior_execution_owner) = 'object' AND "
        "jsonb_typeof(current_execution_owner) = 'object' AND "
        "jsonb_typeof(container_supersession_proof) = 'object' THEN "
        'prior_api_instance_id::text = '
        "prior_execution_owner ->> 'api_instance_id' AND "
        'current_api_instance_id::text = '
        "current_execution_owner ->> 'api_instance_id' AND "
        'authority_worker_instance_id::text = '
        "prior_execution_owner ->> 'authority_worker_instance_id' AND "
        'authority_worker_instance_id::text = '
        "current_execution_owner ->> 'authority_worker_instance_id' AND "
        'authority_worker_instance_id::text = '
        "prior_execution_owner ->> 'pod_uid' AND "
        'authority_worker_instance_id::text = '
        "current_execution_owner ->> 'pod_uid' AND "
        'authority_worker_instance_id::text = '
        "container_supersession_proof ->> 'authority_worker_instance_id' AND "
        'prior_api_instance_id::text = '
        "container_supersession_proof ->> 'prior_api_instance_id' AND "
        'current_api_instance_id::text = '
        "container_supersession_proof ->> 'current_api_instance_id' "
        'ELSE FALSE END) IS TRUE',
        name='serve039_worker_process_supersession_ck'),
)
sqlalchemy.Index(
    'ix_serve_ra_worker_process_supersessions_authority',
    worker_process_supersessions_table.c.cohort_id,
    worker_process_supersessions_table.c.authority_worker_instance_id,
    worker_process_supersessions_table.c.completed_at)

api_instance_gc_cursors_table = sqlalchemy.Table(
    'serve_resource_action_api_instance_gc_cursors',
    SERVE039_METADATA,
    sqlalchemy.Column('cursor_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('sweep_epoch', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('sweep_upper_bound_instance_id',
                      postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('after_instance_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('revision', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('last_operation_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.PrimaryKeyConstraint('cursor_name',
                                    name='pk_serve_ra_api_instance_gc_cursors'),
    sqlalchemy.CheckConstraint("cursor_name = 'authority-worker-v2'",
                               name='ck_serve_ra_api_gc_cursor_name'),
    sqlalchemy.CheckConstraint('sweep_epoch >= 0 AND revision > 0',
                               name='ck_serve_ra_api_gc_cursor_counters'),
    sqlalchemy.CheckConstraint(
        '((sweep_upper_bound_instance_id IS NULL AND '
        'after_instance_id IS NULL) OR '
        '(sweep_upper_bound_instance_id IS NOT NULL AND '
        '(after_instance_id IS NULL OR '
        'after_instance_id <= sweep_upper_bound_instance_id)))',
        name='ck_serve_ra_api_gc_cursor_bounds'),
)

EXECUTION_AUTHORITY_LINEAGE = execution_authority_lineage_table
ATTEMPT_TERMINAL_AUTHORITY = attempt_terminal_authority_table
SHADOW_REQUEST_TERMINAL_HISTORY = shadow_request_terminal_history_table
SHADOW_ADMISSION_FALLBACK_HISTORY = shadow_admission_fallback_history_table
SHADOW_ADMISSION_FALLBACK_PROGRESS_HISTORY = (
    shadow_admission_fallback_progress_history_table)
SHADOW_SETTLEMENT_HISTORY = shadow_settlement_history_table
SHADOW_EXECUTION_HISTORY = shadow_execution_history_table
WORKER_PROCESS_SUPERSESSIONS = worker_process_supersessions_table
API_INSTANCE_GC_CURSORS = api_instance_gc_cursors_table

# Complete reflected contracts for the three existing relations altered by
# Serve039.  Each uses its own metadata plus the smallest FK target stub so
# SQLAlchemy can resolve the historical cross-metadata FK without importing a
# second mutable copy of the target relation into the altered-relation set.
_serve039_lease_metadata = sqlalchemy.MetaData()
sqlalchemy.Table(
    resource_action_state_schema.WORKER_COHORTS.name, _serve039_lease_metadata,
    sqlalchemy.Column('cohort_id', sqlalchemy.Text, primary_key=True))
worker_registration_leases_v2_table = WORKER_REGISTRATION_LEASES.to_metadata(
    _serve039_lease_metadata)
for _constraint in tuple(worker_registration_leases_v2_table.constraints):
    if _constraint.name == 'serve038_worker_lease_closed_shape_ck':
        worker_registration_leases_v2_table.constraints.remove(_constraint)
for _column in worker_registration_lease_execution_owner_columns():
    worker_registration_leases_v2_table.append_column(_column)
for _constraint in worker_registration_lease_execution_owner_check_constraints(
):
    worker_registration_leases_v2_table.append_constraint(_constraint)
sqlalchemy.Index(
    'uq_serve_ra_worker_registration_leases_execution_owner',
    worker_registration_leases_v2_table.c.execution_owner_api_instance_id,
    unique=True,
    postgresql_where=sqlalchemy.text(
        'execution_owner_api_instance_id IS NOT NULL'))

_serve039_shadow_parent_metadata = sqlalchemy.MetaData()
sqlalchemy.Table(
    resource_action_state_schema.SHADOW_COVERAGE.name,
    _serve039_shadow_parent_metadata,
    sqlalchemy.Column('decision_id',
                      postgresql.UUID(as_uuid=True),
                      primary_key=True))
shadow_samples_v2_table = resource_action_state_schema.SHADOW_SAMPLES.to_metadata(
    _serve039_shadow_parent_metadata)
for _column in shadow_parent_execution_route_columns():
    shadow_samples_v2_table.append_column(_column)
for _constraint in shadow_parent_execution_route_check_constraints():
    shadow_samples_v2_table.append_constraint(_constraint)

_serve039_shadow_child_metadata = sqlalchemy.MetaData()
sqlalchemy.Table(
    resource_action_state_schema.SHADOW_SAMPLES.name,
    _serve039_shadow_child_metadata,
    sqlalchemy.Column('would_be_action_id',
                      postgresql.UUID(as_uuid=True),
                      primary_key=True))
shadow_attempts_v2_table = resource_action_state_schema.SHADOW_ATTEMPTS.to_metadata(
    _serve039_shadow_child_metadata)
for _constraint in tuple(shadow_attempts_v2_table.constraints):
    if _constraint.name in ('ck_serve_ra_shadow_attempts_execution',
                            'ck_serve_ra_shadow_attempts_phase_shape'):
        shadow_attempts_v2_table.constraints.remove(_constraint)
for _constraint in shadow_child_execution_check_constraints():
    shadow_attempts_v2_table.append_constraint(_constraint)

WORKER_REGISTRATION_LEASES_V2 = worker_registration_leases_v2_table
SHADOW_SAMPLES_V2 = shadow_samples_v2_table
SHADOW_ATTEMPTS_V2 = shadow_attempts_v2_table
SERVE039_ALTERED_RELATION_TABLES = (
    WORKER_REGISTRATION_LEASES_V2,
    SHADOW_SAMPLES_V2,
    SHADOW_ATTEMPTS_V2,
)
