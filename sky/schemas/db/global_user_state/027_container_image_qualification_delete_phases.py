"""Persist qualification delete phases and repository tombstones.

Revision ID: 027
Revises: 026
Create Date: 2026-07-24

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '027'
down_revision: str | Sequence[str] | None = '026'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_MUTATION_TABLE = 'container_image_qualification_mutation'
_QUARANTINE_TABLE = 'container_image_qualification_repository_quarantines'
_LEGACY_DELETE_QUARANTINE_REASON = 'LEGACY_DELETE_OUTCOME_UNKNOWN'
_LEGACY_RESTORATION_QUARANTINE_REASON = (
    'LEGACY_RESTORATION_EVIDENCE_INCOMPLETE')


def _upgrade_mutation_table() -> None:
    op.drop_constraint('ck_container_image_qualification_mutation_state',
                       _MUTATION_TABLE,
                       type_='check')
    op.drop_constraint('ck_container_image_qualification_mutation_lease',
                       _MUTATION_TABLE,
                       type_='check')
    op.add_column(_MUTATION_TABLE,
                  sqlalchemy.Column('delete_phase', sqlalchemy.Text))
    op.add_column(_MUTATION_TABLE,
                  sqlalchemy.Column('quarantine_reason', sqlalchemy.Text))


def _adopt_legacy_mutation(bind: sqlalchemy.engine.Connection) -> None:
    """Quarantines a phase-less singleton created by migration 026."""
    quarantined_at = int(
        bind.execute(
            sqlalchemy.select(
                sqlalchemy.cast(
                    sqlalchemy.func.floor(
                        sqlalchemy.extract('epoch',
                                           sqlalchemy.func.clock_timestamp())),
                    sqlalchemy.BigInteger))).scalar_one())
    reasons = {
        'delete_reason': _LEGACY_DELETE_QUARANTINE_REASON,
        'restoration_reason': _LEGACY_RESTORATION_QUARANTINE_REASON,
        'quarantined_at': quarantined_at,
    }
    bind.execute(
        sqlalchemy.text(f"""
            INSERT INTO {_QUARANTINE_TABLE} (
                repository_arn,
                owner_profile_revision_id,
                owner_target,
                owner_target_fingerprint,
                runtime_digest,
                lifecycle_proof_id,
                quarantine_reason,
                quarantined_at
            )
            SELECT repository_arn,
                   owner_profile_revision_id,
                   owner_target,
                   owner_target_fingerprint,
                   runtime_digest,
                   lifecycle_proof_id,
                   CASE state
                       WHEN 'DELETING' THEN :delete_reason
                       WHEN 'RESTORING' THEN :restoration_reason
                   END,
                   :quarantined_at
            FROM {_MUTATION_TABLE}
            WHERE state IN ('DELETING', 'RESTORING')
        """), reasons)
    bind.execute(
        sqlalchemy.text(f"""
            UPDATE {_MUTATION_TABLE}
            SET state = 'QUARANTINED',
                mutation_lease_token = NULL,
                mutation_lease_expires_at = NULL,
                delete_phase = NULL,
                quarantine_reason = CASE state
                    WHEN 'DELETING' THEN :delete_reason
                    WHEN 'RESTORING' THEN :restoration_reason
                END,
                updated_at = :quarantined_at
            WHERE state IN ('DELETING', 'RESTORING')
        """), reasons)


def _create_mutation_constraints() -> None:
    op.create_check_constraint(
        'ck_container_image_qualification_mutation_state', _MUTATION_TABLE,
        "state IN ('DELETING', 'RESTORING', 'QUARANTINED')")
    op.create_check_constraint(
        'ck_container_image_qualification_mutation_delete_phase',
        _MUTATION_TABLE,
        "delete_phase IS NULL OR delete_phase IN ('PRE_INTENT', 'IN_FLIGHT', "
        "'READBACK')")
    op.create_check_constraint(
        'ck_container_image_qualification_mutation_lease', _MUTATION_TABLE,
        "(state = 'DELETING' AND delete_phase IS NOT NULL "
        'AND mutation_lease_token IS NOT NULL '
        'AND mutation_lease_expires_at IS NOT NULL '
        'AND mutation_lease_expires_at > updated_at '
        'AND quarantine_reason IS NULL) OR '
        "(state = 'RESTORING' AND delete_phase IS NULL "
        'AND mutation_lease_token IS NULL '
        'AND mutation_lease_expires_at IS NULL '
        'AND quarantine_reason IS NULL) OR '
        "(state = 'QUARANTINED' AND delete_phase IS NULL "
        'AND mutation_lease_token IS NULL '
        'AND mutation_lease_expires_at IS NULL '
        "AND quarantine_reason IS NOT NULL AND quarantine_reason <> '')")


def _create_repository_quarantines() -> None:
    op.create_table(
        _QUARANTINE_TABLE,
        sqlalchemy.Column('repository_arn', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column(
            'owner_profile_revision_id',
            sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_profile_revisions.id'),
            nullable=False),
        sqlalchemy.Column('owner_target', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('owner_target_fingerprint',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('runtime_digest', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('lifecycle_proof_id', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('quarantine_reason', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('quarantined_at',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.CheckConstraint(
            "repository_arn <> '' AND owner_target <> '' "
            "AND owner_target_fingerprint <> '' AND runtime_digest <> '' "
            "AND lifecycle_proof_id <> '' AND quarantine_reason <> '' "
            'AND quarantined_at >= 0',
            name=(
                'ck_container_image_qualification_repository_quarantine_identity'
            )),
    )
    op.create_index(
        'ix_container_image_qualification_repository_quarantines_history',
        _QUARANTINE_TABLE, ['quarantined_at', 'repository_arn'])


def upgrade():
    """Add durable qualification request phases and physical tombstones."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    _upgrade_mutation_table()
    _create_repository_quarantines()
    _adopt_legacy_mutation(bind)
    _create_mutation_constraints()


def downgrade():
    """Remove phase state only when no mutation or tombstone would be lost."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    mutation_count = int(
        bind.execute(sqlalchemy.text(
            f'SELECT COUNT(*) FROM {_MUTATION_TABLE}')).scalar_one())
    quarantine_count = int(
        bind.execute(
            sqlalchemy.text(
                f'SELECT COUNT(*) FROM {_QUARANTINE_TABLE}')).scalar_one())
    if mutation_count or quarantine_count:
        raise RuntimeError(
            'Migration 027 downgrade requires empty qualification mutation '
            'and repository quarantine tables.')
    op.drop_table(_QUARANTINE_TABLE)
    op.drop_constraint('ck_container_image_qualification_mutation_delete_phase',
                       _MUTATION_TABLE,
                       type_='check')
    op.drop_constraint('ck_container_image_qualification_mutation_lease',
                       _MUTATION_TABLE,
                       type_='check')
    op.drop_constraint('ck_container_image_qualification_mutation_state',
                       _MUTATION_TABLE,
                       type_='check')
    op.drop_column(_MUTATION_TABLE, 'quarantine_reason')
    op.drop_column(_MUTATION_TABLE, 'delete_phase')
    op.create_check_constraint(
        'ck_container_image_qualification_mutation_state', _MUTATION_TABLE,
        "state IN ('DELETING', 'RESTORING')")
    op.create_check_constraint(
        'ck_container_image_qualification_mutation_lease', _MUTATION_TABLE,
        "(state = 'DELETING' AND mutation_lease_token IS NOT NULL "
        'AND mutation_lease_expires_at IS NOT NULL '
        "AND mutation_lease_expires_at > updated_at) OR "
        "(state = 'RESTORING' AND mutation_lease_token IS NULL "
        'AND mutation_lease_expires_at IS NULL)')
