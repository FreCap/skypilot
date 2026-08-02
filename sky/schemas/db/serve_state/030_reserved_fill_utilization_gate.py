"""Persist reserved-fill utilization signal and release state.

Revision ID: 030
Revises: 029
Create Date: 2026-07-25

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '030'
down_revision: str | Sequence[str] | None = '029'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the utilization gate's claim signal and durable release state.

    All columns are nullable with no server default, so a pre-030 row and a
    row written by a binary that predates the gate both read as "no signal",
    which the broker treats as ungated (today's exact behavior).

    activity_ts is the anti-skew witness. upsert_reserved_fill_claim builds
    its values dict from the columns its own binary knows about, and the
    ON CONFLICT DO UPDATE set_ comprehension iterates that dict, so an old
    binary heartbeating a migrated row advances heartbeat_ts while leaving
    demonstrated_need frozen. A frozen 0 would read as "permanently idle"
    and walk a busy service down to zero. Pairing every write with
    activity_ts lets the broker reject the claim when
    heartbeat_ts - activity_ts exceeds the staleness bound.

    utilization_state on the round row carries the per-claimant release
    target across controller restarts, api-server pod recreation and broker
    writer rotation. It is the structural sibling of feed_state, written
    under the same lease CAS, and an old binary's publish omits it from its
    values dict so the state survives a mixed-version round untouched.
    """
    # Skip tables this database does not have. add_column_to_table_alembic
    # only tolerates "already exists"; a MISSING table re-raises, and on
    # PostgreSQL the failed statement then poisons the surrounding block. A
    # database stamped past revision 004 without ever running it (an upgrade
    # from a hand-built legacy schema, which the migration-chain tests
    # construct) has no reserved-fill tables at all. Nothing is lost by
    # skipping: whenever those tables are finally created they come from
    # Base.metadata, which already declares these columns.
    inspector = sa.inspect(op.get_bind())
    claims_present = inspector.has_table('reserved_fill_claims')
    rounds_present = inspector.has_table('reserved_fill_rounds')
    with op.get_context().autocommit_block():
        if claims_present:
            db_utils.add_column_to_table_alembic('reserved_fill_claims',
                                                 'demonstrated_need',
                                                 sa.Integer())
            db_utils.add_column_to_table_alembic('reserved_fill_claims',
                                                 'boot_hold', sa.Integer())
            db_utils.add_column_to_table_alembic('reserved_fill_claims',
                                                 'activity_ts', sa.Float())
        if rounds_present:
            db_utils.add_column_to_table_alembic('reserved_fill_rounds',
                                                 'utilization_state', sa.Text())


def downgrade() -> None:
    # Additive nullable columns are safe for an old binary to ignore, and
    # dropping them would restart every in-progress release from scratch on
    # a later forward deployment.
    pass
