"""Add the durable multi-pool reserved-fill protocol.

Revision ID: 035
Revises: 034
Create Date: 2026-08-04

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.serve import serve_state_schema

# revision identifiers, used by Alembic.
revision: str = '035'
down_revision: str | Sequence[str] | None = '034'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _add_round_fence_columns(bind: sa.engine.Connection) -> None:
    """Add non-null v1 defaults without rewriting existing round meaning."""
    inspector = sa.inspect(bind)
    if not inspector.has_table('reserved_fill_rounds'):
        return
    columns = {
        str(column['name'])
        for column in inspector.get_columns('reserved_fill_rounds')
    }
    if 'protocol_version' not in columns:
        op.add_column(
            'reserved_fill_rounds',
            sa.Column('protocol_version',
                      sa.Integer(),
                      nullable=False,
                      server_default='1'))
    if 'claim_generations' not in columns:
        op.add_column(
            'reserved_fill_rounds',
            sa.Column('claim_generations',
                      sa.Text(),
                      nullable=False,
                      server_default='{}'))
    if 'feed_by_accelerator' not in columns:
        # NULL means the writer had no exact-card measurement (including every
        # pre-035 round).  It is deliberately not coerced to an empty mapping:
        # an empty mapping is an authoritative exact measurement that permits
        # no shaped launch.
        op.add_column(
            'reserved_fill_rounds',
            sa.Column('feed_by_accelerator', sa.Text(), nullable=True))


def _add_protocol_audit_columns(bind: sa.engine.Connection) -> None:
    """Complete v2 fencing/audit fields on a pre-created v035 table."""
    inspector = sa.inspect(bind)
    if not inspector.has_table('reserved_fill_protocol_state'):
        return
    columns = {
        str(column['name'])
        for column in inspector.get_columns('reserved_fill_protocol_state')
    }
    if 'claim_generation' not in columns:
        op.add_column(
            'reserved_fill_protocol_state',
            sa.Column('claim_generation',
                      sa.BigInteger(),
                      nullable=False,
                      server_default='0'))
    audit_columns = {
        'deployment_uid': sa.Text(),
        'pod_inventory_count': sa.Integer(),
        'pod_inventory_sha256': sa.Text(),
    }
    for name, column_type in audit_columns.items():
        if name not in columns:
            op.add_column('reserved_fill_protocol_state',
                          sa.Column(name, column_type, nullable=True))


def _seed_protocol_state(bind: sa.engine.Connection) -> None:
    bind.execute(
        sa.text('INSERT INTO reserved_fill_protocol_state '
                '(id, protocol_version, claim_generation, changed_at) '
                'VALUES (1, 1, 0, 0) '
                'ON CONFLICT (id) DO NOTHING'))


def _copy_legacy_claims_as_shadows(bind: sa.engine.Connection) -> None:
    """Copy rollback-compatible claims without making them authoritative."""
    inspector = sa.inspect(bind)
    if not inspector.has_table('reserved_fill_claims'):
        return
    bind.execute(
        sa.text("""
            INSERT INTO reserved_fill_service_claim_sets
                (service_name, claim_set_state, generation, edge_count,
                 semantic_hash, global_headroom, utilization_ceiling,
                 utilization_state, heartbeat_ts)
            SELECT legacy.service_name, 'migration_shadow', 0, 1, NULL,
                   legacy.effective_cap, legacy.effective_cap, NULL,
                   COALESCE(legacy.heartbeat_ts, 0)
            FROM reserved_fill_claims AS legacy
            WHERE 1 = 1
            ON CONFLICT (service_name) DO NOTHING
        """))
    bind.execute(
        sa.text("""
            INSERT INTO reserved_fill_pool_claims
                (service_name, pool_key, legacy_pool_key, pool_position,
                 access_context, physical_cluster_uid, accelerator_names,
                 service_generation, weight, floor_replicas,
                 gpus_per_replica, holdings_fill, effective_cap, launchable,
                 demonstrated_need, boot_hold, activity_ts, heartbeat_ts)
            SELECT legacy.service_name, legacy.pool_key, legacy.pool_key, 0,
                   NULL, NULL, NULL, 0, legacy.weight,
                   legacy.floor_replicas, legacy.gpus_per_replica,
                   legacy.holdings_fill, legacy.effective_cap,
                   legacy.launchable, legacy.demonstrated_need,
                   legacy.boot_hold, legacy.activity_ts,
                   COALESCE(legacy.heartbeat_ts, 0)
            FROM reserved_fill_claims AS legacy
            WHERE 1 = 1
            ON CONFLICT (service_name, pool_key) DO NOTHING
        """))


def upgrade() -> None:
    """Install inert shadows and the explicit protocol-v2 activation gate."""
    bind = op.get_bind()
    # Fresh databases may already have these tables because revision 001 uses
    # the current metadata graph.  Existing databases reach this revision
    # without them, so every create is deliberately idempotent.
    for table in (serve_state_schema.reserved_fill_protocol_state_table,
                  serve_state_schema.reserved_fill_service_claim_sets_table,
                  serve_state_schema.reserved_fill_pool_claims_table):
        table.create(bind, checkfirst=True)
    _add_protocol_audit_columns(bind)
    _add_round_fence_columns(bind)
    _seed_protocol_state(bind)
    _copy_legacy_claims_as_shadows(bind)


def downgrade() -> None:
    """Retain protocol fences and shadows for application rollback."""
    raise RuntimeError(
        'SkyServe schema 035 is additive and cannot be downgraded. Demote '
        'reserved-fill to protocol v1 before rolling back the application.')
