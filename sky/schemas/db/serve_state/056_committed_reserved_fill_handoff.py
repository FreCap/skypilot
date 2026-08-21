"""Bind each new protocol-v2 fill replica to one committed intent.

Revision ID: 056
Revises: 055
Create Date: 2026-08-21

Serve056 is additive and PostgreSQL-only.  Existing link-NULL replicas remain
historical cleanup inventory.  A new protocol-v2 reserved-fill replica is
launch-authoritative only when its initial INSERT names an exact immutable
COMMITTED Serve052 intent.  The ReplicaInfo JSON remains a checked projection;
the scalar link is the database authority.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '056'
down_revision: str | Sequence[str] | None = '055'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ASSOCIATIONS = 'serve_ordinary_launch_associations'
_INTENTS = 'serve_zero_cost_actuation_intents'
_REPLICAS = 'replicas'
_SERVICES = 'services'
_INTENT_LINK = 'reserved_fill_intent_idempotency_key'

_INTENT_SERVICE_KEY_UNIQUE = 'serve056_intent_service_key_uq'
_REPLICA_INTENT_UNIQUE = 'serve056_replica_intent_uq'
_REPLICA_INTENT_CHECK = 'serve056_replica_intent_key_ck'
_REPLICA_INTENT_FOREIGN_KEY = 'serve056_replica_intent_fk'
_REPLICA_GUARD_FUNCTION = 'skyserve047_guard_replica_non_pool_authorization'
_REPLICA_GUARD_TRIGGER = 'skyserve047_replica_non_pool_authorization_guard'
_INTENT_GUARD_FUNCTION = 'skyserve056_guard_committed_fill_intent'
_INTENT_GUARD_TRIGGER = 'skyserve056_committed_fill_intent_guard'
_INTENT_CONSISTENCY_FUNCTION = 'skyserve056_check_committed_fill_intent'
_INTENT_CONSISTENCY_TRIGGER = 'skyserve056_committed_fill_intent_consistency'
_REPLICA_CONSISTENCY_FUNCTION = 'skyserve056_check_fill_replica_handoff'
_REPLICA_CONSISTENCY_TRIGGER = 'skyserve056_fill_replica_handoff_consistency'

_PROTOCOL_V2_JSON_FIELDS = (
    'reserved_fill_service_generation',
    'reserved_fill_physical_cluster_uid',
    'reserved_fill_kubernetes_context',
    'reserved_fill_allocation_generation',
    'reserved_fill_allocation_input_sha256',
    'reserved_fill_allocation_claim_generation',
    'reserved_fill_reconciliation_gate_generation',
    'reserved_fill_reclaim_fleet_bundle_sha256',
    'reserved_fill_reclaim_policy_revision',
    'reserved_fill_reclaim_provider_inventory_sha256',
    'reserved_fill_worker_projection_sha256',
    'reserved_fill_observation_generation',
    'reserved_fill_observation_sequence',
    _INTENT_LINK,
)


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Committed reserved-fill handoff is PostgreSQL-only.')


def _install_intent_guard() -> None:
    """Make the COMMITTED side of the handoff update-immutable."""
    op.execute(f'''
        CREATE OR REPLACE FUNCTION {_INTENT_GUARD_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            IF OLD.state = 'COMMITTED' AND NEW IS DISTINCT FROM OLD THEN
                RAISE EXCEPTION
                    'committed reserved-fill intent is immutable';
            END IF;
            RETURN NEW;
        END;
        $function$
    ''')
    op.execute(f'DROP TRIGGER IF EXISTS {_INTENT_GUARD_TRIGGER} '
               f'ON {_INTENTS}')
    op.execute(f'''
        CREATE TRIGGER {_INTENT_GUARD_TRIGGER}
        BEFORE UPDATE ON {_INTENTS}
        FOR EACH ROW EXECUTE FUNCTION {_INTENT_GUARD_FUNCTION}()
    ''')


def _install_replica_guard() -> None:
    """Enforce the two-phase intent -> replica -> association graph."""
    protocol_v2_claimed = ' OR\n                   '.join(
        f"NEW.replica_state ->> '{field}' IS NOT NULL"
        for field in _PROTOCOL_V2_JSON_FIELDS)
    op.execute(f'''
        CREATE OR REPLACE FUNCTION {_REPLICA_GUARD_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        DECLARE
            committed_intent RECORD;
            current_service RECORD;
            exact_association RECORD;
            protocol_v2_claimed boolean;
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                IF NEW.non_pool_launch_authorization IS DISTINCT FROM
                        OLD.non_pool_launch_authorization THEN
                    RAISE EXCEPTION
                        'replica non-pool launch authorization is initial-insert-only';
                END IF;
                IF NEW.{_INTENT_LINK} IS DISTINCT FROM
                        OLD.{_INTENT_LINK} THEN
                    RAISE EXCEPTION
                        'replica reserved-fill intent link is initial-insert-only';
                END IF;
            END IF;

            protocol_v2_claimed := COALESCE(
                {protocol_v2_claimed}, FALSE);
            IF NEW.{_INTENT_LINK} IS NULL THEN
                IF TG_OP = 'INSERT' AND protocol_v2_claimed THEN
                    RAISE EXCEPTION
                        'new protocol-v2 replica requires a committed intent link';
                END IF;
                -- Retained unlinked rows are cleanup-only.  Their ordinary
                -- status projection may still advance, but the application
                -- provider guard cannot derive launch authority from them.
                RETURN NEW;
            END IF;

            IF NEW.replica_state_version IS DISTINCT FROM 1 OR
               jsonb_typeof(NEW.replica_state) IS DISTINCT FROM 'object' OR
               NEW.replica_state -> 'reserved_fill' IS DISTINCT FROM
                    'true'::jsonb OR
               NEW.replica_state -> 'is_zero_cost' IS DISTINCT FROM
                    'true'::jsonb OR
               NEW.replica_state -> 'is_spot' IS DISTINCT FROM
                    'false'::jsonb OR
               NEW.is_spot IS DISTINCT FROM FALSE OR
               NEW.paid_capacity_pool_key IS NOT NULL OR
               NEW.replica_state -> 'paid_capacity_pool_key' IS DISTINCT FROM
                    'null'::jsonb OR
               NEW.replica_state ->> '{_INTENT_LINK}' IS DISTINCT FROM
                    NEW.{_INTENT_LINK} OR
               NEW.replica_state ->> 'replica_id' IS DISTINCT FROM
                    NEW.replica_id::text OR
               NEW.replica_state ->> 'version' IS DISTINCT FROM
                    NEW.version::text OR
               NEW.replica_state ->> 'cluster_name' IS DISTINCT FROM
                    NEW.cluster_name THEN
                RAISE EXCEPTION
                    'intent-linked replica is not an exact zero-cost fill';
            END IF;

            SELECT intent.* INTO committed_intent
              FROM {_INTENTS} AS intent
             WHERE intent.service_name = NEW.service_name
               AND intent.intent_idempotency_key = NEW.{_INTENT_LINK};
            IF NOT FOUND OR
               committed_intent.state IS DISTINCT FROM 'COMMITTED' OR
               committed_intent.protocol_version IS DISTINCT FROM 2 OR
               committed_intent.replica_id IS DISTINCT FROM NEW.replica_id OR
               committed_intent.replica_record_id::text IS DISTINCT FROM
                    NEW.replica_state ->> 'replica_record_id' OR
               committed_intent.service_version IS DISTINCT FROM NEW.version OR
               committed_intent.pool_key IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_pool_key' OR
               committed_intent.service_generation::text IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_service_generation' OR
               committed_intent.physical_cluster_uid IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_physical_cluster_uid' OR
               committed_intent.kubernetes_context IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_kubernetes_context' OR
               committed_intent.allocation_generation::text IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_allocation_generation' OR
               committed_intent.allocation_input_sha256 IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_allocation_input_sha256' OR
               committed_intent.allocation_claim_generation::text IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_allocation_claim_generation' OR
               committed_intent.reconciliation_gate_generation::text IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_reconciliation_gate_generation' OR
               committed_intent.reclaim_fleet_bundle_sha256 IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_reclaim_fleet_bundle_sha256' OR
               committed_intent.reclaim_policy_revision IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_reclaim_policy_revision' OR
               committed_intent.reclaim_provider_inventory_sha256 IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_reclaim_provider_inventory_sha256' OR
               committed_intent.worker_projection_sha256 IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_worker_projection_sha256' OR
               committed_intent.observation_generation::text IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_observation_generation' OR
               committed_intent.observation_sequence::text IS DISTINCT FROM
                    NEW.replica_state ->> 'reserved_fill_observation_sequence' OR
               committed_intent.planned_capacity::text IS DISTINCT FROM
                    NEW.replica_state ->> 'planned_capacity' THEN
                RAISE EXCEPTION
                    'replica intent link does not match its committed projection';
            END IF;

            -- Intent serialization, Python handoff validation, and the
            -- provider fence all select allowed_locations[0]. Compare that
            -- location's exact launch coordinates. Do not compare the whole
            -- JSON object: ReplicaInfo losslessly encodes image_id as a pair
            -- list while the intent stores the equivalent JSON object.
            IF jsonb_typeof(committed_intent.allowed_locations)
                    IS DISTINCT FROM 'array' OR
               jsonb_array_length(committed_intent.allowed_locations) < 1 OR
               jsonb_typeof(NEW.replica_state -> 'location')
                    IS DISTINCT FROM 'object' OR
               NEW.replica_state -> 'resources_override' IS DISTINCT FROM
                    NEW.replica_state -> 'location' OR
               lower(NEW.replica_state -> 'location' ->> 'cloud') IS DISTINCT
                    FROM 'kubernetes' OR
               NEW.replica_state -> 'location' ->> 'region' IS DISTINCT FROM
                    committed_intent.kubernetes_context OR
               committed_intent.allowed_locations -> 0 ->> 'region'
                    IS DISTINCT FROM committed_intent.kubernetes_context OR
               lower(committed_intent.allowed_locations -> 0 ->> 'cloud')
                    IS DISTINCT FROM 'kubernetes' OR
               NEW.replica_state -> 'location' -> 'zone' IS DISTINCT FROM
                    committed_intent.allowed_locations -> 0 -> 'zone' OR
               NEW.replica_state -> 'location' -> 'use_spot' IS DISTINCT FROM
                    'false'::jsonb OR
               committed_intent.allowed_locations -> 0 -> 'use_spot'
                    IS DISTINCT FROM 'false'::jsonb OR
               jsonb_typeof(NEW.replica_state -> 'location' ->
                    'accelerators') IS DISTINCT FROM 'object' OR
               (SELECT count(*)
                  FROM jsonb_each(
                    NEW.replica_state -> 'location' -> 'accelerators'))
                    IS DISTINCT FROM 1 OR
               NOT EXISTS (
                    SELECT 1
                      FROM jsonb_each(
                        NEW.replica_state -> 'location' -> 'accelerators')
                           AS shape(card, amount)
                     WHERE lower(shape.card) = lower(
                            committed_intent.accelerator)
                       AND shape.amount =
                            to_jsonb(committed_intent.accelerator_count)) OR
               jsonb_typeof(committed_intent.allowed_locations -> 0 ->
                    'accelerators') IS DISTINCT FROM 'object' OR
               (SELECT count(*)
                  FROM jsonb_each(committed_intent.allowed_locations -> 0 ->
                                  'accelerators')) IS DISTINCT FROM 1 OR
               NOT EXISTS (
                    SELECT 1
                      FROM jsonb_each(committed_intent.allowed_locations -> 0
                                      -> 'accelerators')
                           AS allowed_shape(card, amount)
                     WHERE lower(allowed_shape.card) = lower(
                            committed_intent.accelerator)
                       AND allowed_shape.amount =
                            to_jsonb(committed_intent.accelerator_count)) THEN
                RAISE EXCEPTION
                    'replica location does not exactly match an allowed committed location';
            END IF;

            IF TG_OP = 'INSERT' THEN
                SELECT service.* INTO current_service
                  FROM {_SERVICES} AS service
                 WHERE service.name = NEW.service_name;
                IF NOT FOUND OR
                   current_service.hash IS DISTINCT FROM
                        committed_intent.service_hash OR
                   current_service.lifecycle_epoch IS DISTINCT FROM
                        committed_intent.service_lifecycle_epoch OR
                   current_service.current_version IS DISTINCT FROM
                        committed_intent.service_version OR
                   current_service.resource_scope IS DISTINCT FROM
                        committed_intent.service_hash OR
                   current_service.reserved_fill_actuation_mode
                        IS DISTINCT FROM 'DURABLE_INTENT' OR
                   current_service.reserved_fill_actuation_epoch IS DISTINCT
                        FROM committed_intent.actuation_epoch OR
                   current_service.reserved_fill_actuation_capable IS NOT TRUE OR
                   current_service.reserved_fill_actuation_controller_incarnation
                        IS DISTINCT FROM current_service.controller_incarnation OR
                   current_service.reserved_fill_actuation_protocol_version
                        IS DISTINCT FROM 1 OR
                   current_service.ordinary_launch_binding_mode
                        IS DISTINCT FROM 'bound' OR
                   current_service.ordinary_launch_binding_capable IS NOT TRUE OR
                   current_service.non_pool_launch_binding_capable IS NOT TRUE OR
                   current_service.non_pool_launch_controller_incarnation
                        IS DISTINCT FROM current_service.controller_incarnation OR
                   current_service.non_pool_launch_binding_protocol_version
                        IS DISTINCT FROM 2 THEN
                    RAISE EXCEPTION
                        'new linked replica lost its committed service owner';
                END IF;
            END IF;

            IF NEW.ordinary_launch_association_id IS NOT NULL THEN
                SELECT association.* INTO exact_association
                  FROM {_ASSOCIATIONS} AS association
                 WHERE association.association_id =
                        NEW.ordinary_launch_association_id;
                IF NOT FOUND OR
                   exact_association.binding_protocol_version
                        IS DISTINCT FROM 2 OR
                   exact_association.profile_kind
                        IS DISTINCT FROM 'RESERVED_FILL' OR
                   exact_association.authorization_kind
                        IS DISTINCT FROM 'RESERVED_FILL_ALLOCATION' OR
                   exact_association.authorization_reference IS DISTINCT FROM
                        'reserved-fill:' || NEW.{_INTENT_LINK} OR
                   exact_association.authorization_generation IS DISTINCT FROM
                        committed_intent.allocation_generation OR
                   exact_association.service_name IS DISTINCT FROM
                        NEW.service_name OR
                   exact_association.service_hash IS DISTINCT FROM
                        committed_intent.service_hash OR
                   exact_association.service_lifecycle_epoch IS DISTINCT FROM
                        committed_intent.service_lifecycle_epoch OR
                   exact_association.service_version IS DISTINCT FROM
                        committed_intent.service_version OR
                   exact_association.replica_id IS DISTINCT FROM
                        NEW.replica_id OR
                   exact_association.replica_record_id IS DISTINCT FROM
                        committed_intent.replica_record_id OR
                   exact_association.cluster_name IS DISTINCT FROM
                        NEW.cluster_name OR
                   exact_association.paid_capacity_pool_key IS NOT NULL THEN
                    RAISE EXCEPTION
                        'intent-linked replica association is not exact';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $function$
    ''')
    op.execute(f'DROP TRIGGER IF EXISTS {_REPLICA_GUARD_TRIGGER} '
               f'ON {_REPLICAS}')
    op.execute(f'''
        CREATE TRIGGER {_REPLICA_GUARD_TRIGGER}
        BEFORE INSERT OR UPDATE ON {_REPLICAS}
        FOR EACH ROW EXECUTE FUNCTION {_REPLICA_GUARD_FUNCTION}()
    ''')

    # The replica must be inserted before the association can point back to
    # it.  Defer only this closing-edge check until transaction commit; the
    # immediate trigger above still validates every individual row mutation.
    op.execute(f'''
        CREATE OR REPLACE FUNCTION {_REPLICA_CONSISTENCY_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        DECLARE
            current_replica RECORD;
            committed_intent RECORD;
            exact_association RECORD;
        BEGIN
            SELECT replica.* INTO current_replica
              FROM {_REPLICAS} AS replica
             WHERE replica.service_name = NEW.service_name
               AND replica.replica_id = NEW.replica_id;
            IF NOT FOUND OR current_replica.{_INTENT_LINK} IS NULL THEN
                RETURN NULL;
            END IF;
            SELECT intent.* INTO committed_intent
              FROM {_INTENTS} AS intent
             WHERE intent.service_name = current_replica.service_name
               AND intent.intent_idempotency_key =
                    current_replica.{_INTENT_LINK};
            SELECT association.* INTO exact_association
              FROM {_ASSOCIATIONS} AS association
             WHERE association.association_id =
                    current_replica.ordinary_launch_association_id;
            IF current_replica.ordinary_launch_association_id IS NULL OR
               committed_intent.intent_idempotency_key IS NULL OR
               committed_intent.state IS DISTINCT FROM 'COMMITTED' OR
               exact_association.association_id IS NULL OR
               exact_association.binding_protocol_version
                    IS DISTINCT FROM 2 OR
               exact_association.profile_kind
                    IS DISTINCT FROM 'RESERVED_FILL' OR
               exact_association.authorization_kind
                    IS DISTINCT FROM 'RESERVED_FILL_ALLOCATION' OR
               exact_association.authorization_reference IS DISTINCT FROM
                    'reserved-fill:' || current_replica.{_INTENT_LINK} OR
               exact_association.authorization_generation IS DISTINCT FROM
                    committed_intent.allocation_generation OR
               exact_association.service_name IS DISTINCT FROM
                    current_replica.service_name OR
               exact_association.service_hash IS DISTINCT FROM
                    committed_intent.service_hash OR
               exact_association.service_lifecycle_epoch IS DISTINCT FROM
                    committed_intent.service_lifecycle_epoch OR
               exact_association.service_version IS DISTINCT FROM
                    committed_intent.service_version OR
               exact_association.replica_id IS DISTINCT FROM
                    current_replica.replica_id OR
               exact_association.replica_record_id IS DISTINCT FROM
                    committed_intent.replica_record_id OR
               exact_association.cluster_name IS DISTINCT FROM
                    current_replica.cluster_name OR
               exact_association.paid_capacity_pool_key IS NOT NULL THEN
                RAISE EXCEPTION
                    'committed fill replica lacks its exact association';
            END IF;
            RETURN NULL;
        END;
        $function$
    ''')
    op.execute(f'DROP TRIGGER IF EXISTS {_REPLICA_CONSISTENCY_TRIGGER} '
               f'ON {_REPLICAS}')
    op.execute(f'''
        CREATE CONSTRAINT TRIGGER {_REPLICA_CONSISTENCY_TRIGGER}
        AFTER INSERT ON {_REPLICAS}
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION {_REPLICA_CONSISTENCY_FUNCTION}()
    ''')

    # The replica-side trigger proves that every linked INSERT closes its
    # association edge.  Prove the reverse direction as well: a freshly
    # COMMITTED intent cannot survive the transaction if a later statement
    # removes or fails to bind that replica.  Historical committed intents do
    # not fire this trigger during migration and remain cleanup inventory.
    op.execute(f'''
        CREATE OR REPLACE FUNCTION {_INTENT_CONSISTENCY_FUNCTION}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        DECLARE
            exact_replica RECORD;
            exact_association RECORD;
        BEGIN
            SELECT replica.* INTO exact_replica
              FROM {_REPLICAS} AS replica
             WHERE replica.service_name = NEW.service_name
               AND replica.replica_id = NEW.replica_id
               AND replica.{_INTENT_LINK} = NEW.intent_idempotency_key;
            IF NOT FOUND OR
               exact_replica.replica_state ->> 'replica_record_id' IS DISTINCT
                    FROM NEW.replica_record_id::text OR
               exact_replica.ordinary_launch_association_id IS NULL THEN
                RAISE EXCEPTION
                    'committed fill intent lacks its exact replica handoff';
            END IF;
            SELECT association.* INTO exact_association
              FROM {_ASSOCIATIONS} AS association
             WHERE association.association_id =
                    exact_replica.ordinary_launch_association_id;
            IF NOT FOUND OR
               exact_association.binding_protocol_version
                    IS DISTINCT FROM 2 OR
               exact_association.profile_kind
                    IS DISTINCT FROM 'RESERVED_FILL' OR
               exact_association.authorization_kind
                    IS DISTINCT FROM 'RESERVED_FILL_ALLOCATION' OR
               exact_association.authorization_reference IS DISTINCT FROM
                    'reserved-fill:' || NEW.intent_idempotency_key OR
               exact_association.authorization_generation IS DISTINCT FROM
                    NEW.allocation_generation OR
               exact_association.service_name IS DISTINCT FROM
                    NEW.service_name OR
               exact_association.service_hash IS DISTINCT FROM
                    NEW.service_hash OR
               exact_association.service_lifecycle_epoch IS DISTINCT FROM
                    NEW.service_lifecycle_epoch OR
               exact_association.service_version IS DISTINCT FROM
                    NEW.service_version OR
               exact_association.replica_id IS DISTINCT FROM NEW.replica_id OR
               exact_association.replica_record_id IS DISTINCT FROM
                    NEW.replica_record_id OR
               exact_association.cluster_name IS DISTINCT FROM
                    exact_replica.cluster_name OR
               exact_association.paid_capacity_pool_key IS NOT NULL THEN
                RAISE EXCEPTION
                    'committed fill intent lacks its exact association handoff';
            END IF;
            RETURN NULL;
        END;
        $function$
    ''')
    op.execute(f'DROP TRIGGER IF EXISTS {_INTENT_CONSISTENCY_TRIGGER} '
               f'ON {_INTENTS}')
    op.execute(f'''
        CREATE CONSTRAINT TRIGGER {_INTENT_CONSISTENCY_TRIGGER}
        AFTER UPDATE OF state ON {_INTENTS}
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        WHEN (NEW.state = 'COMMITTED')
        EXECUTE FUNCTION {_INTENT_CONSISTENCY_FUNCTION}()
    ''')


def upgrade() -> None:
    """Install a fresh-only normalized committed-intent handoff."""
    _require_postgresql()
    connection = op.get_bind()
    # DDL takes strong locks in any case.  Acquire them once in the canonical
    # service -> intent -> replica -> association order so a rolling old
    # writer either commits before this migration or observes all guards.
    connection.execute(
        sa.text(
            'LOCK TABLE services, serve_zero_cost_actuation_intents, replicas, '
            'serve_ordinary_launch_associations IN ACCESS EXCLUSIVE MODE'))
    op.create_unique_constraint(_INTENT_SERVICE_KEY_UNIQUE, _INTENTS,
                                ['service_name', 'intent_idempotency_key'])
    op.add_column(_REPLICAS, sa.Column(_INTENT_LINK, sa.Text(), nullable=True))
    op.create_check_constraint(
        _REPLICA_INTENT_CHECK, _REPLICAS,
        f"{_INTENT_LINK} IS NULL OR {_INTENT_LINK} ~ '^[0-9a-f]{{64}}$'")
    op.create_unique_constraint(_REPLICA_INTENT_UNIQUE, _REPLICAS,
                                ['service_name', _INTENT_LINK])
    op.create_foreign_key(_REPLICA_INTENT_FOREIGN_KEY,
                          _REPLICAS,
                          _INTENTS, ['service_name', _INTENT_LINK],
                          ['service_name', 'intent_idempotency_key'],
                          ondelete='RESTRICT')
    _install_intent_guard()
    _install_replica_guard()


def downgrade() -> None:
    """Preserve committed provider authority across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve056 is forward-only; committed reserved-fill authority is '
        'durable.')
