"""Durable exact-idle authority for retiring paid SkyServe replicas."""

from collections.abc import Mapping
import dataclasses
import enum
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky.serve import capacity_admission_schema
from sky.serve import demand_state_schema
from sky.serve import route_projection_schema
from sky.serve import serve_state_schema


class PaidRetirementError(RuntimeError):
    """Base class for paid-retirement authority failures."""


class PaidRetirementConflict(PaidRetirementError):
    """The requested retirement no longer matches current authority."""


class PaidRetirementState(str, enum.Enum):
    ACTIVE = 'ACTIVE'
    COMMITTED = 'COMMITTED'
    CANCELLED = 'CANCELLED'


@dataclasses.dataclass(frozen=True)
class FreshZeroAuthority:
    """One database-fresh aggregate-zero demand observation."""

    service_hash: str
    demand_source_epoch: int
    demand_feed_generation: int
    capacity_plan_generation: int
    capacity_plan_sha256: str
    route_generation: int

    def __post_init__(self) -> None:
        if not isinstance(self.service_hash, str) or not self.service_hash:
            raise ValueError('service_hash must be nonempty.')
        for name in ('demand_source_epoch', 'demand_feed_generation',
                     'capacity_plan_generation', 'route_generation'):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f'{name} must be a positive integer.')
        if (not isinstance(self.capacity_plan_sha256, str) or
                len(self.capacity_plan_sha256) != 64 or
                any(character not in '0123456789abcdef'
                    for character in self.capacity_plan_sha256)):
            raise ValueError('capacity_plan_sha256 must be lowercase SHA-256.')


metadata = sqlalchemy.MetaData()

serve_paid_replica_retirements_table = sqlalchemy.Table(
    'serve_paid_replica_retirements',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('replica_record_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('service_lifecycle_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('controller_incarnation',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('controller_owner_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('controller_pid', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('controller_ip', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('demand_source_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('demand_feed_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('capacity_plan_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('capacity_plan_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('route_generation', sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('route_url', sqlalchemy.Text),
    sqlalchemy.Column('requires_idle_proof', sqlalchemy.Boolean,
                      nullable=False),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('committed_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('cancelled_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.CheckConstraint(
        'replica_id > 0 AND service_lifecycle_epoch > 0 AND '
        'controller_owner_epoch > 0 AND controller_pid > 0 AND '
        'service_version > 0 AND demand_source_epoch > 0 AND '
        'demand_feed_generation > 0 AND capacity_plan_generation > 0 AND '
        'route_generation > 0',
        name='serve051_paid_retirement_positive_ck'),
    sqlalchemy.CheckConstraint(
        'length(service_hash) > 0 AND length(controller_ip) > 0 AND '
        "capacity_plan_sha256 ~ '^[0-9a-f]{64}$'",
        name='serve051_paid_retirement_text_nonempty_ck'),
    sqlalchemy.CheckConstraint(
        '((requires_idle_proof AND route_url IS NOT NULL AND '
        'length(route_url) > 0) OR '
        '(NOT requires_idle_proof AND route_url IS NULL))',
        name='serve051_paid_retirement_route_shape_ck'),
    sqlalchemy.CheckConstraint("state IN ('ACTIVE', 'COMMITTED', 'CANCELLED')",
                               name='serve051_paid_retirement_state_ck'),
    sqlalchemy.CheckConstraint(
        "((state = 'ACTIVE' AND committed_at IS NULL AND "
        'cancelled_at IS NULL) OR '
        "(state = 'COMMITTED' AND committed_at IS NOT NULL AND "
        'cancelled_at IS NULL) OR '
        "(state = 'CANCELLED' AND committed_at IS NULL AND "
        'cancelled_at IS NOT NULL))',
        name='serve051_paid_retirement_terminal_shape_ck'),
)
sqlalchemy.Index('ix_serve051_paid_retirement_active',
                 serve_paid_replica_retirements_table.c.service_name,
                 serve_paid_replica_retirements_table.c.state)

_RETIREMENTS = serve_paid_replica_retirements_table
_SERVICES = serve_state_schema.services_table
_REPLICAS = serve_state_schema.replicas_table
_DEMAND_GENERATIONS = (demand_state_schema.serve_demand_feed_generations_table)
_CAPACITY_PLAN_HEADS = capacity_admission_schema.serve_capacity_plan_heads_table
_CAPACITY_PLANS = capacity_admission_schema.serve_capacity_plans_table
_ROUTE_HEADS = route_projection_schema.serve_route_heads_table
_ROUTE_LEASES = route_projection_schema.serve_route_replica_leases_table


def _canonical_record_id(value: object) -> uuid.UUID:
    if not isinstance(value, str):
        raise PaidRetirementConflict('Replica record identity is invalid.')
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise PaidRetirementConflict(
            'Replica record identity is invalid.') from error
    if str(parsed) != value:
        raise PaidRetirementConflict('Replica record identity is invalid.')
    return parsed


def _owner_matches(
    owner: Mapping[str, Any],
    authority: FreshZeroAuthority,
    expected_controller_owner: tuple[int | None, str | None],
) -> bool:
    return bool(
        owner.get('hash') == authority.service_hash and
        owner.get('pool') in (0, False) and
        owner.get('demand_source_mode') == 'DURABLE_FEED' and
        owner.get('demand_source_epoch') == authority.demand_source_epoch and
        owner.get('route_source_mode') == 'DURABLE_PROJECTED' and
        owner.get('route_projection_protocol_version') == 2 and
        owner.get('controller_pid') == expected_controller_owner[0] and
        owner.get('controller_ip') == expected_controller_owner[1])


def _lock_authority(
    session: orm.Session,
    service_name: str,
    authority: FreshZeroAuthority,
    expected_controller_owner: tuple[int | None, str | None],
) -> Mapping[str, Any]:
    owner = session.execute(
        sqlalchemy.select(_SERVICES).where(_SERVICES.c.name == service_name).
        with_for_update()).mappings().one_or_none()
    if owner is None or not _owner_matches(owner, authority,
                                           expected_controller_owner):
        raise PaidRetirementConflict(
            'Fresh-zero retirement lost service ownership or protocol 2.')
    demand_generation = session.execute(
        sqlalchemy.select(_DEMAND_GENERATIONS.c.generation).where(
            _DEMAND_GENERATIONS.c.service_name == service_name,
            _DEMAND_GENERATIONS.c.service_hash ==
            authority.service_hash).with_for_update()).scalar_one_or_none()
    route_head = session.execute(
        sqlalchemy.select(_ROUTE_HEADS).where(
            _ROUTE_HEADS.c.service_name ==
            service_name).with_for_update()).mappings().one_or_none()
    route_generation = (None
                        if route_head is None else route_head['generation'])
    if (demand_generation != authority.demand_feed_generation or
            route_generation != authority.route_generation):
        raise PaidRetirementConflict(
            'Fresh-zero demand or route generation advanced.')
    now = session.execute(sqlalchemy.select(
        sqlalchemy.func.clock_timestamp())).scalar_one()
    plan_head = session.execute(
        sqlalchemy.select(_CAPACITY_PLAN_HEADS).where(
            _CAPACITY_PLAN_HEADS.c.service_name ==
            service_name).with_for_update()).mappings().one_or_none()
    plan = session.execute(
        sqlalchemy.select(_CAPACITY_PLANS).where(
            _CAPACITY_PLANS.c.service_name == service_name,
            _CAPACITY_PLANS.c.generation == authority.capacity_plan_generation).
        with_for_update()).mappings().one_or_none()
    payload = None if plan is None else plan['payload']
    normalized = (payload.get('normalized_demand') if isinstance(
        payload, Mapping) else None)
    targets = (payload.get('capacity_target_by_accelerator') if isinstance(
        payload, Mapping) else None)
    paid_residual = (payload.get('paid_residual_by_accelerator') if isinstance(
        payload, Mapping) else None)
    if (plan_head is None or plan is None or route_head is None or
            route_head['valid_until'] <= now or
            plan_head['generation'] != authority.capacity_plan_generation or
            plan_head['valid_until'] <= now or
            plan_head['demand_feed_generation']
            != authority.demand_feed_generation or
            plan['content_sha256'] != authority.capacity_plan_sha256 or
            plan['service_hash'] != authority.service_hash or
            plan['demand_source_epoch'] != authority.demand_source_epoch or
            plan['route_generation'] != authority.route_generation or
            not isinstance(normalized, Mapping) or
            normalized.get('fresh_aggregate_zero') is not True or
            not isinstance(targets, Mapping) or not targets or any(
                type(count) is not int or count != 0
                for count in targets.values()) or paid_residual != {}):
        raise PaidRetirementConflict(
            'Fresh-zero capacity plan is no longer authoritative.')
    return owner


def admit_in_session(
    session: orm.Session,
    service_name: str,
    replica_id: int,
    replica_record_id: str,
    service_version: int,
    requires_idle_proof: bool,
    authority: FreshZeroAuthority,
    expected_controller_owner: tuple[int | None, str | None],
) -> Mapping[str, Any]:
    """Admit or refresh one exact retirement in the replica transaction."""
    if type(replica_id) is not int or replica_id < 1:
        raise PaidRetirementConflict('Replica ID is invalid.')
    if type(service_version) is not int or service_version < 1:
        raise PaidRetirementConflict('Replica version is invalid.')
    if type(requires_idle_proof) is not bool:
        raise PaidRetirementConflict('Idle-proof requirement is invalid.')
    record_id = _canonical_record_id(replica_record_id)
    owner = _lock_authority(session, service_name, authority,
                            expected_controller_owner)
    replica = session.execute(
        sqlalchemy.select(
            _REPLICAS.c.version,
            _REPLICAS.c.replica_state['replica_record_id'].as_string().label(
                'replica_record_id'),
            _REPLICAS.c.replica_state['is_zero_cost'].as_boolean().label(
                'is_zero_cost'),
        ).where(_REPLICAS.c.service_name == service_name, _REPLICAS.c.replica_id
                == replica_id).with_for_update()).mappings().one_or_none()
    if (replica is None or replica['version'] != service_version or
            replica['replica_record_id'] != replica_record_id or
            replica['is_zero_cost'] is not False):
        raise PaidRetirementConflict(
            'Fresh-zero retirement target is not the current paid replica.')
    existing = session.execute(
        sqlalchemy.select(_RETIREMENTS).where(
            _RETIREMENTS.c.service_name == service_name,
            _RETIREMENTS.c.replica_id ==
            replica_id).with_for_update()).mappings().one_or_none()
    if (existing is not None and existing['replica_record_id'] == record_id and
            existing['state'] == PaidRetirementState.COMMITTED.value):
        return existing
    route_url = None
    if requires_idle_proof:
        lease = session.execute(
            sqlalchemy.select(_ROUTE_LEASES.c.route_url).where(
                _ROUTE_LEASES.c.service_name == service_name,
                _ROUTE_LEASES.c.service_hash == authority.service_hash,
                _ROUTE_LEASES.c.replica_id == replica_id,
                _ROUTE_LEASES.c.replica_record_id == record_id,
                _ROUTE_LEASES.c.revoked_at.is_(None),
            ).with_for_update()).mappings().one_or_none()
        if lease is None:
            if (existing is None or
                    existing['replica_record_id'] != record_id or
                    existing['state'] != PaidRetirementState.ACTIVE.value or
                    not existing['route_url']):
                raise PaidRetirementConflict(
                    'A previously-routable paid replica has no exact route '
                    'material for idle proof.')
            route_url = existing['route_url']
        else:
            route_url = lease['route_url']
    now = session.execute(sqlalchemy.select(
        sqlalchemy.func.clock_timestamp())).scalar_one()
    state = (PaidRetirementState.ACTIVE.value
             if requires_idle_proof else PaidRetirementState.COMMITTED.value)
    values = {
        'service_name': service_name,
        'replica_id': replica_id,
        'service_hash': authority.service_hash,
        'replica_record_id': record_id,
        'service_lifecycle_epoch': int(owner['lifecycle_epoch']),
        'controller_incarnation': owner['controller_incarnation'],
        'controller_owner_epoch': int(owner['controller_owner_epoch']),
        'controller_pid': int(owner['controller_pid']),
        'controller_ip': owner['controller_ip'],
        'service_version': service_version,
        'demand_source_epoch': authority.demand_source_epoch,
        'demand_feed_generation': authority.demand_feed_generation,
        'capacity_plan_generation': authority.capacity_plan_generation,
        'capacity_plan_sha256': authority.capacity_plan_sha256,
        'route_generation': authority.route_generation,
        'route_url': route_url,
        'requires_idle_proof': requires_idle_proof,
        'state': state,
        'created_at': (now if existing is None or existing['replica_record_id']
                       != record_id else existing['created_at']),
        'updated_at': now,
        'committed_at': now if state == PaidRetirementState.COMMITTED.value else
                        None,
        'cancelled_at': None,
    }
    insert = postgresql.insert(_RETIREMENTS).values(**values)
    session.execute(
        insert.on_conflict_do_update(index_elements=[
            _RETIREMENTS.c.service_name, _RETIREMENTS.c.replica_id
        ],
                                     set_={
                                         key: value
                                         for key, value in values.items()
                                         if key not in ('service_name',
                                                        'replica_id')
                                     }))
    return values


def commit_in_session(
    session: orm.Session,
    service_name: str,
    replica_id: int,
    replica_record_id: str,
    authority: FreshZeroAuthority,
    expected_controller_owner: tuple[int | None, str | None],
) -> bool:
    """Irreversibly commit teardown after exact zero-occupancy proof."""
    record_id = _canonical_record_id(replica_record_id)
    _lock_authority(session, service_name, authority, expected_controller_owner)
    now = session.execute(sqlalchemy.select(
        sqlalchemy.func.clock_timestamp())).scalar_one()
    result = session.execute(
        sqlalchemy.update(_RETIREMENTS).where(
            _RETIREMENTS.c.service_name == service_name,
            _RETIREMENTS.c.replica_id == replica_id,
            _RETIREMENTS.c.replica_record_id == record_id,
            _RETIREMENTS.c.demand_source_epoch == authority.demand_source_epoch,
            _RETIREMENTS.c.demand_feed_generation ==
            authority.demand_feed_generation,
            _RETIREMENTS.c.capacity_plan_generation ==
            authority.capacity_plan_generation,
            _RETIREMENTS.c.capacity_plan_sha256 ==
            authority.capacity_plan_sha256,
            _RETIREMENTS.c.route_generation == authority.route_generation,
            _RETIREMENTS.c.state == PaidRetirementState.ACTIVE.value,
        ).values(state=PaidRetirementState.COMMITTED.value,
                 committed_at=now,
                 cancelled_at=None,
                 updated_at=now))
    return result.rowcount == 1


def cancel_in_session(
    session: orm.Session,
    service_name: str,
    replica_id: int,
    replica_record_id: str,
    positive_demand_generation: int,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
) -> bool:
    """Cancel only an uncommitted retirement fenced by newer demand."""
    record_id = _canonical_record_id(replica_record_id)
    if type(positive_demand_generation
           ) is not int or positive_demand_generation < 1:
        raise PaidRetirementConflict('Positive demand generation is invalid.')
    owner = session.execute(
        sqlalchemy.select(_SERVICES).where(_SERVICES.c.name == service_name).
        with_for_update()).mappings().one_or_none()
    if (owner is None or owner['hash'] != expected_service_hash or
        (owner['controller_pid'], owner['controller_ip'])
            != expected_controller_owner):
        raise PaidRetirementConflict(
            'Paid-retirement cancellation lost service ownership.')
    current_generation = session.execute(
        sqlalchemy.select(_DEMAND_GENERATIONS.c.generation).where(
            _DEMAND_GENERATIONS.c.service_name == service_name,
            _DEMAND_GENERATIONS.c.service_hash ==
            expected_service_hash).with_for_update()).scalar_one_or_none()
    if current_generation != positive_demand_generation:
        raise PaidRetirementConflict(
            'Positive demand generation advanced before cancellation.')
    now = session.execute(sqlalchemy.select(
        sqlalchemy.func.clock_timestamp())).scalar_one()
    result = session.execute(
        sqlalchemy.update(_RETIREMENTS).where(
            _RETIREMENTS.c.service_name == service_name,
            _RETIREMENTS.c.replica_id == replica_id,
            _RETIREMENTS.c.replica_record_id == record_id,
            _RETIREMENTS.c.service_hash == expected_service_hash,
            _RETIREMENTS.c.state == PaidRetirementState.ACTIVE.value,
            _RETIREMENTS.c.demand_feed_generation < positive_demand_generation,
        ).values(state=PaidRetirementState.CANCELLED.value,
                 committed_at=None,
                 cancelled_at=now,
                 updated_at=now))
    return result.rowcount == 1


def list_for_service(service_name: str) -> dict[int, dict[str, Any]]:
    """Return current retirement records keyed by replica ID."""
    engine = serve_state_schema.get_database_engine()
    if engine.dialect.name != 'postgresql':
        return {}
    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(_RETIREMENTS).where(
                _RETIREMENTS.c.service_name == service_name)).mappings().all()
    return {int(row['replica_id']): dict(row) for row in rows}


def get_for_replica(service_name: str,
                    replica_id: int) -> dict[str, Any] | None:
    """Return one exact retirement record without provider access."""
    engine = serve_state_schema.get_database_engine()
    if engine.dialect.name != 'postgresql':
        return None
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(_RETIREMENTS).where(
                _RETIREMENTS.c.service_name == service_name,
                _RETIREMENTS.c.replica_id ==
                replica_id)).mappings().one_or_none()
    return None if row is None else dict(row)


def delete_in_session(session: orm.Session, service_name: str,
                      replica_ids: list[int]) -> None:
    """Delete terminal operational intents with their exact replica rows."""
    if not replica_ids:
        return
    session.execute(
        sqlalchemy.delete(_RETIREMENTS).where(
            _RETIREMENTS.c.service_name == service_name,
            _RETIREMENTS.c.replica_id.in_(replica_ids)))
