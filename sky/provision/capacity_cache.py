"""DB-backed capacity-exhaustion cache.

Failover runs inside per-request worker subprocesses (see the launch dispatch
topology in ``sky/server/requests/executor.py``), so a process-local memory of
"this shape has no capacity" would be invisible to the other workers. This
module persists exhaustion state in a dedicated, process-shared DB
(``capacity_cache``) so that every worker of the same pod (and, on Postgres
deployments, every pod) fast-skips a recently-exhausted shape at the failover
site instead of re-running a full multi-AZ provision sweep that is doomed to
fail.

Scope is deliberately narrow -- only what is *unambiguously* an exhausted shape:
  - AWS only. The recognized error code is AWS-specific; the caller records only
    for AWS launches (see ``_capacity_cache_applies`` in the backend).
  - Physical *open* instance capacity only (``InsufficientInstanceCapacity``).
    Quota (account-scoped, fast-clearing) is handled by backoff alone, and
    reservation-eligible launches opt out entirely (see
    ``_launch_targets_specific_reservation`` in the backend) because a targeted
    reservation is separately-selectable capacity -- so a cached open-capacity
    block can never wrongly suppress a shape that actually has capacity.
  - Fresh launches only. The caller consults/records only when there is no prior
    cluster, so the cache never short-circuits existing-cluster lifecycle
    handling (config-hash reuse, teardown of partial instances) and ``num_nodes``
    is always the full acquisition size (nothing is already running).

What a shape is keyed on (``ResourceKey``): cloud, account, region, zone,
instance_type, use_spot, and num_nodes.
  - ``account`` is the *account* identifier (e.g. the AWS account id), not a
    principal/role. It disambiguates the shared DB across accounts: AWS AZ *names*
    (``us-east-1a``) map to different physical AZs per account, so an entry from
    one account must not block another. Same account, different IAM roles share
    correctly (same account id).
  - ``num_nodes`` matters because cloud APIs allocate the whole request atomically
    (AWS submits the count as both ``MinCount`` and ``MaxCount``), so a failed
    64-node request says nothing about a 1-node request.

Recoverability guarantee (hard requirement): every entry carries a bounded TTL
and **no entry ever blocks permanently**. The TTL is clamped to
``_MAX_TTL_SECONDS`` in code, so even a mis-configured (or non-finite) config
value cannot produce an unprunable row. Once ``exhausted_until`` passes, the
shape is probed for real again; a successful provision clears the exact zones it
attempted immediately.

Consumption is a best-effort *hint* read fresh at each failover attempt (not a
lock): if N workers start on a cold cache, all N may probe the same doomed shape
before the first failure is recorded -- there is no singleflight. The win is on
the *next* wave: once one worker records the exhaustion, subsequent attempts
fast-skip it until the TTL lapses.
"""
import math
import time
from typing import Iterable, NamedTuple, Optional, Set

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.ext import declarative

from sky import sky_logging
from sky import skypilot_config
from sky.utils import common_utils
from sky.utils.db import db_utils

logger = sky_logging.init_logger(__name__)

# Default TTL (seconds); overridable via
# provision.capacity_exhaustion_ttl.capacity_seconds.
_DEFAULT_CAPACITY_TTL_SECONDS = 120.0
# Hard ceiling on the TTL, enforced in code regardless of config. This is what
# makes the recoverability guarantee unconditional: a block can never outlive
# this window, so a mis-configured or non-finite TTL cannot wedge a shape
# permanently.
_MAX_TTL_SECONDS = 3600.0


class ResourceKey(NamedTuple):
    """A launchable shape keyed into the exhaustion cache.

    ``zone`` and ``account`` are normalized to ``''`` (never ``None``) for
    regions without a zone concept / clouds without an account concept, so they
    are safe as part of a composite primary key on Postgres (which disallows
    NULLs in PK columns).
    """
    cloud: str
    account: str
    region: str
    zone: str
    instance_type: str
    use_spot: bool
    num_nodes: int


Base = declarative.declarative_base()

capacity_exhaustion_table = sqlalchemy.Table(
    'capacity_exhaustion',
    Base.metadata,
    sqlalchemy.Column('cloud', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('account', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('region', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('zone', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('instance_type', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('use_spot', sqlalchemy.Boolean, primary_key=True),
    sqlalchemy.Column('num_nodes', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('exhausted_until', sqlalchemy.Float),
)


def _set_sqlite_pragma(dbapi_conn, _) -> None:
    """Enables WAL + a generous busy timeout on each new SQLite connection.

    WAL lets readers and the single writer proceed concurrently, and the busy
    timeout absorbs the brief write contention from many workers marking/
    clearing shapes at once. Tolerant of a transient lock (the pragma is an
    optimization, not required for correctness).
    """
    cursor = dbapi_conn.cursor()
    try:
        # busy_timeout first, and in its own statement, so a locked WAL switch
        # (tolerated below) does not also skip installing the timeout.
        cursor.execute('PRAGMA busy_timeout=30000')
        try:
            cursor.execute('PRAGMA journal_mode=WAL')
        except Exception as e:  # pylint: disable=broad-except
            if 'database is locked' not in str(e):
                raise
    finally:
        cursor.close()


def _expected_columns() -> Set[str]:
    return {c.name for c in capacity_exhaustion_table.columns}


def _create_table(engine: sqlalchemy.engine.Engine) -> None:
    # Register the SQLite pragma listener BEFORE any connection is opened (table
    # inspection/creation below opens the first pooled connection), so that
    # connection -- which may be reused for the life of the pool -- also gets
    # WAL + busy_timeout. Skipped on WSL, which has known WAL locking issues
    # (mirrors sky/utils/db/kv_cache.py).
    if (engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value and
            not common_utils.is_wsl()):
        sqlalchemy.event.listen(engine, 'connect', _set_sqlite_pragma)
    # Self-heal on a schema drift. This is disposable hint data with no
    # migration story: if a pre-existing table has a stale column set (e.g. an
    # older release's schema), drop and recreate it rather than silently failing
    # every read/write on the missing columns (which would disable the cache).
    #
    # Errors are intentionally NOT swallowed here: DatabaseManager caches the
    # engine only after this returns successfully, so letting a failure
    # propagate (e.g. two processes racing the same drop/recreate) leaves the
    # engine uncached and lets the next best-effort cache call retry, rather than
    # permanently marking init "done" with a missing table.
    inspector = sqlalchemy.inspect(engine)
    if inspector.has_table(capacity_exhaustion_table.name):
        existing = {
            c['name']
            for c in inspector.get_columns(capacity_exhaustion_table.name)
        }
        if existing != _expected_columns():
            logger.debug('capacity_cache: schema drift '
                         f'({existing} != {_expected_columns()}); recreating.')
            capacity_exhaustion_table.drop(bind=engine, checkfirst=True)
    db_utils.add_all_tables_to_db_sqlalchemy(Base.metadata, engine)


_db_manager = db_utils.DatabaseManager('capacity_cache', _create_table)


def _now(now: Optional[float]) -> float:
    return time.time() if now is None else now


def _clamp_ttl(value: float, default: float) -> float:
    """Clamps a configured TTL to ``(0, _MAX_TTL_SECONDS]``.

    A non-finite or negative value falls back to ``default``; anything above the
    hard ceiling is capped. This is the code-side half of the recoverability
    guarantee -- config validation is best-effort, this is authoritative.
    """
    if not math.isfinite(value) or value < 0:
        value = default
    return min(value, _MAX_TTL_SECONDS)


def _ttl_seconds() -> float:
    """Capacity-exhaustion TTL (config-driven, hard-clamped)."""
    return _clamp_ttl(
        float(
            skypilot_config.get_nested(
                ('provision', 'capacity_exhaustion_ttl', 'capacity_seconds'),
                _DEFAULT_CAPACITY_TTL_SECONDS)), _DEFAULT_CAPACITY_TTL_SECONDS)


def _insert_func(engine: sqlalchemy.engine.Engine):
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        return sqlite.insert
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return postgresql.insert
    raise ValueError('Unsupported database dialect: '
                     f'{engine.dialect.name}')


def _greatest(engine: sqlalchemy.engine.Engine, a, b):
    """Scalar max(a, b), dialect-aware (SQLite ``max`` / Postgres ``greatest``).

    Used to keep ``exhausted_until`` monotonic non-decreasing on upsert: two
    concurrent marks compute their expiry independently, and an older one
    committing last must not shorten a window a newer one already extended.
    """
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        return sqlalchemy.func.max(a, b)
    return sqlalchemy.func.greatest(a, b)


def _prune_expired(session: orm.Session, now: float) -> None:
    """Deletes rows whose TTL has passed. Called only on the write path so
    reads stay pure SELECTs (no reader ever takes a write lock)."""
    t = capacity_exhaustion_table
    session.execute(sqlalchemy.delete(t).where(t.c.exhausted_until <= now))


def mark_exhausted(key: ResourceKey, now: Optional[float] = None) -> None:
    """Records that ``key`` is capacity-exhausted until ``now + ttl``.

    Upserts the row and prunes expired rows opportunistically in the same write
    transaction. The upsert only ever *extends* the window (monotonic
    ``GREATEST``), so an older mark committing after a newer one cannot shorten
    a live block.
    """
    now = _now(now)
    exhausted_until = now + _ttl_seconds()
    engine = _db_manager.get_engine()
    insert = _insert_func(engine)
    t = capacity_exhaustion_table
    with orm.Session(engine) as session:
        _prune_expired(session, now)
        stmt = insert(t).values(cloud=key.cloud,
                                account=key.account,
                                region=key.region,
                                zone=key.zone,
                                instance_type=key.instance_type,
                                use_spot=key.use_spot,
                                num_nodes=key.num_nodes,
                                exhausted_until=exhausted_until)
        stmt = stmt.on_conflict_do_update(index_elements=[
            t.c.cloud, t.c.account, t.c.region, t.c.zone, t.c.instance_type,
            t.c.use_spot, t.c.num_nodes
        ],
                                          set_={
                                              t.c.exhausted_until: _greatest(
                                                  engine, t.c.exhausted_until,
                                                  stmt.excluded.exhausted_until)
                                          })
        session.execute(stmt)
        session.commit()


def active_exhausted_keys(candidates: Iterable[ResourceKey],
                          now: Optional[float] = None) -> Set[ResourceKey]:
    """Returns the still-exhausted subset of ``candidates``.

    A pure read that filters to the given candidate keys *in SQL* (never a full
    table scan): expired rows are excluded by the ``exhausted_until > now``
    predicate, and physical pruning is deferred to the write path -- so
    concurrent readers never contend for a write lock.
    """
    now = _now(now)
    candidate_list = list(candidates)
    if not candidate_list:
        return set()
    engine = _db_manager.get_engine()
    t = capacity_exhaustion_table
    key_cols = (t.c.cloud, t.c.account, t.c.region, t.c.zone, t.c.instance_type,
                t.c.use_spot, t.c.num_nodes)
    key_tuples = [(k.cloud, k.account, k.region, k.zone, k.instance_type,
                   k.use_spot, k.num_nodes) for k in candidate_list]
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(*key_cols).where(
                sqlalchemy.tuple_(*key_cols).in_(key_tuples)).where(
                    t.c.exhausted_until > now))
        return {
            ResourceKey(cloud=row.cloud,
                        account=row.account,
                        region=row.region,
                        zone=row.zone,
                        instance_type=row.instance_type,
                        use_spot=bool(row.use_spot),
                        num_nodes=int(row.num_nodes)) for row in rows
        }


def clear(key: ResourceKey) -> None:
    """Clears the exact ``key`` on a successful provision.

    Scoped to the precise shape+zone that just succeeded -- NOT its zone
    siblings: a success in one AZ says nothing about a genuinely-exhausted
    sibling AZ, and erasing the sibling would make the next launch re-probe (and
    re-fail) it, defeating the point. The caller clears one key per zone it
    actually attempted.
    """
    engine = _db_manager.get_engine()
    t = capacity_exhaustion_table
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(t).where(t.c.cloud == key.cloud).where(
                t.c.account == key.account).where(
                    t.c.region == key.region).where(t.c.zone == key.zone).where(
                        t.c.instance_type == key.instance_type).where(
                            t.c.use_spot == key.use_spot).where(
                                t.c.num_nodes == key.num_nodes))
        session.commit()
