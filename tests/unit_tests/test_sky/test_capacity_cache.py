"""Tests for the DB-backed capacity-exhaustion cache.

The central guarantee under test is *recoverability*: every entry carries a
bounded TTL and no code path ever yields a permanent block.
"""
# pylint: disable=invalid-name,protected-access
import math

import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import orm

from sky.provision import capacity_cache
from sky.utils import common_utils


def _key(zone: str = 'us-east-1a',
         use_spot: bool = True,
         num_nodes: int = 1,
         account: str = '0001'):
    return capacity_cache.ResourceKey(cloud='aws',
                                      account=account,
                                      region='us-east-1',
                                      zone=zone,
                                      instance_type='g6.4xlarge',
                                      use_spot=use_spot,
                                      num_nodes=num_nodes)


@pytest.fixture
def _mock_cache_db(tmp_path, monkeypatch):
    """Point the capacity cache at a fresh sqlite DB and pin the TTL."""
    db_path = tmp_path / 'capacity_cache_testing.db'
    engine = create_engine(f'sqlite:///{db_path}')
    monkeypatch.setattr(capacity_cache._db_manager, '_engine', engine)
    capacity_cache.Base.metadata.create_all(engine)
    monkeypatch.setattr(capacity_cache, '_ttl_seconds', lambda: 120.0)
    yield engine


def _row_count(engine) -> int:
    with orm.Session(engine) as session:
        return session.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(  # pylint: disable=not-callable
                capacity_cache.capacity_exhaustion_table)).scalar()


def test_round_trip(_mock_cache_db):
    """mark_exhausted -> active_exhausted_keys returns it."""
    key = _key()
    capacity_cache.mark_exhausted(key, now=1000.0)
    active = capacity_cache.active_exhausted_keys([key], now=1000.0)
    assert active == {key}


def test_active_filters_non_candidates(_mock_cache_db):
    marked = _key(zone='us-east-1a')
    other = _key(zone='us-east-1b')
    capacity_cache.mark_exhausted(marked, now=1000.0)
    # Only the marked key is exhausted; the sibling candidate is not returned.
    assert capacity_cache.active_exhausted_keys([other], now=1000.0) == set()
    assert capacity_cache.active_exhausted_keys([marked, other],
                                                now=1000.0) == {marked}
    # An empty candidate set never touches the DB.
    assert capacity_cache.active_exhausted_keys([], now=1000.0) == set()


def test_ttl_expiry_forces_reprobe(_mock_cache_db):
    """Past the TTL, the key is no longer active -> real re-probe happens."""
    key = _key()
    capacity_cache.mark_exhausted(key, now=1000.0)
    # Just before expiry (1000 + 120): still blocked.
    assert capacity_cache.active_exhausted_keys([key], now=1119.0) == {key}
    # After expiry: not blocked (filtered by the `> now` read predicate).
    assert capacity_cache.active_exhausted_keys([key], now=1121.0) == set()
    # Reads never write, so the expired row physically lingers...
    assert _row_count(_mock_cache_db) == 1
    # ...until the next write prunes it. Marking a different shape sweeps the
    # stale row, leaving only the fresh one.
    capacity_cache.mark_exhausted(_key(zone='other'), now=1121.0)
    assert _row_count(_mock_cache_db) == 1
    assert capacity_cache.active_exhausted_keys([key], now=1121.0) == set()


def test_clear_removes_only_the_exact_zone(_mock_cache_db):
    """A success in one AZ clears ONLY that AZ -- a genuinely-exhausted sibling
    stays blocked, so the next launch does not re-probe (and re-fail) it."""
    zone_a = _key(zone='us-east-1a')
    zone_b = _key(zone='us-east-1b')
    capacity_cache.mark_exhausted(zone_a, now=0.0)
    capacity_cache.mark_exhausted(zone_b, now=0.0)

    capacity_cache.clear(zone_a)

    active = capacity_cache.active_exhausted_keys([zone_a, zone_b], now=1.0)
    # Only zone_a is cleared; the still-exhausted sibling zone_b remains blocked.
    assert active == {zone_b}


def test_account_is_part_of_the_key(_mock_cache_db):
    """AZ names are account-specific, so entries are isolated per account: one
    account's exhaustion must not block, or be cleared by, another's."""
    acct_a = _key(account='0001')
    acct_b = _key(account='0002')
    capacity_cache.mark_exhausted(acct_a, now=0.0)
    # acct_b's identically-named AZ is untouched.
    assert capacity_cache.active_exhausted_keys([acct_a, acct_b],
                                                now=1.0) == {acct_a}
    # A success under acct_b does not clear acct_a's block.
    capacity_cache.clear(acct_b)
    assert capacity_cache.active_exhausted_keys([acct_a, acct_b],
                                                now=1.0) == {acct_a}


def test_num_nodes_is_part_of_the_key(_mock_cache_db):
    """A failed N-node request must not block a smaller request of the same
    shape, and a small success must not clear the larger block."""
    one = _key(num_nodes=1)
    many = _key(num_nodes=64)
    capacity_cache.mark_exhausted(many, now=0.0)
    # The 64-node exhaustion leaves the 1-node shape probeable.
    assert capacity_cache.active_exhausted_keys([one, many], now=1.0) == {many}
    # A 1-node success does not clear the 64-node block.
    capacity_cache.clear(one)
    assert capacity_cache.active_exhausted_keys([many], now=1.0) == {many}


def test_mark_upsert_extends_window(_mock_cache_db):
    key = _key()
    capacity_cache.mark_exhausted(key, now=0.0)  # until 120
    capacity_cache.mark_exhausted(key, now=100.0)  # until 220
    assert _row_count(_mock_cache_db) == 1
    assert capacity_cache.active_exhausted_keys([key], now=150.0) == {key}


def test_mark_upsert_is_monotonic(_mock_cache_db):
    """An older mark committing after a newer one must not shorten the window:
    the upsert takes GREATEST(existing, new), so a stale expiry never wins."""
    key = _key()
    capacity_cache.mark_exhausted(key, now=100.0)  # until 220
    # A later call with an EARLIER timestamp (e.g. a slow, older failure landing
    # after a fresher one) would compute until=120 -- it must not overwrite 220.
    capacity_cache.mark_exhausted(key, now=0.0)  # would be until 120
    assert _row_count(_mock_cache_db) == 1
    # Still blocked at 150 (220 held), not expired as it would be under 120.
    assert capacity_cache.active_exhausted_keys([key], now=150.0) == {key}


def test_ttl_clamped_to_hard_ceiling(monkeypatch):
    """A non-finite or oversized configured TTL cannot outlive the hard cap --
    the code-side backstop for the recoverability guarantee."""
    # Non-finite falls back to the default, then is capped.
    assert capacity_cache._clamp_ttl(math.inf, 30.0) == 30.0
    assert capacity_cache._clamp_ttl(float('nan'), 45.0) == 45.0
    assert capacity_cache._clamp_ttl(-5.0, 30.0) == 30.0
    # A finite-but-huge value is capped at the ceiling.
    assert capacity_cache._clamp_ttl(10 * capacity_cache._MAX_TTL_SECONDS,
                                     30.0) == capacity_cache._MAX_TTL_SECONDS
    # A sane value passes through untouched.
    assert capacity_cache._clamp_ttl(60.0, 30.0) == 60.0

    # End to end: a finite-but-huge config value is capped at the ceiling.
    monkeypatch.setattr(capacity_cache.skypilot_config, 'get_nested',
                        lambda *a, **k: 10 * capacity_cache._MAX_TTL_SECONDS)
    assert capacity_cache._ttl_seconds() == capacity_cache._MAX_TTL_SECONDS
    # End to end: a non-finite config value falls back to the (bounded) default.
    monkeypatch.setattr(capacity_cache.skypilot_config, 'get_nested',
                        lambda *a, **k: math.inf)
    assert capacity_cache._ttl_seconds(
    ) == capacity_cache._DEFAULT_CAPACITY_TTL_SECONDS


def test_create_table_recreates_on_schema_drift(tmp_path):
    """A pre-existing table with a stale column set is dropped and recreated
    (disposable hint data, no migration story) rather than silently breaking
    every read/write on the missing columns."""
    db_path = tmp_path / 'drift.db'
    engine = create_engine(f'sqlite:///{db_path}')
    # An older schema: no num_nodes, has a legacy `reason` column, plus a stray
    # row.
    old_md = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'capacity_exhaustion', old_md,
        sqlalchemy.Column('cloud', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('region', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('zone', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('instance_type', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('use_spot', sqlalchemy.Boolean, primary_key=True),
        sqlalchemy.Column('reason', sqlalchemy.Text),
        sqlalchemy.Column('exhausted_until', sqlalchemy.Float))
    old_md.create_all(engine)

    capacity_cache._create_table(engine)

    inspector = sqlalchemy.inspect(engine)
    cols = {c['name'] for c in inspector.get_columns('capacity_exhaustion')}
    assert cols == capacity_cache._expected_columns()
    assert 'num_nodes' in cols and 'reason' not in cols


def test_create_table_enables_wal_on_sqlite(tmp_path):
    """The SQLite WAL/busy_timeout pragma listener is registered during
    table creation, so even the first pooled connection gets WAL."""
    if common_utils.is_wsl():
        pytest.skip(
            'WAL is deliberately skipped on WSL (known locking issues).')
    db_path = tmp_path / 'wal.db'
    engine = create_engine(f'sqlite:///{db_path}')
    capacity_cache._create_table(engine)
    with engine.connect() as conn:
        mode = conn.exec_driver_sql('PRAGMA journal_mode').scalar()
    assert mode.lower() == 'wal'


def test_recoverability_no_permanent_block(_mock_cache_db):
    """The key guarantee: exhaustion always auto-lifts.

    Mark a shape exhausted, advance past the TTL, and assert it is probed
    again; on a now-available attempt it succeeds and is cleared. No path
    yields a permanent block.
    """
    engine = _mock_cache_db
    key = _key()

    capacity_cache.mark_exhausted(key, now=1000.0)
    assert capacity_cache.active_exhausted_keys([key], now=1000.0) == {key}

    # (1) Time alone lifts the block once the TTL passes -- a real re-probe is
    # now allowed WITHOUT any explicit clear.
    assert capacity_cache.active_exhausted_keys([key], now=2000.0) == set()

    # (2) That real re-probe finds capacity -> clear() removes the entry so
    # availability is reflected immediately.
    capacity_cache.mark_exhausted(key, now=2000.0)  # re-armed
    capacity_cache.clear(key)  # provision succeeded
    assert capacity_cache.active_exhausted_keys([key], now=2001.0) == set()
    assert _row_count(engine) == 0
