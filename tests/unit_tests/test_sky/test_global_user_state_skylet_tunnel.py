"""Incarnation-fenced Skylet tunnel metadata state tests."""

import pickle
from unittest import mock
import uuid

import pytest
from sqlalchemy import orm

from sky import global_user_state
from sky.backends import skylet_transport
from sky.skylet import constants
from sky.utils.db import db_utils


class _Handle:
    launched_resources = None
    stable_internal_external_ips = [('1.2.3.4', '5.6.7.8')]


def _fresh_db(tmp_path, monkeypatch):
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    monkeypatch.setattr(
        global_user_state,
        '_db_manager',
        db_utils.DatabaseManager(
            'state',
            global_user_state.create_table,
            post_init_fn=lambda _: global_user_state._sqlite_supports_returning(
            ),
        ),
    )


def _add_cluster(name: str) -> str:
    return global_user_state.add_or_update_cluster(
        cluster_name=name,
        cluster_handle=_Handle(),
        requested_resources=set(),
        ready=False,
    )


def test_tunnel_snapshot_preserves_raw_blob_and_cas(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('tunnel-cas')
    empty = global_user_state.get_cluster_skylet_ssh_tunnel_snapshot(
        'tunnel-cas')
    assert empty is not None
    assert empty.cluster_hash == cluster_hash
    assert empty.metadata is None
    assert empty.serialized_metadata is None

    generation = str(uuid.UUID(int=1))
    metadata = (12345, 23456, generation)
    assert global_user_state.compare_and_set_cluster_skylet_ssh_tunnel_metadata(
        'tunnel-cas', observed=empty,
        replacement=metadata) is skylet_transport.TunnelMutationResult.UPDATED

    published = global_user_state.get_cluster_skylet_ssh_tunnel_snapshot(
        'tunnel-cas')
    assert published is not None
    assert published.cluster_hash == cluster_hash
    assert published.metadata == metadata
    assert published.serialized_metadata == pickle.dumps(metadata)

    assert global_user_state.compare_and_set_cluster_skylet_ssh_tunnel_metadata(
        'tunnel-cas', observed=empty,
        replacement=(1, 1)) is skylet_transport.TunnelMutationResult.CONFLICT
    assert global_user_state.get_cluster_skylet_ssh_tunnel_snapshot(
        'tunnel-cas') == published

    assert global_user_state.compare_and_set_cluster_skylet_ssh_tunnel_metadata(
        'tunnel-cas', observed=published,
        replacement=None) is skylet_transport.TunnelMutationResult.UPDATED
    cleared = global_user_state.get_cluster_skylet_ssh_tunnel_snapshot(
        'tunnel-cas')
    assert cleared is not None
    assert cleared.metadata is None
    assert cleared.serialized_metadata is None


def test_tunnel_cas_nonnull_hash_and_generation_fence_recreation(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    old_hash = _add_cluster('tunnel-recreated')
    empty = global_user_state.get_cluster_skylet_ssh_tunnel_snapshot(
        'tunnel-recreated')
    assert empty is not None
    first = (12000, 22000, str(uuid.UUID(int=2)))
    assert global_user_state.compare_and_set_cluster_skylet_ssh_tunnel_metadata(
        'tunnel-recreated', observed=empty,
        replacement=first) is skylet_transport.TunnelMutationResult.UPDATED
    stale = global_user_state.get_cluster_skylet_ssh_tunnel_snapshot(
        'tunnel-recreated')
    assert stale is not None
    assert stale.cluster_hash == old_hash

    engine = global_user_state._db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(global_user_state.cluster_table.delete().where(
            global_user_state.cluster_table.c.name == 'tunnel-recreated'))
        session.execute(global_user_state.cluster_table.insert().values(
            name='tunnel-recreated',
            cluster_hash='replacement-hash',
            skylet_ssh_tunnel_metadata=stale.serialized_metadata,
        ))
        session.commit()

    assert global_user_state.compare_and_set_cluster_skylet_ssh_tunnel_metadata(
        'tunnel-recreated', observed=stale,
        replacement=None) is skylet_transport.TunnelMutationResult.CONFLICT
    replacement = global_user_state.get_cluster_skylet_ssh_tunnel_snapshot(
        'tunnel-recreated')
    assert replacement is not None
    assert replacement.cluster_hash == 'replacement-hash'
    assert replacement.serialized_metadata == stale.serialized_metadata

    second = (first[0], first[1], str(uuid.UUID(int=3)))
    assert global_user_state.compare_and_set_cluster_skylet_ssh_tunnel_metadata(
        'tunnel-recreated', observed=replacement,
        replacement=second) is skylet_transport.TunnelMutationResult.UPDATED
    assert global_user_state.compare_and_set_cluster_skylet_ssh_tunnel_metadata(
        'tunnel-recreated', observed=replacement,
        replacement=None) is skylet_transport.TunnelMutationResult.CONFLICT


def test_tunnel_mutation_fails_closed_for_null_hash_aba(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('unfenced-tunnel')
    raw_metadata = pickle.dumps((13000, 23000))
    engine = global_user_state._db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(global_user_state.cluster_table.update().where(
            global_user_state.cluster_table.c.name == 'unfenced-tunnel').values(
                cluster_hash=None,
                skylet_ssh_tunnel_metadata=raw_metadata,
            ))
        session.commit()

    stale = global_user_state.get_cluster_skylet_ssh_tunnel_snapshot(
        'unfenced-tunnel')
    assert stale is not None
    assert stale.cluster_hash is None
    assert stale.serialized_metadata == raw_metadata

    with orm.Session(engine) as session:
        session.execute(global_user_state.cluster_table.delete().where(
            global_user_state.cluster_table.c.name == 'unfenced-tunnel'))
        session.execute(global_user_state.cluster_table.insert().values(
            name='unfenced-tunnel',
            cluster_hash=None,
            skylet_ssh_tunnel_metadata=raw_metadata,
        ))
        session.commit()

    with mock.patch.object(global_user_state._db_manager,
                           'get_engine') as get_engine:
        outcome = (global_user_state.
                   compare_and_set_cluster_skylet_ssh_tunnel_metadata(
                       'unfenced-tunnel',
                       observed=stale,
                       replacement=None,
                   ))
    assert outcome is (
        skylet_transport.TunnelMutationResult.UNFENCED_CLUSTER_INCARNATION)
    get_engine.assert_not_called()
    replacement = global_user_state.get_cluster_skylet_ssh_tunnel_snapshot(
        'unfenced-tunnel')
    assert replacement is not None
    assert replacement.cluster_hash is None
    assert replacement.serialized_metadata == raw_metadata


@pytest.mark.parametrize('malformed', [
    pickle.dumps(('wrong-shape',)),
    pickle.dumps(None),
    b'not-a-pickle',
])
def test_exact_blob_repair_can_clear_malformed_metadata(tmp_path, monkeypatch,
                                                        malformed):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('malformed-tunnel')
    engine = global_user_state._db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(global_user_state.cluster_table.update().where(
            global_user_state.cluster_table.c.name ==
            'malformed-tunnel').values(skylet_ssh_tunnel_metadata=malformed))
        session.commit()

    observed = global_user_state.get_cluster_skylet_ssh_tunnel_snapshot(
        'malformed-tunnel')
    assert observed is not None
    assert observed.serialized_metadata == malformed
    assert observed.metadata is not None
    assert global_user_state.compare_and_set_cluster_skylet_ssh_tunnel_metadata(
        'malformed-tunnel', observed=observed,
        replacement=None) is skylet_transport.TunnelMutationResult.UPDATED
