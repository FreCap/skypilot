"""Mechanical rollback-gate tests for reserved-fill protocol v1."""
# pylint: disable=protected-access

import contextlib
from types import SimpleNamespace
from unittest import mock

import pytest

from sky.serve import reserved_capacity_broker as broker
from sky.serve import reserved_capacity_demotion
from sky.serve import serve_state
from sky.skylet import constants as skylet_constants


class _TrackedLock:
    """Small lock double that exposes whether the critical section is held."""

    def __init__(self) -> None:
        self.held = False

    @contextlib.contextmanager
    def acquire(self, *, blocking):
        assert blocking
        assert not self.held
        self.held = True
        try:
            yield
        finally:
            self.held = False


def _install_demotion_preconditions(monkeypatch, lock: _TrackedLock) -> None:
    monkeypatch.setattr(broker.locks, 'get_lock', lambda *_args: lock)
    monkeypatch.setattr(serve_state, 'get_database_engine',
                        mock.Mock(return_value=object()))
    monkeypatch.setattr(
        broker.migration_utils, 'get_current_alembic_revision',
        mock.Mock(
            side_effect=lambda _engine, section: {
                broker.migration_utils.SERVE_DB_NAME: '035',
                broker.migration_utils.API_REQUESTS_DB_NAME: '008',
            }[section]))
    monkeypatch.setattr(serve_state, 'get_reserved_fill_protocol_state',
                        mock.Mock(return_value={'protocol_version': 2}))


def test_demotion_attests_stable_token_bound_rollout_under_lock(monkeypatch):
    lock = _TrackedLock()
    _install_demotion_preconditions(monkeypatch, lock)
    rollout = mock.Mock()

    def attest():
        assert lock.held
        return rollout

    monkeypatch.setattr(broker, '_read_stable_writer_rollout', attest)

    def persist_protocol(*args, **kwargs):
        assert lock.held
        assert args == (broker.PROTOCOL_V1,)
        assert kwargs == {
            'expected_protocol_version': broker.PROTOCOL_V2,
            'changed_at': 1234.0,
        }
        return True

    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        persist_protocol)
    monkeypatch.setattr(broker.time, 'time', lambda: 1234.0)
    clear_caches = mock.Mock()
    monkeypatch.setattr(broker, 'clear_caches', clear_caches)

    assert broker.demote_protocol_v1()
    assert not lock.held
    clear_caches.assert_called_once_with()


def test_demotion_rejects_unstable_rollout_before_database_mutation(
        monkeypatch):
    lock = _TrackedLock()
    _install_demotion_preconditions(monkeypatch, lock)
    monkeypatch.setattr(
        broker, '_read_stable_writer_rollout',
        mock.Mock(
            side_effect=broker.ProtocolV2ActivationError('rollout moved')))
    setter = mock.Mock()
    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        setter)

    with pytest.raises(broker.ProtocolV2ActivationError, match='rollout moved'):
        broker.demote_protocol_v1()
    setter.assert_not_called()


def test_demotion_rejects_schema_other_than_exact_035(monkeypatch):
    lock = _TrackedLock()
    monkeypatch.setattr(broker.locks, 'get_lock', lambda *_args: lock)
    monkeypatch.setattr(serve_state, 'get_database_engine',
                        mock.Mock(return_value=object()))
    monkeypatch.setattr(broker.migration_utils, 'get_current_alembic_revision',
                        mock.Mock(return_value='034'))
    attester = mock.Mock()
    monkeypatch.setattr(broker, '_read_stable_writer_rollout', attester)
    setter = mock.Mock()
    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        setter)

    with pytest.raises(broker.ProtocolV1DemotionError,
                       match='exact Serve schema revision 035'):
        broker.demote_protocol_v1()
    attester.assert_not_called()
    setter.assert_not_called()


def test_demotion_rejects_api_request_schema_other_than_exact_008(monkeypatch):
    lock = _TrackedLock()
    monkeypatch.setattr(broker.locks, 'get_lock', lambda *_args: lock)
    monkeypatch.setattr(serve_state, 'get_database_engine',
                        mock.Mock(return_value=object()))
    monkeypatch.setattr(
        broker.migration_utils, 'get_current_alembic_revision',
        mock.Mock(
            side_effect=lambda _engine, section: {
                broker.migration_utils.SERVE_DB_NAME: '035',
                broker.migration_utils.API_REQUESTS_DB_NAME: '007',
            }[section]))
    attester = mock.Mock()
    monkeypatch.setattr(broker, '_read_stable_writer_rollout', attester)

    with pytest.raises(broker.ProtocolV1DemotionError,
                       match='exact API-request schema revision 008'):
        broker.demote_protocol_v1()
    attester.assert_not_called()


def test_demotion_rejects_inactive_protocol_before_rollout_read(monkeypatch):
    lock = _TrackedLock()
    _install_demotion_preconditions(monkeypatch, lock)
    monkeypatch.setattr(serve_state, 'get_reserved_fill_protocol_state',
                        mock.Mock(return_value={'protocol_version': 1}))
    attester = mock.Mock()
    monkeypatch.setattr(broker, '_read_stable_writer_rollout', attester)

    with pytest.raises(broker.ProtocolV1DemotionError, match='already active'):
        broker.demote_protocol_v1()
    attester.assert_not_called()


def test_demotion_rejects_failed_projection_transaction(monkeypatch):
    lock = _TrackedLock()
    _install_demotion_preconditions(monkeypatch, lock)
    monkeypatch.setattr(broker, '_read_stable_writer_rollout', mock.Mock())
    monkeypatch.setattr(serve_state, 'set_reserved_fill_protocol_version',
                        mock.Mock(return_value=False))

    with pytest.raises(broker.ProtocolV1DemotionError,
                       match='legacy projection'):
        broker.demote_protocol_v1()


def test_demotion_projection_accepts_production_pool_identity():
    claim_set = SimpleNamespace(generation=7,
                                edge_count=1,
                                semantic_hash='semantic-hash')
    edge = SimpleNamespace(service_generation=7,
                           pool_position=0,
                           weight=2.0,
                           floor_replicas=1,
                           gpus_per_replica=8,
                           holdings_fill=3,
                           effective_cap=4,
                           launchable=1,
                           heartbeat_ts=1234.0,
                           accelerator_names='["h200"]',
                           legacy_pool_key='["phx","h200"]',
                           pool_key='["v2","cluster-uid","h200"]',
                           access_context='phx',
                           physical_cluster_uid='cluster-uid',
                           demonstrated_need=None,
                           boot_hold=None,
                           activity_ts=None)

    assert serve_state._demotion_legacy_projection(
        claim_set, edge, global_generation=9) == {
            'legacy_pool_key': '["phx","h200"]',
            'weight': 2.0,
            'floor_replicas': 1,
            'gpus_per_replica': 8,
            'holdings_fill': 3,
            'effective_cap': 4,
            'launchable': 1,
            'demonstrated_need': None,
            'boot_hold': None,
            'activity_ts': None,
            'heartbeat_ts': 1234.0,
        }


def test_demotion_cli_accepts_no_identity_input(monkeypatch):
    monkeypatch.setenv(skylet_constants.ENV_VAR_DB_CONNECTION_URI,
                       'postgresql://configured')
    monkeypatch.setattr(
        serve_state, 'get_database_engine',
        lambda: SimpleNamespace(dialect=SimpleNamespace(name='postgresql')))
    demote = mock.Mock(return_value=True)
    monkeypatch.setattr(reserved_capacity_demotion.reserved_capacity_broker,
                        'demote_protocol_v1', demote)
    monkeypatch.setattr(serve_state, 'get_reserved_fill_protocol_state',
                        lambda: {
                            'protocol_version': 1,
                            'claim_generation': 7,
                        })

    exit_code, output = reserved_capacity_demotion.run_cli([])

    assert exit_code == 0
    assert output == '{"changed":true,"claim_generation":7,' \
        '"protocol_version":1}'
    demote.assert_called_once_with()


def test_demotion_cli_rejects_identity_override_arguments():
    with pytest.raises(SystemExit):
        reserved_capacity_demotion.run_cli(
            ['--namespace', 'spoofed', '--deployment', 'old-deployment'])
