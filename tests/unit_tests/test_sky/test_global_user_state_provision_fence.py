"""Generation-fence regressions for provisioning state writes."""
# pylint: disable=protected-access
import pytest

from sky import global_user_state
from sky.skylet import constants
from sky.utils import status_lib
from sky.utils.db import db_utils


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


class _Handle:
    launched_resources = None

    def __init__(self, marker: str):
        self.marker = marker
        self.stable_internal_external_ips = [('1.2.3.4', '5.6.7.8')]


def _add_cluster(name: str,
                 marker: str,
                 *,
                 ready: bool = False,
                 provision_log_path: str | None = None) -> str:
    return global_user_state.add_or_update_cluster(
        cluster_name=name,
        cluster_handle=_Handle(marker),
        requested_resources=set(),
        ready=ready,
        provision_log_path=provision_log_path,
    )


def test_stale_ready_write_cannot_restore_deleted_cluster(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('race', 'initial')
    global_user_state.remove_cluster('race', terminate=True)

    with pytest.raises(ValueError, match=cluster_hash):
        global_user_state.add_or_update_cluster(
            cluster_name='race',
            cluster_handle=_Handle('stale-ready'),
            requested_resources=set(),
            ready=True,
            existing_cluster_hash=cluster_hash,
        )

    assert global_user_state.get_cluster_from_name('race') is None


def test_stale_completion_writes_cannot_modify_replacement(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    stale_hash = _add_cluster('race', 'stale')
    global_user_state.remove_cluster('race', terminate=True)
    replacement_hash = _add_cluster('race', 'replacement')
    assert replacement_hash != stale_hash

    assert global_user_state.get_handle_from_cluster_name(
        'race', existing_cluster_hash=stale_hash) is None
    with pytest.raises(ValueError, match=stale_hash):
        global_user_state.update_cluster_handle(
            'race', _Handle('stale-handle'), existing_cluster_hash=stale_hash)
    with pytest.raises(ValueError, match=stale_hash):
        global_user_state.set_owner_identity_for_cluster(
            'race', ['stale-owner'], existing_cluster_hash=stale_hash)

    global_user_state.add_cluster_event(
        'race',
        status_lib.ClusterStatus.UP,
        'stale success',
        global_user_state.ClusterEventType.STATUS_CHANGE,
        existing_cluster_hash=stale_hash)
    with pytest.raises(ValueError, match=stale_hash):
        global_user_state.add_or_update_cluster(
            cluster_name='race',
            cluster_handle=_Handle('stale-ready'),
            requested_resources=set(),
            ready=True,
            existing_cluster_hash=stale_hash,
        )

    replacement = global_user_state.get_cluster_from_name('race')
    assert replacement is not None
    assert replacement['cluster_hash'] == replacement_hash
    assert replacement['handle'].marker == 'replacement'
    assert replacement['owner'] is None
    assert replacement['status'] is status_lib.ClusterStatus.INIT
    assert 'stale success' not in global_user_state.get_cluster_events(
        cluster_name=None,
        cluster_hash=replacement_hash,
        event_type=global_user_state.ClusterEventType.STATUS_CHANGE)


def test_stale_failure_cleanup_cannot_modify_replacement(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    stale_hash = _add_cluster('race', 'stale')
    global_user_state.remove_cluster('race', terminate=True)
    replacement_hash = _add_cluster('race', 'replacement')

    global_user_state.remove_cluster('race',
                                     terminate=False,
                                     existing_cluster_hash=stale_hash)
    replacement = global_user_state.get_cluster_from_name('race')
    assert replacement is not None
    assert replacement['cluster_hash'] == replacement_hash
    assert replacement['status'] is status_lib.ClusterStatus.INIT
    assert replacement['handle'].marker == 'replacement'
    assert replacement['handle'].stable_internal_external_ips is not None

    global_user_state.remove_cluster('race',
                                     terminate=True,
                                     existing_cluster_hash=stale_hash)
    replacement = global_user_state.get_cluster_from_name('race')
    assert replacement is not None
    assert replacement['cluster_hash'] == replacement_hash
    assert replacement['handle'].marker == 'replacement'


def test_matching_failure_cleanup_preserves_stop_and_terminate_behavior(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('race',
                                'matching',
                                ready=True,
                                provision_log_path='/logs/provision.log')

    global_user_state.remove_cluster('race',
                                     terminate=False,
                                     existing_cluster_hash=cluster_hash)
    stopped = global_user_state.get_cluster_from_name('race')
    assert stopped is not None
    assert stopped['status'] is status_lib.ClusterStatus.STOPPED
    assert stopped['handle'].stable_internal_external_ips is None

    global_user_state.remove_cluster('race',
                                     terminate=True,
                                     existing_cluster_hash=cluster_hash)
    assert global_user_state.get_cluster_from_name('race') is None
    assert (global_user_state.get_cluster_history_provision_log_path('race') ==
            '/logs/provision.log')
    usage_intervals = global_user_state._get_cluster_usage_intervals(
        cluster_hash)
    assert usage_intervals
    assert usage_intervals[-1][1] is not None
