"""Tests for low-cardinality operator notification state."""
import concurrent.futures

from sky import global_user_state
from sky.skylet import constants
from sky.utils.db import db_utils


def _fresh_db(tmp_path, monkeypatch):
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    monkeypatch.setattr(
        global_user_state,
        '_db_manager',
        db_utils.DatabaseManager(
            'state',
            global_user_state.create_table,
            # pylint: disable=protected-access
            post_init_fn=lambda _: global_user_state._sqlite_supports_returning(
            ),
        ),
    )


def test_continuous_incident_is_coalesced_until_quiet(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)

    global_user_state.record_operator_notification(
        'insufficient_quota',
        'first',
        dedupe_window_seconds=3600,
        emitted_at=100,
    )
    first = global_user_state.get_operator_notifications('operator-a', 0)
    first_sequence = first['latest_sequence']
    assert first['unread_count'] == 1
    assert first['notifications'][0]['occurrence_count'] == 1

    assert global_user_state.mark_operator_notifications_read(
        'operator-a', first_sequence, updated_at=101) == first_sequence
    global_user_state.record_operator_notification(
        'insufficient_quota',
        'latest actionable text',
        dedupe_window_seconds=3600,
        emitted_at=200,
    )

    coalesced = global_user_state.get_operator_notifications('operator-a', 0)
    notification = coalesced['notifications'][0]
    assert notification['sequence'] == first_sequence
    assert notification['message'] == 'latest actionable text'
    assert notification['first_seen_at'] == 100
    assert notification['last_seen_at'] == 200
    assert notification['occurrence_count'] == 2
    assert notification['unread'] is False
    assert coalesced['unread_count'] == 0

    global_user_state.record_operator_notification(
        'insufficient_quota',
        'new incident',
        dedupe_window_seconds=3600,
        emitted_at=3801,
    )
    re_alerted = global_user_state.get_operator_notifications('operator-a', 0)
    assert re_alerted['latest_sequence'] > first_sequence
    assert re_alerted['notifications'][0]['unread'] is True
    assert re_alerted['notifications'][0]['occurrence_count'] == 3


def test_notification_cursors_are_isolated_monotonic_and_clamped(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    global_user_state.record_operator_notification('insufficient_quota',
                                                   'quota',
                                                   3600,
                                                   emitted_at=100)
    current = global_user_state.get_operator_notifications('operator-a', 0)
    sequence = current['latest_sequence']

    assert global_user_state.mark_operator_notifications_read(
        'operator-a', sequence) == sequence
    assert global_user_state.get_operator_notifications('operator-a',
                                                        0)['unread_count'] == 0
    assert global_user_state.get_operator_notifications('operator-b',
                                                        0)['unread_count'] == 1

    # A stale acknowledgement cannot move the cursor backward, and a buggy or
    # malicious future acknowledgement is clamped to an issued sequence.
    assert global_user_state.mark_operator_notifications_read('operator-a',
                                                              0) == sequence
    assert global_user_state.mark_operator_notifications_read(
        'operator-b', 1_000_000) == sequence


def test_notification_lookback_filters_by_latest_occurrence(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    global_user_state.record_operator_notification('insufficient_quota',
                                                   'old',
                                                   3600,
                                                   emitted_at=100)
    assert not global_user_state.get_operator_notifications(
        'operator', since=101)['notifications']

    global_user_state.record_operator_notification('insufficient_quota',
                                                   'recent',
                                                   3600,
                                                   emitted_at=200)
    recent = global_user_state.get_operator_notifications('operator', since=150)
    assert [item['message'] for item in recent['notifications']] == ['recent']


def test_concurrent_occurrences_keep_one_category_row(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)

    def record(_):
        global_user_state.record_operator_notification('insufficient_quota',
                                                       'quota',
                                                       3600,
                                                       emitted_at=100)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(record, range(20)))

    result = global_user_state.get_operator_notifications('operator', 0)
    assert len(result['notifications']) == 1
    assert result['notifications'][0]['occurrence_count'] == 20
