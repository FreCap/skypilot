"""Tests for low-cardinality operator notification state."""
import concurrent.futures
import inspect

from sky import global_user_state
from sky.skylet import constants
from sky.utils.db import db_utils


def _wrapper_depth(func):
    depth = 0
    while hasattr(func, '__wrapped__'):
        depth += 1
        func = func.__wrapped__
    return depth


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


def test_operator_notification_facade_contract(tmp_path, monkeypatch):
    # Private members below are intentional compatibility seams.
    # pylint: disable=protected-access
    assert str(inspect.signature(
        global_user_state.record_operator_notification)) == (
            '(category: str, message: str, dedupe_window_seconds: int, '
            'emitted_at: int | None = None) -> None')
    assert str(inspect.signature(
        global_user_state.get_operator_notifications)) == (
            '(user_id: str, since: int) -> dict[str, typing.Any]')
    assert str(
        inspect.signature(
            global_user_state.mark_operator_notifications_read)) == (
                '(user_id: str, through_sequence: int, '
                'updated_at: int | None = None) -> int')
    for function_name in (
            'record_operator_notification',
            'get_operator_notifications',
            'mark_operator_notifications_read',
    ):
        function = getattr(global_user_state, function_name)
        assert function.__module__ == global_user_state.__name__
        assert function.__qualname__ == function_name
    assert _wrapper_depth(global_user_state.record_operator_notification) == 1
    assert _wrapper_depth(global_user_state.get_operator_notifications) == 1
    assert _wrapper_depth(
        global_user_state.mark_operator_notifications_read) == 2
    assert callable(global_user_state._operator_notification_insert_func)
    assert callable(global_user_state._next_operator_notification_sequence)

    _fresh_db(tmp_path, monkeypatch)
    delegate = global_user_state._db_manager
    delegate.get_engine()

    class CountingDatabaseManager:

        def __init__(self):
            self.get_engine_calls = 0

        def get_engine(self):
            self.get_engine_calls += 1
            return delegate.get_engine()

    counting_manager = CountingDatabaseManager()
    monkeypatch.setattr(global_user_state, '_db_manager', counting_manager)
    global_user_state.record_operator_notification('insufficient_quota',
                                                   'quota',
                                                   3600,
                                                   emitted_at=100)
    result = global_user_state.get_operator_notifications('operator', 0)
    assert global_user_state.mark_operator_notifications_read(
        'operator', result['latest_sequence'], updated_at=101) == 1
    assert counting_manager.get_engine_calls == 3
    # pylint: enable=protected-access


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


def test_stale_occurrence_does_not_replace_latest_message(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    global_user_state.record_operator_notification('insufficient_quota',
                                                   'newer actionable text',
                                                   3600,
                                                   emitted_at=200)
    global_user_state.record_operator_notification('insufficient_quota',
                                                   'stale actionable text',
                                                   3600,
                                                   emitted_at=150)

    notification = global_user_state.get_operator_notifications(
        'operator', 0)['notifications'][0]
    assert notification['first_seen_at'] == 150
    assert notification['last_seen_at'] == 200
    assert notification['message'] == 'newer actionable text'


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
