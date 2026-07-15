"""Tests for the fail-open operator notification writer."""
from unittest import mock

from sky.utils import operator_notifications


def test_record_notification_is_fail_open(monkeypatch):
    persist = mock.Mock(side_effect=RuntimeError('database unavailable'))
    monkeypatch.setattr(operator_notifications.global_user_state,
                        'record_operator_notification', persist)
    monkeypatch.setattr(operator_notifications, '_failure_reported', False)

    assert not operator_notifications.record_notification(
        operator_notifications.OperatorNotificationCategory.INSUFFICIENT_QUOTA,
        'quota is exhausted',
        dedupe_window_seconds=3600)
    persist.assert_called_once()


def test_record_notification_bounds_message(monkeypatch):
    persist = mock.Mock()
    monkeypatch.setattr(operator_notifications.global_user_state,
                        'record_operator_notification', persist)

    assert operator_notifications.record_notification(
        operator_notifications.OperatorNotificationCategory.INSUFFICIENT_QUOTA,
        'x' * (operator_notifications.MAX_MESSAGE_LENGTH + 10),
        dedupe_window_seconds=3600)
    assert len(
        persist.call_args.args[1]) == operator_notifications.MAX_MESSAGE_LENGTH
