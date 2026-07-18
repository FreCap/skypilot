"""Tests for client-side OAuth helpers."""

from sky.client import oauth


def test_auth_callback_handler_accepts_base_log_message_keywords():
    """The logging override must preserve the base handler's call contract."""
    handler = object.__new__(oauth._AuthCallbackHandler)  # pylint: disable=protected-access

    handler.log_message(format='OAuth callback received')
