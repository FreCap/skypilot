"""Signed, scope-bound cursors for operational events."""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import hmac
import json
from typing import Any
import uuid

from sky.events import api_models

_CURSOR_VERSION = 1
_CURSOR_SCOPE = 'operational-events'
_CURSOR_KEY_SALT = b'skypilot-operational-events-cursor-v1\0'
_MAX_CURSOR_BYTES = 16 * 1024


class StaleCursorError(ValueError):
    """The cursor is invalid for the current query or authorization scope."""


@dataclasses.dataclass(frozen=True)
class CursorState:
    position: int
    high_watermark: int


@dataclasses.dataclass(frozen=True)
class CursorBindings:
    principal_id: str
    is_admin: bool
    workspaces: tuple[str, ...]
    filters: dict[str, Any]


def derive_key(authority_id: str) -> bytes:
    """Derive an event-specific HMAC key from the shared DB authority."""
    try:
        authority = uuid.UUID(authority_id)
    except (TypeError, ValueError, AttributeError) as e:
        raise RuntimeError(
            'Operational event cursor authority is invalid.') from e
    return hashlib.sha256(_CURSOR_KEY_SALT + authority.bytes).digest()


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b'=').decode('ascii')


def _decode(value: str) -> bytes:
    try:
        padding = '=' * (-len(value) % 4)
        return base64.urlsafe_b64decode(value + padding)
    except (ValueError, UnicodeEncodeError) as e:
        raise StaleCursorError('Invalid operational event cursor.') from e


def issue(
    key: bytes,
    bindings: CursorBindings,
    direction: api_models.TraversalDirection,
    state: CursorState,
) -> str:
    """Issue one opaque cursor bound to query and authorization state."""
    payload = {
        'v': _CURSOR_VERSION,
        'scope': _CURSOR_SCOPE,
        'principal_id': bindings.principal_id,
        'is_admin': bindings.is_admin,
        'workspaces': list(bindings.workspaces),
        'filters': bindings.filters,
        'direction': direction.value,
        'state': {
            'position': state.position,
            'high_watermark': state.high_watermark,
        },
    }
    encoded_payload = json.dumps(payload, sort_keys=True,
                                 separators=(',', ':')).encode('utf-8')
    signature = hmac.new(key, encoded_payload, hashlib.sha256).digest()
    return f'{_encode(encoded_payload)}.{_encode(signature)}'


def verify(
    cursor: str,
    key: bytes,
    bindings: CursorBindings,
    direction: api_models.TraversalDirection,
) -> CursorState:
    """Verify and decode one cursor without exposing mismatch details."""
    if len(cursor.encode('utf-8')) > _MAX_CURSOR_BYTES:
        raise StaleCursorError('Invalid operational event cursor.')
    try:
        payload_part, signature_part = cursor.split('.', 1)
    except ValueError as e:
        raise StaleCursorError('Invalid operational event cursor.') from e
    encoded_payload = _decode(payload_part)
    supplied_signature = _decode(signature_part)
    expected_signature = hmac.new(key, encoded_payload, hashlib.sha256).digest()
    if not hmac.compare_digest(supplied_signature, expected_signature):
        raise StaleCursorError('Invalid operational event cursor.')
    try:
        payload = json.loads(encoded_payload)
        expected = {
            'v': _CURSOR_VERSION,
            'scope': _CURSOR_SCOPE,
            'principal_id': bindings.principal_id,
            'is_admin': bindings.is_admin,
            'workspaces': list(bindings.workspaces),
            'filters': bindings.filters,
            'direction': direction.value,
        }
        if not isinstance(payload, dict):
            raise ValueError
        for name, expected_value in expected.items():
            if payload.get(name) != expected_value:
                raise ValueError
        if set(payload) != set(expected) | {'state'}:
            raise ValueError
        raw_state = payload['state']
        if not isinstance(raw_state, dict) or set(raw_state) != {
                'position', 'high_watermark'
        }:
            raise ValueError
        position = raw_state['position']
        high_watermark = raw_state['high_watermark']
        if (isinstance(position, bool) or not isinstance(position, int) or
                isinstance(high_watermark, bool) or
                not isinstance(high_watermark, int) or position < 0 or
                position > high_watermark):
            raise ValueError
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as e:
        raise StaleCursorError('Invalid operational event cursor.') from e
    return CursorState(position=position, high_watermark=high_watermark)
