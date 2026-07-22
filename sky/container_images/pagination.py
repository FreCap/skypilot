"""Opaque workspace/filter-bound cursors for direct image read APIs."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any

from sky.container_images import catalog_state

_CURSOR_VERSION = 1


class InvalidCursorError(ValueError):
    pass


def _key() -> bytes:
    authority = catalog_state.get_catalog_authority_id()
    return hashlib.sha256(
        f'skypilot-image-cursor:{authority}'.encode()).digest()


def encode(*, scope: str, workspace: str, filters: dict[str, Any],
           key: tuple[int, str]) -> str:
    body = json.dumps(
        {
            'v': _CURSOR_VERSION,
            'scope': scope,
            'workspace': workspace,
            'filters': filters,
            'key': [key[0], key[1]],
        },
        sort_keys=True,
        separators=(',', ':')).encode()
    signature = hmac.new(_key(), body, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(body + signature).decode().rstrip('=')


def decode(cursor: str, *, scope: str, workspace: str,
           filters: dict[str, Any]) -> tuple[int, str]:
    if not isinstance(cursor, str) or not cursor or len(cursor) > 4096:
        raise InvalidCursorError('Invalid image page cursor.')
    try:
        padded = cursor + '=' * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded)
        body, signature = raw[:-32], raw[-32:]
        if len(body) == 0 or not hmac.compare_digest(
                signature,
                hmac.new(_key(), body, hashlib.sha256).digest()):
            raise InvalidCursorError('Invalid image page cursor.')
        payload = json.loads(body)
    except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
        raise InvalidCursorError('Invalid image page cursor.') from None
    if (not isinstance(payload, dict) or payload.get('v') != _CURSOR_VERSION or
            payload.get('scope') != scope or
            payload.get('workspace') != workspace or
            payload.get('filters') != filters):
        raise InvalidCursorError('Image page cursor does not match this query.')
    key = payload.get('key')
    if (not isinstance(key, list) or len(key) != 2 or
            not isinstance(key[0], int) or not isinstance(key[1], str)):
        raise InvalidCursorError('Invalid image page cursor.')
    return key[0], key[1]
