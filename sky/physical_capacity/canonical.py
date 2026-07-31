"""Bounded generic canonical JSON for the revision-001 capacity schema.

Only the generic envelope and limits are defined here.  C2 adds two closed
domains for its bounded, read-only evidence scan; payload shapes remain in
``contracts`` rather than weakening the generic encoder.
"""

import enum
import hashlib
import json
from typing import Any

MAX_CANONICAL_DEPTH = 16
MAX_CANONICAL_ITEMS = 4096
MAX_CANONICAL_STRING_BYTES = 4096
MAX_CANONICAL_JSON_BYTES = 65536
MAX_WORKSPACE_IDENTIFIER_BYTES = 256
MAX_SOURCE_IDENTIFIER_BYTES = 256
MAX_SOURCE_KEY_BYTES = 512
MAX_ERROR_CODE_BYTES = 128

_MIN_SIGNED_64_BIT_INTEGER = -(1 << 63)
_MAX_SIGNED_64_BIT_INTEGER = (1 << 63) - 1
_SCHEMA_VERSION = 1


class CanonicalDomain(str, enum.Enum):
    """Closed identity and digest domains accepted by schema version 1."""

    PLACEMENT_CONTRACT = 'placement_contract'
    TOPOLOGY = 'topology'
    PHYSICAL_SPEC = 'physical_spec'
    INTENT = 'intent'
    SOURCE_INCARNATION = 'source_incarnation'
    SOURCE_FINGERPRINT = 'source_fingerprint'
    SOURCE_PARTITION = 'source_partition'
    PROJECTION_CURSOR = 'projection_cursor'
    SCOPE_ENTRY = 'scope_entry'
    EVIDENCE_RECORD = 'evidence_record'


def validate_bounded_string(value: object,
                            *,
                            max_bytes: int,
                            field: str,
                            allow_empty: bool = False) -> str:
    """Validate a UTF-8 string against an explicit byte limit."""
    if not isinstance(max_bytes, int) or isinstance(max_bytes,
                                                    bool) or max_bytes < 0:
        raise ValueError('max_bytes must be a non-negative integer.')
    if not isinstance(value, str):
        raise ValueError(f'{field} must be a string.')
    if not value and not allow_empty:
        raise ValueError(f'{field} must not be empty.')
    try:
        encoded = value.encode('utf-8')
    except UnicodeEncodeError as e:
        raise ValueError(f'{field} must be valid UTF-8.') from e
    if len(encoded) > max_bytes:
        raise ValueError(f'{field} must be at most {max_bytes} UTF-8 bytes.')
    return value


def _validate_value(value: object, *, depth: int, active: set[int],
                    item_count: list[int]) -> None:
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        if not _MIN_SIGNED_64_BIT_INTEGER <= value <= (
                _MAX_SIGNED_64_BIT_INTEGER):
            raise ValueError('Canonical integers must fit signed 64-bit.')
        return
    if isinstance(value, float):
        raise ValueError(
            'Canonical JSON does not permit floating-point values.')
    if isinstance(value, str):
        validate_bounded_string(value,
                                max_bytes=MAX_CANONICAL_STRING_BYTES,
                                field='Canonical string',
                                allow_empty=True)
        return

    if not isinstance(value, (dict, list)):
        raise ValueError(
            'Canonical JSON values must be objects, lists, strings, signed '
            '64-bit integers, booleans, or null.')
    if depth > MAX_CANONICAL_DEPTH:
        raise ValueError(
            f'Canonical JSON nesting exceeds {MAX_CANONICAL_DEPTH}.')

    identity = id(value)
    if identity in active:
        raise ValueError('Canonical JSON must not contain reference cycles.')
    active.add(identity)
    try:
        item_count[0] += len(value)
        if item_count[0] > MAX_CANONICAL_ITEMS:
            raise ValueError(
                f'Canonical JSON exceeds {MAX_CANONICAL_ITEMS} aggregate '
                'keys/list elements.')
        if isinstance(value, dict):
            for key, child in value.items():
                if not isinstance(key, str):
                    raise ValueError('Canonical JSON object keys must be '
                                     'strings.')
                validate_bounded_string(key,
                                        max_bytes=MAX_CANONICAL_STRING_BYTES,
                                        field='Canonical object key',
                                        allow_empty=True)
                _validate_value(child,
                                depth=depth + 1,
                                active=active,
                                item_count=item_count)
        else:
            for child in value:
                _validate_value(child,
                                depth=depth + 1,
                                active=active,
                                item_count=item_count)
    finally:
        active.remove(identity)


def validate_payload(payload: object) -> None:
    """Validate one generic canonical root object without serializing it."""
    if not isinstance(payload, dict):
        raise ValueError('Canonical payload must be a root object.')
    _validate_value(payload, depth=1, active=set(), item_count=[0])


def _normalize_domain(domain: CanonicalDomain | str) -> CanonicalDomain:
    try:
        return CanonicalDomain(domain)
    except (TypeError, ValueError) as e:
        raise ValueError(f'Unknown canonical domain: {domain!r}.') from e


def canonical_payload_json_bytes(payload: object) -> bytes:
    """Encode a bounded root object without assigning a hash domain.

    This is only for validating unhashed JSONB such as revision-001 scan
    counters.  Anything used as an identity or digest input must use
    ``canonical_json_bytes()`` with an explicit domain instead.
    """
    validate_payload(payload)
    try:
        encoded = json.dumps(payload,
                             sort_keys=True,
                             separators=(',', ':'),
                             ensure_ascii=False,
                             allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, UnicodeEncodeError) as e:
        raise ValueError('Canonical JSON encoding failed.') from e
    if len(encoded) > MAX_CANONICAL_JSON_BYTES:
        raise ValueError(
            f'Canonical JSON exceeds {MAX_CANONICAL_JSON_BYTES} bytes.')
    return encoded


def canonical_json_bytes(
    payload: object,
    *,
    domain: CanonicalDomain | str,
    schema_version: int = _SCHEMA_VERSION,
) -> bytes:
    """Encode one validated payload in its domain-separated v1 envelope."""
    if (not isinstance(schema_version, int) or
            isinstance(schema_version, bool) or
            schema_version != _SCHEMA_VERSION):
        raise ValueError(f'Canonical schema_version must be {_SCHEMA_VERSION}.')
    normalized_domain = _normalize_domain(domain)
    validate_payload(payload)
    envelope: dict[str, Any] = {
        'domain': normalized_domain.value,
        'schema_version': schema_version,
        'payload': payload,
    }
    try:
        encoded = json.dumps(envelope,
                             sort_keys=True,
                             separators=(',', ':'),
                             ensure_ascii=False,
                             allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, UnicodeEncodeError) as e:
        raise ValueError('Canonical JSON encoding failed.') from e
    if len(encoded) > MAX_CANONICAL_JSON_BYTES:
        raise ValueError(
            f'Canonical JSON exceeds {MAX_CANONICAL_JSON_BYTES} bytes.')
    return encoded


def canonical_hash(
    payload: object,
    *,
    domain: CanonicalDomain | str,
    schema_version: int = _SCHEMA_VERSION,
) -> str:
    """Return the lowercase SHA-256 digest of a canonical envelope."""
    encoded = canonical_json_bytes(payload,
                                   domain=domain,
                                   schema_version=schema_version)
    return hashlib.sha256(encoded).hexdigest()
