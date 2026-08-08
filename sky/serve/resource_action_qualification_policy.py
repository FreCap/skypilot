"""Fail-closed loader for the fixed API-role qualification trust file.

This module deliberately accepts no path, environment, database, or request
override.  Qualification policy bytes become a trust source only through the
chart-owned fixed projection and this exact canonical parser.
"""

from __future__ import annotations

import dataclasses
import json
import os
import stat
from typing import Any

from sky.serve import resource_action_authority

_QUALIFICATION_POLICY_PATH = (
    resource_action_authority.RESOURCE_ACTION_QUALIFICATION_POLICY_PATH_V1)
_MAX_POLICY_BYTES = 65_536
_READ_CHUNK_BYTES = 8_192


class ResourceActionQualificationPolicyUnavailable(RuntimeError):
    """The immutable server-owned qualification policy is unavailable."""


@dataclasses.dataclass(frozen=True)
class LoadedResourceActionQualificationPolicyV1:
    """One byte-validated policy and its recomputed fixed-path reference."""

    policy: resource_action_authority.ResourceActionQualificationPolicyV1
    reference: resource_action_authority.ResourceActionQualificationPolicyRefV1

    def __post_init__(self) -> None:
        if type(self.policy) is not (
                resource_action_authority.ResourceActionQualificationPolicyV1):
            raise TypeError('loaded qualification policy is not typed.')
        if type(self.reference) is not (resource_action_authority.
                                        ResourceActionQualificationPolicyRefV1):
            raise TypeError('loaded qualification policy reference is not '
                            'typed.')
        expected = (resource_action_authority.
                    ResourceActionQualificationPolicyRefV1.for_policy(
                        self.policy))
        if self.reference.canonical_bytes != expected.canonical_bytes:
            raise ValueError('loaded qualification policy reference does not '
                             'match its bytes.')


@dataclasses.dataclass(frozen=True)
class LoadedResourceActionQualificationPolicyV2:
    """One exact post-035 policy and its fixed-path byte reference."""

    policy: resource_action_authority.ResourceActionQualificationPolicyV2
    reference: resource_action_authority.ResourceActionQualificationPolicyRefV1

    def __post_init__(self) -> None:
        if type(self.policy) is not (
                resource_action_authority.ResourceActionQualificationPolicyV2):
            raise TypeError('loaded V2 qualification policy is not typed.')
        if type(self.reference) is not (resource_action_authority.
                                        ResourceActionQualificationPolicyRefV1):
            raise TypeError('loaded V2 policy reference is not typed.')
        expected = (resource_action_authority.
                    ResourceActionQualificationPolicyRefV1.for_policy_v2(
                        self.policy))
        if self.reference.canonical_bytes != expected.canonical_bytes:
            raise ValueError('loaded V2 policy reference does not match its '
                             'bytes.')


def _read_policy_bytes() -> bytes:
    required_flags = ('O_CLOEXEC', 'O_DIRECTORY', 'O_NOFOLLOW', 'O_NONBLOCK')
    if any(not hasattr(os, flag) for flag in required_flags):
        raise ResourceActionQualificationPolicyUnavailable(
            'Descriptor-safe qualification policy reads are unsupported.')
    descriptor: int | None = None
    try:
        components = _QUALIFICATION_POLICY_PATH.split('/')
        if (not _QUALIFICATION_POLICY_PATH.startswith('/') or
                components[0] != '' or any(component in ('', '.', '..')
                                           for component in components[1:])):
            raise ResourceActionQualificationPolicyUnavailable(
                'Qualification policy path is not the fixed absolute path.')
        directory_flags = (os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY |
                           os.O_NOFOLLOW)
        read_flags = (os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW |
                      os.O_NONBLOCK)
        directory_descriptor = os.open('/', directory_flags)
        try:
            for component in components[1:-1]:
                next_descriptor = os.open(component,
                                          directory_flags,
                                          dir_fd=directory_descriptor)
                os.close(directory_descriptor)
                directory_descriptor = next_descriptor
            descriptor = os.open(components[-1],
                                 read_flags,
                                 dir_fd=directory_descriptor)
        finally:
            os.close(directory_descriptor)
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_size < 1 or
                before.st_size > _MAX_POLICY_BYTES):
            raise ResourceActionQualificationPolicyUnavailable(
                'Qualification policy is not one bounded regular file.')
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(_READ_CHUNK_BYTES, remaining))
            if not chunk:
                raise ResourceActionQualificationPolicyUnavailable(
                    'Qualification policy changed while it was read.')
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ResourceActionQualificationPolicyUnavailable(
                'Qualification policy grew while it was read.')
        after = os.fstat(descriptor)
        stable_before = (before.st_dev, before.st_ino, before.st_mode,
                         before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        stable_after = (after.st_dev, after.st_ino, after.st_mode,
                        after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        if stable_before != stable_after:
            raise ResourceActionQualificationPolicyUnavailable(
                'Qualification policy changed while it was read.')
        return b''.join(chunks)
    except OSError as e:
        raise ResourceActionQualificationPolicyUnavailable(
            'Qualification policy cannot be opened safely.') from e
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _parse_policy_bytes(
    raw_bytes: bytes
) -> resource_action_authority.ResourceActionQualificationPolicyV1:

    def _closed_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate JSON key')
            result[key] = value
        return result

    def _forbid_noninteger_number(_: str) -> Any:
        raise ValueError('noninteger JSON number')

    try:
        value = json.loads(raw_bytes.decode('utf-8'),
                           object_pairs_hook=_closed_pairs,
                           parse_float=_forbid_noninteger_number,
                           parse_constant=_forbid_noninteger_number)
        policy = (resource_action_authority.ResourceActionQualificationPolicyV1.
                  from_value(value))
        if raw_bytes != policy.canonical_bytes:
            raise ValueError('qualification policy bytes are not canonical')
        return policy
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, TypeError,
            ValueError) as e:
        raise ResourceActionQualificationPolicyUnavailable(
            'Qualification policy bytes are invalid.') from e


def _parse_policy_bytes_v2(
    raw_bytes: bytes
) -> resource_action_authority.ResourceActionQualificationPolicyV2:
    """Parse only exact canonical V2 policy bytes; never reinterpret V1."""

    def _closed_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate JSON key')
            result[key] = value
        return result

    def _forbid_noninteger_number(_: str) -> Any:
        raise ValueError('noninteger JSON number')

    try:
        value = json.loads(raw_bytes.decode('utf-8'),
                           object_pairs_hook=_closed_pairs,
                           parse_float=_forbid_noninteger_number,
                           parse_constant=_forbid_noninteger_number)
        policy = (resource_action_authority.ResourceActionQualificationPolicyV2.
                  from_value(value))
        if raw_bytes != policy.canonical_bytes:
            raise ValueError('V2 qualification policy bytes are not canonical')
        return policy
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, TypeError,
            ValueError) as e:
        raise ResourceActionQualificationPolicyUnavailable(
            'V2 qualification policy bytes are invalid.') from e


def load_resource_action_qualification_policy_v1(
) -> LoadedResourceActionQualificationPolicyV1:
    """Load only the exact fixed-path canonical qualification policy.

    Missing, unsafe, malformed, duplicate-key, noncanonical, or oversized
    input raises one closed unavailable result.  There is intentionally no
    fallback path or mutable override.
    """

    policy = _parse_policy_bytes(_read_policy_bytes())
    reference = (resource_action_authority.
                 ResourceActionQualificationPolicyRefV1.for_policy(policy))
    return LoadedResourceActionQualificationPolicyV1(policy=policy,
                                                     reference=reference)


def load_resource_action_qualification_policy_v2(
) -> LoadedResourceActionQualificationPolicyV2:
    """Load only the exact fixed-path Serve036/037 policy contract."""

    policy = _parse_policy_bytes_v2(_read_policy_bytes())
    reference = (resource_action_authority.
                 ResourceActionQualificationPolicyRefV1.for_policy_v2(policy))
    return LoadedResourceActionQualificationPolicyV2(policy=policy,
                                                     reference=reference)
