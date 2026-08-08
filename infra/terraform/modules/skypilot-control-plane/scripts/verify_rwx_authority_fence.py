"""Fail-closed verifier for the digest-sealed RWX cutover authority fence.

This file is embedded into the Helm init-container command by Terraform.  Keep
it dependency-free: the pinned SkyPilot operations image only guarantees
Python's standard library.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any

FENCE_PATH = Path('/var/run/skypilot/rwx-authority/fence.json')
MAX_FENCE_BYTES = 64 * 1024
_EXPECTED_SHA256_ENV = 'SKYPILOT_RWX_AUTHORITY_FENCE_EXPECTED_SHA256'
_EXPECTED_IDENTITY_ENV = 'SKYPILOT_RWX_AUTHORITY_FENCE_EXPECTED_IDENTITY'
_EXPECTED_POSTGRES_EVIDENCE_SHA256_ENV = (
    'SKYPILOT_RWX_AUTHORITY_FENCE_EXPECTED_POSTGRES_EVIDENCE_SHA256')

_SHA256_PATTERN = re.compile(r'^[0-9a-f]{64}$')
_UUID_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
_DNS_SUBDOMAIN_PATTERN = re.compile(
    r'^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?'
    r'(?:\.[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?)*$')
_EBS_VOLUME_PATTERN = re.compile(r'^vol-[0-9a-f]{8}(?:[0-9a-f]{9})?$')
_EBS_SNAPSHOT_PATTERN = re.compile(r'^snap-[0-9a-f]{8}(?:[0-9a-f]{9})?$')
_EFS_FILESYSTEM_PATTERN = re.compile(r'^fs-[0-9a-f]{8}(?:[0-9a-f]{9})?$')
_EFS_ACCESS_POINT_PATTERN = re.compile(r'^fsap-[0-9a-f]{8}(?:[0-9a-f]{9})?$')
_RFC3339_UTC_PATTERN = re.compile(
    r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$')


class FenceVerificationError(RuntimeError):
    """The authority fence did not satisfy the steady-state contract."""


def _exact_keys(value: Any, expected: set[str], *,
                label: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise FenceVerificationError(f'{label} must be an object.')
    actual = set(value)
    if actual != expected:
        raise FenceVerificationError(f'{label} has an invalid field set.')
    return value


def _string(value: Any, pattern: re.Pattern[str], *, label: str) -> str:
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise FenceVerificationError(f'{label} has an invalid format.')
    return value


def _dns_subdomain(value: Any, *, label: str) -> str:
    result = _string(value, _DNS_SUBDOMAIN_PATTERN, label=label)
    if len(result) > 253:
        raise FenceVerificationError(f'{label} is too long.')
    return result


def _uuid(value: Any, *, label: str) -> str:
    return _string(value, _UUID_PATTERN, label=label)


def _sha256(value: Any, *, label: str) -> str:
    return _string(value, _SHA256_PATTERN, label=label)


def _nonnegative_integer(value: Any, *, label: str) -> int:
    if type(value) is not int or value < 0 or value > 2**63 - 1:
        raise FenceVerificationError(
            f'{label} must be a nonnegative 64-bit integer.')
    return value


def _rfc3339_utc(value: Any, *, label: str) -> datetime.datetime:
    result = _string(value, _RFC3339_UTC_PATTERN, label=label)
    try:
        parsed = datetime.datetime.fromisoformat(result[:-1] + '+00:00')
    except ValueError as error:
        raise FenceVerificationError(
            f'{label} is not a real UTC timestamp.') from error
    if parsed.utcoffset() != datetime.timedelta(0):
        raise FenceVerificationError(f'{label} must use UTC.')
    return parsed


def _validate_identity(value: Any, *, label: str) -> dict[str, Any]:
    identity = _exact_keys(value,
                           {'namespace', 'release_name', 'source', 'target'},
                           label=label)
    _dns_subdomain(identity['namespace'], label=f'{label}.namespace')
    _dns_subdomain(identity['release_name'], label=f'{label}.release_name')

    source = _exact_keys(identity['source'], {
        'pvc_namespace', 'pvc_name', 'pvc_uid', 'pv_name', 'pv_uid',
        'ebs_volume_id'
    },
                         label=f'{label}.source')
    _dns_subdomain(source['pvc_namespace'],
                   label=f'{label}.source.pvc_namespace')
    _dns_subdomain(source['pvc_name'], label=f'{label}.source.pvc_name')
    _uuid(source['pvc_uid'], label=f'{label}.source.pvc_uid')
    _dns_subdomain(source['pv_name'], label=f'{label}.source.pv_name')
    _uuid(source['pv_uid'], label=f'{label}.source.pv_uid')
    _string(source['ebs_volume_id'],
            _EBS_VOLUME_PATTERN,
            label=f'{label}.source.ebs_volume_id')

    target = _exact_keys(identity['target'], {
        'state_claim_name',
        'state_pvc_namespace',
        'filesystem_id',
        'state_access_point_id',
        'state_pv_name',
        'state_pv_uid',
        'state_pvc_uid',
        'authority_claim_name',
        'authority_pvc_namespace',
        'authority_access_point_id',
        'authority_pv_name',
        'authority_pv_uid',
        'authority_pvc_uid',
    },
                         label=f'{label}.target')
    _dns_subdomain(target['state_claim_name'],
                   label=f'{label}.target.state_claim_name')
    _dns_subdomain(target['state_pvc_namespace'],
                   label=f'{label}.target.state_pvc_namespace')
    _dns_subdomain(target['authority_claim_name'],
                   label=f'{label}.target.authority_claim_name')
    _dns_subdomain(target['authority_pvc_namespace'],
                   label=f'{label}.target.authority_pvc_namespace')
    _string(target['filesystem_id'],
            _EFS_FILESYSTEM_PATTERN,
            label=f'{label}.target.filesystem_id')
    _string(target['state_access_point_id'],
            _EFS_ACCESS_POINT_PATTERN,
            label=f'{label}.target.state_access_point_id')
    _string(target['authority_access_point_id'],
            _EFS_ACCESS_POINT_PATTERN,
            label=f'{label}.target.authority_access_point_id')
    _dns_subdomain(target['state_pv_name'],
                   label=f'{label}.target.state_pv_name')
    _dns_subdomain(target['authority_pv_name'],
                   label=f'{label}.target.authority_pv_name')
    for field in ('state_pv_uid', 'state_pvc_uid', 'authority_pv_uid',
                  'authority_pvc_uid'):
        _uuid(target[field], label=f'{label}.target.{field}')

    if target['state_claim_name'] == target['authority_claim_name']:
        raise FenceVerificationError(
            f'{label} must use distinct state and authority claims.')
    if (source['pvc_namespace'] != identity['namespace'] or
            target['state_pvc_namespace'] != identity['namespace'] or
            target['authority_pvc_namespace'] != identity['namespace']):
        raise FenceVerificationError(
            f'{label} PVC namespaces must match the release namespace.')
    if target['state_access_point_id'] == target['authority_access_point_id']:
        raise FenceVerificationError(
            f'{label} must use distinct state and authority access points.')
    if target['state_pv_name'] == target['authority_pv_name']:
        raise FenceVerificationError(
            f'{label} must use distinct state and authority PVs.')
    if target['state_pv_uid'] == target['authority_pv_uid']:
        raise FenceVerificationError(
            f'{label} must use distinct state and authority PVs.')
    if target['state_pvc_uid'] == target['authority_pvc_uid']:
        raise FenceVerificationError(
            f'{label} must use distinct state and authority PVCs.')
    if source['pvc_name'] in (target['state_claim_name'],
                              target['authority_claim_name']):
        raise FenceVerificationError(
            f'{label} must use distinct source, state, and authority claims.')
    if source['pv_name'] in (target['state_pv_name'],
                             target['authority_pv_name']):
        raise FenceVerificationError(
            f'{label} must use distinct source, state, and authority PVs.')
    if source['pv_uid'] in (target['state_pv_uid'], target['authority_pv_uid']):
        raise FenceVerificationError(
            f'{label} must use distinct source, state, and authority PVs.')
    if source['pvc_uid'] in (target['state_pvc_uid'],
                             target['authority_pvc_uid']):
        raise FenceVerificationError(
            f'{label} must use distinct source, state, and authority PVCs.')
    return identity


def _validate_postgres_evidence(value: Any) -> dict[str, Any]:
    evidence = _exact_keys(value, {
        'schema_version',
        'metadata_key',
        'cutover_marker_sha256',
        'cutover_format_version',
        'cutover_completed_at',
        'cutover_request_count',
        'cutover_queue_count',
        'cutover_logical_sha256',
        'observed_at',
        'database_schema_revision',
        'current_request_count',
        'current_queue_count',
        'current_nonterminal_count',
        'current_claimed_count',
        'current_logical_sha256',
    },
                           label='fence.postgres_evidence')
    if (type(evidence['schema_version']) is not int or
            evidence['schema_version'] != 1):
        raise FenceVerificationError(
            'fence.postgres_evidence.schema_version must be 1.')
    if (type(evidence['metadata_key']) is not str or
            evidence['metadata_key'] != 'sqlite-to-postgres-cutover.v1'):
        raise FenceVerificationError(
            'fence.postgres_evidence.metadata_key is invalid.')
    _sha256(evidence['cutover_marker_sha256'],
            label='fence.postgres_evidence.cutover_marker_sha256')
    if (type(evidence['cutover_format_version']) is not int or
            evidence['cutover_format_version'] != 1):
        raise FenceVerificationError(
            'fence.postgres_evidence.cutover_format_version must be 1.')
    _rfc3339_utc(evidence['cutover_completed_at'],
                 label='fence.postgres_evidence.cutover_completed_at')
    cutover_request_count = _nonnegative_integer(
        evidence['cutover_request_count'],
        label='fence.postgres_evidence.cutover_request_count')
    cutover_queue_count = _nonnegative_integer(
        evidence['cutover_queue_count'],
        label='fence.postgres_evidence.cutover_queue_count')
    if cutover_queue_count > cutover_request_count:
        raise FenceVerificationError(
            'fence historical queue count exceeds request count.')
    _sha256(evidence['cutover_logical_sha256'],
            label='fence.postgres_evidence.cutover_logical_sha256')
    _rfc3339_utc(evidence['observed_at'],
                 label='fence.postgres_evidence.observed_at')
    revision = evidence['database_schema_revision']
    if (type(revision) is not str or not revision or len(revision) > 128 or
            re.fullmatch(r'[0-9A-Za-z._-]+', revision) is None):
        raise FenceVerificationError(
            'fence.postgres_evidence.database_schema_revision is invalid.')
    current_request_count = _nonnegative_integer(
        evidence['current_request_count'],
        label='fence.postgres_evidence.current_request_count')
    for field in ('current_queue_count', 'current_nonterminal_count',
                  'current_claimed_count'):
        if type(evidence[field]) is not int or evidence[field] != 0:
            raise FenceVerificationError(
                f'fence.postgres_evidence.{field} must be integer zero.')
    _sha256(evidence['current_logical_sha256'],
            label='fence.postgres_evidence.current_logical_sha256')
    if current_request_count < cutover_request_count:
        raise FenceVerificationError(
            'fence PostgreSQL request count regressed after cutover.')
    return evidence


def _canonical_json_bytes(value: Any, *, label: str) -> bytes:
    try:
        return json.dumps(value,
                          allow_nan=False,
                          ensure_ascii=True,
                          separators=(',', ':'),
                          sort_keys=True).encode('utf-8')
    except (TypeError, ValueError) as error:
        raise FenceVerificationError(
            f'{label} cannot be canonicalized.') from error


def _canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(
        _canonical_json_bytes(value,
                              label='fence PostgreSQL evidence')).hexdigest()


def _validate_payload(value: Any) -> dict[str, Any]:
    payload = _exact_keys(value, {
        'schema_version',
        'status',
        'identity',
        'snapshots',
        'manifest',
        'postgres_evidence',
        'postgres_evidence_sha256',
        'generation_intent_sha256',
        'attempt_generation',
        'zero_at',
        'work_cutoff',
        'api_ready_deadline',
        'completed_at',
    },
                          label='fence')
    if type(payload['schema_version']
           ) is not int or payload['schema_version'] != 1:
        raise FenceVerificationError('fence.schema_version must be 1.')
    if type(payload['status']) is not str or payload['status'] != 'complete':
        raise FenceVerificationError('fence.status must be complete.')
    _validate_identity(payload['identity'], label='fence.identity')

    snapshots = _exact_keys(payload['snapshots'], {
        'baseline_source_id',
        'baseline_encrypted_id',
        'quiesced_source_id',
        'quiesced_encrypted_id',
    },
                            label='fence.snapshots')
    snapshot_ids = []
    for field in ('baseline_source_id', 'baseline_encrypted_id',
                  'quiesced_source_id', 'quiesced_encrypted_id'):
        snapshot_ids.append(
            _string(snapshots[field],
                    _EBS_SNAPSHOT_PATTERN,
                    label=f'fence.snapshots.{field}'))
    if len(set(snapshot_ids)) != len(snapshot_ids):
        raise FenceVerificationError(
            'fence snapshot identities must be distinct.')

    evidence = _validate_postgres_evidence(payload['postgres_evidence'])
    evidence_sha256 = _sha256(payload['postgres_evidence_sha256'],
                              label='fence.postgres_evidence_sha256')
    if _canonical_json_sha256(evidence) != evidence_sha256:
        raise FenceVerificationError(
            'fence PostgreSQL evidence SHA-256 does not match.')
    _sha256(payload['generation_intent_sha256'],
            label='fence.generation_intent_sha256')
    attempt_generation = payload['attempt_generation']
    if (type(attempt_generation) is not int or attempt_generation <= 0 or
            attempt_generation > 2**63 - 1):
        raise FenceVerificationError(
            'fence.attempt_generation must be a positive 64-bit integer.')

    manifest = _exact_keys(payload['manifest'],
                           {'sha256', 'entry_count', 'byte_count'},
                           label='fence.manifest')
    _sha256(manifest['sha256'], label='fence.manifest.sha256')
    entry_count = manifest['entry_count']
    byte_count = manifest['byte_count']
    if (type(entry_count) is not int or entry_count <= 0 or
            entry_count > 2**63 - 1):
        raise FenceVerificationError(
            'fence.manifest.entry_count must be a positive 64-bit integer.')
    if (type(byte_count) is not int or byte_count < 0 or
            byte_count > 2**63 - 1):
        raise FenceVerificationError(
            'fence.manifest.byte_count must be a nonnegative 64-bit integer.')

    zero_at = _rfc3339_utc(payload['zero_at'], label='fence.zero_at')
    work_cutoff = _rfc3339_utc(payload['work_cutoff'],
                               label='fence.work_cutoff')
    api_ready_deadline = _rfc3339_utc(payload['api_ready_deadline'],
                                      label='fence.api_ready_deadline')
    completed_at = _rfc3339_utc(payload['completed_at'],
                                label='fence.completed_at')
    if not zero_at < completed_at < work_cutoff:
        raise FenceVerificationError(
            'fence timestamps violate the API-zero work window.')
    if work_cutoff - zero_at != datetime.timedelta(seconds=2700):
        raise FenceVerificationError(
            'fence work cutoff must be exactly 2700 seconds after API zero.')
    if api_ready_deadline - zero_at != datetime.timedelta(seconds=7200):
        raise FenceVerificationError(
            'fence API-ready deadline must be exactly 7200 seconds after API zero.'
        )
    cutover_completed_at = _rfc3339_utc(
        evidence['cutover_completed_at'],
        label='fence.postgres_evidence.cutover_completed_at')
    observed_at = _rfc3339_utc(evidence['observed_at'],
                               label='fence.postgres_evidence.observed_at')
    if not cutover_completed_at <= zero_at < observed_at < completed_at:
        raise FenceVerificationError(
            'fence PostgreSQL evidence timestamps violate cutover chronology.')
    return payload


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise FenceVerificationError('fence JSON contains duplicate keys.')
        result[key] = value
    return result


def _parse_json(raw: bytes, *, label: str) -> Any:
    try:
        text = raw.decode('utf-8', errors='strict')
        return json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                FenceVerificationError(f'{label} contains a non-JSON number.')))
    except UnicodeDecodeError as error:
        raise FenceVerificationError(f'{label} is not UTF-8.') from error
    except json.JSONDecodeError as error:
        raise FenceVerificationError(f'{label} is not valid JSON.') from error


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_mode, value.st_nlink,
            value.st_uid, value.st_gid, value.st_size, value.st_mtime_ns,
            value.st_ctime_ns)


def _validate_fence_stat(value: os.stat_result) -> None:
    if not stat.S_ISREG(value.st_mode):
        raise FenceVerificationError('authority fence must be a regular file.')
    if value.st_mode & 0o222:
        raise FenceVerificationError('authority fence must have no write bits.')
    if value.st_nlink != 1:
        raise FenceVerificationError('authority fence must not be hard linked.')
    if value.st_size <= 0 or value.st_size > MAX_FENCE_BYTES:
        raise FenceVerificationError('authority fence has an invalid size.')


def _read_fence(path: Path) -> bytes:
    if path.name in ('', '.', '..') or path.parent == path:
        raise FenceVerificationError('authority fence path is invalid.')
    if not hasattr(os, 'O_NOFOLLOW') or not hasattr(os, 'O_DIRECTORY'):
        raise FenceVerificationError(
            'no-follow file operations are unavailable.')

    try:
        directory_lstat = os.lstat(path.parent)
    except OSError as error:
        raise FenceVerificationError(
            'authority directory is unavailable.') from error
    if not stat.S_ISDIR(directory_lstat.st_mode):
        raise FenceVerificationError(
            'authority directory must not be a symlink.')

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory_flags |= getattr(os, 'O_CLOEXEC', 0)
    try:
        directory_fd = os.open(path.parent, directory_flags)
    except OSError as error:
        raise FenceVerificationError(
            'authority directory cannot be opened safely.') from error
    try:
        if (_stat_identity(directory_lstat)
                != _stat_identity(os.fstat(directory_fd))):
            raise FenceVerificationError(
                'authority directory changed while it was opened.')
        before: os.stat_result | None = None
        try:
            before = os.stat(path.name,
                             dir_fd=directory_fd,
                             follow_symlinks=False)
        except OSError as error:
            raise FenceVerificationError(
                'authority fence is unavailable.') from error
        if before is None:
            raise FenceVerificationError(
                'authority fence metadata is unavailable.')
        _validate_fence_stat(before)

        # O_NONBLOCK prevents a path-swap to a FIFO/device from hanging before
        # the post-open fstat can reject the non-regular inode.
        file_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        file_flags |= getattr(os, 'O_CLOEXEC', 0)
        file_fd: int | None = None
        try:
            file_fd = os.open(path.name, file_flags, dir_fd=directory_fd)
        except OSError as error:
            raise FenceVerificationError(
                'authority fence cannot be opened safely.') from error
        if file_fd is None:
            raise FenceVerificationError(
                'authority fence descriptor is unavailable.')
        try:
            opened = os.fstat(file_fd)
            _validate_fence_stat(opened)
            if _stat_identity(before) != _stat_identity(opened):
                raise FenceVerificationError(
                    'authority fence changed while it was opened.')
            chunks = []
            total = 0
            while True:
                chunk = os.read(file_fd, min(8192, MAX_FENCE_BYTES + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total > MAX_FENCE_BYTES:
                    raise FenceVerificationError(
                        'authority fence is too large.')
            after = os.fstat(file_fd)
            if (_stat_identity(opened) != _stat_identity(after) or
                    total != after.st_size):
                raise FenceVerificationError(
                    'authority fence changed while it was read.')
            _validate_fence_stat(after)
            published_after: os.stat_result | None = None
            try:
                published_after = os.stat(path.name,
                                          dir_fd=directory_fd,
                                          follow_symlinks=False)
            except OSError as error:
                raise FenceVerificationError(
                    'authority fence changed while it was read.') from error
            if published_after is None:
                raise FenceVerificationError(
                    'authority fence changed while it was read.')
            _validate_fence_stat(published_after)
            if _stat_identity(after) != _stat_identity(published_after):
                raise FenceVerificationError(
                    'authority fence changed while it was read.')
            return b''.join(chunks)
        finally:
            os.close(file_fd)
    finally:
        os.close(directory_fd)


def verify_fence(path: Path, expected_sha256: str,
                 expected_postgres_evidence_sha256: str,
                 expected_identity: Any) -> None:
    """Verifies one fence without granting authority on partial success."""
    _sha256(expected_sha256, label='expected fence SHA-256')
    _sha256(expected_postgres_evidence_sha256,
            label='expected PostgreSQL evidence SHA-256')
    expected = _validate_identity(expected_identity, label='expected identity')
    raw = _read_fence(path)
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise FenceVerificationError('authority fence SHA-256 does not match.')
    payload = _validate_payload(_parse_json(raw, label='authority fence'))
    if raw != _canonical_json_bytes(payload, label='authority fence'):
        raise FenceVerificationError(
            'authority fence bytes are not canonical JSON.')
    if payload['identity'] != expected:
        raise FenceVerificationError('authority fence identity does not match.')
    if (payload['postgres_evidence_sha256']
            != expected_postgres_evidence_sha256):
        raise FenceVerificationError(
            'authority fence PostgreSQL evidence does not match.')


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == '':
        raise FenceVerificationError(f'{name} is required.')
    return value


def main() -> int:
    try:
        expected_identity = _parse_json(
            _required_env(_EXPECTED_IDENTITY_ENV).encode('utf-8'),
            label='expected identity')
        verify_fence(FENCE_PATH, _required_env(_EXPECTED_SHA256_ENV),
                     _required_env(_EXPECTED_POSTGRES_EVIDENCE_SHA256_ENV),
                     expected_identity)
    except FenceVerificationError as error:
        print(f'RWX authority fence verification failed: {error}',
              file=sys.stderr,
              flush=True)
        return 1
    except Exception:  # pylint: disable=broad-exception-caught
        # An unanticipated parser or filesystem failure must never let the
        # workload start or print fence contents into pod logs.
        print('RWX authority fence verification failed unexpectedly.',
              file=sys.stderr,
              flush=True)
        return 1
    print('RWX authority fence verified.', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
