"""Tests for the embedded RWX authority-fence verifier."""

from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any
import unittest
from unittest import mock

import verify_rwx_authority_fence as verifier


def _identity() -> dict[str, Any]:
    return {
        'namespace': 'skypilot',
        'release_name': 'skypilot',
        'source': {
            'pvc_namespace': 'skypilot',
            'pvc_name': 'skypilot-state',
            'pvc_uid': '11111111-1111-1111-1111-111111111111',
            'pv_name': 'pvc-11111111-1111-1111-1111-111111111111',
            'pv_uid': '22222222-2222-2222-2222-222222222222',
            'ebs_volume_id': 'vol-11111111111111111',
        },
        'target': {
            'state_pvc_namespace': 'skypilot',
            'state_claim_name': 'skypilot-state-rwx',
            'filesystem_id': 'fs-11111111111111111',
            'state_access_point_id': 'fsap-11111111111111111',
            'state_pv_name': 'skypilot-state-rwx-pv',
            'state_pv_uid': '33333333-3333-3333-3333-333333333333',
            'state_pvc_uid': '44444444-4444-4444-4444-444444444444',
            'authority_claim_name': 'skypilot-state-authority',
            'authority_pvc_namespace': 'skypilot',
            'authority_access_point_id': 'fsap-22222222222222222',
            'authority_pv_name': 'skypilot-state-authority-pv',
            'authority_pv_uid': '55555555-5555-5555-5555-555555555555',
            'authority_pvc_uid': '66666666-6666-6666-6666-666666666666',
        },
    }


def _postgres_evidence() -> dict[str, Any]:
    return {
        'schema_version': 1,
        'metadata_key': 'sqlite-to-postgres-cutover.v1',
        'cutover_marker_sha256': 'b' * 64,
        'cutover_format_version': 1,
        'cutover_completed_at': '2026-07-01T10:00:00Z',
        'cutover_request_count': 40,
        'cutover_queue_count': 2,
        'cutover_logical_sha256': 'c' * 64,
        'observed_at': '2026-08-08T12:30:00.123456Z',
        'database_schema_revision': '039_request_store',
        'current_request_count': 42,
        'current_queue_count': 0,
        'current_nonterminal_count': 0,
        'current_claimed_count': 0,
        'current_logical_sha256': 'd' * 64,
    }


def _canonical_sha256(value: Any) -> str:
    raw = json.dumps(value,
                     allow_nan=False,
                     ensure_ascii=True,
                     separators=(',', ':'),
                     sort_keys=True).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


def _postgres_evidence_sha256() -> str:
    return _canonical_sha256(_postgres_evidence())


def _payload() -> dict[str, Any]:
    return {
        'schema_version': 1,
        'status': 'complete',
        'identity': _identity(),
        'snapshots': {
            'baseline_source_id': 'snap-11111111111111111',
            'baseline_encrypted_id': 'snap-22222222222222222',
            'quiesced_source_id': 'snap-33333333333333333',
            'quiesced_encrypted_id': 'snap-44444444444444444',
        },
        'manifest': {
            'sha256': 'a' * 64,
            'entry_count': 42,
            'byte_count': 1024,
        },
        'postgres_evidence': _postgres_evidence(),
        'postgres_evidence_sha256': _postgres_evidence_sha256(),
        'generation_intent_sha256': 'e' * 64,
        'attempt_generation': 1,
        'zero_at': '2026-08-08T12:00:00Z',
        'work_cutoff': '2026-08-08T12:45:00Z',
        'api_ready_deadline': '2026-08-08T14:00:00Z',
        'completed_at': '2026-08-08T12:34:56.123456Z',
    }


def _encode(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, separators=(',', ':'),
                      sort_keys=True).encode('utf-8')


class VerifyRwxAuthorityFenceTest(unittest.TestCase):

    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        self.root = Path(self._temporary.name)
        self.fence = self.root / 'fence.json'

    def _write(self, raw: bytes, *, mode: int = 0o444) -> str:
        if self.fence.exists():
            self.fence.chmod(0o600)
        self.fence.write_bytes(raw)
        self.fence.chmod(mode)
        return hashlib.sha256(raw).hexdigest()

    def test_accepts_exact_digest_sealed_fence(self) -> None:
        digest = self._write(_encode(_payload()))

        verifier.verify_fence(self.fence, digest, _postgres_evidence_sha256(),
                              _identity())

    def test_rejects_symlink_even_when_target_bytes_match(self) -> None:
        raw = _encode(_payload())
        target = self.root / 'real-fence.json'
        target.write_bytes(raw)
        target.chmod(0o444)
        self.fence.symlink_to(target)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'regular file'):
            verifier.verify_fence(self.fence,
                                  hashlib.sha256(raw).hexdigest(),
                                  _postgres_evidence_sha256(), _identity())

    def test_rejects_any_write_bit(self) -> None:
        raw = _encode(_payload())
        digest = self._write(raw, mode=0o644)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'no write bits'):
            verifier.verify_fence(self.fence, digest,
                                  _postgres_evidence_sha256(), _identity())

    def test_rejects_hard_link(self) -> None:
        raw = _encode(_payload())
        digest = self._write(raw)
        (self.root / 'second-name.json').hardlink_to(self.fence)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'hard linked'):
            verifier.verify_fence(self.fence, digest,
                                  _postgres_evidence_sha256(), _identity())

    def test_rejects_exact_byte_digest_mismatch(self) -> None:
        self._write(_encode(_payload()))

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'SHA-256 does not match'):
            verifier.verify_fence(self.fence, 'f' * 64,
                                  _postgres_evidence_sha256(), _identity())

    def test_rejects_well_formed_fence_for_another_bound_identity(self) -> None:
        target_updates = {
            'filesystem_id': 'fs-77777777777777777',
            'state_claim_name': 'other-state-claim',
            'state_access_point_id': 'fsap-77777777777777777',
            'state_pv_name': 'other-state-pv',
            'state_pv_uid': '77777777-7777-7777-7777-777777777777',
            'state_pvc_uid': '88888888-8888-8888-8888-888888888888',
            'authority_claim_name': 'other-authority-claim',
            'authority_access_point_id': 'fsap-88888888888888888',
            'authority_pv_name': 'other-authority-pv',
            'authority_pv_uid': '99999999-9999-9999-9999-999999999999',
            'authority_pvc_uid': 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
        }
        for field, value in target_updates.items():
            with self.subTest(field=field):
                payload = _payload()
                identity = copy.deepcopy(_identity())
                identity['target'][field] = value
                payload['identity'] = identity
                raw = _encode(payload)
                digest = self._write(raw)

                with self.assertRaisesRegex(verifier.FenceVerificationError,
                                            'identity does not match'):
                    verifier.verify_fence(self.fence, digest,
                                          _postgres_evidence_sha256(),
                                          _identity())

    def test_rejects_concurrent_path_replacement(self) -> None:
        raw = _encode(_payload())
        digest = self._write(raw)
        replacement = self.root / 'replacement.json'
        replacement.write_bytes(raw)
        replacement.chmod(0o444)
        real_read = os.read
        replaced = False

        def _read_then_replace(file_descriptor: int, count: int) -> bytes:
            nonlocal replaced
            chunk = real_read(file_descriptor, count)
            if not replaced:
                replacement.replace(self.fence)
                replaced = True
            return chunk

        with mock.patch.object(os, 'read', side_effect=_read_then_replace):
            with self.assertRaisesRegex(verifier.FenceVerificationError,
                                        'changed while it was read'):
                verifier.verify_fence(self.fence, digest,
                                      _postgres_evidence_sha256(), _identity())

    def test_fifo_path_swap_cannot_block_before_fstat(self) -> None:
        raw = _encode(_payload())
        digest = self._write(raw)
        fifo = self.root / 'replacement-fifo'
        os.mkfifo(fifo, mode=0o444)
        real_open = os.open
        swapped = False

        def _open_after_fifo_swap(path: Any,
                                  flags: int,
                                  mode: int = 0o777,
                                  *,
                                  dir_fd: int | None = None) -> int:
            nonlocal swapped
            if path == self.fence.name and dir_fd is not None and not swapped:
                fifo.replace(self.fence)
                swapped = True
            return real_open(path, flags, mode, dir_fd=dir_fd)

        with mock.patch.object(os, 'open', side_effect=_open_after_fifo_swap):
            with self.assertRaisesRegex(verifier.FenceVerificationError,
                                        'regular file'):
                verifier.verify_fence(self.fence, digest,
                                      _postgres_evidence_sha256(), _identity())

    def test_rejects_unknown_schema_field(self) -> None:
        payload = _payload()
        payload['unreviewed_extension'] = True
        raw = _encode(payload)
        digest = self._write(raw)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'invalid field set'):
            verifier.verify_fence(self.fence, digest,
                                  _postgres_evidence_sha256(), _identity())

    def test_rejects_noncanonical_fence_bytes(self) -> None:
        raw = _encode(_payload()) + b'\n'
        digest = self._write(raw)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'not canonical JSON'):
            verifier.verify_fence(self.fence, digest,
                                  _postgres_evidence_sha256(), _identity())

    def test_rejects_duplicate_json_keys(self) -> None:
        raw = _encode(_payload()).replace(
            b'"completed_at":',
            b'"completed_at":"2026-08-08T00:00:00Z","completed_at":', 1)
        digest = self._write(raw)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'duplicate keys'):
            verifier.verify_fence(self.fence, digest,
                                  _postgres_evidence_sha256(), _identity())

    def test_rejects_reused_state_authority_access_point(self) -> None:
        payload = _payload()
        identity = payload['identity']
        identity['target'][
            'authority_access_point_id'] = 'fsap-11111111111111111'
        raw = _encode(payload)
        digest = self._write(raw)

        with self.assertRaisesRegex(
                verifier.FenceVerificationError,
                'distinct state and authority access points'):
            verifier.verify_fence(self.fence, digest,
                                  _postgres_evidence_sha256(), _identity())

    def test_rejects_postgres_evidence_digest_mismatch(self) -> None:
        payload = _payload()
        payload['postgres_evidence_sha256'] = 'f' * 64
        raw = _encode(payload)
        digest = self._write(raw)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'evidence SHA-256 does not match'):
            verifier.verify_fence(self.fence, digest,
                                  _postgres_evidence_sha256(), _identity())

    def test_rejects_postgres_evidence_not_accepted_by_operator(self) -> None:
        raw = _encode(_payload())
        digest = self._write(raw)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'PostgreSQL evidence does not match'):
            verifier.verify_fence(self.fence, digest, 'f' * 64, _identity())

    def test_rejects_post_cutover_request_count_regression(self) -> None:
        payload = _payload()
        evidence = payload['postgres_evidence']
        evidence['current_request_count'] = 39
        payload['postgres_evidence_sha256'] = _canonical_sha256(evidence)
        raw = _encode(payload)
        digest = self._write(raw)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'request count regressed'):
            verifier.verify_fence(self.fence, digest,
                                  payload['postgres_evidence_sha256'],
                                  _identity())

    def test_rejects_historical_queue_count_above_request_count(self) -> None:
        payload = _payload()
        evidence = payload['postgres_evidence']
        evidence['cutover_queue_count'] = 41
        payload['postgres_evidence_sha256'] = _canonical_sha256(evidence)
        raw = _encode(payload)
        digest = self._write(raw)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'historical queue count'):
            verifier.verify_fence(self.fence, digest,
                                  payload['postgres_evidence_sha256'],
                                  _identity())

    def test_rejects_postgres_evidence_outside_cutover_window(self) -> None:
        for field, invalid_value in (
            ('cutover_completed_at', '2026-08-08T12:00:01Z'),
            ('observed_at', '2026-08-08T12:00:00Z'),
            ('observed_at', '2026-08-08T12:34:56.123456Z'),
        ):
            with self.subTest(field=field, invalid_value=invalid_value):
                payload = _payload()
                evidence = payload['postgres_evidence']
                evidence[field] = invalid_value
                payload['postgres_evidence_sha256'] = _canonical_sha256(
                    evidence)
                raw = _encode(payload)
                digest = self._write(raw)

                with self.assertRaisesRegex(verifier.FenceVerificationError,
                                            'cutover chronology'):
                    verifier.verify_fence(self.fence, digest,
                                          payload['postgres_evidence_sha256'],
                                          _identity())

    def test_rejects_pvc_namespace_outside_release_namespace(self) -> None:
        payload = _payload()
        payload['identity']['target'][
            'authority_pvc_namespace'] = 'another-namespace'
        raw = _encode(payload)
        digest = self._write(raw)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'PVC namespaces'):
            verifier.verify_fence(self.fence, digest,
                                  _postgres_evidence_sha256(),
                                  payload['identity'])

    def test_rejects_invalid_kubernetes_dns_subdomain(self) -> None:
        payload = _payload()
        payload['identity']['target']['state_claim_name'] = 'state..claim'
        raw = _encode(payload)
        digest = self._write(raw)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'invalid format'):
            verifier.verify_fence(self.fence, digest,
                                  _postgres_evidence_sha256(),
                                  payload['identity'])

    def test_rejects_nonliteral_zero_current_queue_count(self) -> None:
        for invalid_value in (False, 1):
            with self.subTest(invalid_value=invalid_value):
                payload = _payload()
                evidence = payload['postgres_evidence']
                evidence['current_queue_count'] = invalid_value
                payload['postgres_evidence_sha256'] = _canonical_sha256(
                    evidence)
                raw = _encode(payload)
                digest = self._write(raw)

                with self.assertRaisesRegex(verifier.FenceVerificationError,
                                            'must be integer zero'):
                    verifier.verify_fence(self.fence, digest,
                                          payload['postgres_evidence_sha256'],
                                          _identity())

    def test_rejects_unexpected_postgres_evidence_field(self) -> None:
        payload = _payload()
        evidence = payload['postgres_evidence']
        evidence['source_path'] = '/must/not/appear'
        payload['postgres_evidence_sha256'] = _canonical_sha256(evidence)
        raw = _encode(payload)
        digest = self._write(raw)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'invalid field set'):
            verifier.verify_fence(self.fence, digest,
                                  payload['postgres_evidence_sha256'],
                                  _identity())

    def test_rejects_unbounded_work_window(self) -> None:
        payload = _payload()
        payload['work_cutoff'] = '2026-08-08T12:46:00Z'
        raw = _encode(payload)
        digest = self._write(raw)

        with self.assertRaisesRegex(verifier.FenceVerificationError,
                                    'exactly 2700 seconds'):
            verifier.verify_fence(self.fence, digest,
                                  _postgres_evidence_sha256(), _identity())


if __name__ == '__main__':
    unittest.main()
