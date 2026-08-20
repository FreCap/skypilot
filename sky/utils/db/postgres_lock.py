"""Lightweight PostgreSQL advisory-lock key derivation."""

import hashlib


def postgres_lock_key(lock_id: str) -> int:
    """Convert a stable string ID to PostgreSQL's positive int8 key space."""
    digest = hashlib.sha256(lock_id.encode('utf-8')).digest()
    return int.from_bytes(digest[:8], 'big') & ((1 << 63) - 1)
