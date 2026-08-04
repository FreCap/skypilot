"""Read-only lifecycle action store foundation."""

from sky.lifecycle_actions.state import FoundationSnapshot
from sky.lifecycle_actions.state import initialize_and_verify
from sky.lifecycle_actions.state import OwnershipScopeSnapshot
from sky.lifecycle_actions.state import read_foundation
from sky.lifecycle_actions.state import StoreIdentitySnapshot

__all__ = [
    'FoundationSnapshot',
    'OwnershipScopeSnapshot',
    'StoreIdentitySnapshot',
    'initialize_and_verify',
    'read_foundation',
]
