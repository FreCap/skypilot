"""Configuration for Seeweb provisioning."""

from typing import Any


def bootstrap_instances(*args, **_kwargs) -> dict[str, Any]:
    """Bootstrap instances for Seeweb.

    Seeweb doesn't require any special configuration bootstrapping,
    so we just return the config as-is.
    """
    config = args[2]
    return config
