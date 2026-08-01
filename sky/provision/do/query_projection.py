"""Pure projection of DigitalOcean query results."""

from typing import Any

from sky.utils import status_lib


def project_query_instances(
    instances: dict[str, Any],
) -> dict[str, tuple[status_lib.ClusterStatus | None, str | None]]:
    """Translate filtered DigitalOcean instances to SkyPilot statuses."""
    status_map = {
        'new': status_lib.ClusterStatus.INIT,
        'archive': status_lib.ClusterStatus.INIT,
        'active': status_lib.ClusterStatus.UP,
        'off': status_lib.ClusterStatus.STOPPED,
    }
    statuses: dict[str, tuple[status_lib.ClusterStatus | None, str | None]] = {}
    for instance_meta in instances.values():
        status = status_map[instance_meta['status']]
        statuses[instance_meta['name']] = (status, None)
    return statuses
