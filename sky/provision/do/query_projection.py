"""Pure projection of DigitalOcean query results."""

from typing import Any

from sky.utils import status_lib


def project_query_instances(
    instances: dict[str, Any],
    cluster_status: Any,
) -> dict[str, tuple[status_lib.ClusterStatus | None, str | None]]:
    """Translate filtered DigitalOcean instances to SkyPilot statuses."""
    status_map = {
        'new': cluster_status.INIT,
        'archive': cluster_status.INIT,
        'active': cluster_status.UP,
        'off': cluster_status.STOPPED,
    }
    statuses: dict[str, tuple[status_lib.ClusterStatus | None, str | None]] = {}
    for instance_meta in instances.values():
        status = status_map[instance_meta['status']]
        statuses[instance_meta['name']] = (status, None)
    return statuses
