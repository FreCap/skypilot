"""Persistence gateway for incarnation-fenced Skylet tunnel metadata."""

from collections.abc import Callable
import dataclasses
import pickle
import typing
from typing import Any

import sqlalchemy

from sky.adaptors import common as adaptors_common

if typing.TYPE_CHECKING:
    from sky.backends import skylet_transport
else:
    skylet_transport = adaptors_common.LazyImport(
        'sky.backends.skylet_transport')

SkyletSSHTunnelMetadata = tuple[int, int] | tuple[int, int, str]


@dataclasses.dataclass(frozen=True, slots=True)
class ClusterSkyletSSHTunnelSnapshotV1:
    """One same-row cluster incarnation and exact tunnel metadata read."""

    cluster_hash: str | None
    metadata: object | None
    serialized_metadata: bytes | None


# Preserve the historical public and pickle identity exposed by the facade.
ClusterSkyletSSHTunnelSnapshotV1.__module__ = 'sky.global_user_state'

_MALFORMED_SKYLET_SSH_TUNNEL_METADATA = object()


def get_cluster_skylet_ssh_tunnel_snapshot(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    cluster_table: sqlalchemy.Table,
    cluster_name: str,
) -> ClusterSkyletSSHTunnelSnapshotV1 | None:
    """Returns the cluster hash and exact tunnel blob from one row read."""
    engine = engine_getter()
    with session_factory(engine) as session:
        row = session.query(
            cluster_table.c.cluster_hash,
            cluster_table.c.skylet_ssh_tunnel_metadata).filter_by(
                name=cluster_name).first()
    if row is None:
        return None
    serialized_metadata = row.skylet_ssh_tunnel_metadata
    if serialized_metadata is not None:
        serialized_metadata = bytes(serialized_metadata)
        try:
            metadata = pickle.loads(serialized_metadata)
        except Exception:  # pylint: disable=broad-except
            # Keep the exact bytes available for an explicitly fenced repair,
            # but never let automatic tunnel recovery reinterpret corruption
            # as missing metadata.
            metadata = _MALFORMED_SKYLET_SSH_TUNNEL_METADATA
        if metadata is None:
            metadata = _MALFORMED_SKYLET_SSH_TUNNEL_METADATA
    else:
        metadata = None
    return ClusterSkyletSSHTunnelSnapshotV1(
        cluster_hash=row.cluster_hash,
        metadata=metadata,
        serialized_metadata=serialized_metadata,
    )


def compare_and_set_cluster_skylet_ssh_tunnel_metadata(
    engine_getter: Callable[[], sqlalchemy.engine.Engine],
    session_factory: Any,
    cluster_table: sqlalchemy.Table,
    cluster_name: str,
    *,
    observed: ClusterSkyletSSHTunnelSnapshotV1,
    replacement: SkyletSSHTunnelMetadata | None,
) -> 'skylet_transport.TunnelMutationResult':
    """Fenced compare-and-set for one exact tunnel metadata observation."""
    if not isinstance(observed, ClusterSkyletSSHTunnelSnapshotV1):
        raise TypeError('observed must be a tunnel metadata snapshot.')

    if observed.cluster_hash is None:
        return skylet_transport.TunnelMutationResult.UNFENCED_CLUSTER_INCARNATION

    # A null-to-null row recreation has no incarnation fence and has already
    # returned above, before obtaining an engine or SQL session.
    engine = engine_getter()
    with session_factory(engine) as session:
        predicate = (cluster_table.c.skylet_ssh_tunnel_metadata.is_(None)
                     if observed.serialized_metadata is None else
                     cluster_table.c.skylet_ssh_tunnel_metadata
                     == observed.serialized_metadata)
        replacement_blob = (pickle.dumps(replacement)
                            if replacement is not None else None)
        result = session.execute(
            sqlalchemy.update(cluster_table).where(
                cluster_table.c.name == cluster_name,
                cluster_table.c.cluster_hash == observed.cluster_hash,
                predicate,
            ).values(skylet_ssh_tunnel_metadata=replacement_blob))
        session.commit()
    count = result.rowcount
    if count == 1:
        return skylet_transport.TunnelMutationResult.UPDATED
    if count == 0:
        return skylet_transport.TunnelMutationResult.CONFLICT
    raise RuntimeError('Tunnel metadata compare-and-set affected an invalid '
                       f'number of rows: {count}.')
