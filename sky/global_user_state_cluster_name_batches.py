"""Helpers for bounded unique cluster-name batch queries."""


def get_unique_cluster_name_batches(cluster_names: list[str],
                                    chunk_size: int) -> list[list[str]]:
    """Return cluster-name batches after preserving first-seen uniqueness."""
    if chunk_size < 1:
        raise ValueError('chunk_size must be positive.')
    unique_cluster_names = list(dict.fromkeys(cluster_names))
    return [
        unique_cluster_names[offset:offset + chunk_size]
        for offset in range(0, len(unique_cluster_names), chunk_size)
    ]
