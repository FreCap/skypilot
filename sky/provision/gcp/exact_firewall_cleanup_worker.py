"""Private process boundary for one exact GCP firewall cleanup."""

import math
import sys

from sky.provision.gcp import instance


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print('Malformed exact GCP firewall worker arguments.', file=sys.stderr)
        return 2
    project_id, cluster_name_on_cloud, budget_text = argv
    try:
        budget_seconds = float(budget_text)
        if not math.isfinite(budget_seconds) or budget_seconds <= 0:
            raise ValueError('budget must be positive and finite')
        # Monotonic readings never cross a process boundary. Construct this
        # process's deadline from the parent's frozen remaining duration.
        deadline_monotonic = instance.time.monotonic() + budget_seconds
        deleted = instance._delete_exact_cluster_ports_firewall_direct(  # pylint: disable=protected-access
            project_id,
            cluster_name_on_cloud,
            deadline_monotonic=deadline_monotonic)
    except BaseException as error:  # pylint: disable=broad-except
        print(f'{type(error).__name__}: {error}', file=sys.stderr, flush=True)
        return 1
    return (
        0 if deleted else instance._EXACT_FIREWALL_ALREADY_ABSENT_RETURN_CODE)  # pylint: disable=protected-access


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
