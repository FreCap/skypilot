"""Machine-readable full-fleet preflight for deployment automation."""

import contextlib
import json
import logging
import sys
import time

from boltz_reserved_fill_reclaim_policy.policy import (
    BoltzReservedFillReclaimPolicy)


def main() -> int:
    """Print exactly one stable JSON object and return success/failure."""
    previous_logging_threshold = logging.root.manager.disable
    try:
        logging.disable(logging.CRITICAL)
        with contextlib.redirect_stdout(sys.stderr):
            payload = BoltzReservedFillReclaimPolicy().preflight(
                deadline_monotonic=time.monotonic() + 5.0, emit_log=False)
    except Exception:  # pylint: disable=broad-except
        payload = {
            'schema_version': 1,
            'operation': 'preflight',
            'success': False,
            'error_code': 'ATTESTATION_FAILED',
        }
        print(json.dumps(payload, sort_keys=True, separators=(',', ':')))
        return 1
    finally:
        logging.disable(previous_logging_threshold)
    print(
        json.dumps(payload,
                   sort_keys=True,
                   separators=(',', ':'),
                   allow_nan=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
