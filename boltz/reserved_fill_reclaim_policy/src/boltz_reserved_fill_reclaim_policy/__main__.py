"""Run the deployment preflight with ``python -m``."""

import contextlib
import sys

with contextlib.redirect_stdout(sys.stderr):
    from boltz_reserved_fill_reclaim_policy import preflight

sys.exit(preflight.main())
