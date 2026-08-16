"""Distinct durable handler for generalized non-pool Serve launches.

This handler intentionally reuses the normal request executor and the existing
``sky.execution.launch`` implementation.  Its separate stable registry name is
the mixed-version fence: API011 permits this handler only with a complete
protocol-v2 profile/cohort envelope, and an ordinary-only executor can never
claim it.
"""

from typing import Any

from sky import exceptions
from sky.adaptors import common as adaptors_common
from sky.server.requests import storage as request_storage

NON_POOL_LAUNCH_HANDLER_NAME = 'sky.server.requests.non_pool_launch:launch'

execution = adaptors_common.LazyImport('sky.execution')
ordinary_launch_binding = adaptors_common.LazyImport(
    'sky.serve.ordinary_launch_binding')


def launch(*args: Any, **kwargs: Any) -> Any:
    """Execute one exact generic launch on the existing launch path."""
    claim = request_storage.active_execution_claim()
    if claim is None or claim.worker_instance_id is None:
        raise exceptions.RequestCancelled(
            'Bound non-pool launch has no exact durable execution claim.')

    # ``sky.execution.launch`` owns the common last pre-provider boundary and
    # validates the complete v2 context there.  Validating again here would
    # inspect a transport-specific kwargs shape instead of the LaunchBody.
    return execution.launch(*args, **kwargs)
