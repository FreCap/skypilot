"""Low-latency infrastructure summary for the dashboard."""

from typing import Any

import fastapi

from sky import core
from sky.server import common as server_common
from sky.utils import common_utils
from sky.utils import context
from sky.workspaces import core as workspaces_core

router = fastapi.APIRouter()


@router.get('/infra_summary')
@context.contextual
def get_infra_summary(request: fastapi.Request) -> dict[str, Any]:
    """Return accessible workspace infrastructure without request scheduling.

    The dashboard only needs workspace names and cached enabled
    infrastructure for its first paint. Scheduling the two underlying reads as
    separate short requests adds multiple seconds of process and result-handoff
    overhead before the page can display any rows.
    """
    auth_user = request.state.auth_user
    if auth_user is None:
        raise fastapi.HTTPException(status_code=401,
                                    detail='Not authenticated.')

    common_utils.set_current_user(auth_user)
    server_common.refresh_workspace_state_for_sync_handler()
    workspace_names = sorted(workspaces_core.get_accessible_workspace_names())
    enabled_infrastructure = core.enabled_clouds_batch(workspace_names,
                                                       expand=True)
    return {
        'version': 1,
        'workspaces': [{
            'name': workspace_name,
            'infrastructure': enabled_infrastructure.get(workspace_name, []),
        } for workspace_name in workspace_names],
    }
