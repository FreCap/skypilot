"""CLI response gateway for SkyServe services and Managed Jobs pools."""

from typing import Any

import click
import colorama

from sky import exceptions
from sky import serve as serve_lib
from sky.client import sdk
from sky.server import common as server_common
from sky.usage import usage_lib
from sky.utils import common
from sky.utils import common_utils
from sky.utils import controller_utils
from sky.utils import status_lib


def _handle_services_request(
    request_id: server_common.RequestId[list[dict[str, Any]]],
    service_names: list[str] | None,
    show_all: bool,
    show_endpoint: bool,
    pool: bool = False,  # pylint: disable=redefined-outer-name
    is_called_by_user: bool = False
) -> tuple[int | None, str]:
    """Get service statuses.

    Args:
        service_names: If not None, only show the statuses of these services.
        show_all: Show all information of each service.
        show_endpoint: If True, only show the endpoint of the service.
        pool: If True, the request is for a pool. Otherwise for a service.
        is_called_by_user: If this function is called by user directly, or an
            internal call.

    Returns:
        A tuple of (num_services, msg). If num_services is None, it means there
        is an error when querying the services. In this case, msg contains the
        error message. Otherwise, msg contains the formatted service table.
    """
    noun = 'pool' if pool else 'service'
    num_services = None
    try:
        if not is_called_by_user:
            usage_lib.messages.usage.set_internal()
        service_records = sdk.get(request_id)
        num_services = len(service_records)
    except exceptions.ClusterNotUpError as e:
        controller_status = e.cluster_status
        msg = str(e)
        if controller_status is None:
            msg += (f' (See: {colorama.Style.BRIGHT}sky serve -h'
                    f'{colorama.Style.RESET_ALL})')
    except RuntimeError as e:
        msg = ''
        try:
            # Check the controller status again, as the RuntimeError is likely
            # due to the controller being autostopped when querying the
            # services.
            # Since we are client-side, we may not know the exact name of the
            # controller, so use the prefix with a wildcard.
            # Query status of the controller cluster.
            # Probe the controller matching the request: pools run on the
            # JOBS controller, services on the serve controller.
            controller_prefix = (common.JOB_CONTROLLER_PREFIX if pool else
                                 common.SKY_SERVE_CONTROLLER_PREFIX)
            records = sdk.get(
                sdk.status(cluster_names=[controller_prefix + '*'],
                           all_users=True))
            # Only degrade to the "no live services/pools" hint when a
            # controller cluster record EXISTS and is STOPPED (autostopped
            # controller — the case this fallback was written for). `not
            # records` must NOT take this path: in consolidation mode there
            # is never a controller cluster, so an absent record is the
            # steady HEALTHY state — and reaching this handler at all means
            # the status request itself failed (e.g. an ingress 504 on a
            # slow response). Rendering that as a normal-looking empty table
            # silently misreports a live fleet as nonexistent (observed live
            # against a 250-replica service); the connection error below is
            # the truthful output. A truly never-launched controller
            # surfaces as ClusterNotUpError, which is handled separately
            # above.
            if (records and
                    records[0]['status'] == status_lib.ClusterStatus.STOPPED):
                controller_type = (
                    controller_utils.Controllers.JOBS_CONTROLLER if pool else
                    controller_utils.Controllers.SKY_SERVE_CONTROLLER)
                msg = controller_type.value.default_hint_if_non_existent
        except Exception:  # pylint: disable=broad-except
            # This is to an best effort to find the latest controller status to
            # print more helpful message, so we can ignore any exception to
            # print the original error.
            pass
        if not msg:
            # This is an actual error (connection issues), not a normal state.
            # Format the error message and raise a new exception.
            # Use 'from None' to suppress the exception chain and only show
            # the formatted message.
            error_msg = (
                f'Failed to fetch {noun} statuses due to connection issues. '
                'Please try again later. Details: '
                f'{common_utils.format_exception(e, use_bracket=True)}')
            raise RuntimeError(error_msg) from None
    else:
        if show_endpoint:
            if len(service_records) != 1:
                plural = 's' if len(service_records) > 1 else ''
                service_num = (str(len(service_records))
                               if service_records else 'No')
                raise click.UsageError(
                    f'{service_num} service{plural} found. Please specify '
                    'an existing service to show its endpoint. Usage: '
                    'sky serve status --endpoint <service-name>')
            endpoint = service_records[0]['endpoint']
            msg = '-' if endpoint is None else endpoint
        else:
            msg = serve_lib.format_service_table(service_records, show_all,
                                                 pool)
            service_not_found_msg = ''
            if service_names is not None:
                for service_name in service_names:
                    if not any(service_name == record['name']
                               for record in service_records):
                        service_not_found_msg += (
                            f'\n{noun.capitalize()} '
                            f'{service_name!r} not found.')
            if service_not_found_msg:
                msg += f'\n{service_not_found_msg}'
    return num_services, msg
