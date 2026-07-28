"""Shared Serve and managed-job pool CLI log lifecycle."""

import pathlib

import click
import colorama

from sky import exceptions
from sky import jobs as managed_jobs
from sky import serve as serve_lib
from sky import sky_logging
from sky.skylet import constants
from sky.utils import rich_utils
from sky.utils import ux_utils

logger = sky_logging.init_logger('sky.client.cli.command')


def _handle_serve_logs(
        service_name: str,
        follow: bool,
        controller: bool,
        load_balancer: bool,
        replica_ids: tuple[int, ...],
        sync_down: bool,
        tail: int | None,
        pool: bool,  # pylint: disable=redefined-outer-name
):
    noun = 'pool' if pool else 'service'
    capnoun = noun.capitalize()
    repnoun = 'worker' if pool else 'replica'
    if tail is not None:
        if tail < 0:
            raise click.UsageError('--tail must be a non-negative integer.')
        # TODO(arda): We could add ability to tail and follow logs together.
        if follow:
            follow = False
            logger.warning(
                f'{colorama.Fore.YELLOW}'
                '--tail and --follow cannot be used together. '
                f'Changed the mode to --no-follow.{colorama.Style.RESET_ALL}')

    chosen_components: set[serve_lib.ServiceComponent] = set()
    if controller:
        chosen_components.add(serve_lib.ServiceComponent.CONTROLLER)
    if load_balancer:
        assert not pool, 'Load balancer is not supported for pools.'
        chosen_components.add(serve_lib.ServiceComponent.LOAD_BALANCER)
    # replica_ids contains the specific replica IDs provided by the user.
    # If it's not empty, it implies the user wants replica logs.
    if replica_ids:
        chosen_components.add(serve_lib.ServiceComponent.REPLICA)

    if sync_down:
        # For sync-down, multiple targets are allowed.
        # If no specific components/replicas are mentioned, sync all.
        # Note: Multiple replicas or targets can only be specified when
        # using --sync-down.
        targets_to_sync = list(chosen_components)
        if not targets_to_sync and not replica_ids:
            # Default to all components if nothing specific is requested
            targets_to_sync = [
                serve_lib.ServiceComponent.CONTROLLER,
                serve_lib.ServiceComponent.REPLICA,
            ]
            if not pool:
                targets_to_sync.append(serve_lib.ServiceComponent.LOAD_BALANCER)

        timestamp = sky_logging.get_run_timestamp()
        log_dir = (pathlib.Path(constants.SKY_LOGS_DIRECTORY) / noun /
                   f'{service_name}_{timestamp}').expanduser()
        log_dir.mkdir(parents=True, exist_ok=True)

        with rich_utils.client_status(
                ux_utils.spinner_message(f'Downloading {noun} logs...')):
            if pool:
                managed_jobs.pool_sync_down_logs(service_name,
                                                 str(log_dir),
                                                 targets=targets_to_sync,
                                                 worker_ids=list(replica_ids),
                                                 tail=tail)
            else:
                serve_lib.sync_down_logs(service_name,
                                         str(log_dir),
                                         targets=targets_to_sync,
                                         replica_ids=list(replica_ids),
                                         tail=tail)
        style = colorama.Style
        fore = colorama.Fore
        logger.info(f'{fore.CYAN}{capnoun} {service_name} logs: '
                    f'{log_dir}{style.RESET_ALL}')
        return

    # Tailing requires exactly one target.
    num_targets = len(chosen_components)
    # If REPLICA component is chosen, len(replica_ids) must be 1 for tailing.
    if serve_lib.ServiceComponent.REPLICA in chosen_components:
        if len(replica_ids) != 1:
            raise click.UsageError(
                f'Can only tail logs from a single {repnoun} at a time. '
                f'Provide exactly one {repnoun.upper()}_ID or use --sync-down '
                f'to download logs from multiple {repnoun}s.')
        # If replica is chosen and len is 1, num_targets effectively counts it.
        # We need to ensure no other component (controller/LB) is selected.
        if num_targets > 1:
            raise click.UsageError(
                'Can only tail logs from one target at a time (controller, '
                f'load balancer, or a single {repnoun}). Use --sync-down '
                'to download logs from multiple sources.')
    elif num_targets == 0:
        raise click.UsageError(
            'Specify a target to tail: --controller, --load-balancer, or '
            f'a {repnoun.upper()}_ID.')
    elif num_targets > 1:
        raise click.UsageError(
            'Can only tail logs from one target at a time. Use --sync-down '
            'to download logs from multiple sources.')

    # At this point, we have exactly one target for tailing.
    assert len(chosen_components) == 1
    assert len(replica_ids) in [0, 1]
    target_component = chosen_components.pop()
    target_replica_id: int | None = replica_ids[0] if replica_ids else None

    try:
        if pool:
            managed_jobs.pool_tail_logs(service_name,
                                        target=target_component,
                                        worker_id=target_replica_id,
                                        follow=follow,
                                        tail=tail)
        else:
            serve_lib.tail_logs(service_name,
                                target=target_component,
                                replica_id=target_replica_id,
                                follow=follow,
                                tail=tail)
    except exceptions.ClusterNotUpError:
        with ux_utils.print_exception_no_traceback():
            raise
