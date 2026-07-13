"""Cluster diagnostic collection for debug dumps."""
import json
import os
import posixpath
import traceback
from typing import Any, Optional

from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky.server.requests import requests as requests_lib
from sky.skylet import constants as skylet_constants
from sky.utils import debug_dump_helpers
from sky.utils import status_lib
from sky.utils import ux_utils

logger = sky_logging.init_logger(__name__)


def _full_traceback() -> str:
    """Capture the full traceback, bypassing any tracebacklimit."""
    with ux_utils.enable_traceback():
        return traceback.format_exc()


# Short connection timeout for the skylet-log-path resolution command. The
# debug dump may run against many clusters, some of which are still
# provisioning or otherwise unreachable; we want such a node to fail fast
# rather than hang the dump waiting to connect.
SKYLET_LOG_RESOLVE_CONNECT_TIMEOUT = 10

# Total wall-clock timeout for the skylet-log rsync. Reachability is already
# gated by the resolve step above (connect_timeout), so this is a backstop
# for a connected-but-stalled transfer (flaky network, oversized rotated
# log): generous enough not to trip on a healthy node + small log, tight
# enough that one bad node can't hang the whole dump.
SKYLET_LOG_RSYNC_TIMEOUT = 60


def resolve_remote_skylet_log_path(runner: Any, cluster_name: str) -> str:
    """Resolve the absolute skylet log path on the head node.

    Skylet writes its log to ``$SKY_RUNTIME_DIR/.sky/skylet.log``, where
    SKY_RUNTIME_DIR defaults to ``$HOME``. The runtime dir can be relocated off
    ``$HOME``, so resolve it through the same remote shell environment used to
    start skylet.
    """
    default_path = posixpath.join('~', skylet_constants.SKYLET_LOG_FILE)
    cmd = (f'echo "${{{skylet_constants.SKY_RUNTIME_DIR_ENV_VAR_KEY}:-$HOME}}/'
           f'{skylet_constants.SKYLET_LOG_FILE}"')
    try:
        returncode, stdout, _ = runner.run(
            cmd,
            require_outputs=True,
            stream_logs=False,
            source_bashrc=True,
            connect_timeout=SKYLET_LOG_RESOLVE_CONNECT_TIMEOUT)
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'Failed to resolve skylet log path on cluster '
                     f'{cluster_name!r}, falling back to {default_path!r}: {e}')
        return default_path
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if returncode != 0 or not lines:
        logger.debug(f'Could not resolve skylet log path on cluster '
                     f'{cluster_name!r} (rc={returncode}), falling back to '
                     f'{default_path!r}')
        return default_path
    return lines[-1]


def collect_cluster_skylet_log(
        cluster_name: str,
        cluster_dir: str,
        handle: Any,
        errors: list[dict[str, str]] | None = None,
        status: Optional['status_lib.ClusterStatus'] = None) -> None:
    """Rsync the head node's skylet log into the cluster dump directory."""
    expected_failure = (status == status_lib.ClusterStatus.INIT)

    def _record_failure(message: str, exc: BaseException) -> None:
        detail = str(exc) or type(exc).__name__
        if expected_failure:
            logger.debug(f'{message} (expected for {status} cluster '
                         f'{cluster_name!r}): {detail}')
            return
        logger.warning(f'{message}: {detail}')
        if errors is not None:
            errors.append({
                'component': 'clusters',
                'resource': f'{cluster_name}/skylet_log',
                'error': detail,
                'traceback': _full_traceback()
            })

    try:
        runners = handle.get_command_runners()
    except Exception as e:  # pylint: disable=broad-except
        _record_failure(
            f'Failed to get command runners for cluster {cluster_name}', e)
        return

    if not runners:
        logger.debug(f'No command runners for cluster {cluster_name!r}; '
                     f'skipping skylet log')
        return

    runner = runners[0]
    remote_path = resolve_remote_skylet_log_path(runner, cluster_name)
    target = os.path.join(cluster_dir, 'skylet.log')
    try:
        runner.rsync(source=remote_path,
                     target=target,
                     up=False,
                     stream_logs=False,
                     timeout=SKYLET_LOG_RSYNC_TIMEOUT)
        logger.debug(f'Collected skylet log for cluster {cluster_name!r}')
    except exceptions.CommandError as e:
        if e.returncode == exceptions.RSYNC_FILE_NOT_FOUND_CODE:
            logger.debug(f'No skylet log found on cluster {cluster_name!r}')
        else:
            _record_failure(
                f'Failed to rsync skylet log for cluster {cluster_name}', e)
    except Exception as e:  # pylint: disable=broad-except
        _record_failure(
            f'Failed to collect skylet log for cluster {cluster_name}', e)


def dump_cluster_info(cluster_names: set[str],
                      dump_dir: str,
                      errors: list[dict[str, str]] | None = None) -> None:
    """Collect cluster state, events, associated requests, and skylet logs."""
    if not cluster_names:
        logger.debug('No clusters to dump')
        return
    logger.debug(f'Entering _dump_cluster_info for '
                 f'{len(cluster_names)} clusters')

    clusters_dir = os.path.join(dump_dir, 'clusters')
    os.makedirs(clusters_dir, exist_ok=True)

    for cluster_name in cluster_names:
        cluster_dir = os.path.join(clusters_dir, cluster_name)
        os.makedirs(cluster_dir, exist_ok=True)

        cluster_record = None
        try:
            cluster_record = global_user_state.get_cluster_from_name(
                cluster_name)
            if cluster_record is not None:
                cluster_info = debug_dump_helpers.serialize_cluster_record(
                    cluster_record)
                cluster_info_path = os.path.join(cluster_dir,
                                                 'cluster_info.json')
                with open(cluster_info_path, 'w', encoding='utf-8') as f:
                    json.dump(cluster_info, f, indent=2, default=str)
                logger.debug(f'Dumped cluster {cluster_name!r} '
                             f'(status={cluster_record.get("status")})')
            else:
                logger.debug(f'Cluster {cluster_name!r} not found in DB')
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Failed to get info for cluster '
                           f'{cluster_name}: {e}')
            if errors is not None:
                errors.append({
                    'component': 'clusters',
                    'resource': cluster_name,
                    'error': str(e),
                    'traceback': _full_traceback()
                })

        try:
            cluster_hash = (cluster_record.get('cluster_hash')
                            if cluster_record else None)
            if cluster_hash:
                for event_data in debug_dump_helpers.get_cluster_events_data(
                        cluster_hash):
                    event_file = f'events_{event_data["event_type"]}.json'
                    event_path = os.path.join(cluster_dir, event_file)
                    with open(event_path, 'w', encoding='utf-8') as f:
                        json.dump(event_data['events'],
                                  f,
                                  indent=2,
                                  default=str)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Failed to get events for cluster '
                           f'{cluster_name}: {e}')
            if errors is not None:
                errors.append({
                    'component': 'clusters',
                    'resource': f'{cluster_name}/events',
                    'error': str(e),
                    'traceback': _full_traceback()
                })

        try:
            requests = requests_lib.get_request_tasks(
                requests_lib.RequestTaskFilter(
                    cluster_names=[cluster_name],
                    fields=['request_id', 'name', 'status', 'created_at']))
            associated_requests = [{
                'request_id': r.request_id,
                'name': r.name,
                'status': r.status.value if r.status else None,
                'created_at': r.created_at,
                'created_at_human': debug_dump_helpers.epoch_to_human(
                    r.created_at),
            } for r in requests]

            assoc_path = os.path.join(cluster_dir, 'associated_requests.json')
            with open(assoc_path, 'w', encoding='utf-8') as f:
                json.dump(associated_requests, f, indent=2, default=str)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Failed to get associated requests for cluster '
                           f'{cluster_name}: {e}')
            if errors is not None:
                errors.append({
                    'component': 'clusters',
                    'resource': f'{cluster_name}/associated_requests',
                    'error': str(e),
                    'traceback': _full_traceback()
                })

        status = cluster_record.get('status') if cluster_record else None
        handle = cluster_record.get('handle') if cluster_record else None

        if status != status_lib.ClusterStatus.STOPPED and handle is not None:
            collect_cluster_skylet_log(cluster_name, cluster_dir, handle,
                                       errors, status)
        else:
            logger.debug(f'Skipping skylet log for cluster {cluster_name!r} '
                         f'(status={status})')

    logger.debug('Exiting _dump_cluster_info')
