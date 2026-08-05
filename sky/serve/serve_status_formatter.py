"""CLI table formatting for SkyServe status records."""
from typing import Any

import colorama

from sky import backends
from sky.serve import serve_state
from sky.utils import log_utils
from sky.utils import resources_utils

_REPLICA_TRUNC_NUM = 10


def _get_replicas(service_record: dict[str, Any]) -> str:
    ready = service_record.get('ready_replicas')
    total = service_record.get('total_replicas')
    if (isinstance(ready, int) and not isinstance(ready, bool) and
            ready >= 0 and isinstance(total, int) and
            not isinstance(total, bool) and total >= 0):
        return f'{ready}/{total}'
    ready_replica_num, total_replica_num = 0, 0
    for info in service_record['replica_info']:
        if info['status'] == serve_state.ReplicaStatus.READY:
            ready_replica_num += 1
        # TODO(MaoZiming): add a column showing failed replicas number.
        if info['status'] not in serve_state.ReplicaStatus.failed_statuses():
            total_replica_num += 1
    return f'{ready_replica_num}/{total_replica_num}'


def format_service_table(service_records: list[dict[str, Any]], show_all: bool,
                         pool: bool) -> str:
    noun = 'pool' if pool else 'service'
    if not service_records:
        return f'No existing {noun}s.'

    service_columns = [
        'NAME', 'VERSION', 'UPTIME', 'STATUS',
        'REPLICAS' if not pool else 'WORKERS'
    ]
    if not pool:
        service_columns.append('ENDPOINT')
    if show_all:
        service_columns.extend([
            'AUTOSCALING_POLICY', 'LOAD_BALANCING_POLICY', 'REQUESTED_RESOURCES'
        ])
        if pool:
            # Remove the load balancing policy column for pools.
            service_columns.pop(-2)
    service_table = log_utils.create_table(service_columns)

    replica_infos: list[dict[str, Any]] = []
    for record in service_records:
        for replica in record['replica_info']:
            replica['service_name'] = record['name']
            replica_infos.append(replica)

        service_name = record['name']
        version = ','.join(
            str(v) for v in record['active_versions']
        ) if 'active_versions' in record and record['active_versions'] else '-'
        uptime = log_utils.readable_time_duration(record['uptime'],
                                                  absolute=True)
        service_status = record['status']
        status_str = service_status.colored_str()
        replicas = _get_replicas(record)
        endpoint = record['endpoint']
        if endpoint is None:
            endpoint = '-'
        policy = record['policy']
        requested_resources_str = record['requested_resources_str']
        load_balancing_policy = record['load_balancing_policy']

        service_values = [
            service_name,
            version,
            uptime,
            status_str,
            replicas,
        ]
        if not pool:
            service_values.append(endpoint)
        if show_all:
            service_values.extend(
                [policy, load_balancing_policy, requested_resources_str])
            if pool:
                service_values.pop(-2)
        service_table.add_row(service_values)

    replica_table = _format_replica_table(replica_infos, show_all, pool)
    replica_noun = 'Pool Workers' if pool else 'Service Replicas'
    return (f'{service_table}\n'
            f'\n{colorama.Fore.CYAN}{colorama.Style.BRIGHT}'
            f'{replica_noun}{colorama.Style.RESET_ALL}\n'
            f'{replica_table}')


def _format_replica_table(replica_records: list[dict[str, Any]], show_all: bool,
                          pool: bool) -> str:
    noun = 'worker' if pool else 'replica'
    if not replica_records:
        return f'No existing {noun}s.'

    replica_columns = [
        'POOL_NAME' if pool else 'SERVICE_NAME', 'ID', 'VERSION', 'ENDPOINT',
        'LAUNCHED', 'INFRA', 'RESOURCES', 'STATUS'
    ]
    if pool:
        replica_columns.append('USED_BY')
        # Remove the endpoint column for pool workers.
        replica_columns.pop(3)
    replica_table = log_utils.create_table(replica_columns)

    truncate_hint = ''
    if not show_all:
        if len(replica_records) > _REPLICA_TRUNC_NUM:
            # `sky jobs pool status` owns an `--all` flag; `sky serve status`
            # does not, and reaches show_all only through `--verbose`. Naming
            # the wrong one sends an operator staring at a truncated table to
            # a flag that exits with "no such option".
            flag = '--all' if pool else '-v'
            truncate_hint = f'\n... (use {flag} to show all {noun}s)'
        replica_records = replica_records[:_REPLICA_TRUNC_NUM]

    for record in replica_records:
        endpoint = record.get('endpoint', '-')
        service_name = record['service_name']
        replica_id = record['replica_id']
        version = (record['version'] if 'version' in record else '-')
        replica_endpoint = endpoint if endpoint else '-'
        launched_at = log_utils.readable_time_duration(record['launched_at'])
        infra = '-'
        resources_str = '-'
        replica_status = record['status']
        status_str = replica_status.colored_str()
        used_by = record.get('used_by', None)
        if used_by is None:
            used_by_str = '-'
        elif isinstance(used_by, str):
            used_by_str = used_by
        else:
            if len(used_by) > 2:
                used_by_str = (
                    f'{used_by[0]}, {used_by[1]}, +{len(used_by) - 2}'
                    ' more')
            elif len(used_by) == 2:
                used_by_str = f'{used_by[0]}, {used_by[1]}'
            elif len(used_by) == 1:
                used_by_str = str(used_by[0])
            else:
                used_by_str = '-'

        # Prefer pre-computed string fields from the server (new servers
        # ship these alongside or instead of a pickled handle to keep wire
        # payload small). Fall back to computing them locally from
        # ``record['handle']`` for back-compat with old servers.
        infra_pre = record.get('infra')
        if infra_pre is not None:
            infra = infra_pre
        if show_all:
            resources_pre = (record.get('resources_str_full') or
                             record.get('resources_str'))
        else:
            resources_pre = record.get('resources_str')
        if resources_pre is not None:
            resources_str = resources_pre

        if infra_pre is None or resources_pre is None:
            replica_handle: backends.CloudVmRayResourceHandle | None = record.get(
                'handle')
            if (replica_handle is not None and
                    replica_handle.launched_resources is not None):
                if infra_pre is None:
                    infra = (
                        replica_handle.launched_resources.infra.formatted_str())
                if resources_pre is None:
                    simplified = not show_all
                    resources_str_simple, resources_str_full = (
                        resources_utils.get_readable_resources_repr(
                            replica_handle, simplified_only=simplified))
                    if simplified:
                        resources_str = resources_str_simple
                    else:
                        assert resources_str_full is not None
                        resources_str = resources_str_full

        replica_values = [
            service_name,
            replica_id,
            version,
            replica_endpoint,
            launched_at,
            infra,
            resources_str,
            status_str,
        ]
        if pool:
            replica_values.append(used_by_str)
            replica_values.pop(3)
        replica_table.add_row(replica_values)

    return f'{replica_table}{truncate_hint}'
