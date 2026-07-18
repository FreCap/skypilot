"""Managed-job debug dump manifest collection."""

from collections.abc import Callable
import concurrent.futures
import contextlib
import json
import os
import pathlib
import re
import traceback
from typing import Any

from sky.utils import ux_utils

# Pattern matching the "From controller <UUID>" line that the controller
# emits at job-claim time (see sky/jobs/controller.py: run_job). Used by
# the debug-dump manifest to scope controller_system/*.log files to the
# controllers that actually ran the requested jobs. HA recovery causes
# the per-job log (opened in append mode at sky/utils/context.py:146) to
# receive a fresh "From controller …" line each time a new controller
# picks up the job — and that line can land arbitrarily far into the
# file after hours of intervening status-check output, so we scan the
# whole file rather than just the head.
_CONTROLLER_UUID_LOG_RE = re.compile(
    r'From controller ([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-'
    r'[0-9a-f]{4}-[0-9a-f]{12})')


def _full_traceback() -> str:
    """Capture the full traceback, bypassing any tracebacklimit."""
    with ux_utils.enable_traceback():
        return traceback.format_exc()


@contextlib.contextmanager
def _catch_to_errors(errors: list[dict[str, str]], component: str,
                     resource: str):
    """Catch exceptions and append to errors list with traceback."""
    try:
        yield
    except Exception as e:  # pylint: disable=broad-except
        errors.append({
            'component': component,
            'resource': resource,
            'error': str(e),
            'traceback': _full_traceback(),
        })


def collect_debug_dump_manifest(
    job_ids: list[int],
    *,
    collect_job_debug_manifest_func: Callable[..., Any],
    collect_cluster_debug_manifest_func: Callable[..., None],
    collect_controller_system_log_paths_func: Callable[..., None],
) -> dict[str, Any]:
    """Collect a debug dump manifest from the controller."""
    inline_data: list[dict[str, str]] = []
    file_paths: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []

    # Collect per-job data in parallel.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(collect_job_debug_manifest_func, job_ids))

    # Merge results and collect cluster info for unique clusters.
    seen_cluster_names: set[str] = set()
    seen_controller_uuids: set[str] = set()
    for job_id, (job_inline, job_files, job_errors, cluster_name,
                 controller_uuids) in zip(job_ids, results):
        inline_data.extend(job_inline)
        file_paths.extend(job_files)
        errors.extend(job_errors)
        seen_controller_uuids.update(controller_uuids)
        if cluster_name and cluster_name not in seen_cluster_names:
            seen_cluster_names.add(cluster_name)
            job_prefix = f'managed_jobs/{job_id}'
            collect_cluster_debug_manifest_func(cluster_name, job_prefix,
                                                inline_data, errors)

    # Collect controller system log paths (shared, not per-job). Scope to
    # the controllers that actually ran the requested jobs — globbing the
    # whole directory would drag in thousands of unrelated controller
    # processes' logs.
    collect_controller_system_log_paths_func(file_paths, errors,
                                             seen_controller_uuids)

    return {
        'inline_data': inline_data,
        'file_paths': file_paths,
        'errors': errors,
    }


def collect_job_debug_manifest(
    job_id: int,
    *,
    jobs_controller_logs_dir: str,
    managed_job_state: Any,
    debug_dump_helpers: Any,
    generate_cluster_name: Callable[[str, int], str],
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]],
           str | None, set[str]]:
    """Collect debug manifest entries for a single managed job.

    Returns:
        (inline_data, file_paths, errors, cluster_name, controller_uuids)
        for this job. ``controller_uuids`` is the set of parent controller
        UUIDs that ran this job (empty if no <jobid>.log exists yet or the
        log doesn't contain the marker — e.g., the job never started).
    """
    inline_data: list[dict[str, str]] = []
    file_paths: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    controller_uuids: set[str] = set()
    job_prefix = f'managed_jobs/{job_id}'

    # 1. Controller log for this job (FILE — needs rsync). Also parse its
    # head for "From controller <UUID>" so the caller can scope the
    # shared controller_system/*.log set to only the controllers that
    # actually ran this job.
    with _catch_to_errors(errors, 'managed_jobs', f'{job_id}/controller_log'):
        controller_logs_dir = pathlib.Path(
            jobs_controller_logs_dir).expanduser()
        log_file = controller_logs_dir / f'{job_id}.log'
        if log_file.is_file():
            file_paths.append({
                'remote_path': str(log_file),
                'relative_path': f'{job_prefix}/{job_id}.log',
            })
            try:
                # Stream the file line by line: HA recovery appends a
                # fresh "From controller <UUID>" line after the prior
                # controller's entire output, which can be many MB into
                # the file. Bounded memory regardless of file size.
                with open(log_file, encoding='utf-8', errors='replace') as f:
                    for line in f:
                        match = _CONTROLLER_UUID_LOG_RE.search(line)
                        if match is not None:
                            controller_uuids.add(match.group(1))
            except OSError:
                # File disappeared / unreadable between is_file() and open;
                # leave controller_uuids unchanged.
                pass

    # 2. Job info from DB (inline — small data).
    with _catch_to_errors(errors, 'managed_jobs', f'{job_id}/job_info'):
        tasks = managed_job_state.get_managed_job_tasks(job_id)
        if tasks:
            for task in tasks:
                user_yaml = task.get('user_yaml')
                if isinstance(user_yaml, str):
                    task['user_yaml'] = debug_dump_helpers.redact_task_yaml(
                        user_yaml)
            inline_data.append({
                'relative_path': f'{job_prefix}/job_info.json',
                'content': json.dumps(tasks, indent=2, default=str),
            })

    # 3. Job events from DB (inline — small data).
    with _catch_to_errors(errors, 'managed_jobs', f'{job_id}/events'):
        events = managed_job_state.get_job_events(job_id, limit=1000)
        if events:
            serializable_events = []
            for event in events:
                serializable_events.append({
                    'spot_job_id': event.get('spot_job_id'),
                    'task_id': event.get('task_id'),
                    'new_status': str(event.get('new_status')),
                    'code': event.get('code'),
                    'reason': event.get('reason'),
                    'timestamp': str(event.get('timestamp')),
                })
            inline_data.append({
                'relative_path': f'{job_prefix}/job_events.json',
                'content': json.dumps(serializable_events,
                                      indent=2,
                                      default=str),
            })

    # 4. Job run logs (FILE — needs rsync).
    with _catch_to_errors(errors, 'managed_jobs', f'{job_id}/run_logs'):
        task_info = managed_job_state.get_all_task_ids_names_statuses_logs(
            job_id)
        for task_idx, (_, _, _, local_log_file, _) in enumerate(task_info):
            if local_log_file and os.path.exists(local_log_file):
                suffix = f'_task{task_idx}' if len(task_info) > 1 else ''
                file_paths.append({
                    'remote_path': str(pathlib.Path(local_log_file)),
                    'relative_path': f'{job_prefix}/run{suffix}.log',
                })

    # 5. Resolve cluster name (cluster info collected in caller for dedup).
    cluster_name = None
    with _catch_to_errors(errors, 'managed_jobs', f'{job_id}/cluster_info'):
        cluster_name, _ = managed_job_state.get_pool_submit_info(job_id)
        if cluster_name is None:
            task_info = managed_job_state.get_all_task_ids_names_statuses_logs(
                job_id)
            if task_info:
                _, task_name, _, _, _ = task_info[0]
                cluster_name = generate_cluster_name(task_name, job_id)

    return inline_data, file_paths, errors, cluster_name, controller_uuids


def collect_cluster_debug_manifest(
    cluster_name: str,
    job_prefix: str,
    inline_data: list[dict[str, str]],
    errors: list[dict[str, str]],
    *,
    global_user_state: Any,
    debug_dump_helpers: Any,
) -> None:
    """Collect cluster info and events for a managed job's cluster."""
    cluster_prefix = f'{job_prefix}/clusters/{cluster_name}'

    with _catch_to_errors(errors, 'managed_jobs',
                          f'{cluster_name}/cluster_info'):
        cluster_record = global_user_state.get_cluster_from_name(cluster_name)
        if cluster_record is None:
            return
        cluster_info = debug_dump_helpers.serialize_cluster_record(
            cluster_record)
        inline_data.append({
            'relative_path': f'{cluster_prefix}/cluster_info.json',
            'content': json.dumps(cluster_info, indent=2, default=str),
        })

        cluster_hash = cluster_record.get('cluster_hash')
        if not cluster_hash:
            return
        for event_data in debug_dump_helpers.get_cluster_events_data(
                cluster_hash):
            inline_data.append({
                'relative_path': f'{cluster_prefix}/'
                                 f'events_{event_data["event_type"]}.json',
                'content': json.dumps(event_data['events'],
                                      indent=2,
                                      default=str),
            })


def collect_controller_system_log_paths(
    file_paths: list[dict[str, str]],
    errors: list[dict[str, str]],
    relevant_uuids: set[str],
    *,
    jobs_controller_logs_dir: str,
) -> None:
    """Collect controller system log file paths (controller_*.log files).

    Only the controllers whose UUIDs appear in ``relevant_uuids`` are
    included. UUIDs are sourced from "From controller <UUID>" lines in
    each requested job's <jobid>.log (see collect_job_debug_manifest).
    If ``relevant_uuids`` is empty (no requested job has a log yet, or
    none of them recorded a controller marker), no controller_system
    files are included — we do not fall back to globbing.
    """
    if not relevant_uuids:
        return
    with _catch_to_errors(errors, 'managed_jobs', 'controller_system/logs'):
        controller_logs_dir = pathlib.Path(
            jobs_controller_logs_dir).expanduser()
        if not controller_logs_dir.exists():
            return
        for uuid_str in relevant_uuids:
            log_file = controller_logs_dir / f'controller_{uuid_str}.log'
            if log_file.is_file():
                file_paths.append({
                    'remote_path': str(log_file),
                    'relative_path': f'managed_jobs/controller_system/'
                                     f'{log_file.name}',
                })
