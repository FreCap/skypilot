"""Utilities for managing managed job file content.

The helpers in this module fetch job file content (DAG YAML/env files) from the
database-first storage added for managed jobs, transparently falling back to
legacy on-disk paths when needed. Consumers should prefer the string-based
helpers so controllers never have to rely on local disk state.
"""

import os

from sky import sky_logging
from sky import skypilot_config
from sky.jobs import state as managed_job_state

logger = sky_logging.init_logger(__name__)


def get_job_dag_content(job_id: int) -> str | None:
    """Get DAG YAML content for a job from database or disk.

    Args:
        job_id: The job ID

    Returns:
        DAG YAML content as string, or None if not found
    """
    file_info = managed_job_state.get_job_file_contents(job_id)

    # Prefer content stored in the database
    if file_info['dag_yaml_content'] is not None:
        return file_info['dag_yaml_content']

    # Fallback to disk path for backward compatibility
    dag_yaml_path = file_info.get('dag_yaml_path')
    if dag_yaml_path and os.path.exists(dag_yaml_path):
        try:
            with open(dag_yaml_path, encoding='utf-8') as f:
                content = f.read()
                logger.debug('Loaded DAG YAML from disk for job %s: %s', job_id,
                             dag_yaml_path)
                return content
        except (FileNotFoundError, OSError) as e:
            logger.warning(
                f'Failed to read DAG YAML from disk {dag_yaml_path}: {e}')

    logger.warning(f'DAG YAML content not found for job {job_id}')
    return None


def get_job_env_content(job_id: int) -> str | None:
    """Get environment file content for a job from database or disk.

    Args:
        job_id: The job ID

    Returns:
        Environment file content as string, or None if not found
    """
    file_info = managed_job_state.get_job_file_contents(job_id)

    # Prefer content stored in the database
    if file_info['env_file_content'] is not None:
        return file_info['env_file_content']

    # Fallback to disk path for backward compatibility
    env_file_path = file_info.get('env_file_path')
    if env_file_path and os.path.exists(env_file_path):
        try:
            with open(env_file_path, encoding='utf-8') as f:
                content = f.read()
                logger.debug('Loaded environment file from disk for job %s: %s',
                             job_id, env_file_path)
                return content
        except (FileNotFoundError, OSError) as e:
            logger.warning(
                f'Failed to read environment file from disk {env_file_path}: '
                f'{e}')

    # Environment file is optional, so don't warn if not found
    return None


def restore_job_config_file(job_id: int) -> tuple[str, bytes] | None:
    """Restore and return the exact job config snapshot, when configured.

    This reads the config file content from the database and writes it to the
    path specified in the SKYPILOT_CONFIG environment variable. This ensures
    that jobs can run on any controller, even if the original config file
    doesn't exist on disk.

    For backward compatibility with jobs submitted before config persistence was
    implemented, we fall back to using the file if it already exists on disk.

    Args:
        job_id: The job ID

    Returns:
        The unexpanded ``SKYPILOT_CONFIG`` path and exact restored bytes, or
        ``None`` when no config snapshot is configured or available.
    """
    config_path = os.environ.get(skypilot_config.ENV_VAR_SKYPILOT_CONFIG)
    if not config_path:
        # No config file for this job
        return None

    file_info = managed_job_state.get_job_file_contents(job_id)
    config_content = file_info['config_file_content']

    # Expand ~ in config path
    config_path_expanded = os.path.expanduser(config_path)

    if config_content is not None:
        # Config content is in database - restore it
        # Ensure the directory exists
        os.makedirs(os.path.dirname(config_path_expanded), exist_ok=True)
        config_bytes = config_content.encode('utf-8')
        # Write bytes so the content attested by guarded controllers is
        # exactly the content later read by reload_config().
        with open(config_path_expanded, 'wb') as f:
            f.write(config_bytes)
        logger.info(f'Restored config file for job {job_id} to '
                    f'{config_path_expanded} ({len(config_bytes)} bytes)')
        return config_path, config_bytes
    elif os.path.exists(config_path_expanded):
        # Backward compatibility: config not in DB but file exists on disk
        # This can happen for jobs submitted before config persistence
        logger.debug(f'Config file for job {job_id} not in database, but '
                     f'found on disk at {config_path_expanded}')
        with open(config_path_expanded, 'rb') as config_file:
            return config_path, config_file.read()
    else:
        # Config should exist but doesn't - warn about it
        logger.warning(
            f'SKYPILOT_CONFIG is set to {config_path} but config content not '
            f'found in database or on disk for job {job_id}. The job may fail '
            f'if it relies on custom config settings.')
        return None
