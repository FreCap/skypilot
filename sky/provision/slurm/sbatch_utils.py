"""Slurm sbatch directive construction."""

from typing import Any

import colorama

from sky import sky_logging
from sky.adaptors import slurm
from sky.provision.slurm import utils as slurm_utils

logger = sky_logging.init_logger(__name__)

# sbatch options that SkyPilot controls and must not be overridden by users.
# These are either set dynamically based on the resource spec, or are required
# for SkyPilot's job lifecycle management.
_SBATCH_PROTECTED_OPTIONS = frozenset({
    'job-name',
    'output',
    'error',
    'nodes',
    'wait-all-nodes',
    'no-requeue',
    'cpus-per-task',
    'mem',
    'gres',
    'partition',
})


def _build_custom_sbatch_directives(sbatch_options: dict[str, Any]) -> str:
    """Build #SBATCH directive lines from user-supplied sbatch_options.

    Args:
        sbatch_options: Dict mapping sbatch option names to values.

    Returns:
        A string of #SBATCH directives, one per line. Protected options
        managed by SkyPilot are skipped with a warning.
    """
    if not sbatch_options:
        return ''

    # Normalize: replace underscores with hyphens (sbatch uses hyphens).
    normalized = {k.replace('_', '-'): v for k, v in sbatch_options.items()}

    # Warn and skip protected options.
    conflicting = set(normalized.keys()) & _SBATCH_PROTECTED_OPTIONS
    if conflicting:
        logger.warning(
            f'{colorama.Fore.YELLOW}Ignoring protected sbatch options '
            f'managed by SkyPilot: {sorted(conflicting)}. Remove them '
            f'from slurm.sbatch_options in ~/.sky/config.yaml.'
            f'{colorama.Style.RESET_ALL}')
        for key in conflicting:
            del normalized[key]

    # Build directive lines.
    lines = []
    for key in sorted(normalized):
        value = normalized[key]
        if value is None or value is False:
            continue
        # Defense in depth: schema validation rejects newlines, but
        # guard here too to prevent script injection.
        str_value = str(value)
        if '\n' in key or '\n' in str_value:
            raise ValueError(
                f'Newline characters are not allowed in sbatch options: '
                f'{key!r}={str_value!r}')
        if key in ('time', 't'):
            slurm_utils.validate_sbatch_time(str_value)
        if value is True:
            lines.append(f'#SBATCH --{key}')
        else:
            lines.append(f'#SBATCH --{key}={value}')
    if not lines:
        return ''
    # Prefix with newline so it slots in after other directives
    # in the provision script f-string.
    return '\n' + '\n'.join(lines)


def _compute_time_directive(sbatch_options: dict[str, Any],
                            partition_info: 'slurm.SlurmPartition',
                            partition: str) -> str:
    """Compute the auto-generated ``#SBATCH --time=...`` directive.

    Priority: user-supplied > partition MaxTime > partition DefaultTime >
    warn-and-omit. The MaxTime-before-DefaultTime ordering preserves
    longstanding behavior (pre-existing code always emitted
    ``--time={MaxTime}`` and ignored ``DefaultTime``). DefaultTime is
    only consulted when MaxTime is UNLIMITED — emitting
    ``--time=UNLIMITED`` is the #9370 footgun (backfill scheduler
    refuses to schedule ahead of maintenance reservations).

    TODO(kevin): consider preferring DefaultTime over MaxTime. Arguments:
    (1) matches Slurm's own default-resolution order; (2) DefaultTime
    is the more intentional admin signal — MaxTime is usually the
    ceiling, DefaultTime is "what a typical job should get";
    (3) friendlier to the backfill scheduler; (4) less surprising for
    admins who explicitly configured DefaultTime.

    Returns the directive line (no trailing newline), or empty string
    when no auto-generated directive should be emitted (user supplied
    their own, or DefaultTime path, or warn path).
    """
    # Match _build_custom_sbatch_directives' emit criteria: None and False
    # are skipped there (the convention for boolean-shaped options like
    # `exclusive: false`), so we treat them the same way here. Otherwise
    # `time: false` would suppress both the user's directive AND the auto
    # fallback, silently bypassing the safety net.
    user_supplied_time = any(
        sbatch_options.get(k) not in (None, False) for k in ('time', 't'))
    if user_supplied_time:
        return ''
    # MaxTime first: preserve pre-existing behavior for partitions where
    # MaxTime is set.
    if partition_info.maxtime is not None:
        max_time = slurm_utils.format_slurm_duration(partition_info.maxtime)
        return f'#SBATCH --time={max_time}'
    # MaxTime is UNLIMITED / NONE. Fall back to DefaultTime (the #9370
    # fix path) so Slurm doesn't see --time=UNLIMITED.
    if partition_info.default_time is not None:
        return ''
    logger.warning(
        f'Partition {partition!r} has no MaxTime or DefaultTime configured. '
        'Submitting without --time may cause the job to hang behind '
        'maintenance reservations. Set slurm.sbatch_options.time in your '
        'task YAML or in ~/.sky/config.yaml.')
    return ''


def _build_sbatch_directives(sbatch_options: dict[str, Any],
                             partition_info: 'slurm.SlurmPartition',
                             partition: str) -> str:
    """Combine auto-generated and user-supplied ``#SBATCH`` directives.

    Returns a string with a leading newline so it slots into the sbatch
    script f-string after the pre-existing directives, or empty string
    when nothing to emit.
    """
    user_block = _build_custom_sbatch_directives(sbatch_options)
    auto_time = _compute_time_directive(sbatch_options, partition_info,
                                        partition)
    if not auto_time:
        return user_block
    # user_block is either '' or '\n#SBATCH ...' (leading \n, no trailing).
    return '\n' + auto_time + user_block
