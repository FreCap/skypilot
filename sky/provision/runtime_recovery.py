"""Generate and install runtime survivability hooks on provisioned VMs."""
import base64
import os

from sky import sky_logging
from sky.provision import common
from sky.skylet import constants
from sky.utils import command_runner
from sky.utils import common_utils

# Keep the historical logger namespace for unchanged debug output.
logger = sky_logging.init_logger('sky.provision.instance_setup')

# Stdlib only — deliberately doesn't import `sky` so the read still
# works during the K8s bootstrap's brief stable→dev skypilot reinstall
# window. Falls back to SKY_REMOTE_RAY_PORT (not legacy 6379) so the
# health-check poll lands on something Ray could plausibly be on.
_READ_RAY_PORT_PY = (
    'import json, os; '
    'd = os.path.expanduser(os.environ.get('
    f'\'{constants.SKY_RUNTIME_DIR_ENV_VAR_KEY}\', \'~\')); '
    'print(json.load(open(os.path.join(d, '
    f'\'{constants.SKY_REMOTE_RAY_PORT_FILE}\')))[\'ray_port\'])')
# Sets $RAY_PORT to the port of the Ray cluster SkyPilot launched, falling
# back to the default port. Shared by the health-probe payload command below
# and the reboot recovery script's "is ray already running" guard.
_READ_RAY_PORT_CMD = (
    f'RAY_PORT=$({constants.SKY_PYTHON_CMD} -c "{_READ_RAY_PORT_PY}" '
    f'2> /dev/null || echo {constants.SKY_REMOTE_RAY_PORT})')
# Restart skylet when the version does not match to keep the skylet up-to-date.
# We need to activate the python environment to make sure autostop in skylet
# can find the cloud SDK/CLI in PATH.
MAYBE_SKYLET_RESTART_CMD = (f'{constants.ACTIVATE_SKY_REMOTE_PYTHON_ENV}; '
                            f'{constants.SKY_PYTHON_CMD} -m '
                            'sky.skylet.attempt_skylet;')

# Re-arm autostop after a reboot. skylet's AutostopEvent deliberately skips
# enforcement when the persisted config's boot_time differs from the current
# boot (sky/skylet/events.py) — intended semantics for cloud stop/start
# cycles — so a skylet restarted by the reboot recovery hook would otherwise
# never enforce autostop again. Re-persisting the same settings via
# set_autostop() stamps the current boot_time and resets the idle clock
# (correct: a fresh boot counts as activity). No-op when autostop was never
# set, or on a spurious run within the same boot. The snippet deliberately
# contains no quote characters so it can be embedded in a double-quoted
# `python -c` (existing hooks are untouched: set_autostop() only writes the
# hooks list when a legacy `hook` argument is passed).
_REARM_AUTOSTOP_PY = (
    'import psutil; '
    'from sky.skylet import autostop_lib; '
    'c = autostop_lib.get_autostop_config(); '
    '(autostop_lib.set_autostop(c.autostop_idle_minutes, c.backend, '
    'c.wait_for, c.down) '
    'if c.autostop_idle_minutes >= 0 and c.boot_time != psutil.boot_time() '
    'else None)')
MAYBE_REARM_AUTOSTOP_CMD = (f'{constants.ACTIVATE_SKY_REMOTE_PYTHON_ENV}; '
                            f'{constants.SKY_PYTHON_CMD} -c '
                            f'"{_REARM_AUTOSTOP_PY}" || true;')

# ---------------------------------------------------------------------------
# Runtime reboot recovery.
#
# Ray and skylet are started by provisioning as plain processes with no boot
# persistence: after an in-VM reboot sshd comes back (systemd-managed) but the
# SkyPilot runtime does not, leaving the cluster stuck in INIT ("ray cluster
# unhealthy") forever. Worse, autostop/autodown is enforced BY skylet ON the
# VM, so a rebooted machine can never self-terminate.
#
# To recover, provisioning writes a per-node recovery script (the exact ray
# start command that was just executed, plus the idempotent skylet restart on
# the head) and wires it to a systemd oneshot unit, falling back to a cron
# @reboot entry. Installation is strictly best-effort: it must never fail
# provisioning.
# ---------------------------------------------------------------------------

# Env var (on the API server / launching machine) to disable installing the
# reboot recovery boot hook at provision time.
_DISABLE_REBOOT_RECOVERY_ENV_VAR = 'SKYPILOT_DISABLE_REBOOT_RECOVERY'

# Path of the recovery script on the node. $HOME is expanded on the node at
# install time, so the systemd unit gets an absolute ExecStart path.
_RUNTIME_RECOVERY_SCRIPT_PATH = '$HOME/.sky/runtime-recovery.sh'
_RUNTIME_RECOVERY_SERVICE_NAME = 'skypilot-runtime-recovery.service'
_RUNTIME_RECOVERY_SERVICE_PATH = (
    f'/etc/systemd/system/{_RUNTIME_RECOVERY_SERVICE_NAME}')

# The boot hook (systemd/cron) provides a near-empty environment, while
# provisioning ran these commands through the command runner with
# source_bashrc=True, i.e. `/bin/bash --login -i -c 'true && source
# ~/.bashrc && export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && (cmd)'`
# (see CommandRunner._get_command_to_run in sky/utils/command_runner.py).
# The header below re-execs the script through that same login+interactive
# bash invocation, so ~/.bashrc's interactive-shell guard passes and
# PATH-dependent fallbacks (`which ray`, `which python3`, the cloud CLIs
# skylet's autostop teardown needs) resolve identically. `source ~/.bashrc`
# is chained with `;` instead of `&&` so a failing bashrc cannot abort the
# recovery. bash's "no job control"-style warnings without a tty are
# harmless. Any exported env from the login shell (including a user-set
# SKY_RUNTIME_DIR; SkyPilot itself only sets it node-side on Slurm, where
# the hook is never installed) is inherited by the re-exec'd script.
_RUNTIME_RECOVERY_SCRIPT_HEADER = (
    '#!/usr/bin/env bash\n'
    '# Generated by SkyPilot at provision time. Restarts the SkyPilot\n'
    '# runtime (ray and, on the head node, skylet) after an in-VM reboot.\n'
    '# Best-effort: deliberately no `set -e`.\n'
    'if [ -z "${SKYPILOT_RUNTIME_RECOVERY_REEXEC:-}" ]; then\n'
    'export SKYPILOT_RUNTIME_RECOVERY_REEXEC=1\n'
    '# Mirror the provisioning command runner env (source_bashrc=True).\n'
    'exec /bin/bash --login -i -c '
    '"true && source ~/.bashrc > /dev/null 2>&1; '
    'export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore; '
    'exec /bin/bash $0"\n'
    'fi\n'
    'cd "$HOME" || true\n')


def _runtime_recovery_head_script(ray_head_cmd: str) -> str:
    """Returns the reboot recovery script for the head node.

    Order matters: skylet (autostop enforcement — the critical thing to
    bring back) is restarted and re-armed BEFORE ray, because the ray
    head start command ends with an unbounded "wait until ray is
    initialized" loop; a wedged ray recovery must not block autostop
    from resuming.

    The ray start command is guarded by the same `ray status` probe used
    by RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND: the head start command
    begins with `ray stop`, and a spurious execution of this script must
    not take down a healthy ray cluster.
    """
    return (
        _RUNTIME_RECOVERY_SCRIPT_HEADER + f'{MAYBE_SKYLET_RESTART_CMD}\n'
        f'{MAYBE_REARM_AUTOSTOP_CMD}\n'
        f'{_READ_RAY_PORT_CMD}\n'
        f'if ! RAY_ADDRESS=127.0.0.1:$RAY_PORT {constants.SKY_RAY_CMD} status '
        '> /dev/null 2>&1; then\n'
        # Subshell: the start command ends with `|| exit 1`, which must
        # not abort the rest of the script.
        f'( {ray_head_cmd} )\n'
        'fi\n')


def _runtime_recovery_worker_script(ray_worker_cmd: str) -> str:
    """Returns the reboot recovery script for a worker node.

    The join command carries the "is the raylet already connected to the
    head" guard (the no_restart variant of the worker start command), so
    a spurious run is a no-op. The join is retried for a bounded window,
    since after a whole-cluster reboot the head's ray may come back
    after the worker boots.
    """
    return (
        _RUNTIME_RECOVERY_SCRIPT_HEADER + 'for _ in $(seq 1 30); do\n'
        # Subshell: the join command ends with `|| exit 1`, which must
        # not abort the retry loop.
        f'if ( {ray_worker_cmd} ); then\n'
        'exit 0\n'
        'fi\n'
        'sleep 10\n'
        'done\n')


def _runtime_recovery_install_cmd(recovery_script: str) -> str:
    """Returns the command that installs the reboot recovery boot hook.

    The recovery script embeds the ray start command, which is full of
    quotes, backticks and dollar signs, so it is shipped base64-encoded;
    the installer script itself is also shipped base64-encoded and piped
    to bash, so the payload survives the command runners' quoting layers
    untouched.

    The installer prefers a systemd oneshot unit (requires systemd as
    PID 1 and passwordless sudo) and falls back to a cron @reboot entry.
    """
    script_b64 = base64.b64encode(
        recovery_script.encode('utf-8')).decode('ascii')
    # NOTE: $HOME, $(whoami) and $crontab_entry are expanded on the node
    # when the installer runs; {script_b64} and the paths are filled in
    # server-side.
    installer = f"""#!/usr/bin/env bash
# Installs the SkyPilot runtime reboot recovery boot hook. Best-effort.
mkdir -p "$HOME/.sky"
echo {script_b64} | base64 -d > {_RUNTIME_RECOVERY_SCRIPT_PATH}
chmod 755 {_RUNTIME_RECOVERY_SCRIPT_PATH}
if command -v systemctl > /dev/null 2>&1 && \
    [ -d /run/systemd/system ] && \
    sudo -n true > /dev/null 2>&1; then
  sudo tee {_RUNTIME_RECOVERY_SERVICE_PATH} > /dev/null << EOF
[Unit]
Description=SkyPilot runtime recovery (ray + skylet) after reboot
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$(whoami)
ExecStart=/bin/bash {_RUNTIME_RECOVERY_SCRIPT_PATH}
RemainAfterExit=yes
TimeoutStartSec=infinity

[Install]
WantedBy=multi-user.target
EOF
  sudo systemctl daemon-reload
  sudo systemctl enable {_RUNTIME_RECOVERY_SERVICE_NAME}
else
  crontab_entry="@reboot bash {_RUNTIME_RECOVERY_SCRIPT_PATH}"
  (crontab -l 2> /dev/null | grep -F "runtime-recovery.sh" > /dev/null) || \
    {{ {{ crontab -l 2> /dev/null || true; }}; echo "$crontab_entry"; }} | \
    crontab -
fi
"""
    installer_b64 = base64.b64encode(installer.encode('utf-8')).decode('ascii')
    return f'echo {installer_b64} | base64 -d | bash'


def _should_install_reboot_recovery(cluster_info: common.ClusterInfo) -> bool:
    """Whether to install the reboot recovery boot hook on this cluster."""
    if os.environ.get(_DISABLE_REBOOT_RECOVERY_ENV_VAR) == '1':
        return False
    # Kubernetes-based pods have no systemd/reboot semantics: a "rebooted"
    # pod is recreated by the orchestrator and goes through provisioning
    # again. Slurm nodes are shared machines not owned by SkyPilot, so we
    # must not install host-level boot hooks there.
    if cluster_info.provider_name.lower() in ('kubernetes', 'ssh', 'slurm'):
        return False
    # In docker-in-VM mode the runtime lives inside the container and the
    # command runner executes there: neither a systemd unit nor a cron
    # entry written inside the container would run on VM boot.
    if cluster_info.docker_user is not None:
        return False
    return True


# ---------------------------------------------------------------------------
# Skylet watchdog.
#
# Skylet is started by provisioning as a plain nohup'd process (see
# sky/skylet/attempt_skylet.py). If it dies mid-life (OOM, a bug, a stray
# kill) nothing restarts it until the next `sky launch`/`sky start`/reboot —
# and skylet is what enforces autostop/autodown ON the VM, so a cluster with
# a dead skylet silently stops self-terminating and keeps billing. A SIGKILL
# death also leaves no trace in skylet.log.
#
# Provisioning therefore installs a once-a-minute watchdog on the head node:
# a cheap /proc aliveness check that, when skylet is gone, appends a
# timestamped post-mortem (plus the tail of skylet.log) to
# ~/.sky/skylet-watchdog.log and re-runs the exact skylet start command
# provisioning used (including the cluster-identity env exports). It prefers
# a systemd timer (some distros, e.g. Amazon Linux 2023, ship no cron) and
# falls back to a cron entry. Installation is strictly best-effort.
# ---------------------------------------------------------------------------

# Env var (on the API server / launching machine) to disable installing the
# skylet watchdog at provision time.
_DISABLE_SKYLET_WATCHDOG_ENV_VAR = 'SKYPILOT_DISABLE_SKYLET_WATCHDOG'

_SKYLET_WATCHDOG_SCRIPT_PATH = '$HOME/.sky/skylet-watchdog.sh'
_SKYLET_WATCHDOG_SERVICE_NAME = 'skypilot-skylet-watchdog.service'
_SKYLET_WATCHDOG_TIMER_NAME = 'skypilot-skylet-watchdog.timer'

# The watchdog script. The healthy path (skylet alive) must stay cheap — it
# runs every minute — so the aliveness check uses only /proc reads and runs
# BEFORE the login-shell re-exec; the (rare) restart path re-execs through
# the same login+interactive bash used at provisioning (source_bashrc=True)
# so the restarted skylet resolves the cloud CLIs its autostop teardown
# needs, exactly like _RUNTIME_RECOVERY_SCRIPT_HEADER does for the reboot
# hook. The skylet start command (env exports + attempt_skylet) is
# substituted at provision time; attempt_skylet itself is idempotent, so a
# lost race with a concurrent manual restart is harmless. The log is
# truncated to its tail at 1 MiB so a crash-looping skylet cannot fill the
# disk. On legacy nodes without a pid file the check conservatively reports
# not-running and attempt_skylet no-ops; new provisions always write the
# pid file.
_SKYLET_WATCHDOG_SCRIPT_TEMPLATE = """#!/usr/bin/env bash
# Generated by SkyPilot at provision time. Restarts skylet if it died.
# Best-effort: deliberately no `set -e`.
exec 9> "$HOME/.sky/.skylet-watchdog.lock"
flock -n 9 || exit 0
pid=$(cat "$HOME/.sky/skylet_pid" 2>/dev/null)
if [ -n "$pid" ] && [ -d "/proc/$pid" ] && \\
    tr "\\0" " " < "/proc/$pid/cmdline" 2>/dev/null | \\
    grep -q "sky.skylet.skylet"; then
  exit 0
fi
LOG="$HOME/.sky/skylet-watchdog.log"
if [ -f "$LOG" ] && [ "$(wc -c < "$LOG")" -gt 1048576 ]; then
  tail -c 524288 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
fi
if [ -z "${SKYPILOT_SKYLET_WATCHDOG_REEXEC:-}" ]; then
  export SKYPILOT_SKYLET_WATCHDOG_REEXEC=1
  {
    echo "$(date "+%Y-%m-%d %H:%M:%S") skylet not running \
(pid: ${pid:-none}); restarting. Last skylet.log lines:"
    tail -n 20 "$HOME/.sky/skylet.log" 2>/dev/null
  } >> "$LOG"
  # Mirror the provisioning command runner env (source_bashrc=True).
  exec /bin/bash --login -i -c "true && \
source ~/.bashrc > /dev/null 2>&1; \
export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore; \
exec /bin/bash $0" >> "$LOG" 2>&1
fi
( __SKYLET_START_CMD__ ) >> "$LOG" 2>&1
echo "$(date "+%Y-%m-%d %H:%M:%S") skylet restart attempted \
(exit $?)" >> "$LOG"
"""


def _skylet_watchdog_script(skylet_start_cmd: str) -> str:
    """Returns the watchdog script with the start command substituted.

    Plain string substitution (not an f-string/format) so the template's
    shell braces need no escaping and the start command's own quoting
    survives untouched — the script is shipped base64-encoded.
    """
    return _SKYLET_WATCHDOG_SCRIPT_TEMPLATE.replace('__SKYLET_START_CMD__',
                                                    skylet_start_cmd)


def _skylet_watchdog_install_cmd(watchdog_script: str) -> str:
    """Returns the command that installs the skylet watchdog on the head.

    Prefers a systemd timer (requires systemd as PID 1 and passwordless
    sudo; some distros ship no cron daemon) and falls back to a cron
    entry. Both the watchdog script and the installer are shipped
    base64-encoded to survive the command runners' quoting layers, the
    same scheme as _runtime_recovery_install_cmd.
    """
    script_b64 = base64.b64encode(
        watchdog_script.encode('utf-8')).decode('ascii')
    installer = f"""#!/usr/bin/env bash
# Installs the SkyPilot skylet watchdog. Best-effort.
mkdir -p "$HOME/.sky"
echo {script_b64} | base64 -d > {_SKYLET_WATCHDOG_SCRIPT_PATH}
chmod 755 {_SKYLET_WATCHDOG_SCRIPT_PATH}
if command -v systemctl > /dev/null 2>&1 && \
    [ -d /run/systemd/system ] && \
    sudo -n true > /dev/null 2>&1; then
  sudo tee /etc/systemd/system/{_SKYLET_WATCHDOG_SERVICE_NAME} \
      > /dev/null << EOF
[Unit]
Description=SkyPilot skylet watchdog (restart skylet if it died)

[Service]
Type=oneshot
User=$(whoami)
ExecStart=/bin/bash {_SKYLET_WATCHDOG_SCRIPT_PATH}
EOF
  sudo tee /etc/systemd/system/{_SKYLET_WATCHDOG_TIMER_NAME} > /dev/null << EOF
[Unit]
Description=Run the SkyPilot skylet watchdog every minute

[Timer]
OnBootSec=90
OnUnitActiveSec=60
AccuracySec=15

[Install]
WantedBy=timers.target
EOF
  sudo systemctl daemon-reload
  sudo systemctl enable --now {_SKYLET_WATCHDOG_TIMER_NAME}
else
  crontab_entry="* * * * * /bin/bash {_SKYLET_WATCHDOG_SCRIPT_PATH}"
  (crontab -l 2> /dev/null | grep -F "skylet-watchdog.sh" > /dev/null) || \
    {{ {{ crontab -l 2> /dev/null || true; }}; echo "$crontab_entry"; }} | \
    crontab -
fi
"""
    installer_b64 = base64.b64encode(installer.encode('utf-8')).decode('ascii')
    return f'echo {installer_b64} | base64 -d | bash'


def _should_install_skylet_watchdog(cluster_info: common.ClusterInfo) -> bool:
    """Whether to install the skylet watchdog on this cluster's head.

    Same host-level constraints as the reboot recovery hook: no
    systemd/cron hooks on Kubernetes pods (recreated through provisioning
    by the orchestrator), on shared SSH/Slurm machines SkyPilot does not
    own, or inside docker-in-VM containers.
    """
    if os.environ.get(_DISABLE_SKYLET_WATCHDOG_ENV_VAR) == '1':
        return False
    if cluster_info.provider_name.lower() in ('kubernetes', 'ssh', 'slurm'):
        return False
    if cluster_info.docker_user is not None:
        return False
    return True


def _install_skylet_watchdog(runner: command_runner.CommandRunner,
                             skylet_start_cmd: str,
                             cluster_info: common.ClusterInfo,
                             log_path: str) -> None:
    """Best-effort install of the skylet watchdog on the head node.

    Never raises: failing to install the watchdog must not fail
    provisioning (the cluster works without it; skylet just will not be
    auto-restarted if it dies). The caller is wrapped in _auto_retry, and
    an exception here must not re-trigger the skylet start command.
    """
    if not _should_install_skylet_watchdog(cluster_info):
        return
    try:
        returncode, stdout, stderr = runner.run(_skylet_watchdog_install_cmd(
            _skylet_watchdog_script(skylet_start_cmd)),
                                                stream_logs=False,
                                                require_outputs=True,
                                                log_path=log_path)
        if returncode:
            logger.debug('Failed to install the skylet watchdog '
                         f'(exit code {returncode}): '
                         f'===== stdout =====\n{stdout}\n'
                         f'===== stderr ====={stderr}')
    except Exception as e:  # pylint: disable=broad-except
        logger.debug('Failed to install the skylet watchdog: '
                     f'{common_utils.format_exception(e)}')


def _install_runtime_reboot_recovery(runner: command_runner.CommandRunner,
                                     recovery_script: str,
                                     cluster_info: common.ClusterInfo,
                                     log_path: str) -> None:
    """Best-effort install of the reboot recovery boot hook on one node.

    Never raises: failing to install the hook must not fail provisioning
    (the cluster works without it; it just will not self-recover from an
    in-VM reboot). In particular, the callers are wrapped in _auto_retry,
    and an exception here must not re-trigger the ray start commands.
    """
    if not _should_install_reboot_recovery(cluster_info):
        return
    try:
        returncode, stdout, stderr = runner.run(
            _runtime_recovery_install_cmd(recovery_script),
            stream_logs=False,
            require_outputs=True,
            log_path=log_path)
        if returncode:
            logger.debug('Failed to install the runtime reboot recovery hook '
                         f'(exit code {returncode}): '
                         f'===== stdout =====\n{stdout}\n'
                         f'===== stderr ====={stderr}')
    except Exception as e:  # pylint: disable=broad-except
        logger.debug('Failed to install the runtime reboot recovery hook: '
                     f'{common_utils.format_exception(e)}')


# Preserve historical function identities for imports and pickling.
_FACADE_FUNCTIONS = (
    _runtime_recovery_head_script,
    _runtime_recovery_worker_script,
    _runtime_recovery_install_cmd,
    _should_install_reboot_recovery,
    _skylet_watchdog_script,
    _skylet_watchdog_install_cmd,
    _should_install_skylet_watchdog,
    _install_skylet_watchdog,
    _install_runtime_reboot_recovery,
)
for _function in _FACADE_FUNCTIONS:
    _function.__module__ = 'sky.provision.instance_setup'
del _function
