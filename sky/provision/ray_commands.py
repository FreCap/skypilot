"""Ray runtime command construction for provisioned instances."""

import base64
from collections.abc import Callable
import gzip
import io
import json
import os
from typing import Any

from sky.provision.runtime_recovery import _READ_RAY_PORT_CMD
from sky.skylet import constants
from sky.utils import accelerator_registry
from sky.utils import source_utils

# Increase the limit of the number of open files for the raylet process,
# as the `ulimit` may not take effect at this point, because it requires
# all the sessions to be reloaded. This is a workaround.
RAY_PRLIMIT = (
    'which prlimit && for id in $(pgrep -f raylet/raylet); '
    'do sudo prlimit --nofile=1048576:1048576 --pid=$id || true; done;')

DUMP_RAY_PORTS = (f'{constants.SKY_PYTHON_CMD} -c \'import json, os; '
                  f'runtime_dir = os.path.expanduser(os.environ.get('
                  f'"{constants.SKY_RUNTIME_DIR_ENV_VAR_KEY}", "~")); '
                  f'json.dump({constants.SKY_REMOTE_RAY_PORT_DICT_STR}, '
                  f'open(os.path.join(runtime_dir, '
                  f'"{constants.SKY_REMOTE_RAY_PORT_FILE}"), "w", '
                  'encoding="utf-8"))\';')

HOST_NETWORK_ENV_FILE = '/tmp/sky_host_network_ports.env'
HOST_NETWORK_PROBE_TARGET = '/tmp/sky_host_network_probe.py'


def host_network_probe_b64() -> str:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        'kubernetes', 'host_network_probe.py')
    with open(path, encoding='utf-8') as f:
        source = f.read()
    minified = source_utils.minify_python_source(source)
    # Sanity-check: minified source must still parse.
    compile(minified, path, 'exec')
    compressed_buffer = io.BytesIO()
    with gzip.GzipFile(filename='',
                       mode='wb',
                       fileobj=compressed_buffer,
                       compresslevel=9,
                       mtime=0) as gzip_file:
        gzip_file.write(minified.encode('utf-8'))
    compressed = compressed_buffer.getvalue()
    return base64.b64encode(compressed).decode('ascii')


def host_network_probe_cmd(mode: str, probe_b64: Callable[[], str]) -> str:
    """Builds the hostNetwork-gated Ray and sshd port probe snippet.

    The probe is shipped gzip+base64-inline because the Kubernetes template
    installs stable SkyPilot before the development wheel arrives. Keeping the
    payload on one line also preserves rendered YAML block indentation.
    """
    assert mode in ('head', 'worker'), mode
    return (
        'if [ "${SKYPILOT_HOST_NETWORK:-0}" = "1" ] && '
        '[ -n "${SKYPILOT_RAY_PORTS_CONFIGMAP_NAME:-}" ]; then '
        f'echo \'{probe_b64()}\' | base64 -d | gunzip > '
        f'{HOST_NETWORK_PROBE_TARGET}; '
        f'{constants.SKY_PYTHON_CMD} {HOST_NETWORK_PROBE_TARGET} '
        f'--mode {mode} '
        f'--env-file {HOST_NETWORK_ENV_FILE} '
        '--configmap-name "$SKYPILOT_RAY_PORTS_CONFIGMAP_NAME" '
        '--configmap-namespace '
        '"$SKYPILOT_RAY_PORTS_CONFIGMAP_NAMESPACE" || exit 1; '
        f'set -a; . {HOST_NETWORK_ENV_FILE}; set +a; '
        # Delete-then-append rather than sed-in-place: sshd_config files
        # without an existing Port directive would otherwise keep the
        # default 22 (where the K8s node's own sshd already listens).
        'if [ -n "${SKYPILOT_SSHD_PORT:-}" ]; then '
        'sudo sh -c "'
        'sed -i -E \'/^[[:space:]]*#?[[:space:]]*Port[[:space:]]+/d\' '
        '/etc/ssh/sshd_config && '
        'echo Port ${SKYPILOT_SSHD_PORT} >> /etc/ssh/sshd_config && '
        'service ssh restart"; '
        'fi; '
        'fi; ')


RAY_PORT_COMMAND = (
    f'{_READ_RAY_PORT_CMD};'
    f'{constants.SKY_PYTHON_CMD} -c "from sky.utils import message_utils; '
    'print(message_utils.encode_payload({\'ray_port\': $RAY_PORT}))"')

# Command that calls `ray status` with SkyPilot's Ray port set.
RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND = (
    f'{RAY_PORT_COMMAND}; '
    f'RAY_ADDRESS=127.0.0.1:$RAY_PORT {constants.SKY_RAY_CMD} status')

# Command that waits for the ray status to be initialized. Otherwise, a later
# `sky status -r` may fail due to the ray cluster not being ready.
RAY_HEAD_WAIT_INITIALIZED_COMMAND = (
    'while `RAY_ADDRESS=127.0.0.1:${SKYPILOT_RAY_PORT:-'
    f'{constants.SKY_REMOTE_RAY_PORT}}} '
    f'{constants.SKY_RAY_CMD} status | grep -q "No cluster status."`; do '
    'sleep 0.5; '
    'echo "Waiting ray cluster to be initialized"; '
    'done;')


def ray_gpu_options(custom_resource: str) -> str:
    """Returns GPU options for the ray start command."""
    acc_dict = json.loads(custom_resource)
    assert len(acc_dict) == 1, acc_dict
    acc_name, acc_count = list(acc_dict.items())[0]
    if accelerator_registry.is_schedulable_non_gpu_accelerator(acc_name):
        return ''
    return f' --num-gpus={acc_count}'


# Ray port flags shared by the head and worker start commands. The
# ${VAR:-default} forms take the probed value when the hostNetwork probe ran;
# the ${VAR:+--flag=...} forms vanish when unset.
SHARED_RAY_PORT_FLAGS = (
    '--object-manager-port=${SKYPILOT_RAY_OBJECT_MANAGER_PORT:-8076} '
    '${SKYPILOT_RAY_NODE_MANAGER_PORT:+'
    '--node-manager-port=$SKYPILOT_RAY_NODE_MANAGER_PORT} '
    '${SKYPILOT_RAY_DASHBOARD_AGENT_LISTEN_PORT:+'
    '--dashboard-agent-listen-port='
    '$SKYPILOT_RAY_DASHBOARD_AGENT_LISTEN_PORT} '
    '${SKYPILOT_RAY_RUNTIME_ENV_AGENT_PORT:+'
    '--runtime-env-agent-port=$SKYPILOT_RAY_RUNTIME_ENV_AGENT_PORT} '
    '${SKYPILOT_RAY_METRICS_EXPORT_PORT:+'
    '--metrics-export-port=$SKYPILOT_RAY_METRICS_EXPORT_PORT}')


def ray_head_start_command(custom_resource: str | None,
                           custom_ray_options: dict[str, Any] | None,
                           probe_command: Callable[[str], str],
                           gpu_options: Callable[[str], str]) -> str:
    """Returns the command to start Ray on the head node."""
    ray_options = (
        # --disable-usage-stats in `ray start` saves 10 seconds of idle wait.
        f'--disable-usage-stats '
        f'--port=${{SKYPILOT_RAY_PORT:-{constants.SKY_REMOTE_RAY_PORT}}} '
        f'--dashboard-port=${{SKYPILOT_RAY_DASHBOARD_PORT:-'
        f'{constants.SKY_REMOTE_RAY_DASHBOARD_PORT}}} '
        f'--min-worker-port 11002 '
        f'{SHARED_RAY_PORT_FLAGS} '
        # Head-only: workers don't run the Ray Client server.
        '${SKYPILOT_RAY_CLIENT_SERVER_PORT:+'
        '--ray-client-server-port=$SKYPILOT_RAY_CLIENT_SERVER_PORT} '
        f'--temp-dir={constants.SKY_REMOTE_RAY_TEMPDIR}')
    if custom_resource:
        ray_options += f' --resources=\'{custom_resource}\''
        ray_options += gpu_options(custom_resource)
    if custom_ray_options:
        if 'use_external_ip' in custom_ray_options:
            custom_ray_options.pop('use_external_ip')
        for key, value in custom_ray_options.items():
            ray_options += f' --{key}={value}'

    return (
        probe_command('head') + f'{constants.SKY_RAY_CMD} stop; '
        'RAY_SCHEDULER_EVENTS=0 RAY_DEDUP_LOGS=0 '
        # Increase the warning threshold for fractional-CPU controller actors.
        'RAY_worker_maximum_startup_concurrency=$(( 3 * $(nproc --all) )) '
        f'{constants.SKY_RAY_CMD} start --head {ray_options} || exit 1;' +
        RAY_PRLIMIT + DUMP_RAY_PORTS + RAY_HEAD_WAIT_INITIALIZED_COMMAND)


def ray_worker_start_command(custom_resource: str | None,
                             custom_ray_options: dict[str, Any] | None,
                             no_restart: bool, probe_command: Callable[[str],
                                                                       str],
                             gpu_options: Callable[[str], str]) -> str:
    """Returns the command to start Ray on a worker node."""
    ray_options = ('--address=${SKYPILOT_RAY_HEAD_IP}:${SKYPILOT_RAY_PORT} '
                   f'{SHARED_RAY_PORT_FLAGS}')
    if custom_resource:
        ray_options += f' --resources=\'{custom_resource}\''
        ray_options += gpu_options(custom_resource)
    if custom_ray_options:
        for key, value in custom_ray_options.items():
            ray_options += f' --{key}={value}'

    cmd = (
        'RAY_SCHEDULER_EVENTS=0 RAY_DEDUP_LOGS=0 '
        f'{constants.SKY_RAY_CMD} start --disable-usage-stats {ray_options} || '
        'exit 1;' + RAY_PRLIMIT)
    if no_restart:
        cmd = (
            'ps aux | grep "ray/raylet/raylet" | '
            'grep "gcs-address=${SKYPILOT_RAY_HEAD_IP}:${SKYPILOT_RAY_PORT}" '
            f'|| {{ {cmd} }}')
    else:
        cmd = f'{constants.SKY_RAY_CMD} stop; ' + cmd
    return probe_command('worker') + cmd
