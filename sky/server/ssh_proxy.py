"""Provider-specific WebSocket routes for cluster SSH proxies."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import os
import shlex
import shutil
import struct
import subprocess
import typing

import fastapi

from sky import clouds
from sky import core
from sky import sky_logging
from sky.metrics import utils as metrics_utils
from sky.provision.slurm import utils as slurm_utils
from sky.server import constants as server_constants
from sky.server import websocket_utils
from sky.utils import command_runner
from sky.utils import context_utils
from sky.utils import env_options
from sky.utils import status_lib

if typing.TYPE_CHECKING:
    from sky import backends

router = fastapi.APIRouter()

logger = sky_logging.init_logger(__name__)

# Resolved once at import so `subprocess.Popen(executable=...)` gets an
# absolute path — a required precondition for Python subprocess to route
# through posix_spawn instead of fork_exec.
_KUBECTL_PATH: str | None = shutil.which('kubectl')

SSHMessageType = websocket_utils.SSHMessageType


async def _get_cluster_and_validate(
    cluster_name: str,
    cloud_type: type[clouds.Cloud],
) -> 'backends.CloudVmRayResourceHandle':
    """Fetch cluster status and validate it's UP and correct cloud type."""
    # Run core.status in another thread to avoid blocking the event loop.
    # Use summary_response=True to skip expensive DB columns (owner, metadata,
    # last_creation_yaml) and cluster event queries that are unnecessary for
    # simple cluster validation. This keeps per-call overhead low enough to
    # handle 20+ concurrent WebSocket SSH connections without timeout.
    # TODO(aylei): core.status() will be called with server user, which has
    # permission to all workspaces, this will break workspace isolation.
    # It is ok for now, as users with limited access will not get the ssh config
    # for the clusters in non-accessible workspaces.
    with ThreadPoolExecutor(max_workers=1) as thread_pool_executor:
        cluster_records = await context_utils.to_thread_with_executor(
            thread_pool_executor,
            core.status,
            cluster_name,
            all_users=True,
            summary_response=True)

    if not cluster_records:
        raise fastapi.HTTPException(status_code=404,
                                    detail=f'Cluster {cluster_name} not found')
    cluster_record = cluster_records[0]

    if cluster_record['status'] not in (status_lib.ClusterStatus.INIT,
                                        status_lib.ClusterStatus.UP,
                                        status_lib.ClusterStatus.AUTOSTOPPING):
        raise fastapi.HTTPException(
            status_code=400, detail=f'Cluster {cluster_name} is not running')

    handle: backends.CloudVmRayResourceHandle | None = cluster_record['handle']
    assert handle is not None, 'Cluster handle is None'
    if not isinstance(handle.launched_resources.cloud, cloud_type):
        raise fastapi.HTTPException(
            status_code=400,
            detail=f'Cluster {cluster_name} is not a {str(cloud_type())} '
            'cluster. Use ssh to connect to the cluster instead.')

    return handle


@router.websocket('/kubernetes-pod-ssh-proxy')
async def kubernetes_pod_ssh_proxy(websocket: fastapi.WebSocket,
                                   cluster_name: str,
                                   client_version: int | None = None,
                                   no_redirect: int | None = None) -> None:
    """Proxies SSH to the Kubernetes pod with websocket."""
    await websocket.accept()
    logger.info(f'WebSocket connection accepted for cluster: {cluster_name}')

    timestamps_supported = client_version is not None and client_version > 21
    logger.info(f'Websocket timestamps supported: {timestamps_supported}, \
        client_version = {client_version}')

    # Check if there is a hook wants to redirect this connection.
    if (no_redirect != 1 and websocket_utils.ssh_redirect_hook is not None and
            client_version is not None and client_version
            >= server_constants.MIN_SSH_REDIRECT_PROTOCOL_VERSION):
        try:
            redirect_info = await websocket_utils.ssh_redirect_hook(
                websocket, cluster_name)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'SSH redirect hook failed for {cluster_name}: {e}')
            redirect_info = None
        if redirect_info is not None:
            frame = (struct.pack('!B', SSHMessageType.REDIRECT) +
                     json.dumps(redirect_info).encode())
            await websocket.send_bytes(frame)
            await websocket.close()
            return

    handle = await _get_cluster_and_validate(cluster_name, clouds.Kubernetes)

    # Under hostNetwork the pod's sshd binds a probed port (not 22,
    # which is owned by the K8s node's own sshd). head_ssh_port flows
    # from InstanceInfo.ssh_port through cached_external_ssh_ports.
    head_ssh_port = handle.head_ssh_port or 22
    kubectl_cmd = handle.get_command_runners()[0].port_forward_command(
        port_forward=[(None, head_ssh_port)])
    # Under uvloop, `asyncio.create_subprocess_exec` goes through libuv's
    # `uv_spawn`, which on Linux always uses fork().
    # The forked child runs `PyOS_AfterFork_Child` which tears down inherited
    # Python objects; if any sqlite3 statement is in that set, its
    # destructor calls `sqlite3_free → pthread_mutex_lock` on the sqlite3
    # static allocator mutex. That mutex was held by another parent thread at
    # the fork moment (aiosqlite worker), and the child only has one thread,
    # so no one ever releases it. Child deadlocks before execv, leaks the
    # parent's inherited fds (including every `.<request>.lock` flock), and
    # the parent's event loop stall trips uvicorn's 5s ping-timeout →
    # parent SIGKILL.
    # Run `subprocess.Popen` in a worker thread to bypass uvloop's transport
    # entirely.
    if _KUBECTL_PATH is None or not os.path.isabs(_KUBECTL_PATH):
        raise RuntimeError(
            'kubectl not found on PATH with an absolute path; refusing to '
            'fall back to fork-based spawn which risks the SQLite-mutex '
            'ghost-worker deadlock.')
    argv = [_KUBECTL_PATH] + list(kubectl_cmd[1:])

    def _spawn_sync() -> subprocess.Popen:
        return subprocess.Popen(
            argv,
            executable=_KUBECTL_PATH,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            close_fds=False,
        )

    loop = asyncio.get_running_loop()
    proc = await loop.run_in_executor(None, _spawn_sync)
    logger.info(f'Started kubectl port-forward with command: {kubectl_cmd}')

    # Wrap the sync Popen's stdout pipe as an asyncio StreamReader so the
    # rest of this handler can stay async.
    assert proc.stdout is not None
    stdout_reader = asyncio.StreamReader(loop=loop)
    await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(stdout_reader, loop=loop),
        proc.stdout)

    # Wait for port-forward to be ready and get the local port
    local_port = None
    while True:
        stdout_line = await stdout_reader.readline()
        if stdout_line:
            decoded_line = stdout_line.decode()
            logger.info(f'kubectl port-forward stdout: {decoded_line}')
            if 'Forwarding from 127.0.0.1' in decoded_line:
                port_str = decoded_line.split(':')[-1]
                local_port = int(port_str.replace(' -> ', ':').split(':')[0])
                break
        else:
            await websocket.close()
            return

    logger.info(f'Starting port-forward to local port: {local_port}')
    conn_gauge = metrics_utils.SKY_APISERVER_WEBSOCKET_CONNECTIONS.labels(
        pid=os.getpid())
    ssh_failed = False
    try:
        conn_gauge.inc()
        # Connect to the local port
        reader, writer = await asyncio.open_connection('127.0.0.1', local_port)

        async def write_and_drain(data: bytes) -> None:
            writer.write(data)
            await writer.drain()

        async def close_writer() -> None:
            writer.close()

        ssh_failed = await websocket_utils.run_websocket_proxy(
            websocket,
            read_from_backend=lambda: reader.read(1024),
            write_to_backend=write_and_drain,
            close_backend=close_writer,
            timestamps_supported=timestamps_supported,
        )
    finally:
        conn_gauge.dec()
        reason = ''
        try:
            logger.info('Terminating kubectl port-forward process')
            proc.terminate()
        except ProcessLookupError:
            stdout = await stdout_reader.read()
            logger.error('kubectl port-forward was terminated before the '
                         'ssh websocket connection was closed. Remaining '
                         f'output: {str(stdout)}')
            reason = 'KubectlPortForwardExit'
        else:
            if ssh_failed:
                reason = 'SSHToPodDisconnected'
            else:
                reason = 'ClientClosed'
        metrics_utils.SKY_APISERVER_WEBSOCKET_CLOSED_TOTAL.labels(
            pid=os.getpid(), reason=reason).inc()
        # Reap the kubectl child. `asyncio.create_subprocess_exec` had this
        # handled by asyncio's child watcher; `subprocess.Popen` is outside
        # that watcher so we must wait() ourselves or leave a zombie.
        try:
            await asyncio.wait_for(loop.run_in_executor(None, proc.wait),
                                   timeout=5)
        except asyncio.TimeoutError:
            logger.warning(
                'kubectl did not exit 5s after SIGTERM; sending SIGKILL.')
            proc.kill()
            await loop.run_in_executor(None, proc.wait)


@router.websocket('/slurm-job-ssh-proxy')
async def slurm_job_ssh_proxy(websocket: fastapi.WebSocket,
                              cluster_name: str,
                              worker: int = 0,
                              client_version: int | None = None) -> None:
    """Proxies SSH to the Slurm job via sshd inside srun."""
    await websocket.accept()
    logger.info(f'WebSocket connection accepted for cluster: '
                f'{cluster_name}')

    timestamps_supported = client_version is not None and client_version > 21
    logger.info(f'Websocket timestamps supported: {timestamps_supported}, \
        client_version = {client_version}')

    handle = await _get_cluster_and_validate(cluster_name, clouds.Slurm)

    assert handle.cached_cluster_info is not None, 'Cached cluster info is None'
    provider_config = handle.cached_cluster_info.provider_config
    assert provider_config is not None, 'Provider config is None'
    login_node_ssh_config = provider_config['ssh']
    login_node_host = login_node_ssh_config['hostname']
    login_node_port = int(login_node_ssh_config['port'])
    login_node_user = login_node_ssh_config['user']
    login_node_key = login_node_ssh_config.get('private_key', None)
    login_node_proxy_command = login_node_ssh_config.get('proxycommand', None)
    login_node_proxy_jump = login_node_ssh_config.get('proxyjump', None)

    login_node_runner = command_runner.SSHCommandRunner(
        (login_node_host, login_node_port),
        login_node_user,
        login_node_key,
        ssh_proxy_command=login_node_proxy_command,
        ssh_proxy_jump=login_node_proxy_jump,
    )

    ssh_cmd = login_node_runner.ssh_base_command(
        ssh_mode=command_runner.SshMode.NON_INTERACTIVE,
        port_forward=None,
        connect_timeout=None)

    # There can only be one InstanceInfo per instance_id.
    head_instance = handle.cached_cluster_info.get_head_instance()
    assert head_instance is not None, 'Head instance is None'
    job_id = head_instance.tags['job_id']

    # Instances are ordered: head first, then workers
    instances = handle.cached_cluster_info.instances
    node_hostnames = [inst[0].tags['node'] for inst in instances.values()]
    if worker >= len(node_hostnames):
        raise fastapi.HTTPException(
            status_code=400,
            detail=f'Worker index {worker} out of range. '
            f'Cluster has {len(node_hostnames)} nodes.')
    target_node = node_hostnames[worker]

    # Run sshd inside the Slurm job "container" via srun, such that it inherits
    # the resource constraints of the Slurm job.
    is_container_image = handle.launched_resources.extract_docker_image(
    ) is not None
    ssh_cmd += [
        shlex.quote(
            slurm_utils.srun_sshd_command(
                job_id,
                target_node,
                login_node_user,
                handle.cluster_name_on_cloud,
                is_container_image,
            ))
    ]

    proc = await asyncio.create_subprocess_shell(
        ' '.join(ssh_cmd),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,  # Capture stderr separately for logging
    )
    assert proc.stdin is not None
    assert proc.stdout is not None
    assert proc.stderr is not None

    stdin = proc.stdin
    stdout = proc.stdout
    stderr = proc.stderr

    async def log_stderr():
        while True:
            line = await stderr.readline()
            if not line:
                break
            logger.debug(f'srun stderr: {line.decode().rstrip()}')

    stderr_task = None
    if env_options.Options.SHOW_DEBUG_INFO.get():
        stderr_task = asyncio.create_task(log_stderr())
    conn_gauge = metrics_utils.SKY_APISERVER_WEBSOCKET_CONNECTIONS.labels(
        pid=os.getpid())
    ssh_failed = False
    try:
        conn_gauge.inc()

        async def write_and_drain(data: bytes) -> None:
            stdin.write(data)
            await stdin.drain()

        async def close_stdin() -> None:
            stdin.close()

        ssh_failed = await websocket_utils.run_websocket_proxy(
            websocket,
            read_from_backend=lambda: stdout.read(4096),
            write_to_backend=write_and_drain,
            close_backend=close_stdin,
            timestamps_supported=timestamps_supported,
        )

    finally:
        conn_gauge.dec()
        reason = ''
        try:
            logger.info('Terminating srun process')
            proc.terminate()
        except ProcessLookupError:
            stdout_data = await stdout.read()
            logger.error('srun process was terminated before the '
                         'ssh websocket connection was closed. Remaining '
                         f'output: {str(stdout_data)}')
            reason = 'SrunProcessExit'
        else:
            if ssh_failed:
                reason = 'SSHToSlurmJobDisconnected'
            else:
                reason = 'ClientClosed'

        metrics_utils.SKY_APISERVER_WEBSOCKET_CLOSED_TOTAL.labels(
            pid=os.getpid(), reason=reason).inc()

        # Cancel the stderr logging task if it's still running
        if stderr_task is not None and not stderr_task.done():
            stderr_task.cancel()
            try:
                await stderr_task
            except asyncio.CancelledError:
                pass
