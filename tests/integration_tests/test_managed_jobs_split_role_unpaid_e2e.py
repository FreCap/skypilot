"""Unpaid process-level E2E for managed-jobs split-role recovery."""
# pylint: disable=redefined-outer-name

import contextlib
import dataclasses
import fcntl
import json
import os
import pathlib
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import typing
import uuid

import pytest
import requests
import sqlalchemy

from sky.server import constants as server_constants

try:
    from testcontainers import postgres as testcontainers_postgres
except ImportError:
    testcontainers_postgres = None

pytestmark = pytest.mark.unpaid_e2e

_REQUIRE_TEST = os.environ.get(
    'SKYPILOT_REQUIRE_MANAGED_JOBS_UNPAID_E2E') == '1'
_TERMINAL_REQUEST_STATUSES = ('SUCCEEDED', 'FAILED', 'CANCELLED')
_C1_ID = '11111111-1111-4111-8111-111111111111'
_C2_ID = '22222222-2222-4222-8222-222222222222'
_C3_ID = '33333333-3333-4333-8333-333333333333'
_API_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
_EXECUTOR_ID = 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee'


def _fail_or_skip(message: str) -> typing.NoReturn:
    if _REQUIRE_TEST:
        pytest.fail(message)
    pytest.skip(message)


@pytest.fixture(scope='module')
def postgres_url():
    configured_url = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
    container = None
    admin_engine = None
    database = None
    quoted_database = None
    try:
        if configured_url is None:
            if shutil.which('docker') is None:
                _fail_or_skip('Docker is unavailable and '
                              'SKYPILOT_TEST_POSTGRES_URL is unset.')
            if testcontainers_postgres is None:
                _fail_or_skip('testcontainers[postgres] is unavailable.')
            try:
                assert testcontainers_postgres is not None
                container = testcontainers_postgres.PostgresContainer(
                    'postgres:16')
                container.start()
                configured_url = container.get_connection_url()
            except Exception as e:  # pylint: disable=broad-except
                _fail_or_skip(f'Could not start the PostgreSQL container: {e}')

        assert configured_url is not None
        admin_engine = sqlalchemy.create_engine(configured_url,
                                                isolation_level='AUTOCOMMIT')
        database = f'skypilot_managed_jobs_e2e_{uuid.uuid4().hex}'
        quoted_database = admin_engine.dialect.identifier_preparer.quote(
            database)
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE {quoted_database}')
        test_url = sqlalchemy.engine.make_url(configured_url).set(
            database=database)
        # testcontainers exposes its synchronous psycopg2 driver in the URL.
        # SkyPilot selects its own sync/async drivers and accepts only the
        # canonical PostgreSQL scheme at the configuration boundary.
        if test_url.drivername.startswith('postgresql+'):
            test_url = test_url.set(drivername='postgresql')
        yield test_url.render_as_string(hide_password=False)
    finally:
        if (admin_engine is not None and database is not None and
                quoted_database is not None):
            with contextlib.suppress(Exception):
                with admin_engine.connect() as connection:
                    connection.execute(
                        sqlalchemy.text('SELECT pg_terminate_backend(pid) '
                                        'FROM pg_stat_activity '
                                        'WHERE datname = :database '
                                        'AND pid <> pg_backend_pid()'),
                        {'database': database})
                    connection.exec_driver_sql(
                        f'DROP DATABASE IF EXISTS {quoted_database}')
            admin_engine.dispose()
        if container is not None:
            with contextlib.suppress(Exception):
                container.stop()


def _free_ports(count: int) -> list[int]:
    listeners = []
    try:
        for _ in range(count):
            listener = socket.socket()
            listener.bind(('127.0.0.1', 0))
            listeners.append(listener)
        return [int(listener.getsockname()[1]) for listener in listeners]
    finally:
        for listener in listeners:
            listener.close()


def _read_fault_state(path: pathlib.Path) -> dict[str, typing.Any]:
    with path.open('r', encoding='utf-8') as state_file:
        fcntl.flock(state_file, fcntl.LOCK_SH)
        return typing.cast(dict[str, typing.Any], json.load(state_file))


def _update_fault_state(path: pathlib.Path, **updates: typing.Any) -> None:
    with path.open('r+', encoding='utf-8') as state_file:
        fcntl.flock(state_file, fcntl.LOCK_EX)
        state = typing.cast(dict[str, typing.Any], json.load(state_file))
        state.update(updates)
        state_file.seek(0)
        state_file.truncate()
        json.dump(state, state_file, sort_keys=True)
        state_file.flush()
        os.fsync(state_file.fileno())


def _wait_for(predicate: typing.Callable[[], typing.Any],
              description: str,
              timeout: float = 120) -> typing.Any:
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except Exception as e:  # pylint: disable=broad-except
            last_error = e
        time.sleep(0.2)
    detail = f'; last error: {last_error}' if last_error is not None else ''
    raise AssertionError(f'Timed out waiting for {description}{detail}')


@dataclasses.dataclass
class _RoleProcess:
    """One isolated split-role server subprocess."""

    role: str
    instance_id: str
    process: subprocess.Popen
    log_path: pathlib.Path
    log_file: typing.IO[str]

    def tail(self) -> str:
        self.log_file.flush()
        with self.log_path.open('r', encoding='utf-8', errors='replace') as f:
            return ''.join(f.readlines()[-120:])

    def kill_pod(self) -> None:
        if self.process.poll() is not None:
            return
        os.killpg(self.process.pid, signal.SIGKILL)
        self.process.wait(timeout=20)

    def stop(self) -> None:
        if self.process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=20)
        self.log_file.close()


def _start_role(*, role: str, instance_id: str, api_port: int, health_port: int,
                metrics_port: int, root: pathlib.Path,
                common_env: dict[str, str]) -> _RoleProcess:
    home = root / f'home-{role}-{instance_id}'
    signal_dir = home / '.sky'
    signal_dir.mkdir(parents=True)
    (signal_dir / '.jobs_controller_consolidation_reloaded_signal').touch()
    env = common_env.copy()
    env.update({
        'HOME': str(home),
        'SKYPILOT_API_SERVER_INSTANCE_ID': instance_id,
        'SKYPILOT_API_SERVER_ROLE': role,
        'SKYPILOT_POD_NAME': f'unpaid-e2e-{role}-{instance_id[:8]}',
        'SKYPILOT_POD_NAMESPACE': 'unpaid-e2e',
        'SKYPILOT_POD_UID': instance_id,
        'POD_IP': '127.0.0.1',
    })
    log_path = root / f'{role}-{instance_id}.log'
    log_file = log_path.open('w', encoding='utf-8')
    process = subprocess.Popen([
        sys.executable, '-m', 'sky.server.server', '--deploy', '--role', role,
        '--host', '127.0.0.1', '--port',
        str(api_port), '--role-health-port',
        str(health_port), '--metrics-port',
        str(metrics_port)
    ],
                               env=env,
                               stdout=log_file,
                               stderr=subprocess.STDOUT,
                               text=True,
                               start_new_session=True)
    return _RoleProcess(role, instance_id, process, log_path, log_file)


def _wait_role_ready(role: _RoleProcess, url: str) -> None:

    def ready() -> bool:
        if role.process.poll() is not None:
            raise RuntimeError(f'{role.role} exited with '
                               f'{role.process.returncode}:\n{role.tail()}')
        response = requests.get(url, timeout=1)
        return response.status_code == 200

    try:
        _wait_for(ready, f'{role.role} readiness at {url}')
    except Exception as e:
        raise AssertionError(f'{e}\n{role.tail()}') from e


def _rows(
    engine: sqlalchemy.Engine,
    query: str,
    parameters: dict[str, typing.Any] | None = None
) -> list[dict[str, typing.Any]]:
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(query), parameters or {})
        return [dict(row) for row in result.mappings()]


def _job_row(engine: sqlalchemy.Engine) -> dict[str, typing.Any] | None:
    rows = _rows(
        engine, 'SELECT j.*, s.status AS task_status '
        'FROM job_info AS j JOIN spot AS s '
        'ON s.spot_job_id = j.spot_job_id '
        'ORDER BY j.spot_job_id DESC LIMIT 1')
    return rows[0] if rows else None


def _nested_requests(engine: sqlalchemy.Engine,
                     job_id: int) -> list[dict[str, typing.Any]]:
    return _rows(
        engine, 'SELECT request_id, handler_name, execution_class, status, '
        'user_id, worker_instance_id::text AS worker_instance_id, '
        'controller_generation, execution_generation, '
        'execution_quiesced_generation, execution_quiesced_at, '
        'managed_job_id, '
        'managed_job_controller_instance_id::text '
        'AS managed_job_controller_instance_id, '
        'managed_job_controller_generation, managed_job_controller_slot_id, '
        'managed_job_controller_slot_attempt::text '
        'AS managed_job_controller_slot_attempt '
        'FROM api_requests WHERE managed_job_id = :job_id '
        'ORDER BY created_at, request_id', {'job_id': job_id})


def _request_row(engine: sqlalchemy.Engine,
                 request_id: str) -> dict[str, typing.Any] | None:
    rows = _rows(
        engine, 'SELECT status, return_value, error, '
        'worker_instance_id::text AS worker_instance_id '
        'FROM api_requests WHERE request_id = :request_id',
        {'request_id': request_id})
    return rows[0] if rows else None


def _bootstrap_database(env: dict[str, str], root: pathlib.Path) -> None:
    migration_env = env.copy()
    migration_home = root / 'home-migrations'
    migration_home.mkdir()
    migration_env['HOME'] = str(migration_home)
    migration_env['SKYPILOT_STATE_DB_MIGRATION_MODE'] = 'bootstrap'
    result = subprocess.run(
        [sys.executable, '-m', 'sky.server.database_migrations'],
        env=migration_env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=180,
        check=False)
    (root / 'database-migrations.log').write_text(result.stdout,
                                                  encoding='utf-8')
    assert result.returncode == 0, result.stdout


def _install_rsync_shim(root: pathlib.Path) -> pathlib.Path:
    """Install the local-copy subset used by consolidation-mode launch."""
    bin_dir = root / 'bin'
    bin_dir.mkdir()
    shim = bin_dir / 'rsync'
    shim.write_text(f'''#!{sys.executable}
import pathlib
import shutil
import sys

source = pathlib.Path(sys.argv[-2]).expanduser()
destination = pathlib.Path(sys.argv[-1]).expanduser()
if source.is_dir():
    if destination.is_dir():
        destination = destination / source.name
    shutil.copytree(source, destination, dirs_exist_ok=True)
else:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
''',
                    encoding='utf-8')
    shim.chmod(0o755)
    return bin_dir


def test_managed_job_nested_requests_survive_two_controller_successors(
        postgres_url: str, tmp_path: pathlib.Path) -> None:
    """Exercise auth, role routing, claim/origin separation and adoption."""
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    fault_state = tmp_path / 'fault-state.json'
    fault_state.write_text(json.dumps({
        'pause_cleanup': 1,
        'cleanup_paused': 0,
        'pause_recovery': 0,
        'recovery_paused': 0,
        'nonloopback_auth_installed': 0,
        'controller_executor_provision_guard_installed': 0,
        'controller_executor_cloud_discovery_stub_installed': 0,
        'cloud_discovery_calls_stubbed': 0,
        'result_fault_controller_instance_id': _C1_ID,
        'c1_down_request_id': '',
        'c1_last_down_handler_request_id': '',
        'c1_down_handler_entries': 0,
        'result_observation_fault_request_id': '',
        'result_observation_faults_emitted': 0,
    }),
                           encoding='utf-8')
    plugin_config = tmp_path / 'plugins.yaml'
    plugin_config.write_text(
        'plugins:\n'
        '  - class: managed_jobs_split_role_fault_plugin.'
        'ManagedJobsSplitRoleFaultPlugin\n'
        '    parameters:\n'
        f'      state_path: {json.dumps(str(fault_state))}\n',
        encoding='utf-8')

    (api_port, api_health, executor_health, c1_health, c2_health, c3_health,
     *metrics_ports) = _free_ports(11)
    api_url = f'http://127.0.0.1:{api_port}'
    python_path = os.pathsep.join([
        str(pathlib.Path(__file__).parent),
        str(repo_root),
        os.environ.get('PYTHONPATH', ''),
    ])
    common_env = os.environ.copy()
    common_env.pop('SKYPILOT_SERVICE_ACCOUNT_TOKEN', None)
    common_env.update({
        'ENABLE_SERVICE_ACCOUNTS': 'true',
        'IS_SKYPILOT_SERVER': 'true',
        'PATH': os.pathsep.join(
            [str(_install_rsync_shim(tmp_path)),
             common_env.get('PATH', '')]),
        'PYTHONPATH': python_path,
        'PYTHONUNBUFFERED': '1',
        'SKYPILOT_API_DEPLOYMENT_NAME': 'unpaid-e2e',
        'SKYPILOT_API_REQUEST_BACKEND': 'postgres',
        'SKYPILOT_API_REQUIRE_EXECUTION_QUIESCENCE_BACKENDS': 'true',
        'SKYPILOT_API_SERVER_ENDPOINT': api_url,
        'SKYPILOT_API_SERVER_STORAGE_ENABLED': 'false',
        'SKYPILOT_CONTROLLER_CUTOVER_QUIESCENCE_SECONDS': '0',
        'SKYPILOT_DB_CONNECTION_URI': postgres_url,
        'SKYPILOT_DEV': '1',
        'SKYPILOT_DISABLE_USAGE_COLLECTION': '1',
        'SKYPILOT_EXECUTION_DRAIN_SECONDS': '0',
        'SKYPILOT_POD_CPU_CORE_LIMIT': '1',
        'SKYPILOT_POD_MEMORY_BYTES_LIMIT': str(5 * 1024**3),
        'SKYPILOT_RELEASE_NAME': 'unpaid-e2e',
        'SKYPILOT_ROLLING_UPDATE_ENABLED': 'true',
        'SKYPILOT_SERVER_CONFIG_MODE': 'postgres',
        'SKYPILOT_SERVER_PLUGINS_CONFIG': str(plugin_config),
        'SKYPILOT_SERVER_SERVE_CONTROLLER_HOLD': 'true',
        'SKYPILOT_SKIP_CLOUD_IDENTITY_CHECK': '1',
        'SKYPILOT_STATE_DB_MIGRATION_MODE': 'verify',
    })
    _bootstrap_database(common_env, tmp_path)
    engine = sqlalchemy.create_engine(postgres_url)
    processes: list[_RoleProcess] = []
    try:
        api = _start_role(role='api',
                          instance_id=_API_ID,
                          api_port=api_port,
                          health_port=api_health,
                          metrics_port=metrics_ports[0],
                          root=tmp_path,
                          common_env=common_env)
        processes.append(api)
        _wait_role_ready(api, f'{api_url}/api/health/ready')
        _wait_for(
            lambda: _read_fault_state(fault_state).get(
                'nonloopback_auth_installed') == 1,
            'non-loopback authentication guard')

        executor = _start_role(role='executor',
                               instance_id=_EXECUTOR_ID,
                               api_port=api_port,
                               health_port=executor_health,
                               metrics_port=metrics_ports[1],
                               root=tmp_path,
                               common_env=common_env)
        processes.append(executor)
        _wait_role_ready(executor, f'http://127.0.0.1:{executor_health}/readyz')

        c1 = _start_role(role='controller',
                         instance_id=_C1_ID,
                         api_port=api_port,
                         health_port=c1_health,
                         metrics_port=metrics_ports[2],
                         root=tmp_path,
                         common_env=common_env)
        processes.append(c1)
        _wait_role_ready(c1, f'http://127.0.0.1:{c1_health}/readyz')
        _wait_for(
            lambda: _read_fault_state(fault_state).get(
                'controller_executor_provision_guard_installed') == 1,
            'controller executor fail-closed provisioning guard')
        _wait_for(lambda: _read_fault_state(fault_state).get(
            'controller_executor_cloud_discovery_stub_installed') == 1,
                  'controller executor hermetic cloud discovery stub',
                  timeout=10)

        response = requests.post(f'{api_url}/jobs/launch',
                                 headers={
                                     server_constants.API_VERSION_HEADER: str(
                                         server_constants.API_VERSION)
                                 },
                                 json={
                                     'task': 'name: split-role-empty\n',
                                     'name': 'split-role-empty',
                                     'env_vars': {
                                         'SKYPILOT_USER_ID': 'unpaid-e2e-user',
                                         'SKYPILOT_USER': 'unpaid-e2e-user',
                                     },
                                     'using_remote_api_server': True,
                                 },
                                 timeout=10)
        response.raise_for_status()
        assert response.headers.get('X-Skypilot-Request-ID')

        def c1_cleanup_barrier():
            current_fault_state = _read_fault_state(fault_state)
            if current_fault_state.get('cleanup_paused') == 1:
                return True
            raise RuntimeError(
                'C1 has not reached cleanup; '
                f'fault_state={json.dumps(current_fault_state, sort_keys=True)}; '
                f'process_returncode={c1.process.poll()}')

        _wait_for(c1_cleanup_barrier, 'C1 cleanup barrier')
        assert _read_fault_state(
            fault_state)['cloud_discovery_calls_stubbed'] >= 4
        c1_job = typing.cast(dict[str, typing.Any], _job_row(engine))
        job_id = int(c1_job['spot_job_id'])
        assert c1_job['task_status'] == 'SUCCEEDED'
        assert c1_job['schedule_state'] != 'DONE'
        assert c1_job['controller_instance_id'] == _C1_ID
        assert c1_job['controller_generation'] is not None
        assert c1_job['controller_slot_id'] is not None
        assert c1_job['controller_slot_attempt'] is not None

        def settled_nested_requests() -> list[dict[str, typing.Any]] | None:
            rows = _nested_requests(engine, job_id)
            if not rows:
                return None
            if any(row['status'] not in _TERMINAL_REQUEST_STATUSES or
                   row['execution_quiesced_generation'] !=
                   row['execution_generation'] or
                   row['execution_quiesced_at'] is None for row in rows):
                return None
            return rows

        c1_nested = _wait_for(settled_nested_requests,
                              'nested request completion receipts')
        assert {'sky.core:user_initiated_down', 'sky.core:status'
               } <= {row['handler_name'] for row in c1_nested}
        c1_down_requests = [
            row for row in c1_nested
            if row['handler_name'] == 'sky.core:user_initiated_down'
        ]
        assert len(c1_down_requests) == 1
        assert c1_down_requests[0]['execution_generation'] == 1
        reconciled_fault_state = _read_fault_state(fault_state)
        c1_down_request_id = c1_down_requests[0]['request_id']
        assert reconciled_fault_state['c1_down_request_id'] == (
            c1_down_request_id)
        assert reconciled_fault_state['c1_last_down_handler_request_id'] == (
            c1_down_request_id)
        assert reconciled_fault_state[
            'result_observation_fault_request_id'] == c1_down_request_id
        assert reconciled_fault_state['result_observation_faults_emitted'] == 4
        # This proves same-ID result reconciliation did not execute a second
        # C1 handler.  The unpaid harness intentionally does not claim
        # provider-level or cross-process exactly-once effects.
        assert reconciled_fault_state['c1_down_handler_entries'] == 1
        for row in c1_nested:
            assert row['execution_class'] == 'normal'
            assert row['status'] in _TERMINAL_REQUEST_STATUSES
            assert row['user_id'] == 'unpaid-e2e-user'
            assert row['worker_instance_id'] == _C1_ID
            assert row['worker_instance_id'] != _EXECUTOR_ID
            assert row['controller_generation'] is None
            assert row['managed_job_id'] == job_id
            assert row['managed_job_controller_instance_id'] == _C1_ID
            assert (row['managed_job_controller_generation'] ==
                    c1_job['controller_generation'])
            assert (row['managed_job_controller_slot_id'] ==
                    c1_job['controller_slot_id'])
            assert (row['managed_job_controller_slot_attempt'] ==
                    c1_job['controller_slot_attempt'])

        c1.kill_pod()
        _update_fault_state(fault_state, pause_recovery=1)
        c2 = _start_role(role='controller',
                         instance_id=_C2_ID,
                         api_port=api_port,
                         health_port=c2_health,
                         metrics_port=metrics_ports[3],
                         root=tmp_path,
                         common_env=common_env)
        processes.append(c2)
        _wait_for(
            lambda: _read_fault_state(fault_state).get('recovery_paused') == 1,
            'C2 post-recovery barrier')

        reset_job = typing.cast(dict[str, typing.Any], _job_row(engine))
        assert reset_job['spot_job_id'] == job_id
        assert reset_job['task_status'] == 'SUCCEEDED'
        assert reset_job['schedule_state'] == 'WAITING'
        for column in ('controller_instance_id', 'controller_generation',
                       'controller_slot_id', 'controller_slot_attempt'):
            assert reset_job[column] is None
        assert _nested_requests(engine, job_id) == c1_nested

        c2.kill_pod()
        c3 = _start_role(role='controller',
                         instance_id=_C3_ID,
                         api_port=api_port,
                         health_port=c3_health,
                         metrics_port=metrics_ports[4],
                         root=tmp_path,
                         common_env=common_env)
        processes.append(c3)
        _wait_role_ready(c3, f'http://127.0.0.1:{c3_health}/readyz')
        done_job = _wait_for(
            lambda: (row if (row := _job_row(engine)) is not None and row[
                'schedule_state'] == 'DONE' else None),
            'C3 finalization of the recovered job')
        assert done_job['task_status'] == 'SUCCEEDED'
        final_nested = _wait_for(settled_nested_requests,
                                 'C3 nested request completion receipts')
        retained_ids = {row['request_id'] for row in final_nested}
        assert {row['request_id'] for row in c1_nested} <= retained_ids
        c3_nested = [
            row for row in final_nested
            if row['managed_job_controller_instance_id'] == _C3_ID
        ]
        assert {'sky.core:user_initiated_down', 'sky.core:status'
               } <= {row['handler_name'] for row in c3_nested}
        assert done_job['controller_instance_id'] == _C3_ID
        assert done_job['controller_generation'] is not None
        assert done_job['controller_slot_id'] is not None
        assert done_job['controller_slot_attempt'] is not None
        for row in c3_nested:
            assert row['execution_class'] == 'normal'
            assert row['status'] in _TERMINAL_REQUEST_STATUSES
            assert row['user_id'] == 'unpaid-e2e-user'
            assert row['worker_instance_id'] == _C3_ID
            assert row['worker_instance_id'] != _EXECUTOR_ID
            assert row['controller_generation'] is None
            assert row['managed_job_id'] == job_id
            assert (row['managed_job_controller_generation'] ==
                    done_job['controller_generation'])
            assert (row['managed_job_controller_slot_id'] ==
                    done_job['controller_slot_id'])
            assert (row['managed_job_controller_slot_attempt'] ==
                    done_job['controller_slot_attempt'])

        # Query through the real route and split-role request executor.  A
        # non-empty result is essential: an empty list is already JSON-native
        # and cannot detect missing enum/datetime wire encoding.
        for include_cluster_events in (False, True):
            events_response = requests.post(
                f'{api_url}/jobs/events',
                headers={
                    server_constants.API_VERSION_HEADER: str(
                        server_constants.API_VERSION)
                },
                json={
                    'job_id': job_id,
                    'task_id': 0,
                    'limit': 100,
                    'include_cluster_events': include_cluster_events,
                },
                timeout=10)
            events_response.raise_for_status()
            events_request_id = events_response.headers.get(
                'X-Skypilot-Request-ID')
            assert events_request_id is not None

            events_request = _wait_for(
                lambda request_id=events_request_id:
                (row if (row := _request_row(engine, request_id)) is not None
                 and row['status'] in _TERMINAL_REQUEST_STATUSES else None),
                'managed job events request completion')
            assert events_request['status'] == 'SUCCEEDED', events_request[
                'error']
            assert events_request['worker_instance_id'] == _C3_ID
            events = events_request['return_value']
            assert events
            assert all(isinstance(event['new_status'], str) for event in events)
            assert all(isinstance(event['timestamp'], str) for event in events)

        token_owners = _rows(
            engine, 'SELECT token_id FROM api_access_tokens '
            'WHERE job_id = :job_id', {'job_id': job_id})
        assert len(token_owners) == 1
        assert not _rows(
            engine, 'SELECT token_id FROM service_account_tokens '
            'WHERE token_id = :token_id', token_owners[0])
        remaining_tokens = _rows(
            engine, 'SELECT token_name, created_at, expires_at '
            'FROM service_account_tokens '
            "WHERE token_name LIKE 'managed-job-%'")
        # C1 was SIGKILLed after its cleanup boundary, so its private bearer
        # cannot be synchronously revoked.  It must instead remain bounded by
        # the managed-job TTL and therefore eligible for the expiry sweeper.
        assert len(remaining_tokens) == 1
        assert re.fullmatch(rf'managed-job-controller-{job_id}-[0-9a-f]{{8}}',
                            remaining_tokens[0]['token_name'])
        assert remaining_tokens[0]['created_at'] is not None
        assert remaining_tokens[0]['expires_at'] is not None
        token_lifetime = (remaining_tokens[0]['expires_at'] -
                          remaining_tokens[0]['created_at'])
        assert 3 * 24 * 60 * 60 - 5 <= token_lifetime <= 3 * 24 * 60 * 60
    finally:
        for process in reversed(processes):
            process.stop()
        engine.dispose()
