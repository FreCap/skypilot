"""Deterministic crash barriers for the split-role managed-jobs E2E."""

import asyncio
import fcntl
import json
import os
import time
import typing

import fastapi
from starlette import responses as starlette_responses

from sky import core as sky_core
from sky.backends import cloud_vm_ray_backend
from sky.jobs import controller as managed_job_controller
from sky.jobs import managed_job_refresh_thread
from sky.jobs import utils as managed_job_utils
from sky.server import plugins
from sky.server.auth import loopback as auth_loopback
from sky.utils import common_utils

_RESULT_OBSERVATION_FAULTS: tuple[tuple[int, dict[str, str]], ...] = (
    (502, {
        'detail': 'Injected request-result gateway failure (1/4).'
    }),
    (200, {
        'malformed': 'Injected invalid request-result payload (2/4).'
    }),
    (504, {
        'detail': 'Injected request-result gateway timeout (3/4).'
    }),
    (200, {
        'malformed': 'Injected invalid request-result payload (4/4).'
    }),
)


def _mutate_state(
        path: str, mutation: typing.Callable[[dict[str, typing.Any]],
                                             None]) -> None:
    with open(path, 'a+', encoding='utf-8') as state_file:
        fcntl.flock(state_file, fcntl.LOCK_EX)
        state_file.seek(0)
        raw = state_file.read()
        state = json.loads(raw) if raw else {}
        mutation(state)
        state_file.seek(0)
        state_file.truncate()
        json.dump(state, state_file, sort_keys=True)
        state_file.flush()
        os.fsync(state_file.fileno())


def _consume_barrier(path: str, arm_key: str, reached_key: str) -> bool:
    paused = False

    def consume(state: dict[str, typing.Any]) -> None:
        nonlocal paused
        if state.get(arm_key, 0) <= 0:
            return
        state[arm_key] -= 1
        state[reached_key] = state.get(reached_key, 0) + 1
        paused = True

    _mutate_state(path, consume)
    return paused


def _consume_result_observation_fault(
        path: str, request_id: str) -> tuple[int, dict[str, str]] | None:
    """Consume the next scripted fault only for the exact captured request."""
    fault: tuple[int, dict[str, str]] | None = None

    def consume(state: dict[str, typing.Any]) -> None:
        nonlocal fault
        target_request_id = state.get('c1_down_request_id')
        if not target_request_id or request_id != target_request_id:
            return
        fault_index = int(state.get('result_observation_faults_emitted', 0))
        if fault_index >= len(_RESULT_OBSERVATION_FAULTS):
            return
        fault = _RESULT_OBSERVATION_FAULTS[fault_index]
        state['result_observation_faults_emitted'] = fault_index + 1
        state['result_observation_fault_request_id'] = request_id

    _mutate_state(path, consume)
    return fault


def _record_c1_down_handler_entry(path: str) -> None:
    """Capture and count the exact C1 down request at handler entry."""
    controller_instance_id = os.environ.get('SKYPILOT_API_SERVER_INSTANCE_ID')
    request_id = common_utils.get_current_request_id()

    def record(state: dict[str, typing.Any]) -> None:
        if controller_instance_id != state.get(
                'result_fault_controller_instance_id'):
            return
        state['c1_down_handler_entries'] = int(
            state.get('c1_down_handler_entries', 0)) + 1
        state['c1_last_down_handler_request_id'] = request_id
        if not state.get('c1_down_request_id'):
            state['c1_down_request_id'] = request_id

    _mutate_state(path, record)


def _pause_forever() -> typing.NoReturn:
    while True:
        time.sleep(1)


def _block_billable_provisioning(*args: typing.Any,
                                 **kwargs: typing.Any) -> typing.NoReturn:
    del args, kwargs
    raise AssertionError('The unpaid managed-jobs E2E reached VM provisioning.')


class ManagedJobsSplitRoleFaultPlugin(plugins.BasePlugin):
    """Pause exact lifecycle boundaries so the test can kill a role."""

    load_contexts = frozenset({
        plugins.PluginContext.MAIN, plugins.PluginContext.UVICORN,
        plugins.PluginContext.CONTROLLER, plugins.PluginContext.EXECUTOR
    })

    def __init__(self, state_path: str):
        self._state_path = state_path

    def install(self, extension_context: plugins.ExtensionContext) -> None:
        if extension_context.context == plugins.PluginContext.UVICORN:
            if os.environ.get('SKYPILOT_API_SERVER_ROLE') != 'api':
                return
            # The process E2E uses loopback TCP in place of Pod networking.
            # Force the production bearer path so nested controller requests
            # cannot pass through the localhost trust exemption.
            auth_loopback.is_loopback_request = lambda request: False

            def record_nonloopback_auth(state: dict[str, typing.Any]) -> None:
                state['nonloopback_auth_installed'] = 1

            _mutate_state(self._state_path, record_nonloopback_auth)

            app = extension_context.app
            assert app is not None

            @app.middleware('http')
            async def inject_result_observation_faults(
                request: fastapi.Request,
                call_next: typing.Callable[
                    [fastapi.Request],
                    typing.Awaitable[starlette_responses.Response]],
            ) -> starlette_responses.Response:
                # Let the production endpoint wait for and read the durable
                # terminal row first.  Replacing that response models a lost
                # observation ACK; it cannot hide an unfinished request.
                response = await call_next(request)
                if request.url.path == '/api/get':
                    request_id = request.query_params.get('request_id')
                    if request_id is not None:
                        fault = _consume_result_observation_fault(
                            self._state_path, request_id)
                        if fault is not None:
                            status_code, body = fault
                            return starlette_responses.JSONResponse(
                                status_code=status_code, content=body)
                return response

            return

        if extension_context.context == plugins.PluginContext.EXECUTOR:
            # Request handlers run in spawn-based executor processes.  Poison
            # their canonical VM backend boundary before they can touch any
            # cloud provider, even if the empty-task shortcut regresses.
            setattr(cloud_vm_ray_backend.CloudVmRayBackend, '_provision',
                    _block_billable_provisioning)
            if os.environ.get('SKYPILOT_API_SERVER_ROLE') == 'controller':
                original_down = sky_core.down

                def down_with_handler_counter(*args: typing.Any,
                                              **kwargs: typing.Any) -> None:
                    _record_c1_down_handler_entry(self._state_path)
                    return original_down(*args, **kwargs)

                sky_core.down = down_with_handler_counter

                def record_executor_guard(state: dict[str, typing.Any]) -> None:
                    state['controller_executor_provision_guard_installed'] = 1

                _mutate_state(self._state_path, record_executor_guard)
            return

        if extension_context.context == plugins.PluginContext.MAIN:
            if os.environ.get('SKYPILOT_API_SERVER_ROLE') != 'controller':
                return
            managed_job_refresh_thread._RECOVERY_WAIT_AFTER_ACQUIRE_SECONDS = 0  # pylint: disable=protected-access
            original = managed_job_utils.ha_recovery_for_consolidation_mode

            def recovery_with_barrier() -> None:
                original()
                if _consume_barrier(self._state_path, 'pause_recovery',
                                    'recovery_paused'):
                    _pause_forever()

            managed_job_utils.ha_recovery_for_consolidation_mode = (
                recovery_with_barrier)
            return

        original_cleanup = managed_job_controller.ControllerManager._cleanup  # pylint: disable=protected-access

        async def cleanup_with_barrier(*args: typing.Any,
                                       **kwargs: typing.Any) -> None:
            await original_cleanup(*args, **kwargs)
            if _consume_barrier(self._state_path, 'pause_cleanup',
                                'cleanup_paused'):
                while True:
                    await asyncio.sleep(1)

        managed_job_controller.ControllerManager._cleanup = cleanup_with_barrier  # pylint: disable=protected-access
