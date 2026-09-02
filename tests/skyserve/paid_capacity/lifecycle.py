"""Own one paid-Spot qualification service from creation through cleanup.

This is the billable test entrypoint.  It is deliberately the only component
that mutates service lifecycle: the qualifier itself remains read-only.  Once
``serve up`` is attempted, normal ``serve down`` and exact cleanup observation
are attempted for every success, failure, or interrupt.  Provider-native
deletion is never automatic; a failed exact cleanup is an operator escalation.
"""

import argparse
import asyncio
import dataclasses
import hashlib
import json
import math
import os
import pathlib
import signal
import time
from typing import Awaitable, Callable, Protocol, TypeVar
import urllib.parse

import qualify

_T = TypeVar('_T')


class LifecycleError(RuntimeError):
    """The qualification lifecycle could not complete safely."""


@dataclasses.dataclass(frozen=True, kw_only=True)
class CommandResult:
    """Credential-free result of one SkyPilot CLI invocation."""

    returncode: int
    stdout: str


class ServiceLifecycle(Protocol):
    """Narrow swappable boundary around SkyServe lifecycle operations."""

    async def ensure_absent(self, service_name: str) -> None:
        ...

    async def up(self, service_name: str, service_yaml: pathlib.Path) -> None:
        ...

    async def endpoint(self, service_name: str) -> str:
        ...

    async def down(self, service_name: str) -> None:
        ...


class SkyCliLifecycle:
    """Production lifecycle implementation using only normal SkyServe CLI."""

    def __init__(self, *, executable: str, command_timeout_seconds: float,
                 endpoint_timeout_seconds: float, down_timeout_seconds: float,
                 poll_seconds: float, workspace: str | None) -> None:
        self._executable = executable
        self._command_timeout_seconds = command_timeout_seconds
        self._endpoint_timeout_seconds = endpoint_timeout_seconds
        self._down_timeout_seconds = down_timeout_seconds
        self._poll_seconds = poll_seconds
        self._workspace = workspace

    async def _run(self, *arguments: str, capture: bool) -> CommandResult:
        command = list(arguments)
        if self._workspace is not None:
            command.extend(('--config', f'active_workspace={self._workspace}'))
        stdout = asyncio.subprocess.PIPE if capture else None
        stderr = asyncio.subprocess.STDOUT if capture else None
        process = await asyncio.create_subprocess_exec(self._executable,
                                                       *command,
                                                       stdout=stdout,
                                                       stderr=stderr)
        try:
            output, _ = await asyncio.wait_for(
                process.communicate(), timeout=self._command_timeout_seconds)
        except BaseException:
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=10)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
            raise
        assert process.returncode is not None
        return CommandResult(returncode=process.returncode,
                             stdout=('' if output is None else output.decode(
                                 errors='replace').strip()))

    async def ensure_absent(self, service_name: str) -> None:
        result = await self._run('serve',
                                 'status',
                                 '--endpoint',
                                 service_name,
                                 capture=True)
        if result.returncode == 0:
            raise LifecycleError(
                'Refusing to replace a pre-existing qualification service.')
        if 'No service found.' not in result.stdout:
            raise LifecycleError(
                'Could not prove the qualification service name is unused.')

    async def up(self, service_name: str, service_yaml: pathlib.Path) -> None:
        result = await self._run('serve',
                                 'up',
                                 '-n',
                                 service_name,
                                 '-y',
                                 str(service_yaml),
                                 capture=False)
        if result.returncode != 0:
            raise LifecycleError('Normal sky serve up failed.')

    async def endpoint(self, service_name: str) -> str:
        deadline = time.monotonic() + self._endpoint_timeout_seconds
        while time.monotonic() < deadline:
            started = time.monotonic()
            result = await self._run('serve',
                                     'status',
                                     '--endpoint',
                                     service_name,
                                     capture=True)
            if result.returncode == 0 and result.stdout != '-':
                parsed = urllib.parse.urlparse(result.stdout)
                if parsed.scheme not in ('http', 'https') or not parsed.netloc:
                    raise LifecycleError(
                        'SkyServe returned a malformed qualification endpoint.')
                return result.stdout
            await asyncio.sleep(
                max(0, started + self._poll_seconds - time.monotonic()))
        raise LifecycleError('Qualification endpoint did not become ready.')

    async def down(self, service_name: str) -> None:
        deadline = time.monotonic() + self._down_timeout_seconds
        while time.monotonic() < deadline:
            started = time.monotonic()
            result = await self._run('serve',
                                     'down',
                                     '-y',
                                     service_name,
                                     capture=False)
            if result.returncode == 0:
                return
            await asyncio.sleep(
                max(0, started + self._poll_seconds - time.monotonic()))
        raise LifecycleError('Normal sky serve down did not succeed.')


@dataclasses.dataclass(frozen=True, kw_only=True)
class LifecycleArtifacts:
    """All immutable inputs and durable outputs for one disposable service."""

    rendered_service: pathlib.Path
    provider_scope: pathlib.Path
    qualification_receipt: pathlib.Path
    cleanup_receipt: pathlib.Path
    lifecycle_receipt: pathlib.Path

    @classmethod
    def create(cls, directory: pathlib.Path,
               service_name: str) -> 'LifecycleArtifacts':
        return cls(
            rendered_service=directory / f'{service_name}-service.yaml',
            provider_scope=directory / f'{service_name}-scope.json',
            qualification_receipt=directory / f'{service_name}-qualify.json',
            cleanup_receipt=directory / f'{service_name}-cleanup.json',
            lifecycle_receipt=directory / f'{service_name}-lifecycle.json')


class LifecycleReceipt:
    """Atomically persisted lifecycle/finalizer evidence."""

    def __init__(self, *, path: pathlib.Path, service_name: str,
                 profile: str) -> None:
        self._path = path
        self._payload = {
            'schema_version': 1,
            'service_name': service_name,
            'profile': profile,
            'started_at': time.time(),
            'stages': [],
            'emergency_provider_cleanup': 'not_performed',
        }
        self._flush()

    def stage(self,
              name: str,
              started_at: float,
              error: BaseException | None = None) -> None:
        self._payload['stages'].append({
            'name': name,
            'started_at': started_at,
            'finished_at': time.time(),
            'outcome': 'passed' if error is None else 'failed',
            **({} if error is None else {
                   'error_type': type(error).__name__
               }),
        })
        self._flush()

    def finish(self, *, primary_error: BaseException | None,
               scope_recovery_error: BaseException | None,
               serve_down_error: BaseException | None,
               cleanup_evidence_error: BaseException | None,
               cleanup_required: bool, cleanup_receipt: pathlib.Path) -> None:
        cleanup_sha256 = None
        try:
            cleanup_sha256 = hashlib.sha256(
                cleanup_receipt.read_bytes()).hexdigest()
        except OSError:
            pass
        exact_cleanup_proven = (cleanup_evidence_error is None and
                                cleanup_sha256 is not None
                                if cleanup_required else None)
        self._payload.update({
            'finished_at': time.time(),
            'outcome': ('passed' if all(
                error is None
                for error in (primary_error, scope_recovery_error,
                              serve_down_error,
                              cleanup_evidence_error)) else 'failed'),
            'primary_error_type':
                (None if primary_error is None else type(primary_error).__name__
                ),
            'scope_recovery_error_type':
                (None if scope_recovery_error is None else
                 type(scope_recovery_error).__name__),
            'serve_down_error_type': (None if serve_down_error is None else
                                      type(serve_down_error).__name__),
            'cleanup_evidence_error_type':
                (None if cleanup_evidence_error is None else
                 type(cleanup_evidence_error).__name__),
            'cleanup_evidence_required': cleanup_required,
            'exact_cleanup_proven': exact_cleanup_proven,
            'cleanup_receipt_sha256': cleanup_sha256,
            'operator_escalation_required': (cleanup_required and
                                             exact_cleanup_proven is not True),
        })
        self._flush()

    def _flush(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_suffix(f'{self._path.suffix}.tmp')
        with temporary.open('w', encoding='utf-8') as stream:
            json.dump(self._payload, stream, indent=2, sort_keys=True)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self._path)


async def _record_stage(receipt: LifecycleReceipt, name: str,
                        operation: Callable[[], Awaitable[_T]]) -> _T:
    started_at = time.time()
    try:
        result = await operation()
    except BaseException as error:  # pylint: disable=broad-exception-caught
        receipt.stage(name, started_at, error)
        raise
    receipt.stage(name, started_at)
    return result


async def run_lifecycle(args: argparse.Namespace,
                        lifecycle: ServiceLifecycle | None = None) -> None:
    """Run one disposable service and always execute its normal finalizer."""
    timeouts = (args.command_timeout_seconds, args.endpoint_timeout_seconds,
                args.scope_timeout_seconds, args.down_timeout_seconds,
                args.cleanup_timeout_seconds, args.poll_seconds)
    if any(not math.isfinite(value) or value <= 0 for value in timeouts):
        raise LifecycleError('Lifecycle timeouts must be positive and finite.')
    if (args.workspace is not None and
        (not isinstance(args.workspace, str) or not args.workspace)):
        raise LifecycleError('Workspace must be a non-empty string.')
    artifacts = LifecycleArtifacts.create(pathlib.Path(args.artifacts_dir),
                                          args.service_name)
    existing = [
        path for path in dataclasses.astuple(artifacts) if path.exists()
    ]
    if existing:
        raise LifecycleError(
            'Refusing to overwrite paid qualification evidence artifacts.')
    receipt = LifecycleReceipt(path=artifacts.lifecycle_receipt,
                               service_name=args.service_name,
                               profile=args.profile)
    lifecycle = lifecycle or SkyCliLifecycle(
        executable=args.sky_cli,
        command_timeout_seconds=args.command_timeout_seconds,
        endpoint_timeout_seconds=args.endpoint_timeout_seconds,
        down_timeout_seconds=args.down_timeout_seconds,
        poll_seconds=args.poll_seconds,
        workspace=args.workspace)
    primary_error: BaseException | None = None
    scope_recovery_error: BaseException | None = None
    serve_down_error: BaseException | None = None
    cleanup_evidence_error: BaseException | None = None
    lifecycle_owned = False
    scope_args = argparse.Namespace(service_name=args.service_name,
                                    output=str(artifacts.provider_scope),
                                    timeout_seconds=args.scope_timeout_seconds,
                                    poll_seconds=args.poll_seconds,
                                    postgres_url_env=args.postgres_url_env)
    try:
        render_args = argparse.Namespace(profile=args.profile,
                                         provider=args.provider,
                                         source=args.source,
                                         economic_receipt=args.economic_receipt,
                                         output=str(artifacts.rendered_service))
        await _record_stage(
            receipt, 'render',
            lambda: asyncio.to_thread(qualify.render_service, render_args))
        await _record_stage(receipt, 'prove-service-absent',
                            lambda: lifecycle.ensure_absent(args.service_name))
        # Ownership begins before the mutating request: a lost acknowledgement
        # must still trigger teardown of the uniquely preflighted name.
        lifecycle_owned = True
        await _record_stage(
            receipt, 'serve-up',
            lambda: lifecycle.up(args.service_name, artifacts.rendered_service))
        await _record_stage(
            receipt, 'freeze-scope',
            lambda: asyncio.to_thread(qualify.freeze_provider_scope, scope_args)
        )
        endpoint = await _record_stage(
            receipt, 'endpoint', lambda: lifecycle.endpoint(args.service_name))
        await _record_stage(
            receipt, 'qualify', lambda: qualify.qualify(
                argparse.Namespace(profile=args.profile,
                                   provider=args.provider,
                                   service_name=args.service_name,
                                   endpoint=endpoint,
                                   receipt=str(artifacts.qualification_receipt),
                                   scope=str(artifacts.provider_scope),
                                   economic_receipt=args.economic_receipt,
                                   auth_token_env=args.auth_token_env,
                                   postgres_url_env=args.postgres_url_env)))
    except BaseException as error:  # pylint: disable=broad-exception-caught
        primary_error = error
    finally:
        if lifecycle_owned:
            # A successful create followed by a lost acknowledgement reaches
            # this finalizer before the normal freeze stage.  Recover exact
            # provider authority while the service still exists; never invent
            # a scope merely to make cleanup appear successful.
            if not artifacts.provider_scope.exists():
                try:
                    await _record_stage(
                        receipt,
                        'freeze-scope-recovery', lambda: asyncio.to_thread(
                            qualify.freeze_provider_scope, scope_args))
                except BaseException as error:  # pylint: disable=broad-exception-caught
                    scope_recovery_error = error
            try:
                await _record_stage(receipt, 'serve-down',
                                    lambda: lifecycle.down(args.service_name))
            except BaseException as error:  # pylint: disable=broad-exception-caught
                serve_down_error = error
            try:
                await _record_stage(
                    receipt, 'wait-cleanup', lambda: qualify.wait_for_cleanup(
                        argparse.Namespace(
                            service_name=args.service_name,
                            scope=str(artifacts.provider_scope),
                            receipt=str(artifacts.qualification_receipt),
                            output=str(artifacts.cleanup_receipt),
                            timeout_seconds=args.cleanup_timeout_seconds,
                            poll_seconds=args.poll_seconds,
                            postgres_url_env=args.postgres_url_env)))
            except BaseException as error:  # pylint: disable=broad-exception-caught
                cleanup_evidence_error = error
            if (cleanup_evidence_error is None and
                    not artifacts.cleanup_receipt.is_file()):
                cleanup_evidence_error = LifecycleError(
                    'Cleanup returned without a durable evidence receipt.')
        receipt.finish(primary_error=primary_error,
                       scope_recovery_error=scope_recovery_error,
                       serve_down_error=serve_down_error,
                       cleanup_evidence_error=cleanup_evidence_error,
                       cleanup_required=lifecycle_owned,
                       cleanup_receipt=artifacts.cleanup_receipt)
    finalizer_errors: list[BaseException] = []
    if cleanup_evidence_error is not None:
        error = LifecycleError(
            'Normal teardown lacks exact-zero cleanup evidence; explicit '
            'operator escalation is required.')
        error.__cause__ = cleanup_evidence_error
        finalizer_errors.append(error)
    if scope_recovery_error is not None:
        error = LifecycleError(
            'Provider scope recovery failed before proven cleanup.')
        error.__cause__ = scope_recovery_error
        finalizer_errors.append(error)
    if serve_down_error is not None:
        error = LifecycleError(
            'Normal sky serve down failed, but exact-zero cleanup was proven.')
        error.__cause__ = serve_down_error
        finalizer_errors.append(error)
    if primary_error is not None and finalizer_errors:
        raise BaseExceptionGroup(
            'Paid qualification failed and its lifecycle finalizer also '
            'failed; inspect the lifecycle receipt.',
            [primary_error, *finalizer_errors])
    if primary_error is not None:
        raise primary_error
    if len(finalizer_errors) == 1:
        raise finalizer_errors[0]
    if finalizer_errors:
        raise BaseExceptionGroup(
            'Paid qualification lifecycle finalization had multiple failures; '
            'inspect the lifecycle receipt.', finalizer_errors)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', choices=qualify.PROFILES, required=True)
    parser.add_argument('--provider', choices=('aws', 'gcp'))
    parser.add_argument('--service-name', required=True)
    parser.add_argument('--artifacts-dir', required=True)
    parser.add_argument('--source',
                        default=str(
                            pathlib.Path(__file__).with_name('service.yaml')))
    parser.add_argument('--economic-receipt')
    parser.add_argument('--workspace')
    parser.add_argument('--sky-cli', default='sky')
    parser.add_argument('--command-timeout-seconds',
                        type=float,
                        default=15 * 60)
    parser.add_argument('--endpoint-timeout-seconds',
                        type=float,
                        default=15 * 60)
    parser.add_argument('--scope-timeout-seconds', type=float, default=5 * 60)
    parser.add_argument('--down-timeout-seconds', type=float, default=5 * 60)
    parser.add_argument('--cleanup-timeout-seconds',
                        type=float,
                        default=30 * 60)
    parser.add_argument('--poll-seconds', type=float, default=10)
    parser.add_argument('--auth-token-env',
                        default='SKYPILOT_SERVE_E2E_AUTH_TOKEN')
    parser.add_argument('--postgres-url-env',
                        default='SKYPILOT_DB_CONNECTION_URI')
    return parser


async def _run_with_signal_finalizer(args: argparse.Namespace) -> None:
    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    installed = []
    for sig in (signal.SIGTERM, signal.SIGHUP):
        try:
            loop.add_signal_handler(sig, task.cancel)
            installed.append(sig)
        except NotImplementedError:
            pass
    try:
        await run_lifecycle(args)
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)


def main() -> None:
    asyncio.run(_run_with_signal_finalizer(_parser().parse_args()))


if __name__ == '__main__':
    main()
