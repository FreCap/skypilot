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
import enum
import json
import math
import os
import pathlib
import signal
import time
from typing import Awaitable, Callable, Protocol, TypeVar
import urllib.parse

import qualify

from sky.serve import constants as serve_constants
from sky.serve import lb_k8s

_T = TypeVar('_T')
_LIFECYCLE_REAP_GRACE_SECONDS = 10.0


class LifecycleError(RuntimeError):
    """The qualification lifecycle could not complete safely."""


class LifecycleFailureGroup(LifecycleError):
    """Multiple lifecycle failures preserved on every supported Python."""

    def __init__(self, message: str, exceptions: list[BaseException]) -> None:
        if not exceptions:
            raise ValueError('Lifecycle failure group must not be empty.')
        self.exceptions = tuple(exceptions)
        summary = ', '.join(type(error).__name__ for error in exceptions)
        super().__init__(f'{message} Failures: {summary}.')


@dataclasses.dataclass(frozen=True, kw_only=True)
class CommandResult:
    """Credential-free result of one SkyPilot CLI invocation."""

    returncode: int
    stdout: str


class EndpointMode(str, enum.Enum):
    """Network path used only by the qualification request client."""

    PUBLISHED = 'published'
    IN_CLUSTER = 'in-cluster'

    def __str__(self) -> str:
        return self.value


@dataclasses.dataclass(frozen=True, kw_only=True)
class EndpointResolutionRequest:
    """Durable authority needed to resolve one qualification endpoint."""

    service_name: str
    provider_scope: pathlib.Path


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


class ServiceEndpointResolver(Protocol):
    """Swappable endpoint projection with no service lifecycle authority."""

    async def resolve(self, request: EndpointResolutionRequest) -> str:
        ...


class PublishedEndpointResolver:
    """Resolve the normal provider-published SkyServe endpoint."""

    def __init__(self, lifecycle: ServiceLifecycle) -> None:
        self._lifecycle = lifecycle

    async def resolve(self, request: EndpointResolutionRequest) -> str:
        return await self._lifecycle.endpoint(request.service_name)


class InClusterEndpointResolver:
    """Resolve the incarnation-scoped LB Service through cluster DNS."""

    def __init__(self, *, namespace: str | None = None) -> None:
        if namespace is not None and (not isinstance(namespace, str) or
                                      not namespace):
            raise ValueError('LB namespace must be a non-empty string.')
        self._namespace = namespace

    async def resolve(self, request: EndpointResolutionRequest) -> str:
        # The frozen provider receipt is written from the current committed
        # service row before qualification traffic, so no mutable lookup can
        # redirect this client to another same-name service incarnation.
        scope = qualify.read_provider_scope(request.provider_scope,
                                            request.service_name)
        namespace = self._namespace or lb_k8s.get_lb_namespace()
        service_name = lb_k8s.lb_service_name(request.service_name,
                                              scope.resource_scope)
        return (f'http://{service_name}.{namespace}:'
                f'{serve_constants.LOAD_BALANCER_PORT_START}')


def _endpoint_resolver(mode: EndpointMode,
                       lifecycle: ServiceLifecycle) -> ServiceEndpointResolver:
    if mode is EndpointMode.PUBLISHED:
        return PublishedEndpointResolver(lifecycle)
    if mode is EndpointMode.IN_CLUSTER:
        return InClusterEndpointResolver()
    raise ValueError(f'Unsupported endpoint mode: {mode!r}.')


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

    async def _run(
        self,
        *arguments: str,
        capture: bool,
        phase_deadline: qualify.AbsoluteDeadline | None = None,
    ) -> CommandResult:
        command = list(arguments)
        if self._workspace is not None:
            command.extend(('--config', f'active_workspace={self._workspace}'))
        stdout = asyncio.subprocess.PIPE if capture else None
        stderr = asyncio.subprocess.STDOUT if capture else None
        if phase_deadline is None:
            command_deadline = qualify.AbsoluteDeadline.after(
                self._command_timeout_seconds)
        else:
            command_deadline = phase_deadline.capped_after(
                self._command_timeout_seconds)
        spawn = asyncio.create_task(
            asyncio.create_subprocess_exec(self._executable,
                                           *command,
                                           stdout=stdout,
                                           stderr=stderr,
                                           start_new_session=True))
        try:
            process = await asyncio.wait_for(
                asyncio.shield(spawn), timeout=command_deadline.remaining())
        except BaseException:

            async def _settle_spawn() -> None:
                spawn.cancel()
                result, = await asyncio.gather(spawn, return_exceptions=True)
                if not isinstance(result, BaseException):
                    await qualify.terminate_and_reap_process(
                        result, grace_seconds=_LIFECYCLE_REAP_GRACE_SECONDS)

            await qualify._await_cancellation_resistant(
                asyncio.create_task(_settle_spawn()))
            raise
        communication = asyncio.create_task(process.communicate())
        try:
            output, _ = await asyncio.wait_for(
                asyncio.shield(communication),
                timeout=command_deadline.remaining())
        except BaseException:
            await qualify.terminate_and_reap_process(
                process, grace_seconds=_LIFECYCLE_REAP_GRACE_SECONDS)
            await qualify._settle_process_communication(
                communication, grace_seconds=_LIFECYCLE_REAP_GRACE_SECONDS)
            raise
        # A successful CLI process may still leave an inherited credential
        # helper behind.  Its output is not publishable until the fresh process
        # group created above is physically extinct.
        await qualify.terminate_and_reap_process(
            process, grace_seconds=_LIFECYCLE_REAP_GRACE_SECONDS)
        await qualify._settle_process_communication(
            communication, grace_seconds=_LIFECYCLE_REAP_GRACE_SECONDS)
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
        deadline = qualify.AbsoluteDeadline.after(
            self._endpoint_timeout_seconds)
        while not deadline.expired():
            started = time.monotonic()
            try:
                result = await self._run('serve',
                                         'status',
                                         '--endpoint',
                                         service_name,
                                         capture=True,
                                         phase_deadline=deadline)
            except asyncio.TimeoutError:
                if deadline.expired():
                    break
                raise
            if result.returncode == 0 and result.stdout != '-':
                parsed = urllib.parse.urlparse(result.stdout)
                if parsed.scheme not in ('http', 'https') or not parsed.netloc:
                    raise LifecycleError(
                        'SkyServe returned a malformed qualification endpoint.')
                return result.stdout
            await deadline.sleep(
                max(0, started + self._poll_seconds - time.monotonic()))
        raise LifecycleError('Qualification endpoint did not become ready.')

    async def down(self, service_name: str) -> None:
        """Obtain durable teardown admission; native absence is proved later."""
        deadline = qualify.AbsoluteDeadline.after(self._down_timeout_seconds)
        while not deadline.expired():
            started = time.monotonic()
            try:
                result = await self._run('serve',
                                         'down',
                                         '-y',
                                         service_name,
                                         capture=False,
                                         phase_deadline=deadline)
            except asyncio.TimeoutError:
                if deadline.expired():
                    break
                raise
            if result.returncode == 0:
                return
            await deadline.sleep(
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
            'finalizer_scope': 'in_process_cooperative_cancellation',
            'owner_loss_requires_operator_escalation': True,
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
            'outcome': qualify._receipt_outcome(error),
            **({} if error is None else {
                   'error_type': type(error).__name__
               }),
        })
        self._flush()

    def finish(self, *, primary_error: BaseException | None,
               scope_recovery_error: BaseException | None,
               cleanup_scope_freeze_error: BaseException | None,
               serve_down_error: BaseException | None,
               cleanup_evidence_error: BaseException | None,
               cleanup_required: bool, serve_up_acknowledged: bool,
               validated_cleanup_sha256: str | None) -> None:
        exact_cleanup_proven = (
            serve_up_acknowledged and cleanup_scope_freeze_error is None and
            cleanup_evidence_error is None and
            validated_cleanup_sha256 is not None if cleanup_required else None)
        errors = (primary_error, scope_recovery_error,
                  cleanup_scope_freeze_error, serve_down_error,
                  cleanup_evidence_error)
        non_null_errors = tuple(error for error in errors if error is not None)
        if not non_null_errors:
            outcome = 'passed'
        elif all(
                isinstance(error, (asyncio.CancelledError, KeyboardInterrupt))
                for error in non_null_errors):
            outcome = 'interrupted'
        else:
            outcome = 'failed'
        self._payload.update({
            'finished_at': time.time(),
            'outcome': outcome,
            'primary_error_type':
                (None if primary_error is None else type(primary_error).__name__
                ),
            'scope_recovery_error_type':
                (None if scope_recovery_error is None else
                 type(scope_recovery_error).__name__),
            'cleanup_scope_freeze_error_type':
                (None if cleanup_scope_freeze_error is None else
                 type(cleanup_scope_freeze_error).__name__),
            'serve_down_error_type': (None if serve_down_error is None else
                                      type(serve_down_error).__name__),
            'cleanup_evidence_error_type':
                (None if cleanup_evidence_error is None else
                 type(cleanup_evidence_error).__name__),
            'cleanup_evidence_required': cleanup_required,
            'serve_up_acknowledged': serve_up_acknowledged,
            'exact_cleanup_proven': exact_cleanup_proven,
            'cleanup_receipt_sha256': validated_cleanup_sha256,
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


async def _complete_cancellation_resistant(
        operation: Awaitable[_T]) -> tuple[_T, asyncio.CancelledError | None]:
    """Finish one owned finalizer while deferring caller cancellation."""
    task = asyncio.create_task(operation)
    deferred_cancellation = None
    while True:
        try:
            result = await asyncio.shield(task)
            return result, deferred_cancellation
        except asyncio.CancelledError as error:
            if deferred_cancellation is None:
                deferred_cancellation = error
            if task.done():
                return task.result(), deferred_cancellation


async def run_lifecycle(args: argparse.Namespace,
                        lifecycle: ServiceLifecycle | None = None) -> None:
    """Run one disposable service and always execute its normal finalizer."""
    timeouts = (args.command_timeout_seconds, args.endpoint_timeout_seconds,
                args.scope_timeout_seconds, args.down_timeout_seconds,
                args.cleanup_timeout_seconds, args.cleanup_zero_hold_seconds,
                args.poll_seconds)
    if any(not math.isfinite(value) or value <= 0 for value in timeouts):
        raise LifecycleError('Lifecycle timeouts must be positive and finite.')
    profile = qualify.PROFILES.get(args.profile)
    if profile is None:
        raise LifecycleError('Qualification profile is invalid.')
    if args.cleanup_zero_hold_seconds != profile.zero_hold_seconds:
        raise LifecycleError(
            'Cleanup hold must equal the qualification profile policy.')
    if args.cleanup_zero_hold_seconds >= args.cleanup_timeout_seconds:
        raise LifecycleError(
            'Cleanup timeout must exceed the exact-zero hold interval.')
    if (args.workspace is not None and
        (not isinstance(args.workspace, str) or not args.workspace)):
        raise LifecycleError('Workspace must be a non-empty string.')
    try:
        endpoint_mode = EndpointMode(args.endpoint_mode)
    except (TypeError, ValueError) as error:
        raise LifecycleError('Qualification endpoint mode is invalid.') \
            from error
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
    endpoint_resolver = _endpoint_resolver(endpoint_mode, lifecycle)
    primary_error: BaseException | None = None
    scope_recovery_error: BaseException | None = None
    cleanup_scope_freeze_error: BaseException | None = None
    serve_down_error: BaseException | None = None
    cleanup_evidence_error: BaseException | None = None
    validated_cleanup_sha256: str | None = None
    lifecycle_owned = False
    serve_up_acknowledged = False
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
        serve_up_acknowledged = True
        await _record_stage(receipt, 'freeze-scope',
                            lambda: qualify.freeze_provider_scope(scope_args))
        endpoint_request = EndpointResolutionRequest(
            service_name=args.service_name,
            provider_scope=artifacts.provider_scope)
        endpoint = await _record_stage(
            receipt, 'endpoint',
            lambda: endpoint_resolver.resolve(endpoint_request))
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

        async def _finalize_owned_lifecycle() -> None:
            nonlocal scope_recovery_error
            nonlocal cleanup_scope_freeze_error
            nonlocal serve_down_error
            nonlocal cleanup_evidence_error
            nonlocal validated_cleanup_sha256
            if lifecycle_owned:
                # A successful create followed by a lost acknowledgement
                # reaches this finalizer before the normal freeze stage.
                # Recover exact provider authority while the service still
                # exists; never invent a scope merely to make cleanup appear
                # successful.
                if not artifacts.provider_scope.exists():
                    try:
                        await _record_stage(
                            receipt, 'freeze-scope-recovery',
                            lambda: qualify.freeze_provider_scope(scope_args))
                    except BaseException as error:  # pylint: disable=broad-exception-caught
                        scope_recovery_error = error
                if artifacts.provider_scope.exists():
                    try:
                        await _record_stage(
                            receipt, 'freeze-cleanup-scope',
                            lambda: qualify.freeze_cleanup_scope(
                                argparse.Namespace(
                                    service_name=args.service_name,
                                    scope=str(artifacts.provider_scope),
                                    receipt=str(artifacts.qualification_receipt
                                               ),
                                    timeout_seconds=args.scope_timeout_seconds,
                                    poll_seconds=args.poll_seconds,
                                    postgres_url_env=args.postgres_url_env)))
                    except BaseException as error:  # pylint: disable=broad-exception-caught
                        cleanup_scope_freeze_error = error
                try:
                    await _record_stage(
                        receipt, 'serve-down',
                        lambda: lifecycle.down(args.service_name))
                except BaseException as error:  # pylint: disable=broad-exception-caught
                    serve_down_error = error
                try:
                    await _record_stage(
                        receipt, 'wait-cleanup',
                        lambda: qualify.wait_for_cleanup(
                            argparse.Namespace(
                                service_name=args.service_name,
                                scope=str(artifacts.provider_scope),
                                receipt=str(artifacts.qualification_receipt),
                                output=str(artifacts.cleanup_receipt),
                                timeout_seconds=args.cleanup_timeout_seconds,
                                zero_hold_seconds=(args.
                                                   cleanup_zero_hold_seconds),
                                poll_seconds=args.poll_seconds,
                                postgres_url_env=args.postgres_url_env)))
                except BaseException as error:  # pylint: disable=broad-exception-caught
                    cleanup_evidence_error = error
                if cleanup_evidence_error is None:
                    try:
                        validated_cleanup_sha256 = await _record_stage(
                            receipt, 'validate-cleanup',
                            lambda: asyncio.to_thread(
                                qualify.validate_lifecycle_cleanup_evidence,
                                cleanup_path=artifacts.cleanup_receipt,
                                qualification_path=(artifacts.
                                                    qualification_receipt),
                                provider_scope_path=artifacts.provider_scope,
                                service_name=args.service_name,
                                profile_name=args.profile))
                    except BaseException as error:  # pylint: disable=broad-exception-caught
                        cleanup_evidence_error = error

        _, deferred_cancellation = await _complete_cancellation_resistant(
            _finalize_owned_lifecycle())
        if primary_error is None and deferred_cancellation is not None:
            primary_error = deferred_cancellation
        receipt.finish(primary_error=primary_error,
                       scope_recovery_error=scope_recovery_error,
                       cleanup_scope_freeze_error=(cleanup_scope_freeze_error),
                       serve_down_error=serve_down_error,
                       cleanup_evidence_error=cleanup_evidence_error,
                       cleanup_required=lifecycle_owned,
                       serve_up_acknowledged=serve_up_acknowledged,
                       validated_cleanup_sha256=validated_cleanup_sha256)
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
    if cleanup_scope_freeze_error is not None:
        error = LifecycleError(
            'Pre-down cleanup identity scope was not frozen; explicit '
            'operator escalation is required.')
        error.__cause__ = cleanup_scope_freeze_error
        finalizer_errors.append(error)
    if serve_down_error is not None:
        error = LifecycleError(
            'Normal sky serve down failed, but exact-zero cleanup was proven.')
        error.__cause__ = serve_down_error
        finalizer_errors.append(error)
    if primary_error is not None and finalizer_errors:
        raise LifecycleFailureGroup(
            'Paid qualification failed and its lifecycle finalizer also '
            'failed; inspect the lifecycle receipt.',
            [primary_error, *finalizer_errors])
    if primary_error is not None:
        raise primary_error
    if len(finalizer_errors) == 1:
        raise finalizer_errors[0]
    if finalizer_errors:
        raise LifecycleFailureGroup(
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
    parser.add_argument('--endpoint-mode',
                        type=EndpointMode,
                        choices=tuple(EndpointMode),
                        default=EndpointMode.PUBLISHED,
                        help=('Endpoint path for qualification traffic. '
                              'Published is the default; in-cluster resolves '
                              'the incarnation-scoped LB Service DNS.'))
    parser.add_argument('--scope-timeout-seconds', type=float, default=5 * 60)
    parser.add_argument('--down-timeout-seconds', type=float, default=5 * 60)
    parser.add_argument('--cleanup-timeout-seconds',
                        type=float,
                        default=30 * 60)
    parser.add_argument('--cleanup-zero-hold-seconds',
                        type=float,
                        default=6 * 60)
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
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
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
