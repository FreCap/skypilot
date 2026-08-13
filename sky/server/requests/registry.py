"""Stable registries for durable API request execution."""

from __future__ import annotations

from collections.abc import Callable
import dataclasses
import enum
import importlib
import inspect
import threading
from typing import Any

import orjson

from sky.server.requests import payloads


class ExecutionClass(enum.Enum):
    """Worker class allowed to claim a request."""

    NORMAL = 'normal'
    CONTROLLER = 'controller'


class HandlerClaimScope(enum.Enum):
    """Deprecated compatibility surface for released request plugins.

    The dedicated authority claimant is retired.  ``GENERAL`` remains so
    plugins written against released SkyPilot versions continue to register;
    every other scope is rejected by :func:`register_handler`.
    """

    GENERAL = 'general'
    RESOURCE_ACTION_AUTHORITY = 'resource_action_authority'


class ReplayPolicy(enum.Enum):
    """Policy applied after an execution owner is lost."""

    NEVER = 'never'
    RECONCILE = 'reconcile'
    READ_ONLY = 'read_only'


class CancellationPolicy(enum.Enum):
    """How cancellation is delivered to an execution owner."""

    FENCED_PROCESS = 'fenced_process'
    COOPERATIVE = 'cooperative'


@dataclasses.dataclass(frozen=True)
class HandlerRegistration:
    """Stable metadata for one executable request handler."""

    name: str
    func: Callable[..., Any]
    execution_class: ExecutionClass
    replay_policy: ReplayPolicy
    cancellation_policy: CancellationPolicy
    claim_scope: HandlerClaimScope = HandlerClaimScope.GENERAL
    aliases: tuple[str, ...] = ()


_BUILTIN_HANDLER_MODULES = (
    'sky.catalog',
    'sky.check',
    'sky.core',
    'sky.execution',
    'sky.jobs.server.core',
    'sky.provision.kubernetes.utils',
    'sky.provision.slurm.utils',
    'sky.recipes.core',
    'sky.serve.server.core',
    'sky.server.requests.ordinary_launch',
    'sky.server.requests.requests',
    'sky.ssh_node_pools.core',
    'sky.utils.kubernetes.gpu_labeler',
    'sky.volumes.server.core',
    'sky.workspaces.core',
)

_CONTROLLER_HANDLER_MODULES = frozenset({
    'sky.jobs.server.core',
    'sky.serve.server.core',
})

_READ_ONLY_HANDLER_NAMES = frozenset({
    'sky.catalog:list_accelerator_counts',
    'sky.catalog:list_accelerators',
    'sky.check:check',
    'sky.core:cost_report',
    'sky.core:download_logs',
    'sky.core:enabled_clouds',
    'sky.core:enabled_clouds_batch',
    'sky.core:endpoints',
    'sky.core:get_all_contexts',
    'sky.core:get_cluster_events',
    'sky.core:job_status',
    'sky.core:optimize',
    'sky.core:queue',
    'sky.core:realtime_kubernetes_gpu_availability',
    'sky.core:realtime_slurm_gpu_availability',
    'sky.core:status',
    'sky.core:status_kubernetes',
    'sky.core:storage_ls',
    'sky.core:tail_hook_logs',
    'sky.core:tail_logs',
    'sky.jobs.server.core:download_logs',
    'sky.jobs.server.core:get_job_events',
    'sky.jobs.server.core:pool_status',
    'sky.jobs.server.core:pool_sync_down_logs',
    'sky.jobs.server.core:pool_tail_logs',
    'sky.jobs.server.core:queue',
    'sky.jobs.server.core:queue_v2_api',
    'sky.jobs.server.core:tail_logs',
    'sky.jobs.server.core:wait',
    'sky.provision.kubernetes.utils:get_kubernetes_node_info',
    'sky.provision.slurm.utils:slurm_node_info',
    'sky.recipes.core:get_recipe',
    'sky.recipes.core:list_recipes',
    'sky.serve.server.core:placement',
    'sky.serve.server.core:status',
    'sky.serve.server.core:sync_down_logs',
    'sky.serve.server.core:tail_logs',
    'sky.volumes.server.core:volume_list',
    'sky.workspaces.core:get_config',
    'sky.workspaces.core:get_workspaces',
})

_HANDLERS: dict[str, HandlerRegistration] = {}
_HANDLER_NAMES_BY_IDENTITY: dict[int, str] = {}
_PAYLOAD_TYPES: dict[str, type[payloads.RequestBody]] = {}
_REGISTRY_LOCK = threading.RLock()
_BUILTINS_REGISTERED = False
_BUILTINS_REGISTRATION_IN_PROGRESS = False


def _handler_name(func: Callable[..., Any]) -> str:
    module = getattr(func, '__module__', None)
    qualname = getattr(func, '__qualname__', None)
    if not module or not qualname or '<locals>' in qualname:
        raise ValueError(
            'Durable request handlers must be importable module-level '
            f'callables, got {func!r}.')
    return f'{module}:{qualname}'


def register_handler(
        func: Callable[..., Any],
        *,
        name: str | None = None,
        execution_class: ExecutionClass = ExecutionClass.NORMAL,
        replay_policy: ReplayPolicy = ReplayPolicy.NEVER,
        cancellation_policy: CancellationPolicy = (
            CancellationPolicy.FENCED_PROCESS),
        claim_scope: HandlerClaimScope = HandlerClaimScope.GENERAL,
        aliases: tuple[str, ...] = (),
) -> HandlerRegistration:
    """Register one durable handler and reject conflicting identities."""
    if claim_scope is not HandlerClaimScope.GENERAL:
        raise ValueError('Non-general handler claim scopes have been retired.')
    stable_name = name or _handler_name(func)
    registration = HandlerRegistration(
        name=stable_name,
        func=func,
        execution_class=execution_class,
        replay_policy=replay_policy,
        cancellation_policy=cancellation_policy,
        claim_scope=claim_scope,
        aliases=aliases,
    )
    with _REGISTRY_LOCK:
        for registered_name in (stable_name, *aliases):
            existing = _HANDLERS.get(registered_name)
            if existing is not None and existing != registration:
                raise ValueError(
                    f'Durable request handler name {registered_name!r} is '
                    'already registered with different metadata.')
            _HANDLERS[registered_name] = registration
        existing_name = _HANDLER_NAMES_BY_IDENTITY.get(id(func))
        if existing_name is not None and existing_name != stable_name:
            raise ValueError(f'Handler {func!r} is already registered as '
                             f'{existing_name!r}, not {stable_name!r}.')
        _HANDLER_NAMES_BY_IDENTITY[id(func)] = stable_name
    return registration


def _register_module_handlers(module_name: str) -> None:
    module = importlib.import_module(module_name)
    execution_class = (ExecutionClass.CONTROLLER
                       if module_name in _CONTROLLER_HANDLER_MODULES else
                       ExecutionClass.NORMAL)
    for attribute_name, value in vars(module).items():
        if not inspect.isfunction(value):
            continue
        if value.__module__ != module_name:
            continue
        if attribute_name.startswith('_'):
            continue
        stable_name = _handler_name(value)
        replay_policy = (ReplayPolicy.READ_ONLY if stable_name
                         in _READ_ONLY_HANDLER_NAMES else ReplayPolicy.NEVER)
        register_handler(value,
                         execution_class=execution_class,
                         replay_policy=replay_policy)


def register_builtin_handlers() -> None:
    """Populate the closed registry of built-in request handlers."""
    global _BUILTINS_REGISTERED, _BUILTINS_REGISTRATION_IN_PROGRESS
    if _BUILTINS_REGISTERED:
        return
    with _REGISTRY_LOCK:
        if (_BUILTINS_REGISTERED or _BUILTINS_REGISTRATION_IN_PROGRESS):
            return
        _BUILTINS_REGISTRATION_IN_PROGRESS = True
        try:
            for module_name in _BUILTIN_HANDLER_MODULES:
                _register_module_handlers(module_name)
            _register_payload_types()
            _BUILTINS_REGISTERED = True
        finally:
            _BUILTINS_REGISTRATION_IN_PROGRESS = False


def registration_for_handler(func: Callable[..., Any]) -> HandlerRegistration:
    """Return registered metadata for a callable used by a request."""
    register_builtin_handlers()
    with _REGISTRY_LOCK:
        name = _HANDLER_NAMES_BY_IDENTITY.get(id(func))
        if name is not None:
            return _HANDLERS[name]
    raise ValueError(
        f'Request handler {_handler_name(func)!r} is not registered. Core '
        'handlers must live in an explicitly allowed module; plugins must '
        'register handlers during MAIN and EXECUTOR loading.')


def resolve_handler(name: str) -> HandlerRegistration:
    """Resolve a stable handler name without importing row-controlled code."""
    register_builtin_handlers()
    with _REGISTRY_LOCK:
        registration = _HANDLERS.get(name)
    if registration is None:
        raise ValueError(f'Unknown durable request handler {name!r}.')
    return registration


def _payload_type_name(payload_type: type[payloads.RequestBody]) -> str:
    return f'{payload_type.__module__}:{payload_type.__qualname__}'


def register_payload_type(
        payload_type: type[payloads.RequestBody],
        *,
        name: str | None = None,
        aliases: tuple[str, ...] = (),
) -> str:
    """Register one Pydantic request-body type."""
    if not issubclass(payload_type, payloads.RequestBody):
        raise TypeError(f'{payload_type!r} is not a RequestBody subclass.')
    stable_name = name or _payload_type_name(payload_type)
    with _REGISTRY_LOCK:
        for registered_name in (stable_name, *aliases):
            existing = _PAYLOAD_TYPES.get(registered_name)
            if existing is not None and existing is not payload_type:
                raise ValueError(
                    f'Durable payload type {registered_name!r} is already '
                    'registered with a different class.')
            _PAYLOAD_TYPES[registered_name] = payload_type
    return stable_name


def _request_body_subclasses() -> list[type[payloads.RequestBody]]:
    pending = list(payloads.RequestBody.__subclasses__())
    result: list[type[payloads.RequestBody]] = []
    while pending:
        payload_type = pending.pop()
        result.append(payload_type)
        pending.extend(payload_type.__subclasses__())
    return result


def _register_payload_types() -> None:
    register_payload_type(payloads.RequestBody)
    for payload_type in _request_body_subclasses():
        register_payload_type(payload_type)


def encode_payload(body: payloads.RequestBody) -> tuple[str, dict[str, Any]]:
    """Encode a request body into a registered JSON object."""
    register_builtin_handlers()
    payload_type = type(body)
    name = _payload_type_name(payload_type)
    with _REGISTRY_LOCK:
        registered_type = _PAYLOAD_TYPES.get(name)
    if registered_type is not payload_type:
        raise ValueError(f'Request payload type {name!r} is not registered.')
    encoded = orjson.loads(body.model_dump_json())
    if not isinstance(encoded, dict):
        raise ValueError(
            f'Request payload {name!r} did not encode as an object.')
    return name, encoded


def decode_payload(name: str, value: dict[str, Any]) -> payloads.RequestBody:
    """Decode a JSON object through the closed payload registry."""
    register_builtin_handlers()
    with _REGISTRY_LOCK:
        payload_type = _PAYLOAD_TYPES.get(name)
    if payload_type is None:
        raise ValueError(f'Unknown durable request payload type {name!r}.')
    return payload_type.model_validate(value)


def registered_handlers() -> tuple[HandlerRegistration, ...]:
    """Return unique registrations for tests and compatibility adverts."""
    register_builtin_handlers()
    with _REGISTRY_LOCK:
        return tuple({
            registration.name: registration
            for registration in _HANDLERS.values()
        }.values())
