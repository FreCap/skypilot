"""Typed capability contracts for cloud provisioners."""

from __future__ import annotations

import dataclasses
import inspect
import types
import typing
from typing import Any

if typing.TYPE_CHECKING:
    from sky.provision import common
    from sky.provision import TemplateOverrideFn
    from sky.utils import status_lib

INSTANCE_LIFECYCLE_V1_METHODS: typing.Final[tuple[str, ...]] = (
    'query_instances',
    'bootstrap_instances',
    'run_instances',
    'stop_instances',
    'terminate_instances',
    'wait_instances',
    'get_cluster_info',
)


@typing.runtime_checkable
class QueryInstancesFnV1(typing.Protocol):
    """Exact callable contract for one synchronous instance query."""

    # pylint: disable=unnecessary-ellipsis

    def __call__(
        self,
        cluster_name: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
        non_terminated_only: bool = True,
        retry_if_missing: bool = False,
    ) -> dict[str, tuple[status_lib.ClusterStatus | None, str | None]]:
        ...


@typing.runtime_checkable
class InstanceLifecycleV1(typing.Protocol):
    """Synchronous instance lifecycle implemented by new provisioners.

    These signatures mirror the public functions in ``sky.provision`` after
    the facade has consumed ``provider_name``.
    """

    # pylint: disable=unnecessary-ellipsis

    def query_instances(
        self,
        cluster_name: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
        non_terminated_only: bool = True,
        retry_if_missing: bool = False,
    ) -> dict[str, tuple[status_lib.ClusterStatus | None, str | None]]:
        ...

    def bootstrap_instances(
        self,
        region: str,
        cluster_name_on_cloud: str,
        config: common.ProvisionConfig,
    ) -> common.ProvisionConfig:
        ...

    def run_instances(
        self,
        region: str,
        cluster_name: str,
        cluster_name_on_cloud: str,
        config: common.ProvisionConfig,
    ) -> common.ProvisionRecord:
        ...

    def stop_instances(
        self,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any],
        worker_only: bool = False,
    ) -> None:
        ...

    def terminate_instances(
        self,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any],
        worker_only: bool = False,
    ) -> None:
        ...

    def wait_instances(
        self,
        region: str,
        cluster_name_on_cloud: str,
        state: status_lib.ClusterStatus | None,
    ) -> None:
        ...

    def get_cluster_info(
        self,
        region: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
    ) -> common.ClusterInfo:
        ...


@dataclasses.dataclass(frozen=True)
class LegacyInstanceLifecycleAdapter:
    """Typed boundary around a module-shaped legacy provisioner.

    Legacy modules have small signature and annotation differences. Keeping
    those differences behind this adapter lets the strict facet remain exact
    while forwarding the facade's original arguments without transformation.
    """

    module: Any

    def query_instances(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, tuple[status_lib.ClusterStatus | None, str | None]]:
        return self.module.query_instances(*args, **kwargs)

    def bootstrap_instances(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> common.ProvisionConfig:
        return self.module.bootstrap_instances(*args, **kwargs)

    def run_instances(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> common.ProvisionRecord:
        return self.module.run_instances(*args, **kwargs)

    def stop_instances(self, *args: Any, **kwargs: Any) -> None:
        self.module.stop_instances(*args, **kwargs)

    def terminate_instances(self, *args: Any, **kwargs: Any) -> None:
        self.module.terminate_instances(*args, **kwargs)

    def wait_instances(self, *args: Any, **kwargs: Any) -> None:
        self.module.wait_instances(*args, **kwargs)

    def get_cluster_info(self, *args: Any, **kwargs: Any) -> common.ClusterInfo:
        return self.module.get_cluster_info(*args, **kwargs)


@dataclasses.dataclass(frozen=True)
class BuiltinQueryInstancesDiagnosticV1:
    """One in-tree query entry point with its authoritative identity."""

    authoritative_implementation: QueryInstancesFnV1
    diagnostic_implementation: QueryInstancesFnV1


@dataclasses.dataclass(frozen=True)
class ProvisionerBundleV1:
    """One immutable owner for the V1 instance lifecycle facet."""

    canonical_name: str
    instance_lifecycle: InstanceLifecycleV1
    template_override: TemplateOverrideFn | None = None
    legacy_module: Any | None = None
    builtin_query_instances_diagnostic: (BuiltinQueryInstancesDiagnosticV1 |
                                         None) = None


def _callable_v1_validation_error(
    implementation: Any,
    protocol_implementation: Any,
    actual_signature: inspect.Signature | None = None,
) -> str | None:
    """Return one callable-shape error, or None when it is compatible."""
    if not callable(implementation):
        return 'missing or not callable'
    if (inspect.iscoroutinefunction(implementation) or
            inspect.isasyncgenfunction(implementation)):
        return 'must be synchronous'
    expected_parameters = tuple(
        inspect.signature(protocol_implementation).parameters.values())[
            1:]  # Drop protocol ``self``.
    if actual_signature is None:
        try:
            actual_signature = inspect.signature(implementation)
        except (TypeError, ValueError) as exception:
            return f'signature unavailable: {exception}'
    actual_parameters = tuple(actual_signature.parameters.values())
    if (len(actual_parameters) == len(expected_parameters) and all(
            actual.name == expected.name and actual.kind is expected.kind and
        (actual.default is expected.default or (type(actual.default) is type(
            expected.default) and actual.default == expected.default))
            for actual, expected in zip(actual_parameters, expected_parameters))
       ):
        return None
    expected_signature = inspect.Signature(expected_parameters)
    actual_signature = inspect.Signature(actual_parameters)
    return f'expected {expected_signature}, got {actual_signature}'


def _exact_builtin_query_function_validation_error(
        implementation: Any) -> str | None:
    """Reject every built-in diagnostic shape except a bare Python function."""
    if type(implementation) is not types.FunctionType:
        return 'must be an exact Python function'
    missing = object()
    if inspect.getattr_static(implementation, '__wrapped__',
                              missing) is not missing:
        return 'must be undecorated'
    return None


def _code_derived_function_signature(
        implementation: types.FunctionType) -> inspect.Signature:
    """Return a signature without consulting writable inspection metadata."""
    clean_function = types.FunctionType(
        implementation.__code__,
        implementation.__globals__,
        implementation.__name__,
        implementation.__defaults__,
        implementation.__closure__,
    )
    clean_function.__kwdefaults__ = implementation.__kwdefaults__
    return inspect.signature(clean_function, follow_wrapped=False)


def instance_lifecycle_v1_validation_errors(lifecycle: Any) -> tuple[str, ...]:
    """Return missing, non-callable, or signature-incompatible V1 methods."""
    errors = []
    for method_name in INSTANCE_LIFECYCLE_V1_METHODS:
        implementation = getattr(lifecycle, method_name, None)
        error = _callable_v1_validation_error(
            implementation, getattr(InstanceLifecycleV1, method_name))
        if error is not None:
            errors.append(f'{method_name}: {error}')
    return tuple(errors)


def builtin_query_instances_diagnostic_v1_validation_errors(
    diagnostic: BuiltinQueryInstancesDiagnosticV1,) -> tuple[str, ...]:
    """Return callable-shape errors for a built-in query diagnostic."""
    errors = []
    for field_name in ('authoritative_implementation',
                       'diagnostic_implementation'):
        implementation = getattr(diagnostic, field_name)
        error = _exact_builtin_query_function_validation_error(implementation)
        if error is None:
            try:
                actual_signature = _code_derived_function_signature(
                    implementation)
            except (TypeError, ValueError) as exception:
                error = f'signature unavailable: {exception}'
            else:
                error = _callable_v1_validation_error(
                    implementation,
                    QueryInstancesFnV1.__call__,
                    actual_signature=actual_signature,
                )
        if error is not None:
            errors.append(f'{field_name}: {error}')
    return tuple(errors)


__all__ = [
    'BuiltinQueryInstancesDiagnosticV1',
    'INSTANCE_LIFECYCLE_V1_METHODS',
    'InstanceLifecycleV1',
    'LegacyInstanceLifecycleAdapter',
    'ProvisionerBundleV1',
    'QueryInstancesFnV1',
    'builtin_query_instances_diagnostic_v1_validation_errors',
    'instance_lifecycle_v1_validation_errors',
]
