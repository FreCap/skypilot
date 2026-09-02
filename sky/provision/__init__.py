"""Cloud provision interface.

This module provides a standard low-level interface that all
providers supported by SkyPilot need to follow.
"""
import dataclasses
import dis
import enum
import functools
import inspect
import math
import types
from types import MappingProxyType
import typing
from typing import Any, Optional, Protocol

from sky import models
from sky import sky_logging
# These provision.<cloud> modules should never fail even if underlying cloud SDK
# dependencies are not installed. This is ensured by using sky.adaptors inside
# these modules, for lazy loading of cloud SDKs.
from sky.provision import aws
from sky.provision import azure
from sky.provision import common
from sky.provision import cudo
from sky.provision import do
from sky.provision import fluidstack
from sky.provision import gcp
from sky.provision import hyperbolic
from sky.provision import kubernetes
from sky.provision import lambda_cloud
from sky.provision import mithril
from sky.provision import nebius
from sky.provision import oci
from sky.provision import paperspace
from sky.provision import primeintellect
from sky.provision import provider_facets
from sky.provision import runpod
from sky.provision import scp
from sky.provision import seeweb
from sky.provision import shadeform
from sky.provision import slurm
from sky.provision import ssh
from sky.provision import vast
from sky.provision import verda
from sky.provision import vsphere
from sky.provision import yotta
from sky.utils import command_runner
from sky.utils import provider_registration
from sky.utils import registry as registry_lib
from sky.utils import timeline

if typing.TYPE_CHECKING:
    from sky import resources as resources_lib
    from sky import task as task_lib
    from sky.provision import provider_registry_audit
    from sky.utils import status_lib

logger = sky_logging.init_logger(__name__)


@dataclasses.dataclass
class TemplateSpec:
    """A cluster config template path plus extra variables.

    Cluster config templates are the Jinja-rendered per-cloud YAMLs
    under ``sky/templates/`` (e.g. ``aws-ray.yml.j2``) that drive
    provisioning. See ``sky.backends.backend_utils.write_cluster_config``.

    ``template_path`` is either an absolute path (for plugin-shipped
    templates) or a bare filename relative to ``sky/templates/`` (for
    in-tree templates). ``variables`` are merged into the standard
    template variables when the template is rendered.
    """
    template_path: str
    variables: dict[str, Any] = dataclasses.field(default_factory=dict)


class TemplateOverrideFn(Protocol):
    """Callable signature for ``Provisioner.template_override``.

    Called by the backend at launch time. Return a ``TemplateSpec`` to
    use a custom cluster config template instead of the cloud's
    default (``_get_cluster_config_template(cloud)`` in
    ``cloud_vm_ray_backend``), or ``None`` to use the cloud's default.

    ``to_provision`` is the resource currently being launched — the same
    one passed into ``Cloud.make_deploy_resources_variables``. Its
    ``region`` is guaranteed non-None (set by the optimizer and asserted
    in the backend before dispatch), so implementations can read it
    directly instead of fishing it out of ``task.best_resources`` or
    the user's pre-optimizer ``task.resources`` alternatives list. The
    backend's failover loop may swap ``to_provision`` between retries;
    ``template_override`` is re-invoked with the current attempt.
    """

    # pylint: disable=unnecessary-ellipsis

    def __call__(
        self,
        task: 'task_lib.Task',
        to_provision: 'resources_lib.Resources',
        *,
        _extra_launch_context: dict[str, Any],
        _is_launched_by_jobs_controller: bool,
    ) -> TemplateSpec | None:
        ...


@dataclasses.dataclass
class Provisioner:
    """Registered provisioner for a cloud.

    ``module`` is a module-shaped object (typically a Python module,
    but any object with the relevant attributes works) providing the
    routed lifecycle functions: ``run_instances``,
    ``terminate_instances``, ``query_instances``, etc. Plugin authors
    look at any built-in cloud module (e.g.
    ``sky/provision/aws.py``, ``sky/provision/kubernetes/__init__.py``)
    for the canonical shape.

    ``template_override`` is an optional hook called at launch time —
    outside the routed lifecycle dispatch — that lets the plugin
    redirect a task to a custom Jinja template + extra variables.
    """
    module: Any
    template_override: TemplateOverrideFn | None = None


_registered_provisioners: dict[str, Provisioner] = {}
_registered_provisioner_bundles: dict[str,
                                      provider_facets.ProvisionerBundleV1] = {}
_legacy_mixed_owner_diagnostics: set[tuple[str, str]] = set()


def _make_builtin_bundle(
    canonical_name: str,
    module: Any,
) -> provider_facets.ProvisionerBundleV1:
    diagnostic = None
    try:
        candidate = inspect.getattr_static(module,
                                           '_QUERY_INSTANCES_DIAGNOSTIC_V1',
                                           None)
        if (type(candidate) is provider_facets.BuiltinQueryInstancesDiagnosticV1
                and not provider_facets.
                builtin_query_instances_diagnostic_v1_validation_errors(
                    candidate)):
            diagnostic = candidate
    except Exception:  # pylint: disable=broad-exception-caught
        # An optional diagnostic must not break authoritative provisioning.
        diagnostic = None
    return provider_facets.ProvisionerBundleV1(
        canonical_name=canonical_name,
        instance_lifecycle=provider_facets.LegacyInstanceLifecycleAdapter(
            module),
        legacy_module=module,
        builtin_query_instances_diagnostic=diagnostic,
    )


# One explicit inventory owns the in-tree new-provisioner implementations.
# Late-bound getters preserve whole-module and attribute monkeypatch seams while
# avoiding the former import-order-dependent ``globals()`` discovery.
_BUILTIN_PROVISIONER_MODULE_GETTERS: dict[str, typing.Callable[[], Any]] = {
    'aws': lambda: aws,
    'azure': lambda: azure,
    'cudo': lambda: cudo,
    'do': lambda: do,
    'fluidstack': lambda: fluidstack,
    'gcp': lambda: gcp,
    'hyperbolic': lambda: hyperbolic,
    'kubernetes': lambda: kubernetes,
    'lambda': lambda: lambda_cloud,
    'mithril': lambda: mithril,
    'nebius': lambda: nebius,
    'oci': lambda: oci,
    'paperspace': lambda: paperspace,
    'primeintellect': lambda: primeintellect,
    'runpod': lambda: runpod,
    'scp': lambda: scp,
    'seeweb': lambda: seeweb,
    'shadeform': lambda: shadeform,
    'slurm': lambda: slurm,
    'ssh': lambda: ssh,
    'vast': lambda: vast,
    'verda': lambda: verda,
    'vsphere': lambda: vsphere,
    'yotta': lambda: yotta,
}


class _BuiltinProvisionerAuditExpectation(typing.NamedTuple):
    """Sealed direct-global getter shape used only by registry auditing."""

    getter: typing.Callable[[], Any]
    module: Any
    function_type: type
    code: types.CodeType
    defaults: tuple[Any, ...] | None
    keyword_defaults: dict[str, Any] | None
    closure: tuple[types.CellType, ...] | None
    globals_mapping: dict[str, Any]
    global_name: str


def _seal_builtin_provisioner_getter(
    getter: typing.Callable[[], Any],) -> _BuiltinProvisionerAuditExpectation:
    """Seal one trusted zero-argument direct-global getter without calling it."""
    if type(getter) is not types.FunctionType:
        raise TypeError(
            'Built-in provisioner getter must be a Python function.')
    code = getter.__code__
    significant_instructions = tuple(
        instruction for instruction in dis.get_instructions(code)
        if instruction.opname not in ('CACHE', 'NOP', 'RESUME'))
    if (code.co_argcount != 0 or code.co_posonlyargcount != 0 or
            code.co_kwonlyargcount != 0 or getter.__defaults__ is not None or
            getter.__kwdefaults__ is not None or
            getter.__closure__ is not None or
            len(significant_instructions) != 2 or
            significant_instructions[0].opname != 'LOAD_GLOBAL' or
            significant_instructions[1].opname != 'RETURN_VALUE' or
            type(significant_instructions[0].argval) is not str):
        raise ValueError('Built-in provisioner getter must be a direct-global '
                         'zero-argument function.')
    global_name = significant_instructions[0].argval
    globals_mapping = getter.__globals__
    module = dict.get(globals_mapping, global_name)
    if module is None:
        raise ValueError('Built-in provisioner getter global must be present.')
    return _BuiltinProvisionerAuditExpectation(
        getter=getter,
        module=module,
        function_type=types.FunctionType,
        code=code,
        defaults=getter.__defaults__,
        keyword_defaults=getter.__kwdefaults__,
        closure=getter.__closure__,
        globals_mapping=globals_mapping,
        global_name=global_name,
    )


# Audit-only expectations are sealed before server plugins can replace a
# built-in inventory getter. Audit capture may read only these exact
# allowlisted direct-global bindings and never executes a getter.
_BUILTIN_PROVISIONER_AUDIT_BASELINE = MappingProxyType({
    canonical_name: _seal_builtin_provisioner_getter(module_getter)
    for canonical_name, module_getter in
    _BUILTIN_PROVISIONER_MODULE_GETTERS.items()
})


def _canonical_provider_name(provider_name: str) -> str:
    """Return the one canonical key used by registration and dispatch."""
    normalized = provider_name.lower()
    if normalized == 'lambda_cloud':
        return 'lambda'
    return normalized


def _get_builtin_provisioner_bundle(
        provider_name: str) -> provider_facets.ProvisionerBundleV1 | None:
    canonical_name = _canonical_provider_name(provider_name)
    module_getter = _BUILTIN_PROVISIONER_MODULE_GETTERS.get(canonical_name)
    if module_getter is None:
        return None
    return _make_builtin_bundle(canonical_name, module_getter())


@enum.unique
class _ProvisionerOperationOwnerV1(enum.Enum):
    """Source that owns one resolved provider operation."""

    STRICT = 'strict'
    LEGACY = 'legacy'
    BUILTIN = 'builtin'


@dataclasses.dataclass(frozen=True)
class _ResolvedProvisionerOperation:
    """One selected operation and its optional diagnostic entry point."""

    owner: _ProvisionerOperationOwnerV1
    authoritative_implementation: Any
    diagnostic_implementation: Any | None = None

    @property
    def implementation(self) -> Any:
        if self.diagnostic_implementation is not None:
            return self.diagnostic_implementation
        return self.authoritative_implementation


_BUILTIN_LIFECYCLE_DISCARD_RETURN_METHODS: typing.Final[frozenset[str]] = (
    frozenset(('stop_instances', 'terminate_instances', 'wait_instances')))


def _pin_builtin_lifecycle_implementation(method_name: str,
                                          implementation: Any) -> Any:
    """Pin one raw built-in method while preserving adapter return semantics."""
    if method_name not in _BUILTIN_LIFECYCLE_DISCARD_RETURN_METHODS:
        return implementation

    def _discard_return(*args: Any, **kwargs: Any) -> None:
        implementation(*args, **kwargs)

    return _discard_return


@dataclasses.dataclass(frozen=True)
class _ProvisionerResolution:
    """One provider lookup shared by lifecycle and template dispatch."""

    canonical_name: str
    strict_bundle: provider_facets.ProvisionerBundleV1 | None
    legacy_registration: Provisioner | None
    builtin_bundle: provider_facets.ProvisionerBundleV1 | None

    @property
    def exists(self) -> bool:
        return (self.strict_bundle is not None or
                self.legacy_registration is not None or
                self.builtin_bundle is not None)

    @property
    def template_override(self) -> TemplateOverrideFn | None:
        if self.strict_bundle is not None:
            return self.strict_bundle.template_override
        if (self.legacy_registration is not None and
                self.legacy_registration.template_override is not None):
            return self.legacy_registration.template_override
        if self.builtin_bundle is not None:
            return self.builtin_bundle.template_override
        return None

    def resolve_operation(
            self, method_name: str) -> _ResolvedProvisionerOperation | None:
        if (self.strict_bundle is not None and
                method_name in provider_facets.INSTANCE_LIFECYCLE_V1_METHODS):
            return _ResolvedProvisionerOperation(
                owner=_ProvisionerOperationOwnerV1.STRICT,
                authoritative_implementation=getattr(
                    self.strict_bundle.instance_lifecycle, method_name),
            )

        legacy_module = (self.legacy_registration.module
                         if self.legacy_registration is not None else None)
        if legacy_module is not None:
            implementation = getattr(legacy_module, method_name, None)
            if implementation is not None:
                return _ResolvedProvisionerOperation(
                    owner=_ProvisionerOperationOwnerV1.LEGACY,
                    authoritative_implementation=implementation,
                )

        if self.builtin_bundle is None:
            return None
        raw_builtin_module = self.builtin_bundle.legacy_module
        if raw_builtin_module is None:
            return None
        raw_builtin_implementation = getattr(raw_builtin_module, method_name,
                                             None)
        if raw_builtin_implementation is None:
            return None

        if (legacy_module is not None and
                method_name in provider_facets.INSTANCE_LIFECYCLE_V1_METHODS):
            plugin_owns_part_of_facet = any(
                callable(getattr(legacy_module, name, None))
                for name in provider_facets.INSTANCE_LIFECYCLE_V1_METHODS)
            diagnostic_key = (self.canonical_name, 'InstanceLifecycleV1')
            if (plugin_owns_part_of_facet and
                    diagnostic_key not in _legacy_mixed_owner_diagnostics):
                _legacy_mixed_owner_diagnostics.add(diagnostic_key)
                logger.warning(
                    'Legacy provisioner %r mixes plugin and built-in '
                    'InstanceLifecycleV1 methods. Register one complete '
                    'ProvisionerBundleV1 to remove fallback.',
                    self.canonical_name)

        authoritative_implementation = raw_builtin_implementation
        if method_name in provider_facets.INSTANCE_LIFECYCLE_V1_METHODS:
            authoritative_implementation = (
                _pin_builtin_lifecycle_implementation(
                    method_name, raw_builtin_implementation))

        diagnostic_implementation = None
        diagnostic = self.builtin_bundle.builtin_query_instances_diagnostic
        if (method_name == 'query_instances' and
                self.legacy_registration is None and type(diagnostic)
                is provider_facets.BuiltinQueryInstancesDiagnosticV1 and
                diagnostic.authoritative_implementation
                is raw_builtin_implementation):
            diagnostic_implementation = diagnostic.diagnostic_implementation

        return _ResolvedProvisionerOperation(
            owner=_ProvisionerOperationOwnerV1.BUILTIN,
            authoritative_implementation=authoritative_implementation,
            diagnostic_implementation=diagnostic_implementation,
        )

    def resolve_instance_status_inventory(self) -> Any | None:
        """Resolve the optional batch observer without mixed-owner fallback."""
        if self.strict_bundle is not None:
            owner: Any = self.strict_bundle.instance_lifecycle
            return (owner if callable(
                getattr(owner, 'query_instances_batch', None)) else None)
        if self.legacy_registration is not None:
            owner = self.legacy_registration.module
            return (owner if callable(
                getattr(owner, 'query_instances_batch', None)) else None)
        if self.builtin_bundle is None:
            return None
        owner = self.builtin_bundle.legacy_module
        return (owner if owner is not None and callable(
            getattr(owner, 'query_instances_batch', None)) else None)


def _resolve_provisioner(provider_name: str) -> _ProvisionerResolution:
    canonical_name = _canonical_provider_name(provider_name)
    return _ProvisionerResolution(
        canonical_name=canonical_name,
        strict_bundle=_registered_provisioner_bundles.get(canonical_name),
        legacy_registration=_registered_provisioners.get(canonical_name),
        builtin_bundle=_get_builtin_provisioner_bundle(canonical_name),
    )


def register_provisioner(
    cloud_name: str,
    module: Any,
    *,
    template_override: TemplateOverrideFn | None = None,
) -> None:
    """Register a Provisioner under a cloud name. Last registration wins.

    Plugins call this in their ``install()`` phase. ``cloud_name``
    matches the lowercase canonical cloud name (e.g. ``'kubernetes'``,
    ``'aws'``).
    """
    canonical_name = _canonical_provider_name(cloud_name)
    with provider_registration.provider_registration_mutation():
        _registered_provisioner_bundles.pop(canonical_name, None)
        _registered_provisioners[canonical_name] = Provisioner(
            module=module, template_override=template_override)
    logger.debug(
        'Registered Provisioner for %r: module=%s, '
        'template_override=%s', canonical_name,
        type(module).__name__, template_override is not None)


def get_registered_provisioner(cloud_name: str) -> Provisioner | None:
    """Return the Provisioner registered for ``cloud_name``, or None."""
    canonical_name = _canonical_provider_name(cloud_name)
    legacy_registration = _registered_provisioners.get(canonical_name)
    if legacy_registration is not None:
        return legacy_registration
    strict_bundle = _registered_provisioner_bundles.get(canonical_name)
    if strict_bundle is None:
        return None
    return Provisioner(module=strict_bundle.instance_lifecycle,
                       template_override=strict_bundle.template_override)


def register_provisioner_bundle(
        bundle: provider_facets.ProvisionerBundleV1) -> None:
    """Register one complete, strictly owned V1 lifecycle facet."""
    if bundle.builtin_query_instances_diagnostic is not None:
        raise ValueError('Strict ProvisionerBundleV1 registration cannot '
                         'declare a built-in query diagnostic.')
    validation_errors = (
        provider_facets.instance_lifecycle_v1_validation_errors(
            bundle.instance_lifecycle))
    if validation_errors:
        raise ValueError('Incomplete InstanceLifecycleV1 for '
                         f'{bundle.canonical_name!r}: '
                         f'{"; ".join(validation_errors)}.')
    if bundle.legacy_module is not None:
        raise ValueError('Strict ProvisionerBundleV1 registration cannot '
                         'declare a legacy module fallback.')

    canonical_name = _canonical_provider_name(bundle.canonical_name)
    if bundle.canonical_name != canonical_name:
        bundle = dataclasses.replace(bundle, canonical_name=canonical_name)
    existing_bundle = _registered_provisioner_bundles.get(canonical_name)
    if (existing_bundle is not None and
            existing_bundle.instance_lifecycle is bundle.instance_lifecycle and
            existing_bundle.template_override is bundle.template_override and
            existing_bundle.legacy_module is bundle.legacy_module and
            existing_bundle.builtin_query_instances_diagnostic
            is bundle.builtin_query_instances_diagnostic):
        logger.debug('ProvisionerBundleV1 for %r is already registered.',
                     canonical_name)
        return
    with provider_registration.provider_registration_mutation():
        _registered_provisioners.pop(canonical_name, None)
        replaced = existing_bundle is not None
        _registered_provisioner_bundles[canonical_name] = bundle
    if replaced:
        logger.info('Replaced strict ProvisionerBundleV1 for %r.',
                    canonical_name)
    else:
        logger.debug('Registered strict ProvisionerBundleV1 for %r.',
                     canonical_name)


def get_provisioner_bundle(
        provider_name: str) -> provider_facets.ProvisionerBundleV1 | None:
    """Return the strict or built-in typed bundle for a provider."""
    resolution = _resolve_provisioner(provider_name)
    if resolution.strict_bundle is not None:
        return resolution.strict_bundle
    return resolution.builtin_bundle


def get_provisioner_template_override(
        provider_name: str) -> TemplateOverrideFn | None:
    """Return the template hook chosen by the shared provider resolver."""
    return _resolve_provisioner(provider_name).template_override


def _route_to_cloud_impl(func):

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        # check the signature to fail early
        inspect.signature(func).bind(*args, **kwargs)
        if args:
            provider_name = args[0]
            args = args[1:]
        else:
            provider_name = kwargs.pop('provider_name')

        resolution = _resolve_provisioner(provider_name)
        assert resolution.exists, (
            f'Unknown provider: {resolution.canonical_name}')
        operation = resolution.resolve_operation(func.__name__)

        if operation is not None:
            return operation.implementation(*args, **kwargs)

        # Neither side implements it — fall back to the decorator's default
        # body (typically ``raise NotImplementedError``).
        return func(provider_name, *args, **kwargs)

    return _wrapper


# pylint: disable=unused-argument


@timeline.event
@_route_to_cloud_impl
def query_instances(
    provider_name: str,
    cluster_name: str,
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any] | None = None,
    non_terminated_only: bool = True,
    retry_if_missing: bool = False,
) -> dict[str, tuple[Optional['status_lib.ClusterStatus'], str | None]]:
    """Query instances.

    Returns a dictionary of instance IDs and a tuple of (status, reason for
    being in status if any).

    A None status means the instance is marked as "terminated"
    or "terminating".

    Args:
        retry_if_missing: Whether to retry the call to the cloud api if the
          cluster is not found when querying the live status on the cloud.
          NOTE: This is currently only used on kubernetes.
    """
    raise NotImplementedError


def _unknown_instance_status_inventory(
    queries: tuple[provider_facets.InstanceStatusInventoryQueryV1, ...],
    error: str,
) -> tuple[provider_facets.InstanceStatusInventoryObservationV1, ...]:
    """Return fail-closed UNKNOWN results for one exact input batch."""
    return tuple(
        provider_facets.InstanceStatusInventoryObservationV1(
            query_id=query.query_id,
            disposition=(
                provider_facets.InstanceStatusInventoryDispositionV1.UNKNOWN),
            error=error) for query in queries)


def query_instances_batch(
    provider_name: str,
    queries: tuple[provider_facets.InstanceStatusInventoryQueryV1, ...],
    *,
    deadline_monotonic: float,
) -> tuple[provider_facets.InstanceStatusInventoryObservationV1, ...]:
    """Read one bounded provider inventory without singleton fallback.

    Providers may partition the immutable batch by credential authority and
    location.  An old provider without this optional capability yields UNKNOWN
    for every query; this function must never hide that gap behind N calls to
    ``query_instances``.
    """
    if type(queries) is not tuple:  # pylint: disable=unidiomatic-typecheck
        raise TypeError('Instance inventory queries must be an exact tuple.')
    if (isinstance(deadline_monotonic, bool) or
            not isinstance(deadline_monotonic, (int, float)) or
            not math.isfinite(deadline_monotonic)):
        raise ValueError('Instance inventory deadline must be finite.')
    if len(queries) > (
            provider_facets.INSTANCE_STATUS_INVENTORY_V1_MAX_QUERIES):
        raise ValueError(
            'Instance inventory batch exceeds its hard bound of '
            f'{provider_facets.INSTANCE_STATUS_INVENTORY_V1_MAX_QUERIES}.')
    query_ids = []
    for query in queries:
        if type(query) is not provider_facets.InstanceStatusInventoryQueryV1:
            raise TypeError('Instance inventory query has an invalid type.')
        if (not query.query_id or not query.cluster_name or
                not query.cluster_name_on_cloud or
                type(query.provider_config) is not dict):
            raise ValueError('Instance inventory query is malformed.')
        query_ids.append(query.query_id)
    if len(set(query_ids)) != len(query_ids):
        raise ValueError('Instance inventory query IDs must be unique.')
    if not queries:
        return ()

    resolution = _resolve_provisioner(provider_name)
    if not resolution.exists:
        return _unknown_instance_status_inventory(
            queries, f'unknown provider {resolution.canonical_name!r}')
    owner = resolution.resolve_instance_status_inventory()
    if owner is None:
        return _unknown_instance_status_inventory(
            queries, 'provider has no batch instance-status capability')
    validation_error = (
        provider_facets.instance_status_inventory_v1_validation_error(owner))
    if validation_error is not None:
        return _unknown_instance_status_inventory(
            queries, f'invalid batch instance-status capability: '
            f'{validation_error}')
    try:
        observations = owner.query_instances_batch(
            queries, deadline_monotonic=float(deadline_monotonic))
    except Exception as error:  # pylint: disable=broad-except
        logger.warning('Batch instance-status observation failed for %r: %s',
                       resolution.canonical_name, error)
        return _unknown_instance_status_inventory(
            queries, f'{type(error).__name__}: {error}')

    if type(observations) is not tuple:  # pylint: disable=unidiomatic-typecheck
        return _unknown_instance_status_inventory(
            queries, 'provider returned a non-tuple inventory observation')
    by_query_id = {}
    malformed = len(observations) != len(queries)
    for observation in observations:
        if (type(observation)
                is not provider_facets.InstanceStatusInventoryObservationV1 or
                observation.query_id in by_query_id):
            malformed = True
            continue
        by_query_id[observation.query_id] = observation
    if malformed or set(by_query_id) != set(query_ids):
        return _unknown_instance_status_inventory(
            queries, 'provider returned a malformed inventory observation')
    return tuple(by_query_id[query_id] for query_id in query_ids)


@_route_to_cloud_impl
def bootstrap_instances(
        provider_name: str, region: str, cluster_name_on_cloud: str,
        config: common.ProvisionConfig) -> common.ProvisionConfig:
    """Bootstrap configurations for a cluster.

    This function sets up auxiliary resources for a specified cluster
    with the provided configuration,
    and returns a ProvisionConfig object with updated configuration.
    These auxiliary resources could include security policies, network
    configurations etc. These resources tend to be free or very cheap,
    but it takes time to set them up from scratch. So we generally
    cache or reuse them when possible.
    """
    raise NotImplementedError


@_route_to_cloud_impl
def apply_volume(provider_name: str,
                 volume_config: models.VolumeConfig) -> models.VolumeConfig:
    """Create or register a volume.

    This function creates or registers a volume with the provided configuration,
    and returns a VolumeConfig object with updated configuration.
    """
    raise NotImplementedError


@_route_to_cloud_impl
def delete_volume(provider_name: str,
                  volume_config: models.VolumeConfig) -> models.VolumeConfig:
    """Delete a volume."""
    raise NotImplementedError


@_route_to_cloud_impl
def get_volume_usedby(
    provider_name: str,
    volume_config: models.VolumeConfig,
) -> tuple[list[str], list[str]]:
    """Get the usedby of a volume.

    Returns:
        usedby_pods: List of pods using the volume. These may include pods
                     not created by SkyPilot.
        usedby_clusters: List of clusters using the volume.
    """
    raise NotImplementedError


@_route_to_cloud_impl
def refresh_volume_config(
    provider_name: str,
    volume_config: models.VolumeConfig,
) -> tuple[bool, models.VolumeConfig]:
    """Whether need to refresh the volume config in the cloud.

    Returns:
        need_refresh: Whether need to refresh the volume config.
        volume_config: The volume config to be refreshed.
    """
    return False, volume_config


@_route_to_cloud_impl
def get_all_volumes_usedby(
    provider_name: str, configs: list[models.VolumeConfig]
) -> tuple[dict[str, Any], dict[str, Any], set[str]]:
    """Get the usedby of all volumes.

    Args:
        provider_name: Name of the provider.
        configs: List of VolumeConfig objects.

    Returns:
        usedby_pods: Dict of usedby pods.
        usedby_clusters: Dict of usedby clusters.
        failed_volume_names: Set of volume names whose usedby info
          failed to fetch.
    """
    raise NotImplementedError


@_route_to_cloud_impl
def map_all_volumes_usedby(
        provider_name: str, used_by_pods: dict[str, Any],
        used_by_clusters: dict[str, Any],
        config: models.VolumeConfig) -> tuple[list[str], list[str]]:
    """Map the usedby resources of a volume."""
    raise NotImplementedError


@_route_to_cloud_impl
def get_all_volumes_errors(
        provider_name: str,
        configs: list[models.VolumeConfig]) -> dict[str, str | None]:
    """Get error messages for all volumes.

    Checks if volumes have errors (e.g., pending state due to
    misconfiguration) and returns appropriate error messages.

    Args:
        provider_name: Name of the provider.
        configs: List of VolumeConfig objects.

    Returns:
        Dictionary mapping volume name to error message (None if no error).
    """
    # Default implementation returns empty dict (no error checking)
    del provider_name, configs
    return {}


@_route_to_cloud_impl
def run_instances(provider_name: str, region: str, cluster_name: str,
                  cluster_name_on_cloud: str,
                  config: common.ProvisionConfig) -> common.ProvisionRecord:
    """Start instances with bootstrapped configuration."""
    raise NotImplementedError


@_route_to_cloud_impl
def stop_instances(
    provider_name: str,
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any],
    worker_only: bool = False,
) -> None:
    """Stop running instances."""
    raise NotImplementedError


@_route_to_cloud_impl
def terminate_instances(
    provider_name: str,
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any],
    worker_only: bool = False,
) -> None:
    """Terminate running or stopped instances."""
    raise NotImplementedError


@_route_to_cloud_impl
def cleanup_cluster_resources(
    provider_name: str,
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any] | None = None,
) -> None:
    """Cleanup all cloud resources for a cluster (services, etc.).

    Called during post-teardown to ensure resources are cleaned up even when
    instances were deleted externally. Currently only Kubernetes needs this
    to clean up orphaned services.

    Args:
        provider_name: Name of the cloud provider
        cluster_name_on_cloud: The cluster name on cloud
        provider_config: Provider configuration dictionary
    """
    # Default implementation does nothing - only Kubernetes overrides this
    del provider_name, cluster_name_on_cloud, provider_config


@_route_to_cloud_impl
def cleanup_custom_multi_network(
    provider_name: str,
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any],
    failover: bool = False,
) -> None:
    """Cleanup custom multi-network."""
    raise NotImplementedError


@_route_to_cloud_impl
def open_ports(
    provider_name: str,
    cluster_name_on_cloud: str,
    ports: list[str],
    provider_config: dict[str, Any] | None = None,
) -> None:
    """Open ports for inbound traffic."""
    raise NotImplementedError


@_route_to_cloud_impl
def cleanup_ports(
    provider_name: str,
    cluster_name_on_cloud: str,
    # TODO: make ports optional and allow cleaning up only specified ports.
    ports: list[str],
    provider_config: dict[str, Any] | None = None,
) -> None:
    """Delete any opened ports."""
    raise NotImplementedError


@_route_to_cloud_impl
def query_ports(
    provider_name: str,
    cluster_name_on_cloud: str,
    ports: list[str],
    head_ip: str | None = None,
    provider_config: dict[str, Any] | None = None,
) -> dict[int, list[common.Endpoint]]:
    """Query details about ports on a cluster.

    If head_ip is provided, it may be used by the cloud implementation to
    return the endpoint without querying the cloud provider. If head_ip is not
    provided, the cloud provider will be queried to get the endpoint info.

    The underlying implementation is responsible for retries and timeout, e.g.
    kubernetes will wait for the service that expose the ports to be ready
    before returning the endpoint info.

    Returns a dict with port as the key and a list of common.Endpoint.
    """
    del provider_name, provider_config, cluster_name_on_cloud  # unused
    return common.query_ports_passthrough(ports, head_ip)


@_route_to_cloud_impl
def wait_instances(provider_name: str, region: str, cluster_name_on_cloud: str,
                   state: Optional['status_lib.ClusterStatus']) -> None:
    """Wait instances until they ends up in the given state."""
    raise NotImplementedError


@_route_to_cloud_impl
def get_cluster_info(
        provider_name: str,
        region: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None) -> common.ClusterInfo:
    """Get the metadata of instances in a cluster."""
    raise NotImplementedError


@_route_to_cloud_impl
def get_command_runners(
    provider_name: str,
    cluster_info: common.ClusterInfo,
    **credentials: dict[str, Any],
) -> list[command_runner.CommandRunner]:
    """Get a command runner for the given cluster."""
    ip_list = cluster_info.get_feasible_ips()
    port_list = cluster_info.get_ssh_ports()
    if len(ip_list) != len(port_list):
        raise ValueError('Cluster connection metadata has mismatched IP and '
                         f'SSH port counts: {len(ip_list)} != '
                         f'{len(port_list)}.')
    node_list = [(ip, port_list[index]) for index, ip in enumerate(ip_list)]
    return command_runner.SSHCommandRunner.make_runner_list(
        node_list=node_list,
        **credentials,
    )


def capture_provider_registry_audit(
    receipt: object,
) -> 'provider_registry_audit.ProviderRegistryAuditSnapshotV1':
    """Capture one frozen read-only view of every provider registry axis."""
    # Imported here because Cloud imports provisioner modules while its own
    # built-in registry baseline is still being constructed.
    # pylint: disable=import-outside-toplevel
    from sky import clouds as clouds_lib
    from sky.provision import provider_registry_audit as registry_audit

    receipt_reason_map = {
        provider_registration.ProviderRegistrationReceiptFailureV1.MISSING_RECEIPT:
            registry_audit.ProviderRegistryAuditCaptureErrorReasonV1.
            MISSING_RECEIPT,
        provider_registration.ProviderRegistrationReceiptFailureV1.INVALID_RECEIPT:
            registry_audit.ProviderRegistryAuditCaptureErrorReasonV1.
            INVALID_RECEIPT,
        provider_registration.ProviderRegistrationReceiptFailureV1.WRONG_PROCESS:
            registry_audit.ProviderRegistryAuditCaptureErrorReasonV1.
            WRONG_PROCESS,
        provider_registration.ProviderRegistrationReceiptFailureV1.STALE_EPOCH:
            registry_audit.ProviderRegistryAuditCaptureErrorReasonV1.
            STALE_EPOCH,
        provider_registration.ProviderRegistrationReceiptFailureV1.ACTIVE_SESSION:
            registry_audit.ProviderRegistryAuditCaptureErrorReasonV1.
            ACTIVE_SESSION,
    }
    context_map = {
        'main': registry_audit.ProviderAuditContextV1.MAIN,
        'uvicorn': registry_audit.ProviderAuditContextV1.UVICORN,
        'executor': registry_audit.ProviderAuditContextV1.EXECUTOR,
        'controller': registry_audit.ProviderAuditContextV1.CONTROLLER,
    }

    def _observe(
        context: 'provider_registry_audit.ProviderAuditContextV1',
    ) -> 'provider_registry_audit._ProviderRegistryAuditObservationV1':
        return registry_audit._observe_provider_registry_audit(  # pylint: disable=protected-access
            capture_context=context,
            cloud_entries=registry_lib.CLOUD_REGISTRY,
            cloud_aliases=registry_lib.CLOUD_REGISTRY._aliases,  # pylint: disable=protected-access
            strict_entries=_registered_provisioner_bundles,
            legacy_entries=_registered_provisioners,
            builtin_getters=_BUILTIN_PROVISIONER_MODULE_GETTERS,
            builtin_cloud_expectations=(
                clouds_lib._BUILTIN_CLOUD_AUDIT_BASELINE),  # pylint: disable=protected-access
            builtin_alias_expectations=(
                clouds_lib._BUILTIN_CLOUD_ALIAS_AUDIT_BASELINE),  # pylint: disable=protected-access
            builtin_provisioner_expectations=(
                _BUILTIN_PROVISIONER_AUDIT_BASELINE),
            strict_container_type=provider_facets.ProvisionerBundleV1,
            legacy_container_type=Provisioner,
        )

    try:
        with provider_registration.provider_registration_capture(
                receipt) as raw_context:
            capture_context = context_map.get(raw_context)
            if capture_context is None:
                raise registry_audit.ProviderRegistryAuditCaptureErrorV1(
                    registry_audit.ProviderRegistryAuditCaptureErrorReasonV1.
                    INVALID_RECEIPT)
            first_observation = _observe(capture_context)

        with provider_registration.provider_registration_capture(receipt):
            second_observation = _observe(capture_context)
            first_signature = first_observation.signature
            second_signature = second_observation.signature
            if (first_signature != second_signature or
                    first_observation.snapshot != second_observation.snapshot):
                first_raw_signature = tuple(
                    item for item in first_signature if item.startswith('raw|'))
                second_raw_signature = tuple(item for item in second_signature
                                             if item.startswith('raw|'))
                if first_raw_signature != second_raw_signature:
                    reason = (
                        registry_audit.ProviderRegistryAuditCaptureErrorReasonV1
                        .REGISTRY_CHANGED)
                else:
                    reason = (
                        registry_audit.ProviderRegistryAuditCaptureErrorReasonV1
                        .OBSERVED_MEMBER_CHANGED)
                raise registry_audit.ProviderRegistryAuditCaptureErrorV1(reason)
    except provider_registration.ProviderRegistrationReceiptError as error:
        raise registry_audit.ProviderRegistryAuditCaptureErrorV1(
            receipt_reason_map[error.reason]) from None

    return first_observation.snapshot
