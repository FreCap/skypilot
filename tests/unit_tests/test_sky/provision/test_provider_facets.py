"""Characterization tests for typed provisioner facets."""

# This suite intentionally characterizes private resolution contracts.
# pylint: disable=protected-access

from __future__ import annotations

import dataclasses
import functools
import importlib
import inspect
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from sky import provision
from sky.provision import provider_facets

_BUILTIN_PROVISIONERS = (
    ('aws', 'sky.provision.aws'),
    ('azure', 'sky.provision.azure'),
    ('cudo', 'sky.provision.cudo'),
    ('do', 'sky.provision.do'),
    ('fluidstack', 'sky.provision.fluidstack'),
    ('gcp', 'sky.provision.gcp'),
    ('hyperbolic', 'sky.provision.hyperbolic'),
    ('kubernetes', 'sky.provision.kubernetes'),
    ('lambda', 'sky.provision.lambda_cloud'),
    ('mithril', 'sky.provision.mithril'),
    ('nebius', 'sky.provision.nebius'),
    ('oci', 'sky.provision.oci'),
    ('paperspace', 'sky.provision.paperspace'),
    ('primeintellect', 'sky.provision.primeintellect'),
    ('runpod', 'sky.provision.runpod'),
    ('scp', 'sky.provision.scp'),
    ('seeweb', 'sky.provision.seeweb'),
    ('shadeform', 'sky.provision.shadeform'),
    ('slurm', 'sky.provision.slurm'),
    ('ssh', 'sky.provision.ssh'),
    ('vast', 'sky.provision.vast'),
    ('verda', 'sky.provision.verda'),
    ('vsphere', 'sky.provision.vsphere'),
    ('yotta', 'sky.provision.yotta'),
)


@pytest.fixture(autouse=True)
def _isolate_plugin_registries(monkeypatch):
    monkeypatch.setattr(provision, '_registered_provisioners', {})
    monkeypatch.setattr(provision, '_registered_provisioner_bundles', {})
    monkeypatch.setattr(provision, '_legacy_mixed_owner_diagnostics', set())


class _RecordingInstanceLifecycle:
    """Complete test implementation of InstanceLifecycleV1."""

    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def _record(self, name: str, *args: Any, **kwargs: Any) -> object:
        self.calls.append((name, args, kwargs))
        return self.result

    def query_instances(
        self,
        cluster_name: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
        non_terminated_only: bool = True,
        retry_if_missing: bool = False,
    ) -> Any:
        return self._record('query_instances', cluster_name,
                            cluster_name_on_cloud, provider_config,
                            non_terminated_only, retry_if_missing)

    def bootstrap_instances(self, region: str, cluster_name_on_cloud: str,
                            config: Any) -> Any:
        return self._record('bootstrap_instances', region,
                            cluster_name_on_cloud, config)

    def run_instances(self, region: str, cluster_name: str,
                      cluster_name_on_cloud: str, config: Any) -> Any:
        return self._record('run_instances', region, cluster_name,
                            cluster_name_on_cloud, config)

    def stop_instances(self,
                       cluster_name_on_cloud: str,
                       provider_config: dict[str, Any],
                       worker_only: bool = False) -> None:
        self._record('stop_instances', cluster_name_on_cloud, provider_config,
                     worker_only)

    def terminate_instances(self,
                            cluster_name_on_cloud: str,
                            provider_config: dict[str, Any],
                            worker_only: bool = False) -> None:
        self._record('terminate_instances', cluster_name_on_cloud,
                     provider_config, worker_only)

    def wait_instances(self, region: str, cluster_name_on_cloud: str,
                       state: Any) -> None:
        self._record('wait_instances', region, cluster_name_on_cloud, state)

    def get_cluster_info(self,
                         region: str,
                         cluster_name_on_cloud: str,
                         provider_config: dict[str, Any] | None = None) -> Any:
        return self._record('get_cluster_info', region, cluster_name_on_cloud,
                            provider_config)


def _make_query_implementation(
    result: object,
    calls: list[tuple[Any, ...]],
):

    def query_instances(
        cluster_name: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
        non_terminated_only: bool = True,
        retry_if_missing: bool = False,
    ) -> object:
        calls.append((cluster_name, cluster_name_on_cloud, provider_config,
                      non_terminated_only, retry_if_missing))
        return result

    return query_instances


def _make_valid_query_diagnostic(
    authoritative_implementation: Any,
    diagnostic_implementation: Any,
) -> provider_facets.BuiltinQueryInstancesDiagnosticV1:
    return provider_facets.BuiltinQueryInstancesDiagnosticV1(
        authoritative_implementation=authoritative_implementation,
        diagnostic_implementation=diagnostic_implementation,
    )


@pytest.mark.parametrize(('provider_name', 'module_name'),
                         _BUILTIN_PROVISIONERS)
def test_builtin_provisioner_has_complete_instance_lifecycle(
        provider_name: str, module_name: str):
    module = importlib.import_module(module_name)
    bundle = provision.get_provisioner_bundle(provider_name)

    assert isinstance(bundle, provider_facets.ProvisionerBundleV1)
    assert bundle.canonical_name == provider_name
    assert isinstance(bundle.instance_lifecycle,
                      provider_facets.LegacyInstanceLifecycleAdapter)
    assert bundle.instance_lifecycle.module is module
    assert bundle.legacy_module is module
    assert bundle.builtin_query_instances_diagnostic is None
    assert isinstance(bundle.instance_lifecycle,
                      provider_facets.InstanceLifecycleV1)
    for method_name in provider_facets.INSTANCE_LIFECYCLE_V1_METHODS:
        assert callable(getattr(bundle.instance_lifecycle, method_name))


def test_scp_wait_state_annotation_drift_is_quarantined():
    bundle = provision.get_provisioner_bundle('scp')
    raw_method = bundle.legacy_module.wait_instances

    assert inspect.signature(raw_method).parameters['state'].annotation is str
    assert isinstance(bundle.instance_lifecycle,
                      provider_facets.InstanceLifecycleV1)


@pytest.mark.parametrize('provider_name', ('seeweb', 'shadeform'))
def test_query_retry_parameter_drift_is_quarantined(provider_name: str):
    bundle = provision.get_provisioner_bundle(provider_name)
    raw_method = bundle.legacy_module.query_instances

    assert 'retry_if_missing' not in inspect.signature(raw_method).parameters
    assert isinstance(bundle.instance_lifecycle,
                      provider_facets.InstanceLifecycleV1)


def test_seeweb_bootstrap_variadic_signature_is_quarantined():
    bundle = provision.get_provisioner_bundle('seeweb')
    parameters = inspect.signature(
        bundle.legacy_module.bootstrap_instances).parameters.values()

    assert any(parameter.kind is inspect.Parameter.VAR_POSITIONAL
               for parameter in parameters)
    assert isinstance(bundle.instance_lifecycle,
                      provider_facets.InstanceLifecycleV1)


def test_verda_query_display_name_drift_is_quarantined():
    bundle = provision.get_provisioner_bundle('verda')
    raw_parameters = inspect.signature(
        bundle.legacy_module.query_instances).parameters

    assert 'cluster_name' not in raw_parameters
    assert 'cluster_name_on_cloud' in raw_parameters
    assert isinstance(bundle.instance_lifecycle,
                      provider_facets.InstanceLifecycleV1)


def test_provisioner_bundle_is_immutable():
    bundle = provider_facets.ProvisionerBundleV1(
        canonical_name='immutable-test',
        instance_lifecycle=_RecordingInstanceLifecycle(object()),
    )

    with pytest.raises(dataclasses.FrozenInstanceError):
        bundle.canonical_name = 'changed'  # type: ignore[misc]


def test_resolved_operation_owner_enum_is_exact():
    assert tuple(
        member.name for member in provision._ProvisionerOperationOwnerV1) == (
            'STRICT',
            'LEGACY',
            'BUILTIN',
        )


def test_direct_all_source_resolution_preserves_precedence_owner_and_identity():
    strict_query = mock.Mock(name='strict-query')
    legacy_query = mock.Mock(name='legacy-query')
    builtin_query = mock.Mock(name='builtin-query')
    strict_bundle = provider_facets.ProvisionerBundleV1(
        canonical_name='all-source-test',
        instance_lifecycle=SimpleNamespace(query_instances=strict_query),
    )
    legacy_registration = provision.Provisioner(module=SimpleNamespace(
        query_instances=legacy_query))
    builtin_module = SimpleNamespace(query_instances=builtin_query)
    builtin_bundle = provider_facets.ProvisionerBundleV1(
        canonical_name='all-source-test',
        instance_lifecycle=provider_facets.LegacyInstanceLifecycleAdapter(
            builtin_module),
        legacy_module=builtin_module,
    )
    all_sources = provision._ProvisionerResolution(
        canonical_name='all-source-test',
        strict_bundle=strict_bundle,
        legacy_registration=legacy_registration,
        builtin_bundle=builtin_bundle,
    )

    strict_operation = all_sources.resolve_operation('query_instances')
    legacy_operation = dataclasses.replace(
        all_sources, strict_bundle=None).resolve_operation('query_instances')
    builtin_operation = dataclasses.replace(
        all_sources,
        strict_bundle=None,
        legacy_registration=None,
    ).resolve_operation('query_instances')

    assert strict_operation is not None
    assert strict_operation.owner is provision._ProvisionerOperationOwnerV1.STRICT
    assert strict_operation.authoritative_implementation is strict_query
    assert strict_operation.diagnostic_implementation is None
    assert strict_operation.implementation is strict_query
    assert legacy_operation is not None
    assert legacy_operation.owner is provision._ProvisionerOperationOwnerV1.LEGACY
    assert legacy_operation.authoritative_implementation is legacy_query
    assert legacy_operation.diagnostic_implementation is None
    assert legacy_operation.implementation is legacy_query
    assert builtin_operation is not None
    assert builtin_operation.owner is provision._ProvisionerOperationOwnerV1.BUILTIN
    assert builtin_operation.authoritative_implementation is builtin_query
    assert builtin_operation.diagnostic_implementation is None
    assert builtin_operation.implementation is builtin_query


def test_old_bundle_construction_defaults_private_diagnostic_to_none():
    lifecycle = _RecordingInstanceLifecycle(object())
    template_override = mock.Mock()
    legacy_module = SimpleNamespace()

    positional = provider_facets.ProvisionerBundleV1('positional-bundle-test',
                                                     lifecycle,
                                                     template_override,
                                                     legacy_module)
    keyword = provider_facets.ProvisionerBundleV1(
        canonical_name='keyword-bundle-test',
        instance_lifecycle=lifecycle,
        template_override=template_override,
        legacy_module=legacy_module,
    )

    assert positional.builtin_query_instances_diagnostic is None
    assert keyword.builtin_query_instances_diagnostic is None


def test_strict_registration_rejects_incomplete_instance_lifecycle():
    incomplete_lifecycle = SimpleNamespace(query_instances=mock.Mock())
    bundle = provider_facets.ProvisionerBundleV1(
        canonical_name='incomplete-test',
        instance_lifecycle=incomplete_lifecycle,
    )

    with pytest.raises(ValueError, match='InstanceLifecycleV1'):
        provision.register_provisioner_bundle(bundle)


def test_strict_diagnostic_registration_rejects_before_validation_or_mutation():
    existing_strict = provider_facets.ProvisionerBundleV1(
        canonical_name='protected-test',
        instance_lifecycle=_RecordingInstanceLifecycle('strict'),
    )
    existing_legacy = provision.Provisioner(module=SimpleNamespace())
    strict_registrations = provision._registered_provisioner_bundles
    legacy_registrations = provision._registered_provisioners
    strict_registrations['protected-test'] = existing_strict
    legacy_registrations['protected-test'] = existing_legacy
    authoritative = _make_query_implementation(object(), [])
    diagnostic = _make_query_implementation(object(), [])
    invalid_bundle = provider_facets.ProvisionerBundleV1(
        canonical_name='protected-test',
        instance_lifecycle=SimpleNamespace(),
        builtin_query_instances_diagnostic=_make_valid_query_diagnostic(
            authoritative, diagnostic),
    )

    with pytest.raises(ValueError, match='diagnostic'):
        provision.register_provisioner_bundle(invalid_bundle)

    assert provision._registered_provisioner_bundles is strict_registrations
    assert tuple(strict_registrations) == ('protected-test',)
    assert strict_registrations['protected-test'] is existing_strict
    assert provision._registered_provisioners is legacy_registrations
    assert tuple(legacy_registrations) == ('protected-test',)
    assert legacy_registrations['protected-test'] is existing_legacy


def test_strict_registration_rejects_incompatible_signatures():
    wrong_signature = lambda: None
    lifecycle = SimpleNamespace(
        **{
            method_name: wrong_signature
            for method_name in provider_facets.INSTANCE_LIFECYCLE_V1_METHODS
        })
    bundle = provider_facets.ProvisionerBundleV1(
        canonical_name='wrong-signature-test',
        instance_lifecycle=lifecycle,
    )

    with pytest.raises(ValueError, match='expected'):
        provision.register_provisioner_bundle(bundle)


def test_strict_registration_rejects_async_implementation():

    # The invalid override is the registration failure under test.
    # pylint: disable=missing-class-docstring,invalid-overridden-method
    class _AsyncLifecycle(_RecordingInstanceLifecycle):

        async def query_instances(
            self,
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = True,
            retry_if_missing: bool = False,
        ) -> Any:
            return {}

    bundle = provider_facets.ProvisionerBundleV1(
        canonical_name='async-test',
        instance_lifecycle=_AsyncLifecycle(object()),
    )

    with pytest.raises(ValueError, match='must be synchronous'):
        provision.register_provisioner_bundle(bundle)


def test_strict_registration_routes_the_complete_facet_without_fallback():
    result = object()
    lifecycle = _RecordingInstanceLifecycle(result)
    bundle = provider_facets.ProvisionerBundleV1(
        canonical_name='strict-routing-test',
        instance_lifecycle=lifecycle,
    )
    provision.register_provisioner_bundle(bundle)

    assert provision.query_instances(
        'strict-routing-test',
        'display-name',
        'cloud-name',
        {'region': 'test'},
        False,
        True,
    ) is result
    assert lifecycle.calls == [
        ('query_instances', ('display-name', 'cloud-name', {
            'region': 'test'
        }, False, True), {}),
    ]


def test_strict_registration_preserves_template_override():
    lifecycle = _RecordingInstanceLifecycle(object())
    template_override = mock.Mock()
    bundle = provider_facets.ProvisionerBundleV1(
        canonical_name='strict-template-test',
        instance_lifecycle=lifecycle,
        template_override=template_override,
    )

    provision.register_provisioner_bundle(bundle)

    resolved = provision.get_provisioner_bundle('strict-template-test')
    assert resolved is bundle
    assert resolved.template_override is template_override
    assert provision.get_provisioner_template_override(
        'strict-template-test') is template_override
    assert provision.get_registered_provisioner(
        'strict-template-test').module is lifecycle


def test_strict_registration_preserves_last_registration_wins():
    first = provider_facets.ProvisionerBundleV1(
        canonical_name='strict-replacement-test',
        instance_lifecycle=_RecordingInstanceLifecycle('first'),
    )
    replacement = provider_facets.ProvisionerBundleV1(
        canonical_name='strict-replacement-test',
        instance_lifecycle=_RecordingInstanceLifecycle('replacement'),
    )

    provision.register_provisioner_bundle(first)
    provision.register_provisioner_bundle(replacement)

    assert provision.get_provisioner_bundle(
        'strict-replacement-test') is replacement
    assert provision.query_instances('strict-replacement-test', 'display',
                                     'cloud') == 'replacement'


def test_repeated_strict_registration_is_idempotent(monkeypatch):
    bundle = provider_facets.ProvisionerBundleV1(
        canonical_name='strict-idempotent-test',
        instance_lifecycle=_RecordingInstanceLifecycle('result'),
    )
    replacement_log = mock.Mock()
    monkeypatch.setattr(provision.logger, 'info', replacement_log)

    provision.register_provisioner_bundle(bundle)
    provision.register_provisioner_bundle(bundle)

    assert provision.get_provisioner_bundle('strict-idempotent-test') is bundle
    replacement_log.assert_not_called()


def test_registration_modes_preserve_last_registration_wins():
    strict = _RecordingInstanceLifecycle('strict')
    legacy_query = mock.Mock(return_value='legacy')
    provision.register_provisioner_bundle(
        provider_facets.ProvisionerBundleV1(
            canonical_name='mode-replacement-test',
            instance_lifecycle=strict,
        ))
    provision.register_provisioner(
        'mode-replacement-test',
        SimpleNamespace(query_instances=legacy_query),
    )

    assert provision.query_instances('mode-replacement-test', 'display',
                                     'cloud') == 'legacy'

    replacement = _RecordingInstanceLifecycle('strict-replacement')
    provision.register_provisioner_bundle(
        provider_facets.ProvisionerBundleV1(
            canonical_name='mode-replacement-test',
            instance_lifecycle=replacement,
        ))

    assert provision.query_instances('mode-replacement-test', 'display',
                                     'cloud') == 'strict-replacement'


def test_builtin_module_monkeypatch_remains_a_facade_seam(monkeypatch):
    result = object()
    query_instances = mock.Mock(return_value=result)
    monkeypatch.setattr(provision.aws, 'query_instances', query_instances)

    assert provision.query_instances('aws', 'display', 'cloud') is result
    query_instances.assert_called_once_with('display', 'cloud')


def test_builtin_module_replacement_remains_a_facade_seam(monkeypatch):
    result = object()
    query_instances = mock.Mock(return_value=result)
    monkeypatch.setattr(provision, 'aws',
                        SimpleNamespace(query_instances=query_instances))

    assert provision.query_instances('aws', 'display', 'cloud') is result
    query_instances.assert_called_once_with('display', 'cloud')


def test_resolved_operation_is_pinned_and_next_facade_call_observes_patch(
        monkeypatch):
    first_result = object()
    next_result = object()
    first_query = mock.Mock(return_value=first_result)
    next_query = mock.Mock(return_value=next_result)
    monkeypatch.setattr(provision.aws, 'query_instances', first_query)

    operation = provision._resolve_provisioner('aws').resolve_operation(
        'query_instances')
    assert operation is not None
    assert operation.owner is provision._ProvisionerOperationOwnerV1.BUILTIN
    assert operation.authoritative_implementation is first_query

    monkeypatch.setattr(provision.aws, 'query_instances', next_query)

    assert operation.implementation('first-display',
                                    'first-cloud') is first_result
    assert provision.query_instances('aws', 'next-display',
                                     'next-cloud') is next_result
    first_query.assert_called_once_with('first-display', 'first-cloud')
    next_query.assert_called_once_with('next-display', 'next-cloud')


def test_builtin_descriptor_is_read_once_and_first_callable_is_invoked(
        monkeypatch):
    first_result = object()
    first_query = mock.Mock(return_value=first_result)
    second_query = mock.Mock(return_value=object())

    class _RotatingQueryDescriptor:
        """Return a different query callable for each descriptor read."""

        def __init__(self) -> None:
            self.lookups = 0

        def __get__(self, instance: Any, owner: type[Any]) -> Any:
            del instance, owner
            self.lookups += 1
            if self.lookups == 1:
                return first_query
            return second_query

    descriptor = _RotatingQueryDescriptor()

    class _RotatingModule:
        pass

    setattr(_RotatingModule, 'query_instances', descriptor)
    monkeypatch.setattr(provision, 'aws', _RotatingModule())

    assert provision.query_instances('aws', 'display', 'cloud') is first_result
    assert descriptor.lookups == 1
    first_query.assert_called_once_with('display', 'cloud')
    second_query.assert_not_called()


def test_diagnostic_admission_reuses_the_single_raw_descriptor_read(
        monkeypatch):
    authoritative_calls: list[tuple[Any, ...]] = []
    diagnostic_result = object()
    diagnostic_calls: list[tuple[Any, ...]] = []
    first_query = _make_query_implementation(object(), authoritative_calls)
    second_query = mock.Mock(return_value=object())
    diagnostic = _make_query_implementation(diagnostic_result, diagnostic_calls)

    class _RotatingQueryDescriptor:
        """Return a different query callable for each descriptor read."""

        def __init__(self) -> None:
            self.lookups = 0

        def __get__(self, instance: Any, owner: type[Any]) -> Any:
            del instance, owner
            self.lookups += 1
            if self.lookups == 1:
                return first_query
            return second_query

    descriptor = _RotatingQueryDescriptor()

    class _DiagnosticModule:
        pass

    setattr(_DiagnosticModule, 'query_instances', descriptor)
    setattr(_DiagnosticModule, '_QUERY_INSTANCES_DIAGNOSTIC_V1',
            _make_valid_query_diagnostic(first_query, diagnostic))
    monkeypatch.setattr(provision, 'aws', _DiagnosticModule())

    assert provision.query_instances('aws', 'display',
                                     'cloud') is diagnostic_result
    assert descriptor.lookups == 1
    assert not authoritative_calls
    second_query.assert_not_called()
    assert diagnostic_calls == [('display', 'cloud', None, True, False)]


@pytest.mark.parametrize(
    'replacement',
    (
        SimpleNamespace(),
        SimpleNamespace(query_instances=None),
    ),
)
def test_builtin_missing_method_uses_facade_default(monkeypatch, replacement):
    monkeypatch.setattr(provision, 'aws', replacement)

    resolution = provision._resolve_provisioner('aws')
    assert resolution.resolve_operation('query_instances') is None

    with pytest.raises(NotImplementedError):
        provision.query_instances('aws', 'display', 'cloud')


def test_exact_valid_builtin_diagnostic_is_selected_exactly_once(monkeypatch):
    authoritative_result = object()
    diagnostic_result = object()
    authoritative_calls: list[tuple[Any, ...]] = []
    diagnostic_calls: list[tuple[Any, ...]] = []
    authoritative = _make_query_implementation(authoritative_result,
                                               authoritative_calls)
    diagnostic = _make_query_implementation(diagnostic_result, diagnostic_calls)
    module = SimpleNamespace(
        query_instances=authoritative,
        _QUERY_INSTANCES_DIAGNOSTIC_V1=_make_valid_query_diagnostic(
            authoritative, diagnostic),
    )
    monkeypatch.setattr(provision, 'aws', module)

    operation = provision._resolve_provisioner('aws').resolve_operation(
        'query_instances')

    assert operation is not None
    assert operation.owner is provision._ProvisionerOperationOwnerV1.BUILTIN
    assert operation.authoritative_implementation is authoritative
    assert operation.diagnostic_implementation is diagnostic
    assert operation.implementation is diagnostic
    provider_config = {'region': 'test-region'}
    assert provision.query_instances(
        'aws',
        'display-name',
        'cloud-name',
        provider_config,
        False,
        True,
    ) is diagnostic_result
    assert diagnostic_calls == [
        ('display-name', 'cloud-name', provider_config, False, True),
    ]
    assert not authoritative_calls


def test_diagnostic_exception_propagates_without_authoritative_retry(
        monkeypatch):
    authoritative_calls: list[tuple[Any, ...]] = []
    diagnostic_calls: list[tuple[Any, ...]] = []
    authoritative = _make_query_implementation(object(), authoritative_calls)
    error = RuntimeError('diagnostic failure')

    def diagnostic(
        cluster_name: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
        non_terminated_only: bool = True,
        retry_if_missing: bool = False,
    ) -> object:
        diagnostic_calls.append(
            (cluster_name, cluster_name_on_cloud, provider_config,
             non_terminated_only, retry_if_missing))
        raise error

    monkeypatch.setattr(
        provision, 'aws',
        SimpleNamespace(
            query_instances=authoritative,
            _QUERY_INSTANCES_DIAGNOSTIC_V1=_make_valid_query_diagnostic(
                authoritative, diagnostic),
        ))

    with pytest.raises(RuntimeError) as exc_info:
        provision.query_instances('aws', 'display', 'cloud')

    assert exc_info.value is error
    assert diagnostic_calls == [('display', 'cloud', None, True, False)]
    assert not authoritative_calls


def test_replacing_raw_authoritative_query_invalidates_stale_diagnostic(
        monkeypatch):
    stale_calls: list[tuple[Any, ...]] = []
    replacement_calls: list[tuple[Any, ...]] = []
    diagnostic_calls: list[tuple[Any, ...]] = []
    stale_authoritative = _make_query_implementation(object(), stale_calls)
    replacement_result = object()
    replacement = _make_query_implementation(replacement_result,
                                             replacement_calls)
    diagnostic = _make_query_implementation(object(), diagnostic_calls)
    module = SimpleNamespace(
        query_instances=replacement,
        _QUERY_INSTANCES_DIAGNOSTIC_V1=_make_valid_query_diagnostic(
            stale_authoritative, diagnostic),
    )
    monkeypatch.setattr(provision, 'aws', module)

    operation = provision._resolve_provisioner('aws').resolve_operation(
        'query_instances')

    assert operation is not None
    assert operation.authoritative_implementation is replacement
    assert operation.diagnostic_implementation is None
    assert operation.implementation is replacement
    assert provision.query_instances('aws', 'display',
                                     'cloud') is replacement_result
    assert replacement_calls == [('display', 'cloud', None, True, False)]
    assert not stale_calls
    assert not diagnostic_calls


@pytest.mark.parametrize('legacy_owns_query', (False, True))
def test_any_legacy_registration_suppresses_builtin_diagnostic(
        monkeypatch, legacy_owns_query: bool):
    authoritative_result = object()
    diagnostic_result = object()
    plugin_result = object()
    authoritative_calls: list[tuple[Any, ...]] = []
    diagnostic_calls: list[tuple[Any, ...]] = []
    authoritative = _make_query_implementation(authoritative_result,
                                               authoritative_calls)
    diagnostic = _make_query_implementation(diagnostic_result, diagnostic_calls)
    monkeypatch.setattr(
        provision, 'aws',
        SimpleNamespace(
            query_instances=authoritative,
            _QUERY_INSTANCES_DIAGNOSTIC_V1=_make_valid_query_diagnostic(
                authoritative, diagnostic),
        ))
    plugin_query = mock.Mock(return_value=plugin_result)
    if legacy_owns_query:
        legacy_module = SimpleNamespace(query_instances=plugin_query)
    else:
        legacy_module = SimpleNamespace(stop_instances=mock.Mock())
    provision.register_provisioner('aws', legacy_module)

    operation = provision._resolve_provisioner('aws').resolve_operation(
        'query_instances')

    assert operation is not None
    assert operation.diagnostic_implementation is None
    if legacy_owns_query:
        assert operation.owner is provision._ProvisionerOperationOwnerV1.LEGACY
        assert operation.authoritative_implementation is plugin_query
        assert provision.query_instances('aws', 'display',
                                         'cloud') is plugin_result
        plugin_query.assert_called_once_with('display', 'cloud')
        assert not authoritative_calls
    else:
        assert operation.owner is provision._ProvisionerOperationOwnerV1.BUILTIN
        assert operation.authoritative_implementation is authoritative
        assert provision.query_instances('aws', 'display',
                                         'cloud') is authoritative_result
        assert authoritative_calls == [('display', 'cloud', None, True, False)]
        plugin_query.assert_not_called()
    assert not diagnostic_calls


def test_query_diagnostic_never_attaches_to_stop_and_void_result_is_preserved(
        monkeypatch):
    query_calls: list[tuple[Any, ...]] = []
    diagnostic_calls: list[tuple[Any, ...]] = []
    query = _make_query_implementation(object(), query_calls)
    diagnostic = _make_query_implementation(object(), diagnostic_calls)
    stop_sentinel = object()
    stop = mock.Mock(return_value=stop_sentinel)
    monkeypatch.setattr(
        provision, 'aws',
        SimpleNamespace(
            query_instances=query,
            stop_instances=stop,
            _QUERY_INSTANCES_DIAGNOSTIC_V1=_make_valid_query_diagnostic(
                query, diagnostic),
        ))

    operation = provision._resolve_provisioner('aws').resolve_operation(
        'stop_instances')

    assert operation is not None
    assert operation.owner is provision._ProvisionerOperationOwnerV1.BUILTIN
    assert operation.diagnostic_implementation is None
    assert operation.implementation is operation.authoritative_implementation
    assert operation.authoritative_implementation is not stop
    assert provision.stop_instances('aws', 'cloud-name', {'region': 'test'},
                                    True) is None
    stop.assert_called_once_with('cloud-name', {'region': 'test'}, True)
    assert not query_calls
    assert not diagnostic_calls


@pytest.mark.parametrize(
    'invalid_kind',
    (
        'missing',
        'subclass',
        'descriptor',
        'noncallable-authoritative',
        'noncallable-diagnostic',
        'coroutine',
        'async-callable',
        'variadic',
        'wrong-default',
        'false-as-zero',
        'true-as-one',
        'none-equal',
        'wrong-parameter',
        'uninspectable',
    ),
)
def test_invalid_builtin_diagnostic_is_absent_and_authoritative_query_runs(
        monkeypatch, invalid_kind: str):
    authoritative_result = object()
    authoritative_calls: list[tuple[Any, ...]] = []
    diagnostic_calls: list[tuple[Any, ...]] = []
    authoritative = _make_query_implementation(authoritative_result,
                                               authoritative_calls)
    valid_diagnostic = _make_query_implementation(object(), diagnostic_calls)
    metadata: Any | None = None
    descriptor = None

    if invalid_kind == 'subclass':

        class _DiagnosticSubclass(
                provider_facets.BuiltinQueryInstancesDiagnosticV1):
            pass

        metadata = _DiagnosticSubclass(authoritative, valid_diagnostic)
    elif invalid_kind == 'descriptor':
        valid_metadata = _make_valid_query_diagnostic(authoritative,
                                                      valid_diagnostic)

        class _DiagnosticDescriptor:

            def __init__(self) -> None:
                self.lookups = 0

            def __get__(self, instance: Any, owner: type[Any]) -> Any:
                del instance, owner
                self.lookups += 1
                return valid_metadata

        descriptor = _DiagnosticDescriptor()
    elif invalid_kind == 'noncallable-authoritative':
        metadata = _make_valid_query_diagnostic(object(), valid_diagnostic)
    elif invalid_kind == 'noncallable-diagnostic':
        metadata = _make_valid_query_diagnostic(authoritative, object())
    elif invalid_kind == 'coroutine':

        async def invalid_diagnostic(
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = True,
            retry_if_missing: bool = False,
        ) -> object:
            del (cluster_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

        metadata = _make_valid_query_diagnostic(authoritative,
                                                invalid_diagnostic)
    elif invalid_kind == 'async-callable':

        class _AsyncCallableDiagnostic:
            """Callable object that violates the synchronous query contract."""

            async def __call__(
                self,
                cluster_name: str,
                cluster_name_on_cloud: str,
                provider_config: dict[str, Any] | None = None,
                non_terminated_only: bool = True,
                retry_if_missing: bool = False,
            ) -> object:
                del (cluster_name, cluster_name_on_cloud, provider_config,
                     non_terminated_only, retry_if_missing)
                return object()

        metadata = _make_valid_query_diagnostic(authoritative,
                                                _AsyncCallableDiagnostic())
    elif invalid_kind == 'variadic':

        def invalid_diagnostic(*args: Any, **kwargs: Any) -> object:
            del args, kwargs
            return object()

        metadata = _make_valid_query_diagnostic(authoritative,
                                                invalid_diagnostic)
    elif invalid_kind == 'wrong-default':

        def invalid_diagnostic(
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = True,
            retry_if_missing: bool = True,
        ) -> object:
            del (cluster_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

        metadata = _make_valid_query_diagnostic(authoritative,
                                                invalid_diagnostic)
    elif invalid_kind == 'false-as-zero':

        def invalid_diagnostic(
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = True,
            retry_if_missing: bool = 0,
        ) -> object:
            del (cluster_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

        metadata = _make_valid_query_diagnostic(authoritative,
                                                invalid_diagnostic)
    elif invalid_kind == 'true-as-one':

        def invalid_diagnostic(
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = 1,
            retry_if_missing: bool = False,
        ) -> object:
            del (cluster_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

        metadata = _make_valid_query_diagnostic(authoritative,
                                                invalid_diagnostic)
    elif invalid_kind == 'none-equal':

        class _NoneEqualDefault:
            """Wrong-type default that compares equal to None."""

            def __eq__(self, other: object) -> bool:
                return other is None

        none_equal_default = _NoneEqualDefault()

        def invalid_diagnostic(
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = none_equal_default,
            non_terminated_only: bool = True,
            retry_if_missing: bool = False,
        ) -> object:
            del (cluster_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

        metadata = _make_valid_query_diagnostic(authoritative,
                                                invalid_diagnostic)
    elif invalid_kind == 'wrong-parameter':

        def invalid_diagnostic(
            display_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = True,
            retry_if_missing: bool = False,
        ) -> object:
            del (display_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

        metadata = _make_valid_query_diagnostic(authoritative,
                                                invalid_diagnostic)
    elif invalid_kind == 'uninspectable':

        class _UninspectableDiagnostic:
            """Callable whose runtime signature cannot be inspected."""

            @property
            def __signature__(self) -> inspect.Signature:
                raise ValueError('signature unavailable')

            def __call__(
                self,
                cluster_name: str,
                cluster_name_on_cloud: str,
                provider_config: dict[str, Any] | None = None,
                non_terminated_only: bool = True,
                retry_if_missing: bool = False,
            ) -> object:
                del (cluster_name, cluster_name_on_cloud, provider_config,
                     non_terminated_only, retry_if_missing)
                return object()

        metadata = _make_valid_query_diagnostic(authoritative,
                                                _UninspectableDiagnostic())
    else:
        assert invalid_kind == 'missing'

    if descriptor is not None:

        class _DescriptorModule:
            pass

        setattr(_DescriptorModule, '_QUERY_INSTANCES_DIAGNOSTIC_V1', descriptor)
        module = _DescriptorModule()
        module.query_instances = authoritative
    else:
        module = SimpleNamespace(query_instances=authoritative)
        if metadata is not None:
            module._QUERY_INSTANCES_DIAGNOSTIC_V1 = metadata
    monkeypatch.setattr(provision, 'aws', module)

    bundle = provision._get_builtin_provisioner_bundle('aws')
    assert bundle is not None
    assert bundle.builtin_query_instances_diagnostic is None
    assert provision.query_instances('aws', 'display',
                                     'cloud') is authoritative_result
    assert authoritative_calls == [('display', 'cloud', None, True, False)]
    assert not diagnostic_calls
    if descriptor is not None:
        assert descriptor.lookups == 0


@pytest.mark.parametrize(
    'field_name', ('authoritative_implementation', 'diagnostic_implementation'))
@pytest.mark.parametrize(
    'callable_shape',
    ('bound-method', 'callable-class', 'callable-instance', 'partial',
     'cache-wrapper', 'decorated-function', 'custom-call-descriptor',
     'singledispatchmethod-descriptor'))
def test_every_diagnostic_field_rejects_non_function_shapes(
        monkeypatch, field_name: str, callable_shape: str):
    authoritative_result = object()
    authoritative_calls: list[tuple[Any, ...]] = []
    authoritative = _make_query_implementation(authoritative_result,
                                               authoritative_calls)
    valid_diagnostic = _make_query_implementation(object(), [])

    class _BoundOwner:
        """Own a signature-compatible bound method."""

        def implementation(
            self,
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = True,
            retry_if_missing: bool = False,
        ) -> object:
            del (cluster_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

    class _CallableClass:
        """Expose a signature-compatible callable class."""

        def __new__(
            cls,
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = True,
            retry_if_missing: bool = False,
        ) -> object:
            del (cls, cluster_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

    class _CallableInstance:
        """Expose a signature-compatible callable instance."""

        def __call__(
            self,
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = True,
            retry_if_missing: bool = False,
        ) -> object:
            del (cluster_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

    class _CallDescriptor:
        """Resolve an instance call to a query function."""

        def __get__(self, instance: Any, owner: type[Any]) -> Any:
            del instance, owner
            return valid_diagnostic

    class _DescriptorCallable:
        """Expose query calling through a custom descriptor."""
        __call__ = _CallDescriptor()
        __signature__ = inspect.signature(valid_diagnostic)

    class _DispatchCallable:
        """Expose query calling through singledispatchmethod."""

        @functools.singledispatchmethod
        @staticmethod
        def __call__(
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = True,
            retry_if_missing: bool = False,
        ) -> object:
            del (cluster_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

    @functools.wraps(valid_diagnostic)
    def decorated_function(*args: Any, **kwargs: Any) -> object:
        return valid_diagnostic(*args, **kwargs)

    invalid_implementations = {
        'bound-method': _BoundOwner().implementation,
        'callable-class': _CallableClass,
        'callable-instance': _CallableInstance(),
        'partial': functools.partial(valid_diagnostic),
        'cache-wrapper': functools.lru_cache()(valid_diagnostic),
        'decorated-function': decorated_function,
        'custom-call-descriptor': _DescriptorCallable(),
        'singledispatchmethod-descriptor': _DispatchCallable(),
    }
    implementations = {
        'authoritative_implementation': authoritative,
        'diagnostic_implementation': valid_diagnostic,
    }
    implementations[field_name] = invalid_implementations[callable_shape]
    metadata = provider_facets.BuiltinQueryInstancesDiagnosticV1(
        **implementations)
    monkeypatch.setattr(
        provision, 'aws',
        SimpleNamespace(
            query_instances=authoritative,
            _QUERY_INSTANCES_DIAGNOSTIC_V1=metadata,
        ))

    bundle = provision._get_builtin_provisioner_bundle('aws')
    assert bundle is not None
    assert bundle.builtin_query_instances_diagnostic is None
    assert provision.query_instances('aws', 'display',
                                     'cloud') is authoritative_result
    assert authoritative_calls == [('display', 'cloud', None, True, False)]


@pytest.mark.parametrize(
    'field_name', ('authoritative_implementation', 'diagnostic_implementation'))
@pytest.mark.parametrize('override_kind',
                         ('signature', 'text-signature', 'partialmethod'))
def test_code_derived_validation_ignores_signature_overrides(
        monkeypatch, field_name: str, override_kind: str):
    authoritative_result = object()
    authoritative_calls: list[tuple[Any, ...]] = []
    authoritative = _make_query_implementation(authoritative_result,
                                               authoritative_calls)
    valid_diagnostic = _make_query_implementation(object(), [])

    def wrong_implementation(only_argument: str) -> object:
        del only_argument
        return object()

    if override_kind == 'signature':
        wrong_implementation.__signature__ = inspect.signature(  # type: ignore[attr-defined]
            valid_diagnostic)
    elif override_kind == 'text-signature':
        wrong_implementation.__text_signature__ = (  # type: ignore[attr-defined]
            '(cluster_name, cluster_name_on_cloud, provider_config=None, '
            'non_terminated_only=True, retry_if_missing=False)')
    else:
        assert override_kind == 'partialmethod'
        wrong_implementation._partialmethod = functools.partialmethod(  # type: ignore[attr-defined]
            valid_diagnostic)
    assert tuple(inspect.signature(wrong_implementation).parameters) == (
        'cluster_name',
        'cluster_name_on_cloud',
        'provider_config',
        'non_terminated_only',
        'retry_if_missing',
    )

    implementations = {
        'authoritative_implementation': authoritative,
        'diagnostic_implementation': valid_diagnostic,
    }
    implementations[field_name] = wrong_implementation
    metadata = provider_facets.BuiltinQueryInstancesDiagnosticV1(
        **implementations)
    monkeypatch.setattr(
        provision, 'aws',
        SimpleNamespace(
            query_instances=authoritative,
            _QUERY_INSTANCES_DIAGNOSTIC_V1=metadata,
        ))

    bundle = provision._get_builtin_provisioner_bundle('aws')
    assert bundle is not None
    assert bundle.builtin_query_instances_diagnostic is None
    assert provision.query_instances('aws', 'display',
                                     'cloud') is authoritative_result
    assert authoritative_calls == [('display', 'cloud', None, True, False)]


@pytest.mark.parametrize(
    'field_name', ('authoritative_implementation', 'diagnostic_implementation'))
@pytest.mark.parametrize(
    'async_shape', ('coroutine-function', 'callable-object', 'async-generator',
                    'classmethod-callable', 'staticmethod-callable',
                    'partial-callable', 'lru-cache-wrapper', 'nested-wraps'))
def test_every_diagnostic_field_rejects_asynchronous_callables(
        monkeypatch, field_name: str, async_shape: str):
    authoritative_result = object()
    authoritative_calls: list[tuple[Any, ...]] = []
    authoritative = _make_query_implementation(authoritative_result,
                                               authoritative_calls)
    valid_diagnostic = _make_query_implementation(object(), [])

    async def coroutine_function(
        cluster_name: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
        non_terminated_only: bool = True,
        retry_if_missing: bool = False,
    ) -> object:
        del (cluster_name, cluster_name_on_cloud, provider_config,
             non_terminated_only, retry_if_missing)
        return object()

    async def async_generator(
        cluster_name: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
        non_terminated_only: bool = True,
        retry_if_missing: bool = False,
    ):
        del (cluster_name, cluster_name_on_cloud, provider_config,
             non_terminated_only, retry_if_missing)
        yield object()

    class _AsyncCallable:
        """Callable object that violates the synchronous query contract."""

        async def __call__(
            self,
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = True,
            retry_if_missing: bool = False,
        ) -> object:
            del (cluster_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

    class _AsyncClassMethodCallable:
        """Callable object whose classmethod descriptor is asynchronous."""

        @classmethod
        async def __call__(
            cls,
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = True,
            retry_if_missing: bool = False,
        ) -> object:
            del (cls, cluster_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

    class _AsyncStaticMethodCallable:
        """Callable object whose staticmethod descriptor is asynchronous."""

        @staticmethod
        async def __call__(
            cluster_name: str,
            cluster_name_on_cloud: str,
            provider_config: dict[str, Any] | None = None,
            non_terminated_only: bool = True,
            retry_if_missing: bool = False,
        ) -> object:
            del (cluster_name, cluster_name_on_cloud, provider_config,
                 non_terminated_only, retry_if_missing)
            return object()

    def synchronous_base(
        cluster_name: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
        non_terminated_only: bool = True,
        retry_if_missing: bool = False,
    ) -> object:
        del (cluster_name, cluster_name_on_cloud, provider_config,
             non_terminated_only, retry_if_missing)
        return object()

    @functools.wraps(synchronous_base)
    async def asynchronous_middle(
        cluster_name: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
        non_terminated_only: bool = True,
        retry_if_missing: bool = False,
    ) -> object:
        del (cluster_name, cluster_name_on_cloud, provider_config,
             non_terminated_only, retry_if_missing)
        return object()

    @functools.wraps(asynchronous_middle)
    def synchronous_outer(
        cluster_name: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None,
        non_terminated_only: bool = True,
        retry_if_missing: bool = False,
    ) -> object:
        return asynchronous_middle(cluster_name, cluster_name_on_cloud,
                                   provider_config, non_terminated_only,
                                   retry_if_missing)

    invalid_implementation: Any
    if async_shape == 'coroutine-function':
        invalid_implementation = coroutine_function
    elif async_shape == 'callable-object':
        invalid_implementation = _AsyncCallable()
    elif async_shape == 'async-generator':
        invalid_implementation = async_generator
    elif async_shape == 'classmethod-callable':
        invalid_implementation = _AsyncClassMethodCallable()
    elif async_shape == 'staticmethod-callable':
        invalid_implementation = _AsyncStaticMethodCallable()
    elif async_shape == 'partial-callable':
        invalid_implementation = functools.partial(_AsyncCallable())
    elif async_shape == 'lru-cache-wrapper':
        invalid_implementation = functools.lru_cache()(coroutine_function)
    else:
        assert async_shape == 'nested-wraps'
        invalid_implementation = synchronous_outer

    implementations = {
        'authoritative_implementation': authoritative,
        'diagnostic_implementation': valid_diagnostic,
    }
    implementations[field_name] = invalid_implementation
    metadata = provider_facets.BuiltinQueryInstancesDiagnosticV1(
        **implementations)
    monkeypatch.setattr(
        provision, 'aws',
        SimpleNamespace(
            query_instances=authoritative,
            _QUERY_INSTANCES_DIAGNOSTIC_V1=metadata,
        ))

    bundle = provision._get_builtin_provisioner_bundle('aws')
    assert bundle is not None
    assert bundle.builtin_query_instances_diagnostic is None
    assert provision.query_instances('aws', 'display',
                                     'cloud') is authoritative_result
    assert authoritative_calls == [('display', 'cloud', None, True, False)]


@pytest.mark.parametrize(
    'field_name', ('authoritative_implementation', 'diagnostic_implementation'))
def test_every_diagnostic_field_rejects_a_cyclic_wrapped_callable(
        monkeypatch, field_name: str):
    authoritative_result = object()
    authoritative_calls: list[tuple[Any, ...]] = []
    authoritative = _make_query_implementation(authoritative_result,
                                               authoritative_calls)
    valid_diagnostic = _make_query_implementation(object(), [])
    cyclic = _make_query_implementation(object(), [])
    cyclic.__wrapped__ = cyclic  # type: ignore[attr-defined]
    implementations = {
        'authoritative_implementation': authoritative,
        'diagnostic_implementation': valid_diagnostic,
    }
    implementations[field_name] = cyclic
    metadata = provider_facets.BuiltinQueryInstancesDiagnosticV1(
        **implementations)
    monkeypatch.setattr(
        provision, 'aws',
        SimpleNamespace(
            query_instances=authoritative,
            _QUERY_INSTANCES_DIAGNOSTIC_V1=metadata,
        ))

    bundle = provision._get_builtin_provisioner_bundle('aws')
    assert bundle is not None
    assert bundle.builtin_query_instances_diagnostic is None
    assert provision.query_instances('aws', 'display',
                                     'cloud') is authoritative_result
    assert authoritative_calls == [('display', 'cloud', None, True, False)]


def test_static_diagnostic_discovery_failure_is_non_authoritative(monkeypatch):
    authoritative_result = object()
    authoritative_calls: list[tuple[Any, ...]] = []
    authoritative = _make_query_implementation(authoritative_result,
                                               authoritative_calls)
    monkeypatch.setattr(provision, 'aws',
                        SimpleNamespace(query_instances=authoritative))
    original_getattr_static = inspect.getattr_static

    def fail_diagnostic_lookup(target: Any, attribute: str, *args: Any) -> Any:
        if attribute == '_QUERY_INSTANCES_DIAGNOSTIC_V1':
            raise RuntimeError('static discovery failed')
        return original_getattr_static(target, attribute, *args)

    monkeypatch.setattr(provision.inspect, 'getattr_static',
                        fail_diagnostic_lookup)

    assert provision.query_instances('aws', 'display',
                                     'cloud') is authoritative_result
    assert authoritative_calls == [('display', 'cloud', None, True, False)]


def test_legacy_partial_plugin_still_falls_back_to_builtin(monkeypatch):
    plugin_result = object()
    builtin_result = object()
    plugin_query = mock.Mock(return_value=plugin_result)
    builtin_cluster_info = mock.Mock(return_value=builtin_result)
    monkeypatch.setattr(provision.aws, 'get_cluster_info', builtin_cluster_info)
    provision.register_provisioner(
        'aws', SimpleNamespace(query_instances=plugin_query))

    assert provision.query_instances('aws', 'display', 'cloud') is plugin_result
    assert provision.get_cluster_info('aws', 'region',
                                      'cloud') is builtin_result
    plugin_query.assert_called_once_with('display', 'cloud')
    builtin_cluster_info.assert_called_once_with('region', 'cloud')


def test_legacy_registration_preserves_module_identity_and_last_wins():
    first = SimpleNamespace()
    replacement = SimpleNamespace()

    provision.register_provisioner('legacy-replacement-test', first)
    provision.register_provisioner('legacy-replacement-test', replacement)

    assert provision.get_registered_provisioner(
        'legacy-replacement-test').module is replacement


def test_mixed_legacy_facet_diagnostic_is_emitted_once(monkeypatch):
    plugin_query = mock.Mock(return_value={})
    builtin_cluster_info = mock.Mock(return_value=object())
    warning = mock.Mock()
    monkeypatch.setattr(provision.aws, 'get_cluster_info', builtin_cluster_info)
    monkeypatch.setattr(provision.logger, 'warning', warning)
    provision.register_provisioner(
        'aws', SimpleNamespace(query_instances=plugin_query))

    provision.get_cluster_info('aws', 'region', 'cloud')
    provision.get_cluster_info('aws', 'region', 'cloud')

    operation = provision._resolve_provisioner('aws').resolve_operation(
        'get_cluster_info')

    warning.assert_called_once()
    assert operation is not None
    assert operation.owner is provision._ProvisionerOperationOwnerV1.BUILTIN
    assert operation.authoritative_implementation is builtin_cluster_info


def test_strict_facet_never_falls_back_inside_declared_group(monkeypatch):
    result = object()
    lifecycle = _RecordingInstanceLifecycle(result)
    builtin_query = mock.Mock(side_effect=AssertionError('unexpected fallback'))
    monkeypatch.setattr(provision.aws, 'query_instances', builtin_query)
    provision.register_provisioner_bundle(
        provider_facets.ProvisionerBundleV1('aws', lifecycle))

    assert provision.query_instances('aws', 'display', 'cloud') is result
    builtin_query.assert_not_called()


def test_lambda_alias_normalizes_lifecycle_and_template_resolution():
    result = object()
    plugin_query = mock.Mock(return_value=result)
    template_override = mock.Mock()
    provision.register_provisioner(
        'lambda_cloud',
        SimpleNamespace(query_instances=plugin_query),
        template_override=template_override)

    assert provision.query_instances('lambda', 'display', 'cloud') is result
    assert provision.get_registered_provisioner(
        'lambda').module.query_instances is plugin_query
    assert provision.get_registered_provisioner(
        'lambda_cloud').module.query_instances is plugin_query
    assert provision.get_provisioner_template_override(
        'lambda') is template_override
    assert provision.get_provisioner_template_override(
        'lambda_cloud') is template_override


def test_strict_lambda_alias_normalizes_registration():
    lifecycle = _RecordingInstanceLifecycle('strict-lambda')
    provision.register_provisioner_bundle(
        provider_facets.ProvisionerBundleV1(
            canonical_name='lambda_cloud',
            instance_lifecycle=lifecycle,
        ))

    canonical = provision.get_provisioner_bundle('lambda')
    historical_alias = provision.get_provisioner_bundle('lambda_cloud')

    assert canonical is historical_alias
    assert canonical.canonical_name == 'lambda'
    assert provision.query_instances('lambda', 'display',
                                     'cloud') == 'strict-lambda'


def test_lambda_aliases_resolve_the_same_builtin_bundle():
    canonical = provision.get_provisioner_bundle('lambda')
    historical_alias = provision.get_provisioner_bundle('lambda_cloud')

    assert canonical.canonical_name == 'lambda'
    assert historical_alias.canonical_name == 'lambda'
    assert canonical.legacy_module is historical_alias.legacy_module


def test_subprocess_can_import_and_reload_provision_module():
    repository_root = Path(__file__).resolve().parents[4]
    result = subprocess.run(
        [
            sys.executable,
            '-c',
            ('import importlib; import sky.provision as provision; '
             'assert importlib.reload(provision) is provision'),
        ],
        cwd=repository_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_public_facade_signature_and_metadata_are_preserved():
    signature = inspect.signature(provision.query_instances)

    assert tuple(signature.parameters) == (
        'provider_name',
        'cluster_name',
        'cluster_name_on_cloud',
        'provider_config',
        'non_terminated_only',
        'retry_if_missing',
    )
    assert provision.query_instances.__name__ == 'query_instances'
    assert provision.query_instances.__module__ == 'sky.provision'
    assert provision.query_instances.__wrapped__ is not None


def test_meaningful_facade_defaults_survive_missing_optional_methods():
    config = object()
    provision.register_provisioner('optional-default-test', SimpleNamespace())

    assert provision.refresh_volume_config('optional-default-test',
                                           config) == (False, config)
    assert not provision.get_all_volumes_errors('optional-default-test', [])
    assert provision.cleanup_cluster_resources('optional-default-test',
                                               'cluster') is None
