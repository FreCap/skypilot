"""Characterization tests for typed provisioner facets."""

from __future__ import annotations

import dataclasses
import importlib
import inspect
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


def test_strict_registration_rejects_incomplete_instance_lifecycle():
    incomplete_lifecycle = SimpleNamespace(query_instances=mock.Mock())
    bundle = provider_facets.ProvisionerBundleV1(
        canonical_name='incomplete-test',
        instance_lifecycle=incomplete_lifecycle,
    )

    with pytest.raises(ValueError, match='InstanceLifecycleV1'):
        provision.register_provisioner_bundle(bundle)


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


@pytest.mark.parametrize(
    'replacement',
    (
        SimpleNamespace(),
        SimpleNamespace(query_instances=None),
    ),
)
def test_builtin_missing_method_uses_facade_default(monkeypatch, replacement):
    monkeypatch.setattr(provision, 'aws', replacement)

    with pytest.raises(NotImplementedError):
        provision.query_instances('aws', 'display', 'cloud')


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

    warning.assert_called_once()


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
