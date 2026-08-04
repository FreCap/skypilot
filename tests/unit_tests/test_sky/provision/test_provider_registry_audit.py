"""Facade tests for immutable provider-registry audit capture."""

# These characterization tests intentionally inspect private migration
# baselines and capture helpers exposed by the provider facades.
# pylint: disable=protected-access

import contextlib
import dataclasses
import json
import os
import pathlib
import subprocess
import sys
import threading
from types import MappingProxyType
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

from sky import clouds
from sky import provision
from sky.clouds import cloud as cloud_lib
from sky.provision import provider_facets
from sky.provision import provider_registry_audit
from sky.utils import provider_registration
from sky.utils import registry


def _complete_registration_session(
    context: str = 'main',
) -> provider_registration.ProviderRegistrationBarrierV1:
    with provider_registration.provider_registration_session(
            context) as session:
        return session.complete()


def _mapping_identity(
    mapping: Mapping[Any, Any],) -> tuple[tuple[Any, int], ...]:
    return tuple((key, id(value)) for key, value in mapping.items())


def test_clean_builtin_registry_capture_is_exact_and_deterministic():
    receipt = _complete_registration_session()

    first = provision.capture_provider_registry_audit(receipt)
    second = provision.capture_provider_registry_audit(receipt)

    assert first == second
    assert first.capture_context is provider_registry_audit.ProviderAuditContextV1.MAIN
    assert first.is_conformant
    assert tuple(entry.canonical_name for entry in first.entries) == tuple(
        sorted(registry.CLOUD_REGISTRY))
    assert len(first.entries) == 25
    assert len(provision._BUILTIN_PROVISIONER_AUDIT_BASELINE) == 24
    assert isinstance(clouds._BUILTIN_CLOUD_AUDIT_BASELINE, MappingProxyType)
    assert isinstance(clouds._BUILTIN_CLOUD_ALIAS_AUDIT_BASELINE,
                      MappingProxyType)
    assert isinstance(provision._BUILTIN_PROVISIONER_AUDIT_BASELINE,
                      MappingProxyType)
    assert dict(clouds._BUILTIN_CLOUD_ALIAS_AUDIT_BASELINE) == {
        'digitalocean': 'do',
        'k8s': 'kubernetes',
    }
    assert all(
        registry.CLOUD_REGISTRY[name] is singleton and
        type(singleton) is singleton_type for name, (
            singleton,
            singleton_type) in clouds._BUILTIN_CLOUD_AUDIT_BASELINE.items())
    assert all(
        provision._BUILTIN_PROVISIONER_MODULE_GETTERS[name] is expectation.
        getter and dict.get(expectation.globals_mapping,
                            expectation.global_name) is expectation.module
        for name, expectation in
        provision._BUILTIN_PROVISIONER_AUDIT_BASELINE.items())
    assert len(first.aliases) == 3

    ibm_entry = next(
        entry for entry in first.entries if entry.canonical_name == 'ibm')
    assert ibm_entry.partial_classification.name == (
        'IBM_LEGACY_RAY_CLOUD_ONLY')


def test_clean_subprocess_captures_exact_builtin_inventory(tmp_path):
    repo_root = pathlib.Path(__file__).resolve().parents[4]
    script = """
import json
import pathlib
import sys

import sky
from sky import provision
from sky.provision import provider_registry_audit
from sky.utils import provider_registration
from sky.utils import registry

repo_root = pathlib.Path(sys.argv[1]).resolve()
sky_file = pathlib.Path(sky.__file__).resolve()
assert sky_file == (repo_root / 'sky' / '__init__.py').resolve(), sky_file
with provider_registration.provider_registration_session('main') as session:
    receipt = session.complete()
snapshot = provision.capture_provider_registry_audit(receipt)
cloud_names = tuple(sorted(registry.CLOUD_REGISTRY))
assert tuple(entry.canonical_name for entry in snapshot.entries) == cloud_names
assert len(cloud_names) == 25
assert len(provision._BUILTIN_PROVISIONER_MODULE_GETTERS) == 24
cloud_aliases = {
    (alias.alias.text, alias.target.text)
    for alias in snapshot.aliases
    if alias.source is provider_registry_audit.AliasSourceV1.CLOUD_REGISTRY
}
compatibility_aliases = {
    (alias.alias.text, alias.target.text)
    for alias in snapshot.aliases
    if alias.source is
    provider_registry_audit.AliasSourceV1.PROVISIONER_COMPATIBILITY
}
assert cloud_aliases == {('digitalocean', 'do'), ('k8s', 'kubernetes')}
assert compatibility_aliases == {('lambda_cloud', 'lambda')}
ibm_entry = next(entry for entry in snapshot.entries
                 if entry.canonical_name == 'ibm')
assert ibm_entry.partial_classification is (
    provider_registry_audit.PartialClassificationV1.IBM_LEGACY_RAY_CLOUD_ONLY)
assert snapshot.is_conformant
print('PROVIDER_AUDIT_JSON=' + json.dumps({
    'sky_file': str(sky_file),
    'cloud_entries': len(cloud_names),
    'builtin_getters': len(provision._BUILTIN_PROVISIONER_MODULE_GETTERS),
    'cloud_aliases': len(cloud_aliases),
    'compatibility_aliases': len(compatibility_aliases),
    'ibm_partial': ibm_entry.partial_classification.name,
}, sort_keys=True))
"""
    environment = os.environ.copy()
    environment['PYTHONPATH'] = str(repo_root)

    result = subprocess.run(
        [sys.executable, '-c', script,
         str(repo_root)],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    payload_prefix = 'PROVIDER_AUDIT_JSON='
    payloads = [
        json.loads(line[len(payload_prefix):])
        for line in result.stdout.splitlines()
        if line.startswith(payload_prefix)
    ]
    assert payloads == [{
        'sky_file': str((repo_root / 'sky' / '__init__.py').resolve()),
        'cloud_entries': 25,
        'builtin_getters': 24,
        'cloud_aliases': 2,
        'compatibility_aliases': 1,
        'ibm_partial': 'IBM_LEGACY_RAY_CLOUD_ONLY',
    }]


@pytest.mark.parametrize(
    ('plugin_context', 'audit_context'),
    (
        ('main', provider_registry_audit.ProviderAuditContextV1.MAIN),
        ('uvicorn', provider_registry_audit.ProviderAuditContextV1.UVICORN),
        ('executor', provider_registry_audit.ProviderAuditContextV1.EXECUTOR),
        ('controller',
         provider_registry_audit.ProviderAuditContextV1.CONTROLLER),
    ),
)
def test_capture_maps_exact_plugin_context(plugin_context, audit_context):
    receipt = _complete_registration_session(plugin_context)

    snapshot = provision.capture_provider_registry_audit(receipt)

    assert snapshot.capture_context is audit_context


@pytest.mark.parametrize(
    ('receipt', 'reason'),
    (
        (None, provider_registry_audit.
         ProviderRegistryAuditCaptureErrorReasonV1.MISSING_RECEIPT),
        (object(), provider_registry_audit.
         ProviderRegistryAuditCaptureErrorReasonV1.INVALID_RECEIPT),
    ),
)
def test_capture_maps_receipt_failures(receipt, reason):
    with pytest.raises(provider_registry_audit.
                       ProviderRegistryAuditCaptureErrorV1) as error:
        provision.capture_provider_registry_audit(receipt)

    assert error.value.reason is reason


def test_capture_does_not_mutate_registries_or_resolve_dispatch(monkeypatch):
    cloud_entries_before = _mapping_identity(registry.CLOUD_REGISTRY)
    cloud_aliases_before = tuple(registry.CLOUD_REGISTRY._aliases.items())
    strict_before = _mapping_identity(provision._registered_provisioner_bundles)
    legacy_before = _mapping_identity(provision._registered_provisioners)
    getters_before = _mapping_identity(
        provision._BUILTIN_PROVISIONER_MODULE_GETTERS)
    diagnostics_before = frozenset(provision._legacy_mixed_owner_diagnostics)
    aws_bundle_before = provision.get_provisioner_bundle('aws')
    assert aws_bundle_before is not None
    aws_module_before = aws_bundle_before.legacy_module

    def _unexpected_dispatch(*args, **kwargs):
        del args, kwargs
        raise AssertionError('audit capture entered lifecycle dispatch')

    monkeypatch.setattr(provision, '_resolve_provisioner', _unexpected_dispatch)
    receipt = _complete_registration_session()

    snapshot = provision.capture_provider_registry_audit(receipt)

    assert snapshot.entries
    assert _mapping_identity(registry.CLOUD_REGISTRY) == cloud_entries_before
    assert tuple(
        registry.CLOUD_REGISTRY._aliases.items()) == cloud_aliases_before
    assert _mapping_identity(
        provision._registered_provisioner_bundles) == strict_before
    assert _mapping_identity(
        provision._registered_provisioners) == legacy_before
    assert _mapping_identity(
        provision._BUILTIN_PROVISIONER_MODULE_GETTERS) == getters_before
    assert frozenset(
        provision._legacy_mixed_owner_diagnostics) == diagnostics_before
    aws_bundle_after = provision._get_builtin_provisioner_bundle('aws')
    assert aws_bundle_after is not None
    assert aws_bundle_after.legacy_module is aws_module_before


def test_capture_rejects_stale_and_post_registration_receipts():
    stale_receipt = _complete_registration_session('main')
    _complete_registration_session('executor')

    with pytest.raises(provider_registry_audit.
                       ProviderRegistryAuditCaptureErrorV1) as stale_error:
        provision.capture_provider_registry_audit(stale_receipt)
    assert stale_error.value.reason is (
        provider_registry_audit.ProviderRegistryAuditCaptureErrorReasonV1.
        STALE_EPOCH)

    receipt = _complete_registration_session('main')
    provider_name = 'audit-post-registration-test'
    try:
        provision.register_provisioner(provider_name, SimpleNamespace())
        with pytest.raises(
                provider_registry_audit.ProviderRegistryAuditCaptureErrorV1
        ) as mutation_error:
            provision.capture_provider_registry_audit(receipt)
        assert mutation_error.value.reason is (
            provider_registry_audit.ProviderRegistryAuditCaptureErrorReasonV1.
            STALE_EPOCH)
    finally:
        provision._registered_provisioners.pop(provider_name, None)


def test_replaced_builtin_getter_is_never_invoked():
    provider_name = 'aws'
    original_getter = provision._BUILTIN_PROVISIONER_MODULE_GETTERS[
        provider_name]
    replacement_called = False

    def _replacement_getter():
        nonlocal replacement_called
        replacement_called = True
        raise AssertionError('replacement getter must not be invoked')

    provision._BUILTIN_PROVISIONER_MODULE_GETTERS[
        provider_name] = _replacement_getter
    try:
        receipt = _complete_registration_session()
        snapshot = provision.capture_provider_registry_audit(receipt)
    finally:
        provision._BUILTIN_PROVISIONER_MODULE_GETTERS[
            provider_name] = original_getter

    assert not replacement_called
    assert any(issue.code.name == 'REPLACED_BUILTIN_GETTER'
               for issue in snapshot.issues)


def test_capture_rejects_equal_signatures_with_unequal_snapshots(monkeypatch):
    receipt = _complete_registration_session()
    first_snapshot = provider_registry_audit.ProviderRegistryAuditSnapshotV1(
        schema_version=1,
        capture_context=provider_registry_audit.ProviderAuditContextV1.MAIN,
        entries=(),
        aliases=(),
        unkeyed_registrations=(),
        issues=(),
    )
    second_snapshot = dataclasses.replace(first_snapshot, schema_version=2)
    shared_signature = ('member|stable',)
    observations = iter((
        provider_registry_audit._ProviderRegistryAuditObservationV1(
            first_snapshot, shared_signature),
        provider_registry_audit._ProviderRegistryAuditObservationV1(
            second_snapshot, shared_signature),
    ))

    def _observe(**kwargs):
        del kwargs
        return next(observations)

    monkeypatch.setattr(provider_registry_audit,
                        '_observe_provider_registry_audit', _observe)

    with pytest.raises(provider_registry_audit.
                       ProviderRegistryAuditCaptureErrorV1) as error:
        provision.capture_provider_registry_audit(receipt)

    assert error.value.reason is (
        provider_registry_audit.ProviderRegistryAuditCaptureErrorReasonV1.
        OBSERVED_MEMBER_CHANGED)


@pytest.mark.parametrize('replacement_axis', ('strict', 'legacy'))
def test_supported_replacement_never_exposes_pop_assign_intermediate(
        monkeypatch, replacement_axis):
    provider_name = 'atomic-replacement-test'
    intermediate_reached = threading.Event()
    release_replacement = threading.Event()
    capture_attempting = threading.Event()
    capture_done = threading.Event()
    lifecycle_module = SimpleNamespace(
        **{
            method_name: lambda *args, **kwargs: None
            for method_name in provider_facets.INSTANCE_LIFECYCLE_V1_METHODS
        })
    strict_lifecycle_type = type(
        '_AtomicStrictLifecycle', (), {
            method_name:
                getattr(provider_facets.InstanceLifecycleV1, method_name)
            for method_name in provider_facets.INSTANCE_LIFECYCLE_V1_METHODS
        })
    strict_bundle = provider_facets.ProvisionerBundleV1(
        canonical_name=provider_name,
        instance_lifecycle=strict_lifecycle_type(),
    )
    legacy_registration = provision.Provisioner(module=lifecycle_module)

    class _PausingPopDict(dict):
        """Registry mapping that pauses after removal of the losing side."""

        def pop(self, key, default=None):
            value = super().pop(key, default)
            if key == provider_name:
                intermediate_reached.set()
                assert release_replacement.wait(timeout=5)
            return value

    if replacement_axis == 'legacy':
        strict_entries = _PausingPopDict({provider_name: strict_bundle})
        legacy_entries = {}
    else:
        strict_entries = {}
        legacy_entries = _PausingPopDict({provider_name: legacy_registration})
    monkeypatch.setattr(provision, '_registered_provisioner_bundles',
                        strict_entries)
    monkeypatch.setattr(provision, '_registered_provisioners', legacy_entries)
    receipt = _complete_registration_session()

    original_capture = provider_registration.provider_registration_capture

    @contextlib.contextmanager
    def _signalling_capture(capture_receipt):
        capture_attempting.set()
        # The enclosing contextmanager preserves the wrapped context's cleanup.
        with original_capture(capture_receipt) as context:  # pylint: disable=contextmanager-generator-missing-cleanup
            yield context

    monkeypatch.setattr(provider_registration, 'provider_registration_capture',
                        _signalling_capture)
    monkeypatch.setattr(provider_registration, '_validate_receipt_locked',
                        lambda unused_receipt: 'main')
    registration_errors = []
    capture_errors = []
    captured_snapshots = []

    def _replace_registration():
        try:
            if replacement_axis == 'legacy':
                provision.register_provisioner(provider_name, lifecycle_module)
            else:
                provision.register_provisioner_bundle(strict_bundle)
        except BaseException as error:  # pylint: disable=broad-exception-caught
            registration_errors.append(error)

    def _capture():
        try:
            captured_snapshots.append(
                provision.capture_provider_registry_audit(receipt))
        except BaseException as error:  # pylint: disable=broad-exception-caught
            capture_errors.append(error)
        finally:
            capture_done.set()

    registration_worker = threading.Thread(target=_replace_registration)
    registration_worker.start()
    assert intermediate_reached.wait(timeout=2)
    capture_worker = threading.Thread(target=_capture)
    capture_worker.start()
    assert capture_attempting.wait(timeout=2)
    assert not capture_done.is_set()
    release_replacement.set()
    registration_worker.join(timeout=2)
    capture_worker.join(timeout=2)

    assert not registration_worker.is_alive()
    assert not capture_worker.is_alive()
    assert not registration_errors
    assert not capture_errors
    assert len(captured_snapshots) == 1
    entry = next(entry for entry in captured_snapshots[0].entries
                 if entry.canonical_name == provider_name)
    if replacement_axis == 'legacy':
        assert entry.strict.presence is provider_registry_audit.AuditPresenceV1.ABSENT
        assert entry.legacy.presence is provider_registry_audit.AuditPresenceV1.PRESENT
    else:
        assert entry.strict.presence is provider_registry_audit.AuditPresenceV1.PRESENT
        assert entry.legacy.presence is provider_registry_audit.AuditPresenceV1.ABSENT


def test_direct_audited_member_replacement_between_phases_is_rejected(
        monkeypatch):
    receipt = _complete_registration_session()
    aws_type = type(registry.CLOUD_REGISTRY['aws'])
    original_version = vars(aws_type)['PROVISIONER_VERSION']
    first_observed = threading.Event()
    member_replaced = threading.Event()
    mutation_errors = []
    real_observe = provider_registry_audit._observe_provider_registry_audit
    observation_count = 0

    def _observe(**kwargs):
        nonlocal observation_count
        observation = real_observe(**kwargs)
        observation_count += 1
        if observation_count == 1:
            first_observed.set()
            assert member_replaced.wait(timeout=2)
        return observation

    def _replace_member():
        try:
            assert first_observed.wait(timeout=2)
            aws_type.PROVISIONER_VERSION = (
                cloud_lib.ProvisionerVersion.RAY_AUTOSCALER)
        except BaseException as error:  # pylint: disable=broad-exception-caught
            mutation_errors.append(error)
        finally:
            member_replaced.set()

    monkeypatch.setattr(provider_registry_audit,
                        '_observe_provider_registry_audit', _observe)
    worker = threading.Thread(target=_replace_member)
    worker.start()
    try:
        with pytest.raises(provider_registry_audit.
                           ProviderRegistryAuditCaptureErrorV1) as error:
            provision.capture_provider_registry_audit(receipt)
    finally:
        aws_type.PROVISIONER_VERSION = original_version
        member_replaced.set()
        worker.join(timeout=2)

    assert not worker.is_alive()
    assert not mutation_errors
    assert observation_count == 2
    assert error.value.reason is (
        provider_registry_audit.ProviderRegistryAuditCaptureErrorReasonV1.
        OBSERVED_MEMBER_CHANGED)
