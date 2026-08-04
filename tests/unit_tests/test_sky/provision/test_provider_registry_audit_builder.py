"""Pure characterization tests for provider-registry audit construction."""

# pylint: disable=protected-access

import dataclasses
import gc
import os
import re
import types
from typing import Any, Callable, Mapping
import weakref

import pytest

from sky.clouds import cloud as cloud_lib
from sky.provision import provider_facets
from sky.provision import provider_registry_audit as audit


class _Cloud(cloud_lib.Cloud):
    PROVISIONER_VERSION = cloud_lib.ProvisionerVersion.SKYPILOT
    STATUS_VERSION = cloud_lib.StatusVersion.SKYPILOT
    OPEN_PORTS_VERSION = cloud_lib.OpenPortsVersion.RECONCILABLE


@dataclasses.dataclass(frozen=True)
class _StrictContainer:
    canonical_name: str
    instance_lifecycle: Any
    template_override: Any = None


@dataclasses.dataclass(frozen=True)
class _LegacyContainer:
    module: Any
    template_override: Any = None


class _Lifecycle:
    pass


def _lifecycle_method(*args: Any, **kwargs: Any) -> None:
    del args, kwargs


_PROVIDER_VALUE: Any = None
_PROVIDER_CALLS: list[str] = []
_ATTRIBUTE_RESOLUTION_CALLS: list[str] = []


class _CollidingNamespaceKey:
    """Non-string mapping key colliding with one audited string key."""

    def __init__(self, target: str, calls: list[Any]) -> None:
        self._target = target
        self._calls = calls

    def __hash__(self) -> int:
        return hash(self._target)

    def __eq__(self, other: Any) -> bool:
        self._calls.append(other)
        return False


class _CollidingStringKey(str):
    """String-subclass key whose equality must not run during auditing."""

    def __new__(cls, value: str, calls: list[Any]):
        instance = str.__new__(cls, value)
        instance._calls = calls
        return instance

    def __eq__(self, other: Any) -> bool:
        self._calls.append(other)
        return False

    __hash__ = str.__hash__


def _direct_getter_template() -> Any:
    return _PROVIDER_VALUE


def _mutated_getter_template() -> Any:
    _PROVIDER_CALLS.append('called')
    raise AssertionError('the mutated getter must not run')


def _lifecycle(
    *,
    missing: tuple[str, ...] = (),
    replacements: Mapping[str, Any] | None = None,
) -> _Lifecycle:
    members = {
        name: _lifecycle_method
        for name in provider_facets.INSTANCE_LIFECYCLE_V1_METHODS
        if name not in missing
    }
    if replacements is not None:
        members.update(replacements)
    lifecycle = _Lifecycle()
    for name, member in members.items():
        setattr(lifecycle, name, member)
    return lifecycle


def _getter(value: Any,
            calls: list[str] | None = None,
            label: str = 'getter') -> Callable[[], Any]:
    del calls
    return types.FunctionType(_direct_getter_template.__code__,
                              {'_PROVIDER_VALUE': value}, label)


def _expectation(getter: Callable[[], Any], module: Any) -> tuple[Any, ...]:
    globals_mapping = getter.__globals__
    return (getter, module, types.FunctionType, getter.__code__,
            getter.__defaults__, getter.__kwdefaults__, getter.__closure__,
            globals_mapping, '_PROVIDER_VALUE')


def _observe(
    *,
    cloud_entries: Mapping[Any, Any] | None = None,
    cloud_aliases: Mapping[Any, Any] | None = None,
    strict_entries: Mapping[Any, Any] | None = None,
    legacy_entries: Mapping[Any, Any] | None = None,
    builtin_getters: Mapping[Any, Any] | None = None,
    builtin_cloud_expectations: Mapping[str, tuple[Any, type]] | None = None,
    builtin_alias_expectations: Mapping[str, str] | None = None,
    builtin_provisioner_expectations: Mapping[str, Any] | None = None,
    strict_container_type: type = _StrictContainer,
    legacy_container_type: type = _LegacyContainer,
) -> audit._ProviderRegistryAuditObservationV1:
    return audit._observe_provider_registry_audit(
        capture_context=audit.ProviderAuditContextV1.MAIN,
        cloud_entries={} if cloud_entries is None else cloud_entries,
        cloud_aliases={} if cloud_aliases is None else cloud_aliases,
        strict_entries={} if strict_entries is None else strict_entries,
        legacy_entries={} if legacy_entries is None else legacy_entries,
        builtin_getters={} if builtin_getters is None else builtin_getters,
        builtin_cloud_expectations=({} if builtin_cloud_expectations is None
                                    else builtin_cloud_expectations),
        builtin_alias_expectations=({} if builtin_alias_expectations is None
                                    else builtin_alias_expectations),
        builtin_provisioner_expectations=({} if builtin_provisioner_expectations
                                          is None else
                                          builtin_provisioner_expectations),
        strict_container_type=strict_container_type,
        legacy_container_type=legacy_container_type,
    )


def _entry(snapshot: audit.ProviderRegistryAuditSnapshotV1,
           canonical_name: str) -> audit.ProviderRegistryAuditEntryV1:
    return next(entry for entry in snapshot.entries
                if entry.canonical_name == canonical_name)


def _issue_codes(
    snapshot: audit.ProviderRegistryAuditSnapshotV1,
    canonical_name: str | None = None,
) -> set[audit.ProviderRegistryIssueCodeV1]:
    return {
        issue.code
        for issue in snapshot.issues
        if canonical_name is None or issue.canonical_name == canonical_name
    }


def _builtin_fixture(
    canonical_name: str = 'alpha',
    *,
    module: Any | None = None,
) -> tuple[_Cloud, Any, Callable[[], Any], dict[str, tuple[Any, type]], dict[
        str, tuple[Any, Any]]]:
    cloud = _Cloud()
    module = _lifecycle() if module is None else module
    getter = _getter(module)
    return (cloud, module, getter, {
        canonical_name: (cloud, type(cloud))
    }, {
        canonical_name: _expectation(getter, module)
    })


def test_malformed_and_unreachable_registration_names_remain_unkeyed():
    oversized = 'x' * 129
    observation = _observe(
        cloud_entries={
            'UPPER': _Cloud(),
            ' padded': _Cloud(),
            '': _Cloud(),
            oversized: _Cloud(),
            7: _Cloud(),
        })
    snapshot = observation.snapshot

    assert {
        registration.raw_name.kind
        for registration in snapshot.unkeyed_registrations
    } == {
        audit.AuditRawNameKindV1.INVALID_STRING,
        audit.AuditRawNameKindV1.NON_STRING,
    }
    assert {
        registration.raw_name.text
        for registration in snapshot.unkeyed_registrations
    } == {'UPPER', ' padded', '', None}
    assert all(entry.canonical_name not in ('upper', 'padded', oversized)
               for entry in snapshot.entries)
    assert _issue_codes(snapshot) >= {
        audit.ProviderRegistryIssueCodeV1.UNREACHABLE_PROVIDER_KEY,
        audit.ProviderRegistryIssueCodeV1.MALFORMED_PROVIDER_KEY,
    }


def test_alias_anomalies_are_preserved_without_rewriting_registry_keys():
    alpha = _Cloud()
    collision = _Cloud()
    strict = _StrictContainer('conflict', _lifecycle())
    observation = _observe(
        cloud_entries={
            'alpha': alpha,
            'collision': collision,
        },
        cloud_aliases={
            'Bad Alias': 'alpha',
            'bad-target': 'UPPER',
            'collision': 'alpha',
            'conflict': 'alpha',
            'dangling': 'missing',
            'first': 'second',
            'local': 'alpha',
            'second': 'alpha',
        },
        strict_entries={'conflict': strict},
    )
    snapshot = observation.snapshot

    assert len(snapshot.aliases) == 9  # Eight raw aliases plus lambda_cloud.
    assert _issue_codes(snapshot) >= {
        audit.ProviderRegistryIssueCodeV1.MALFORMED_ALIAS,
        audit.ProviderRegistryIssueCodeV1.ALIAS_CANONICAL_COLLISION,
        audit.ProviderRegistryIssueCodeV1.DANGLING_ALIAS,
        audit.ProviderRegistryIssueCodeV1.ALIAS_TO_ALIAS,
        audit.ProviderRegistryIssueCodeV1.EXCLUDED_ALIAS,
        audit.ProviderRegistryIssueCodeV1.ALIAS_PROVISIONER_CANONICAL_CONFLICT,
    }
    assert tuple(entry.canonical_name for entry in snapshot.entries) == tuple(
        sorted(entry.canonical_name for entry in snapshot.entries))
    assert _entry(snapshot, 'alpha').cloud.identity is not None
    assert all(entry.canonical_name != 'upper' for entry in snapshot.entries)


@pytest.mark.parametrize(
    ('field', 'raw_value', 'expected_kind'),
    (
        ('alias', 'x' * 129, audit.AuditRawNameKindV1.INVALID_STRING),
        ('alias', 7, audit.AuditRawNameKindV1.NON_STRING),
        ('target', 'x' * 129, audit.AuditRawNameKindV1.INVALID_STRING),
        ('target', 7, audit.AuditRawNameKindV1.NON_STRING),
    ),
    ids=('oversized-alias', 'nonstring-alias', 'oversized-target',
         'nonstring-target'),
)
def test_malformed_alias_evidence_is_bounded(field, raw_value, expected_kind):
    cloud_aliases = ({
        raw_value: 'alpha'
    } if field == 'alias' else {
        'alias': raw_value
    })

    snapshot = _observe(
        cloud_entries={
            'alpha': _Cloud()
        },
        cloud_aliases=cloud_aliases,
    ).snapshot
    observed_alias = next(value for value in snapshot.aliases
                          if value.source is audit.AliasSourceV1.CLOUD_REGISTRY)
    raw_name = (observed_alias.alias
                if field == 'alias' else observed_alias.target)

    assert raw_name.kind is expected_kind
    assert raw_name.text is None
    assert raw_name.normalized_text is None
    assert raw_name.identity is not None
    assert len(repr(raw_name)) < 512
    assert audit.ProviderRegistryIssueCodeV1.MALFORMED_ALIAS in (
        _issue_codes(snapshot))


def test_replaced_builtin_getter_is_reported_and_never_called():
    cloud, module, original_getter, cloud_expectations, _ = (_builtin_fixture())
    replacement_called = False

    def replacement_getter() -> Any:
        nonlocal replacement_called
        replacement_called = True
        raise AssertionError('the replacement getter must not run')

    observation = _observe(
        cloud_entries={'alpha': cloud},
        builtin_getters={'alpha': replacement_getter},
        builtin_cloud_expectations=cloud_expectations,
        builtin_provisioner_expectations={
            'alpha': (original_getter, module),
        },
    )

    assert not replacement_called
    assert audit.ProviderRegistryIssueCodeV1.REPLACED_BUILTIN_GETTER in (
        _issue_codes(observation.snapshot, 'alpha'))
    assert _entry(observation.snapshot, 'alpha').builtin.source is (
        audit.RegistrationSourceObservationV1.EXTERNAL_OR_REPLACED)


def test_strict_overlay_owns_the_complete_lifecycle():
    cloud, _, getter, cloud_expectations, expectations = _builtin_fixture()
    strict = _StrictContainer('alpha', _lifecycle())

    snapshot = _observe(
        cloud_entries={
            'alpha': cloud
        },
        strict_entries={
            'alpha': strict
        },
        builtin_getters={
            'alpha': getter
        },
        builtin_cloud_expectations=cloud_expectations,
        builtin_provisioner_expectations=expectations,
    ).snapshot
    entry = _entry(snapshot, 'alpha')

    assert entry.instance_lifecycle.strict_completeness is (
        audit.LifecycleCompletenessV1.COMPLETE)
    assert entry.instance_lifecycle.candidate_owners == (
        audit.LifecycleOwnerV1.STRICT,
        audit.LifecycleOwnerV1.BUILTIN,
        audit.LifecycleOwnerV1.FACADE_DEFAULT,
    )
    assert all(method.effective_owner is audit.LifecycleOwnerV1.STRICT
               for method in entry.instance_lifecycle.methods)
    assert _issue_codes(snapshot, 'alpha') >= {
        audit.ProviderRegistryIssueCodeV1.STRICT_SIGNATURE_UNVERIFIED,
        audit.ProviderRegistryIssueCodeV1.PARALLEL_LIFECYCLE_OWNER,
    }


def test_simultaneous_strict_and_legacy_registration_prefers_strict():
    snapshot = _observe(
        strict_entries={
            'alpha': _StrictContainer('alpha', _lifecycle())
        },
        legacy_entries={
            'alpha': _LegacyContainer(_lifecycle())
        },
    ).snapshot
    entry = _entry(snapshot, 'alpha')

    assert entry.strict.presence is audit.AuditPresenceV1.PRESENT
    assert entry.legacy.presence is audit.AuditPresenceV1.PRESENT
    assert audit.ProviderRegistryIssueCodeV1.STRICT_AND_LEGACY_PRESENT in (
        _issue_codes(snapshot, 'alpha'))
    assert all(method.effective_owner is audit.LifecycleOwnerV1.STRICT
               for method in entry.instance_lifecycle.methods)


def test_complete_and_partial_legacy_overlays_project_exact_owners():
    cloud, _, getter, cloud_expectations, expectations = _builtin_fixture()
    complete_legacy = _LegacyContainer(_lifecycle())
    complete = _observe(
        cloud_entries={
            'alpha': cloud
        },
        legacy_entries={
            'alpha': complete_legacy
        },
        builtin_getters={
            'alpha': getter
        },
        builtin_cloud_expectations=cloud_expectations,
        builtin_provisioner_expectations=expectations,
    ).snapshot
    complete_entry = _entry(complete, 'alpha')

    assert complete_entry.instance_lifecycle.legacy_completeness is (
        audit.LifecycleCompletenessV1.COMPLETE)
    assert not complete_entry.instance_lifecycle.mixes_legacy_and_builtin
    assert all(method.effective_owner is audit.LifecycleOwnerV1.LEGACY
               for method in complete_entry.instance_lifecycle.methods)

    partial_legacy = _LegacyContainer(
        _lifecycle(missing=provider_facets.INSTANCE_LIFECYCLE_V1_METHODS[1:]))
    partial = _observe(
        cloud_entries={
            'alpha': cloud
        },
        legacy_entries={
            'alpha': partial_legacy
        },
        builtin_getters={
            'alpha': getter
        },
        builtin_cloud_expectations=cloud_expectations,
        builtin_provisioner_expectations=expectations,
    ).snapshot
    partial_entry = _entry(partial, 'alpha')

    assert partial_entry.instance_lifecycle.legacy_completeness is (
        audit.LifecycleCompletenessV1.PARTIAL)
    assert partial_entry.instance_lifecycle.mixes_legacy_and_builtin
    assert tuple(method.effective_owner
                 for method in partial_entry.instance_lifecycle.methods) == (
                     audit.LifecycleOwnerV1.LEGACY,
                     *(audit.LifecycleOwnerV1.BUILTIN for _ in range(6)),
                 )
    assert audit.ProviderRegistryIssueCodeV1.MIXED_INSTANCE_LIFECYCLE_OWNER in (
        _issue_codes(partial, 'alpha'))


def test_noncallable_legacy_member_still_wins_and_is_an_error():
    cloud, _, getter, cloud_expectations, expectations = _builtin_fixture()
    legacy = _LegacyContainer(
        _lifecycle(replacements={'run_instances': object()}))

    snapshot = _observe(
        cloud_entries={
            'alpha': cloud
        },
        legacy_entries={
            'alpha': legacy
        },
        builtin_getters={
            'alpha': getter
        },
        builtin_cloud_expectations=cloud_expectations,
        builtin_provisioner_expectations=expectations,
    ).snapshot
    entry = _entry(snapshot, 'alpha')
    run_instances = next(method for method in entry.instance_lifecycle.methods
                         if method.method_name == 'run_instances')

    assert run_instances.legacy.state is (
        audit.LifecycleMemberStateV1.NON_CALLABLE)
    assert run_instances.effective_owner is audit.LifecycleOwnerV1.LEGACY
    assert audit.ProviderRegistryIssueCodeV1.NONCALLABLE_LEGACY_MEMBER in (
        _issue_codes(snapshot, 'alpha'))


def test_template_precedence_handles_absent_and_noncallable_hooks():
    cloud, _, getter, cloud_expectations, expectations = _builtin_fixture()
    strict = _StrictContainer('alpha', _lifecycle(), template_override=None)
    strict_snapshot = _observe(
        cloud_entries={
            'alpha': cloud
        },
        strict_entries={
            'alpha': strict
        },
        builtin_getters={
            'alpha': getter
        },
        builtin_cloud_expectations=cloud_expectations,
        builtin_provisioner_expectations=expectations,
    ).snapshot
    strict_entry = _entry(strict_snapshot, 'alpha')

    assert strict_entry.template_ownership.effective_owner is (
        audit.TemplateOwnerV1.STRICT)
    assert strict_entry.template_ownership.strict.state is (
        audit.LifecycleMemberStateV1.ABSENT)
    assert audit.ProviderRegistryIssueCodeV1.NONCALLABLE_TEMPLATE_OVERRIDE not in (
        _issue_codes(strict_snapshot, 'alpha'))

    legacy = _LegacyContainer(_Lifecycle(), template_override=object())
    legacy_snapshot = _observe(
        cloud_entries={
            'alpha': cloud
        },
        legacy_entries={
            'alpha': legacy
        },
        builtin_getters={
            'alpha': getter
        },
        builtin_cloud_expectations=cloud_expectations,
        builtin_provisioner_expectations=expectations,
    ).snapshot
    legacy_entry = _entry(legacy_snapshot, 'alpha')

    assert legacy_entry.template_ownership.effective_owner is (
        audit.TemplateOwnerV1.LEGACY)
    assert legacy_entry.template_ownership.legacy.state is (
        audit.LifecycleMemberStateV1.NON_CALLABLE)
    assert audit.ProviderRegistryIssueCodeV1.NONCALLABLE_TEMPLATE_OVERRIDE in (
        _issue_codes(legacy_snapshot, 'alpha'))


def test_incomplete_builtin_lifecycle_is_reported_without_fabricating_owner():
    module = _lifecycle(missing=('get_cluster_info',))
    cloud, _, getter, cloud_expectations, expectations = _builtin_fixture(
        module=module)

    snapshot = _observe(
        cloud_entries={
            'alpha': cloud
        },
        builtin_getters={
            'alpha': getter
        },
        builtin_cloud_expectations=cloud_expectations,
        builtin_provisioner_expectations=expectations,
    ).snapshot
    entry = _entry(snapshot, 'alpha')
    missing_method = next(method for method in entry.instance_lifecycle.methods
                          if method.method_name == 'get_cluster_info')

    assert entry.instance_lifecycle.builtin_completeness is (
        audit.LifecycleCompletenessV1.PARTIAL)
    assert missing_method.builtin.state is audit.LifecycleMemberStateV1.ABSENT
    assert missing_method.effective_owner is (
        audit.LifecycleOwnerV1.FACADE_DEFAULT)
    assert audit.ProviderRegistryIssueCodeV1.INCOMPLETE_BUILTIN_LIFECYCLE in (
        _issue_codes(snapshot, 'alpha'))


@pytest.mark.parametrize(
    ('axis', 'classification', 'issue_code'),
    (
        ('strict',
         audit.PartialClassificationV1.UNDECLARED_STRICT_PROVISIONER_ONLY,
         audit.ProviderRegistryIssueCodeV1.UNDECLARED_STRICT_PROVISIONER_ONLY),
        ('legacy',
         audit.PartialClassificationV1.UNDECLARED_LEGACY_PROVISIONER_ONLY,
         audit.ProviderRegistryIssueCodeV1.UNDECLARED_LEGACY_PROVISIONER_ONLY),
    ),
)
def test_provisioner_only_registration_requires_an_explicit_declaration(
        axis: str, classification: audit.PartialClassificationV1,
        issue_code: audit.ProviderRegistryIssueCodeV1):
    strict_entries = ({
        'alpha': _StrictContainer('alpha', _lifecycle())
    } if axis == 'strict' else {})
    legacy_entries = ({
        'alpha': _LegacyContainer(_lifecycle())
    } if axis == 'legacy' else {})

    snapshot = _observe(strict_entries=strict_entries,
                        legacy_entries=legacy_entries).snapshot
    entry = _entry(snapshot, 'alpha')

    assert entry.partial_classification is classification
    assert issue_code in _issue_codes(snapshot, 'alpha')


def test_snapshot_is_recursively_frozen_and_detached_from_input_mappings():
    cloud, _, getter, cloud_expectations, expectations = _builtin_fixture()
    cloud_entries = {'alpha': cloud}
    builtin_getters = {'alpha': getter}
    observation = _observe(
        cloud_entries=cloud_entries,
        builtin_getters=builtin_getters,
        builtin_cloud_expectations=cloud_expectations,
        builtin_provisioner_expectations=expectations,
    )
    snapshot = observation.snapshot

    cloud_entries['later'] = _Cloud()
    builtin_getters.clear()

    assert tuple(
        entry.canonical_name for entry in snapshot.entries) == ('alpha',
                                                                'lambda')
    assert isinstance(snapshot.entries, tuple)
    assert isinstance(snapshot.aliases, tuple)
    assert isinstance(snapshot.issues, tuple)
    assert all(
        isinstance(entry.aliases, tuple) and
        isinstance(entry.instance_lifecycle.methods, tuple) and
        isinstance(entry.issues, tuple) for entry in snapshot.entries)
    with pytest.raises(dataclasses.FrozenInstanceError):
        snapshot.schema_version = 2  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        snapshot.entries[0].canonical_name = 'changed'  # type: ignore[misc]


def test_snapshot_and_signature_are_stable_for_unchanged_registry_identity():
    calls: list[str] = []
    alpha = _Cloud()
    beta = _Cloud()
    alpha_module = _lifecycle()
    beta_module = _lifecycle()
    alpha_getter = _getter(alpha_module, calls, 'alpha')
    beta_getter = _getter(beta_module, calls, 'beta')
    cloud_items = [('beta', beta), ('alpha', alpha)]
    getter_items = [('beta', beta_getter), ('alpha', alpha_getter)]
    cloud_expectations = {
        'alpha': (alpha, type(alpha)),
        'beta': (beta, type(beta)),
    }
    provisioner_expectations = {
        'alpha': _expectation(alpha_getter, alpha_module),
        'beta': _expectation(beta_getter, beta_module),
    }

    first = _observe(
        cloud_entries=dict(cloud_items),
        cloud_aliases={
            'zeta': 'beta',
            'gamma': 'alpha'
        },
        builtin_getters=dict(getter_items),
        builtin_cloud_expectations=cloud_expectations,
        builtin_provisioner_expectations=provisioner_expectations,
    )
    second = _observe(
        cloud_entries=dict(reversed(cloud_items)),
        cloud_aliases={
            'gamma': 'alpha',
            'zeta': 'beta'
        },
        builtin_getters=dict(reversed(getter_items)),
        builtin_cloud_expectations=cloud_expectations,
        builtin_provisioner_expectations=provisioner_expectations,
    )

    assert first.snapshot == second.snapshot
    assert first.signature == second.signature
    assert first.signature == tuple(sorted(first.signature))
    assert not calls
    identities = [
        entry.cloud.identity
        for entry in first.snapshot.entries
        if entry.cloud.identity is not None
    ]
    assert identities
    assert all(
        re.fullmatch(r'[0-9a-f]{32}', identity.process_token)
        for identity in identities)


def test_missing_builtin_cloud_remains_an_entry_with_exact_mismatch():
    expected_cloud = _Cloud()

    snapshot = _observe(builtin_cloud_expectations={
        'alpha': (expected_cloud, type(expected_cloud)),
    },).snapshot
    entry = _entry(snapshot, 'alpha')
    mismatches = [
        issue for issue in snapshot.issues
        if issue.code is audit.ProviderRegistryIssueCodeV1.
        CLOUD_BUILTIN_IDENTITY_MISMATCH and issue.canonical_name == 'alpha'
    ]

    assert entry.cloud.presence is audit.AuditPresenceV1.ABSENT
    assert len(mismatches) == 1
    assert mismatches[0].facet is audit.ProviderRegistryFacetV1.CLOUD
    assert mismatches[0].subject_identity is None


@pytest.mark.parametrize(
    ('cloud_aliases', 'expected_subject'),
    (
        ({}, None),
        ({
            'k8s': 'alpha'
        }, 'alpha'),
    ),
)
def test_builtin_cloud_alias_mismatch_has_exact_attribution(
        cloud_aliases: Mapping[str, str], expected_subject: str | None):
    snapshot = _observe(
        cloud_entries={
            'alpha': _Cloud(),
            'kubernetes': _Cloud(),
        },
        cloud_aliases=cloud_aliases,
        builtin_alias_expectations={
            'k8s': 'kubernetes'
        },
    ).snapshot
    mismatches = [
        issue for issue in snapshot.issues if issue.code is
        audit.ProviderRegistryIssueCodeV1.CLOUD_BUILTIN_ALIAS_MISMATCH
    ]

    assert len(mismatches) == 1
    mismatch = mismatches[0]
    assert mismatch.canonical_name == 'kubernetes'
    assert mismatch.facet is audit.ProviderRegistryFacetV1.ALIAS
    if expected_subject is None:
        assert mismatch.subject_identity is None
    else:
        assert mismatch.subject_identity == audit._runtime_identity(
            expected_subject)


def test_in_place_builtin_getter_code_mutation_is_never_executed():
    module = _lifecycle()
    getter = _getter(module)
    expectation = _expectation(getter, module)
    calls: list[str] = []
    getter_globals = getattr(getter, '__globals__')
    getter_globals[  # pylint: disable=unsupported-assignment-operation
        '_PROVIDER_CALLS'] = calls
    getter.__code__ = _mutated_getter_template.__code__

    snapshot = _observe(
        builtin_getters={
            'alpha': getter
        },
        builtin_provisioner_expectations={
            'alpha': expectation
        },
    ).snapshot

    assert not calls
    assert audit.ProviderRegistryIssueCodeV1.REPLACED_BUILTIN_GETTER in (
        _issue_codes(snapshot, 'alpha'))
    assert _entry(snapshot, 'alpha').builtin.source is (
        audit.RegistrationSourceObservationV1.EXTERNAL_OR_REPLACED)


def test_instance_level_lifecycle_switches_are_projected_exactly():
    cloud = _Cloud()
    cloud.PROVISIONER_VERSION = cloud_lib.ProvisionerVersion.RAY_AUTOSCALER
    cloud.STATUS_VERSION = cloud_lib.StatusVersion.CLOUD_CLI
    cloud.OPEN_PORTS_VERSION = cloud_lib.OpenPortsVersion.LAUNCH_ONLY

    entry = _entry(_observe(cloud_entries={'alpha': cloud}).snapshot, 'alpha')

    assert entry.provisioner_version == audit.LifecycleSwitchAuditV1(
        audit.LifecycleSwitchStateV1.VALID,
        cloud_lib.ProvisionerVersion.RAY_AUTOSCALER)
    assert entry.status_version == audit.LifecycleSwitchAuditV1(
        audit.LifecycleSwitchStateV1.VALID, cloud_lib.StatusVersion.CLOUD_CLI)
    assert entry.open_ports_version == audit.LifecycleSwitchAuditV1(
        audit.LifecycleSwitchStateV1.VALID,
        cloud_lib.OpenPortsVersion.LAUNCH_ONLY)


def test_custom_metaclass_attribute_resolution_is_unsafe_and_never_runs():

    class _CountingMeta(type):

        def __getattribute__(cls, name: str) -> Any:
            _ATTRIBUTE_RESOLUTION_CALLS.append(name)
            return super().__getattribute__(name)

    lifecycle_type = _CountingMeta(
        'UnsafeLifecycle', (), {
            method_name: _lifecycle_method
            for method_name in provider_facets.INSTANCE_LIFECYCLE_V1_METHODS
        })
    _ATTRIBUTE_RESOLUTION_CALLS.clear()
    strict = _StrictContainer('alpha', lifecycle_type)

    entry = _entry(_observe(strict_entries={'alpha': strict}).snapshot, 'alpha')

    assert not _ATTRIBUTE_RESOLUTION_CALLS
    assert all(
        method.strict.state is audit.LifecycleMemberStateV1.UNSAFE_DESCRIPTOR
        for method in entry.instance_lifecycle.methods)
    assert all(method.effective_owner is audit.LifecycleOwnerV1.INDETERMINATE
               for method in entry.instance_lifecycle.methods)
    assert entry.instance_lifecycle.strict_completeness is (
        audit.LifecycleCompletenessV1.INDETERMINATE)


def test_custom_instance_attribute_resolution_is_unsafe_and_never_runs():

    class _UnsafeCloud(_Cloud):

        def __getattribute__(self, name: str) -> Any:
            _ATTRIBUTE_RESOLUTION_CALLS.append(name)
            return super().__getattribute__(name)

    cloud = _UnsafeCloud()
    _ATTRIBUTE_RESOLUTION_CALLS.clear()

    snapshot = _observe(cloud_entries={'alpha': cloud}).snapshot
    entry = _entry(snapshot, 'alpha')

    assert not _ATTRIBUTE_RESOLUTION_CALLS
    assert entry.provisioner_version.state is (
        audit.LifecycleSwitchStateV1.MALFORMED)
    assert entry.status_version.state is (
        audit.LifecycleSwitchStateV1.MALFORMED)
    assert entry.open_ports_version.state is (
        audit.LifecycleSwitchStateV1.MALFORMED)
    assert _issue_codes(snapshot, 'alpha') >= {
        audit.ProviderRegistryIssueCodeV1.MALFORMED_LIFECYCLE_SWITCH,
        audit.ProviderRegistryIssueCodeV1.UNSAFE_OFFER_DECLARATION,
        audit.ProviderRegistryIssueCodeV1.UNSAFE_RESOURCE_SUPPORT_PREDICATE,
    }


def test_explicit_none_members_stay_absent_with_dynamic_getattr():
    calls: list[str] = []

    class _ExplicitNoneLifecycle:

        def __getattr__(self, name: str) -> Any:
            calls.append(name)
            raise AssertionError('dynamic lookup must not run')

    for method_name in provider_facets.INSTANCE_LIFECYCLE_V1_METHODS:
        setattr(_ExplicitNoneLifecycle, method_name, None)
    legacy = _LegacyContainer(_ExplicitNoneLifecycle())

    entry = _entry(_observe(legacy_entries={'alpha': legacy}).snapshot, 'alpha')

    assert not calls
    assert all(method.legacy.state is audit.LifecycleMemberStateV1.ABSENT
               for method in entry.instance_lifecycle.methods)
    assert all(method.effective_owner is audit.LifecycleOwnerV1.FACADE_DEFAULT
               for method in entry.instance_lifecycle.methods)
    assert entry.instance_lifecycle.legacy_completeness is (
        audit.LifecycleCompletenessV1.EMPTY)


def test_malformed_strict_field_is_indeterminate_and_mutation_is_signed():
    strict = _StrictContainer('wrong-name', _lifecycle())

    malformed = _observe(strict_entries={'alpha': strict})
    malformed_entry = _entry(malformed.snapshot, 'alpha')
    object.__setattr__(strict, 'canonical_name', 'alpha')
    corrected = _observe(strict_entries={'alpha': strict})

    assert all(method.effective_owner is audit.LifecycleOwnerV1.INDETERMINATE
               for method in malformed_entry.instance_lifecycle.methods)
    assert audit.ProviderRegistryIssueCodeV1.MALFORMED_STRICT_REGISTRATION in (
        _issue_codes(malformed.snapshot, 'alpha'))
    assert malformed.signature != corrected.signature


def test_malformed_legacy_field_is_indeterminate_and_mutation_is_signed():
    legacy = _LegacyContainer(None)

    malformed = _observe(legacy_entries={'alpha': legacy})
    malformed_entry = _entry(malformed.snapshot, 'alpha')
    object.__setattr__(legacy, 'module', _lifecycle())
    corrected = _observe(legacy_entries={'alpha': legacy})

    assert all(method.effective_owner is audit.LifecycleOwnerV1.INDETERMINATE
               for method in malformed_entry.instance_lifecycle.methods)
    assert audit.ProviderRegistryIssueCodeV1.MALFORMED_LEGACY_REGISTRATION in (
        _issue_codes(malformed.snapshot, 'alpha'))
    assert malformed.signature != corrected.signature


def test_missing_implementations_project_the_nonmeaningful_facade_default():
    entry = _entry(
        _observe(cloud_entries={
            'alpha': _Cloud()
        }).snapshot, 'alpha')

    assert entry.instance_lifecycle.candidate_owners == (
        audit.LifecycleOwnerV1.FACADE_DEFAULT,)
    assert all(not method.facade_has_meaningful_default
               for method in entry.instance_lifecycle.methods)
    assert all(method.effective_owner is audit.LifecycleOwnerV1.FACADE_DEFAULT
               for method in entry.instance_lifecycle.methods)


def test_cloud_and_compatibility_alias_targets_are_source_specific():
    strict = _StrictContainer('alpha', _lifecycle())
    snapshot = _observe(
        cloud_entries={
            'lambda': _Cloud()
        },
        cloud_aliases={
            'cloud-to-alpha': 'alpha'
        },
        strict_entries={
            'alpha': strict
        },
    ).snapshot
    aliases = {(alias.alias.text, alias.target.text, alias.source)
               for alias in snapshot.aliases}
    dangling_targets = {
        issue.canonical_name
        for issue in snapshot.issues
        if issue.code is audit.ProviderRegistryIssueCodeV1.DANGLING_ALIAS
    }

    assert ('cloud-to-alpha', 'alpha',
            audit.AliasSourceV1.CLOUD_REGISTRY) in aliases
    assert ('lambda_cloud', 'lambda',
            audit.AliasSourceV1.PROVISIONER_COMPATIBILITY) in aliases
    assert {'alpha', 'lambda'} <= dangling_targets


@pytest.mark.skipif(not hasattr(os, 'fork'), reason='requires os.fork()')
def test_process_identity_tokens_are_rekeyed_after_fork():
    shared_value = object()
    parent_token = audit._runtime_identity(shared_value).process_token
    read_fd, write_fd = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:
        try:
            os.close(read_fd)
            child_token = audit._runtime_identity(shared_value).process_token
            os.write(write_fd, child_token.encode('ascii'))
        finally:
            os.close(write_fd)
            os._exit(0)

    os.close(write_fd)
    try:
        child_token = os.read(read_fd, 32).decode('ascii')
    finally:
        os.close(read_fd)
    _, status = os.waitpid(child_pid, 0)

    assert os.WIFEXITED(status)
    assert os.WEXITSTATUS(status) == 0
    assert re.fullmatch(r'[0-9a-f]{32}', child_token)
    assert child_token != parent_token


def test_first_observation_strongly_anchors_identity_objects_only_privately():
    cloud = _Cloud()
    cloud_reference = weakref.ref(cloud)

    observation = _observe(cloud_entries={'alpha': cloud})
    snapshot = observation.snapshot
    del cloud
    gc.collect()

    assert cloud_reference() is not None
    assert any(
        value is cloud_reference() for value in observation._identity_anchors)
    del observation
    gc.collect()

    assert snapshot.entries
    assert cloud_reference() is None


def test_type_mro_membership_never_invokes_hostile_metaclass_equality():
    equality_calls: list[Any] = []

    class _HostileEqualityMeta(type):

        def __eq__(cls, other: Any) -> bool:
            equality_calls.append(other)
            return type.__eq__(cls, other)

        __hash__ = type.__hash__

    class _HostileEqualityCloud(_Cloud, metaclass=_HostileEqualityMeta):
        pass

    equality_calls.clear()
    snapshot = _observe(cloud_entries={
        'alpha': _HostileEqualityCloud()
    }).snapshot

    assert not equality_calls
    assert audit.ProviderRegistryIssueCodeV1.WRONG_CLOUD_FACET_TYPE not in (
        _issue_codes(snapshot, 'alpha'))


def test_type_mro_lookup_never_invokes_hostile_metaclass_descriptor():
    descriptor_calls: list[Any] = []

    class _HostileMroDescriptor:

        def __get__(self, instance: Any, owner: Any) -> tuple[()]:
            descriptor_calls.append((instance, owner))
            return ()

    class _HostileMroMeta(type):
        __mro__ = _HostileMroDescriptor()

    class _HostileMroCloud(_Cloud, metaclass=_HostileMroMeta):
        pass

    descriptor_calls.clear()
    snapshot = _observe(cloud_entries={'alpha': _HostileMroCloud()}).snapshot

    assert not descriptor_calls
    assert audit.ProviderRegistryIssueCodeV1.WRONG_CLOUD_FACET_TYPE not in (
        _issue_codes(snapshot, 'alpha'))


def test_strict_container_custom_getattribute_is_malformed_without_execution():
    calls: list[str] = []

    class _UnsafeStrictContainer:

        def __init__(self) -> None:
            self.canonical_name = 'alpha'
            self.instance_lifecycle = _lifecycle()
            self.template_override = None

        def __getattribute__(self, name: str) -> Any:
            calls.append(name)
            return object.__getattribute__(self, name)

    strict = _UnsafeStrictContainer()
    calls.clear()
    snapshot = _observe(
        strict_entries={
            'alpha': strict
        },
        strict_container_type=_UnsafeStrictContainer,
    ).snapshot
    entry = _entry(snapshot, 'alpha')

    assert not calls
    assert audit.ProviderRegistryIssueCodeV1.MALFORMED_STRICT_REGISTRATION in (
        _issue_codes(snapshot, 'alpha'))
    assert entry.instance_lifecycle.strict_completeness is (
        audit.LifecycleCompletenessV1.INDETERMINATE)
    assert all(method.effective_owner is audit.LifecycleOwnerV1.INDETERMINATE
               for method in entry.instance_lifecycle.methods)


def test_legacy_container_custom_getattribute_is_malformed_without_execution():
    calls: list[str] = []

    class _UnsafeLegacyContainer:

        def __init__(self) -> None:
            self.module = _lifecycle()
            self.template_override = None

        def __getattribute__(self, name: str) -> Any:
            calls.append(name)
            return object.__getattribute__(self, name)

    legacy = _UnsafeLegacyContainer()
    calls.clear()
    snapshot = _observe(
        legacy_entries={
            'alpha': legacy
        },
        legacy_container_type=_UnsafeLegacyContainer,
    ).snapshot
    entry = _entry(snapshot, 'alpha')

    assert not calls
    assert audit.ProviderRegistryIssueCodeV1.MALFORMED_LEGACY_REGISTRATION in (
        _issue_codes(snapshot, 'alpha'))
    assert entry.instance_lifecycle.legacy_completeness is (
        audit.LifecycleCompletenessV1.INDETERMINATE)
    assert all(method.effective_owner is audit.LifecycleOwnerV1.INDETERMINATE
               for method in entry.instance_lifecycle.methods)


def test_dynamic_cloud_switch_fallback_is_malformed_without_execution():
    calls: list[str] = []

    class _DynamicSwitchCloud:

        def __getattr__(self, name: str) -> Any:
            calls.append(name)
            raise AssertionError('dynamic switch lookup must not run')

    snapshot = _observe(cloud_entries={'alpha': _DynamicSwitchCloud()}).snapshot
    entry = _entry(snapshot, 'alpha')
    switch_issues = [
        issue for issue in entry.issues if issue.code is
        audit.ProviderRegistryIssueCodeV1.MALFORMED_LIFECYCLE_SWITCH
    ]

    assert not calls
    assert entry.provisioner_version.state is (
        audit.LifecycleSwitchStateV1.MALFORMED)
    assert entry.status_version.state is (
        audit.LifecycleSwitchStateV1.MALFORMED)
    assert entry.open_ports_version.state is (
        audit.LifecycleSwitchStateV1.MALFORMED)
    assert len(switch_issues) == 3


def test_in_place_cloud_bases_change_is_signed_and_reclassifies_facet():

    class _CloudFacetBase(cloud_lib.Cloud):
        pass

    class _PlainBase:
        pass

    class _MutableBasesCloud(_CloudFacetBase):
        """Cloud-shaped test object whose bases can change in place."""

        PROVISIONER_VERSION = cloud_lib.ProvisionerVersion.SKYPILOT
        STATUS_VERSION = cloud_lib.StatusVersion.SKYPILOT
        OPEN_PORTS_VERSION = cloud_lib.OpenPortsVersion.RECONCILABLE

        @classmethod
        def get_offer_source(cls) -> None:
            del cls

        def _unsupported_features_for_resources(self) -> dict[Any, Any]:
            del self
            return {}

    cloud = _MutableBasesCloud()
    original_bases = _MutableBasesCloud.__bases__
    try:
        before = _observe(cloud_entries={'alpha': cloud})
        _MutableBasesCloud.__bases__ = (_PlainBase,)
        after = _observe(cloud_entries={'alpha': cloud})
    finally:
        _MutableBasesCloud.__bases__ = original_bases

    before_entry = _entry(before.snapshot, 'alpha')
    after_entry = _entry(after.snapshot, 'alpha')
    assert before.signature != after.signature
    assert audit.ProviderRegistryIssueCodeV1.WRONG_CLOUD_FACET_TYPE not in (
        _issue_codes(before.snapshot, 'alpha'))
    assert audit.ProviderRegistryIssueCodeV1.WRONG_CLOUD_FACET_TYPE in (
        _issue_codes(after.snapshot, 'alpha'))
    assert before_entry.provisioner_version == after_entry.provisioner_version
    assert before_entry.status_version == after_entry.status_version
    assert before_entry.open_ports_version == after_entry.open_ports_version
    assert before_entry.offer_source_identity == after_entry.offer_source_identity
    assert (before_entry.resource_support_predicate_identity ==
            after_entry.resource_support_predicate_identity)


def test_class_mapping_ignores_colliding_string_subclass_without_equality():
    equality_calls: list[Any] = []
    collision_key = _CollidingStringKey('run_instances', equality_calls)
    lifecycle_type = type('_CollisionLifecycle', (), {collision_key: object()})
    assert any(key is collision_key for key in vars(lifecycle_type))
    equality_calls.clear()

    snapshot = _observe(legacy_entries={
        'alpha': _LegacyContainer(lifecycle_type())
    }).snapshot
    run_instances = next(
        method
        for method in _entry(snapshot, 'alpha').instance_lifecycle.methods
        if method.method_name == 'run_instances')

    assert not equality_calls
    assert run_instances.legacy.state is (
        audit.LifecycleMemberStateV1.UNSAFE_DESCRIPTOR)
    assert run_instances.effective_owner is audit.LifecycleOwnerV1.INDETERMINATE


def test_instance_dict_ignores_colliding_nonstring_key_without_equality():
    equality_calls: list[Any] = []
    cloud = _Cloud()
    collision_key = _CollidingNamespaceKey('PROVISIONER_VERSION',
                                           equality_calls)
    vars(cloud)[collision_key] = object()
    equality_calls.clear()

    entry = _entry(_observe(cloud_entries={'alpha': cloud}).snapshot, 'alpha')

    assert not equality_calls
    assert entry.provisioner_version.state is (
        audit.LifecycleSwitchStateV1.MALFORMED)
    assert audit.ProviderRegistryIssueCodeV1.MALFORMED_LIFECYCLE_SWITCH in (
        issue.code for issue in entry.issues)


def test_module_dict_ignores_colliding_nonstring_key_without_equality():
    equality_calls: list[Any] = []
    lifecycle_module = types.ModuleType('collision_lifecycle')
    collision_key = _CollidingNamespaceKey('run_instances', equality_calls)
    vars(lifecycle_module)[collision_key] = object()
    equality_calls.clear()

    snapshot = _observe(legacy_entries={
        'alpha': _LegacyContainer(lifecycle_module)
    }).snapshot
    run_instances = next(
        method
        for method in _entry(snapshot, 'alpha').instance_lifecycle.methods
        if method.method_name == 'run_instances')

    assert not equality_calls
    assert run_instances.legacy.state is (
        audit.LifecycleMemberStateV1.UNSAFE_DESCRIPTOR)
    assert run_instances.effective_owner is audit.LifecycleOwnerV1.INDETERMINATE


def test_getter_globals_ignore_colliding_nonstring_key_without_equality():
    equality_calls: list[Any] = []
    module = _lifecycle()
    collision_key = _CollidingNamespaceKey('_PROVIDER_VALUE', equality_calls)
    getter_globals: dict[Any, Any] = {collision_key: object()}
    getter_globals['_PROVIDER_VALUE'] = module
    getter = types.FunctionType(_direct_getter_template.__code__,
                                getter_globals, 'collision_getter')
    expectation = _expectation(getter, module)
    equality_calls.clear()

    snapshot = _observe(
        builtin_getters={
            'alpha': getter
        },
        builtin_provisioner_expectations={
            'alpha': expectation
        },
    ).snapshot

    assert not equality_calls
    assert _entry(snapshot, 'alpha').builtin.source is (
        audit.RegistrationSourceObservationV1.EXTERNAL_OR_REPLACED)
    assert (
        audit.ProviderRegistryIssueCodeV1.PROVISIONER_BUILTIN_IDENTITY_MISMATCH
        in _issue_codes(snapshot, 'alpha'))


@pytest.mark.parametrize('axis', ('strict', 'legacy'))
def test_winning_container_data_descriptor_is_malformed_without_execution(
        axis: str):
    descriptor_calls: list[str] = []

    class _UnsafeDataDescriptor:
        """Data descriptor that records any accidental execution."""

        def __get__(self, instance: Any, owner: Any) -> Any:
            del instance, owner
            descriptor_calls.append('get')
            raise AssertionError('container descriptor must not run')

        def __set__(self, instance: Any, value: Any) -> None:
            del instance, value
            descriptor_calls.append('set')
            raise AssertionError('container descriptor must not run')

    if axis == 'strict':

        class _DescriptorStrictContainer:
            instance_lifecycle = _UnsafeDataDescriptor()

            def __init__(self) -> None:
                vars(self).update(canonical_name='alpha',
                                  instance_lifecycle=_lifecycle(),
                                  template_override=None)

        container = _DescriptorStrictContainer()
        snapshot = _observe(
            strict_entries={
                'alpha': container
            },
            strict_container_type=_DescriptorStrictContainer,
        ).snapshot
        malformed_code = (
            audit.ProviderRegistryIssueCodeV1.MALFORMED_STRICT_REGISTRATION)
        completeness = _entry(snapshot,
                              'alpha').instance_lifecycle.strict_completeness
    else:

        class _DescriptorLegacyContainer:
            module = _UnsafeDataDescriptor()

            def __init__(self) -> None:
                vars(self).update(module=_lifecycle(), template_override=None)

        container = _DescriptorLegacyContainer()
        snapshot = _observe(
            legacy_entries={
                'alpha': container
            },
            legacy_container_type=_DescriptorLegacyContainer,
        ).snapshot
        malformed_code = (
            audit.ProviderRegistryIssueCodeV1.MALFORMED_LEGACY_REGISTRATION)
        completeness = _entry(snapshot,
                              'alpha').instance_lifecycle.legacy_completeness

    entry = _entry(snapshot, 'alpha')
    assert not descriptor_calls
    assert malformed_code in _issue_codes(snapshot, 'alpha')
    assert completeness is audit.LifecycleCompletenessV1.INDETERMINATE
    assert all(method.effective_owner is audit.LifecycleOwnerV1.INDETERMINATE
               for method in entry.instance_lifecycle.methods)
