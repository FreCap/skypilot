"""Immutable read-only observations of the legacy provider registries."""

from __future__ import annotations

from collections.abc import Mapping
import contextvars
import dataclasses
import enum
import hashlib
import hmac
import os
import secrets
import threading
import types
import typing
from typing import Any

from sky.clouds import cloud as cloud_lib
from sky.provision import provider_facets


class AuditPresenceV1(enum.Enum):
    ABSENT = 'absent'
    PRESENT = 'present'


class AuditRuntimeIdentityKindV1(enum.Enum):
    MODULE = 'module'
    CLASS = 'class'
    INSTANCE = 'instance'
    PYTHON_FUNCTION = 'python_function'
    BUILTIN_FUNCTION = 'builtin_function'
    BOUND_METHOD = 'bound_method'
    CALLABLE_OBJECT = 'callable_object'
    DESCRIPTOR = 'descriptor'
    VALUE = 'value'


class AuditRawNameKindV1(enum.Enum):
    VALID_STRING = 'valid_string'
    INVALID_STRING = 'invalid_string'
    NON_STRING = 'non_string'


class RegistrationKindV1(enum.Enum):
    CLOUD = 'cloud'
    BUILTIN_PROVISIONER = 'builtin_provisioner'
    STRICT_PROVISIONER = 'strict_provisioner'
    LEGACY_PROVISIONER = 'legacy_provisioner'


class RegistrationSourceObservationV1(enum.Enum):
    BUILTIN_BASELINE_MATCH = 'builtin_baseline_match'
    STRICT_REGISTRY_OBSERVED = 'strict_registry_observed'
    LEGACY_REGISTRY_OBSERVED = 'legacy_registry_observed'
    EXTERNAL_OR_REPLACED = 'external_or_replaced'


class AliasSourceV1(enum.Enum):
    CLOUD_REGISTRY = 'cloud_registry'
    PROVISIONER_COMPATIBILITY = 'provisioner_compatibility'


class LifecycleSwitchStateV1(enum.Enum):
    ABSENT = 'absent'
    VALID = 'valid'
    MALFORMED = 'malformed'


class LifecycleMemberStateV1(enum.Enum):
    ABSENT = 'absent'
    CALLABLE = 'callable'
    NON_CALLABLE = 'non_callable'
    UNSAFE_DESCRIPTOR = 'unsafe_descriptor'


class LifecycleOwnerV1(enum.Enum):
    STRICT = 'strict'
    LEGACY = 'legacy'
    BUILTIN = 'builtin'
    FACADE_DEFAULT = 'facade_default'
    ABSENT = 'absent'
    INDETERMINATE = 'indeterminate'


class LifecycleCompletenessV1(enum.Enum):
    EMPTY = 'empty'
    PARTIAL = 'partial'
    COMPLETE = 'complete'
    INDETERMINATE = 'indeterminate'


class TemplateOwnerV1(enum.Enum):
    STRICT = 'strict'
    LEGACY = 'legacy'
    BUILTIN = 'builtin'
    ABSENT = 'absent'
    INDETERMINATE = 'indeterminate'


class ProviderAuditContextV1(enum.Enum):
    MAIN = 'main'
    UVICORN = 'uvicorn'
    EXECUTOR = 'executor'
    CONTROLLER = 'controller'


class ProviderRegistryIssueSeverityV1(enum.Enum):
    WARNING = 'warning'
    ERROR = 'error'


class ProviderRegistryFacetV1(enum.Enum):
    REGISTRY_KEY = 'registry_key'
    ALIAS = 'alias'
    CLOUD = 'cloud'
    LIFECYCLE_SWITCH = 'lifecycle_switch'
    INSTANCE_LIFECYCLE = 'instance_lifecycle'
    TEMPLATE_OVERRIDE = 'template_override'
    OFFER_DECLARATION = 'offer_declaration'
    RESOURCE_SUPPORT_PREDICATE = 'resource_support_predicate'


class PartialClassificationV1(enum.Enum):
    NONE = 'none'
    IBM_LEGACY_RAY_CLOUD_ONLY = 'ibm_legacy_ray_cloud_only'
    UNDECLARED_STRICT_PROVISIONER_ONLY = ('undeclared_strict_provisioner_only')
    UNDECLARED_LEGACY_PROVISIONER_ONLY = ('undeclared_legacy_provisioner_only')
    UNEXPECTED_CLOUD_ONLY = 'unexpected_cloud_only'
    UNEXPECTED_BUILTIN_PROVISIONER_ONLY = (
        'unexpected_builtin_provisioner_only')


class ProviderRegistryIssueCodeV1(enum.Enum):
    """Closed set of provider-registry conformance failures."""

    MALFORMED_PROVIDER_KEY = 'malformed_provider_key'
    UNREACHABLE_PROVIDER_KEY = 'unreachable_provider_key'
    MALFORMED_ALIAS = 'malformed_alias'
    CLOUD_BUILTIN_ALIAS_MISMATCH = 'cloud_builtin_alias_mismatch'
    WRONG_CLOUD_FACET_TYPE = 'wrong_cloud_facet_type'
    CLOUD_BUILTIN_IDENTITY_MISMATCH = 'cloud_builtin_identity_mismatch'
    ALIAS_CANONICAL_COLLISION = 'alias_canonical_collision'
    DANGLING_ALIAS = 'dangling_alias'
    ALIAS_TO_ALIAS = 'alias_to_alias'
    EXCLUDED_ALIAS = 'excluded_alias'
    ALIAS_PROVISIONER_CANONICAL_CONFLICT = (
        'alias_provisioner_canonical_conflict')
    MALFORMED_LIFECYCLE_SWITCH = 'malformed_lifecycle_switch'
    PROVISIONER_BUILTIN_IDENTITY_MISMATCH = (
        'provisioner_builtin_identity_mismatch')
    UNDECLARED_STRICT_PROVISIONER_ONLY = ('undeclared_strict_provisioner_only')
    UNDECLARED_LEGACY_PROVISIONER_ONLY = ('undeclared_legacy_provisioner_only')
    UNEXPECTED_CLOUD_ONLY = 'unexpected_cloud_only'
    UNEXPECTED_BUILTIN_PROVISIONER_ONLY = (
        'unexpected_builtin_provisioner_only')
    SKYPILOT_CLOUD_WITHOUT_LIFECYCLE = ('skypilot_cloud_without_lifecycle')
    STRICT_AND_LEGACY_PRESENT = 'strict_and_legacy_present'
    MALFORMED_STRICT_REGISTRATION = 'malformed_strict_registration'
    MALFORMED_LEGACY_REGISTRATION = 'malformed_legacy_registration'
    INCOMPLETE_STRICT_LIFECYCLE = 'incomplete_strict_lifecycle'
    INCOMPLETE_BUILTIN_LIFECYCLE = 'incomplete_builtin_lifecycle'
    STRICT_SIGNATURE_UNVERIFIED = 'strict_signature_unverified'
    NONCALLABLE_LEGACY_MEMBER = 'noncallable_legacy_member'
    UNSAFE_DYNAMIC_MEMBER = 'unsafe_dynamic_member'
    MIXED_INSTANCE_LIFECYCLE_OWNER = 'mixed_instance_lifecycle_owner'
    REPLACED_BUILTIN_GETTER = 'replaced_builtin_getter'
    NONCALLABLE_TEMPLATE_OVERRIDE = 'noncallable_template_override'
    TEMPLATE_OWNER_INDETERMINATE = 'template_owner_indeterminate'
    UNSAFE_OFFER_DECLARATION = 'unsafe_offer_declaration'
    UNSAFE_RESOURCE_SUPPORT_PREDICATE = ('unsafe_resource_support_predicate')
    PARALLEL_LIFECYCLE_OWNER = 'parallel_lifecycle_owner'


class ProviderRegistryAuditCaptureErrorReasonV1(enum.Enum):
    MISSING_RECEIPT = 'missing_receipt'
    INVALID_RECEIPT = 'invalid_receipt'
    WRONG_PROCESS = 'wrong_process'
    STALE_EPOCH = 'stale_epoch'
    ACTIVE_SESSION = 'active_session'
    REGISTRY_CHANGED = 'registry_changed'
    OBSERVED_MEMBER_CHANGED = 'observed_member_changed'


class ProviderRegistryAuditCaptureErrorV1(RuntimeError):
    """Raised when no truthful whole-registry snapshot can be published."""

    def __init__(self, reason: ProviderRegistryAuditCaptureErrorReasonV1):
        self.reason = reason
        super().__init__(f'Provider registry audit capture failed: '
                         f'{reason.value}.')


@dataclasses.dataclass(frozen=True)
class AuditRuntimeIdentityV1:
    kind: AuditRuntimeIdentityKindV1
    module: str | None
    qualname: str | None
    process_token: str = dataclasses.field(repr=False)


@dataclasses.dataclass(frozen=True)
class AuditRawNameV1:
    kind: AuditRawNameKindV1
    text: str | None
    normalized_text: str | None
    identity: AuditRuntimeIdentityV1 | None


@dataclasses.dataclass(frozen=True)
class RegistrationAuditV1:
    presence: AuditPresenceV1
    kind: RegistrationKindV1
    source: RegistrationSourceObservationV1 | None
    identity: AuditRuntimeIdentityV1 | None
    template_identity: AuditRuntimeIdentityV1 | None


@dataclasses.dataclass(frozen=True)
class AliasAuditV1:
    alias: AuditRawNameV1
    target: AuditRawNameV1
    source: AliasSourceV1


@dataclasses.dataclass(frozen=True)
class UnkeyedRegistrationAuditV1:
    kind: RegistrationKindV1
    raw_name: AuditRawNameV1
    identity: AuditRuntimeIdentityV1


@dataclasses.dataclass(frozen=True)
class LifecycleSwitchAuditV1:
    state: LifecycleSwitchStateV1
    value: enum.Enum | None


@dataclasses.dataclass(frozen=True)
class LifecycleMemberAuditV1:
    state: LifecycleMemberStateV1
    identity: AuditRuntimeIdentityV1 | None


@dataclasses.dataclass(frozen=True)
class LifecycleMethodAuditV1:
    method_name: str
    strict: LifecycleMemberAuditV1
    legacy: LifecycleMemberAuditV1
    builtin: LifecycleMemberAuditV1
    facade_has_meaningful_default: bool
    effective_owner: LifecycleOwnerV1


@dataclasses.dataclass(frozen=True)
class InstanceLifecycleAuditV1:
    methods: tuple[LifecycleMethodAuditV1, ...]
    candidate_owners: tuple[LifecycleOwnerV1, ...]
    strict_completeness: LifecycleCompletenessV1
    legacy_completeness: LifecycleCompletenessV1
    builtin_completeness: LifecycleCompletenessV1
    mixes_legacy_and_builtin: bool


@dataclasses.dataclass(frozen=True)
class TemplateOwnershipAuditV1:
    strict: LifecycleMemberAuditV1
    legacy: LifecycleMemberAuditV1
    builtin: LifecycleMemberAuditV1
    effective_owner: TemplateOwnerV1


@dataclasses.dataclass(frozen=True)
class ProviderRegistryAuditIssueV1:
    code: ProviderRegistryIssueCodeV1
    severity: ProviderRegistryIssueSeverityV1
    canonical_name: str | None
    facet: ProviderRegistryFacetV1
    subject_identity: AuditRuntimeIdentityV1 | None


@dataclasses.dataclass(frozen=True)
class ProviderRegistryAuditEntryV1:
    """Frozen projection of all audited facets for one canonical name."""

    canonical_name: str
    aliases: tuple[AliasAuditV1, ...]
    cloud: RegistrationAuditV1
    strict: RegistrationAuditV1
    legacy: RegistrationAuditV1
    builtin: RegistrationAuditV1
    provisioner_version: LifecycleSwitchAuditV1
    status_version: LifecycleSwitchAuditV1
    open_ports_version: LifecycleSwitchAuditV1
    instance_lifecycle: InstanceLifecycleAuditV1
    template_ownership: TemplateOwnershipAuditV1
    offer_source_identity: AuditRuntimeIdentityV1 | None
    resource_support_predicate_identity: AuditRuntimeIdentityV1 | None
    partial_classification: PartialClassificationV1
    issues: tuple[ProviderRegistryAuditIssueV1, ...]


@dataclasses.dataclass(frozen=True)
class ProviderRegistryAuditSnapshotV1:
    """Recursively immutable whole-registry audit result."""

    schema_version: int
    capture_context: ProviderAuditContextV1
    entries: tuple[ProviderRegistryAuditEntryV1, ...]
    aliases: tuple[AliasAuditV1, ...]
    unkeyed_registrations: tuple[UnkeyedRegistrationAuditV1, ...]
    issues: tuple[ProviderRegistryAuditIssueV1, ...]

    @property
    def is_conformant(self) -> bool:
        return not any(issue.severity is ProviderRegistryIssueSeverityV1.ERROR
                       for issue in self.issues)


@dataclasses.dataclass(frozen=True)
class _ProviderRegistryAuditObservationV1:
    snapshot: ProviderRegistryAuditSnapshotV1
    signature: tuple[str, ...]
    _identity_anchors: tuple[Any, ...] = dataclasses.field(repr=False,
                                                           compare=False,
                                                           default=())


_PROCESS_IDENTITY_KEY: bytes | None = None
_PROCESS_IDENTITY_PROCESS_ID: int | None = None
_PROCESS_IDENTITY_LOCK = threading.Lock()
_IDENTITY_ANCHORS: contextvars.ContextVar[list[Any] | None] = (
    contextvars.ContextVar('provider_registry_audit_identity_anchors',
                           default=None))
_MISSING = object()
_UNSAFE_STATIC_LOOKUP = object()
_TYPE_NAMESPACE_DESCRIPTOR = vars(type)['__dict__']
_TYPE_MRO_DESCRIPTOR = vars(type)['__mro__']
_MAX_NAME_LENGTH = 128
_MAX_METADATA_LENGTH = 256
_PROVISIONER_ALIASES = {'lambda_cloud': 'lambda'}

_ISSUE_METADATA: dict[ProviderRegistryIssueCodeV1, tuple[
    ProviderRegistryIssueSeverityV1, ProviderRegistryFacetV1]] = {
        ProviderRegistryIssueCodeV1.MALFORMED_PROVIDER_KEY:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.REGISTRY_KEY),
        ProviderRegistryIssueCodeV1.UNREACHABLE_PROVIDER_KEY:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.REGISTRY_KEY),
        ProviderRegistryIssueCodeV1.MALFORMED_ALIAS:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.ALIAS),
        ProviderRegistryIssueCodeV1.CLOUD_BUILTIN_ALIAS_MISMATCH:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.ALIAS),
        ProviderRegistryIssueCodeV1.WRONG_CLOUD_FACET_TYPE:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.CLOUD),
        ProviderRegistryIssueCodeV1.CLOUD_BUILTIN_IDENTITY_MISMATCH:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.CLOUD),
        ProviderRegistryIssueCodeV1.ALIAS_CANONICAL_COLLISION:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.ALIAS),
        ProviderRegistryIssueCodeV1.DANGLING_ALIAS:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.ALIAS),
        ProviderRegistryIssueCodeV1.ALIAS_TO_ALIAS:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.ALIAS),
        ProviderRegistryIssueCodeV1.EXCLUDED_ALIAS:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.ALIAS),
        ProviderRegistryIssueCodeV1.ALIAS_PROVISIONER_CANONICAL_CONFLICT:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.ALIAS),
        ProviderRegistryIssueCodeV1.MALFORMED_LIFECYCLE_SWITCH:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.LIFECYCLE_SWITCH),
        ProviderRegistryIssueCodeV1.PROVISIONER_BUILTIN_IDENTITY_MISMATCH:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.UNDECLARED_STRICT_PROVISIONER_ONLY:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.UNDECLARED_LEGACY_PROVISIONER_ONLY:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.UNEXPECTED_CLOUD_ONLY:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.CLOUD),
        ProviderRegistryIssueCodeV1.UNEXPECTED_BUILTIN_PROVISIONER_ONLY:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.SKYPILOT_CLOUD_WITHOUT_LIFECYCLE:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.STRICT_AND_LEGACY_PRESENT:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.MALFORMED_STRICT_REGISTRATION:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.MALFORMED_LEGACY_REGISTRATION:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.INCOMPLETE_STRICT_LIFECYCLE:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.INCOMPLETE_BUILTIN_LIFECYCLE:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.STRICT_SIGNATURE_UNVERIFIED:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.NONCALLABLE_LEGACY_MEMBER:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.UNSAFE_DYNAMIC_MEMBER:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.MIXED_INSTANCE_LIFECYCLE_OWNER:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.REPLACED_BUILTIN_GETTER:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
        ProviderRegistryIssueCodeV1.NONCALLABLE_TEMPLATE_OVERRIDE:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.TEMPLATE_OVERRIDE),
        ProviderRegistryIssueCodeV1.TEMPLATE_OWNER_INDETERMINATE:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.TEMPLATE_OVERRIDE),
        ProviderRegistryIssueCodeV1.UNSAFE_OFFER_DECLARATION:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.OFFER_DECLARATION),
        ProviderRegistryIssueCodeV1.UNSAFE_RESOURCE_SUPPORT_PREDICATE:
            (ProviderRegistryIssueSeverityV1.ERROR,
             ProviderRegistryFacetV1.RESOURCE_SUPPORT_PREDICATE),
        ProviderRegistryIssueCodeV1.PARALLEL_LIFECYCLE_OWNER:
            (ProviderRegistryIssueSeverityV1.WARNING,
             ProviderRegistryFacetV1.INSTANCE_LIFECYCLE),
    }


def _bounded_metadata(value: Any) -> str | None:
    if type(value) is not str or len(value) > _MAX_METADATA_LENGTH:
        return None
    return value


def _exact_string_mapping_get(mapping: Mapping[Any, Any], key: str) -> Any:
    """Read one exact-string key without hashing plugin-controlled keys."""
    try:
        items: typing.Iterable[tuple[Any, Any]]
        if type(mapping) is dict:
            items = dict.items(mapping)
        elif type(mapping) is types.MappingProxyType:
            items = types.MappingProxyType.items(mapping)
        else:
            return _UNSAFE_STATIC_LOOKUP
        found = _MISSING
        for raw_key, value in items:
            if type(raw_key) is not str:
                return _UNSAFE_STATIC_LOOKUP
            if raw_key == key:
                found = value
        if found is not _MISSING:
            return found
    except RuntimeError:
        return _UNSAFE_STATIC_LOOKUP
    return _MISSING


def _reset_process_identity_after_fork() -> None:
    global _PROCESS_IDENTITY_KEY, _PROCESS_IDENTITY_LOCK
    global _PROCESS_IDENTITY_PROCESS_ID
    _PROCESS_IDENTITY_KEY = None
    _PROCESS_IDENTITY_PROCESS_ID = None
    _PROCESS_IDENTITY_LOCK = threading.Lock()


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_reset_process_identity_after_fork)


def _process_identity_key() -> bytes:
    global _PROCESS_IDENTITY_KEY, _PROCESS_IDENTITY_PROCESS_ID
    process_id = os.getpid()
    if (_PROCESS_IDENTITY_KEY is not None and
            _PROCESS_IDENTITY_PROCESS_ID == process_id):
        return _PROCESS_IDENTITY_KEY
    with _PROCESS_IDENTITY_LOCK:
        if (_PROCESS_IDENTITY_KEY is None or
                _PROCESS_IDENTITY_PROCESS_ID != process_id):
            _PROCESS_IDENTITY_KEY = secrets.token_bytes(32)
            _PROCESS_IDENTITY_PROCESS_ID = process_id
        return _PROCESS_IDENTITY_KEY


def _process_token(*objects: Any) -> str:
    anchors = _IDENTITY_ANCHORS.get()
    if anchors is not None:
        anchors.extend(objects)
    payload = b'|'.join(str(id(value)).encode('ascii') for value in objects)
    return hmac.new(_process_identity_key(), payload,
                    hashlib.sha256).hexdigest()[:32]


def _type_metadata(value_type: type, attribute: str) -> str | None:
    value = _class_member(value_type, attribute)
    if value is _MISSING or value is _UNSAFE_STATIC_LOOKUP:
        return None
    return _bounded_metadata(value)


def _safe_type_mro(value_type: type) -> tuple[type, ...]:
    try:
        # Invoke the builtin descriptor directly so a plugin metaclass cannot
        # replace ``__mro__`` or intercept the lookup.
        value = _TYPE_MRO_DESCRIPTOR.__get__(  # pylint: disable=unnecessary-dunder-call
            value_type)
    except (AttributeError, TypeError):
        return ()
    return value if type(value) is tuple else ()


def _is_class_object(value: Any) -> bool:
    return any(ancestor is type for ancestor in _safe_type_mro(type(value)))


def _class_namespace(value_type: type) -> Mapping[str, Any] | None:
    try:
        # Invoke the builtin descriptor directly so a plugin metaclass cannot
        # intercept namespace observation through ``__getattribute__``.
        namespace = _TYPE_NAMESPACE_DESCRIPTOR.__get__(  # pylint: disable=unnecessary-dunder-call
            value_type)
    except (AttributeError, TypeError):
        return None
    return namespace if type(namespace) is types.MappingProxyType else None


def _class_member(value_type: type, attribute: str) -> Any:
    for ancestor in _safe_type_mro(value_type):
        namespace = _class_namespace(ancestor)
        if namespace is None:
            return _UNSAFE_STATIC_LOOKUP
        value = _exact_string_mapping_get(namespace, attribute)
        if value is _UNSAFE_STATIC_LOOKUP:
            return _UNSAFE_STATIC_LOOKUP
        if value is not _MISSING:
            return value
    return _MISSING


def _is_data_descriptor(value: Any) -> bool:
    descriptor_type = type(value)
    getter = _class_member(descriptor_type, '__get__')
    setter = _class_member(descriptor_type, '__set__')
    deleter = _class_member(descriptor_type, '__delete__')
    if any(value is _UNSAFE_STATIC_LOOKUP
           for value in (getter, setter, deleter)):
        return True
    return (getter is not _MISSING and
            (setter is not _MISSING or deleter is not _MISSING))


def _safe_instance_namespace(owner: Any,
                             owner_type: type) -> Mapping[str, Any] | object:
    for ancestor in _safe_type_mro(owner_type):
        namespace = _class_namespace(ancestor)
        if namespace is None:
            return _UNSAFE_STATIC_LOOKUP
        descriptor = _exact_string_mapping_get(namespace, '__dict__')
        if descriptor is _UNSAFE_STATIC_LOOKUP:
            return _UNSAFE_STATIC_LOOKUP
        if descriptor is _MISSING:
            continue
        if type(descriptor) is not types.GetSetDescriptorType:
            return _UNSAFE_STATIC_LOOKUP
        try:
            descriptor_name = object.__getattribute__(descriptor, '__name__')
            descriptor_owner = object.__getattribute__(descriptor,
                                                       '__objclass__')
        except (AttributeError, TypeError):
            return _UNSAFE_STATIC_LOOKUP
        if descriptor_name != '__dict__' or descriptor_owner is not ancestor:
            return _UNSAFE_STATIC_LOOKUP
        try:
            # Invoke only a builtin descriptor directly so plugin lookup hooks
            # cannot run while the registry is being audited.
            instance_namespace = descriptor.__get__(  # pylint: disable=unnecessary-dunder-call
                owner, owner_type)
        except (AttributeError, TypeError):
            return _UNSAFE_STATIC_LOOKUP
        return (instance_namespace
                if type(instance_namespace) is dict else _UNSAFE_STATIC_LOOKUP)
    return _MISSING


def _safe_static_getattr(owner: Any, attribute: str) -> Any:
    owner_type = type(owner)
    if _is_class_object(owner):
        class_result = _class_member(owner, attribute)
        metaclass_result = _class_member(owner_type, attribute)
        if (class_result is _UNSAFE_STATIC_LOOKUP or
                metaclass_result is _UNSAFE_STATIC_LOOKUP):
            return _UNSAFE_STATIC_LOOKUP
        if (metaclass_result is not _MISSING and
                _is_data_descriptor(metaclass_result)):
            return metaclass_result
        return class_result if class_result is not _MISSING else metaclass_result

    class_result = _class_member(owner_type, attribute)
    if class_result is _UNSAFE_STATIC_LOOKUP:
        return _UNSAFE_STATIC_LOOKUP
    instance_namespace = _safe_instance_namespace(owner, owner_type)
    if class_result is not _MISSING and _is_data_descriptor(class_result):
        return class_result
    if instance_namespace is _UNSAFE_STATIC_LOOKUP:
        return _UNSAFE_STATIC_LOOKUP
    if type(instance_namespace) is dict:
        instance_value = _exact_string_mapping_get(instance_namespace,
                                                   attribute)
        if instance_value is _UNSAFE_STATIC_LOOKUP:
            return _UNSAFE_STATIC_LOOKUP
        if instance_value is not _MISSING:
            return instance_value
    return class_result


def _runtime_identity(value: Any) -> AuditRuntimeIdentityV1:
    module: str | None = None
    qualname: str | None = None
    token_objects: tuple[Any, ...] = (value,)
    value_type = type(value)

    if value_type is types.ModuleType:
        kind = AuditRuntimeIdentityKindV1.MODULE
        raw_module = _exact_string_mapping_get(vars(value), '__name__')
        module = _bounded_metadata(raw_module)
    elif value_type is types.FunctionType:
        kind = AuditRuntimeIdentityKindV1.PYTHON_FUNCTION
        module = _bounded_metadata(value.__module__)
        qualname = _bounded_metadata(value.__qualname__)
    elif any(value_type is candidate
             for candidate in (types.BuiltinFunctionType,
                               types.BuiltinMethodType)):
        kind = AuditRuntimeIdentityKindV1.BUILTIN_FUNCTION
        module = _bounded_metadata(value.__module__)
        qualname = _bounded_metadata(value.__qualname__)
    elif value_type is types.MethodType:
        kind = AuditRuntimeIdentityKindV1.BOUND_METHOD
        function = value.__func__
        module = _bounded_metadata(function.__module__)
        qualname = _bounded_metadata(function.__qualname__)
        token_objects = (function, value.__self__)
    elif _is_class_object(value):
        kind = AuditRuntimeIdentityKindV1.CLASS
        module = _type_metadata(value, '__module__')
        qualname = _type_metadata(value, '__qualname__')
    else:
        type_module = _type_metadata(value_type, '__module__')
        type_qualname = _type_metadata(value_type, '__qualname__')
        module = type_module
        qualname = type_qualname
        if callable(value):
            kind = AuditRuntimeIdentityKindV1.CALLABLE_OBJECT
        elif _class_member(value_type, '__get__') is not _MISSING:
            kind = AuditRuntimeIdentityKindV1.DESCRIPTOR
        elif type_module != 'builtins':
            kind = AuditRuntimeIdentityKindV1.INSTANCE
        else:
            kind = AuditRuntimeIdentityKindV1.VALUE

    return AuditRuntimeIdentityV1(kind=kind,
                                  module=module,
                                  qualname=qualname,
                                  process_token=_process_token(*token_objects))


def _raw_name(value: Any) -> AuditRawNameV1:
    if type(value) is not str:
        return AuditRawNameV1(AuditRawNameKindV1.NON_STRING, None, None,
                              _runtime_identity(value))
    if len(value) > _MAX_NAME_LENGTH:
        return AuditRawNameV1(AuditRawNameKindV1.INVALID_STRING, None, None,
                              _runtime_identity(value))
    normalized = value.lower()
    is_valid = bool(value) and value == value.strip() and value == normalized
    if is_valid:
        return AuditRawNameV1(AuditRawNameKindV1.VALID_STRING, value,
                              normalized, None)
    return AuditRawNameV1(AuditRawNameKindV1.INVALID_STRING, value, normalized,
                          _runtime_identity(value))


def _raw_name_sort_key(value: AuditRawNameV1,) -> tuple[str, str, str, str]:
    return (value.kind.value, value.normalized_text or '', value.text or '',
            value.identity.process_token if value.identity is not None else '')


def _alias_sort_key(value: AliasAuditV1) -> tuple[Any, ...]:
    return (value.source.value, _raw_name_sort_key(value.alias),
            _raw_name_sort_key(value.target))


def _issue_sort_key(value: ProviderRegistryAuditIssueV1,) -> tuple[str, ...]:
    return (value.severity.value, value.code.value, value.canonical_name or
            '', value.facet.value, value.subject_identity.process_token
            if value.subject_identity is not None else '')


def _issue(
    code: ProviderRegistryIssueCodeV1,
    canonical_name: str | None = None,
    subject: Any = _MISSING,
) -> ProviderRegistryAuditIssueV1:
    severity, facet = _ISSUE_METADATA[code]
    identity = None if subject is _MISSING else _runtime_identity(subject)
    return ProviderRegistryAuditIssueV1(code, severity, canonical_name, facet,
                                        identity)


def _mapping_items(mapping: Mapping[Any, Any]) -> tuple[tuple[Any, Any], ...]:
    try:
        if any(ancestor is dict for ancestor in _safe_type_mro(type(mapping))):
            raw_dict = typing.cast(dict[Any, Any], mapping)
            return tuple(dict.items(raw_dict))
        if type(mapping) is types.MappingProxyType:
            return tuple(mapping.items())
    except RuntimeError as error:
        raise ProviderRegistryAuditCaptureErrorV1(
            ProviderRegistryAuditCaptureErrorReasonV1.REGISTRY_CHANGED
        ) from error
    raise ProviderRegistryAuditCaptureErrorV1(
        ProviderRegistryAuditCaptureErrorReasonV1.REGISTRY_CHANGED)


def _absent_registration(kind: RegistrationKindV1) -> RegistrationAuditV1:
    return RegistrationAuditV1(AuditPresenceV1.ABSENT, kind, None, None, None)


def _attribute_resolution_hooks(owner: Any) -> tuple[Any, Any]:
    """Return custom ``__getattribute__`` and ``__getattr__`` statically."""
    if type(owner) is types.ModuleType:
        namespace = vars(owner)
        dynamic_getattr = _exact_string_mapping_get(namespace, '__getattr__')
        if dynamic_getattr is _UNSAFE_STATIC_LOOKUP:
            dynamic_getattr = _MISSING
        return _MISSING, dynamic_getattr

    owner_type = type(owner)
    default_getattribute = object.__getattribute__
    if _is_class_object(owner):
        default_getattribute = type.__getattribute__
    elif any(ancestor is types.ModuleType
             for ancestor in _safe_type_mro(owner_type)):
        default_getattribute = types.ModuleType.__getattribute__
    current_getattribute = _class_member(owner_type, '__getattribute__')
    custom_getattribute = (
        current_getattribute if current_getattribute is not _MISSING and
        current_getattribute is not default_getattribute else _MISSING)
    dynamic_getattr = _class_member(owner_type, '__getattr__')
    return custom_getattribute, dynamic_getattr


def _member_observation(
    owner: Any,
    member_name: str,
    signature: list[str],
    signature_label: str,
) -> LifecycleMemberAuditV1:
    if owner is _MISSING or owner is None:
        observation = LifecycleMemberAuditV1(LifecycleMemberStateV1.ABSENT,
                                             None)
        signature.append(f'member|{signature_label}|absent')
        return observation

    custom_getattribute, dynamic_getattr = _attribute_resolution_hooks(owner)
    if type(owner) is types.ModuleType:
        namespace = vars(owner)
        raw_member = _exact_string_mapping_get(namespace, member_name)
    else:
        raw_member = _safe_static_getattr(owner, member_name)

    identity: AuditRuntimeIdentityV1 | None
    if custom_getattribute is not _MISSING:
        state = LifecycleMemberStateV1.UNSAFE_DESCRIPTOR
        identity = _runtime_identity(custom_getattribute)
    elif raw_member is _UNSAFE_STATIC_LOOKUP:
        state = LifecycleMemberStateV1.UNSAFE_DESCRIPTOR
        identity = _runtime_identity(type(owner))
    elif raw_member is _MISSING:
        state = (LifecycleMemberStateV1.UNSAFE_DESCRIPTOR if dynamic_getattr
                 is not _MISSING else LifecycleMemberStateV1.ABSENT)
        identity = (_runtime_identity(dynamic_getattr)
                    if dynamic_getattr is not _MISSING else None)
    elif raw_member is None:
        state = LifecycleMemberStateV1.ABSENT
        identity = None
    else:
        member = raw_member
        member_type = type(member)
        if any(member_type is candidate
               for candidate in (staticmethod, classmethod)):
            member = member.__func__
        member_type = type(member)
        if any(member_type is candidate
               for candidate in (types.FunctionType, types.BuiltinFunctionType,
                                 types.BuiltinMethodType, types.MethodType)):
            state = LifecycleMemberStateV1.CALLABLE
        elif _class_member(type(member), '__get__') is not _MISSING:
            state = LifecycleMemberStateV1.UNSAFE_DESCRIPTOR
        elif callable(member):
            state = LifecycleMemberStateV1.CALLABLE
        else:
            state = LifecycleMemberStateV1.NON_CALLABLE
        identity = _runtime_identity(member)

    token = identity.process_token if identity is not None else ''
    signature.append(f'member|{signature_label}|{state.value}|{token}')
    return LifecycleMemberAuditV1(state, identity)


def _static_field(
    owner: Any,
    field_name: str,
    signature: list[str],
    signature_label: str,
) -> Any:
    if owner is _MISSING or owner is None:
        signature.append(f'field|{signature_label}|missing')
        return _MISSING
    custom_getattribute, dynamic_getattr = _attribute_resolution_hooks(owner)
    if custom_getattribute is not _MISSING:
        signature.append(
            f'field|{signature_label}|unsafe|'
            f'{_runtime_identity(custom_getattribute).process_token}')
        return _MISSING

    unsafe_subject: Any = type(owner)
    if type(owner) is types.ModuleType or _is_class_object(owner):
        value = _safe_static_getattr(owner, field_name)
        if (value is not _MISSING and value is not _UNSAFE_STATIC_LOOKUP and
                _class_member(type(value), '__get__') is not _MISSING):
            unsafe_subject = value
            value = _UNSAFE_STATIC_LOOKUP
    else:
        owner_type = type(owner)
        class_value = _class_member(owner_type, field_name)
        if class_value is _UNSAFE_STATIC_LOOKUP:
            value = _UNSAFE_STATIC_LOOKUP
        elif (class_value is not _MISSING and _is_data_descriptor(class_value)):
            unsafe_subject = class_value
            value = _UNSAFE_STATIC_LOOKUP
        else:
            instance_namespace = _safe_instance_namespace(owner, owner_type)
            if instance_namespace is _UNSAFE_STATIC_LOOKUP:
                value = _UNSAFE_STATIC_LOOKUP
            elif type(instance_namespace) is dict:
                instance_value = _exact_string_mapping_get(
                    instance_namespace, field_name)
                if instance_value is _UNSAFE_STATIC_LOOKUP:
                    value = _UNSAFE_STATIC_LOOKUP
                elif instance_value is not _MISSING:
                    value = instance_value
                else:
                    value = class_value
            else:
                value = class_value
            if (value is class_value and value is not _MISSING and
                    _class_member(type(value), '__get__') is not _MISSING):
                unsafe_subject = value
                value = _UNSAFE_STATIC_LOOKUP
    if value is _UNSAFE_STATIC_LOOKUP:
        signature.append(f'field|{signature_label}|unsafe|'
                         f'{_runtime_identity(unsafe_subject).process_token}')
        return _MISSING
    if value is _MISSING and dynamic_getattr is not _MISSING:
        signature.append(f'field|{signature_label}|unsafe|'
                         f'{_runtime_identity(dynamic_getattr).process_token}')
        return _MISSING
    if value is _MISSING:
        signature.append(f'field|{signature_label}|missing')
    elif value is None:
        signature.append(f'field|{signature_label}|none')
    else:
        signature.append(f'field|{signature_label}|'
                         f'{_runtime_identity(value).process_token}')
    return value


def _builtin_expectation_fields(value: Any) -> tuple[Any, ...] | None:
    if (not any(ancestor is tuple for ancestor in _safe_type_mro(type(value)))
            or tuple.__len__(value) != 9):
        return None
    return tuple(tuple.__getitem__(value, index) for index in range(9))


def _completeness(
    members: tuple[LifecycleMemberAuditV1, ...],) -> LifecycleCompletenessV1:
    states = tuple(member.state for member in members)
    if any(state is LifecycleMemberStateV1.UNSAFE_DESCRIPTOR
           for state in states):
        return LifecycleCompletenessV1.INDETERMINATE
    callable_count = sum(
        state is LifecycleMemberStateV1.CALLABLE for state in states)
    if callable_count == len(states):
        return LifecycleCompletenessV1.COMPLETE
    if all(state is LifecycleMemberStateV1.ABSENT for state in states):
        return LifecycleCompletenessV1.EMPTY
    return LifecycleCompletenessV1.PARTIAL


def _observe_switch(
    cloud: Any,
    attribute: str,
    expected_type: type[enum.Enum],
    canonical_name: str,
    signature: list[str],
    issues: list[ProviderRegistryAuditIssueV1],
) -> LifecycleSwitchAuditV1:
    if cloud is _MISSING:
        signature.append(f'member|{canonical_name}|switch|{attribute}|absent')
        return LifecycleSwitchAuditV1(LifecycleSwitchStateV1.ABSENT, None)
    custom_getattribute, dynamic_getattr = _attribute_resolution_hooks(cloud)
    value = _safe_static_getattr(cloud, attribute)
    if custom_getattribute is not _MISSING:
        state = LifecycleSwitchStateV1.MALFORMED
        result = None
        value = custom_getattribute
        issues.append(
            _issue(ProviderRegistryIssueCodeV1.MALFORMED_LIFECYCLE_SWITCH,
                   canonical_name, custom_getattribute))
    elif value is _UNSAFE_STATIC_LOOKUP:
        state = LifecycleSwitchStateV1.MALFORMED
        result = None
        issues.append(
            _issue(ProviderRegistryIssueCodeV1.MALFORMED_LIFECYCLE_SWITCH,
                   canonical_name, type(cloud)))
    elif value is _MISSING and dynamic_getattr is not _MISSING:
        state = LifecycleSwitchStateV1.MALFORMED
        result = None
        value = dynamic_getattr
        issues.append(
            _issue(ProviderRegistryIssueCodeV1.MALFORMED_LIFECYCLE_SWITCH,
                   canonical_name, dynamic_getattr))
    elif value is _MISSING:
        state = LifecycleSwitchStateV1.ABSENT
        result = None
    elif type(value) is expected_type:
        state = LifecycleSwitchStateV1.VALID
        result = value
    else:
        state = LifecycleSwitchStateV1.MALFORMED
        result = None
        issues.append(
            _issue(ProviderRegistryIssueCodeV1.MALFORMED_LIFECYCLE_SWITCH,
                   canonical_name, value))
    token = '' if value is _MISSING else _runtime_identity(value).process_token
    signature.append(f'member|{canonical_name}|switch|{attribute}|'
                     f'{state.value}|{token}')
    return LifecycleSwitchAuditV1(state, result)


def _entry_issue_subset(
    issues: typing.Iterable[ProviderRegistryAuditIssueV1],
    canonical_name: str,
) -> tuple[ProviderRegistryAuditIssueV1, ...]:
    return tuple(
        sorted((issue for issue in issues
                if issue.canonical_name == canonical_name),
               key=_issue_sort_key))


def _build_provider_registry_audit(
    *,
    capture_context: ProviderAuditContextV1,
    cloud_entries: Mapping[Any, Any],
    cloud_aliases: Mapping[Any, Any],
    strict_entries: Mapping[Any, Any],
    legacy_entries: Mapping[Any, Any],
    builtin_getters: Mapping[Any, Any],
    builtin_cloud_expectations: Mapping[str, tuple[Any, type]],
    builtin_alias_expectations: Mapping[str, str],
    builtin_provisioner_expectations: Mapping[str, Any],
    strict_container_type: type,
    legacy_container_type: type,
) -> _ProviderRegistryAuditObservationV1:
    """Observe raw registry axes without invoking provider implementations."""
    if type(capture_context) is not ProviderAuditContextV1:
        raise ProviderRegistryAuditCaptureErrorV1(
            ProviderRegistryAuditCaptureErrorReasonV1.INVALID_RECEIPT)

    raw_axes = (
        (RegistrationKindV1.CLOUD, _mapping_items(cloud_entries)),
        (RegistrationKindV1.STRICT_PROVISIONER, _mapping_items(strict_entries)),
        (RegistrationKindV1.LEGACY_PROVISIONER, _mapping_items(legacy_entries)),
        (RegistrationKindV1.BUILTIN_PROVISIONER,
         _mapping_items(builtin_getters)),
    )
    alias_items = _mapping_items(cloud_aliases)
    cloud_expectations = dict(_mapping_items(builtin_cloud_expectations))
    expected_aliases = dict(_mapping_items(builtin_alias_expectations))
    provisioner_expectations = dict(
        _mapping_items(builtin_provisioner_expectations))

    signature: list[str] = []
    issues: list[ProviderRegistryAuditIssueV1] = []
    unkeyed: list[UnkeyedRegistrationAuditV1] = []
    valid_axes: dict[RegistrationKindV1, dict[str, Any]] = {
        kind: {} for kind, _ in raw_axes
    }

    for kind, items in raw_axes:
        for raw_key, value in items:
            name = _raw_name(raw_key)
            signature.append(f'raw|{kind.value}|'
                             f'{_runtime_identity(raw_key).process_token}|'
                             f'{_runtime_identity(value).process_token}')
            if name.kind is AuditRawNameKindV1.VALID_STRING:
                assert name.text is not None
                valid_axes[kind][name.text] = value
                continue
            code = (ProviderRegistryIssueCodeV1.UNREACHABLE_PROVIDER_KEY
                    if type(raw_key) is str and raw_key and
                    len(raw_key) <= _MAX_NAME_LENGTH else
                    ProviderRegistryIssueCodeV1.MALFORMED_PROVIDER_KEY)
            issues.append(_issue(code, subject=raw_key))
            unkeyed.append(
                UnkeyedRegistrationAuditV1(kind, name,
                                           _runtime_identity(value)))

    aliases: list[AliasAuditV1] = []
    for raw_alias, raw_target in alias_items:
        alias = AliasAuditV1(_raw_name(raw_alias), _raw_name(raw_target),
                             AliasSourceV1.CLOUD_REGISTRY)
        aliases.append(alias)
        signature.append(
            f'raw|alias|{_runtime_identity(raw_alias).process_token}|'
            f'{_runtime_identity(raw_target).process_token}')
        if (alias.alias.kind is not AuditRawNameKindV1.VALID_STRING or
                alias.target.kind is not AuditRawNameKindV1.VALID_STRING):
            malformed_subject = (raw_alias if alias.alias.kind
                                 is not AuditRawNameKindV1.VALID_STRING else
                                 raw_target)
            issues.append(
                _issue(ProviderRegistryIssueCodeV1.MALFORMED_ALIAS,
                       subject=malformed_subject))

    for raw_alias, raw_target in _PROVISIONER_ALIASES.items():
        aliases.append(
            AliasAuditV1(_raw_name(raw_alias), _raw_name(raw_target),
                         AliasSourceV1.PROVISIONER_COMPATIBILITY))
        signature.append(f'raw|compat_alias|{raw_alias}|{raw_target}')

    cloud_axis = valid_axes[RegistrationKindV1.CLOUD]
    strict_axis = valid_axes[RegistrationKindV1.STRICT_PROVISIONER]
    legacy_axis = valid_axes[RegistrationKindV1.LEGACY_PROVISIONER]
    builtin_axis = valid_axes[RegistrationKindV1.BUILTIN_PROVISIONER]
    cloud_alias_names = {
        alias.alias.text
        for alias in aliases
        if alias.source is AliasSourceV1.CLOUD_REGISTRY and
        alias.alias.kind is AuditRawNameKindV1.VALID_STRING
    }
    provisioner_alias_names = {
        alias.alias.text
        for alias in aliases
        if alias.source is AliasSourceV1.PROVISIONER_COMPATIBILITY and
        alias.alias.kind is AuditRawNameKindV1.VALID_STRING
    }
    provisioner_keys = set(strict_axis) | set(legacy_axis) | set(builtin_axis)

    for alias in aliases:
        if (alias.alias.kind is not AuditRawNameKindV1.VALID_STRING or
                alias.target.kind is not AuditRawNameKindV1.VALID_STRING):
            continue
        assert alias.alias.text is not None and alias.target.text is not None
        alias_name = alias.alias.text
        target = alias.target.text
        is_cloud_alias = alias.source is AliasSourceV1.CLOUD_REGISTRY
        target_keys = cloud_axis if is_cloud_alias else provisioner_keys
        source_alias_names = (cloud_alias_names
                              if is_cloud_alias else provisioner_alias_names)
        if is_cloud_alias and alias_name == 'local':
            issues.append(
                _issue(ProviderRegistryIssueCodeV1.EXCLUDED_ALIAS, target))
        if is_cloud_alias and alias_name in cloud_axis:
            issues.append(
                _issue(ProviderRegistryIssueCodeV1.ALIAS_CANONICAL_COLLISION,
                       target))
        if target not in target_keys:
            code = (ProviderRegistryIssueCodeV1.ALIAS_TO_ALIAS
                    if target in source_alias_names else
                    ProviderRegistryIssueCodeV1.DANGLING_ALIAS)
            issues.append(_issue(code, target))
        if alias_name in provisioner_keys and alias_name != target:
            issues.append(
                _issue(
                    ProviderRegistryIssueCodeV1.
                    ALIAS_PROVISIONER_CANONICAL_CONFLICT, target))

    current_alias_values = {
        raw_alias: raw_target
        for raw_alias, raw_target in alias_items
        if type(raw_alias) is str
    }
    for alias_name, expected_target in expected_aliases.items():
        observed_target = current_alias_values.get(alias_name, _MISSING)
        matches = (type(observed_target) is str and
                   type(expected_target) is str and
                   observed_target == expected_target)
        if not matches:
            issues.append(
                _issue(
                    ProviderRegistryIssueCodeV1.CLOUD_BUILTIN_ALIAS_MISMATCH,
                    expected_target if type(expected_target) is str else None,
                    observed_target))

    canonical_names = (set(cloud_axis) | set(strict_axis) | set(legacy_axis) |
                       set(builtin_axis) | set(cloud_expectations))
    canonical_names.update(
        alias.target.text
        for alias in aliases
        if alias.target.kind is AuditRawNameKindV1.VALID_STRING and
        alias.target.text is not None)

    entries: list[ProviderRegistryAuditEntryV1] = []
    for canonical_name in sorted(canonical_names):
        entry_issues: list[ProviderRegistryAuditIssueV1] = []
        cloud_value = cloud_axis.get(canonical_name, _MISSING)
        strict_value = strict_axis.get(canonical_name, _MISSING)
        legacy_value = legacy_axis.get(canonical_name, _MISSING)
        getter = builtin_axis.get(canonical_name, _MISSING)

        expected_cloud = cloud_expectations.get(canonical_name)
        if cloud_value is _MISSING:
            cloud_registration = _absent_registration(RegistrationKindV1.CLOUD)
            if expected_cloud is not None:
                entry_issues.append(
                    _issue(
                        ProviderRegistryIssueCodeV1.
                        CLOUD_BUILTIN_IDENTITY_MISMATCH, canonical_name))
        else:
            source = RegistrationSourceObservationV1.EXTERNAL_OR_REPLACED
            if (type(expected_cloud) is tuple and len(expected_cloud) == 2 and
                    cloud_value is expected_cloud[0] and
                    type(cloud_value) is expected_cloud[1]):
                source = RegistrationSourceObservationV1.BUILTIN_BASELINE_MATCH
            elif expected_cloud is not None:
                entry_issues.append(
                    _issue(
                        ProviderRegistryIssueCodeV1.
                        CLOUD_BUILTIN_IDENTITY_MISMATCH, canonical_name,
                        cloud_value))
            cloud_mro = _safe_type_mro(type(cloud_value))
            signature.append(f'member|{canonical_name}|cloud_mro|' + '|'.join(
                _runtime_identity(ancestor).process_token
                for ancestor in cloud_mro))
            if not any(ancestor is cloud_lib.Cloud for ancestor in cloud_mro):
                entry_issues.append(
                    _issue(ProviderRegistryIssueCodeV1.WRONG_CLOUD_FACET_TYPE,
                           canonical_name, cloud_value))
            cloud_registration = RegistrationAuditV1(
                AuditPresenceV1.PRESENT, RegistrationKindV1.CLOUD, source,
                _runtime_identity(cloud_value), None)
            signature.append(
                f'member|{canonical_name}|cloud|'
                f'{_runtime_identity(cloud_value).process_token}|'
                f'{_runtime_identity(type(cloud_value)).process_token}')

        strict_lifecycle = _MISSING
        strict_template = _MISSING
        strict_malformed = False
        if strict_value is _MISSING:
            strict_registration = _absent_registration(
                RegistrationKindV1.STRICT_PROVISIONER)
        else:
            strict_identity = _runtime_identity(strict_value)
            if type(strict_value) is not strict_container_type:
                strict_malformed = True
            else:
                strict_lifecycle = _static_field(
                    strict_value, 'instance_lifecycle', signature,
                    f'{canonical_name}|strict|'
                    'instance_lifecycle')
                strict_template = _static_field(
                    strict_value, 'template_override', signature,
                    f'{canonical_name}|strict|'
                    'template_override')
                declared_name = _static_field(
                    strict_value, 'canonical_name', signature,
                    f'{canonical_name}|strict|canonical_name')
                strict_malformed = (strict_lifecycle is _MISSING or
                                    strict_lifecycle is None or
                                    strict_template is _MISSING or
                                    type(declared_name) is not str or
                                    declared_name != canonical_name)
            if strict_malformed:
                entry_issues.append(
                    _issue(
                        ProviderRegistryIssueCodeV1.
                        MALFORMED_STRICT_REGISTRATION, canonical_name,
                        strict_value))
            strict_registration = RegistrationAuditV1(
                AuditPresenceV1.PRESENT, RegistrationKindV1.STRICT_PROVISIONER,
                RegistrationSourceObservationV1.STRICT_REGISTRY_OBSERVED,
                strict_identity, None)

        legacy_module = _MISSING
        legacy_template = _MISSING
        legacy_malformed = False
        if legacy_value is _MISSING:
            legacy_registration = _absent_registration(
                RegistrationKindV1.LEGACY_PROVISIONER)
        else:
            legacy_identity = _runtime_identity(legacy_value)
            if type(legacy_value) is not legacy_container_type:
                legacy_malformed = True
            else:
                legacy_module = _static_field(
                    legacy_value, 'module', signature,
                    f'{canonical_name}|legacy|module')
                legacy_template = _static_field(
                    legacy_value, 'template_override', signature,
                    f'{canonical_name}|legacy|'
                    'template_override')
                legacy_malformed = (legacy_module is _MISSING or
                                    legacy_module is None or
                                    legacy_template is _MISSING)
            if legacy_malformed:
                entry_issues.append(
                    _issue(
                        ProviderRegistryIssueCodeV1.
                        MALFORMED_LEGACY_REGISTRATION, canonical_name,
                        legacy_value))
            legacy_registration = RegistrationAuditV1(
                AuditPresenceV1.PRESENT, RegistrationKindV1.LEGACY_PROVISIONER,
                RegistrationSourceObservationV1.LEGACY_REGISTRY_OBSERVED,
                legacy_identity, None)

        builtin_module = _MISSING
        builtin_indeterminate = False
        if getter is _MISSING:
            builtin_registration = _absent_registration(
                RegistrationKindV1.BUILTIN_PROVISIONER)
            if canonical_name in provisioner_expectations:
                entry_issues.append(
                    _issue(
                        ProviderRegistryIssueCodeV1.
                        PROVISIONER_BUILTIN_IDENTITY_MISMATCH, canonical_name))
        else:
            expected_provisioner = provisioner_expectations.get(canonical_name)
            expectation_fields = _builtin_expectation_fields(
                expected_provisioner)
            getter_identity = _runtime_identity(getter)
            source = RegistrationSourceObservationV1.EXTERNAL_OR_REPLACED
            registration_identity = getter_identity
            signature.append(f'member|{canonical_name}|getter|'
                             f'{getter_identity.process_token}')
            if expectation_fields is None:
                builtin_indeterminate = True
                entry_issues.append(
                    _issue(ProviderRegistryIssueCodeV1.REPLACED_BUILTIN_GETTER,
                           canonical_name, getter))
            else:
                (expected_getter, expected_module, expected_function_type,
                 expected_code, expected_defaults, expected_keyword_defaults,
                 expected_closure, expected_globals,
                 expected_global_name) = expectation_fields
                shape_matches = False
                if (getter is expected_getter and
                        expected_function_type is types.FunctionType and
                        type(getter) is types.FunctionType and
                        type(expected_globals) is dict and
                        type(expected_global_name) is str):
                    code = object.__getattribute__(getter, '__code__')
                    defaults = object.__getattribute__(getter, '__defaults__')
                    keyword_defaults = object.__getattribute__(
                        getter, '__kwdefaults__')
                    closure = object.__getattribute__(getter, '__closure__')
                    globals_mapping = object.__getattribute__(
                        getter, '__globals__')
                    shape_values = (code, defaults, keyword_defaults, closure,
                                    globals_mapping)
                    signature.append(f'member|{canonical_name}|getter_shape|' +
                                     '|'.join(
                                         _runtime_identity(value).process_token
                                         for value in shape_values))
                    shape_matches = (code is expected_code and
                                     defaults is expected_defaults and
                                     keyword_defaults
                                     is expected_keyword_defaults and
                                     closure is expected_closure and
                                     globals_mapping is expected_globals)
                else:
                    signature.append(
                        f'member|{canonical_name}|getter_shape|invalid')
                if not shape_matches:
                    builtin_indeterminate = True
                    entry_issues.append(
                        _issue(
                            ProviderRegistryIssueCodeV1.REPLACED_BUILTIN_GETTER,
                            canonical_name, getter))
                else:
                    result = _exact_string_mapping_get(expected_globals,
                                                       expected_global_name)
                    signature.append(
                        f'member|{canonical_name}|getter_binding|'
                        f'{_runtime_identity(result).process_token}')
                    if result is expected_module:
                        builtin_module = result
                        registration_identity = _runtime_identity(result)
                        source = (RegistrationSourceObservationV1.
                                  BUILTIN_BASELINE_MATCH)
                    else:
                        builtin_indeterminate = True
                        entry_issues.append(
                            _issue(
                                ProviderRegistryIssueCodeV1.
                                PROVISIONER_BUILTIN_IDENTITY_MISMATCH,
                                canonical_name, result))
            builtin_registration = RegistrationAuditV1(
                AuditPresenceV1.PRESENT, RegistrationKindV1.BUILTIN_PROVISIONER,
                source, registration_identity, None)

        strict_members: list[LifecycleMemberAuditV1] = []
        legacy_members: list[LifecycleMemberAuditV1] = []
        builtin_members: list[LifecycleMemberAuditV1] = []
        methods: list[LifecycleMethodAuditV1] = []
        effective_owners: set[LifecycleOwnerV1] = set()
        for method_name in provider_facets.INSTANCE_LIFECYCLE_V1_METHODS:
            strict_member = _member_observation(
                strict_lifecycle, method_name, signature,
                f'{canonical_name}|strict|{method_name}')
            legacy_member = _member_observation(
                legacy_module, method_name, signature,
                f'{canonical_name}|legacy|{method_name}')
            builtin_member = _member_observation(
                builtin_module, method_name, signature,
                f'{canonical_name}|builtin|{method_name}')
            strict_members.append(strict_member)
            legacy_members.append(legacy_member)
            builtin_members.append(builtin_member)

            if strict_value is not _MISSING:
                if (strict_malformed or strict_member.state
                        is LifecycleMemberStateV1.UNSAFE_DESCRIPTOR):
                    owner = LifecycleOwnerV1.INDETERMINATE
                else:
                    owner = LifecycleOwnerV1.STRICT
            elif legacy_value is not _MISSING and legacy_malformed:
                owner = LifecycleOwnerV1.INDETERMINATE
            else:
                if (legacy_member.state
                        is LifecycleMemberStateV1.UNSAFE_DESCRIPTOR):
                    owner = LifecycleOwnerV1.INDETERMINATE
                elif legacy_member.state is not LifecycleMemberStateV1.ABSENT:
                    owner = LifecycleOwnerV1.LEGACY
                elif getter is not _MISSING and builtin_indeterminate:
                    owner = LifecycleOwnerV1.INDETERMINATE
                elif (builtin_member.state
                      is LifecycleMemberStateV1.UNSAFE_DESCRIPTOR):
                    owner = LifecycleOwnerV1.INDETERMINATE
                elif builtin_member.state is not LifecycleMemberStateV1.ABSENT:
                    owner = LifecycleOwnerV1.BUILTIN
                else:
                    owner = LifecycleOwnerV1.FACADE_DEFAULT
            effective_owners.add(owner)
            methods.append(
                LifecycleMethodAuditV1(method_name, strict_member,
                                       legacy_member, builtin_member, False,
                                       owner))

        strict_completeness = _completeness(tuple(strict_members))
        legacy_completeness = _completeness(tuple(legacy_members))
        builtin_completeness = _completeness(tuple(builtin_members))
        if strict_malformed:
            strict_completeness = LifecycleCompletenessV1.INDETERMINATE
        if legacy_malformed:
            legacy_completeness = LifecycleCompletenessV1.INDETERMINATE
        if builtin_indeterminate:
            builtin_completeness = LifecycleCompletenessV1.INDETERMINATE
        mixes = (LifecycleOwnerV1.LEGACY in effective_owners and
                 LifecycleOwnerV1.BUILTIN in effective_owners)
        owner_order = (LifecycleOwnerV1.STRICT, LifecycleOwnerV1.LEGACY,
                       LifecycleOwnerV1.BUILTIN,
                       LifecycleOwnerV1.FACADE_DEFAULT)
        candidate_owners = tuple(owner for owner in owner_order if ((
            owner is LifecycleOwnerV1.STRICT and strict_value is not _MISSING
        ) or (owner is LifecycleOwnerV1.LEGACY and legacy_value is not _MISSING
             ) or (owner is LifecycleOwnerV1.BUILTIN and getter is not _MISSING
                  ) or owner is LifecycleOwnerV1.FACADE_DEFAULT))

        if strict_value is not _MISSING:
            entry_issues.append(
                _issue(ProviderRegistryIssueCodeV1.STRICT_SIGNATURE_UNVERIFIED,
                       canonical_name, strict_value))
            if strict_completeness is not LifecycleCompletenessV1.COMPLETE:
                entry_issues.append(
                    _issue(
                        ProviderRegistryIssueCodeV1.INCOMPLETE_STRICT_LIFECYCLE,
                        canonical_name, strict_value))
        if (getter is not _MISSING and builtin_module is not _MISSING and
                builtin_completeness is not LifecycleCompletenessV1.COMPLETE):
            entry_issues.append(
                _issue(ProviderRegistryIssueCodeV1.INCOMPLETE_BUILTIN_LIFECYCLE,
                       canonical_name, builtin_module))
        if any(member.state is LifecycleMemberStateV1.NON_CALLABLE
               for member in legacy_members):
            entry_issues.append(
                _issue(ProviderRegistryIssueCodeV1.NONCALLABLE_LEGACY_MEMBER,
                       canonical_name, legacy_module))
        if any(member.state is LifecycleMemberStateV1.UNSAFE_DESCRIPTOR
               for member in (*strict_members, *legacy_members,
                              *builtin_members)):
            entry_issues.append(
                _issue(ProviderRegistryIssueCodeV1.UNSAFE_DYNAMIC_MEMBER,
                       canonical_name))
        if mixes:
            entry_issues.append(
                _issue(
                    ProviderRegistryIssueCodeV1.MIXED_INSTANCE_LIFECYCLE_OWNER,
                    canonical_name))
        if (strict_value is not _MISSING and legacy_value is not _MISSING):
            entry_issues.append(
                _issue(ProviderRegistryIssueCodeV1.STRICT_AND_LEGACY_PRESENT,
                       canonical_name))
        if (getter is not _MISSING and
            (strict_value is not _MISSING or legacy_value is not _MISSING)):
            entry_issues.append(
                _issue(ProviderRegistryIssueCodeV1.PARALLEL_LIFECYCLE_OWNER,
                       canonical_name))

        strict_template_member = _member_observation(
            (_MISSING if strict_template is _MISSING else
             SimpleValueOwner(strict_template)), 'value', signature,
            f'{canonical_name}|strict|template')
        legacy_template_member = _member_observation(
            (_MISSING if legacy_template is _MISSING else
             SimpleValueOwner(legacy_template)), 'value', signature,
            f'{canonical_name}|legacy|template')
        builtin_template_member = LifecycleMemberAuditV1(
            LifecycleMemberStateV1.ABSENT, None)
        signature.append(f'member|{canonical_name}|builtin|template|absent')
        if strict_value is not _MISSING:
            template_owner = (TemplateOwnerV1.INDETERMINATE if
                              strict_malformed or strict_template_member.state
                              is LifecycleMemberStateV1.UNSAFE_DESCRIPTOR else
                              TemplateOwnerV1.STRICT)
        elif legacy_value is not _MISSING and legacy_malformed:
            template_owner = TemplateOwnerV1.INDETERMINATE
        elif legacy_template_member.state is LifecycleMemberStateV1.UNSAFE_DESCRIPTOR:
            template_owner = TemplateOwnerV1.INDETERMINATE
        elif legacy_template_member.state is not LifecycleMemberStateV1.ABSENT:
            template_owner = TemplateOwnerV1.LEGACY
        else:
            template_owner = TemplateOwnerV1.ABSENT
        if any(member.state is LifecycleMemberStateV1.NON_CALLABLE
               for member in (strict_template_member, legacy_template_member,
                              builtin_template_member)):
            entry_issues.append(
                _issue(
                    ProviderRegistryIssueCodeV1.NONCALLABLE_TEMPLATE_OVERRIDE,
                    canonical_name))
        if template_owner is TemplateOwnerV1.INDETERMINATE:
            entry_issues.append(
                _issue(ProviderRegistryIssueCodeV1.TEMPLATE_OWNER_INDETERMINATE,
                       canonical_name))
        strict_registration = dataclasses.replace(
            strict_registration,
            template_identity=strict_template_member.identity)
        legacy_registration = dataclasses.replace(
            legacy_registration,
            template_identity=legacy_template_member.identity)

        offer_member = _member_observation(
            cloud_value, 'get_offer_source', signature,
            f'{canonical_name}|cloud|get_offer_source')
        support_member = _member_observation(
            cloud_value, '_unsupported_features_for_resources', signature,
            f'{canonical_name}|cloud|resource_support')
        if (cloud_value is not _MISSING and
                offer_member.state is not LifecycleMemberStateV1.CALLABLE):
            entry_issues.append(
                _issue(ProviderRegistryIssueCodeV1.UNSAFE_OFFER_DECLARATION,
                       canonical_name))
        if (cloud_value is not _MISSING and
                support_member.state is not LifecycleMemberStateV1.CALLABLE):
            entry_issues.append(
                _issue(
                    ProviderRegistryIssueCodeV1.
                    UNSAFE_RESOURCE_SUPPORT_PREDICATE, canonical_name))

        provisioner_switch = _observe_switch(cloud_value, 'PROVISIONER_VERSION',
                                             cloud_lib.ProvisionerVersion,
                                             canonical_name, signature,
                                             entry_issues)
        status_switch = _observe_switch(cloud_value, 'STATUS_VERSION',
                                        cloud_lib.StatusVersion, canonical_name,
                                        signature, entry_issues)
        ports_switch = _observe_switch(cloud_value, 'OPEN_PORTS_VERSION',
                                       cloud_lib.OpenPortsVersion,
                                       canonical_name, signature, entry_issues)

        has_cloud = cloud_value is not _MISSING
        has_strict = strict_value is not _MISSING
        has_legacy = legacy_value is not _MISSING
        has_builtin = getter is not _MISSING
        partial = PartialClassificationV1.NONE
        if has_cloud and not (has_strict or has_legacy or has_builtin):
            is_expected_ibm = (
                canonical_name == 'ibm' and cloud_registration.source
                is RegistrationSourceObservationV1.BUILTIN_BASELINE_MATCH and
                provisioner_switch.value
                is cloud_lib.ProvisionerVersion.RAY_AUTOSCALER and
                status_switch.value is cloud_lib.StatusVersion.CLOUD_CLI)
            if is_expected_ibm:
                partial = PartialClassificationV1.IBM_LEGACY_RAY_CLOUD_ONLY
            else:
                partial = PartialClassificationV1.UNEXPECTED_CLOUD_ONLY
                entry_issues.append(
                    _issue(ProviderRegistryIssueCodeV1.UNEXPECTED_CLOUD_ONLY,
                           canonical_name))
        elif not has_cloud and has_builtin:
            partial = (
                PartialClassificationV1.UNEXPECTED_BUILTIN_PROVISIONER_ONLY)
            entry_issues.append(
                _issue(
                    ProviderRegistryIssueCodeV1.
                    UNEXPECTED_BUILTIN_PROVISIONER_ONLY, canonical_name))
        elif not has_cloud and has_strict and not (has_legacy or has_builtin):
            partial = (
                PartialClassificationV1.UNDECLARED_STRICT_PROVISIONER_ONLY)
            entry_issues.append(
                _issue(
                    ProviderRegistryIssueCodeV1.
                    UNDECLARED_STRICT_PROVISIONER_ONLY, canonical_name))
        elif not has_cloud and has_legacy and not (has_strict or has_builtin):
            partial = (
                PartialClassificationV1.UNDECLARED_LEGACY_PROVISIONER_ONLY)
            entry_issues.append(
                _issue(
                    ProviderRegistryIssueCodeV1.
                    UNDECLARED_LEGACY_PROVISIONER_ONLY, canonical_name))

        if (has_cloud and not (has_strict or has_legacy or has_builtin) and
                provisioner_switch.value
                is cloud_lib.ProvisionerVersion.SKYPILOT):
            entry_issues.append(
                _issue(
                    ProviderRegistryIssueCodeV1.
                    SKYPILOT_CLOUD_WITHOUT_LIFECYCLE, canonical_name))

        entry_aliases = tuple(
            sorted((alias for alias in aliases
                    if alias.target.kind is AuditRawNameKindV1.VALID_STRING and
                    alias.target.text == canonical_name),
                   key=_alias_sort_key))
        issues.extend(entry_issues)
        lifecycle = InstanceLifecycleAuditV1(tuple(methods), candidate_owners,
                                             strict_completeness,
                                             legacy_completeness,
                                             builtin_completeness, mixes)
        template = TemplateOwnershipAuditV1(strict_template_member,
                                            legacy_template_member,
                                            builtin_template_member,
                                            template_owner)
        entries.append(
            ProviderRegistryAuditEntryV1(
                canonical_name, entry_aliases, cloud_registration,
                strict_registration, legacy_registration, builtin_registration,
                provisioner_switch, status_switch, ports_switch, lifecycle,
                template, offer_member.identity, support_member.identity,
                partial, ()))

    sorted_issues = tuple(sorted(issues, key=_issue_sort_key))
    final_entries = tuple(
        dataclasses.replace(entry,
                            issues=_entry_issue_subset(sorted_issues,
                                                       entry.canonical_name))
        for entry in entries)
    sorted_aliases = tuple(sorted(aliases, key=_alias_sort_key))
    sorted_unkeyed = tuple(
        sorted(unkeyed,
               key=lambda value:
               (value.kind.value, _raw_name_sort_key(value.raw_name), value.
                identity.process_token)))
    snapshot = ProviderRegistryAuditSnapshotV1(
        schema_version=1,
        capture_context=capture_context,
        entries=final_entries,
        aliases=sorted_aliases,
        unkeyed_registrations=sorted_unkeyed,
        issues=sorted_issues,
    )
    return _ProviderRegistryAuditObservationV1(snapshot,
                                               tuple(sorted(signature)))


def _observe_provider_registry_audit(
    *,
    capture_context: ProviderAuditContextV1,
    cloud_entries: Mapping[Any, Any],
    cloud_aliases: Mapping[Any, Any],
    strict_entries: Mapping[Any, Any],
    legacy_entries: Mapping[Any, Any],
    builtin_getters: Mapping[Any, Any],
    builtin_cloud_expectations: Mapping[str, tuple[Any, type]],
    builtin_alias_expectations: Mapping[str, str],
    builtin_provisioner_expectations: Mapping[str, Any],
    strict_container_type: type,
    legacy_container_type: type,
) -> _ProviderRegistryAuditObservationV1:
    """Capture one detached observation with private identity anchors."""
    identity_anchors: list[Any] = []
    anchor_token = _IDENTITY_ANCHORS.set(identity_anchors)
    try:
        observation = _build_provider_registry_audit(
            capture_context=capture_context,
            cloud_entries=cloud_entries,
            cloud_aliases=cloud_aliases,
            strict_entries=strict_entries,
            legacy_entries=legacy_entries,
            builtin_getters=builtin_getters,
            builtin_cloud_expectations=builtin_cloud_expectations,
            builtin_alias_expectations=builtin_alias_expectations,
            builtin_provisioner_expectations=(builtin_provisioner_expectations),
            strict_container_type=strict_container_type,
            legacy_container_type=legacy_container_type,
        )
    finally:
        _IDENTITY_ANCHORS.reset(anchor_token)
    return dataclasses.replace(observation,
                               _identity_anchors=tuple(identity_anchors))


@dataclasses.dataclass(frozen=True)
class SimpleValueOwner:
    """Static container used to classify one already-observed hook value."""

    value: Any


__all__ = [
    'AliasAuditV1',
    'AliasSourceV1',
    'AuditPresenceV1',
    'AuditRawNameKindV1',
    'AuditRawNameV1',
    'AuditRuntimeIdentityKindV1',
    'AuditRuntimeIdentityV1',
    'InstanceLifecycleAuditV1',
    'LifecycleCompletenessV1',
    'LifecycleMemberAuditV1',
    'LifecycleMemberStateV1',
    'LifecycleMethodAuditV1',
    'LifecycleOwnerV1',
    'LifecycleSwitchAuditV1',
    'LifecycleSwitchStateV1',
    'PartialClassificationV1',
    'ProviderAuditContextV1',
    'ProviderRegistryAuditCaptureErrorReasonV1',
    'ProviderRegistryAuditCaptureErrorV1',
    'ProviderRegistryAuditEntryV1',
    'ProviderRegistryAuditIssueV1',
    'ProviderRegistryAuditSnapshotV1',
    'ProviderRegistryFacetV1',
    'ProviderRegistryIssueCodeV1',
    'ProviderRegistryIssueSeverityV1',
    'RegistrationAuditV1',
    'RegistrationKindV1',
    'RegistrationSourceObservationV1',
    'TemplateOwnerV1',
    'TemplateOwnershipAuditV1',
    'UnkeyedRegistrationAuditV1',
]
