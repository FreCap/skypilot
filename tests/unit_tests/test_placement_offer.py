"""Acceptance tests for the provider-independent V1 placement offer leaf."""

from __future__ import annotations

import ast
import copy
import dataclasses
import datetime
import hashlib
import importlib
import inspect
import itertools
import json
import pathlib
import re
import sys
import unicodedata

import pytest

_ROOT = pathlib.Path(__file__).parents[2]
_OFFER_PATH = _ROOT / 'sky' / 'placement' / 'offer.py'
_JSON_TYPES_PATH = _ROOT / 'sky' / 'utils' / 'json_types.py'
_UTC = datetime.timezone.utc
_OBSERVED_AT = datetime.datetime(2026, 7, 30, 12, 34, 56, tzinfo=_UTC)
_CAPTURE_ID = '123e4567-e89b-42d3-a456-426614174000'
_SELECTION_CAPTURE_ID = '123e4567-e89b-42d3-b456-426614174001'


def _offer_lib():
    # Keep the acceptance module collectable when a production slice has not
    # landed yet. Each test still fails at its own contract boundary.
    return importlib.import_module('sky.placement.offer')


def _digest(character: str) -> str:
    return f'sha256:{character * 64}'


def _enum_pairs(enum_type) -> tuple[tuple[str, str], ...]:
    return tuple((member.name, member.value) for member in enum_type)


def _dataclass_fields(dataclass_type) -> tuple[str, ...]:
    return tuple(field.name for field in dataclasses.fields(dataclass_type))


def _annotation_text(annotation) -> str | None:
    if annotation is inspect.Signature.empty:
        return None
    if isinstance(annotation, str):
        return annotation
    return inspect.formatannotation(annotation)


def _signature_shape(callable_object):
    signature = inspect.signature(callable_object)
    parameters = tuple(
        (parameter.name, parameter.kind, _annotation_text(parameter.annotation),
         parameter.default is inspect.Signature.empty)
        for parameter in signature.parameters.values())
    return parameters, _annotation_text(signature.return_annotation)


def _assert_signature(
    callable_object,
    expected_parameters: tuple[tuple[str, inspect._ParameterKind, str | None,
                                     bool], ...],
    expected_return: str,
) -> None:
    assert _signature_shape(callable_object) == (expected_parameters,
                                                 expected_return)


def _schema_node(module, kind, **kwargs):
    return module.ProviderPayloadSchemaNodeV1(kind=kind, **kwargs)


def _string_node(module, *, allowed_strings=(), allow_empty=False):
    return _schema_node(module,
                        module.ProviderPayloadNodeKindV1.STRING,
                        allowed_strings=allowed_strings,
                        allow_empty=allow_empty)


def _digest_node(module):
    return _schema_node(module, module.ProviderPayloadNodeKindV1.DIGEST)


def _integer_node(module):
    return _schema_node(module, module.ProviderPayloadNodeKindV1.INTEGER)


def _object_node(module, fields=()):
    return _schema_node(module,
                        module.ProviderPayloadNodeKindV1.OBJECT,
                        fields=fields)


def _array_node(module, item):
    return _schema_node(module,
                        module.ProviderPayloadNodeKindV1.ARRAY,
                        item=item)


def _kubernetes_schema(module, provider: str = 'kubernetes'):
    identity = _object_node(module,
                            fields=(
                                ('rendered_pod_placement_fingerprint',
                                 _digest_node(module)),
                                ('service_account_identity_digest',
                                 _digest_node(module)),
                            ))
    observation = _object_node(
        module,
        fields=(
            ('capacity_evidence',
             _string_node(
                 module,
                 allowed_strings=tuple(
                     sorted(member.value
                            for member in module.OfferCapacityEvidenceV1)))),
            ('configuration_fingerprint', _digest_node(module)),
        ))
    return module.ProviderPayloadSchemaV1(provider=provider,
                                          identity=identity,
                                          observation=observation)


def _resources(module, **overrides):
    fields = {
        'instance_type': '4CPU--16GB',
        'cpus': '4',
        'memory_gib': '16',
        'accelerators': (),
        'disk_tier': None,
        'network_tier': None,
        'placement_constraints_digest': None,
    }
    fields.update(overrides)
    return module.OfferResourcesV1(**fields)


def _make_offer(
    module,
    *,
    operation=None,
    actuation_kind=None,
    provider='kubernetes',
    payload_schema=None,
    scope=None,
    resources=None,
    region='ctx-a',
    candidate_zones=(),
    batching_scope='context',
    price_amount='0.42',
    purchase_mode=None,
    availability=None,
    observed_at=_OBSERVED_AT,
    ttl_seconds=15,
    reservation=None,
    quota=None,
    capacity=None,
    requested_nodes=1,
    identity=None,
    observation=None,
):
    if operation is None:
        operation = module.OfferOperationV1.FRESH_CREATE
    if actuation_kind is None:
        actuation_kind = module.OfferActuationKindV1.DIRECT_POD
    if payload_schema is None:
        payload_schema = _kubernetes_schema(module, provider)
    if scope is None:
        scope = module.OfferScopeV1(
            kind='kubernetes_context_endpoint_identity_namespace_v1',
            id=_digest('1'))
    if resources is None:
        resources = _resources(module)
    if purchase_mode is None:
        purchase_mode = module.OfferPurchaseModeV1.ON_DEMAND
    if availability is None:
        availability = module.OfferAvailabilityV1.UNKNOWN
    if reservation is None:
        reservation = module.OfferReservationEvidenceV1.NOT_APPLICABLE
    if quota is None:
        quota = module.OfferQuotaEvidenceV1.UNKNOWN
    if capacity is None:
        capacity = module.OfferCapacityEvidenceV1.SHAPE_FITS_EXISTING_NODE
    if identity is None:
        identity = {
            'rendered_pod_placement_fingerprint': _digest('2'),
            'service_account_identity_digest': _digest('3'),
        }
    if observation is None:
        observation = {
            'capacity_evidence': capacity.value,
            'configuration_fingerprint': _digest('4'),
        }
    payload = module.OfferProviderPayloadV1.create(
        identity=identity,
        observation=observation,
        payload_schema=payload_schema,
    )
    return module.PlacementOfferV1.create(
        operation=operation,
        actuation_kind=actuation_kind,
        provider=provider,
        scope=scope,
        resources=resources,
        region=region,
        candidate_zones=tuple(candidate_zones),
        batching_scope=batching_scope,
        price=module.OfferPriceV1(
            amount=price_amount,
            basis=module.OfferPriceBasisV1.NODE_HOUR,
            currency=module.OfferCurrencyV1.USD,
        ),
        purchase_mode=purchase_mode,
        availability=availability,
        observed_at=observed_at,
        ttl_seconds=ttl_seconds,
        revalidation_policy=(module.OfferRevalidationPolicyV1.BEFORE_MUTATION),
        evidence=module.OfferEvidenceV1(
            reservation=reservation,
            quota=quota,
            capacity=capacity,
            requested_nodes=requested_nodes,
        ),
        provider_payload=payload,
        payload_schema=payload_schema,
    )


class _Observation:
    """Minimal runtime-conforming observation snapshot."""

    def __init__(
        self,
        *,
        provider='kubernetes',
        capture_id=_CAPTURE_ID,
        observed_at=_OBSERVED_AT,
    ):
        self.provider = provider
        self.capture_id = capture_id
        self.observed_at = observed_at


class _Context:
    """Minimal runtime-conforming actuation context."""

    def __init__(self, *, provider='kubernetes', capture_id=_CAPTURE_ID):
        self.provider = provider
        self.capture_id = capture_id
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _capture(module,
             *,
             provider='kubernetes',
             capture_id=_CAPTURE_ID,
             observed_at=_OBSERVED_AT,
             context=True,
             context_provider=None,
             context_capture_id=None):
    observation = _Observation(provider=provider,
                               capture_id=capture_id,
                               observed_at=observed_at)
    actuation_context = None
    if context:
        actuation_context = _Context(
            provider=(provider
                      if context_provider is None else context_provider),
            capture_id=(capture_id
                        if context_capture_id is None else context_capture_id),
        )
    return module.ObservationCaptureV1(
        observation=observation,
        actuation_context=actuation_context,
    )


def _stable_identity(envelope):
    return {
        'schema_version': envelope['schema_version'],
        'provider': envelope['provider'],
        'operation': envelope['operation'],
        'actuation_kind': envelope['actuation_kind'],
        'scope': envelope['scope'],
        'region': envelope['region'],
        'candidate_zones': envelope['candidate_zones'],
        'batching_scope': envelope['batching_scope'],
        'resources': envelope['resources'],
        'purchase_mode': envelope['purchase_mode'],
        'provider_payload': {
            'version': envelope['provider_payload']['version'],
            'identity': envelope['provider_payload']['identity'],
        },
    }


def _observation_identity(envelope):
    return {
        'offer_id': envelope['offer_id'],
        'price': envelope['price'],
        'availability': envelope['availability'],
        'observed_at': envelope['observed_at'],
        'ttl_seconds': envelope['ttl_seconds'],
        'revalidation_policy': envelope['revalidation_policy'],
        'evidence': envelope['evidence'],
        'provider_payload': {
            'observation': envelope['provider_payload']['observation'],
        },
    }


def _recompute_ids(module, envelope):
    result = copy.deepcopy(envelope)
    stable_digest = hashlib.sha256(
        module.canonical_json_bytes_v1(_stable_identity(result))).hexdigest()
    result['offer_id'] = f"{result['provider']}:sha256:{stable_digest}"
    observation_digest = hashlib.sha256(
        module.canonical_json_bytes_v1(
            _observation_identity(result))).hexdigest()
    result['observation_id'] = f'sha256:{observation_digest}'
    return result


def _assert_json_builtins(value) -> None:
    assert type(value) in (dict, list, str, int, bool, type(None))
    if type(value) is dict:
        assert all(type(key) is str for key in value)
        for child in value.values():
            _assert_json_builtins(child)
    elif type(value) is list:
        for child in value:
            _assert_json_builtins(child)


def _payload_size_schema(module):
    return module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(
            module,
            fields=tuple(
                (key, _string_node(module)) for key in ('a', 'b', 'c', 'd'))),
        observation=_object_node(module),
    )


def _payload_with_exact_size(module, target: int):
    schema = _payload_size_schema(module)
    raw = {
        'version': 1,
        'identity': {
            'a': 'a' * 1024,
            'b': 'b' * 1024,
            'c': 'c' * 1024,
            'd': '',
        },
        'observation': {},
    }
    empty_size = len(module.canonical_json_bytes_v1(raw))
    final_length = target - empty_size
    assert 1 <= final_length <= 1024
    raw['identity']['d'] = 'd' * final_length
    assert len(module.canonical_json_bytes_v1(raw)) == target
    return schema, raw


def _envelope_with_exact_size(module, base, target: int):
    for zone_count in range(1, 33):
        full_zones = tuple(
            f'z{index:02d}-' + ('x' * 1020) for index in range(zone_count - 1))
        for final_length in range(4, 1025):
            candidate = copy.deepcopy(base)
            candidate['candidate_zones'] = list(full_zones +
                                                (f'z{zone_count - 1:02d}-' +
                                                 'y' * (final_length - 4),))
            candidate = _recompute_ids(module, candidate)
            size = len(module.canonical_json_bytes_v1(candidate))
            if size == target:
                return candidate
            if size > target:
                break
    raise AssertionError(f'Could not construct a {target}-byte envelope.')


def test_v1_enum_value_sets_are_exact():
    module = _offer_lib()
    expected_enums = {
        'OfferOperationV1': (
            ('PLAN_CREATE', 'plan_create'),
            ('FRESH_CREATE', 'fresh_create'),
            ('REUSE', 'reuse'),
            ('RESTART', 'restart'),
        ),
        'OfferActuationKindV1': (
            ('DIRECT_POD', 'direct_pod'),
            ('CONTROLLER', 'controller'),
            ('HA_DEPLOYMENT', 'ha_deployment'),
            ('UNKNOWN', 'unknown'),
        ),
        'ObservationFreshnessV1': (
            ('ALLOW_REQUEST_CACHE', 'allow_request_cache'),
            ('REQUIRE_FRESH', 'require_fresh'),
        ),
        'OfferSetStatusV1': (
            ('OK', 'ok'),
            ('NO_OFFERS', 'no_offers'),
            ('NOT_REPRESENTABLE', 'not_representable'),
        ),
        'OfferRevalidationStatusV1': (
            ('VALID', 'valid'),
            ('UNAVAILABLE', 'unavailable'),
            ('NOT_REPRESENTABLE', 'not_representable'),
        ),
        'OfferPriceBasisV1': (('NODE_HOUR', 'node_hour'),),
        'OfferCurrencyV1': (('USD', 'USD'),),
        'OfferPurchaseModeV1': (('ON_DEMAND', 'on_demand'),),
        'OfferAvailabilityV1': (
            ('UNKNOWN', 'unknown'),
            ('UNAVAILABLE', 'unavailable'),
        ),
        'OfferRevalidationPolicyV1': (('BEFORE_MUTATION', 'before_mutation'),),
        'OfferReservationEvidenceV1': (('NOT_APPLICABLE', 'not_applicable'),),
        'OfferQuotaEvidenceV1': (
            ('UNKNOWN', 'unknown'),
            ('UNAVAILABLE', 'unavailable'),
        ),
        'OfferCapacityEvidenceV1': (
            ('SHAPE_FITS_EXISTING_NODE', 'shape_fits_existing_node'),
            ('CONTEXT_UNREACHABLE', 'context_unreachable'),
            ('SHAPE_NO_LONGER_SUPPORTED', 'shape_no_longer_supported'),
            ('CAPACITY_UNAVAILABLE', 'capacity_unavailable'),
            ('PROVIDER_OBJECT_CONFLICT', 'provider_object_conflict'),
        ),
        'ProviderPayloadNodeKindV1': (
            ('STRING', 'string'),
            ('DIGEST', 'digest'),
            ('INTEGER', 'integer'),
            ('BOOLEAN', 'boolean'),
            ('NULL', 'null'),
            ('OBJECT', 'object'),
            ('ARRAY', 'array'),
        ),
        'PlacementOfferActuationModeV1': (
            ('SHADOW', 'shadow'),
            ('SHADOW_LEGACY_FALLBACK', 'shadow_legacy_fallback'),
            ('AUTHORITATIVE', 'authoritative'),
            ('LEGACY_FIRST_ATTEMPT', 'legacy_first_attempt'),
            ('LEGACY_RETRY_AFTER_PROVIDER_ATTEMPT',
             'legacy_retry_after_provider_attempt'),
        ),
        'OfferReasonCodeV1': (
            ('NONE', 'none'),
            ('NO_FEASIBLE_SHAPE', 'no_feasible_shape'),
            ('UNSUPPORTED_OPERATION', 'unsupported_operation'),
            ('UNSUPPORTED_ACTUATION_KIND', 'unsupported_actuation_kind'),
            ('UNSUPPORTED_NODE_COUNT', 'unsupported_node_count'),
            ('UNSUPPORTED_ACCELERATOR', 'unsupported_accelerator'),
            ('UNSUPPORTED_RESOURCE_MODE', 'unsupported_resource_mode'),
            ('UNSUPPORTED_NETWORK_TIER', 'unsupported_network_tier'),
            ('VOLUME_OR_STORAGE_MOUNT', 'volume_or_storage_mount'),
            ('KUEUE_ENABLED', 'kueue_enabled'),
            ('RESERVATION_REQUESTED', 'reservation_requested'),
            ('CUSTOM_PLACEMENT_CONFIG', 'custom_placement_config'),
            ('UNRESOLVED_SCOPE', 'unresolved_scope'),
            ('CONTEXT_UNREACHABLE', 'context_unreachable'),
            ('SCOPE_CHANGED', 'scope_changed'),
            ('CONFIGURATION_CHANGED', 'configuration_changed'),
            ('SHAPE_NO_LONGER_SUPPORTED', 'shape_no_longer_supported'),
            ('CAPACITY_UNAVAILABLE', 'capacity_unavailable'),
            ('QUOTA_UNAVAILABLE', 'quota_unavailable'),
            ('OFFER_IDENTITY_CHANGED', 'offer_identity_changed'),
            ('OBSERVATION_LIMIT_EXCEEDED', 'observation_limit_exceeded'),
            ('PROVIDER_OBJECT_CONFLICT', 'provider_object_conflict'),
            ('SOURCE_ERROR', 'source_error'),
            ('RETRY_AFTER_PROVIDER_ATTEMPT', 'retry_after_provider_attempt'),
        ),
    }
    for name, expected in expected_enums.items():
        assert _enum_pairs(getattr(module, name)) == expected

    expected_dataclasses = {
        'OfferRequestV1': (
            'resources',
            'num_nodes',
            'workspace',
            'has_volume_mounts',
            'has_storage_mounts',
            'operation',
            'actuation_kind',
        ),
        'ObservationCaptureV1': ('observation', 'actuation_context'),
        'OfferScopeV1': ('kind', 'id'),
        'OfferAcceleratorV1': ('name', 'count'),
        'OfferResourcesV1': (
            'instance_type',
            'cpus',
            'memory_gib',
            'accelerators',
            'disk_tier',
            'network_tier',
            'placement_constraints_digest',
        ),
        'OfferPriceV1': ('amount', 'basis', 'currency'),
        'OfferEvidenceV1': (
            'reservation',
            'quota',
            'capacity',
            'requested_nodes',
        ),
        'OfferProviderPayloadV1': ('version', 'identity', 'observation'),
        'PlacementOfferV1': (
            'schema_version',
            'operation',
            'actuation_kind',
            'offer_id',
            'observation_id',
            'provider',
            'scope',
            'resources',
            'region',
            'candidate_zones',
            'batching_scope',
            'price',
            'purchase_mode',
            'availability',
            'observed_at',
            'ttl_seconds',
            'revalidation_policy',
            'evidence',
            'provider_payload',
        ),
        'ProviderPayloadSchemaNodeV1': (
            'kind',
            'fields',
            'item',
            'allowed_strings',
            'allow_empty',
        ),
        'ProviderPayloadSchemaV1': ('provider', 'identity', 'observation'),
        'OfferSetResultV1': ('status', 'offers', 'reason_code'),
        'OfferRevalidationResultV1': ('status', 'offer', 'reason_code'),
        'TaskPlacementDecisionV1': (
            'task_index',
            'resources_fingerprint',
            'operation',
            'offer',
            'selection_capture_id',
        ),
        'OptimizationOfferPlanV1': ('decisions',),
        'PlacementOfferHandoffV1': (
            'mode',
            'offer',
            'actuation_context',
            'provider_attempt_count',
            'reason_code',
        ),
    }
    for name, expected in expected_dataclasses.items():
        assert _dataclass_fields(getattr(module, name)) == expected

    property_kind = inspect.Parameter.POSITIONAL_OR_KEYWORD
    keyword_kind = inspect.Parameter.KEYWORD_ONLY
    positional_kind = inspect.Parameter.POSITIONAL_OR_KEYWORD
    _assert_signature(
        module.ProviderObservationSnapshotV1.provider.fget,
        (('self', property_kind, None, True),),
        'str',
    )
    _assert_signature(
        module.ProviderObservationSnapshotV1.observed_at.fget,
        (('self', property_kind, None, True),),
        'datetime.datetime',
    )
    _assert_signature(
        module.ProviderObservationSnapshotV1.capture_id.fget,
        (('self', property_kind, None, True),),
        'str',
    )
    _assert_signature(
        module.ProviderActuationContextV1.provider.fget,
        (('self', property_kind, None, True),),
        'str',
    )
    _assert_signature(
        module.ProviderActuationContextV1.capture_id.fget,
        (('self', property_kind, None, True),),
        'str',
    )
    _assert_signature(
        module.ProviderActuationContextV1.close,
        (('self', positional_kind, None, True),),
        'None',
    )
    assert tuple(name for name, value in
                 module.ProviderObservationSnapshotV1.__dict__.items()
                 if isinstance(value, property)) == ('provider', 'observed_at',
                                                     'capture_id')
    assert tuple(
        name
        for name, value in module.ProviderActuationContextV1.__dict__.items()
        if isinstance(value, property) or inspect.isfunction(value)
        if not name.startswith('_')) == ('provider', 'capture_id', 'close')
    assert tuple(name for name, value in module.OfferSourceV1.__dict__.items()
                 if inspect.isfunction(value) and not name.startswith('_')) == (
                     'capture_observation', 'list_offers', 'revalidate')
    _assert_signature(
        module.OfferSourceV1.capture_observation,
        (
            ('self', positional_kind, None, True),
            ('request', positional_kind, 'OfferRequestV1', True),
            ('observed_at', keyword_kind, 'datetime.datetime', True),
            ('freshness', keyword_kind, 'ObservationFreshnessV1', True),
        ),
        'ObservationCaptureV1',
    )
    _assert_signature(
        module.OfferSourceV1.list_offers,
        (
            ('self', positional_kind, None, True),
            ('request', positional_kind, 'OfferRequestV1', True),
            ('observation', keyword_kind, 'ProviderObservationSnapshotV1',
             True),
        ),
        'OfferSetResultV1',
    )
    _assert_signature(
        module.OfferSourceV1.revalidate,
        (
            ('self', positional_kind, None, True),
            ('offer', positional_kind, 'PlacementOfferV1', True),
            ('request', positional_kind, 'OfferRequestV1', True),
            ('observation', keyword_kind, 'ProviderObservationSnapshotV1',
             True),
        ),
        'OfferRevalidationResultV1',
    )

    factory_parameters = {
        module.OfferProviderPayloadV1.create: (
            ('identity', keyword_kind, 'dict[str, JSONValue]', True),
            ('observation', keyword_kind, 'dict[str, JSONValue]', True),
            ('payload_schema', keyword_kind, 'ProviderPayloadSchemaV1', True),
        ),
        module.PlacementOfferV1.create: tuple(
            (name, keyword_kind, annotation, True) for name, annotation in (
                ('operation', 'OfferOperationV1'),
                ('actuation_kind', 'OfferActuationKindV1'),
                ('provider', 'str'),
                ('scope', 'OfferScopeV1'),
                ('resources', 'OfferResourcesV1'),
                ('region', 'str'),
                ('candidate_zones', 'tuple[str, ...]'),
                ('batching_scope', 'str'),
                ('price', 'OfferPriceV1'),
                ('purchase_mode', 'OfferPurchaseModeV1'),
                ('availability', 'OfferAvailabilityV1'),
                ('observed_at', 'datetime.datetime'),
                ('ttl_seconds', 'int'),
                ('revalidation_policy', 'OfferRevalidationPolicyV1'),
                ('evidence', 'OfferEvidenceV1'),
                ('provider_payload', 'OfferProviderPayloadV1'),
                ('payload_schema', 'ProviderPayloadSchemaV1'),
            )),
        module.PlacementOfferV1.from_envelope: (
            ('envelope', positional_kind, 'dict[str, JSONValue]', True),
            ('payload_schema', keyword_kind, 'ProviderPayloadSchemaV1', True),
        ),
        module.PlacementOfferV1.from_json: (
            ('serialized', positional_kind, 'str | bytes', True),
            ('payload_schema', keyword_kind, 'ProviderPayloadSchemaV1', True),
        ),
    }
    expected_returns = {
        module.OfferProviderPayloadV1.create: 'OfferProviderPayloadV1',
        module.PlacementOfferV1.create: 'PlacementOfferV1',
        module.PlacementOfferV1.from_envelope: 'PlacementOfferV1',
        module.PlacementOfferV1.from_json: 'PlacementOfferV1',
    }
    for factory, parameters in factory_parameters.items():
        _assert_signature(factory, parameters, expected_returns[factory])
    _assert_signature(
        module.OfferRevalidationResultV1.valid,
        (
            ('original', positional_kind, 'PlacementOfferV1', True),
            ('replacement', positional_kind, 'PlacementOfferV1', True),
        ),
        'OfferRevalidationResultV1',
    )
    _assert_signature(
        module.OfferRevalidationResultV1.unavailable,
        (
            ('original', positional_kind, 'PlacementOfferV1', True),
            ('replacement', positional_kind, 'PlacementOfferV1', True),
            ('reason_code', positional_kind, 'OfferReasonCodeV1', True),
        ),
        'OfferRevalidationResultV1',
    )
    _assert_signature(
        module.OfferRevalidationResultV1.not_representable,
        (('reason_code', positional_kind, 'OfferReasonCodeV1', True),),
        'OfferRevalidationResultV1',
    )

    for class_name in (
            'OfferProviderPayloadV1',
            'PlacementOfferV1',
            'OfferRevalidationResultV1',
    ):
        class_object = getattr(module, class_name)
        assert class_object.__dataclass_params__.init is False
        assert not tuple(inspect.signature(class_object).parameters)
        with pytest.raises(TypeError):
            class_object()

    request_resources = object()
    request = module.OfferRequestV1(
        resources=request_resources,
        num_nodes=1,
        workspace=None,
        has_volume_mounts=False,
        has_storage_mounts=False,
        operation=module.OfferOperationV1.PLAN_CREATE,
        actuation_kind=module.OfferActuationKindV1.DIRECT_POD,
    )
    assert request.resources is request_resources
    assert request.num_nodes == 1
    assert request.workspace is None
    assert request.has_volume_mounts is False
    assert request.has_storage_mounts is False
    assert request.operation is module.OfferOperationV1.PLAN_CREATE
    assert request.actuation_kind is module.OfferActuationKindV1.DIRECT_POD
    for replacements in (
        {
            'num_nodes': 0
        },
        {
            'num_nodes': 10_001
        },
        {
            'num_nodes': True
        },
        {
            'workspace': 1
        },
        {
            'has_volume_mounts': 0
        },
        {
            'has_storage_mounts': 0
        },
        {
            'operation': 'plan_create'
        },
        {
            'actuation_kind': 'direct_pod'
        },
    ):
        with pytest.raises(ValueError):
            dataclasses.replace(request, **replacements)


def test_offer_payload_is_recursively_immutable_and_detached():
    module = _offer_lib()
    string = _string_node(module, allowed_strings=('alpha', 'beta'))
    nested_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(
            module,
            fields=(
                ('array',
                 _array_node(
                     module,
                     _object_node(
                         module,
                         fields=(('enabled',
                                  _schema_node(
                                      module,
                                      module.ProviderPayloadNodeKindV1.BOOLEAN)
                                 ),)))),
                ('nested', _object_node(module, fields=(('label', string),))),
            )),
        observation=_object_node(module,
                                 fields=(('count', _integer_node(module)),)),
    )
    identity = {
        'nested': {
            'label': 'alpha',
        },
        'array': [{
            'enabled': True,
        }],
    }
    observation = {'count': 7}
    payload = module.OfferProviderPayloadV1.create(
        identity=identity,
        observation=observation,
        payload_schema=nested_schema,
    )
    reverse_payload = module.OfferProviderPayloadV1.create(
        identity={
            'array': [{
                'enabled': True,
            }],
            'nested': {
                'label': 'alpha',
            },
        },
        observation={'count': 7},
        payload_schema=nested_schema,
    )
    assert payload == reverse_payload
    assert hash(payload) == hash(reverse_payload)
    assert hash(payload.identity) == hash(reverse_payload.identity)
    identity['nested']['label'] = 'beta'
    identity['array'][0]['enabled'] = False
    observation['count'] = 8
    assert payload.to_json()['identity']['nested']['label'] == 'alpha'
    assert payload.to_json()['identity']['array'][0]['enabled'] is True
    assert payload.to_json()['observation']['count'] == 7
    with pytest.raises(TypeError):
        payload.identity['nested'] = {}  # type: ignore[index]
    with pytest.raises(TypeError):
        payload.identity['nested']['label'] = 'beta'  # type: ignore[index]
    first_thaw = payload.to_json()
    second_thaw = payload.to_json()
    first_thaw['identity']['array'][0]['enabled'] = False
    assert second_thaw['identity']['array'][0]['enabled'] is True
    assert payload.to_json()['identity']['array'][0]['enabled'] is True

    kinds = module.ProviderPayloadNodeKindV1
    invalid_node_arguments = [
        # Structural input types must be exact tuples.
        dict(kind=kinds.OBJECT, fields=[]),
        dict(kind=kinds.OBJECT, fields=(['key', string],)),
        dict(kind=kinds.OBJECT, fields=(('key', string, string),)),
        dict(kind=kinds.OBJECT, fields=(('key', object()),)),
        dict(kind=kinds.OBJECT, fields=(('b', string), ('a', string))),
        dict(kind=kinds.OBJECT, fields=(('a', string), ('a', string))),
        dict(kind=kinds.OBJECT, fields=(('', string),)),
        dict(kind=kinds.OBJECT, item=string),
        dict(kind=kinds.OBJECT, allowed_strings=('alpha',)),
        dict(kind=kinds.OBJECT, allow_empty=True),
        dict(kind=kinds.ARRAY, item=None),
        dict(kind=kinds.ARRAY, item=object()),
        dict(kind=kinds.ARRAY, item=string, fields=(('a', string),)),
        dict(kind=kinds.ARRAY, item=string, allowed_strings=('alpha',)),
        dict(kind=kinds.ARRAY, item=string, allow_empty=True),
    ]
    scalar_kinds = (
        kinds.STRING,
        kinds.DIGEST,
        kinds.INTEGER,
        kinds.BOOLEAN,
        kinds.NULL,
    )
    for kind in scalar_kinds:
        invalid_node_arguments.extend((
            dict(kind=kind, fields=(('a', string),)),
            dict(kind=kind, item=string),
        ))
    for kind in (kinds.DIGEST, kinds.INTEGER, kinds.BOOLEAN, kinds.NULL):
        invalid_node_arguments.extend((
            dict(kind=kind, allowed_strings=('alpha',)),
            dict(kind=kind, allow_empty=True),
        ))
    invalid_node_arguments.extend((
        dict(kind=kinds.STRING, allowed_strings=['alpha']),
        dict(kind=kinds.STRING, allowed_strings=('beta', 'alpha')),
        dict(kind=kinds.STRING, allowed_strings=('alpha', 'alpha')),
        dict(kind=kinds.STRING, allowed_strings=('',), allow_empty=False),
        dict(kind=kinds.STRING, allowed_strings=(1,)),
        dict(kind=kinds.STRING, allowed_strings=('e\u0301',)),
        dict(kind=kinds.STRING, allow_empty=1),
        dict(kind='string'),
    ))
    for arguments in invalid_node_arguments:
        with pytest.raises((TypeError, ValueError)):
            module.ProviderPayloadSchemaNodeV1(**arguments)

    object_root = _object_node(module)
    for provider in (
            '',
            '-kubernetes',
            'kubernetes-',
            'Kubernetes',
            'kubernetes/',
            'a' * 64,
            None,
    ):
        with pytest.raises((TypeError, ValueError)):
            module.ProviderPayloadSchemaV1(provider=provider,
                                           identity=object_root,
                                           observation=object_root)
    for identity_root, observation_root in (
        (string, object_root),
        (object_root, string),
        (object(), object_root),
        (object_root, object()),
    ):
        with pytest.raises((TypeError, ValueError)):
            module.ProviderPayloadSchemaV1(
                provider='kubernetes',
                identity=identity_root,
                observation=observation_root,
            )
    empty_string_node = _string_node(module,
                                     allowed_strings=('',),
                                     allow_empty=True)
    assert empty_string_node.allowed_strings == ('',)
    empty_string_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module,
                              fields=(('empty_value', empty_string_node),)),
        observation=_object_node(module),
    )
    empty_string_payload = module.OfferProviderPayloadV1.create(
        identity={'empty_value': ''},
        observation={},
        payload_schema=empty_string_schema,
    )
    assert empty_string_payload.to_json()['identity']['empty_value'] == ''
    nonempty_string_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module,
                              fields=(('empty_value', _string_node(module)),)),
        observation=_object_node(module),
    )
    with pytest.raises(ValueError):
        module.OfferProviderPayloadV1.create(
            identity={'empty_value': ''},
            observation={},
            payload_schema=nonempty_string_schema,
        )

    invalid_payloads = (
        ({
            'nested': {
                'label': 'alpha'
            },
            'array': [{
                'enabled': True
            }],
            'unknown': 1,
        }, {
            'count': 7
        }),
        ({
            'nested': {
                'label': 'alpha'
            },
        }, {
            'count': 7
        }),
        ({
            'nested': {},
            'array': [{
                'enabled': True
            }],
        }, {
            'count': 7
        }),
        ({
            'nested': {
                'label': 'gamma'
            },
            'array': [{
                'enabled': True
            }],
        }, {
            'count': 7
        }),
        ({
            'nested': {
                'label': ''
            },
            'array': [{
                'enabled': True
            }],
        }, {
            'count': 7
        }),
        ({
            'nested': {
                'label': 'alpha',
                'unknown': 1
            },
            'array': [{
                'enabled': True
            }],
        }, {
            'count': 7
        }),
        ({
            'nested': {
                'label': 'alpha'
            },
            'array': [{
                'enabled': 1
            }],
        }, {
            'count': 7
        }),
        ({
            'nested': {
                'label': 'alpha'
            },
            'array': [{
                'enabled': True
            }],
        }, {
            'count': True
        }),
    )
    for bad_identity, bad_observation in invalid_payloads:
        with pytest.raises((TypeError, ValueError)):
            module.OfferProviderPayloadV1.create(
                identity=bad_identity,
                observation=bad_observation,
                payload_schema=nested_schema,
            )

    mismatched_schema = dataclasses.replace(nested_schema, provider='gcp')
    with pytest.raises((TypeError, ValueError)):
        _make_offer(module,
                    payload_schema=mismatched_schema,
                    identity={
                        'array': [{
                            'enabled': True
                        }],
                        'nested': {
                            'label': 'alpha'
                        },
                    },
                    observation={'count': 7})


def test_observation_capture_requires_matching_provider_and_capture_id():
    module = _offer_lib()
    offer = _make_offer(module)
    capture = _capture(module)
    assert capture.observation.capture_id == _CAPTURE_ID
    assert capture.actuation_context.capture_id == _CAPTURE_ID
    context = module.validate_authoritative_capture_v1(
        offer,
        capture,
        freshness=module.ObservationFreshnessV1.REQUIRE_FRESH,
        selection_capture_id=_SELECTION_CAPTURE_ID,
    )
    assert context is capture.actuation_context

    for bad_id in (
            _CAPTURE_ID.upper(),
            _CAPTURE_ID.replace('-', ''),
            f'{{{_CAPTURE_ID}}}',
            '00000000-0000-0000-0000-000000000000',
            '123e4567-e89b-12d3-a456-426614174000',
            '123e4567-e89b-42d3-7456-426614174000',
            'provider-native-id',
    ):
        with pytest.raises((TypeError, ValueError)):
            _capture(module, capture_id=bad_id)
        with pytest.raises((TypeError, ValueError)):
            _capture(module, context_capture_id=bad_id)
        with pytest.raises((TypeError, ValueError)):
            module.validate_authoritative_capture_v1(
                offer,
                capture,
                freshness=module.ObservationFreshnessV1.REQUIRE_FRESH,
                selection_capture_id=bad_id,
            )

    for kwargs in (
            dict(provider='Kubernetes'),
            dict(context_provider='gcp'),
            dict(context_capture_id=_SELECTION_CAPTURE_ID),
            dict(observed_at=_OBSERVED_AT.replace(tzinfo=None)),
            dict(observed_at=_OBSERVED_AT.replace(microsecond=1)),
            dict(observed_at=_OBSERVED_AT.astimezone(
                datetime.timezone(datetime.timedelta(hours=1)))),
    ):
        with pytest.raises((TypeError, ValueError)):
            _capture(module, **kwargs)

    class _BadObservation:
        provider = 'kubernetes'
        capture_id = _CAPTURE_ID

    class _BadContext:
        provider = 'kubernetes'
        capture_id = _CAPTURE_ID

    class _NonCallableCloseContext:
        provider = 'kubernetes'
        capture_id = _CAPTURE_ID
        close = 1

    with pytest.raises((TypeError, ValueError)):
        module.ObservationCaptureV1(observation=_BadObservation(),
                                    actuation_context=None)
    with pytest.raises((TypeError, ValueError)):
        module.ObservationCaptureV1(observation=_Observation(),
                                    actuation_context=_BadContext())
    non_callable_context = _NonCallableCloseContext()
    with pytest.raises((TypeError, ValueError)):
        module.ObservationCaptureV1(
            observation=_Observation(),
            actuation_context=non_callable_context,
        )
    malformed_capture = object.__new__(module.ObservationCaptureV1)
    object.__setattr__(malformed_capture, 'observation', _Observation())
    object.__setattr__(malformed_capture, 'actuation_context',
                       non_callable_context)
    with pytest.raises((TypeError, ValueError)):
        module.validate_authoritative_capture_v1(
            offer,
            malformed_capture,
            freshness=module.ObservationFreshnessV1.REQUIRE_FRESH,
            selection_capture_id=_SELECTION_CAPTURE_ID,
        )
    with pytest.raises((TypeError, ValueError)):
        module.PlacementOfferHandoffV1(
            mode=module.PlacementOfferActuationModeV1.AUTHORITATIVE,
            offer=offer,
            actuation_context=non_callable_context,
            provider_attempt_count=1,
            reason_code=module.OfferReasonCodeV1.NONE,
        )

    no_context = _capture(module, context=False)
    invalid_helper_calls = (
        dict(capture=no_context,
             freshness=module.ObservationFreshnessV1.REQUIRE_FRESH,
             selection_capture_id=_SELECTION_CAPTURE_ID),
        dict(capture=capture,
             freshness=module.ObservationFreshnessV1.ALLOW_REQUEST_CACHE,
             selection_capture_id=_SELECTION_CAPTURE_ID),
        dict(capture=capture,
             freshness=module.ObservationFreshnessV1.REQUIRE_FRESH,
             selection_capture_id=_CAPTURE_ID),
        dict(capture=capture,
             freshness=None,
             selection_capture_id=_SELECTION_CAPTURE_ID),
        dict(capture=capture,
             freshness=module.ObservationFreshnessV1.REQUIRE_FRESH,
             selection_capture_id=None),
        dict(capture=_capture(module,
                              observed_at=_OBSERVED_AT +
                              datetime.timedelta(seconds=1)),
             freshness=module.ObservationFreshnessV1.REQUIRE_FRESH,
             selection_capture_id=_SELECTION_CAPTURE_ID),
    )
    for call in invalid_helper_calls:
        with pytest.raises((TypeError, ValueError)):
            module.validate_authoritative_capture_v1(
                offer,
                call['capture'],
                freshness=call['freshness'],
                selection_capture_id=call['selection_capture_id'],
            )

    gcp_schema = _kubernetes_schema(module, provider='gcp')
    gcp_offer = _make_offer(module, provider='gcp', payload_schema=gcp_schema)
    with pytest.raises((TypeError, ValueError)):
        module.validate_authoritative_capture_v1(
            gcp_offer,
            capture,
            freshness=module.ObservationFreshnessV1.REQUIRE_FRESH,
            selection_capture_id=_SELECTION_CAPTURE_ID,
        )


def test_offer_set_result_disposition_matrix():
    module = _offer_lib()
    offer = _make_offer(module)
    allowed_not_representable = {
        module.OfferReasonCodeV1.UNSUPPORTED_OPERATION,
        module.OfferReasonCodeV1.UNSUPPORTED_ACTUATION_KIND,
        module.OfferReasonCodeV1.UNSUPPORTED_NODE_COUNT,
        module.OfferReasonCodeV1.UNSUPPORTED_ACCELERATOR,
        module.OfferReasonCodeV1.UNSUPPORTED_RESOURCE_MODE,
        module.OfferReasonCodeV1.UNSUPPORTED_NETWORK_TIER,
        module.OfferReasonCodeV1.VOLUME_OR_STORAGE_MOUNT,
        module.OfferReasonCodeV1.KUEUE_ENABLED,
        module.OfferReasonCodeV1.RESERVATION_REQUESTED,
        module.OfferReasonCodeV1.CUSTOM_PLACEMENT_CONFIG,
        module.OfferReasonCodeV1.UNRESOLVED_SCOPE,
        module.OfferReasonCodeV1.OBSERVATION_LIMIT_EXCEEDED,
    }
    for status, offers, reason in itertools.product(
            module.OfferSetStatusV1,
        ((), (offer,)),
            module.OfferReasonCodeV1,
    ):
        valid = (status is module.OfferSetStatusV1.OK and offers == (offer,) and
                 reason is module.OfferReasonCodeV1.NONE) or (
                     status is module.OfferSetStatusV1.NO_OFFERS and
                     not offers and
                     reason is module.OfferReasonCodeV1.NO_FEASIBLE_SHAPE) or (
                         status is module.OfferSetStatusV1.NOT_REPRESENTABLE and
                         not offers and reason in allowed_not_representable)
        if valid:
            result = module.OfferSetResultV1(status=status,
                                             offers=offers,
                                             reason_code=reason)
            assert result.offers is offers
        else:
            with pytest.raises((TypeError, ValueError)):
                module.OfferSetResultV1(status=status,
                                        offers=offers,
                                        reason_code=reason)
    with pytest.raises((TypeError, ValueError)):
        module.OfferSetResultV1(status=module.OfferSetStatusV1.OK,
                                offers=[offer],
                                reason_code=module.OfferReasonCodeV1.NONE)


def test_offer_revalidation_result_disposition_matrix():
    module = _offer_lib()
    original = _make_offer(module)
    statuses = module.OfferRevalidationStatusV1
    reasons = module.OfferReasonCodeV1
    expected_rows = {
        (module.OfferAvailabilityV1.UNKNOWN, module.OfferReservationEvidenceV1.NOT_APPLICABLE, module.OfferQuotaEvidenceV1.UNKNOWN, module.OfferCapacityEvidenceV1.SHAPE_FITS_EXISTING_NODE):
            ('valid', reasons.NONE),
        (module.OfferAvailabilityV1.UNAVAILABLE, module.OfferReservationEvidenceV1.NOT_APPLICABLE, module.OfferQuotaEvidenceV1.UNKNOWN, module.OfferCapacityEvidenceV1.CONTEXT_UNREACHABLE):
            ('unavailable', reasons.CONTEXT_UNREACHABLE),
        (module.OfferAvailabilityV1.UNAVAILABLE, module.OfferReservationEvidenceV1.NOT_APPLICABLE, module.OfferQuotaEvidenceV1.UNKNOWN, module.OfferCapacityEvidenceV1.SHAPE_NO_LONGER_SUPPORTED):
            ('unavailable', reasons.SHAPE_NO_LONGER_SUPPORTED),
        (module.OfferAvailabilityV1.UNAVAILABLE, module.OfferReservationEvidenceV1.NOT_APPLICABLE, module.OfferQuotaEvidenceV1.UNKNOWN, module.OfferCapacityEvidenceV1.CAPACITY_UNAVAILABLE):
            ('unavailable', reasons.CAPACITY_UNAVAILABLE),
        (module.OfferAvailabilityV1.UNAVAILABLE, module.OfferReservationEvidenceV1.NOT_APPLICABLE, module.OfferQuotaEvidenceV1.UNAVAILABLE, module.OfferCapacityEvidenceV1.SHAPE_FITS_EXISTING_NODE):
            ('unavailable', reasons.QUOTA_UNAVAILABLE),
        (module.OfferAvailabilityV1.UNAVAILABLE, module.OfferReservationEvidenceV1.NOT_APPLICABLE, module.OfferQuotaEvidenceV1.UNKNOWN, module.OfferCapacityEvidenceV1.PROVIDER_OBJECT_CONFLICT):
            ('unavailable', reasons.PROVIDER_OBJECT_CONFLICT),
    }
    for availability, reservation, quota, capacity in itertools.product(
            module.OfferAvailabilityV1,
            module.OfferReservationEvidenceV1,
            module.OfferQuotaEvidenceV1,
            module.OfferCapacityEvidenceV1,
    ):
        replacement = _make_offer(
            module,
            availability=availability,
            reservation=reservation,
            quota=quota,
            capacity=capacity,
            observed_at=_OBSERVED_AT + datetime.timedelta(seconds=1),
        )
        row = (availability, reservation, quota, capacity)
        if expected_rows.get(row) == ('valid', reasons.NONE):
            result = module.OfferRevalidationResultV1.valid(
                original, replacement)
            assert (result.status, result.offer,
                    result.reason_code) == (statuses.VALID, replacement,
                                            reasons.NONE)
        else:
            with pytest.raises((TypeError, ValueError)):
                module.OfferRevalidationResultV1.valid(original, replacement)
        for reason in reasons:
            if expected_rows.get(row) == ('unavailable', reason):
                result = module.OfferRevalidationResultV1.unavailable(
                    original, replacement, reason)
                assert (result.status, result.offer,
                        result.reason_code) == (statuses.UNAVAILABLE,
                                                replacement, reason)
            else:
                with pytest.raises((TypeError, ValueError)):
                    module.OfferRevalidationResultV1.unavailable(
                        original, replacement, reason)

    not_representable_reasons = {
        reasons.SCOPE_CHANGED,
        reasons.CONFIGURATION_CHANGED,
        reasons.OFFER_IDENTITY_CHANGED,
    }
    for reason in reasons:
        if reason in not_representable_reasons:
            result = module.OfferRevalidationResultV1.not_representable(reason)
            assert (result.status, result.offer,
                    result.reason_code) == (statuses.NOT_REPRESENTABLE, None,
                                            reason)
        else:
            with pytest.raises((TypeError, ValueError)):
                module.OfferRevalidationResultV1.not_representable(reason)

    valid_replacement = _make_offer(module,
                                    observed_at=_OBSERVED_AT +
                                    datetime.timedelta(seconds=1))
    bad_replacements = (
        _make_offer(module,
                    region='ctx-b',
                    observed_at=_OBSERVED_AT + datetime.timedelta(seconds=1)),
        _make_offer(module,
                    requested_nodes=2,
                    observed_at=_OBSERVED_AT + datetime.timedelta(seconds=1)),
        _make_offer(module,
                    observed_at=_OBSERVED_AT - datetime.timedelta(seconds=1)),
    )
    for replacement in bad_replacements:
        with pytest.raises((TypeError, ValueError)):
            module.OfferRevalidationResultV1.valid(original, replacement)
    unavailable = _make_offer(
        module,
        availability=module.OfferAvailabilityV1.UNAVAILABLE,
        capacity=module.OfferCapacityEvidenceV1.CAPACITY_UNAVAILABLE,
        observed_at=_OBSERVED_AT + datetime.timedelta(seconds=1),
    )
    module.OfferRevalidationResultV1.unavailable(original, unavailable,
                                                 reasons.CAPACITY_UNAVAILABLE)
    invalid_unavailable_replacements = (
        _make_offer(
            module,
            region='ctx-b',
            availability=module.OfferAvailabilityV1.UNAVAILABLE,
            capacity=module.OfferCapacityEvidenceV1.CAPACITY_UNAVAILABLE,
            observed_at=_OBSERVED_AT + datetime.timedelta(seconds=1),
        ),
        _make_offer(
            module,
            requested_nodes=2,
            availability=module.OfferAvailabilityV1.UNAVAILABLE,
            capacity=module.OfferCapacityEvidenceV1.CAPACITY_UNAVAILABLE,
            observed_at=_OBSERVED_AT + datetime.timedelta(seconds=1),
        ),
        _make_offer(
            module,
            availability=module.OfferAvailabilityV1.UNAVAILABLE,
            capacity=module.OfferCapacityEvidenceV1.CAPACITY_UNAVAILABLE,
            observed_at=_OBSERVED_AT - datetime.timedelta(seconds=1),
        ),
    )
    for replacement in invalid_unavailable_replacements:
        with pytest.raises((TypeError, ValueError)):
            module.OfferRevalidationResultV1.unavailable(
                original, replacement, reasons.CAPACITY_UNAVAILABLE)
    with pytest.raises((TypeError, ValueError)):
        module.OfferRevalidationResultV1(
            status=statuses.VALID,
            offer=valid_replacement,
            reason_code=reasons.NONE,
        )


def test_stable_and_observation_identity_field_partition():
    module = _offer_lib()
    offer = _make_offer(module)
    envelope = offer.to_envelope()
    stable_bytes = (
        b'{"actuation_kind":"direct_pod","batching_scope":"context",'
        b'"candidate_zones":[],"operation":"fresh_create","provider":'
        b'"kubernetes","provider_payload":{"identity":{'
        b'"rendered_pod_placement_fingerprint":"sha256:'
        b'2222222222222222222222222222222222222222222222222222222222222222",'
        b'"service_account_identity_digest":"sha256:'
        b'3333333333333333333333333333333333333333333333333333333333333333"},'
        b'"version":1},"purchase_mode":"on_demand","region":"ctx-a",'
        b'"resources":{"accelerators":[],"cpus":"4","disk_tier":null,'
        b'"instance_type":"4CPU--16GB","memory_gib":"16","network_tier":null,'
        b'"placement_constraints_digest":null},"schema_version":1,"scope":{'
        b'"id":"sha256:'
        b'1111111111111111111111111111111111111111111111111111111111111111",'
        b'"kind":"kubernetes_context_endpoint_identity_namespace_v1"}}')
    observation_bytes = (
        b'{"availability":"unknown","evidence":{"capacity":'
        b'"shape_fits_existing_node","quota":"unknown","requested_nodes":1,'
        b'"reservation":"not_applicable"},"observed_at":'
        b'"2026-07-30T12:34:56Z","offer_id":"kubernetes:sha256:'
        b'823cab1acaeb98b85aed588752725bdd97b8a7b7dafe4f387ac90331d165d09d",'
        b'"price":{"amount":"0.42","basis":"node_hour","currency":"USD"},'
        b'"provider_payload":{"observation":{"capacity_evidence":'
        b'"shape_fits_existing_node","configuration_fingerprint":"sha256:'
        b'4444444444444444444444444444444444444444444444444444444444444444"}},'
        b'"revalidation_policy":"before_mutation","ttl_seconds":15}')
    assert module.canonical_json_bytes_v1(
        _stable_identity(envelope)) == stable_bytes
    assert module.canonical_json_bytes_v1(
        _observation_identity(envelope)) == observation_bytes
    assert len(stable_bytes) == 770
    assert len(observation_bytes) == 585
    assert offer.offer_id == (
        'kubernetes:sha256:'
        '823cab1acaeb98b85aed588752725bdd97b8a7b7dafe4f387ac90331d165d09d')
    assert offer.observation_id == (
        'sha256:'
        '283016b7089fd4bc58c81ab53eba4fdc685d1a27b754649e46581902e702fc85')
    expires_at = _OBSERVED_AT + datetime.timedelta(seconds=offer.ttl_seconds)
    assert not offer.is_expired(expires_at - datetime.timedelta(microseconds=1))
    assert offer.is_expired(expires_at)
    assert offer.is_expired(expires_at + datetime.timedelta(microseconds=1))

    gcp_schema = _kubernetes_schema(module, provider='gcp')
    stable_variants = (
        dict(operation=module.OfferOperationV1.PLAN_CREATE),
        dict(actuation_kind=module.OfferActuationKindV1.CONTROLLER),
        dict(provider='gcp', payload_schema=gcp_schema),
        dict(
            scope=module.OfferScopeV1(kind='alternate_scope', id=_digest('1'))),
        dict(resources=_resources(module, cpus='5')),
        dict(region='ctx-b'),
        dict(candidate_zones=('zone-a',)),
        dict(batching_scope='region'),
        dict(
            identity={
                'rendered_pod_placement_fingerprint': _digest('5'),
                'service_account_identity_digest': _digest('3'),
            }),
    )
    for arguments in stable_variants:
        changed = _make_offer(module, **arguments)
        assert changed.offer_id != offer.offer_id
        assert changed.observation_id != offer.observation_id

    observation_variants = (
        dict(price_amount='0.43'),
        dict(observed_at=_OBSERVED_AT + datetime.timedelta(seconds=1)),
        dict(ttl_seconds=16),
        dict(requested_nodes=2),
        dict(quota=module.OfferQuotaEvidenceV1.UNAVAILABLE),
        dict(availability=module.OfferAvailabilityV1.UNAVAILABLE,
             capacity=module.OfferCapacityEvidenceV1.CAPACITY_UNAVAILABLE),
        dict(
            observation={
                'capacity_evidence': 'shape_fits_existing_node',
                'configuration_fingerprint': _digest('5'),
            }),
    )
    for arguments in observation_variants:
        changed = _make_offer(module, **arguments)
        assert changed.offer_id == offer.offer_id
        assert changed.observation_id != offer.observation_id


def test_envelope_round_trip_returns_fresh_json_builtins():
    module = _offer_lib()
    schema = _kubernetes_schema(module)
    offer = _make_offer(module, candidate_zones=('zone-b', 'zone-a'))
    first = offer.to_envelope()
    second = offer.to_envelope()
    _assert_json_builtins(first)
    assert first == second
    assert first is not second
    assert first['resources'] is not second['resources']
    assert (first['provider_payload']['identity']
            is not second['provider_payload']['identity'])
    first['resources']['instance_type'] = 'changed'
    first['provider_payload']['identity'][
        'rendered_pod_placement_fingerprint'] = _digest('9')
    assert second == offer.to_envelope()
    assert module.PlacementOfferV1.from_envelope(second,
                                                 payload_schema=schema) == offer
    serialized = offer.to_json()
    assert module.PlacementOfferV1.from_json(serialized,
                                             payload_schema=schema) == offer
    assert module.PlacementOfferV1.from_json(serialized.encode('utf-8'),
                                             payload_schema=schema) == offer
    parsed = module.PlacementOfferV1.from_json(serialized,
                                               payload_schema=schema)
    parsed_envelope = parsed.to_envelope()
    parsed_envelope['candidate_zones'].append('zone-c')
    assert parsed.to_envelope()['candidate_zones'] == ['zone-b', 'zone-a']
    assert offer.to_envelope()['candidate_zones'] == ['zone-b', 'zone-a']


def test_envelope_recomputes_and_rejects_mismatched_digests():
    module = _offer_lib()
    schema = _kubernetes_schema(module)
    offer = _make_offer(module)
    envelope = offer.to_envelope()
    independently_computed = _recompute_ids(module, envelope)
    assert independently_computed['offer_id'] == offer.offer_id
    assert independently_computed['observation_id'] == offer.observation_id
    mutations = (
        ('offer_id', f'kubernetes:{_digest("9")}'),
        ('observation_id', _digest('9')),
        ('region', 'ctx-b'),
        ('ttl_seconds', 16),
    )
    for key, value in mutations:
        tampered = copy.deepcopy(envelope)
        tampered[key] = value
        with pytest.raises((TypeError, ValueError)):
            module.PlacementOfferV1.from_envelope(tampered,
                                                  payload_schema=schema)
        with pytest.raises((TypeError, ValueError)):
            module.PlacementOfferV1.from_json(json.dumps(tampered),
                                              payload_schema=schema)
    stable_tampered = copy.deepcopy(envelope)
    stable_tampered['provider_payload']['identity'][
        'rendered_pod_placement_fingerprint'] = _digest('9')
    with pytest.raises((TypeError, ValueError)):
        module.PlacementOfferV1.from_envelope(stable_tampered,
                                              payload_schema=schema)
    observation_tampered = copy.deepcopy(envelope)
    observation_tampered['provider_payload']['observation'][
        'configuration_fingerprint'] = _digest('9')
    with pytest.raises((TypeError, ValueError)):
        module.PlacementOfferV1.from_envelope(observation_tampered,
                                              payload_schema=schema)


def test_plan_create_cannot_be_enveloped_or_handed_off():
    module = _offer_lib()
    reasons = module.OfferReasonCodeV1
    for operation in (
            module.OfferOperationV1.REUSE,
            module.OfferOperationV1.RESTART,
    ):
        with pytest.raises((TypeError, ValueError)):
            _make_offer(module, operation=operation)
    plan_offer = _make_offer(module,
                             operation=module.OfferOperationV1.PLAN_CREATE)
    fresh_offer = _make_offer(module)
    with pytest.raises((TypeError, ValueError)):
        plan_offer.to_envelope()
    with pytest.raises((TypeError, ValueError)):
        plan_offer.to_json()
    for mode in module.PlacementOfferActuationModeV1:
        with pytest.raises((TypeError, ValueError)):
            module.PlacementOfferHandoffV1(
                mode=mode,
                offer=plan_offer,
                actuation_context=None,
                provider_attempt_count=1,
                reason_code=reasons.NONE,
            )

    schema = _kubernetes_schema(module)
    persisted_plan = fresh_offer.to_envelope()
    persisted_plan['operation'] = 'plan_create'
    persisted_plan = _recompute_ids(module, persisted_plan)
    with pytest.raises((TypeError, ValueError)):
        module.PlacementOfferV1.from_envelope(persisted_plan,
                                              payload_schema=schema)

    operations = tuple(module.OfferOperationV1)
    offer_options = (None, plan_offer, fresh_offer)
    capture_options = (None, _SELECTION_CAPTURE_ID)
    valid_decisions = []
    for operation, selected_offer, capture_id in itertools.product(
            operations, offer_options, capture_options):
        valid = (operation is module.OfferOperationV1.PLAN_CREATE and
                 ((selected_offer is None) or selected_offer is plan_offer) and
                 not (selected_offer is not None and capture_id is None))
        try:
            decision = module.TaskPlacementDecisionV1(
                task_index=0,
                resources_fingerprint=_digest('a'),
                operation=operation,
                offer=selected_offer,
                selection_capture_id=capture_id,
            )
        except (TypeError, ValueError):
            assert not valid
        else:
            assert valid
            valid_decisions.append(decision)
    assert len(valid_decisions) == 3

    for invalid_capture in (
            _SELECTION_CAPTURE_ID.upper(),
            _SELECTION_CAPTURE_ID.replace('-', ''),
            '123e4567-e89b-12d3-b456-426614174001',
    ):
        with pytest.raises((TypeError, ValueError)):
            module.TaskPlacementDecisionV1(
                task_index=0,
                resources_fingerprint=_digest('a'),
                operation=module.OfferOperationV1.PLAN_CREATE,
                offer=None,
                selection_capture_id=invalid_capture,
            )
    with pytest.raises((TypeError, ValueError)):
        module.TaskPlacementDecisionV1(
            task_index=-1,
            resources_fingerprint=_digest('a'),
            operation=module.OfferOperationV1.PLAN_CREATE,
            offer=None,
            selection_capture_id=None,
        )

    def make_decision(index):
        return module.TaskPlacementDecisionV1(
            task_index=index,
            resources_fingerprint=_digest(hex(index + 10)[-1]),
            operation=module.OfferOperationV1.PLAN_CREATE,
            offer=None,
            selection_capture_id=None,
        )

    ordered = (make_decision(2), make_decision(0), make_decision(5))
    plan = module.OptimizationOfferPlanV1(decisions=ordered)
    assert plan.decisions is ordered
    assert tuple(item.task_index for item in plan.decisions) == (2, 0, 5)
    with pytest.raises((TypeError, ValueError)):
        module.OptimizationOfferPlanV1(decisions=(make_decision(0),
                                                  make_decision(0)))
    with pytest.raises((TypeError, ValueError)):
        module.OptimizationOfferPlanV1(decisions=list(ordered))


def test_envelope_rejects_unknown_duplicate_float_and_secret_like_values():
    module = _offer_lib()
    schema = _kubernetes_schema(module)
    envelope = _make_offer(module).to_envelope()

    unknown_paths = (
        (),
        ('scope',),
        ('resources',),
        ('price',),
        ('evidence',),
        ('provider_payload',),
        ('provider_payload', 'identity'),
        ('provider_payload', 'observation'),
    )
    for path in unknown_paths:
        changed = copy.deepcopy(envelope)
        target = changed
        for part in path:
            target = target[part]
        target['unknown'] = 'value'
        with pytest.raises((TypeError, ValueError)):
            module.PlacementOfferV1.from_envelope(changed,
                                                  payload_schema=schema)
    accelerator_envelope = _make_offer(
        module,
        resources=_resources(module,
                             accelerators=(module.OfferAcceleratorV1(
                                 name='A100', count=1),)),
    ).to_envelope()
    accelerator_envelope['resources']['accelerators'][0]['unknown'] = 1
    with pytest.raises((TypeError, ValueError)):
        module.PlacementOfferV1.from_envelope(accelerator_envelope,
                                              payload_schema=schema)

    duplicate_outer = _make_offer(module).to_json().replace(
        '{"actuation_kind"', '{"schema_version":1,"actuation_kind"', 1)
    duplicate_nested = _make_offer(module).to_json().replace(
        '"scope":{"id"', '"scope":{"kind":"duplicate","id"', 1)
    for serialized in (duplicate_outer, duplicate_nested):
        with pytest.raises((TypeError, ValueError)):
            module.PlacementOfferV1.from_json(serialized, payload_schema=schema)

    deeply_nested = None
    for _ in range(1200):
        deeply_nested = [deeply_nested]
    deep_unknown = copy.deepcopy(envelope)
    deep_unknown['unknown'] = deeply_nested
    with pytest.raises(ValueError):
        module.PlacementOfferV1.from_envelope(deep_unknown,
                                              payload_schema=schema)
    deep_serialized = ('{"unknown":' + '[' * 1200 + 'null' + ']' * 1200 + '}')
    with pytest.raises(ValueError):
        module.PlacementOfferV1.from_json(deep_serialized,
                                          payload_schema=schema)

    for timestamp in (
            '2026-07-30T12:34:56+00:00',
            '2026-02-30T12:34:56Z',
            '2026-07-30T12:34:56.000Z',
    ):
        changed = copy.deepcopy(envelope)
        changed['observed_at'] = timestamp
        with pytest.raises(ValueError, match='observed_at'):
            module.PlacementOfferV1.from_json(json.dumps(changed),
                                              payload_schema=schema)

    for value in (0.0, float('nan'), float('inf'), float('-inf')):
        changed = copy.deepcopy(envelope)
        changed['ttl_seconds'] = value
        with pytest.raises((TypeError, ValueError)):
            module.PlacementOfferV1.from_envelope(changed,
                                                  payload_schema=schema)
    for constant in ('NaN', 'Infinity', '-Infinity'):
        serialized = _make_offer(module).to_json().replace(
            '"ttl_seconds":15', f'"ttl_seconds":{constant}')
        with pytest.raises((TypeError, ValueError)):
            module.PlacementOfferV1.from_json(serialized, payload_schema=schema)
    with pytest.raises((TypeError, ValueError)):
        module.PlacementOfferV1.from_json(bytearray(
            _make_offer(module).to_json(), 'utf-8'),
                                          payload_schema=schema)
    with pytest.raises((TypeError, ValueError)):
        module.PlacementOfferV1.from_json(b'\xff', payload_schema=schema)
    with pytest.raises((TypeError, ValueError)):
        module.PlacementOfferV1.from_envelope([], payload_schema=schema)
    for surrogate in ('\ud800', '\udfff'):
        changed = copy.deepcopy(envelope)
        changed['region'] = surrogate
        with pytest.raises((TypeError, ValueError, UnicodeEncodeError)):
            module.PlacementOfferV1.from_envelope(changed,
                                                  payload_schema=schema)
        serialized = _make_offer(module).to_json().replace(
            '"ctx-a"', json.dumps(surrogate))
        with pytest.raises((TypeError, ValueError, UnicodeEncodeError)):
            module.PlacementOfferV1.from_json(serialized, payload_schema=schema)

    deny_keys = (
        'secret',
        'PASSWORD',
        'prefix-passwd-suffix',
        'refresh_token',
        'credential',
        'credentials',
        'kubeconfig',
        'authorization',
        'cookie',
        'api_key',
        'api-key',
        'access_key',
        'private-key',
        'client_secret',
        'apikey',
        'accesskey',
        'privatekey',
        'clientsecret',
    )
    allow_keys = (
        'tokenizer',
        'credentialed',
        'monkey',
        'key_count',
    )
    for key in deny_keys:
        with pytest.raises((TypeError, ValueError)):
            _object_node(module, fields=((key, _string_node(module)),))
    for key in allow_keys:
        node = _object_node(module, fields=((key, _string_node(module)),))
        assert node.fields[0][0] == key


def test_from_json_normalizes_decoder_recursion(monkeypatch):
    module = _offer_lib()
    schema = _kubernetes_schema(module)

    def raise_recursion(*unused_args, **unused_kwargs):
        raise RecursionError('forced decoder recursion exhaustion')

    monkeypatch.setattr(module.json, 'loads', raise_recursion)
    with pytest.raises(ValueError, match='not valid V1 JSON') as exc_info:
        module.PlacementOfferV1.from_json('{}', payload_schema=schema)
    assert isinstance(exc_info.value.__cause__, RecursionError)


def test_envelope_scalar_collection_depth_and_byte_boundaries(monkeypatch):
    module = _offer_lib()

    # Fixed Unicode 3.2 NFC goldens: these code points changed behavior in
    # later runtime databases, so default unicodedata.normalize is forbidden.
    left_right_pairs = (
        ('\u0301\U00016ff0', '\U00016ff0\u0301'),
        ('\u0301\U00016ff1', '\U00016ff1\u0301'),
    )
    for left, right in left_right_pairs:
        assert unicodedata.ucd_3_2_0.normalize('NFC', left) == right
        _resources(module, instance_type=right)
        with pytest.raises((TypeError, ValueError)):
            _resources(module, instance_type=left)

    monkeypatch.setattr(unicodedata, 'category', lambda unused: 'Ll')
    for codepoint in (*range(0x00, 0x20), *range(0x7f, 0xa0)):
        control = chr(codepoint)
        with pytest.raises((TypeError, ValueError)):
            _resources(module, instance_type=f'a{control}b')

    provider63 = 'p' * 63
    provider63_schema = _kubernetes_schema(module, provider=provider63)
    _make_offer(module, provider=provider63, payload_schema=provider63_schema)
    with pytest.raises((TypeError, ValueError)):
        _kubernetes_schema(module, provider='p' * 64)

    scope128 = module.OfferScopeV1(kind='s' * 128, id=_digest('1'))
    _make_offer(module, scope=scope128)
    with pytest.raises((TypeError, ValueError)):
        module.OfferScopeV1(kind='s' * 129, id=_digest('1'))
    for invalid_digest in (
            'sha256:' + 'a' * 63,
            'sha256:' + 'A' * 64,
            'md5:' + 'a' * 64,
    ):
        with pytest.raises((TypeError, ValueError)):
            module.OfferScopeV1(kind='scope', id=invalid_digest)
    _make_offer(module, batching_scope='b' * 128)
    with pytest.raises((TypeError, ValueError)):
        _make_offer(module, batching_scope='b' * 129)

    _resources(module, instance_type='a' * 256)
    with pytest.raises((TypeError, ValueError)):
        _resources(module, instance_type='')
    with pytest.raises((TypeError, ValueError)):
        _resources(module, instance_type='a' * 257)
    maximum_decimal = '9' * 38 + '.' + '0' * 17 + '1'
    _resources(module, cpus=maximum_decimal, memory_gib=maximum_decimal)
    _make_offer(module, price_amount=maximum_decimal)
    for invalid_decimal in (
            '9' * 39,
            '01',
            '1.0',
            '1.',
            '-1',
            '1e2',
            '',
    ):
        with pytest.raises((TypeError, ValueError)):
            _resources(module, cpus=invalid_decimal)
        with pytest.raises((TypeError, ValueError)):
            _make_offer(module, price_amount=invalid_decimal)
    _resources(module, disk_tier='d' * 64, network_tier='n' * 64)
    with pytest.raises((TypeError, ValueError)):
        _resources(module, disk_tier='d' * 65)
    with pytest.raises((TypeError, ValueError)):
        _resources(module, network_tier='n' * 65)

    maximum_accelerator = module.OfferAcceleratorV1(name='a' * 128,
                                                    count=2_147_483_647)
    sorted_accelerators = tuple(
        module.OfferAcceleratorV1(name=f'a{index}', count=1)
        for index in range(8))
    _resources(module, accelerators=sorted_accelerators)
    _resources(module, accelerators=(maximum_accelerator,))
    for name, count in (
        ('', 1),
        ('a' * 129, 1),
        ('a', 0),
        ('a', 2_147_483_648),
        ('a', True),
    ):
        with pytest.raises((TypeError, ValueError)):
            module.OfferAcceleratorV1(name=name, count=count)
    with pytest.raises((TypeError, ValueError)):
        _resources(module,
                   accelerators=tuple(
                       module.OfferAcceleratorV1(name=f'a{index}', count=1)
                       for index in range(9)))
    with pytest.raises((TypeError, ValueError)):
        _resources(module,
                   accelerators=(sorted_accelerators[1],
                                 sorted_accelerators[0]))
    with pytest.raises((TypeError, ValueError)):
        _resources(module,
                   accelerators=(sorted_accelerators[0],
                                 sorted_accelerators[0]))

    _make_offer(module, region='r' * 1024)
    for invalid_region in ('', 'r' * 1025):
        with pytest.raises((TypeError, ValueError)):
            _make_offer(module, region=invalid_region)
    _make_offer(module, candidate_zones=('z' * 1024,))
    for invalid_zone in ('', 'z' * 1025):
        with pytest.raises((TypeError, ValueError)):
            _make_offer(module, candidate_zones=(invalid_zone,))
    with pytest.raises((TypeError, ValueError)):
        _make_offer(module, candidate_zones=('zone', 'zone'))
    _make_offer(module, candidate_zones=tuple(f'z{i}' for i in range(32)))
    with pytest.raises((TypeError, ValueError)):
        _make_offer(module, candidate_zones=tuple(f'z{i}' for i in range(33)))
    for ttl_seconds in (1, 300):
        _make_offer(module, ttl_seconds=ttl_seconds)
    for ttl_seconds in (0, 301, True):
        with pytest.raises((TypeError, ValueError)):
            _make_offer(module, ttl_seconds=ttl_seconds)
    for invalid_observed_at in (
            _OBSERVED_AT.replace(tzinfo=None),
            _OBSERVED_AT.replace(microsecond=1),
            _OBSERVED_AT.astimezone(
                datetime.timezone(datetime.timedelta(hours=1))),
    ):
        with pytest.raises((TypeError, ValueError)):
            _make_offer(module, observed_at=invalid_observed_at)
    for requested_nodes in (1, 10_000):
        _make_offer(module, requested_nodes=requested_nodes)
    for requested_nodes in (0, 10_001, True):
        with pytest.raises((TypeError, ValueError)):
            _make_offer(module, requested_nodes=requested_nodes)

    string_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module,
                              fields=(('value', _string_node(module)),)),
        observation=_object_node(module),
    )
    module.OfferProviderPayloadV1.create(
        identity={'value': 'x' * 1024},
        observation={},
        payload_schema=string_schema,
    )
    with pytest.raises((TypeError, ValueError)):
        module.OfferProviderPayloadV1.create(
            identity={'value': 'x' * 1025},
            observation={},
            payload_schema=string_schema,
        )

    key64 = 'k' * 64
    key64_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module, fields=((key64, _string_node(module)),)),
        observation=_object_node(module),
    )
    module.OfferProviderPayloadV1.create(
        identity={key64: 'value'},
        observation={},
        payload_schema=key64_schema,
    )
    for invalid_key in ('', 'k' * 65, 'bad\nkey', 'nonascii-\u00e9'):
        with pytest.raises((TypeError, ValueError)):
            _object_node(module, fields=((invalid_key, _string_node(module)),))

    scalar_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(
            module,
            fields=(
                ('boolean',
                 _schema_node(module,
                              module.ProviderPayloadNodeKindV1.BOOLEAN)),
                ('digest', _digest_node(module)),
                ('null',
                 _schema_node(module, module.ProviderPayloadNodeKindV1.NULL)),
            )),
        observation=_object_node(module),
    )
    module.OfferProviderPayloadV1.create(
        identity={
            'boolean': True,
            'digest': _digest('1'),
            'null': None,
        },
        observation={},
        payload_schema=scalar_schema,
    )
    for bad_identity in (
        {
            'boolean': 1,
            'digest': _digest('1'),
            'null': None,
        },
        {
            'boolean': True,
            'digest': 'sha256:' + 'A' * 64,
            'null': None,
        },
        {
            'boolean': True,
            'digest': _digest('1'),
            'null': False,
        },
    ):
        with pytest.raises((TypeError, ValueError)):
            module.OfferProviderPayloadV1.create(
                identity=bad_identity,
                observation={},
                payload_schema=scalar_schema,
            )

    integer_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module,
                              fields=(('value', _integer_node(module)),)),
        observation=_object_node(module),
    )
    for value in (-(2**63), 2**63 - 1):
        module.OfferProviderPayloadV1.create(
            identity={'value': value},
            observation={},
            payload_schema=integer_schema,
        )
    for value in (-(2**63) - 1, 2**63, True):
        with pytest.raises((TypeError, ValueError)):
            module.OfferProviderPayloadV1.create(
                identity={'value': value},
                observation={},
                payload_schema=integer_schema,
            )

    array_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module,
                              fields=(('values',
                                       _array_node(module,
                                                   _integer_node(module))),)),
        observation=_object_node(module),
    )
    module.OfferProviderPayloadV1.create(
        identity={'values': list(range(32))},
        observation={},
        payload_schema=array_schema,
    )
    with pytest.raises((TypeError, ValueError)):
        module.OfferProviderPayloadV1.create(
            identity={'values': list(range(33))},
            observation={},
            payload_schema=array_schema,
        )

    fields32 = tuple((f'k{i:02d}', _integer_node(module)) for i in range(32))
    object32_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module, fields=fields32),
        observation=_object_node(module),
    )
    module.OfferProviderPayloadV1.create(
        identity={f'k{i:02d}': i for i in range(32)},
        observation={},
        payload_schema=object32_schema,
    )
    with pytest.raises((TypeError, ValueError)):
        _object_node(
            module,
            fields=tuple(
                (f'k{i:02d}', _integer_node(module)) for i in range(33)),
        )

    combined64_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module, fields=fields32),
        observation=_object_node(module, fields=fields32),
    )
    module.OfferProviderPayloadV1.create(
        identity={f'k{i:02d}': i for i in range(32)},
        observation={f'k{i:02d}': i for i in range(32)},
        payload_schema=combined64_schema,
    )
    nested_fields = (
        ('child',
         _object_node(module, fields=(
             ('extra', _integer_node(module)),))),) + (fields32[1:])
    combined65_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module, fields=nested_fields),
        observation=_object_node(module, fields=fields32),
    )
    with pytest.raises((TypeError, ValueError)):
        module.OfferProviderPayloadV1.create(
            identity={
                'child': {
                    'extra': 1
                },
                **{
                    f'k{i:02d}': i for i in range(1, 32)
                },
            },
            observation={f'k{i:02d}': i for i in range(32)},
            payload_schema=combined65_schema,
        )

    arrays_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module,
                              fields=tuple(
                                  (f'a{i}',
                                   _array_node(module, _integer_node(module)))
                                  for i in range(4))),
        observation=_object_node(module),
    )
    arrays128 = {f'a{i}': list(range(32)) for i in range(4)}
    module.OfferProviderPayloadV1.create(
        identity=arrays128,
        observation={},
        payload_schema=arrays_schema,
    )
    arrays129_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module,
                              fields=tuple(
                                  (f'a{i}',
                                   _array_node(module, _integer_node(module)))
                                  for i in range(5))),
        observation=_object_node(module),
    )
    with pytest.raises((TypeError, ValueError)):
        module.OfferProviderPayloadV1.create(
            identity={
                **arrays128,
                'a4': [0],
            },
            observation={},
            payload_schema=arrays129_schema,
        )

    def nested_schema(depth):
        node = _integer_node(module)
        for _ in range(depth):
            node = _object_node(module, fields=(('child', node),))
        return node

    def nested_value(depth):
        value = 1
        for _ in range(depth):
            value = {'child': value}
        return value

    depth4_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module, fields=(('root', nested_schema(4)),)),
        observation=_object_node(module),
    )
    module.OfferProviderPayloadV1.create(
        identity={'root': nested_value(4)},
        observation={},
        payload_schema=depth4_schema,
    )
    depth5_schema = module.ProviderPayloadSchemaV1(
        provider='kubernetes',
        identity=_object_node(module, fields=(('root', nested_schema(5)),)),
        observation=_object_node(module),
    )
    with pytest.raises((TypeError, ValueError)):
        module.OfferProviderPayloadV1.create(
            identity={'root': nested_value(5)},
            observation={},
            payload_schema=depth5_schema,
        )

    schema4096, raw4096 = _payload_with_exact_size(module, 4096)
    payload4096 = module.OfferProviderPayloadV1.create(
        identity=raw4096['identity'],
        observation=raw4096['observation'],
        payload_schema=schema4096,
    )
    schema4097, raw4097 = _payload_with_exact_size(module, 4097)
    with pytest.raises((TypeError, ValueError)):
        module.OfferProviderPayloadV1.create(
            identity=raw4097['identity'],
            observation=raw4097['observation'],
            payload_schema=schema4097,
        )

    base_offer = module.PlacementOfferV1.create(
        operation=module.OfferOperationV1.FRESH_CREATE,
        actuation_kind=module.OfferActuationKindV1.DIRECT_POD,
        provider='kubernetes',
        scope=module.OfferScopeV1(
            kind='kubernetes_context_endpoint_identity_namespace_v1',
            id=_digest('1')),
        resources=_resources(module),
        region='ctx-a',
        candidate_zones=(),
        batching_scope='context',
        price=module.OfferPriceV1(
            amount='0.42',
            basis=module.OfferPriceBasisV1.NODE_HOUR,
            currency=module.OfferCurrencyV1.USD,
        ),
        purchase_mode=module.OfferPurchaseModeV1.ON_DEMAND,
        availability=module.OfferAvailabilityV1.UNKNOWN,
        observed_at=_OBSERVED_AT,
        ttl_seconds=15,
        revalidation_policy=(module.OfferRevalidationPolicyV1.BEFORE_MUTATION),
        evidence=module.OfferEvidenceV1(
            reservation=module.OfferReservationEvidenceV1.NOT_APPLICABLE,
            quota=module.OfferQuotaEvidenceV1.UNKNOWN,
            capacity=(module.OfferCapacityEvidenceV1.SHAPE_FITS_EXISTING_NODE),
            requested_nodes=1,
        ),
        provider_payload=payload4096,
        payload_schema=schema4096,
    )
    envelope16384 = _envelope_with_exact_size(module, base_offer.to_envelope(),
                                              16384)
    assert len(module.canonical_json_bytes_v1(envelope16384)) == 16384
    module.PlacementOfferV1.from_envelope(envelope16384,
                                          payload_schema=schema4096)
    envelope16385 = _envelope_with_exact_size(module, base_offer.to_envelope(),
                                              16385)
    assert len(module.canonical_json_bytes_v1(envelope16385)) == 16385
    with pytest.raises((TypeError, ValueError)):
        module.PlacementOfferV1.from_envelope(envelope16385,
                                              payload_schema=schema4096)


def test_handoff_disposition_matrix():
    module = _offer_lib()
    modes = module.PlacementOfferActuationModeV1
    reasons = module.OfferReasonCodeV1
    fresh_offer = _make_offer(module)
    plan_offer = _make_offer(module,
                             operation=module.OfferOperationV1.PLAN_CREATE)
    context = _Context()
    wrong_context = _Context(provider='gcp')
    first_attempt_reasons = {
        reasons.UNSUPPORTED_OPERATION,
        reasons.UNSUPPORTED_ACTUATION_KIND,
        reasons.UNSUPPORTED_NODE_COUNT,
        reasons.UNSUPPORTED_ACCELERATOR,
        reasons.UNSUPPORTED_RESOURCE_MODE,
        reasons.UNSUPPORTED_NETWORK_TIER,
        reasons.VOLUME_OR_STORAGE_MOUNT,
        reasons.KUEUE_ENABLED,
        reasons.RESERVATION_REQUESTED,
        reasons.CUSTOM_PLACEMENT_CONFIG,
        reasons.UNRESOLVED_SCOPE,
        reasons.OBSERVATION_LIMIT_EXCEEDED,
    }
    for mode, offer, actuation_context, attempt, reason in itertools.product(
            modes,
        (None, fresh_offer, plan_offer),
        (None, context, wrong_context),
        (1, 2),
            reasons,
    ):
        valid = (
            mode is modes.SHADOW and offer is fresh_offer and
            actuation_context is None and reason is reasons.NONE) or (
                mode is modes.SHADOW_LEGACY_FALLBACK and offer is None and
                actuation_context is None and
                reason not in {reasons.NONE, reasons.PROVIDER_OBJECT_CONFLICT}
            ) or (mode is modes.AUTHORITATIVE and offer is fresh_offer and
                  actuation_context is context and attempt == 1 and
                  reason is reasons.NONE) or (
                      mode is modes.LEGACY_FIRST_ATTEMPT and offer is None and
                      actuation_context is None and attempt == 1 and
                      reason in first_attempt_reasons) or (
                          mode is modes.LEGACY_RETRY_AFTER_PROVIDER_ATTEMPT and
                          offer is None and actuation_context is None and
                          attempt >= 2 and
                          reason is reasons.RETRY_AFTER_PROVIDER_ATTEMPT)
        try:
            result = module.PlacementOfferHandoffV1(
                mode=mode,
                offer=offer,
                actuation_context=actuation_context,
                provider_attempt_count=attempt,
                reason_code=reason,
            )
        except (TypeError, ValueError):
            assert not valid
        else:
            assert valid
            assert result.provider_attempt_count == attempt
    for attempt in (0, -1):
        with pytest.raises((TypeError, ValueError)):
            module.PlacementOfferHandoffV1(
                mode=modes.SHADOW,
                offer=fresh_offer,
                actuation_context=None,
                provider_attempt_count=attempt,
                reason_code=reasons.NONE,
            )


def test_offer_module_has_only_allowed_leaf_imports():
    offer_source = _OFFER_PATH.read_text()
    json_types_source = _JSON_TYPES_PATH.read_text()
    offer_tree = ast.parse(offer_source)
    json_types_tree = ast.parse(json_types_source)
    ast.parse(offer_source, feature_version=(3, 10))
    ast.parse(json_types_source, feature_version=(3, 10))
    assert isinstance(offer_tree.body[0], ast.Expr)
    assert isinstance(offer_tree.body[1], ast.ImportFrom)
    assert offer_tree.body[1].module == '__future__'
    assert any(
        alias.name == 'annotations' for alias in offer_tree.body[1].names)

    sky_imports = []
    for node in ast.walk(offer_tree):
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            if node.module == 'sky' or node.module.startswith('sky.'):
                sky_imports.append(node.module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == 'sky' or alias.name.startswith('sky.'):
                    sky_imports.append(alias.name)
    assert set(sky_imports) <= {'sky', 'sky.utils.json_types'}
    assert not any(
        imported.startswith(prefix) for imported in sky_imports for prefix in (
            'sky.clouds',
            'sky.optimizer',
            'sky.backends',
            'sky.provision',
            'sky.server',
            'sky.adaptors.kubernetes',
        ))
    top_level_imports = [
        node for node in offer_tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]
    assert not any(
        isinstance(node, ast.ImportFrom) and node.module == 'sky'
        for node in top_level_imports)
    runtime_import_roots = {
        alias.name.split('.')[0] for node in top_level_imports
        if isinstance(node, ast.Import) for alias in node.names
    }
    runtime_import_roots.update(
        node.module.split('.')[0]
        for node in top_level_imports
        if isinstance(node, ast.ImportFrom) and node.module not in (
            None,
            '__future__',
        ))
    assert runtime_import_roots <= (set(sys.stdlib_module_names) | {'sky'})

    class_positions = {
        node.name: index
        for index, node in enumerate(json_types_tree.body)
        if isinstance(node, ast.ClassDef)
    }
    alias_positions = {
        target.id: index for index, node in enumerate(json_types_tree.body)
        if isinstance(node, (ast.Assign, ast.AnnAssign)) for target in (
            node.targets if isinstance(node, ast.Assign) else (node.target,))
        if isinstance(target, ast.Name)
    }
    assert class_positions['FrozenJSONDict'] < alias_positions[
        'FrozenJSONValue']
    frozen_class = next(
        node for node in json_types_tree.body
        if isinstance(node, ast.ClassDef) and node.name == 'FrozenJSONDict')
    assert ast.unparse(
        frozen_class.bases[0]).startswith('collections.abc.Mapping[')
    assert not any(
        isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr) and any(
            isinstance(child, ast.Constant) and child.value == 'FrozenJSONDict'
            for child in ast.walk(node))
        for node in ast.walk(json_types_tree))

    # Importing the leaves exercises the runtime recursive alias on the
    # executing interpreter. CI supplies the Python 3.10 floor and 3.14 ceiling.
    json_types = importlib.import_module('sky.utils.json_types')
    offer = _offer_lib()
    assert json_types.FrozenJSONValue is not None
    assert offer.OfferOperationV1.FRESH_CREATE.value == 'fresh_create'

    setup_source = (_ROOT / 'setup.py').read_text()
    assert "requires_python='>=3.10'" in setup_source
    classifiers = set(
        re.findall(r'Programming Language :: Python :: (3\.\d+)', setup_source))
    assert classifiers == {'3.10', '3.11', '3.12', '3.13', '3.14'}
    workflow = (_ROOT / '.github' / 'workflows' /
                'static-analysis.yml').read_text()
    assert 'worker-floor-import' in workflow
    assert re.search(r'python-version:\s*[\'"]?3\.10', workflow)
    assert 'sky.placement.offer' in workflow
    assert 'sky.utils.json_types' in workflow


def test_placement_types_are_not_publicly_reexported():
    module = _offer_lib()
    sky = importlib.import_module('sky')
    clouds = importlib.import_module('sky.clouds')
    placement = importlib.import_module('sky.placement')
    placement_type_names = tuple(
        name for name, value in vars(module).items() if name.endswith('V1') and
        (inspect.isclass(value) or inspect.isfunction(value)))
    assert placement_type_names
    for package in (sky, clouds, placement):
        for name in placement_type_names:
            assert name not in vars(package)


def test_cloud_offer_source_default_is_none_and_side_effect_free(monkeypatch):
    cloud_module = importlib.import_module('sky.clouds.cloud')
    method = cloud_module.Cloud.get_offer_source
    assert tuple(inspect.signature(method).parameters) == ('self',)
    imported = []
    original_import = __import__

    def recording_import(name, *args, **kwargs):
        imported.append(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr('builtins.__import__', recording_import)
    modules_before = frozenset(sys.modules)
    assert method(object()) is None
    assert frozenset(sys.modules) == modules_before
    assert not imported
