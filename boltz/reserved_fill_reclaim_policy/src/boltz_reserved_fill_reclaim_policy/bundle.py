"""Strict immutable fleet-bundle parsing for the Boltz reclaim policy."""

from collections.abc import Mapping
import dataclasses
from importlib import resources
import hashlib
import json
import re
from typing import Any, Final

from boltz_reserved_fill_reclaim_policy import __version__

_BUNDLE_RESOURCE: Final = 'fleet_bundle.json'
_MAX_BUNDLE_BYTES: Final = 1024 * 1024
_SHA256_RE: Final = re.compile(r'^sha256:[0-9a-f]{64}$')
_ROLE_ARN_RE: Final = re.compile(
    r'^arn:(?:aws|aws-us-gov|aws-cn):iam::[0-9]{12}:'
    r'role/[A-Za-z0-9+=,.@_/-]+$')
_UUID_RE: Final = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-'
                             r'[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
_FLEET_HASH_DOMAIN: Final = b'boltz-reserved-fill/fleet/v1\x00'
_PROVIDER_HASH_DOMAIN: Final = b'boltz-reserved-fill/provider/v1\x00'


class BundleValidationError(ValueError):
    """The code-owned fleet bundle is malformed or internally inconsistent."""


def _object_without_duplicate_keys(
        pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise BundleValidationError(f'Duplicate JSON key {key!r}.')
        result[key] = value
    return result


def _mapping(value: object, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BundleValidationError(f'{path} must be a JSON object.')
    return value


def _list(value: object, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise BundleValidationError(f'{path} must be a JSON array.')
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str],
                path: str) -> None:
    actual = set(value)
    if actual != expected:
        raise BundleValidationError(f'{path} has unexpected schema keys.')


def _text(value: object, path: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise BundleValidationError(f'{path} must be nonempty canonical text.')
    return value


def _integer(value: object, path: str, *, minimum: int = 1) -> int:
    if type(value) is not int or value < minimum:
        raise BundleValidationError(
            f'{path} must be an integer greater than or equal to {minimum}.')
    return value


def _optional_role(value: object, path: str) -> str | None:
    if value is None:
        return None
    role = _text(value, path)
    if _ROLE_ARN_RE.fullmatch(role) is None:
        raise BundleValidationError(f'{path} must be null or an IAM role ARN.')
    return role


def _validate_quota_list(value: object, path: str) -> None:
    quotas = _list(value, path)
    if not quotas:
        raise BundleValidationError(f'{path} must not be empty.')
    flavors: list[str] = []
    for index, item in enumerate(quotas):
        quota = _mapping(item, f'{path}[{index}]')
        _exact_keys(
            quota,
            {'flavor', 'nominal_quota', 'borrowing_limit', 'resource_name'},
            f'{path}[{index}]')
        flavors.append(_text(quota['flavor'], f'{path}[{index}].flavor'))
        if quota['resource_name'] != 'nvidia.com/gpu':
            raise BundleValidationError(
                f'{path}[{index}].resource_name must be nvidia.com/gpu.')
        for key in ('nominal_quota', 'borrowing_limit'):
            quantity = _text(quota[key], f'{path}[{index}].{key}')
            if not quantity.isdecimal():
                raise BundleValidationError(
                    f'{path}[{index}].{key} must be an integer quantity.')
    if len(flavors) != len(set(flavors)):
        raise BundleValidationError(f'{path} has duplicate flavors.')


def _validate_preemption(value: object, path: str) -> None:
    policy = _mapping(value, path)
    _exact_keys(policy, {
        'borrow_within_cohort', 'reclaim_within_cohort', 'within_cluster_queue'
    }, path)
    allowed = {
        'borrow_within_cohort': {'Never', 'LowerPriority'},
        'reclaim_within_cohort': {'Never', 'LowerPriority', 'Any'},
        'within_cluster_queue': {
            'Never', 'LowerPriority', 'LowerOrNewerEqualPriority'
        },
    }
    for key, accepted in allowed.items():
        if policy[key] not in accepted:
            raise BundleValidationError(f'{path}.{key} is unsupported.')


def _validate_fleet_context(value: object, path: str) -> None:
    context = _mapping(value, path)
    _exact_keys(
        context, {
            'accelerators', 'kubernetes_context', 'local_queue_name',
            'namespace', 'physical_cluster_uid', 'pod_identity_role_arn',
            'priority_class', 'queues', 'scheduler_name',
            'service_account_name', 'workload_priority_class_name'
        }, path)
    for key in ('kubernetes_context', 'local_queue_name', 'namespace',
                'scheduler_name', 'service_account_name',
                'workload_priority_class_name'):
        _text(context[key], f'{path}.{key}')
    if _UUID_RE.fullmatch(
            _text(context['physical_cluster_uid'],
                  f'{path}.physical_cluster_uid')) is None:
        raise BundleValidationError(
            f'{path}.physical_cluster_uid must be a UUID.')
    _optional_role(context['pod_identity_role_arn'],
                   f'{path}.pod_identity_role_arn')
    accelerators = _mapping(context['accelerators'], f'{path}.accelerators')
    if not accelerators:
        raise BundleValidationError(f'{path}.accelerators must not be empty.')
    for accelerator, raw_contract in accelerators.items():
        if _text(accelerator,
                 f'{path}.accelerators key') != accelerator.casefold():
            raise BundleValidationError(
                f'{path}.accelerators names must be lowercase.')
        contract = _mapping(raw_contract, f'{path}.accelerators.{accelerator}')
        _exact_keys(
            contract, {
                'count', 'flavors', 'product_label_key', 'product_label_values',
                'resource_name'
            }, f'{path}.accelerators.{accelerator}')
        _integer(contract['count'], f'{path}.accelerators.{accelerator}.count')
        if (contract['resource_name'] != 'nvidia.com/gpu' or
                contract['product_label_key'] != 'nvidia.com/gpu.product'):
            raise BundleValidationError(
                f'{path}.accelerators.{accelerator} has an unsupported GPU '
                'resource or product-label key.')
        for key in ('flavors', 'product_label_values'):
            items = _list(contract[key],
                          f'{path}.accelerators.{accelerator}.{key}')
            if not items or any(
                    type(item) is not str or not item for item in items):
                raise BundleValidationError(
                    f'{path}.accelerators.{accelerator}.{key} must contain '
                    'nonempty text.')
        if len(contract['flavors']) != len(set(contract['flavors'])):
            raise BundleValidationError(
                f'{path}.accelerators.{accelerator}.flavors must be unique.')
        if len(contract['flavors']) != len(contract['product_label_values']):
            raise BundleValidationError(
                f'{path}.accelerators.{accelerator} must bind each flavor '
                'to one observed product value.')

    priority = _mapping(context['priority_class'], f'{path}.priority_class')
    _exact_keys(priority, {'name', 'preemption_policy', 'value'},
                f'{path}.priority_class')
    _text(priority['name'], f'{path}.priority_class.name')
    _integer(priority['value'],
             f'{path}.priority_class.value',
             minimum=-2147483648)
    if priority['value'] > 1000000000:
        raise BundleValidationError(
            f'{path}.priority_class.value exceeds Kubernetes limits.')
    if priority['preemption_policy'] not in ('Never', 'PreemptLowerPriority'):
        raise BundleValidationError(
            f'{path}.priority_class.preemption_policy is unsupported.')

    queues = _mapping(context['queues'], f'{path}.queues')
    _exact_keys(
        queues, {
            'cohort', 'inference_cluster_queue', 'inference_gpu_quotas',
            'inference_preemption', 'research_cluster_queue',
            'research_gpu_quotas', 'research_namespace', 'research_preemption'
        }, f'{path}.queues')
    for key in ('cohort', 'inference_cluster_queue', 'research_cluster_queue',
                'research_namespace'):
        _text(queues[key], f'{path}.queues.{key}')
    _validate_quota_list(queues['inference_gpu_quotas'],
                         f'{path}.queues.inference_gpu_quotas')
    _validate_quota_list(queues['research_gpu_quotas'],
                         f'{path}.queues.research_gpu_quotas')
    _validate_preemption(queues['inference_preemption'],
                         f'{path}.queues.inference_preemption')
    _validate_preemption(queues['research_preemption'],
                         f'{path}.queues.research_preemption')
    inference_quotas = {
        item['flavor']: item for item in queues['inference_gpu_quotas']
    }
    research_quotas = {
        item['flavor']: item for item in queues['research_gpu_quotas']
    }
    if set(inference_quotas) != set(research_quotas):
        raise BundleValidationError(
            f'{path}.queues GPU flavor sets must agree.')
    for flavor, inference_quota in inference_quotas.items():
        research_quota = research_quotas[flavor]
        if (inference_quota['nominal_quota'] != '0' or
                int(inference_quota['borrowing_limit']) > int(
                    research_quota['nominal_quota'])):
            raise BundleValidationError(
                f'{path}.queues inference quota must be zero-nominal and '
                'bounded by research nominal quota.')
    if (queues['inference_preemption'] != {
            'borrow_within_cohort': 'Never',
            'reclaim_within_cohort': 'Never',
            'within_cluster_queue': 'Never'
    } or queues['research_preemption']['reclaim_within_cohort'] != 'Any'):
        raise BundleValidationError(
            f'{path}.queues does not make inference a reclaimable borrower.')


def _validate_provider_context(value: object, path: str) -> None:
    context = _mapping(value, path)
    _exact_keys(
        context, {
            'admission_policy', 'eks', 'kubernetes_context', 'kueue_controller',
            'kueue_webhooks', 'namespace_uid', 'node_inventory',
            'resource_flavors', 'scheduler'
        }, path)
    _text(context['kubernetes_context'], f'{path}.kubernetes_context')
    if _UUID_RE.fullmatch(
            _text(context['namespace_uid'], f'{path}.namespace_uid')) is None:
        raise BundleValidationError(f'{path}.namespace_uid must be a UUID.')

    eks = _mapping(context['eks'], f'{path}.eks')
    _exact_keys(eks, {
        'account_id', 'audit_role_arn', 'cluster_arn', 'cluster_name', 'region'
    }, f'{path}.eks')
    for key in ('account_id', 'cluster_arn', 'cluster_name', 'region'):
        _text(eks[key], f'{path}.eks.{key}')
    if not (len(eks['account_id']) == 12 and eks['account_id'].isdecimal()):
        raise BundleValidationError(f'{path}.eks.account_id is invalid.')
    audit_role = _optional_role(eks['audit_role_arn'],
                                f'{path}.eks.audit_role_arn')
    if audit_role is None or audit_role.split(':', 5)[4] != eks['account_id']:
        raise BundleValidationError(
            f'{path}.eks.audit_role_arn must belong to the EKS account.')
    expected_arn = (f"arn:aws:eks:{eks['region']}:{eks['account_id']}:"
                    f"cluster/{eks['cluster_name']}")
    if eks['cluster_arn'] != expected_arn:
        raise BundleValidationError(f'{path}.eks.cluster_arn is inconsistent.')

    for deployment_key in ('kueue_controller', 'scheduler'):
        deployment = _mapping(context[deployment_key],
                              f'{path}.{deployment_key}')
        expected = {'deployment', 'namespace', 'replicas', 'images'}
        if deployment_key == 'kueue_controller':
            expected.add('config_map')
        else:
            expected.remove('images')
            expected.add('containers')
        _exact_keys(deployment, expected, f'{path}.{deployment_key}')
        for key in ('deployment', 'namespace'):
            _text(deployment[key], f'{path}.{deployment_key}.{key}')
        if deployment_key == 'kueue_controller':
            _text(deployment['config_map'],
                  f'{path}.{deployment_key}.config_map')
            images = _mapping(deployment['images'],
                              f'{path}.{deployment_key}.images')
        else:
            images = _mapping(deployment['containers'],
                              f'{path}.{deployment_key}.containers')
        _integer(deployment['replicas'], f'{path}.{deployment_key}.replicas')
        if not images:
            raise BundleValidationError(
                f'{path}.{deployment_key} image set must not be empty.')
        for name, image in images.items():
            _text(name, f'{path}.{deployment_key} image name')
            image_text = _text(image, f'{path}.{deployment_key}.{name}')
            if deployment_key == 'scheduler' and '@' not in image_text:
                raise BundleValidationError(
                    f'{path}.{deployment_key}.{name} must be immutable.')
            if deployment_key == 'scheduler' and _SHA256_RE.search(
                    image_text.split('@', 1)[-1]) is None:
                raise BundleValidationError(
                    f'{path}.{deployment_key}.{name} has an invalid digest.')

    admission = _mapping(context['admission_policy'],
                         f'{path}.admission_policy')
    _exact_keys(admission, {
        'binding_name', 'name', 'namespace_label_key', 'namespace_label_value'
    }, f'{path}.admission_policy')
    for key, item in admission.items():
        _text(item, f'{path}.admission_policy.{key}')
    webhooks = _mapping(context['kueue_webhooks'], f'{path}.kueue_webhooks')
    _exact_keys(webhooks,
                {'mutating', 'service_name', 'service_port', 'validating'},
                f'{path}.kueue_webhooks')
    _text(webhooks['service_name'], f'{path}.kueue_webhooks.service_name')
    service_port = _integer(webhooks['service_port'],
                            f'{path}.kueue_webhooks.service_port')
    if service_port < 1 or service_port > 65535:
        raise BundleValidationError(
            f'{path}.kueue_webhooks.service_port is invalid.')
    for kind, required_operations in (('mutating', ('CREATE',)),
                                      ('validating', ('CREATE', 'UPDATE'))):
        contract = _mapping(webhooks[kind], f'{path}.kueue_webhooks.{kind}')
        _exact_keys(
            contract,
            {'configuration_name', 'operations', 'path', 'webhook_name'},
            f'{path}.kueue_webhooks.{kind}')
        for key in ('configuration_name', 'path', 'webhook_name'):
            _text(contract[key], f'{path}.kueue_webhooks.{kind}.{key}')
        operations = tuple(
            _text(item, f'{path}.kueue_webhooks.{kind}.operations')
            for item in _list(contract['operations'],
                              f'{path}.kueue_webhooks.{kind}.operations'))
        if operations != required_operations:
            raise BundleValidationError(
                f'{path}.kueue_webhooks.{kind} does not intercept the exact '
                'reviewed Pod operations.')

    flavors = _list(context['resource_flavors'], f'{path}.resource_flavors')
    if not flavors:
        raise BundleValidationError(
            f'{path}.resource_flavors must not be empty.')
    flavor_names: list[str] = []
    for index, item in enumerate(flavors):
        flavor = _mapping(item, f'{path}.resource_flavors[{index}]')
        _exact_keys(flavor, {'name', 'node_labels'},
                    f'{path}.resource_flavors[{index}]')
        flavor_names.append(
            _text(flavor['name'], f'{path}.resource_flavors[{index}].name'))
        labels = _mapping(flavor['node_labels'],
                          f'{path}.resource_flavors[{index}].node_labels')
        if not labels:
            raise BundleValidationError(
                f'{path}.resource_flavors[{index}] must bind provider-owned '
                'Node labels.')
        for key, item in labels.items():
            _text(key, f'{path}.resource_flavors[{index}] label key')
            _text(item, f'{path}.resource_flavors[{index}].{key}')
    if len(flavor_names) != len(set(flavor_names)):
        raise BundleValidationError(f'{path}.resource_flavors has duplicates.')

    inventory = _list(context['node_inventory'], f'{path}.node_inventory')
    if not inventory:
        raise BundleValidationError(f'{path}.node_inventory must not be empty.')
    inventory_flavors: list[str] = []
    selectors: list[tuple[str, str]] = []
    flavor_labels = {item['name']: item['node_labels'] for item in flavors}
    for index, item in enumerate(inventory):
        node = _mapping(item, f'{path}.node_inventory[{index}]')
        _exact_keys(
            node, {
                'capacity_per_node', 'flavor', 'product_label_key',
                'product_label_value', 'resource_name', 'selector_label_key',
                'selector_label_value'
            }, f'{path}.node_inventory[{index}]')
        inventory_flavor = _text(node['flavor'],
                                 f'{path}.node_inventory[{index}].flavor')
        selector_key = _text(
            node['selector_label_key'],
            f'{path}.node_inventory[{index}].selector_label_key')
        selector_value = _text(
            node['selector_label_value'],
            f'{path}.node_inventory[{index}].selector_label_value')
        if (node['resource_name'] != 'nvidia.com/gpu' or
                node['product_label_key'] != 'nvidia.com/gpu.product'):
            raise BundleValidationError(
                f'{path}.node_inventory[{index}] has an unsupported GPU '
                'resource or product-label key.')
        _text(node['product_label_value'],
              f'{path}.node_inventory[{index}].product_label_value')
        _integer(node['capacity_per_node'],
                 f'{path}.node_inventory[{index}].capacity_per_node')
        if (flavor_labels.get(inventory_flavor, {}).get(selector_key)
                != selector_value):
            raise BundleValidationError(
                f'{path}.node_inventory[{index}] selector is not owned by '
                'its ResourceFlavor.')
        inventory_flavors.append(inventory_flavor)
        selectors.append((selector_key, selector_value))
    if (len(inventory_flavors) != len(set(inventory_flavors)) or
            set(inventory_flavors) != set(flavor_names)):
        raise BundleValidationError(
            f'{path}.node_inventory must cover each ResourceFlavor once.')
    if len(selectors) != len(set(selectors)):
        raise BundleValidationError(
            f'{path}.node_inventory selectors must be unique.')


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value,
                      sort_keys=True,
                      separators=(',', ':'),
                      ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def _normalized_section(section: dict[str, Any]) -> dict[str, Any]:
    # JSON round-trip makes the result independent of caller-owned mutable
    # objects. Lists whose order is not semantic are sorted before hashing.
    normalized = json.loads(_canonical_bytes(section))
    normalized['contexts'].sort(key=lambda item: item['kubernetes_context'])
    for context in normalized['contexts']:
        queues = context.get('queues')
        if isinstance(queues, dict):
            queues['inference_gpu_quotas'].sort(key=lambda item: item['flavor'])
            queues['research_gpu_quotas'].sort(key=lambda item: item['flavor'])
        accelerators = context.get('accelerators')
        if isinstance(accelerators, dict):
            for contract in accelerators.values():
                if not isinstance(contract, dict):
                    continue
                pairs = sorted(
                    zip(contract.get('flavors', ()),
                        contract.get('product_label_values', ())))
                contract['flavors'] = [flavor for flavor, _ in pairs]
                contract['product_label_values'] = [
                    product for _, product in pairs
                ]
        flavors = context.get('resource_flavors')
        if isinstance(flavors, list):
            flavors.sort(key=lambda item: item['name'])
        node_inventory = context.get('node_inventory')
        if isinstance(node_inventory, list):
            node_inventory.sort(key=lambda item: item['flavor'])
    return normalized


@dataclasses.dataclass(frozen=True)
class FleetBundle:
    """Validated bundle plus its two domain-separated identities."""

    fleet: dict[str, Any]
    provider_inventory: dict[str, Any]
    fleet_bundle_sha256: str
    provider_inventory_sha256: str

    @property
    def policy_revision(self) -> str:
        return f'boltz-reserved-fill-reclaim-policy/{__version__}'

    @property
    def contexts(self) -> tuple[str, ...]:
        return tuple(
            context['kubernetes_context'] for context in self.fleet['contexts'])

    def fleet_context(self, context_name: str) -> dict[str, Any]:
        for context in self.fleet['contexts']:
            if context['kubernetes_context'] == context_name:
                return context
        raise BundleValidationError(
            'The Kubernetes context is not allowlisted.')

    def provider_context(self, context_name: str) -> dict[str, Any]:
        for context in self.provider_inventory['contexts']:
            if context['kubernetes_context'] == context_name:
                return context
        raise BundleValidationError('The provider context is not allowlisted.')


def parse_bundle_bytes(encoded: bytes) -> FleetBundle:
    """Parse one strict bundle and compute its semantic identities."""
    if not isinstance(encoded,
                      bytes) or not encoded or len(encoded) > _MAX_BUNDLE_BYTES:
        raise BundleValidationError('Fleet bundle size is invalid.')
    try:
        document = json.loads(encoded.decode('utf-8'),
                              object_pairs_hook=_object_without_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BundleValidationError('Fleet bundle is not strict UTF-8 JSON.') \
            from error
    root = _mapping(document, 'bundle')
    _exact_keys(root, {'schema_version', 'fleet', 'provider_inventory'},
                'bundle')
    if root['schema_version'] != 1:
        raise BundleValidationError(
            'Fleet bundle schema version is unsupported.')
    fleet = _mapping(root['fleet'], 'bundle.fleet')
    provider = _mapping(root['provider_inventory'], 'bundle.provider_inventory')
    _exact_keys(fleet, {'contexts'}, 'bundle.fleet')
    _exact_keys(provider, {'contexts'}, 'bundle.provider_inventory')
    fleet_contexts = _list(fleet['contexts'], 'bundle.fleet.contexts')
    provider_contexts = _list(provider['contexts'],
                              'bundle.provider_inventory.contexts')
    if not fleet_contexts or not provider_contexts:
        raise BundleValidationError('Fleet contexts must not be empty.')
    for index, context in enumerate(fleet_contexts):
        _validate_fleet_context(context, f'bundle.fleet.contexts[{index}]')
    for index, context in enumerate(provider_contexts):
        _validate_provider_context(
            context, f'bundle.provider_inventory.contexts[{index}]')

    fleet_names = [item['kubernetes_context'] for item in fleet_contexts]
    provider_names = [item['kubernetes_context'] for item in provider_contexts]
    if (len(fleet_names) != len(set(fleet_names)) or
            len(provider_names) != len(set(provider_names)) or
            set(fleet_names) != set(provider_names)):
        raise BundleValidationError(
            'Fleet and provider context inventories must agree exactly.')
    physical_uids = [item['physical_cluster_uid'] for item in fleet_contexts]
    if len(physical_uids) != len(set(physical_uids)):
        raise BundleValidationError(
            'Each physical cluster must have one canonical context.')
    provider_by_name = {
        item['kubernetes_context']: item for item in provider_contexts
    }
    for fleet_context in fleet_contexts:
        path = f"context {fleet_context['kubernetes_context']}"
        provider_context = provider_by_name[fleet_context['kubernetes_context']]
        flavor_labels = {
            item['name']: item['node_labels']
            for item in provider_context['resource_flavors']
        }
        node_inventory = {
            item['flavor']: item for item in provider_context['node_inventory']
        }
        queue_flavors = {
            item['flavor']
            for item in fleet_context['queues']['inference_gpu_quotas']
        }
        if (queue_flavors != set(flavor_labels) or
                queue_flavors != set(node_inventory)):
            raise BundleValidationError(
                f'{path} queue and provider flavor inventories disagree.')
        accelerator_flavors = {
            flavor for contract in fleet_context['accelerators'].values()
            for flavor in contract['flavors']
        }
        if accelerator_flavors != queue_flavors:
            raise BundleValidationError(
                f'{path} accelerator and queue flavor inventories disagree.')
        for accelerator, contract in fleet_context['accelerators'].items():
            for flavor, product in zip(contract['flavors'],
                                       contract['product_label_values']):
                labels = flavor_labels.get(flavor)
                node = node_inventory.get(flavor)
                if (labels is None or node is None or
                        labels.get(node['selector_label_key'])
                        != node['selector_label_value'] or
                        node['product_label_key']
                        != contract['product_label_key'] or
                        node['product_label_value'] != product or
                        node['resource_name'] != contract['resource_name']):
                    raise BundleValidationError(
                        f'{path} accelerator {accelerator!r} does not bind '
                        'its reviewed flavor, Node selector, and product '
                        'label.')
    normalized_fleet = _normalized_section(fleet)
    normalized_provider = _normalized_section(provider)
    fleet_sha = hashlib.sha256(_FLEET_HASH_DOMAIN +
                               _canonical_bytes(normalized_fleet)).hexdigest()
    provider_sha = hashlib.sha256(
        _PROVIDER_HASH_DOMAIN +
        _canonical_bytes(normalized_provider)).hexdigest()
    return FleetBundle(fleet=normalized_fleet,
                       provider_inventory=normalized_provider,
                       fleet_bundle_sha256=fleet_sha,
                       provider_inventory_sha256=provider_sha)


def load_embedded_bundle() -> FleetBundle:
    """Load the only code-owned production fleet bundle."""
    encoded = resources.files(__package__).joinpath(
        _BUNDLE_RESOURCE).read_bytes()
    return parse_bundle_bytes(encoded)
