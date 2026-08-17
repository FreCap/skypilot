"""Strict immutable fleet-bundle parsing for the Boltz reclaim policy."""

from collections.abc import Mapping
import dataclasses
import decimal
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
_FLEET_HASH_DOMAIN: Final = b'boltz-reserved-fill/fleet/v3\x00'
_PROVIDER_HASH_DOMAIN: Final = b'boltz-reserved-fill/provider/v2\x00'
_REQUIRED_QUEUE_RESOURCES: Final = frozenset(
    {'cpu', 'memory', 'nvidia.com/gpu'})
_RESOURCE_GROUP_KEYS: Final = frozenset({'covered_resources', 'flavors'})
_RESOURCE_FLAVOR_KEYS: Final = frozenset({'name', 'resources'})
_RESOURCE_QUOTA_KEYS: Final = frozenset(
    {'resource_name', 'nominal_quota', 'borrowing_limit'})
_KUEUE_MANAGED_LABEL_KEY: Final = 'boltz.bio/kueue-managed'
_KUEUE_MANAGED_LABEL_VALUE: Final = 'true'
_QUANTITY_RE: Final = re.compile(
    r'^(?P<number>[0-9]+(?:\.[0-9]+)?)'
    r'(?P<suffix>n|u|m|k|K|M|G|T|P|E|Ki|Mi|Gi|Ti|Pi|Ei)?$')
_DECIMAL_QUANTITY_SCALE: Final = {
    '': decimal.Decimal(1),
    'n': decimal.Decimal('1e-9'),
    'u': decimal.Decimal('1e-6'),
    'm': decimal.Decimal('1e-3'),
    'k': decimal.Decimal('1e3'),
    'K': decimal.Decimal('1e3'),
    'M': decimal.Decimal('1e6'),
    'G': decimal.Decimal('1e9'),
    'T': decimal.Decimal('1e12'),
    'P': decimal.Decimal('1e15'),
    'E': decimal.Decimal('1e18'),
    'Ki': decimal.Decimal(2)**10,
    'Mi': decimal.Decimal(2)**20,
    'Gi': decimal.Decimal(2)**30,
    'Ti': decimal.Decimal(2)**40,
    'Pi': decimal.Decimal(2)**50,
    'Ei': decimal.Decimal(2)**60,
}
_WORKER_RESOURCE_PER_GPU: Final = {
    'cpu': decimal.Decimal(4),
    'memory': decimal.Decimal(16) * (decimal.Decimal(2)**30),
    'nvidia.com/gpu': decimal.Decimal(1),
}


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


def _quantity(value: object, path: str) -> decimal.Decimal:
    quantity = _text(value, path)
    match = _QUANTITY_RE.fullmatch(quantity)
    if match is None:
        raise BundleValidationError(
            f'{path} must be a nonnegative Kubernetes quantity.')
    return (decimal.Decimal(match.group('number')) *
            _DECIMAL_QUANTITY_SCALE[match.group('suffix') or ''])


def _validate_resource_groups(
    value: object, path: str
) -> dict[tuple[str, str], tuple[decimal.Decimal, decimal.Decimal]]:
    """Validate the complete Kueue quota topology and index its atoms."""
    groups = _list(value, path)
    if not groups:
        raise BundleValidationError(f'{path} must not be empty.')
    covered_inventory: set[str] = set()
    group_signatures: set[frozenset[str]] = set()
    quota_inventory: dict[tuple[str, str], tuple[decimal.Decimal,
                                                 decimal.Decimal]] = {}
    for group_index, raw_group in enumerate(groups):
        group_path = f'{path}[{group_index}]'
        group = _mapping(raw_group, group_path)
        _exact_keys(group, set(_RESOURCE_GROUP_KEYS), group_path)
        covered_resources = [
            _text(item, f'{group_path}.covered_resources') for item in _list(
                group['covered_resources'], f'{group_path}.covered_resources')
        ]
        if (not covered_resources or
                len(covered_resources) != len(set(covered_resources))):
            raise BundleValidationError(
                f'{group_path}.covered_resources must be nonempty and '
                'unique.')
        signature = frozenset(covered_resources)
        if (signature in group_signatures or
                covered_inventory.intersection(signature)):
            raise BundleValidationError(
                f'{path} has duplicate or overlapping resource groups.')
        group_signatures.add(signature)
        covered_inventory.update(signature)
        if ('nvidia.com/gpu' in signature and
                not _REQUIRED_QUEUE_RESOURCES.issubset(signature)):
            raise BundleValidationError(
                f'{group_path} must co-locate GPU, CPU, and memory quotas.')

        flavors = _list(group['flavors'], f'{group_path}.flavors')
        if not flavors:
            raise BundleValidationError(
                f'{group_path}.flavors must not be empty.')
        flavor_names: set[str] = set()
        for flavor_index, raw_flavor in enumerate(flavors):
            flavor_path = f'{group_path}.flavors[{flavor_index}]'
            flavor = _mapping(raw_flavor, flavor_path)
            _exact_keys(flavor, set(_RESOURCE_FLAVOR_KEYS), flavor_path)
            flavor_name = _text(flavor['name'], f'{flavor_path}.name')
            if flavor_name in flavor_names:
                raise BundleValidationError(
                    f'{group_path} has duplicate flavors.')
            flavor_names.add(flavor_name)
            quota_rows = _list(flavor['resources'], f'{flavor_path}.resources')
            resource_names: set[str] = set()
            for resource_index, raw_resource in enumerate(quota_rows):
                resource_path = (f'{flavor_path}.resources[{resource_index}]')
                resource = _mapping(raw_resource, resource_path)
                _exact_keys(resource, set(_RESOURCE_QUOTA_KEYS), resource_path)
                resource_name = _text(resource['resource_name'],
                                      f'{resource_path}.resource_name')
                atom = (flavor_name, resource_name)
                if (resource_name in resource_names or atom in quota_inventory):
                    raise BundleValidationError(
                        f'{path} has duplicate quota atoms.')
                resource_names.add(resource_name)
                quota_inventory[atom] = (
                    _quantity(resource['nominal_quota'],
                              f'{resource_path}.nominal_quota'),
                    _quantity(resource['borrowing_limit'],
                              f'{resource_path}.borrowing_limit'))
            if resource_names != signature:
                raise BundleValidationError(
                    f'{flavor_path} must have exactly one quota atom for '
                    'every covered resource.')
    return quota_inventory


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
            'accelerators', 'kubernetes_context', 'kueue_admission',
            'namespace', 'physical_cluster_uid', 'pod_identity_role_arn',
            'priority_class', 'scheduler_name', 'service_account_name'
        }, path)
    for key in ('kubernetes_context', 'namespace', 'scheduler_name',
                'service_account_name'):
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
    claimed_flavors: dict[str, str] = {}
    claimed_products: dict[tuple[str, str, str], str] = {}
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
        if len(contract['product_label_values']) != len(
                set(contract['product_label_values'])):
            raise BundleValidationError(
                f'{path}.accelerators.{accelerator}.product_label_values '
                'must be unique.')
        if len(contract['flavors']) != len(contract['product_label_values']):
            raise BundleValidationError(
                f'{path}.accelerators.{accelerator} must bind each flavor '
                'to one observed product value.')
        for flavor, product in zip(contract['flavors'],
                                   contract['product_label_values']):
            previous = claimed_flavors.get(flavor)
            scheduling_atom = (contract['product_label_key'], product,
                               contract['resource_name'])
            previous_product = claimed_products.get(scheduling_atom)
            if previous is not None or previous_product is not None:
                owner = previous or previous_product
                raise BundleValidationError(
                    f'{path}.accelerators.{accelerator} overlaps the exact '
                    f'card contract owned by {owner}.')
            claimed_flavors[flavor] = accelerator
            claimed_products[scheduling_atom] = accelerator

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

    raw_admission = context['kueue_admission']
    if raw_admission is None:
        return
    admission = _mapping(raw_admission, f'{path}.kueue_admission')
    _exact_keys(
        admission, {
            'local_queue_name', 'queues', 'workload_priority_class_name',
            'workload_priority_value'
        }, f'{path}.kueue_admission')
    _text(admission['local_queue_name'],
          f'{path}.kueue_admission.local_queue_name')
    _text(admission['workload_priority_class_name'],
          f'{path}.kueue_admission.workload_priority_class_name')
    _integer(admission['workload_priority_value'],
             f'{path}.kueue_admission.workload_priority_value',
             minimum=-2147483648)
    if admission['workload_priority_value'] > 1000000000:
        raise BundleValidationError(
            f'{path}.kueue_admission.workload_priority_value exceeds '
            'Kubernetes limits.')

    queues = _mapping(admission['queues'], f'{path}.kueue_admission.queues')
    _exact_keys(
        queues, {
            'cohort', 'inference_cluster_queue', 'inference_resource_groups',
            'inference_preemption', 'research_cluster_queue',
            'research_resource_groups', 'research_namespace',
            'research_preemption'
        }, f'{path}.kueue_admission.queues')
    for key in ('cohort', 'inference_cluster_queue', 'research_cluster_queue',
                'research_namespace'):
        _text(queues[key], f'{path}.kueue_admission.queues.{key}')
    inference_groups = _list(
        queues['inference_resource_groups'], f'{path}.kueue_admission.queues.'
        'inference_resource_groups')
    inference_quotas = _validate_resource_groups(
        inference_groups,
        f'{path}.kueue_admission.queues.inference_resource_groups')
    research_quotas = _validate_resource_groups(
        queues['research_resource_groups'],
        f'{path}.kueue_admission.queues.research_resource_groups')
    _validate_preemption(queues['inference_preemption'],
                         f'{path}.kueue_admission.queues.inference_preemption')
    _validate_preemption(queues['research_preemption'],
                         f'{path}.kueue_admission.queues.research_preemption')
    positive_inference_gpu_flavors = {
        flavor for (flavor,
                    resource_name), (_, borrowing) in inference_quotas.items()
        if resource_name == 'nvidia.com/gpu' and borrowing > 0
    }
    if positive_inference_gpu_flavors != set(claimed_flavors):
        raise BundleValidationError(
            f'{path}.kueue_admission.queues positive borrowed GPU flavors '
            'must exactly cover the accelerator contracts.')
    if any(nominal != 0 for nominal, _ in inference_quotas.values()):
        raise BundleValidationError(
            f'{path}.kueue_admission.queues inference quotas must all be '
            'zero-nominal.')
    for flavor in positive_inference_gpu_flavors:
        gpu_borrowing = inference_quotas[(flavor, 'nvidia.com/gpu')][1]
        if (gpu_borrowing <= 0 or
                gpu_borrowing != gpu_borrowing.to_integral_value()):
            raise BundleValidationError(
                f'{path}.kueue_admission.queues must expose positive '
                'integral borrowed GPU capacity.')
        for resource_name in _REQUIRED_QUEUE_RESOURCES:
            atom = (flavor, resource_name)
            inference_quota = inference_quotas.get(atom)
            research_quota = research_quotas.get(atom)
            if inference_quota is None or research_quota is None:
                raise BundleValidationError(
                    f'{path}.kueue_admission.queues must co-locate GPU, CPU, '
                    'and memory quota atoms for every accelerator flavor.')
            inference_nominal, inference_borrowing = inference_quota
            research_nominal, research_borrowing = research_quota
            if (inference_nominal != 0 or inference_borrowing
                    > research_nominal + research_borrowing):
                raise BundleValidationError(
                    f'{path}.kueue_admission.queues borrowed inference '
                    'capacity must be bounded by the paired research queue.')
            minimum_borrowing = (gpu_borrowing *
                                 _WORKER_RESOURCE_PER_GPU[resource_name])
            if inference_borrowing < minimum_borrowing:
                raise BundleValidationError(
                    f'{path}.kueue_admission.queues {resource_name} borrowing '
                    'quota cannot fit the reviewed workers.')
    if queues['research_preemption']['reclaim_within_cohort'] != 'Any':
        raise BundleValidationError(
            f'{path}.kueue_admission.queues does not make inference a '
            'reclaimable borrower.')


def _validate_deployment_contract(value: object, path: str, *,
                                  controller: bool) -> None:
    deployment = _mapping(value, path)
    expected = {'deployment', 'namespace', 'replicas', 'images'}
    if controller:
        expected.add('config_map')
    else:
        expected.remove('images')
        expected.add('containers')
    _exact_keys(deployment, expected, path)
    for key in ('deployment', 'namespace'):
        _text(deployment[key], f'{path}.{key}')
    if controller:
        _text(deployment['config_map'], f'{path}.config_map')
        images = _mapping(deployment['images'], f'{path}.images')
    else:
        images = _mapping(deployment['containers'], f'{path}.containers')
    _integer(deployment['replicas'], f'{path}.replicas')
    if not images:
        raise BundleValidationError(f'{path} image set must not be empty.')
    for name, image in images.items():
        _text(name, f'{path} image name')
        image_text = _text(image, f'{path}.{name}')
        if not controller and ('@' not in image_text or _SHA256_RE.search(
                image_text.split('@', 1)[-1]) is None):
            raise BundleValidationError(
                f'{path}.{name} must use an immutable digest.')


def _validate_kueue_enforcement(value: object, path: str) -> None:
    enforcement = _mapping(value, path)
    _exact_keys(enforcement, {'admission_policy', 'controller', 'webhooks'},
                path)
    _validate_deployment_contract(enforcement['controller'],
                                  f'{path}.controller',
                                  controller=True)
    admission = _mapping(enforcement['admission_policy'],
                         f'{path}.admission_policy')
    _exact_keys(admission, {
        'binding_name', 'name', 'namespace_label_key', 'namespace_label_value'
    }, f'{path}.admission_policy')
    for key, item in admission.items():
        _text(item, f'{path}.admission_policy.{key}')
    if (admission['namespace_label_key'] != _KUEUE_MANAGED_LABEL_KEY or
            admission['namespace_label_value'] != _KUEUE_MANAGED_LABEL_VALUE):
        raise BundleValidationError(
            f'{path}.admission_policy must use the code-owned managed '
            'namespace label.')
    webhooks = _mapping(enforcement['webhooks'], f'{path}.webhooks')
    _exact_keys(webhooks,
                {'mutating', 'service_name', 'service_port', 'validating'},
                f'{path}.webhooks')
    _text(webhooks['service_name'], f'{path}.webhooks.service_name')
    service_port = _integer(webhooks['service_port'],
                            f'{path}.webhooks.service_port')
    if service_port < 1 or service_port > 65535:
        raise BundleValidationError(f'{path}.webhooks.service_port is invalid.')
    for kind, required_operations in (('mutating', ('CREATE',)),
                                      ('validating', ('CREATE', 'UPDATE'))):
        contract = _mapping(webhooks[kind], f'{path}.webhooks.{kind}')
        _exact_keys(
            contract,
            {'configuration_name', 'operations', 'path', 'webhook_name'},
            f'{path}.webhooks.{kind}')
        for key in ('configuration_name', 'path', 'webhook_name'):
            _text(contract[key], f'{path}.webhooks.{kind}.{key}')
        operations = tuple(
            _text(item, f'{path}.webhooks.{kind}.operations') for item in _list(
                contract['operations'], f'{path}.webhooks.{kind}.operations'))
        if operations != required_operations:
            raise BundleValidationError(
                f'{path}.webhooks.{kind} does not intercept the exact '
                'reviewed Pod operations.')


def _validate_provider_context(value: object, path: str) -> None:
    context = _mapping(value, path)
    _exact_keys(
        context, {
            'eks', 'kubernetes_context', 'kueue_enforcement', 'namespace_uid',
            'node_inventory', 'resource_flavors', 'scheduler'
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

    _validate_deployment_contract(context['scheduler'],
                                  f'{path}.scheduler',
                                  controller=False)
    enforcement = context['kueue_enforcement']
    if enforcement is not None:
        _validate_kueue_enforcement(enforcement, f'{path}.kueue_enforcement')

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
        admission = context.get('kueue_admission')
        queues = (admission.get('queues')
                  if isinstance(admission, dict) else None)
        if isinstance(queues, dict):
            for key in ('inference_resource_groups',
                        'research_resource_groups'):
                groups = queues.get(key)
                if not isinstance(groups, list):
                    continue
                for group in groups:
                    group['covered_resources'].sort()
                    group['flavors'].sort(key=lambda item: item['name'])
                    for flavor in group['flavors']:
                        flavor['resources'].sort(
                            key=lambda item: item['resource_name'])
                groups.sort(key=lambda item: tuple(item['covered_resources']))
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
    if root['schema_version'] != 3:
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
        if (fleet_context['scheduler_name']
                != provider_context['scheduler']['deployment']):
            raise BundleValidationError(
                f'{path} projected scheduler and provider deployment '
                'disagree.')
        flavor_labels = {
            item['name']: item['node_labels']
            for item in provider_context['resource_flavors']
        }
        node_inventory = {
            item['flavor']: item for item in provider_context['node_inventory']
        }
        accelerator_flavors = {
            flavor for contract in fleet_context['accelerators'].values()
            for flavor in contract['flavors']
        }
        if (accelerator_flavors != set(flavor_labels) or
                accelerator_flavors != set(node_inventory)):
            raise BundleValidationError(
                f'{path} accelerator and provider flavor inventories '
                'disagree.')
        managed = fleet_context['kueue_admission'] is not None
        enforced = provider_context['kueue_enforcement'] is not None
        if managed != enforced:
            raise BundleValidationError(
                f'{path} Kueue admission and enforcement must be both null '
                'or both configured.')
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
