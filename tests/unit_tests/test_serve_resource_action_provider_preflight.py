"""Static P2a authority evidence, projector, and inventory tests."""

# pylint: disable=protected-access

import copy
import dataclasses
import hashlib
import json
import pathlib

import pytest
import serve_resource_action_test_fixtures as authority_fixtures
import test_serve_resource_action_down_execution_config as down_fixtures
import test_serve_resource_action_launch_execution_config as launch_fixtures

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_provider_preflight as preflight
from sky.serve import resource_actions as actions
from sky.server.requests import registry as request_registry
from sky.server.requests import resource_actions as kernel_actions

_ARTIFACT_ROOT = pathlib.Path(
    'sky/serve/resource_action_artifacts/provider_authority_v1')
_RENDERER_INVENTORY = _ARTIFACT_ROOT / 'renderer_artifact_inventory.json'
_CALLABLE_INVENTORY = _ARTIFACT_ROOT / 'callable_inventory.json'


def _canonical_file(value: object) -> bytes:
    return actions.canonical_json_bytes(value) + b'\n'


def _artifact_reference(path: pathlib.Path) -> dict:
    contents = path.read_bytes()
    return {
        'repo_path': path.as_posix(),
        'byte_size': len(contents),
        'sha256': hashlib.sha256(contents).hexdigest(),
    }


def _launch_request() -> actions.ProviderAuthorityPreflightRequestV1:
    config = launch_fixtures._config()
    subject = config.policy_subject
    capsule = config.capsule
    resource_identity = (subject.source.identity_canonicalization.context.input.
                         resource_identity)
    kubernetes = subject.requested_target.kubernetes
    assert kubernetes is not None
    seed = actions.ProviderLaunchPreflightSeedV1(
        version=1,
        resource_identity=resource_identity,
        workspace=subject.source.content.workspace,
        source=subject.source,
        requested_target=subject.requested_target,
        requested_cloud='kubernetes',
        context_mode='in_cluster',
        target_namespace=kubernetes.namespace,
        resources=subject.resources,
        topology=subject.topology,
        replica_id=resource_identity.replica_id,
        retry_until_up=subject.retry_until_up,
        request_identity=capsule.request_identity,
        config_projection=capsule.config_projection)
    manifest = actions.ProviderAuthorityWorkerCohortManifestV1.from_value(
        authority_fixtures.authority_manifest_value())
    return actions.ProviderAuthorityPreflightRequestV1.create(
        action_kind=kernel_actions.ActionKind.LAUNCH,
        nonce='12345678-1234-4234-8234-123456789abc',
        seed=seed,
        expected_cohort_manifest=manifest)


def _down_request() -> actions.ProviderAuthorityPreflightRequestV1:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        down_fixtures.down_invocation_payload())
    down = invocation.require_down()
    capsule = down.execution_config.capsule
    seed = actions.ProviderDownPreflightSeedV1(
        version=1,
        resource_identity=invocation.resource_identity,
        workspace=down.workspace,
        requested_target=invocation.requested_target,
        prior_launch_basis=down.prior_launch_basis,
        prior_launch_basis_sha256=down.prior_launch_basis.sha256,
        cleanup_target=capsule.cleanup_target,
        cleanup_target_sha256=capsule.cleanup_target.sha256,
        context_mode='in_cluster',
        config_projection=capsule.config_projection)
    manifest = actions.ProviderAuthorityWorkerCohortManifestV1.from_value(
        authority_fixtures.authority_manifest_value())
    return actions.ProviderAuthorityPreflightRequestV1.create(
        action_kind=kernel_actions.ActionKind.DOWN,
        nonce='87654321-4321-4321-8321-cba987654321',
        seed=seed,
        expected_cohort_manifest=manifest)


def test_pod_template_projector_is_deterministic_and_explicit() -> None:
    release_inputs = (
        actions.ProviderAuthorityWorkerPodTemplateReleaseInputsV1.from_value(
            authority_fixtures.authority_release_inputs_value()))
    first = preflight.project_provider_authority_worker_pod_template_v1(
        release_inputs)
    second = preflight.project_provider_authority_worker_pod_template_v1(
        release_inputs)
    assert first.canonical_bytes == second.canonical_bytes
    pod_spec = first.canonical_value()['spec']
    container = pod_spec['containers'][0]
    assert pod_spec['schedulerName'] == 'default-scheduler'
    assert pod_spec['automountServiceAccountToken'] is False
    assert pod_spec['serviceAccount'] == release_inputs.service_account_name
    assert pod_spec['serviceAccountName'] == release_inputs.service_account_name
    assert pod_spec['priority'] == 0
    assert pod_spec['preemptionPolicy'] == 'PreemptLowerPriority'
    assert container['terminationMessagePath'] == '/dev/termination-log'
    assert container['terminationMessagePolicy'] == 'File'
    assert all(container[name]['successThreshold'] == 1
               for name in ('startupProbe', 'livenessProbe', 'readinessProbe'))
    api_mount, = [
        item for item in container['volumeMounts']
        if item['name'] == 'kube-api-access'
    ]
    assert api_mount == {
        'name': 'kube-api-access',
        'mountPath': '/var/run/secrets/kubernetes.io/serviceaccount',
        'readOnly': True,
    }
    api_volume, = [
        item for item in pod_spec['volumes']
        if item['name'] == 'kube-api-access'
    ]
    assert api_volume['projected']['sources'] == [{
        'serviceAccountToken': {
            'expirationSeconds': 3607,
            'path': 'token',
        }
    }, {
        'configMap': {
            'name': 'kube-root-ca.crt',
            'items': [{
                'key': 'ca.crt',
                'path': 'ca.crt',
            }],
        }
    }, {
        'downwardAPI': {
            'items': [{
                'path': 'namespace',
                'fieldRef': {
                    'apiVersion': 'v1',
                    'fieldPath': 'metadata.namespace',
                },
            }],
        }
    }]
    assert pod_spec['tolerations'][-2:] == [{
        'effect': 'NoExecute',
        'key': 'node.kubernetes.io/not-ready',
        'operator': 'Exists',
        'tolerationSeconds': 300,
    }, {
        'effect': 'NoExecute',
        'key': 'node.kubernetes.io/unreachable',
        'operator': 'Exists',
        'tolerationSeconds': 300,
    }]
    materialized = (
        preflight.materialize_provider_authority_worker_pod_template_v1(
            release_inputs, 'a' * 64))
    assert materialized.canonical_value()['metadata']['annotations'][
        'skypilot.co/resource-action-manifest-sha256'] == 'a' * 64
    assert '$MANIFEST_SHA256' not in materialized.canonical_bytes.decode()


@pytest.mark.parametrize('key,effect', [
    ('node.kubernetes.io/not-ready', None),
    ('node.kubernetes.io/not-ready', ''),
    ('node.kubernetes.io/not-ready', 'NoExecute'),
    ('node.kubernetes.io/unreachable', 'NoExecute'),
])
def test_release_inputs_reject_configured_default_toleration_collision(
        key: str, effect: str | None) -> None:
    value = authority_fixtures.authority_release_inputs_value()
    toleration = {
        'key': key,
        'operator': 'Exists',
        'tolerationSeconds': 300,
    }
    if effect is not None:
        toleration['effect'] = effect
    value['tolerations'] = [toleration]
    with pytest.raises(ValueError):
        actions.ProviderAuthorityWorkerPodTemplateReleaseInputsV1.from_value(
            value)


def test_initial_evaluator_is_unavailable_before_acceptance_and_typed_after(
) -> None:
    request = _launch_request()
    accepted: list[actions.ProviderAuthorityWorkerCohortManifestV1] = []
    evaluator = preflight.InitialProviderPreflightEvaluator(
        lambda: accepted[0] if accepted else None)
    assert evaluator(request) is None
    accepted.append(request.expected_cohort_manifest)
    response = evaluator(request)
    assert type(response) is actions.ProviderLaunchAuthorityPreflightResponseV1
    assert response.reason is (actions.ProviderLaunchNotRepresentableReasonV1.
                               PREFLIGHT_UNAVAILABLE_OR_INVALID)
    response.validate_request(request)


def test_down_preflight_envelope_is_closed_nonce_bound_and_kind_typed() -> None:
    request = _down_request()
    assert (actions.ProviderAuthorityPreflightRequestV1.from_value(
        request.canonical_value()).canonical_bytes == request.canonical_bytes)
    response = actions.ProviderDownAuthorityPreflightResponseV1.unavailable(
        request)
    parsed = actions.provider_authority_preflight_response_from_value_v1(
        response.canonical_value())
    assert type(parsed) is actions.ProviderDownAuthorityPreflightResponseV1
    parsed.validate_request(request)

    crossed = copy.deepcopy(response.canonical_value())
    crossed['nonce'] = '12345678-1234-4234-8234-123456789abc'
    with pytest.raises(ValueError, match='request envelope'):
        actions.ProviderDownAuthorityPreflightResponseV1.from_value(
            crossed).validate_request(request)

    wrong_kind = copy.deepcopy(request.canonical_value())
    wrong_kind['action_kind'] = 'launch'
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderAuthorityPreflightRequestV1.from_value(wrong_kind)

    extra = copy.deepcopy(response.canonical_value())
    extra['extra'] = None
    with pytest.raises((TypeError, ValueError), match='unknown or missing'):
        actions.provider_authority_preflight_response_from_value_v1(extra)


def test_checked_in_inventories_are_canonical_and_match_runtime() -> None:
    renderer_contents = _RENDERER_INVENTORY.read_bytes()
    callable_contents = _CALLABLE_INVENTORY.read_bytes()
    renderer_refs = (
        preflight.validate_provider_authority_renderer_artifact_inventory_v1(
            renderer_contents))
    preflight.validate_provider_authority_callable_inventory_v1(
        callable_contents)
    assert len(renderer_refs) == 5
    assert (preflight.project_provider_authority_worker_callable_inventory_v1().
            canonical_bytes == callable_contents[:-1])


@pytest.mark.parametrize('mutation', [
    lambda value: value.update({'version': 2}),
    lambda value: value.update({'contract': 'crossed'}),
    lambda value: value['artifacts'].pop(),
    lambda value: value['artifacts'].append(copy.deepcopy(value['artifacts'][0])
                                           ),
    lambda value: value['artifacts'].__setitem__(
        slice(0, 2), list(reversed(value['artifacts'][:2]))),
    lambda value: value['artifacts'][0].update({'role': 'node_fragment'}),
    lambda value: value['artifacts'][0].update({
        'repo_path':
            'sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
            'node_fragment.json'
    }),
    lambda value: value['artifacts'][0].update({'byte_size': 971}),
    lambda value: value['artifacts'][0].update({'sha256': '0' * 64}),
    lambda value: value['artifacts'][0].update({'extra': None}),
])
def test_renderer_inventory_rejects_every_role_binding_mutation(
        mutation) -> None:
    value = json.loads(_RENDERER_INVENTORY.read_bytes())
    mutation(value)
    with pytest.raises(preflight.ProviderAuthorityStaticEvidenceError):
        preflight.validate_provider_authority_renderer_artifact_inventory_v1(
            _canonical_file(value))


@pytest.mark.parametrize('variant', [
    lambda body: body[:-1],
    lambda body: body + b'\n',
    lambda body: body[:-1] + b'\r\n',
    lambda body: json.dumps(json.loads(body), indent=2).encode() + b'\n',
    lambda body: body.replace(b'{"artifacts":', b'{"version":1,"artifacts":', 1
                             ),
])
def test_renderer_inventory_rejects_noncanonical_bytes(variant) -> None:
    with pytest.raises(preflight.ProviderAuthorityStaticEvidenceError):
        preflight.validate_provider_authority_renderer_artifact_inventory_v1(
            variant(_RENDERER_INVENTORY.read_bytes()))


def test_renderer_leaf_byte_change_fails_with_hash_valid_inventory(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    value = json.loads(_RENDERER_INVENTORY.read_bytes())
    for row in value['artifacts']:
        destination = tmp_path / row['repo_path']
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(pathlib.Path(row['repo_path']).read_bytes())
    changed = tmp_path / value['artifacts'][0]['repo_path']
    contents = bytearray(changed.read_bytes())
    contents[0] ^= 1
    changed.write_bytes(contents)
    monkeypatch.setattr(preflight, '_distribution_root', lambda: str(tmp_path))
    with pytest.raises(preflight.ProviderAuthorityStaticEvidenceError,
                       match='hash differs'):
        preflight.validate_provider_authority_renderer_artifact_inventory_v1(
            _RENDERER_INVENTORY.read_bytes())


def _mutate_callable_field(value: dict, field: str) -> None:
    row = value['handlers'][0]
    mutations = {
        'name': lambda: row.update({'name': 'serve_resource_action_down'}),
        'module': lambda: row.update({'module': 'crossed.module'}),
        'qualname': lambda: row.update({'qualname': 'crossed'}),
        'execution_class': lambda: row.update({'execution_class': 'controller'}
                                             ),
        'claim_scope': lambda: row.update({'claim_scope': 'general'}),
        'replay_policy': lambda: row.update({'replay_policy': 'read_only'}),
        'cancellation_policy': lambda: row.update(
            {'cancellation_policy': 'cooperative'}),
        'aliases': lambda: row.update({'aliases': ['old-name']}),
        'encoder.mode': lambda: row['result_codec']['encoder'].update({
            'mode': ('default' if row['result_codec']['encoder']['mode'] ==
                     'registered' else 'registered')
        }),
        'encoder.module': lambda: row['result_codec']['encoder'].update(
            {'module': 'crossed.encoder'}),
        'encoder.qualname': lambda: row['result_codec']['encoder'].update(
            {'qualname': 'crossed'}),
        'decoder.mode': lambda: row['result_codec']['decoder'].update({
            'mode': ('default' if row['result_codec']['decoder']['mode'] ==
                     'registered' else 'registered')
        }),
        'decoder.module': lambda: row['result_codec']['decoder'].update(
            {'module': 'crossed.decoder'}),
        'decoder.qualname': lambda: row['result_codec']['decoder'].update(
            {'qualname': 'crossed'}),
        'strict_return_value': lambda: row['result_codec'].update({
            'strict_return_value': not row['result_codec']['strict_return_value'
                                                          ]
        }),
    }
    mutations[field]()


@pytest.mark.parametrize('field', [
    'name', 'module', 'qualname', 'execution_class', 'claim_scope',
    'replay_policy', 'cancellation_policy', 'aliases', 'encoder.mode',
    'encoder.module', 'encoder.qualname', 'decoder.mode', 'decoder.module',
    'decoder.qualname', 'strict_return_value'
])
def test_callable_inventory_rejects_every_bound_field(field: str) -> None:
    value = json.loads(_CALLABLE_INVENTORY.read_bytes())
    _mutate_callable_field(value, field)
    with pytest.raises(preflight.ProviderAuthorityStaticEvidenceError):
        preflight.validate_provider_authority_callable_inventory_v1(
            _canonical_file(value))


@pytest.mark.parametrize('mutation', [
    lambda value: value.update({'version': 2}),
    lambda value: value.update({'contract': 'crossed'}),
    lambda value: value['handlers'].pop(),
    lambda value: value['handlers'].append(copy.deepcopy(value['handlers'][0])),
    lambda value: value['handlers'].__setitem__(
        slice(0, 2), list(reversed(value['handlers'][:2]))),
    lambda value: value['handlers'][0].update({'extra': None}),
    lambda value: value['handlers'][0]['result_codec'].update({'extra': None}),
    lambda value: value['handlers'][0]['result_codec']['encoder'].update(
        {'extra': None}),
])
def test_callable_inventory_rejects_closed_shape_mutations(mutation) -> None:
    value = json.loads(_CALLABLE_INVENTORY.read_bytes())
    mutation(value)
    with pytest.raises(preflight.ProviderAuthorityStaticEvidenceError):
        preflight.validate_provider_authority_callable_inventory_v1(
            _canonical_file(value))


def test_callable_inventory_detects_actual_registry_drift(
        monkeypatch: pytest.MonkeyPatch) -> None:
    original = request_registry.resolve_handler

    def _drifted(name: str):
        registration = original(name)
        if name == actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1[0]:
            return dataclasses.replace(registration, aliases=('old-name',))
        return registration

    monkeypatch.setattr(request_registry, 'resolve_handler', _drifted)
    with pytest.raises(preflight.ProviderAuthorityStaticEvidenceError,
                       match='actual runtime registry'):
        preflight.validate_provider_authority_callable_inventory_v1(
            _CALLABLE_INVENTORY.read_bytes())


def test_callable_inventory_rejects_extra_runtime_authority_handler(
        monkeypatch: pytest.MonkeyPatch) -> None:
    registrations = request_registry.registered_handlers()
    authority_registration = next(
        registration for registration in registrations if registration.name ==
        actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1[0])
    extra = dataclasses.replace(authority_registration,
                                name='serve_resource_action_extra')
    monkeypatch.setattr(request_registry, 'registered_handlers', lambda:
                        (*registrations, extra))
    with pytest.raises(preflight.ProviderAuthorityStaticEvidenceError,
                       match='closed four-name'):
        preflight.project_provider_authority_worker_callable_inventory_v1()


@pytest.mark.parametrize('manifest_version', (1, 2))
def test_static_loader_validates_complete_installed_graph(
        monkeypatch: pytest.MonkeyPatch, manifest_version: int) -> None:
    source_path = pathlib.Path(
        'sky/serve/resource_action_provider_preflight.py')
    manifest = authority_fixtures.authority_manifest_value()
    manifest['pod_template_contract'] = _artifact_reference(source_path)
    manifest['pod_template_binding']['projector_artifact_sha256'] = (
        manifest['pod_template_contract']['sha256'])
    manifest['artifact_inventory'] = _artifact_reference(_RENDERER_INVENTORY)
    manifest['callable_inventory'] = _artifact_reference(_CALLABLE_INVENTORY)
    release_inputs = (
        actions.ProviderAuthorityWorkerPodTemplateReleaseInputsV1.from_value(
            manifest['pod_template_binding']['release_inputs']))
    manifest['pod_template_binding']['expected_template_sha256'] = (
        preflight.project_provider_authority_worker_pod_template_v1(
            release_inputs).sha256)
    source_commit = 'a' * 40
    qualification = _canonical_file({
        'version': 1,
        'requested_reference': manifest['image']['requested_reference'],
        'oci_manifest_digest': manifest['image']['oci_manifest_digest'],
        'oci_config_digest': manifest['image']['oci_config_digest'],
        'source_commit': source_commit,
        'platform': 'linux/amd64',
    })
    manifest['image']['qualification_artifact']['byte_size'] = len(
        qualification)
    manifest['image']['qualification_artifact']['sha256'] = hashlib.sha256(
        qualification).hexdigest()
    if manifest_version == 1:
        typed = actions.ProviderAuthorityWorkerCohortManifestV1.from_value(
            manifest)
    else:
        manifest['version'] = 2
        manifest['claim_contract'] = 'frozen_action_cohort_join_v2'
        typed = authority.ProviderAuthorityWorkerCohortManifestV2.from_value(
            manifest)
    fixed_contents = {
        preflight._MANIFEST_PATH: typed.canonical_bytes,
        preflight._QUALIFICATION_PATH: qualification,
    }
    reads: dict[str, int] = {}

    def _read(path: str, *, name: str, maximum_bytes: int) -> bytes:
        del name
        reads[path] = reads.get(path, 0) + 1
        contents = fixed_contents[path]
        assert len(contents) <= maximum_bytes
        return contents

    monkeypatch.setattr(preflight, '_read_fixed_regular_file', _read)
    monkeypatch.setattr(preflight.sky, '__commit__', source_commit)
    loaded = preflight.load_provider_authority_worker_static_evidence()
    assert type(loaded) is type(typed)
    assert loaded.canonical_bytes == typed.canonical_bytes
    assert reads[preflight._MANIFEST_PATH] == 1


@pytest.mark.parametrize('invalid_version', (True, 2.0, 3, '2'))
def test_static_loader_dispatch_rejects_nonexact_manifest_version(
        monkeypatch: pytest.MonkeyPatch, invalid_version: object) -> None:
    value = authority_fixtures.authority_manifest_value()
    value['version'] = invalid_version
    contents = (json.dumps(value, sort_keys=True, separators=(
        ',', ':')).encode('utf-8') if type(invalid_version) is float else
                actions.canonical_json_bytes(value))

    def _read(path: str, *, name: str, maximum_bytes: int) -> bytes:
        del path, name
        assert len(contents) <= maximum_bytes
        return contents

    monkeypatch.setattr(preflight, '_read_fixed_regular_file', _read)
    with pytest.raises(preflight.ProviderAuthorityStaticEvidenceError,
                       match='version|floating-point'):
        preflight.load_provider_authority_worker_static_evidence()
