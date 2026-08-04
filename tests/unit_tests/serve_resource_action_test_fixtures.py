"""Shared canonical values for authority-worker contract tests."""

import hashlib

from sky.serve import resource_actions as actions

INSTALLATION_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
NAMESPACE = 'skypilot-system'
HELM_FULL_NAME = 'skypilot'
COHORT_SUFFIX = 'p2a-v1'
DEPLOYMENT_NAME = f'{HELM_FULL_NAME}-authority-{COHORT_SUFFIX}'
RELEASE_SCOPE_LABEL = hashlib.sha256(
    f'{NAMESPACE}\n{HELM_FULL_NAME}'.encode()).hexdigest()[:63]
COHORT_SCOPE_SHA256 = hashlib.sha256(
    f'{NAMESPACE}\n{HELM_FULL_NAME}\n{COHORT_SUFFIX}'.encode()).hexdigest()
COHORT_ID = (f'ra:{INSTALLATION_ID}:{COHORT_SCOPE_SHA256}:{COHORT_SUFFIX}')
IMAGE_REFERENCE = 'registry.example/authority@sha256:' + '1' * 64


def artifact_value(repo_path: str, digest_character: str) -> dict:
    return {
        'repo_path': repo_path,
        'byte_size': 17,
        'sha256': digest_character * 64,
    }


def authority_release_inputs_value() -> dict:
    return {
        'version': 1,
        'namespace': NAMESPACE,
        'helm_full_name': HELM_FULL_NAME,
        'cohort_suffix': COHORT_SUFFIX,
        'cohort_id': COHORT_ID,
        'deployment_name': DEPLOYMENT_NAME,
        'service_account_name': DEPLOYMENT_NAME,
        'container_name': 'skypilot-authority-worker',
        'image': IMAGE_REFERENCE,
        'image_pull_policy': 'Always',
        'command': ['tini', '--'],
        'args': [
            'python', '-m', 'sky.server.server', '--deploy', '--host',
            '0.0.0.0', '--role', 'authority-worker', '--role-health-port',
            '46581', '--authority-preflight-port', '46583'
        ],
        'health_port': '46581',
        'preflight_port': '46583',
        'manifest_config_map': {
            'name': f'{DEPLOYMENT_NAME}-manifest',
            'key': 'manifest.json',
            'mount_path': '/etc/skypilot/resource-action-authority/manifest.json',
        },
        'qualification_config_map': {
            'name': f'{DEPLOYMENT_NAME}-qualification',
            'key': 'qualification.json',
            'mount_path': '/etc/skypilot/resource-action-authority/qualification.json',
        },
        'auth_secret': {
            'name': 'authority-auth',
            'key': 'tokens',
            'mount_path': '/etc/skypilot/resource-action-authority/auth/tokens',
        },
        'tls_secret': {
            'name': 'authority-tls',
            'cert_key': 'tls.crt',
            'private_key_key': 'tls.key',
            'ca_key': 'ca.crt',
            'cert_path': '/etc/skypilot/resource-action-authority/tls/tls.crt',
            'private_key_path': '/etc/skypilot/resource-action-authority/tls/tls.key',
            'ca_path': '/etc/skypilot/resource-action-authority/tls/ca.crt',
        },
        'database_secret': {
            'name': 'database-uri',
            'key': 'connection_string',
        },
        'downward_api_fields': [{
            'env': 'SKYPILOT_POD_NAME',
            'field_path': 'metadata.name',
        }, {
            'env': 'SKYPILOT_POD_NAMESPACE',
            'field_path': 'metadata.namespace',
        }, {
            'env': 'SKYPILOT_POD_UID',
            'field_path': 'metadata.uid',
        }],
        'literal_env': [{
            'name': 'SKYPILOT_API_REQUEST_BACKEND',
            'value': 'postgres',
        }, {
            'name': 'SKYPILOT_API_SERVER_ROLE',
            'value': 'authority-worker',
        }, {
            'name': 'SKYPILOT_RELEASE_NAME',
            'value': HELM_FULL_NAME,
        }, {
            'name': 'SKYPILOT_RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE',
            'value': '/etc/skypilot/resource-action-authority/auth/tokens',
        }, {
            'name': 'SKYPILOT_STATE_DB_MIGRATION_MODE',
            'value': 'verify',
        }],
        'secret_env': [{
            'name': 'SKYPILOT_DB_CONNECTION_URI',
            'secret_name': 'database-uri',
            'key': 'connection_string',
        }],
        'resources': {},
        'image_pull_secrets': [],
        'pod_labels': [{
            'key': 'skypilot.co/authority-cohort',
            'value': COHORT_SUFFIX,
        }, {
            'key': 'skypilot.co/authority-release-scope',
            'value': RELEASE_SCOPE_LABEL,
        }, {
            'key': 'skypilot.co/role',
            'value': 'resource-action-authority',
        }],
        'pod_annotations_without_manifest_hash': [],
        'pod_security_context': {},
        'container_security_context': {},
        'node_selector': [],
        'affinity': None,
        'tolerations': [],
        'topology_spread_constraints': [],
        'priority_class_name': None,
        'runtime_class_name': None,
        'scheduler_name': None,
        'termination_grace_period_seconds': 60,
    }


def authority_manifest_value() -> dict:
    pod_template_contract = {
        'repo_path': 'sky/serve/resource_action_provider_preflight.py',
        'byte_size': 38368,
        'sha256': '5941d5d0f64b1d40d046023292f766dfcf301042eaf844960b33c924a6c6611d',
    }
    return {
        'version': 1,
        'cohort_id': COHORT_ID,
        'namespace': NAMESPACE,
        'deployment_name': DEPLOYMENT_NAME,
        'service_account_name': DEPLOYMENT_NAME,
        'container_name': 'skypilot-authority-worker',
        'image': {
            'requested_reference': IMAGE_REFERENCE,
            'oci_manifest_digest': 'sha256:' + '1' * 64,
            'oci_config_digest': 'sha256:' + '2' * 64,
            'qualification_artifact': {
                'source': 'helm_chart_configmap_v1',
                'repo_path': ('charts/skypilot/files/'
                              'resource-action-qualifications/p2a-v1.json'),
                'mount_path': ('/etc/skypilot/resource-action-authority/'
                               'qualification.json'),
                'byte_size': 17,
                'sha256': '3' * 64,
            },
        },
        'pod_template_contract': pod_template_contract,
        'pod_template_binding': {
            'version': 1,
            'contract': 'authority_worker_pod_template_v1',
            'projector_artifact_sha256': pod_template_contract['sha256'],
            'release_inputs': authority_release_inputs_value(),
            'expected_template_sha256': '1225e37de2c47e7218f145053b361e5217f5405b05406b6967d7536c3031850c',
            'manifest_hash_annotation_json_pointer':
                ('/metadata/annotations/'
                 'skypilot.co~1resource-action-manifest-sha256'),
            'manifest_hash_placeholder': '$MANIFEST_SHA256',
        },
        'artifact_inventory': {
            'repo_path':
                ('sky/serve/resource_action_artifacts/provider_authority_v1/'
                 'renderer_artifact_inventory.json'),
            'byte_size': 1198,
            'sha256': 'c24b7442135d1b3cc7641d5efbd44375d73df9447e63e7011648eeb67f11da51',
        },
        'callable_inventory': {
            'repo_path':
                ('sky/serve/resource_action_artifacts/provider_authority_v1/'
                 'callable_inventory.json'),
            'byte_size': 2333,
            'sha256': 'a79e56f31d141f326677f4649650897f0388603d9e2a59f0fbe0d3c778214bcb',
        },
        'claim_contract': 'frozen_action_cohort_join_v1',
        'handler_allowlist': list(
            actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1),
    }


def authority_cohort_value() -> dict:
    manifest = authority_manifest_value()
    return {
        'version': 1,
        'manifest': manifest,
        'manifest_sha256': actions.canonical_sha256(manifest),
        'deployment_uid': 'deployment-uid-v1',
        'service_account_uid': 'service-account-uid-v1',
    }


def authority_worker_value(pod_uid: str = 'worker-pod-uid-v1') -> dict:
    cohort = authority_cohort_value()
    manifest = cohort['manifest']
    qualification = manifest['image']
    return {
        'namespace': manifest['namespace'],
        'pod_name': f'worker-{pod_uid}',
        'pod_uid': pod_uid,
        'pod_resource_version': '101',
        'pod_service_account_name': manifest['service_account_name'],
        'pod_controller_owner': {
            'api_version': 'apps/v1',
            'kind': 'ReplicaSet',
            'name': f'{DEPLOYMENT_NAME}-abc',
            'uid': 'replicaset-uid-v1',
        },
        'replica_set_name': f'{DEPLOYMENT_NAME}-abc',
        'replica_set_uid': 'replicaset-uid-v1',
        'replica_set_resource_version': '102',
        'replica_set_controller_owner': {
            'api_version': 'apps/v1',
            'kind': 'Deployment',
            'name': manifest['deployment_name'],
            'uid': cohort['deployment_uid'],
        },
        'deployment_name': manifest['deployment_name'],
        'deployment_uid': cohort['deployment_uid'],
        'deployment_resource_version': '103',
        'deployment_generation': 5,
        'deployment_observed_generation': 5,
        'pod_template_contract_sha256': manifest['pod_template_contract']
                                        ['sha256'],
        'image': {
            'qualification': qualification,
            'runtime': {
                'raw_image_id': 'containerd://sha256:' + '2' * 64,
                'runtime_image_id_scheme': 'containerd',
                'runtime_image_id_digest': 'sha256:' + '2' * 64,
                'qualified_oci_manifest_digest': 'sha256:' + '1' * 64,
                'qualified_oci_config_digest': 'sha256:' + '2' * 64,
                'qualification_artifact_sha256':
                    qualification['qualification_artifact']['sha256'],
                'runtime_id_contract': 'qualified_oci_config_digest_v1',
            },
        },
        'service_account_uid': cohort['service_account_uid'],
        'artifact_inventory_sha256': manifest['artifact_inventory']['sha256'],
        'callable_inventory_sha256': manifest['callable_inventory']['sha256'],
        'handler_allowlist_sha256': actions.canonical_sha256(
            manifest['handler_allowlist']),
        'observed_at': '2026-08-01T01:01:00.000000Z',
    }
