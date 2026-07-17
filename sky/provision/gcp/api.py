"""GCP provisioning API gateway helpers."""
import logging
import time
import typing

from sky.adaptors import gcp
from sky.provision.gcp import constants

# Preserve the historical logger namespace for moved gateway operations.
logger = logging.getLogger('sky.provision.gcp.config')

if typing.TYPE_CHECKING:
    import google.cloud


def wait_for_crm_operation(operation, crm):
    """Poll for cloud resource manager operation until finished."""
    logger.info('wait_for_crm_operation: '
                f'Waiting for operation {operation} to finish...')

    result = None
    for _ in range(constants.MAX_POLLS):
        result = crm.operations().get(name=operation['name']).execute()
        if 'error' in result:
            raise Exception(result['error'])

        if 'done' in result and result['done']:
            logger.info('wait_for_crm_operation: Operation done.')
            break

        time.sleep(constants.POLL_INTERVAL)

    if result is None:
        raise RuntimeError('Cloud resource manager polling did not run.')
    return result


def wait_for_compute_global_operation(project_name, operation, compute):
    """Poll for global compute operation until finished."""
    logger.info('wait_for_compute_global_operation: '
                'Waiting for operation {} to finish...'.format(
                    operation['name']))

    result = None
    for _ in range(constants.MAX_POLLS):
        result = (compute.globalOperations().get(
            project=project_name,
            operation=operation['name'],
        ).execute())
        if 'error' in result:
            raise Exception(result['error'])

        if result['status'] == 'DONE':
            logger.info('wait_for_compute_global_operation: Operation done.')
            break

        time.sleep(constants.POLL_INTERVAL)

    if result is None:
        raise RuntimeError('Global compute polling did not run.')
    return result


def wait_for_compute_region_operation(project_name, region, operation, compute):
    """Poll for region compute operation until finished."""
    logger.info('wait_for_compute_region_operation: '
                'Waiting for operation {} to finish...'.format(
                    operation['name']))

    result = None
    for _ in range(constants.MAX_POLLS):
        result = (compute.regionOperations().get(
            project=project_name,
            region=region,
            operation=operation['name'],
        ).execute())
        if 'error' in result:
            raise Exception(result['error'])

        if result['status'] == 'DONE':
            logger.info('wait_for_compute_region_operation: Operation done.')
            break

        time.sleep(constants.POLL_INTERVAL)

    if result is None:
        raise RuntimeError('Regional compute polling did not run.')
    return result


def _create_crm(gcp_credentials=None):
    return gcp.build('cloudresourcemanager',
                     'v1',
                     credentials=gcp_credentials,
                     cache_discovery=False)


def _create_iam(gcp_credentials=None):
    return gcp.build('iam',
                     'v1',
                     credentials=gcp_credentials,
                     cache_discovery=False)


def _create_compute(gcp_credentials=None):
    return gcp.build('compute',
                     'v1',
                     credentials=gcp_credentials,
                     cache_discovery=False)


def _create_tpu(gcp_credentials=None):
    return gcp.build(
        'tpu',
        constants.TPU_VM_VERSION,
        credentials=gcp_credentials,
        cache_discovery=False,
        discoveryServiceUrl='https://tpu.googleapis.com/$discovery/rest',
    )


def _delete_firewall_rule(project_id: str, compute, name):
    operation = (compute.firewalls().delete(project=project_id,
                                            firewall=name).execute())
    response = wait_for_compute_global_operation(project_id, operation, compute)
    return response


# pylint: disable=redefined-builtin
def _list_firewall_rules(project_id, compute, filter=None):
    response = (compute.firewalls().list(
        project=project_id,
        filter=filter,
    ).execute())
    return response['items'] if 'items' in response else []


def _create_vpcnet(project_id: str, compute, body):
    operation = (compute.networks().insert(project=project_id,
                                           body=body).execute())
    response = wait_for_compute_global_operation(project_id, operation, compute)
    return response


def _list_vpcnets(project_id: str, compute, filter=None):  # pylint: disable=redefined-builtin
    response = (compute.networks().list(
        project=project_id,
        filter=filter,
    ).execute())

    return (list(sorted(response['items'], key=lambda x: x['name']))
            if 'items' in response else [])


def _delete_vpcnet(project_id: str, compute, vpcnet_name: str):
    operation = compute.networks().delete(
        project=project_id,
        network=vpcnet_name,
    ).execute()
    return wait_for_compute_global_operation(project_id, operation, compute)


def _list_subnets(
        project_id: str,
        region: str,
        compute,
        network=None
) -> list['google.cloud.compute_v1.types.compute.Subnetwork']:
    response = (compute.subnetworks().list(
        project=project_id,
        region=region,
    ).execute())

    items = response['items'] if 'items' in response else []
    if network is None:
        return items

    # Filter by network (VPC) name.
    #
    # Note we do not directly use the filter (network=<...>) arg of the list()
    # call above, because it'd involve constructing a long URL of the following
    # format and passing it as the filter value:
    # 'https://www.googleapis.com/compute/v1/projects/<project_id>/global/networks/<network_name>' # pylint: disable=line-too-long
    matched_items = []
    for item in items:
        if network == _network_interface_to_vpc_name(item):
            matched_items.append(item)
    return matched_items


def _network_interface_to_vpc_name(network_interface: dict[str, str]) -> str:
    """Returns the VPC name of a network interface."""
    return network_interface['network'].split('/')[-1]


def _get_project(project_id: str, crm):
    try:
        project = crm.projects().get(projectId=project_id).execute()
    except gcp.http_error_exception() as e:
        if e.resp.status != 403:
            raise
        project = None

    return project


def _create_project(project_id: str, crm):
    operation = (crm.projects().create(body={
        'projectId': project_id,
        'name': project_id
    }).execute())

    result = wait_for_crm_operation(operation, crm)

    return result


def _get_service_account(account: str, project_id: str, iam):
    full_name = f'projects/{project_id}/serviceAccounts/{account}'
    try:
        service_account = iam.projects().serviceAccounts().get(
            name=full_name).execute()
    except gcp.http_error_exception() as e:
        if e.resp.status not in [403, 404]:
            # SkyPilot: added 403, which means the service account doesn't
            # exist, or not accessible by the current account, which is fine,
            # as we do the fallback in the caller.
            raise
        service_account = None

    return service_account


def _create_service_account(account_id: str, account_config, project_id: str,
                            iam):
    service_account = (iam.projects().serviceAccounts().create(
        name=f'projects/{project_id}',
        body={
            'accountId': account_id,
            'serviceAccount': account_config,
        },
    ).execute())

    return service_account


def _add_iam_policy_binding(service_account, policy, crm, iam):
    """Add new IAM roles for the service account."""
    del iam
    project_id = service_account['projectId']

    result = (crm.projects().setIamPolicy(
        resource=project_id,
        body={
            'policy': policy,
        },
    ).execute())

    return result


def _create_subnet(project_id: str, region: str, compute, vpc_name: str,
                   subnet_name: str, ip_cidr_range: str):
    body = {
        'name': subnet_name,
        'ipCidrRange': ip_cidr_range,
        'network': f'projects/{project_id}/global/networks/{vpc_name}',
        'region': region,
    }
    operation = compute.subnetworks().insert(project=project_id,
                                             region=region,
                                             body=body).execute()
    response = wait_for_compute_region_operation(project_id, region, operation,
                                                 compute)
    return response


def _delete_subnet(project_id: str, region: str, compute, subnet_name: str):
    operation = compute.subnetworks().delete(
        project=project_id,
        region=region,
        subnetwork=subnet_name,
    ).execute()
    return wait_for_compute_region_operation(project_id, region, operation,
                                             compute)


def _create_placement_policy(project_id: str, region: str, compute,
                             placement_policy: dict):
    operation = compute.resourcePolicies().insert(
        project=project_id, region=region, body=placement_policy).execute()
    response = wait_for_compute_region_operation(project_id, region, operation,
                                                 compute)
    return response


def _get_placement_policy(project_id: str, region: str, compute, name: str):
    try:
        placement_policy = (compute.resourcePolicies().get(
            project=project_id,
            region=region,
            resourcePolicy=name,
        ).execute())
    except gcp.http_error_exception() as e:
        if e.resp.status == 404:
            return None
        raise
    return placement_policy


# Keep the historical facade identity so old pickles and introspection continue
# to resolve these gateway functions through sky.provision.gcp.config.
_FACADE_MODULE = 'sky.provision.gcp.config'
for _gateway_function in (
        wait_for_crm_operation,
        wait_for_compute_global_operation,
        wait_for_compute_region_operation,
        _create_crm,
        _create_iam,
        _create_compute,
        _create_tpu,
        _delete_firewall_rule,
        _list_firewall_rules,
        _create_vpcnet,
        _list_vpcnets,
        _delete_vpcnet,
        _list_subnets,
        _network_interface_to_vpc_name,
        _get_project,
        _create_project,
        _get_service_account,
        _create_service_account,
        _add_iam_policy_binding,
        _create_subnet,
        _delete_subnet,
        _create_placement_policy,
        _get_placement_policy,
):
    _gateway_function.__module__ = _FACADE_MODULE
