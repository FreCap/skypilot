"""Legacy GCP TPU node lifecycle gateway."""

from collections.abc import Callable
import subprocess
from typing import Any

from sky import sky_logging
from sky.provision import common
from sky.provision import constants as provision_constants

TPU_NODE_CREATION_FAILURE = 'Failed to provision TPU node.'

# Keep the historical logger namespace while instance_utils remains the public
# facade for these helpers.
logger = sky_logging.init_logger('sky.provision.gcp.instance_utils')


def create_tpu_node(
    project_id: str,
    zone: str,
    tpu_node_config: dict[str, str],
    vpc_name: str,
    format_errors: Callable[[list[dict[str, str]], Any, str | None], str],
) -> None:
    """Create a TPU node with gcloud CLI."""
    tpu_name = tpu_node_config['name']
    tpu_type = tpu_node_config['acceleratorType']
    try:
        cmd = (f'gcloud compute tpus create {tpu_name} '
               f'--project={project_id} '
               f'--zone={zone} '
               f'--version={tpu_node_config["runtimeVersion"]} '
               f'--accelerator-type={tpu_type} '
               f'--labels={provision_constants.TAG_SKYPILOT_MANAGED}='
               f'{provision_constants.SKYPILOT_MANAGED_TAG_VALUE} '
               f'--network={vpc_name}')
        logger.debug(f'Creating TPU {tpu_name} with command:\n{cmd}')
        proc = subprocess.run(
            f'yes | {cmd}',
            capture_output=True,
            shell=True,
            check=True,
        )
        stdout = proc.stdout.decode('ascii')
        logger.debug(stdout)
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode('ascii')
        logger.debug(stderr)
        if 'ALREADY_EXISTS' in stderr:
            # FIXME: should use 'start' on stopped TPUs, replacing
            # 'create'. Or it can be in a "deleting" state. Investigate the
            # right thing to do (force kill + re-provision?).
            logger.warning(f'TPU {tpu_name} already exists; skipped creation.')
            return
        provisioner_err = common.ProvisionerError(TPU_NODE_CREATION_FAILURE)
        if 'RESOURCE_EXHAUSTED' in stderr:
            provisioner_err.errors = [{
                'code': 'RESOURCE_EXHAUSTED',
                'domain': 'tpu',
                'message': f'TPU {tpu_name} creation failed due to quota '
                           'exhaustion. Please visit '
                           'https://console.cloud.google.com/iam-admin/quotas '
                           'for more information.'
            }]
            format_errors(provisioner_err.errors, e, zone)
            raise provisioner_err from e

        if 'PERMISSION_DENIED' in stderr:
            provisioner_err.errors = [{
                'code': 'PERMISSION_DENIED',
                'domain': 'tpu',
                'message': 'TPUs are not available in this zone.'
            }]
            format_errors(provisioner_err.errors, e, zone)
            raise provisioner_err from e

        if 'no more capacity in the zone' in stderr:
            provisioner_err.errors = [{
                'code': 'CapacityExceeded',
                'domain': 'tpu',
                'message': 'No more capacity in this zone.'
            }]
            format_errors(provisioner_err.errors, e, zone)
            raise provisioner_err from e

        if 'CloudTpu received an invalid AcceleratorType' in stderr:
            # INVALID_ARGUMENT: CloudTpu received an invalid
            # AcceleratorType, "v3-8" for zone "us-central1-c". Valid
            # values are "v2-8, ".
            provisioner_err.errors = [{
                'code': 'INVALID_ARGUMENT',
                'domain': 'tpu',
                'message': (f'TPU type {tpu_type} is not available in this '
                            f'zone {zone}.')
            }]
            format_errors(provisioner_err.errors, e, zone)
            raise provisioner_err from e

        # TODO(zhwu): Add more error code handling, if needed.
        provisioner_err.errors = [{
            'code': 'UNKNOWN',
            'domain': 'tpu',
            'message': stderr
        }]
        format_errors(provisioner_err.errors, e, zone)
        raise provisioner_err from e


def delete_tpu_node(project_id: str, zone: str, tpu_node_config: dict[str,
                                                                      str]):
    """Delete a TPU node with gcloud CLI.

    This is used for both stopping and terminating a cluster with a TPU node. It
    is ok to call this function to delete the TPU node when stopping the cluster
    because the host VM will be stopped and have all the information preserved.
    """
    tpu_name = tpu_node_config['name']
    try:
        cmd = (f'gcloud compute tpus delete {tpu_name} '
               f'--project={project_id} '
               f'--zone={zone}')
        logger.debug(f'Deleting TPU {tpu_name} with cmd:\n{cmd}')
        proc = subprocess.run(
            f'yes | {cmd}',
            capture_output=True,
            shell=True,
            check=True,
        )
        stdout = proc.stdout.decode('ascii')
        logger.debug(stdout)
    except subprocess.CalledProcessError as e:
        stdout = e.stdout.decode('ascii')
        stderr = e.stderr.decode('ascii')
        if 'ERROR: (gcloud.compute.tpus.delete) NOT_FOUND' in stderr:
            logger.warning(f'TPU {tpu_name} does not exist; skipped deletion.')
        else:
            raise RuntimeError(f'\nFailed to terminate TPU node {tpu_name} for '
                               'cluster {cluster_name}:\n'
                               '**** STDOUT ****\n'
                               f'{stdout}\n'
                               '**** STDERR ****\n'
                               f'{stderr}') from e
