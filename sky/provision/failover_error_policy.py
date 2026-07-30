"""Provider failure parsing and resource block-list policy."""
from collections.abc import Callable
import textwrap

import colorama

from sky import catalog
from sky import clouds
from sky import exceptions
from sky import resources as resources_lib
from sky import sky_logging
from sky.provision import common as provision_common
from sky.utils import common_utils
from sky.utils import ux_utils

logger = sky_logging.init_logger('sky.backends.cloud_vm_ray_backend')

_RSYNC_NOT_FOUND_MESSAGE = (
    '`rsync` command is not found in the specified image. '
    'Please use an image with rsync installed.')


def _add_to_blocked_resources(blocked_resources: set['resources_lib.Resources'],
                              resources: 'resources_lib.Resources') -> None:
    # If the resources is already blocked by blocked_resources, we don't need to
    # add it again to avoid duplicated entries.
    for r in blocked_resources:
        if resources.should_be_blocked_by(r):
            return
    blocked_resources.add(resources)


class FailoverCloudErrorHandlerV1:
    """Handles errors during provisioning and updates the blocked_resources.

    Deprecated: Newly added cloud should use the FailoverCloudErrorHandlerV2,
    which is more robust by parsing the errors raised by the cloud's API in a
    more structured way, instead of directly based on the stdout and stderr.
    """

    @staticmethod
    def _handle_errors(stdout: str, stderr: str,
                       is_error_str_known: Callable[[str], bool]) -> list[str]:
        stdout_splits = stdout.split('\n')
        stderr_splits = stderr.split('\n')
        errors = [
            s.strip()
            for s in stdout_splits + stderr_splits
            if is_error_str_known(s.strip())
        ]
        if errors:
            return errors
        if 'rsync: command not found' in stderr:
            with ux_utils.print_exception_no_traceback():
                e = RuntimeError(_RSYNC_NOT_FOUND_MESSAGE)
                setattr(e, 'detailed_reason',
                        f'stdout: {stdout}\nstderr: {stderr}')
                raise e
        detailed_reason = textwrap.dedent(f"""\
        ====== stdout ======
        {stdout}
        ====== stderr ======
        {stderr}
        """)
        logger.info('====== stdout ======')
        print(stdout)
        logger.info('====== stderr ======')
        print(stderr)
        with ux_utils.print_exception_no_traceback():
            e = RuntimeError('Errors occurred during provision; '
                             'check logs above.')
            setattr(e, 'detailed_reason', detailed_reason)
            raise e

    @staticmethod
    def _ibm_handler(blocked_resources: set['resources_lib.Resources'],
                     launchable_resources: 'resources_lib.Resources',
                     region: 'clouds.Region', zones: list['clouds.Zone'] | None,
                     stdout: str, stderr: str):

        errors = FailoverCloudErrorHandlerV1._handle_errors(
            stdout, stderr,
            lambda x: 'ERR' in x.strip() or 'PANIC' in x.strip())

        logger.warning(f'Got error(s) on IBM cluster, in {region.name}:')
        messages = '\n\t'.join(errors)
        style = colorama.Style
        logger.warning(f'{style.DIM}\t{messages}{style.RESET_ALL}')

        for zone in zones:  # type: ignore[union-attr]
            _add_to_blocked_resources(blocked_resources,
                                      launchable_resources.copy(zone=zone.name))

    @staticmethod
    def update_blocklist_on_error(
            blocked_resources: set['resources_lib.Resources'],
            launchable_resources: 'resources_lib.Resources',
            region: 'clouds.Region', zones: list['clouds.Zone'] | None,
            stdout: str | None, stderr: str | None) -> bool:
        """Handles cloud-specific errors and updates the block list.

        This parses textual stdout/stderr because we don't directly use the
        underlying clouds' SDKs.  If we did that, we could catch proper
        exceptions instead.

        Returns:
          definitely_no_nodes_launched: bool, True if definitely no nodes
            launched (e.g., due to VPC errors we have never sent the provision
            request), False otherwise.
        """
        assert launchable_resources.region == region.name, (
            launchable_resources, region)
        if stdout is None:
            # Gang scheduling failure (head node is definitely up, but some
            # workers' provisioning failed).  Simply block the zones.
            assert stderr is None, stderr
            if zones is not None:
                for zone in zones:
                    _add_to_blocked_resources(
                        blocked_resources,
                        launchable_resources.copy(zone=zone.name))
            return False  # definitely_no_nodes_launched
        assert stdout is not None and stderr is not None, (stdout, stderr)

        # TODO(zongheng): refactor into Cloud interface?
        cloud = launchable_resources.cloud
        handler = getattr(FailoverCloudErrorHandlerV1,
                          f'_{str(cloud).lower()}_handler')
        if handler is None:
            raise NotImplementedError(
                f'Cloud {cloud} unknown, or has not added '
                'support for parsing and handling provision failures. '
                'Please implement a handler in FailoverCloudErrorHandlerV1 when'
                'ray-autoscaler-based provisioner is used for the cloud.')
        handler(blocked_resources, launchable_resources, region, zones, stdout,
                stderr)

        stdout_splits = stdout.split('\n')
        stderr_splits = stderr.split('\n')
        # Determining whether head node launch *may* have been requested based
        # on outputs is tricky. We are conservative here by choosing an "early
        # enough" output line in the following:
        # https://github.com/ray-project/ray/blob/03b6bc7b5a305877501110ec04710a9c57011479/python/ray/autoscaler/_private/commands.py#L704-L737  # pylint: disable=line-too-long
        # This is okay, because we mainly want to use the return value of this
        # func to skip cleaning up never-launched clusters that encountered VPC
        # errors; their launch should not have printed any such outputs.
        head_node_launch_may_have_been_requested = any(
            'Acquiring an up-to-date head node' in line
            for line in stdout_splits + stderr_splits)
        # If head node request has definitely not been sent (this happens when
        # there are errors during node provider "bootstrapping", e.g.,
        # VPC-not-found errors), then definitely no nodes are launched.
        definitely_no_nodes_launched = (
            not head_node_launch_may_have_been_requested)

        return definitely_no_nodes_launched


class FailoverCloudErrorHandlerV2:
    """Handles errors during provisioning and updates the blocked_resources.

    This is a more robust version of FailoverCloudErrorHandlerV1. V2 parses
    the errors raised by the cloud's API using the exception, instead of the
    stdout and stderr.
    """

    @staticmethod
    def _azure_handler(blocked_resources: set['resources_lib.Resources'],
                       launchable_resources: 'resources_lib.Resources',
                       region: 'clouds.Region', zones: list['clouds.Zone'],
                       err: Exception):
        del region, zones  # Unused.
        if '(ReadOnlyDisabledSubscription)' in str(err):
            logger.info(
                f'{colorama.Style.DIM}Azure subscription is read-only. '
                'Skip provisioning on Azure. Please check the subscription set '
                'with az account set -s <subscription_id>.'
                f'{colorama.Style.RESET_ALL}')
            _add_to_blocked_resources(
                blocked_resources,
                resources_lib.Resources(cloud=clouds.Azure()))
        elif 'ClientAuthenticationError' in str(err):
            _add_to_blocked_resources(
                blocked_resources,
                resources_lib.Resources(cloud=clouds.Azure()))
        else:
            _add_to_blocked_resources(blocked_resources,
                                      launchable_resources.copy(zone=None))

    @staticmethod
    def _gcp_handler(blocked_resources: set['resources_lib.Resources'],
                     launchable_resources: 'resources_lib.Resources',
                     region: 'clouds.Region', zones: list['clouds.Zone'],
                     err: Exception):
        assert zones and len(zones) == 1, zones
        zone = zones[0]

        if not isinstance(err, provision_common.ProvisionerError):
            logger.warning(f'{colorama.Style.DIM}Got an unparsed error: {err}; '
                           f'blocking resources by its zone {zone.name}'
                           f'{colorama.Style.RESET_ALL}')
            _add_to_blocked_resources(blocked_resources,
                                      launchable_resources.copy(zone=zone.name))
            return
        errors = err.errors

        for e in errors:
            code = e['code']
            message = e['message']

            if code in ('QUOTA_EXCEEDED', 'quotaExceeded'):
                if '\'GPUS_ALL_REGIONS\' exceeded' in message:
                    # Global quota.  All regions in GCP will fail.  Ex:
                    # Quota 'GPUS_ALL_REGIONS' exceeded.  Limit: 1.0
                    # globally.
                    # This skip is only correct if we implement "first
                    # retry the region/zone of an existing cluster with the
                    # same name" correctly.
                    _add_to_blocked_resources(
                        blocked_resources,
                        launchable_resources.copy(region=None, zone=None))
                else:
                    # Per region.  Ex: Quota 'CPUS' exceeded.  Limit: 24.0
                    # in region us-west1.
                    _add_to_blocked_resources(
                        blocked_resources, launchable_resources.copy(zone=None))
            elif code in [
                    'ZONE_RESOURCE_POOL_EXHAUSTED',
                    'ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS',
                    'UNSUPPORTED_OPERATION',
                    'insufficientCapacity',
            ]:  # Per zone.
                # Return codes can be found at https://cloud.google.com/compute/docs/troubleshooting/troubleshooting-vm-creation # pylint: disable=line-too-long
                # However, UNSUPPORTED_OPERATION is observed empirically
                # when VM is preempted during creation.  This seems to be
                # not documented by GCP.
                _add_to_blocked_resources(
                    blocked_resources,
                    launchable_resources.copy(zone=zone.name))
            elif code in ['RESOURCE_NOT_READY']:
                # This code is returned when the VM is still STOPPING.
                _add_to_blocked_resources(
                    blocked_resources,
                    launchable_resources.copy(zone=zone.name))
            elif code in ['RESOURCE_OPERATION_RATE_EXCEEDED']:
                # This code can happen when the VM is being created with a
                # machine image, and the VM and the machine image are on
                # different zones. We already have the retry when calling the
                # insert API, but if it still fails, we should block the zone
                # to avoid infinite retry.
                _add_to_blocked_resources(
                    blocked_resources,
                    launchable_resources.copy(zone=zone.name))
            elif code in [3, 8, 9]:
                # Error code 3 means TPU is preempted during creation.
                # Example:
                # {'code': 3, 'message': 'Cloud TPU received a bad request. update is not supported while in state PREEMPTED [EID: 0x73013519f5b7feb2]'} # pylint: disable=line-too-long
                # Error code 8 means TPU resources is out of
                # capacity. Example:
                # {'code': 8, 'message': 'There is no more capacity in the zone "europe-west4-a"; you can try in another zone where Cloud TPU Nodes are offered (see https://cloud.google.com/tpu/docs/regions) [EID: 0x1bc8f9d790be9142]'} # pylint: disable=line-too-long
                # Error code 9 means TPU resources is insufficient reserved
                # capacity. Example:
                # {'code': 9, 'message': 'Insufficient reserved capacity. Contact customer support to increase your reservation. [EID: 0x2f8bc266e74261a]'} # pylint: disable=line-too-long
                _add_to_blocked_resources(
                    blocked_resources,
                    launchable_resources.copy(zone=zone.name))
            elif code == 'RESOURCE_NOT_FOUND':
                # https://github.com/skypilot-org/skypilot/issues/1797
                # In the inner provision loop we have used retries to
                # recover but failed. This indicates this zone is most
                # likely out of capacity. The provision loop will terminate
                # any potentially live VMs before moving onto the next
                # zone.
                _add_to_blocked_resources(
                    blocked_resources,
                    launchable_resources.copy(zone=zone.name))
            elif code == 'VPC_NOT_FOUND':
                # User has specified a VPC that does not exist. On GCP, VPC is
                # global. So we skip the entire cloud.
                _add_to_blocked_resources(
                    blocked_resources,
                    launchable_resources.copy(region=None, zone=None))
            elif code == 'SUBNET_NOT_FOUND_FOR_VPC':
                if (launchable_resources.accelerators is not None and any(
                        acc.lower().startswith('tpu-v4')
                        for acc in launchable_resources.accelerators.keys()) and
                        region.name == 'us-central2'):
                    # us-central2 is a TPU v4 only region. The subnet for this
                    # region may not exist when the user does not have the TPU
                    # v4 quota. We should skip this region.
                    logger.warning('Please check if you have TPU v4 quotas '
                                   f'in {region.name}.')
                _add_to_blocked_resources(
                    blocked_resources,
                    launchable_resources.copy(region=region.name, zone=None))
            elif code == 'type.googleapis.com/google.rpc.QuotaFailure':
                # TPU VM pod specific error.
                if 'in region' in message:
                    # Example:
                    # "Quota 'TPUV2sPreemptiblePodPerProjectPerRegionForTPUAPI'
                    # exhausted. Limit 32 in region europe-west4"
                    _add_to_blocked_resources(
                        blocked_resources,
                        launchable_resources.copy(region=region.name,
                                                  zone=None))
                elif 'in zone' in message:
                    # Example:
                    # "Quota 'TPUV2sPreemptiblePodPerProjectPerZoneForTPUAPI'
                    # exhausted. Limit 32 in zone europe-west4-a"
                    _add_to_blocked_resources(
                        blocked_resources,
                        launchable_resources.copy(zone=zone.name))

            elif 'Requested disk size cannot be smaller than the image size' in message:
                logger.info('Skipping all regions due to disk size issue.')
                _add_to_blocked_resources(
                    blocked_resources,
                    launchable_resources.copy(region=None, zone=None))
            elif 'Policy update access denied.' in message or code == 'IAM_PERMISSION_DENIED':
                logger.info(
                    'Skipping all regions due to service account not '
                    'having the required permissions and the user '
                    'account does not have enough permission to '
                    'update it. Please contact your administrator and '
                    'check out: https://docs.skypilot.co/en/latest/cloud-setup/cloud-permissions/gcp.html\n'  # pylint: disable=line-too-long
                    f'Details: {message}')
                _add_to_blocked_resources(
                    blocked_resources,
                    launchable_resources.copy(region=None, zone=None))
            elif 'is not found or access is unauthorized' in message:
                # Parse HttpError for unauthorized regions. Example:
                # googleapiclient.errors.HttpError: <HttpError 403 when requesting ... returned "Location us-east1-d is not found or access is unauthorized.". # pylint: disable=line-too-long
                # Details: "Location us-east1-d is not found or access is
                # unauthorized.">
                _add_to_blocked_resources(
                    blocked_resources,
                    launchable_resources.copy(zone=zone.name))
            else:
                logger.debug('Got unparsed error blocking resources by zone: '
                             f'{e}.')
                _add_to_blocked_resources(
                    blocked_resources,
                    launchable_resources.copy(zone=zone.name))

    @staticmethod
    def _lambda_handler(blocked_resources: set['resources_lib.Resources'],
                        launchable_resources: 'resources_lib.Resources',
                        region: 'clouds.Region',
                        zones: list['clouds.Zone'] | None, error: Exception):
        output = str(error)
        # Sometimes, lambda cloud error will list available regions.
        if output.find('Regions with capacity available:') != -1:
            for r in catalog.regions('lambda'):
                if output.find(r.name) == -1:
                    _add_to_blocked_resources(
                        blocked_resources,
                        launchable_resources.copy(region=r.name, zone=None))
        else:
            FailoverCloudErrorHandlerV2._default_handler(
                blocked_resources, launchable_resources, region, zones, error)

    @staticmethod
    def _aws_handler(blocked_resources: set['resources_lib.Resources'],
                     launchable_resources: 'resources_lib.Resources',
                     region: 'clouds.Region', zones: list['clouds.Zone'] | None,
                     error: Exception) -> None:
        logger.info(f'AWS handler error: {error}')
        # Block AWS if the credential has expired.
        if isinstance(error, exceptions.InvalidCloudCredentials):
            _add_to_blocked_resources(
                blocked_resources, resources_lib.Resources(cloud=clouds.AWS()))
        else:
            FailoverCloudErrorHandlerV2._default_handler(
                blocked_resources, launchable_resources, region, zones, error)

    @staticmethod
    def _scp_handler(blocked_resources: set['resources_lib.Resources'],
                     launchable_resources: 'resources_lib.Resources',
                     region: 'clouds.Region', zones: list['clouds.Zone'] | None,
                     error: Exception) -> None:
        logger.info(f'SCP handler error: {error}')
        # Block SCP if the credential has expired.
        if isinstance(error, exceptions.InvalidCloudCredentials):
            _add_to_blocked_resources(
                blocked_resources, resources_lib.Resources(cloud=clouds.SCP()))
        else:
            FailoverCloudErrorHandlerV2._default_handler(
                blocked_resources, launchable_resources, region, zones, error)

    @staticmethod
    def _default_handler(blocked_resources: set['resources_lib.Resources'],
                         launchable_resources: 'resources_lib.Resources',
                         region: 'clouds.Region',
                         zones: list['clouds.Zone'] | None,
                         error: Exception) -> None:
        """Handles cloud-specific errors and updates the block list."""
        del region  # Unused.
        logger.debug(
            f'Got error(s) in {launchable_resources.cloud}:'
            f'{common_utils.format_exception(error, use_bracket=True)}')
        if zones is None:
            _add_to_blocked_resources(blocked_resources,
                                      launchable_resources.copy(zone=None))
        else:
            for zone in zones:
                _add_to_blocked_resources(
                    blocked_resources,
                    launchable_resources.copy(zone=zone.name))

    @staticmethod
    def update_blocklist_on_error(
            blocked_resources: set['resources_lib.Resources'],
            launchable_resources: 'resources_lib.Resources',
            region: 'clouds.Region', zones: list['clouds.Zone'] | None,
            error: Exception) -> None:
        """Handles cloud-specific errors and updates the block list."""
        cloud = launchable_resources.cloud
        handler = getattr(FailoverCloudErrorHandlerV2,
                          f'_{str(cloud).lower()}_handler',
                          FailoverCloudErrorHandlerV2._default_handler)
        handler(blocked_resources, launchable_resources, region, zones, error)


# Preserve reflection and pickle lookup through the historical backend facade.
_HISTORICAL_MODULE = 'sky.backends.cloud_vm_ray_backend'
_add_to_blocked_resources.__module__ = _HISTORICAL_MODULE
for _handler_class in (FailoverCloudErrorHandlerV1,
                       FailoverCloudErrorHandlerV2):
    _handler_class.__module__ = _HISTORICAL_MODULE
    for _handler_name in vars(_handler_class):
        _handler_symbol = getattr(_handler_class, _handler_name)
        if callable(_handler_symbol):
            _handler_symbol.__module__ = _HISTORICAL_MODULE
del _handler_class
del _handler_name
del _handler_symbol
