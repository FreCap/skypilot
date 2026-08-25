# pyright: reportOptionalMemberAccess=error
"""Cloud-neutral VM provision utils."""
import contextlib
import dataclasses
import json
import time
import traceback

import colorama

import sky
from sky import clouds
from sky import exceptions
from sky import global_user_state
from sky import logs
from sky import provision
from sky import resources as resources_lib
from sky import sky_logging
from sky.adaptors import aws
from sky.adaptors import kubernetes as kubernetes_adaptor
from sky.backends import backend_utils
from sky.jobs.server import utils as server_jobs_utils
from sky.provision import capacity_policy
from sky.provision import common as provision_common
from sky.provision import constants as provision_constants
from sky.provision import instance_setup
from sky.provision import logging as provision_logging
from sky.provision import metadata_utils
from sky.provision import ssh_wait
from sky.provision import volume as provision_volume
from sky.skylet import constants
from sky.utils import common
from sky.utils import common_utils
from sky.utils import message_utils
from sky.utils import resources_utils
from sky.utils import rich_utils
from sky.utils import status_lib
from sky.utils import timeline
from sky.utils import ux_utils

# Do not use __name__ as we do not want to propagate logs to sky.provision,
# which will be customized in sky.provision.logging.
logger = sky_logging.init_logger('sky.provisioner')

# The maximum number of retries for waiting for instances to be ready and
# teardown instances when provisioning fails.
_MAX_RETRY = 3
_TITLE = '\n\n' + '=' * 20 + ' {} ' + '=' * 20 + '\n'


@contextlib.contextmanager
def _runtime_effect_guard(factory: provision_common.ProviderEffectGuardFactory |
                          None,):
    """Hold one fresh authorization around a bounded runtime mutation."""
    if factory is None:
        yield
        return
    with factory():
        yield


def _bulk_provision(
    cloud: clouds.Cloud,
    region: clouds.Region,
    cluster_name: resources_utils.ClusterName,
    bootstrap_config: provision_common.ProvisionConfig,
) -> provision_common.ProvisionRecord:
    provider_name = repr(cloud)
    region_name = region.name

    start = time.time()

    expected_provider_effect_guard_factory = (
        bootstrap_config.provider_effect_guard_factory)
    expected_kueue_runtime = bootstrap_config.kueue_admission_runtime

    # Ephemeral volumes are optional provider objects.  Keep their creation in
    # a separate bounded effect epoch when this launch carries runtime
    # authorization; a stale reserved-fill request must not leak a PVC before
    # the later Pod-create boundary gets a chance to reject it.
    with _runtime_effect_guard(expected_provider_effect_guard_factory):
        provision_volume.provision_ephemeral_volumes(cloud, region_name,
                                                     cluster_name.name_on_cloud,
                                                     bootstrap_config)

    # TODO(suquark): Should we cache the bootstrapped result?
    #  Currently it is not necessary as bootstrapping takes
    #  only ~3s, caching it seems over-engineering and could
    #  cause other issues like the cache is not synced
    #  with the cloud configuration.
    # Kubernetes bootstrap creates or patches Services, RBAC, namespaces, and
    # other provider objects.  It is short, but it is not passive validation;
    # fence the complete bootstrap transaction before run_instances enters its
    # finer Pod mutation epochs and passive admission/scheduling waits.
    with _runtime_effect_guard(expected_provider_effect_guard_factory):
        config = provision.bootstrap_instances(provider_name, region_name,
                                               cluster_name.name_on_cloud,
                                               bootstrap_config)
    if (getattr(config, 'provider_effect_guard_factory', None)
            is not expected_provider_effect_guard_factory):
        raise RuntimeError('Provider bootstrap changed its runtime effect '
                           'authorization boundary.')
    if (getattr(config, 'kueue_admission_runtime', None)
            is not expected_kueue_runtime):
        raise RuntimeError('Provider bootstrap changed its runtime Kueue '
                           'admission boundary.')

    provision_record = provision.run_instances(provider_name,
                                               region_name,
                                               str(cluster_name),
                                               cluster_name.name_on_cloud,
                                               config=config)

    # Kubernetes-based clouds' run_instances already synchronously wait for all
    # pods to be scheduled and running, and their wait_instances is a no-op,
    # so skip the post-run wait/retry loop entirely.
    if provider_name.lower() not in provision_constants.K8S_BASED_CLOUDS:
        backoff = common_utils.Backoff(initial_backoff=1, max_backoff_factor=3)
        logger.debug(
            f'\nWaiting for instances of {cluster_name!r} to be ready...')
        rich_utils.force_update_status(
            ux_utils.spinner_message('Launching - Checking instance status',
                                     str(provision_logging.config.log_path),
                                     cluster_name=str(cluster_name)))
        # AWS would take a very short time (<<1s) updating the state of the
        # instance.
        time.sleep(1)
        for retry_cnt in range(_MAX_RETRY):
            try:
                provision.wait_instances(provider_name,
                                         region_name,
                                         cluster_name.name_on_cloud,
                                         state=status_lib.ClusterStatus.UP)
                break
            except (aws.botocore_exceptions().WaiterError, RuntimeError):
                time.sleep(backoff.current_backoff())
        else:
            raise RuntimeError(
                f'Failed to wait for instances of {cluster_name!r} to be '
                f'ready on the cloud provider after max retries {_MAX_RETRY}.')
        logger.debug(
            f'Instances of {cluster_name!r} are ready after {retry_cnt} '
            'retries.')

    logger.debug(
        f'\nProvisioning {cluster_name!r} took {time.time() - start:.2f} '
        f'seconds.')

    # Add cluster event for provisioning completion.
    global_user_state.add_cluster_event(
        str(cluster_name), status_lib.ClusterStatus.INIT,
        f'Instances launched on {cloud.display_name()} in {region}',
        global_user_state.ClusterEventType.STATUS_CHANGE)

    return provision_record


def bulk_provision(
    cloud: clouds.Cloud,
    region: clouds.Region,
    zones: list[clouds.Zone] | None,
    cluster_name: resources_utils.ClusterName,
    num_nodes: int,
    cluster_yaml: str,
    prev_cluster_ever_up: bool,
    log_dir: str,
    ports_to_open_on_launch: list[int] | None = None,
    *,
    cluster_incarnation: str | None = None,
    provider_create_idempotency_token: str | None = None,
    provider_create_account_id: str | None = None,
    provider_effect_guard_factory: (provision_common.ProviderEffectGuardFactory
                                    | None) = None,
    kueue_admission_runtime: (provision_common.KueuePodAdmissionRuntime |
                              None) = None,
) -> provision_common.ProvisionRecord:
    """Provisions a cluster and wait until fully provisioned.

    Raises:
        StopFailoverError: Raised when during failover cleanup, tearing
            down any potentially live cluster failed despite retries
        Cloud specific exceptions: If the provisioning process failed, cloud-
            specific exceptions will be raised by the cloud APIs.
    """
    if (kueue_admission_runtime is not None and
            not isinstance(kueue_admission_runtime,
                           provision_common.KueuePodAdmissionRuntime)):
        raise exceptions.ReservedFillLaunchFenceError(
            'Kueue reserved-fill provisioning requires one complete typed '
            'admission runtime.')
    if kueue_admission_runtime is not None and num_nodes != 1:
        raise exceptions.ReservedFillLaunchFenceError(
            'Kueue reserved-fill provisioning requires exactly one node and '
            'Pod.')
    original_config = global_user_state.get_cluster_yaml_dict(cluster_yaml)
    head_node_type = original_config['head_node_type']
    bootstrap_config = provision_common.ProvisionConfig(
        provider_config=original_config['provider'],
        authentication_config=original_config['auth'],
        docker_config=original_config.get('docker', {}),
        # NOTE: (might be a legacy issue) we call it
        # 'ray_head_default' in 'gcp-ray.yaml'
        node_config=original_config['available_node_types'][head_node_type]
        ['node_config'],
        count=num_nodes,
        tags={},
        resume_stopped_nodes=True,
        ports_to_open_on_launch=ports_to_open_on_launch,
        cluster_incarnation=cluster_incarnation,
        provider_create_idempotency_token=provider_create_idempotency_token,
        provider_create_account_id=provider_create_account_id,
        provider_effect_guard_factory=provider_effect_guard_factory,
        kueue_admission_runtime=kueue_admission_runtime)

    with provision_logging.setup_provision_logging(log_dir):
        try:
            logger.debug(f'SkyPilot version: {sky.__version__}; '
                         f'commit: {sky.__commit__}')
            logger.debug(_TITLE.format('Provisioning'))
            redacted_config = bootstrap_config.get_redacted_config()
            logger.debug('Provision config:\n'
                         f'{json.dumps(redacted_config, indent=2)}')
            return _bulk_provision(cloud, region, cluster_name,
                                   bootstrap_config)
        except exceptions.NoClusterLaunchedError:
            # Skip the teardown if the cluster was never launched.
            raise
        except exceptions.InvalidCloudCredentials:
            # Skip the teardown if the cloud config is expired and
            # the provisioner should failover to other clouds.
            raise
        except exceptions.InconsistentHighAvailabilityError:
            # Skip the teardown if the high availability property in the
            # user config is inconsistent with the actual cluster.
            # This error is a user error instead of a provisioning failure.
            # And there is no possibility to fix it by teardown.
            raise
        except exceptions.ExecutionPausedError:
            # Pausing to wait on an external condition: keep the resources for
            # resume, do not tear down.
            raise
        except kubernetes_adaptor.KubernetesPhysicalClusterIdentityError:
            # The kubeconfig target changed after durable placement. Cleanup
            # through that alias could mutate the replacement cluster, so
            # propagate the fail-closed signal without provider teardown.
            raise
        except (exceptions.ServeReplicaLaunchFenceError,
                exceptions.ReservedFillLaunchFenceError):
            # A stale or indeterminate terminal authority must never drive
            # destructive cleanup from this request. The current durable
            # service owner reconciles any already-created object.
            raise
        except Exception as exc:  # pylint: disable=broad-except
            provider_negative_ack = None
            if (isinstance(exc, provision_common.ProviderCreateRejectedError)
                    and provider_create_idempotency_token is not None and
                    provider_create_account_id is not None):
                provider_negative_ack = (
                    capacity_policy.validate_provider_negative_ack(
                        getattr(exc, 'provider_negative_ack', None),
                        cluster_name=cluster_name.name_on_cloud,
                        requested_count=num_nodes,
                        client_token=provider_create_idempotency_token,
                        expected_aws_account_id=provider_create_account_id))
            if provider_negative_ack is not None:
                # The provider proved that this exact create had no effects.
                # Re-canonicalize the attached receipt and preserve the typed
                # rejection; a generic teardown would add provider I/O that
                # can mask this durable evidence with StopFailoverError.
                exc.provider_negative_ack = provider_negative_ack
                raise
            if provider_effect_guard_factory is not None:
                # The instrumented path is Kubernetes reserved fill. Its
                # durable replica owner is the one cleanup authority; this
                # stale/failed request must not run an opaque destructive
                # teardown after leaving a bounded compute mutation epoch.
                raise exceptions.ReservedFillLaunchFenceError(
                    'Reserved-fill Kubernetes provisioning stopped; durable '
                    'reserved-fill reconciliation owns exact cleanup.') from exc
            zone_str = 'all zones'
            if zones:
                zone_str = ','.join(zone.name for zone in zones)
            logger.debug(f'Failed to provision {cluster_name.display_name!r} '
                         f'on {cloud} ({zone_str}).')
            logger.debug(f'bulk_provision for {cluster_name!r} '
                         f'failed. Stacktrace:\n{traceback.format_exc()}')
            # If the cluster was ever up, stop it; otherwise terminate it.
            terminate = not prev_cluster_ever_up
            terminate_str = ('Terminating' if terminate else 'Stopping')
            logger.debug(f'{terminate_str} the failed cluster.')
            retry_cnt = 1
            while True:
                try:
                    teardown_cluster(
                        repr(cloud),
                        cluster_name,
                        terminate=terminate,
                        provider_config=original_config['provider'])
                    break
                except NotImplementedError as e:
                    assert not terminate, (
                        'Terminating must be supported by all clouds')
                    exc_msg = common_utils.format_exception(exc).replace(
                        '\n', ' ')
                    # If the underlying cloud does not support stopping
                    # instances, we should stop failover as well.
                    raise provision_common.StopFailoverError(
                        f'Provisioning cluster {cluster_name.display_name} '
                        f'failed: {exc_msg}. Failover is stopped for safety '
                        'because the cluster was previously in UP state but '
                        f'{cloud} does not support stopping instances to '
                        'preserve the cluster state. Please try launching the '
                        'cluster again, or terminate it with: '
                        f'sky down {cluster_name.display_name}') from e
                except Exception as e:  # pylint: disable=broad-except
                    logger.debug(f'{terminate_str} {cluster_name!r} failed.')
                    logger.debug(f'Stacktrace:\n{traceback.format_exc()}')
                    retry_cnt += 1
                    if retry_cnt <= _MAX_RETRY:
                        logger.debug(f'Retrying {retry_cnt}/{_MAX_RETRY}...')
                        time.sleep(5)
                        continue
                    formatted_exception = common_utils.format_exception(
                        e, use_bracket=True)
                    raise provision_common.StopFailoverError(
                        'During provisioner\'s failover, '
                        f'{terminate_str.lower()} {cluster_name!r} failed. '
                        'This can cause resource leakage. Please check the '
                        'failure and the cluster status on the cloud, and '
                        'manually terminate the cluster. '
                        f'Details: {formatted_exception}') from e
            raise


# The backend uses this import-generation identity to preserve the exact old
# call shape for rebound functions and replacement modules. A module reload
# reconstructs the function and this alias together.
_BUILTIN_BULK_PROVISION = bulk_provision


def teardown_cluster(cloud_name: str, cluster_name: resources_utils.ClusterName,
                     terminate: bool, provider_config: dict) -> None:
    """Deleting or stopping a cluster.

    Raises:
        Cloud specific exceptions: If the teardown process failed, cloud-
            specific exceptions will be raised by the cloud APIs.
    """
    if terminate:
        try:
            provision.terminate_instances(cloud_name,
                                          cluster_name.name_on_cloud,
                                          provider_config)
        except RuntimeError as e:
            if provision_constants.ERROR_NO_NODES_LAUNCHED in str(e):
                logger.info(
                    'Ignoring teardown failure as no nodes were launched.')
                logger.debug(f'Stacktrace: {traceback.format_exc()}')
            else:
                raise
        metadata_utils.remove_cluster_metadata(cluster_name.name_on_cloud)
        # This won't crash because not found volumes is ignored.
        provision_volume.delete_ephemeral_volumes(provider_config)
    else:
        provision.stop_instances(cloud_name, cluster_name.name_on_cloud,
                                 provider_config)


def _ssh_probe_command(ip: str,
                       ssh_port: int,
                       ssh_user: str,
                       ssh_private_key: str,
                       ssh_probe_timeout: int,
                       ssh_proxy_command: str | None = None) -> list[str]:
    return ssh_wait.ssh_probe_command(ip, ssh_port, ssh_user, ssh_private_key,
                                      ssh_probe_timeout, ssh_proxy_command)


def _wait_ssh_connection_direct(ip: str,
                                ssh_port: int,
                                ssh_user: str,
                                ssh_private_key: str,
                                ssh_probe_timeout: int,
                                ssh_control_name: str | None = None,
                                ssh_proxy_command: str | None = None,
                                **kwargs) -> tuple[bool, str]:
    return ssh_wait.wait_ssh_connection_direct(
        ip, ssh_port, ssh_user, ssh_private_key, ssh_probe_timeout,
        ssh_control_name, ssh_proxy_command, _wait_ssh_connection_indirect,
        _ssh_probe_command, **kwargs)


def _wait_ssh_connection_indirect(ip: str,
                                  ssh_port: int,
                                  ssh_user: str,
                                  ssh_private_key: str,
                                  ssh_probe_timeout: int,
                                  ssh_control_name: str | None = None,
                                  ssh_proxy_command: str | None = None,
                                  **kwargs) -> tuple[bool, str]:
    return ssh_wait.wait_ssh_connection_indirect(
        ip, ssh_port, ssh_user, ssh_private_key, ssh_probe_timeout,
        ssh_control_name, ssh_proxy_command, _ssh_probe_command, **kwargs)


@timeline.event
def wait_for_ssh(cluster_info: provision_common.ClusterInfo,
                 ssh_credentials: dict[str, str]):
    return ssh_wait.wait_for_ssh(cluster_info, ssh_credentials,
                                 _wait_ssh_connection_direct,
                                 _wait_ssh_connection_indirect)


def _post_provision_setup(
    launched_resources: resources_lib.Resources,
    cluster_name: resources_utils.ClusterName, handle_cluster_yaml: str,
    provision_record: provision_common.ProvisionRecord,
    custom_resource: str | None, existing_cluster_hash: str | None,
    provider_effect_guard_factory: provision_common.ProviderEffectGuardFactory |
    None
) -> provision_common.ClusterInfo:
    config_from_yaml = global_user_state.get_cluster_yaml_dict(
        handle_cluster_yaml)
    provider_config = config_from_yaml.get('provider')
    cloud_name = repr(launched_resources.cloud)
    cluster_info = provision.get_cluster_info(cloud_name,
                                              provision_record.region,
                                              cluster_name.name_on_cloud,
                                              provider_config=provider_config)

    # Update cluster info in handle so cluster instance ids are set. This
    # allows us to expose provision logs to debug nodes that failed during post
    # provision setup.
    handle = global_user_state.get_handle_from_cluster_name(
        cluster_name.display_name, existing_cluster_hash=existing_cluster_hash)
    if handle is None:
        raise exceptions.ClusterDoesNotExist(
            f'Cluster {cluster_name.display_name!r} was removed or replaced '
            'while provisioning was in progress.')
    handle.cached_cluster_info = cluster_info
    global_user_state.update_cluster_handle(
        cluster_name.display_name,
        handle,
        existing_cluster_hash=existing_cluster_hash)

    if cluster_info.num_instances > 1:
        # Only worker nodes have logs in the per-instance log directory. Head
        # node's log will be redirected to the main log file.
        per_instance_log_dir = metadata_utils.get_instance_log_dir(
            cluster_name.name_on_cloud, '*')
        logger.debug('For per-instance logs, run: '
                     f'tail -n 100 -f {per_instance_log_dir}/*.log')

    logger.debug(
        'Provision record:\n'
        f'{json.dumps(dataclasses.asdict(provision_record), indent=2)}\n'
        'Cluster info:\n'
        f'{json.dumps(dataclasses.asdict(cluster_info), indent=2)}')
    head_instance = cluster_info.get_head_instance()
    if head_instance is None:
        e = RuntimeError(f'Provision failed for cluster {cluster_name!r}. '
                         'Could not find any head instance. To fix: refresh '
                         f'status with: sky status -r; and retry provisioning.')
        setattr(e, 'detailed_reason', str(cluster_info))
        raise e

    # TODO(suquark): Move wheel build here in future PRs.
    # We don't set docker_user here, as we are configuring the VM itself.
    ssh_credentials = backend_utils.ssh_credential_from_yaml(
        handle_cluster_yaml, ssh_user=cluster_info.ssh_user)
    docker_config = config_from_yaml.get('docker', {})

    with rich_utils.safe_status(
            ux_utils.spinner_message('Launching - Waiting for SSH access',
                                     provision_logging.config.log_path,
                                     cluster_name=str(cluster_name))) as status:
        # If on Kubernetes, skip SSH check since the pods are guaranteed to be
        # ready by the provisioner, and we use kubectl instead of SSH to run the
        # commands and rsync on the pods. SSH will still be ready after a while
        # for the users to SSH into the pod.
        is_k8s_cloud = cloud_name.lower(
        ) in provision_constants.K8S_BASED_CLOUDS
        is_slurm_cloud = cloud_name.lower() == 'slurm'
        if not is_k8s_cloud and not is_slurm_cloud:
            logger.debug(
                f'\nWaiting for SSH to be available for {cluster_name!r} ...')
            wait_for_ssh(cluster_info, ssh_credentials)
            logger.debug(f'SSH Connection ready for {cluster_name!r}')
        vm_str = 'Instance' if not is_k8s_cloud else 'Pod'
        plural = '' if len(cluster_info.instances) == 1 else 's'
        verb = 'is' if len(cluster_info.instances) == 1 else 'are'
        indent_str = (ux_utils.INDENT_SYMBOL
                      if docker_config else ux_utils.INDENT_LAST_SYMBOL)
        logger.info(f'{indent_str}{colorama.Style.DIM}{vm_str}{plural} {verb} '
                    f'up.{colorama.Style.RESET_ALL}')

        # It's promised by the cluster config that docker_config does not
        # exist for docker-native clouds, i.e. they provide docker containers
        # instead of full VMs, like Kubernetes and RunPod, as it requires some
        # special handlings to run docker inside their docker virtualization.
        # For their Docker image settings, we do them when provisioning the
        # cluster. See provision/{cloud}/instance.py:get_cluster_info for more
        # details.
        if docker_config:
            status.update(
                ux_utils.spinner_message(
                    'Launching - Initializing docker container',
                    provision_logging.config.log_path,
                    cluster_name=str(cluster_name)))
            with _runtime_effect_guard(provider_effect_guard_factory):
                docker_user = instance_setup.initialize_docker(
                    cluster_name.name_on_cloud,
                    docker_config=docker_config,
                    cluster_info=cluster_info,
                    ssh_credentials=ssh_credentials)
            if docker_user is None:
                raise RuntimeError(
                    f'Failed to retrieve docker user for {cluster_name!r}. '
                    'Please check your docker configuration.')

            cluster_info.docker_user = docker_user
            ssh_credentials['docker_user'] = docker_user
            logger.debug(f'Docker user: {docker_user}')
            logger.info(f'{ux_utils.INDENT_LAST_SYMBOL}{colorama.Style.DIM}'
                        f'Docker container is up.{colorama.Style.RESET_ALL}')

        # Check version compatibility for jobs controller clusters
        if cluster_name.display_name.startswith(common.JOB_CONTROLLER_PREFIX):
            # TODO(zeping): remove this in v0.12.0
            # This only happens in upgrade from <0.9.3 to > 0.10.0
            # After 0.10.0 no incompatibility issue
            # See https://github.com/skypilot-org/skypilot/pull/6096
            # For more details
            status.update(
                ux_utils.spinner_message(
                    'Checking controller version compatibility'))

            try:
                server_jobs_utils.check_version_mismatch_and_non_terminal_jobs()
            except exceptions.ClusterNotUpError:
                # Controller is not up yet during initial provisioning, that
                # also means no non-terminal jobs, so no incompatibility in
                # this case.
                pass

        # We mount the metadata with sky wheel for speedup.
        # NOTE: currently we mount all credentials for all nodes, because
        # (1) jobs controllers need permission to launch/down nodes of
        #     multiple clouds
        # (2) head instances need permission for auto stop or auto down
        #     nodes for the current cloud
        # (3) all instances need permission to mount storage for all clouds
        # It is possible to have a "smaller" permission model, but we leave that
        # for later.
        file_mounts = config_from_yaml.get('file_mounts', {})

        runtime_preparation_str = (ux_utils.spinner_message(
            'Preparing SkyPilot runtime ({step}/3 - {step_name})',
            provision_logging.config.log_path,
            cluster_name=str(cluster_name)))
        status.update(
            runtime_preparation_str.format(step=1, step_name='initializing'))
        with _runtime_effect_guard(provider_effect_guard_factory):
            instance_setup.internal_file_mounts(cluster_name.name_on_cloud,
                                                file_mounts, cluster_info,
                                                ssh_credentials)

        status.update(
            runtime_preparation_str.format(step=2, step_name='dependencies'))
        with _runtime_effect_guard(provider_effect_guard_factory):
            instance_setup.setup_runtime_on_cluster(
                cluster_name.name_on_cloud, config_from_yaml['setup_commands'],
                cluster_info, ssh_credentials)

        runners = provision.get_command_runners(cloud_name, cluster_info,
                                                **ssh_credentials)
        head_runner = runners[0]

        def is_ray_cluster_healthy(ray_status_output: str,
                                   expected_num_nodes: int) -> bool:
            """Parse the output of `ray status` to get #active nodes.

            The output of `ray status` looks like:
            Node status
            ---------------------------------------------------------------
            Active:
              1 node_291a8b849439ad6186387c35dc76dc43f9058108f09e8b68108cf9ec
              1 node_0945fbaaa7f0b15a19d2fd3dc48f3a1e2d7c97e4a50ca965f67acbfd
            Pending:
            (no pending nodes)
            Recent failures:
            (no failures)
            """
            start = ray_status_output.find('Active:')
            end = ray_status_output.find('Pending:', start)
            if start == -1 or end == -1:
                return False
            num_active_nodes = 0
            for line in ray_status_output[start:end].split('\n'):
                if line.strip() and not line.startswith('Active:'):
                    num_active_nodes += 1
            return num_active_nodes == expected_num_nodes

        def check_ray_port_and_cluster_healthy() -> tuple[int, bool, bool]:
            head_ray_needs_restart = True
            ray_cluster_healthy = False
            ray_port = constants.SKY_REMOTE_RAY_PORT

            # Check if head node Ray is alive
            returncode, stdout, _ = head_runner.run(
                instance_setup.RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND,
                stream_logs=False,
                require_outputs=True)
            if not returncode:
                ray_port = message_utils.decode_payload(stdout)['ray_port']
                logger.debug(f'Ray cluster on head is up with port {ray_port}.')

            head_ray_needs_restart = bool(returncode)
            # This is a best effort check to see if the ray cluster has expected
            # number of nodes connected.
            ray_cluster_healthy = (not head_ray_needs_restart and
                                   is_ray_cluster_healthy(
                                       stdout, cluster_info.num_instances))
            return ray_port, ray_cluster_healthy, head_ray_needs_restart

        status.update(
            runtime_preparation_str.format(step=3, step_name='runtime'))

        skip_ray_setup = False
        ray_port = constants.SKY_REMOTE_RAY_PORT
        head_ray_needs_restart = True
        ray_cluster_healthy = False
        if (launched_resources.cloud is not None and
                not launched_resources.cloud.uses_ray()):
            skip_ray_setup = True
            logger.debug('Skip Ray cluster setup as cloud does not use Ray.')
        elif (not provision_record.is_instance_just_booted(
                head_instance.instance_id)):
            # Check if head node Ray is alive
            (ray_port, ray_cluster_healthy,
             head_ray_needs_restart) = check_ray_port_and_cluster_healthy()
        elif cloud_name.lower() in provision_constants.K8S_BASED_CLOUDS:
            timeout = 90  # 1.5-min maximum timeout
            start = time.time()
            while True:
                # Wait until Ray cluster is ready
                (ray_port, ray_cluster_healthy,
                 head_ray_needs_restart) = check_ray_port_and_cluster_healthy()
                if ray_cluster_healthy:
                    logger.debug('Ray cluster is ready. Skip head and worker '
                                 'node ray cluster setup.')
                    break
                if time.time() - start > timeout:
                    # In most cases, the ray cluster will be ready after a few
                    # seconds. Trigger ray start on head or worker nodes to be
                    # safe, if the ray cluster is not ready after timeout.
                    break
                logger.debug('Ray cluster is not ready yet, waiting for the '
                             'async setup to complete...')
                time.sleep(1)

        if skip_ray_setup:
            logger.debug('Skip Ray cluster setup on the head node.')
        elif head_ray_needs_restart:
            logger.debug('Starting Ray on the entire cluster.')
            with _runtime_effect_guard(provider_effect_guard_factory):
                instance_setup.start_ray_on_head_node(
                    cluster_name.name_on_cloud,
                    custom_resource=custom_resource,
                    cluster_info=cluster_info,
                    ssh_credentials=ssh_credentials)
        else:
            logger.debug('Ray cluster on head is ready. Skip starting ray '
                         'cluster on head node.')

        # NOTE: We have to check all worker nodes to make sure they are all
        #  healthy, otherwise we can only start Ray on newly started worker
        #  nodes like this:
        #
        # worker_ips = []
        # for inst in cluster_info.instances.values():
        #     if provision_record.is_instance_just_booted(inst.instance_id):
        #         worker_ips.append(inst.public_ip)

        # We don't need to restart ray on worker nodes if the ray cluster is
        # already healthy, i.e. the head node has expected number of nodes
        # connected to the ray cluster.
        if skip_ray_setup:
            logger.debug('Skip Ray cluster setup on the worker nodes.')
        elif cluster_info.num_instances > 1 and not ray_cluster_healthy:
            with _runtime_effect_guard(provider_effect_guard_factory):
                instance_setup.start_ray_on_worker_nodes(
                    cluster_name.name_on_cloud,
                    no_restart=not head_ray_needs_restart,
                    custom_resource=custom_resource,
                    # Pass the ray_port to worker nodes for backward
                    # compatibility as in some existing clusters the ray_port
                    # is not dumped with instance_setup._DUMP_RAY_PORTS.
                    ray_port=ray_port,
                    cluster_info=cluster_info,
                    ssh_credentials=ssh_credentials)
        elif ray_cluster_healthy:
            logger.debug('Ray cluster is ready. Skip starting ray cluster on '
                         'worker nodes.')

        logging_agent = logs.get_logging_agent()
        if logging_agent:
            status.update(
                ux_utils.spinner_message('Setting up logging agent',
                                         provision_logging.config.log_path,
                                         cluster_name=str(cluster_name)))
            with _runtime_effect_guard(provider_effect_guard_factory):
                instance_setup.setup_logging_on_cluster(logging_agent,
                                                        cluster_name,
                                                        cluster_info,
                                                        ssh_credentials)

        with _runtime_effect_guard(provider_effect_guard_factory):
            instance_setup.start_skylet_on_head_node(cluster_name, cluster_info,
                                                     ssh_credentials,
                                                     launched_resources)

    logger.info(
        ux_utils.finishing_message(f'Cluster launched: {cluster_name}.',
                                   provision_logging.config.log_path,
                                   cluster_name=str(cluster_name)))
    return cluster_info


@timeline.event
def post_provision_runtime_setup(
    launched_resources: resources_lib.Resources,
    cluster_name: resources_utils.ClusterName,
    handle_cluster_yaml: str,
    provision_record: provision_common.ProvisionRecord,
    custom_resource: str | None,
    log_dir: str,
    existing_cluster_hash: str | None = None,
    provider_effect_guard_factory: provision_common.ProviderEffectGuardFactory |
    None = None,
) -> provision_common.ClusterInfo:
    """Run internal setup commands after provisioning and before user setup.

    Here are the steps:
    1. Wait for SSH to be ready.
    2. Mount the cloud credentials, skypilot wheel,
       and other necessary files to the VM.
    3. Run setup commands to install dependencies.
    4. Start ray cluster and skylet.
    5. (Optional) Setup logging agent.

    Raises:
        RuntimeError: If the setup process encounters any error.
    """
    with provision_logging.setup_provision_logging(log_dir):
        try:
            logger.debug(_TITLE.format('System Setup After Provision'))
            return _post_provision_setup(
                launched_resources,
                cluster_name,
                handle_cluster_yaml=handle_cluster_yaml,
                provision_record=provision_record,
                custom_resource=custom_resource,
                existing_cluster_hash=existing_cluster_hash,
                provider_effect_guard_factory=provider_effect_guard_factory)
        except Exception:  # pylint: disable=broad-except
            logger.error(
                ux_utils.error_message(
                    'Failed to set up SkyPilot runtime on cluster.',
                    provision_logging.config.log_path))
            if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
                logger.debug(f'Stacktrace:\n{traceback.format_exc()}')
            with ux_utils.print_exception_no_traceback():
                raise
