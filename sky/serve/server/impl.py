"""Implementation of the SkyServe core APIs."""
import contextlib
import hashlib
import os
import pathlib
import re
import secrets
import shlex
import signal
import tempfile
import threading
import typing
from typing import Any, Optional
import uuid

import colorama
import filelock

from sky import backends
from sky import exceptions
from sky import execution
from sky import sky_logging
from sky import skypilot_config
from sky import task as task_lib
from sky.adaptors import common as adaptors_common
from sky.backends import backend_utils
from sky.catalog import common as service_catalog_common
from sky.container_images import catalog_state as container_image_catalog_state
from sky.data import data_utils
from sky.data import storage as storage_lib
from sky.serve import constants as serve_constants
from sky.serve import lb_k8s
from sky.serve import maintenance
from sky.serve import serve_rpc_utils
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve.server import status as serve_status
from sky.server import runtime_profile
from sky.server.requests import request_names
from sky.skylet import constants
from sky.skylet import job_lib
from sky.utils import admin_policy_utils
from sky.utils import command_runner
from sky.utils import common
from sky.utils import common_utils
from sky.utils import controller_utils
from sky.utils import dag_utils
from sky.utils import rich_utils
from sky.utils import subprocess_utils
from sky.utils import ux_utils
from sky.utils import yaml_utils

if typing.TYPE_CHECKING:
    import grpc

    from sky.serve import service as service_lib
else:
    grpc = adaptors_common.LazyImport('grpc')
    service_lib = adaptors_common.LazyImport('sky.serve.service')

logger = sky_logging.init_logger(__name__)

_KUBERNETES_LABEL_VALUE_MAX_LENGTH = 63
_STORAGE_NAME_MAX_LENGTH = 63


def _validate_guarded_ha_task_inputs(dag: Any) -> None:
    """Validate the final policy-mutated Serve DAG before local preparation."""
    runtime_profile.validate_final_task_artifact_inputs(dag.tasks,
                                                        product='SkyServe')


def _prepare_scoped_ephemeral_storage(
        task: 'task_lib.Task',
        resource_scope: str,
        reuse_existing_scope: bool = False) -> tuple[str, str, set[str]]:
    """Namespace deletable storage and return remote mounts to retain."""
    existing_scope = task.metadata.get(
        serve_constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY)
    storage_generation = uuid.uuid4().hex
    if (reuse_existing_scope and isinstance(existing_scope, dict) and
            existing_scope.get('resource_scope') == resource_scope and
            isinstance(existing_scope.get('storage_generation'), str)):
        storage_generation = existing_scope['storage_generation']
    scope_id = serve_utils.generate_ephemeral_storage_scope_id(
        resource_scope, storage_generation)
    unowned_remote_mounts: set[str] = set()
    existing_owned_mounts: set[str] = set()
    if (reuse_existing_scope and isinstance(existing_scope, dict) and
            existing_scope.get('resource_scope') == resource_scope and
            existing_scope.get('scope_id') == scope_id):
        raw_owned_mounts = existing_scope.get('storage_mounts', [])
        if isinstance(raw_owned_mounts, list):
            existing_owned_mounts = {
                mount for mount in raw_owned_mounts if isinstance(mount, str)
            }
    seen_storage_ids: set[int] = set()
    for mount_path, storage in task.storage_mounts.items():
        if storage.persistent:
            continue
        source = storage.source
        if (isinstance(source, str) and
            (data_utils.is_cloud_store_url(source) or '://' in source)):
            # A user-supplied remote bucket cannot be renamed without changing
            # the data being mounted. Keep it outside the owned manifest and
            # retain it on teardown even if persistent:false was requested.
            if mount_path not in existing_owned_mounts:
                unowned_remote_mounts.add(mount_path)
            continue
        if id(storage) in seen_storage_ids:
            continue
        seen_storage_ids.add(id(storage))
        if storage.name is None:
            continue
        suffix = f'-{scope_id}'
        if storage.name.endswith(suffix):
            continue
        prefix_length = _STORAGE_NAME_MAX_LENGTH - len(suffix)
        prefix = storage.name[:prefix_length].rstrip('-') or 'skyserve'
        storage.name = f'{prefix}{suffix}'
    return scope_id, storage_generation, unowned_remote_mounts


def _record_scoped_ephemeral_storage(task: 'task_lib.Task', resource_scope: str,
                                     scope_id: str, storage_generation: str,
                                     unowned_remote_mounts: set[str]) -> None:
    """Persist the exact mount paths whose external resources we own."""
    owned_mounts = sorted(
        mount_path for mount_path, storage in task.storage_mounts.items()
        if (not storage.persistent and
            mount_path not in unowned_remote_mounts and serve_utils.
            ephemeral_storage_identity_matches_scope(storage, scope_id)))
    task.metadata[serve_constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY] = {
        'resource_scope': resource_scope,
        'scope_id': scope_id,
        'storage_generation': storage_generation,
        'storage_mounts': owned_mounts,
    }


def _persist_scoped_ephemeral_storage_intent(task: 'task_lib.Task',
                                             service_name: str,
                                             resource_scope: str,
                                             storage_generation: str,
                                             pool: bool, lifecycle_epoch: int,
                                             provisional: bool) -> None:
    """Persist already-recorded cleanup inventory in the local Serve DB."""
    yaml_content = yaml_utils.dump_yaml_str(task.to_yaml_config())
    if not serve_state.add_ephemeral_storage_cleanup_intent(
            service_name, resource_scope, storage_generation, yaml_content,
            pool, lifecycle_epoch, provisional):
        raise RuntimeError(f'Lost lifecycle ownership before publishing '
                           f'scoped storage cleanup intent for '
                           f'{service_name!r}.')


def _get_committed_storage_generations(
        service_name: str) -> set[tuple[str, str]] | None:
    """Snapshot committed scoped-storage generations for one service.

    Returns None if any committed YAML is unreadable so callers can fail
    closed and retain the durable cleanup intent.
    """
    committed_generations: set[tuple[str, str]] = set()
    for yaml_content in serve_state.get_version_yaml_contents(
            service_name).values():
        try:
            version_task = service_lib.load_task_for_storage_cleanup(
                yaml_content)
        except Exception:  # pylint: disable=broad-except
            return None
        metadata = version_task.metadata.get(
            serve_constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY)
        if not isinstance(metadata, dict):
            continue
        resource_scope = metadata.get('resource_scope')
        storage_generation = metadata.get('storage_generation')
        if (isinstance(resource_scope, str) and
                isinstance(storage_generation, str)):
            committed_generations.add((resource_scope, storage_generation))
    return committed_generations


def _cleanup_provisional_storage_intents(service_name: str,
                                         failed_lifecycle_epoch: int,
                                         lifecycle_lock: Any) -> None:
    """Best-effort eager cleanup for one failed up/update operation."""
    try:
        intents = serve_state.get_ephemeral_storage_cleanup_intents(
            service_name,
            lifecycle_epoch=failed_lifecycle_epoch,
            provisional=True)
        if not intents:
            return
        # Fence the failed submitter before deciding a generation is
        # unreferenced. A controller carrying the old epoch can no longer
        # commit after this point; a commit that won before us is visible in
        # the version scan below.
        cleanup_epoch = serve_utils.advance_service_lifecycle_epoch(
            lifecycle_lock)
        committed_generations = _get_committed_storage_generations(service_name)
        if committed_generations is None:
            logger.info('Retaining provisional storage cleanup intent(s) for '
                        f'{service_name!r}: committed service metadata was '
                        'unreadable.')
            return
        cleanable = [
            intent for intent in intents
            if (intent['resource_scope'],
                intent['storage_generation']) not in committed_generations
        ]
        if not cleanable:
            logger.info('Retaining provisional storage cleanup intent(s) for '
                        f'{service_name!r}: their generation is referenced by '
                        'committed service metadata.')
            return
        cleaned = all(
            service_lib.cleanup_storage(intent['yaml_content'],
                                        intent['resource_scope'])
            for intent in cleanable)
        if cleaned:
            scopes = {intent['resource_scope'] for intent in cleanable}
            remove_cleanup_intents = (
                serve_state.remove_provisional_ephemeral_storage_cleanup_intents
            )
            for resource_scope in scopes:
                removed = remove_cleanup_intents(service_name, resource_scope,
                                                 failed_lifecycle_epoch,
                                                 cleanup_epoch)
                if not removed:
                    logger.warning(
                        f'Cleaned provisional storage for {service_name!r} but '
                        'lost the lifecycle fence before removing its durable '
                        'cleanup intent; a later purge will retry idempotently.'
                    )
    except Exception as e:  # pylint: disable=broad-except
        logger.warning(
            f'Failed to eagerly clean provisional storage for '
            f'{service_name!r}; durable cleanup intent was retained: '
            f'{common_utils.format_exception(e)}')


def _service_test_request_command(endpoint: str) -> str:
    """Return a copyable request example for the installed LB contract."""
    if not serve_utils.is_lb_data_plane_auth_enabled():
        return f'curl {endpoint}'
    header = shlex.quote(
        f'{serve_constants.LB_AUTHORIZATION_HEADER}: Bearer <token>')
    return f'curl -H {header} {endpoint}'


_external_service_endpoint_url = serve_status.external_service_endpoint_url


def _wait_for_service_registration(handle: backends.CloudVmRayResourceHandle,
                                   backend: backends.CloudVmRayBackend,
                                   service_name: str, controller_job_id: int,
                                   pool: bool, resource_scope: str) -> None:
    """Wait for registration while preserving a fresh incarnation's scope."""
    if serve_utils.is_consolidation_mode(pool):
        # The API process shares the Serve DB and scoped controller files.
        # Calling directly carries the preallocated scope even when the
        # controller failed before inserting its row; the legacy gRPC request
        # has no field for that identity.
        lb_port_payload = serve_utils.wait_service_registration(
            service_name,
            controller_job_id,
            pool,
            expected_resource_scope=resource_scope)
        serve_utils.load_service_initialization_result(lb_port_payload)
        return

    use_legacy = not handle.is_grpc_enabled_with_flag
    if handle.is_grpc_enabled_with_flag:
        try:
            serve_rpc_utils.RpcRunner.wait_service_registration(
                handle, service_name, controller_job_id, pool)
        except exceptions.SkyletMethodNotImplementedError:
            use_legacy = True

    if use_legacy:
        code = serve_utils.ServeCodeGen.wait_service_registration(
            service_name, controller_job_id, pool)
        returncode, lb_port_payload, _ = backend.run_on_head(
            handle, code, require_outputs=True, stream_logs=False)
        noun = 'pool' if pool else 'service'
        subprocess_utils.handle_returncode(
            returncode, code, f'Failed to wait for {noun} initialization',
            lb_port_payload)
        serve_utils.load_service_initialization_result(lb_port_payload)


def _get_service_record(service_name: str,
                        pool: bool,
                        handle: backends.CloudVmRayResourceHandle,
                        backend: backends.CloudVmRayBackend,
                        *,
                        include_yaml: bool = False) -> dict[str, Any] | None:
    """Get service metadata without materializing the replica inventory."""
    noun = 'pool' if pool else 'service'

    assert isinstance(handle, backends.CloudVmRayResourceHandle)
    if serve_utils.is_consolidation_mode(pool):
        # The API server and consolidated controller share the Serve DB.
        # Update fencing only needs service-row metadata; routing this through
        # full status serializes every historical replica and can make a
        # large-fleet update unable to start.
        service_record = serve_state.get_service_status_snapshot(
            service_name, require_version=True)
        if service_record is None or service_record['pool'] != pool:
            return None
        if include_yaml:
            version = service_record.get('version')
            if version is not None:
                service_record['yaml_content'] = serve_utils.get_yaml_content(
                    service_name, version, service_record.get('resource_scope'))
        return service_record

    use_legacy = not handle.is_grpc_enabled_with_flag
    service_statuses = None

    if not use_legacy:
        try:
            service_statuses = serve_rpc_utils.RpcRunner.get_service_status(
                handle, [service_name],
                pool,
                summary_only=True,
                include_target_num_replicas=False)
        except exceptions.SkyletMethodNotImplementedError:
            use_legacy = True

    if use_legacy:
        code = serve_utils.ServeCodeGen.get_service_status(
            [service_name],
            pool=pool,
            summary_only=True,
            include_target_num_replicas=False)
        returncode, serve_status_payload, stderr = backend.run_on_head(
            handle,
            code,
            require_outputs=True,
            stream_logs=False,
            separate_stderr=True)
        try:
            subprocess_utils.handle_returncode(returncode,
                                               code,
                                               f'Failed to get {noun} status',
                                               stderr,
                                               stream_logs=True)
        except exceptions.CommandError as e:
            raise RuntimeError(e.error_msg) from e

        service_statuses = serve_utils.load_service_status(serve_status_payload)

    if service_statuses is None:
        raise RuntimeError(f'Failed to get {noun} status.')
    assert len(service_statuses) <= 1, service_statuses
    if not service_statuses:
        return None
    return service_statuses[0]


def _require_service_update_workspace(service_record: dict[str, Any],
                                      service_name: str, noun: str) -> str:
    """Returns the durable workspace after fencing the request scope."""
    active_workspace = skypilot_config.get_active_workspace()
    stored_workspace = service_record.get('workspace')
    if isinstance(stored_workspace, str) and stored_workspace:
        if active_workspace != stored_workspace:
            raise RuntimeError(
                f'Cannot update {noun} {service_name!r} from a different '
                'workspace than the one that owns it.')
        return stored_workspace
    try:
        stored_workspace = serve_utils.resolve_service_workspace(
            service_name, service_record, active_workspace)
    except RuntimeError as e:
        raise RuntimeError(f'Cannot safely update legacy {noun} '
                           f'{service_name!r}: {e}') from e
    if active_workspace != stored_workspace:
        raise RuntimeError(
            f'Cannot update {noun} {service_name!r} from a different '
            'workspace than the one that owns it.')
    return stored_workspace


def _maybe_display_run_warning(task: 'task_lib.Task') -> None:
    # We do not block the user from creating a pool with a run section
    # in order to enable using the same yaml for pool creation
    # and job submission. But we want to make it clear that 'run' will not
    # be respected here.
    if task.run is not None:
        logger.warning(
            f'{colorama.Fore.YELLOW} Pool creation does not support the '
            '`run` section. Creating the pool while ignoring the '
            f'`run` section.{colorama.Style.RESET_ALL}')


def _validate_service_name(service_name: str, pool: bool) -> None:
    """Validate every downstream use of a service/pool name."""
    noun = 'pool' if pool else 'service'
    capnoun = noun.capitalize()
    if re.fullmatch(constants.CLUSTER_NAME_VALID_REGEX, service_name) is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'{capnoun} name {service_name!r} is invalid: '
                             f'ensure it is fully matched by regex (e.g., '
                             'only contains lower letters, numbers and dash): '
                             f'{constants.CLUSTER_NAME_VALID_REGEX}')
    # External-LB objects retain the service name as an ownership label so the
    # orphan reaper can map them back to the DB row. Kubernetes label values
    # are capped at 63 characters; reject before mounts/provisioning rather
    # than letting Deployment creation fail late.
    if not pool and len(service_name) > _KUBERNETES_LABEL_VALUE_MAX_LENGTH:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                f'Service name {service_name!r} is too long for the external '
                'SkyServe load balancer: use at most '
                f'{_KUBERNETES_LABEL_VALUE_MAX_LENGTH} characters.')


def _require_supported_service_topology(task: 'task_lib.Task',
                                        pool: bool) -> None:
    """Reject unsupported service topologies before any provisioning work.

    Pools have no inference endpoint and therefore do not need a load
    balancer. Real services have one supported topology: an in-cluster,
    consolidated controller plus a per-service external Kubernetes LB.
    """
    if pool:
        if (serve_utils.is_external_load_balancer_mode() and
                not serve_utils.is_consolidation_mode(pool=True)):
            raise RuntimeError(
                'The external-only deployment requires '
                'jobs.controller.consolidation_mode=true for pools so replica '
                'launch ownership can be validated by the API-server DB.')
        return
    service_spec = task.service
    assert service_spec is not None
    serve_utils.validate_logical_replica_task(task)
    serve_utils.validate_external_lb_service_spec(service_spec)
    # Validate the platform (Kubernetes identity, stable proxy address, and
    # all three projected auth rings) before syncing mounts or launching a
    # dedicated controller VM that can never host the external-only topology.
    lb_k8s.require_external_lb_runtime()
    if not serve_utils.is_consolidation_mode(pool=False):
        raise RuntimeError(
            'External-only SkyServe requires '
            'serve.controller.consolidation_mode=true so the controller runs '
            'inside the Kubernetes API-server pod; dedicated controller VMs '
            'are no longer supported.')


def up(
    task: 'task_lib.Task',
    service_name: str | None = None,
    pool: bool = False,
    submitted_yaml_content: str | None = None,
) -> tuple[str, str]:
    """Spins up a service or pool under the cross-pod name lifecycle lock."""
    if not pool and maintenance.is_controller_hold_active():
        raise RuntimeError(
            'SkyServe creation is disabled while the server controller hold '
            'is active.')
    if service_name is None:
        service_name = serve_utils.generate_service_name(pool)
    lifecycle_lock = serve_utils.get_service_lifecycle_lock(service_name)
    with lifecycle_lock:
        return _up_impl(task, service_name, pool, lifecycle_lock,
                        submitted_yaml_content)


def _up_impl(task: 'task_lib.Task',
             service_name: str,
             pool: bool,
             lifecycle_lock: Any,
             submitted_yaml_content: str | None = None) -> tuple[str, str]:
    """Run up and eagerly clean only this operation's uncommitted storage."""
    lifecycle_epoch = serve_utils.get_service_lifecycle_epoch(lifecycle_lock)
    try:
        return _up_impl_body(task, service_name, pool, lifecycle_lock,
                             submitted_yaml_content)
    except BaseException:
        # Non-consolidated pools keep their authoritative Serve DB on the
        # remote jobs controller. Never interpret API-local intents as owners
        # of resources committed in that other database.
        if serve_utils.is_consolidation_mode(pool):
            _cleanup_provisional_storage_intents(service_name, lifecycle_epoch,
                                                 lifecycle_lock)
        raise


def _up_impl_body(task: 'task_lib.Task',
                  service_name: str,
                  pool: bool,
                  lifecycle_lock: Any,
                  submitted_yaml_content: str | None = None) -> tuple[str, str]:
    """Spins up a service or a pool."""

    def _assert_lifecycle_lock(phase: str) -> None:
        if not serve_utils.lifecycle_lock_is_valid(lifecycle_lock):
            raise RuntimeError(f'Lost lifecycle ownership while {phase} for '
                               f'{service_name!r}; retry creation.')

    _assert_lifecycle_lock('starting creation')
    consolidation_mode = serve_utils.is_consolidation_mode(pool)
    if (consolidation_mode and
            serve_state.get_service_hash(service_name) is not None):
        noun = 'pool' if pool else 'service'
        raise RuntimeError(f'{noun.capitalize()} {service_name!r} already '
                           'exists; choose a new name or update it.')
    if (consolidation_mode and service_name
            in serve_state.get_orphaned_service_child_names([service_name])):
        predecessor_pool = serve_state.get_orphaned_service_child_mode(
            service_name)
        if predecessor_pool is None:
            purge_guidance = ('inspect its mixed or unreadable predecessor '
                              'metadata and purge it in the original mode')
        else:
            purge_cmd = (f'sky jobs pool down {service_name} --purge'
                         if predecessor_pool else
                         f'sky serve down {service_name} --purge')
            purge_guidance = f'run `{purge_cmd}`'
        raise RuntimeError(
            f'Cannot safely reuse {service_name!r}: predecessor cleanup '
            f'inventory still exists. Please {purge_guidance} before retrying.')
    # Allocate the incarnation before creating *any* file or external child.
    # The same value becomes the durable DB hash and every child-resource
    # namespace, so work already in flight after lock loss remains confined to
    # this incarnation and cannot collide with a same-name successor.
    service_incarnation = str(uuid.uuid4())
    resource_scope = service_incarnation
    lifecycle_epoch = serve_utils.get_service_lifecycle_epoch(lifecycle_lock)
    controller_lifecycle_epoch = (lifecycle_epoch
                                  if consolidation_mode else None)
    task.validate(
        skip_file_mounts=(
            runtime_profile.guarded_ha_ephemeral_artifacts_enabled()),
        skip_workdir=(runtime_profile.guarded_ha_ephemeral_artifacts_enabled()))
    serve_utils.validate_service_task(task, pool=pool)
    assert task.service is not None
    assert task.service.pool == pool, 'Inconsistent pool flag.'
    noun = 'pool' if pool else 'service'
    # The name becomes a controller/replica cluster name and, for services, a
    # Kubernetes LB ownership label.
    _validate_service_name(service_name, pool)

    dag = dag_utils.convert_entrypoint_to_dag(task)
    # Always apply the policy again here, even though it might have been applied
    # in the CLI. This is to ensure that we apply the policy to the final DAG
    # and get the mutated config.
    dag, mutated_user_config = admin_policy_utils.apply(
        dag, request_name=request_names.AdminPolicyRequestName.SERVE_UP)
    _validate_guarded_ha_task_inputs(dag)
    task = dag.tasks[0]
    task.validate()
    assert task.service is not None
    service_workspace = (skypilot_config.get_active_workspace() or
                         constants.SKYPILOT_DEFAULT_WORKSPACE)
    serve_utils.snapshot_service_container_images(task,
                                                  workspace=service_workspace)
    _require_supported_service_topology(task, pool)
    dag.resolve_and_validate_volumes()
    dag.pre_mount_volumes()
    if pool:
        _maybe_display_run_warning(task)
        # Use dummy run script for pool.
        task.run = serve_constants.POOL_DUMMY_RUN_COMMAND

    (storage_scope_id, storage_generation,
     unowned_remote_storage_mounts) = (_prepare_scoped_ephemeral_storage(
         task, resource_scope))

    def _persist_storage_intent(prepared_task: 'task_lib.Task') -> None:
        _record_scoped_ephemeral_storage(prepared_task, resource_scope,
                                         storage_scope_id, storage_generation,
                                         unowned_remote_storage_mounts)
        if not consolidation_mode:
            return
        _persist_scoped_ephemeral_storage_intent(prepared_task,
                                                 service_name,
                                                 resource_scope,
                                                 storage_generation,
                                                 pool,
                                                 lifecycle_epoch,
                                                 provisional=True)

    with rich_utils.safe_status(
            ux_utils.spinner_message(f'Initializing {noun}')):
        # Handle file mounts using two-hop approach when cloud storage
        # unavailable
        storage_clouds = (
            storage_lib.get_cached_enabled_storage_cloud_names_or_refresh())
        force_disable_cloud_bucket = skypilot_config.get_nested(
            ('serve', 'force_disable_cloud_bucket'), False)
        if storage_clouds and not force_disable_cloud_bucket:
            controller_utils.maybe_translate_local_file_mounts_and_sync_up(
                task,
                task_type='serve',
                run_id=storage_scope_id,
                on_storage_mounts_prepared=_persist_storage_intent)
            local_to_controller_file_mounts = {}
        else:
            # Fall back to two-hop file_mount uploading when no cloud storage
            if task.storage_mounts:
                raise exceptions.NotSupportedError(
                    'Cloud-based file_mounts are specified, but no cloud '
                    'storage is available. Please specify local '
                    'file_mounts only.')
            local_to_controller_file_mounts = (
                controller_utils.translate_local_file_mounts_to_two_hop(
                    task, run_id=storage_scope_id))
    # Refresh the pre-upload record with resolved store handles. On a crash
    # inside sync, the earlier deterministic manifest remains available.
    _persist_storage_intent(task)

    with tempfile.NamedTemporaryFile(
            prefix=f'service-task-{service_name}-',
            mode='w',
    ) as service_file, tempfile.NamedTemporaryFile(
            prefix=f'submitted-service-task-{service_name}-',
            mode='w',
    ) as submitted_service_file, tempfile.NamedTemporaryFile(
            prefix=f'controller-task-{service_name}-',
            mode='w',
    ) as controller_file:
        controller = controller_utils.get_controller_for_pool(pool)
        controller_name = controller.value.cluster_name
        task_config = task.to_yaml_config()
        yaml_utils.dump_yaml(service_file.name, task_config)
        if submitted_yaml_content is not None:
            submitted_service_file.write(submitted_yaml_content)
            submitted_service_file.flush()
        remote_tmp_task_yaml_path = (
            serve_utils.generate_remote_tmp_task_yaml_file_name(
                service_name, resource_scope))
        remote_submitted_task_yaml_path = (
            serve_utils.generate_remote_tmp_submitted_task_yaml_file_name(
                service_name, resource_scope)
            if submitted_yaml_content is not None else None)
        remote_config_yaml_path = (
            serve_utils.generate_remote_config_yaml_file_name(
                service_name, resource_scope))
        controller_log_file = (
            serve_utils.generate_remote_controller_log_file_name(
                service_name, resource_scope))
        controller_resources = controller_utils.get_controller_resources(
            controller=controller, task_resources=task.resources)
        controller_user_config = controller_utils.controller_config_snapshot(
            mutated_user_config, workspace=service_workspace)
        controller_user_config['active_workspace'] = service_workspace
        controller_job_id = None
        if serve_utils.is_consolidation_mode(pool):
            # We need a unique integer per sky.serve.up call to avoid name
            # conflict. Originally in non-consolidation mode, this is the ray
            # job id; now we use the request id hash instead. Here we also
            # make sure it is a 32-bit integer to avoid overflow on sqlalchemy.
            rid = common_utils.get_current_request_id()
            controller_job_id = hash(uuid.UUID(rid).int) & 0x7FFFFFFF

        vars_to_fill: dict[str, Any] = {
            'remote_task_yaml_path': remote_tmp_task_yaml_path,
            'local_task_yaml_path': service_file.name,
            'remote_submitted_task_yaml_path': remote_submitted_task_yaml_path,
            'local_submitted_task_yaml_path':
                (submitted_service_file.name
                 if submitted_yaml_content is not None else None),
            'service_name': service_name,
            'service_incarnation': service_incarnation,
            'created_by': shlex.quote(common_utils.get_current_user_name()),
            'lifecycle_epoch': controller_lifecycle_epoch,
            'controller_log_file': controller_log_file,
            'remote_user_config_path': remote_config_yaml_path,
            'local_to_controller_file_mounts': local_to_controller_file_mounts,
            'modified_catalogs':
                service_catalog_common.get_modified_catalog_file_mounts(),
            'consolidation_mode_job_id': controller_job_id,
            'entrypoint': shlex.quote(common_utils.get_current_command()),
            'workspace': shlex.quote(service_workspace),
            **controller_utils.shared_controller_vars_to_fill(
                controller=controller_utils.Controllers.SKY_SERVE_CONTROLLER,
                remote_user_config_path=remote_config_yaml_path,
                local_user_config=controller_user_config,
            ),
        }
        catalog_authority = (
            container_image_catalog_state.get_catalog_authority_id())
        vars_to_fill['controller_envs'][
            constants.CONTAINER_IMAGE_CATALOG_AUTHORITY_ENV_VAR] = (
                catalog_authority)
        common_utils.fill_template(serve_constants.CONTROLLER_TEMPLATE,
                                   vars_to_fill,
                                   output_path=controller_file.name)
        controller_task = task_lib.Task.from_yaml(controller_file.name)
        controller_task.set_resources(controller_resources)

        # # Set service_name so the backend will know to modify default ray
        # task CPU usage to custom value instead of default 0.5 vCPU. We need
        # to set it to a smaller value to support a larger number of services.
        controller_task.service_name = service_name

        # We directly submit the request to the controller and let the
        # controller to check name conflict. Suppose we have multiple
        # sky.serve.up() with same service name, the first one will
        # successfully write its job id to controller service database;
        # and for all following sky.serve.up(), the controller will throw
        # an exception (name conflict detected) and exit. Therefore the
        # controller job id in database could be use as an indicator of
        # whether the service is already running. If the id is the same
        # with the current job id, we know the service is up and running
        # for the first time; otherwise it is a name conflict.
        # Since the controller may be shared among multiple users, launch the
        # controller with the API server's user hash.
        if not serve_utils.is_consolidation_mode(pool):
            print(f'{colorama.Fore.YELLOW}Launching controller for '
                  f'{service_name!r}...{colorama.Style.RESET_ALL}')
            with common.with_server_user(
            ), skypilot_config.local_active_workspace_ctx(
                    constants.SKYPILOT_DEFAULT_WORKSPACE
            ), (
                    # Serve controller is not placed in kueue, as the controller
                    # pod is considered a "system" pod and is not subject to
                    # queue limits or preemption.
                    skypilot_config.remove_queue_name_from_config()):
                _assert_lifecycle_lock('launching the controller')
                controller_job_id, controller_handle = execution.launch(
                    task=controller_task,
                    cluster_name=controller_name,
                    retry_until_up=True,
                    _request_name=request_names.AdminPolicyRequestName.
                    SERVE_LAUNCH_CONTROLLER,
                    _disable_controller_check=True,
                )
        else:
            controller_type = controller_utils.get_controller_for_pool(pool)
            controller_handle = backend_utils.is_controller_accessible(
                controller=controller_type, stopped_message='')
            backend = backend_utils.get_backend_from_handle(controller_handle)
            assert isinstance(backend, backends.CloudVmRayBackend)
            _assert_lifecycle_lock('syncing controller files')
            backend.sync_file_mounts(
                handle=controller_handle,
                all_file_mounts=controller_task.file_mounts,
                storage_mounts=controller_task.storage_mounts)
            run_script = controller_task.run
            assert isinstance(run_script, str)
            # Manually add the env variables to the run script. Originally
            # this is done in ray jobs submission but now we have to do it
            # manually because there is no ray runtime on the API server.
            env_cmds = [
                f'export {k}={v!r}' for k, v in controller_task.envs.items()
            ]
            # Config bytes are persisted per immutable version by `_start` and
            # restored from PostgreSQL before this script is executed. Keeping
            # them out of the shell command avoids credentials in command logs
            # and the operating system's argv-size limit.
            run_script = serve_utils.strip_legacy_ha_recovery_config_payload(
                '\n'.join(env_cmds + [run_script]), remote_config_yaml_path)
            # Dump script for high availability recovery.
            _assert_lifecycle_lock('publishing the recovery script')
            if not serve_state.set_ha_recovery_script(
                    service_name,
                    run_script,
                    expected_lifecycle_epoch=lifecycle_epoch):
                raise RuntimeError(
                    f'Lost lifecycle ownership while publishing recovery '
                    f'script for {service_name!r}.')
            self_pod_ip_dbg = os.environ.get('POD_IP', '<unset>')
            logger.debug(f'Serve up() run_on_head: spawning controller '
                         f'subprocess locally on {self_pod_ip_dbg}')
            # LocalProcessCommandRunner (used for consolidation-mode spawns)
            # supplies a clean server env to the subprocess so per-request
            # env pollution doesn't leak into the long-lived serve
            # controller. See LocalProcessCommandRunner.run for details.
            _assert_lifecycle_lock('spawning the controller')
            backend.run_on_head(controller_handle, run_script)

        style = colorama.Style
        fore = colorama.Fore

        assert controller_job_id is not None and controller_handle is not None
        assert isinstance(controller_handle, backends.CloudVmRayResourceHandle)
        backend = backend_utils.get_backend_from_handle(controller_handle)
        assert isinstance(backend, backends.CloudVmRayBackend)
        # TODO(tian): Cache endpoint locally to speedup. Endpoint won't
        # change after the first time, so there is no consistency issue.
        try:
            with rich_utils.safe_status(
                    ux_utils.spinner_message(
                        f'Waiting for the {noun} to register')):
                _assert_lifecycle_lock('waiting for registration')
                # This checks the controller job id in the database and waits
                # for the historical registration-port sentinel. The actual
                # endpoint is always derived from the Kubernetes LB Service.
                _wait_for_service_registration(controller_handle, backend,
                                               service_name, controller_job_id,
                                               pool, resource_scope)
        except (exceptions.CommandError, grpc.FutureTimeoutError,
                grpc.RpcError):
            if serve_utils.is_consolidation_mode(pool):
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        f'Failed to wait for {noun} initialization. '
                        'Please check the logs above for more details.'
                    ) from None
            statuses = backend.get_job_status(controller_handle,
                                              [controller_job_id],
                                              stream_logs=False)
            controller_job_status = list(statuses.values())[0]
            if controller_job_status == job_lib.JobStatus.PENDING:
                # Max number of services reached due to vCPU constraint.
                # The controller job is pending due to ray job scheduling.
                # We manually cancel the job here.
                backend.cancel_jobs(controller_handle, [controller_job_id])
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        controller_utils.get_max_services_error_message(
                            pool)) from None
            else:
                # Possible cases:
                # (1) name conflict;
                # (2) max number of services reached due to memory
                # constraint. The job will successfully run on the
                # controller, but there will be an error thrown due
                # to memory constraint check in the controller.
                # See sky/serve/service.py for more details.
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        'Failed to spin up the service. Please '
                        'check the logs above for more details.') from None
        else:
            _assert_lifecycle_lock('completing registration')
            # Pools have no inference endpoint. Keep returning a string for
            # the internal up() compatibility contract; apply() discards it.
            endpoint = ''
            if not pool:
                # The registration port remains a DB/API readiness sentinel,
                # but does not
                # participate in endpoint construction: every external LB
                # Service exposes the fixed Kubernetes Service port.
                external_endpoint = _external_service_endpoint_url(
                    service_name, resource_scope)
                if external_endpoint is None:
                    raise RuntimeError(
                        'The external load balancer endpoint is unavailable '
                        f'for service {service_name!r}. The per-service '
                        'Kubernetes Service is the only supported inference '
                        'endpoint; check the API server external-LB runtime '
                        'configuration.')
                endpoint = external_endpoint

        if pool:
            logger.info(
                f'{fore.CYAN}Pool name: '
                f'{style.BRIGHT}{service_name}{style.RESET_ALL}'
                f'\n📋 Useful Commands'
                f'\n{ux_utils.INDENT_SYMBOL}To submit jobs to the pool:\t\t'
                f'{ux_utils.BOLD}sky jobs launch --pool {service_name} '
                f'<yaml_file>{ux_utils.RESET_BOLD}'
                f'\n{ux_utils.INDENT_SYMBOL}To submit multiple jobs:\t\t'
                f'{ux_utils.BOLD}sky jobs launch --pool {service_name} '
                f'--num-jobs 10 <yaml_file>{ux_utils.RESET_BOLD}'
                f'\n{ux_utils.INDENT_SYMBOL}To check the pool status:\t\t'
                f'{ux_utils.BOLD}sky jobs pool status {service_name}'
                f'{ux_utils.RESET_BOLD}'
                f'\n{ux_utils.INDENT_SYMBOL}To terminate the pool:\t\t'
                f'{ux_utils.BOLD}sky jobs pool down {service_name}'
                f'{ux_utils.RESET_BOLD}'
                f'\n{ux_utils.INDENT_LAST_SYMBOL}'
                'To update the number of workers:\t'
                f'{ux_utils.BOLD}sky jobs pool apply --pool {service_name} '
                f'--workers 5{ux_utils.RESET_BOLD}'
                '\n\n' + ux_utils.finishing_message('Successfully created pool '
                                                    f'{service_name!r}.'))
        else:
            test_request_command = _service_test_request_command(endpoint)
            logger.info(
                f'{fore.CYAN}Service name: '
                f'{style.BRIGHT}{service_name}{style.RESET_ALL}'
                f'\n{fore.CYAN}Endpoint URL: '
                f'{style.BRIGHT}{endpoint}{style.RESET_ALL}'
                f'\n📋 Useful Commands'
                f'\n{ux_utils.INDENT_SYMBOL}To check service status:\t'
                f'{ux_utils.BOLD}sky serve status {service_name} '
                f'[--endpoint]{ux_utils.RESET_BOLD}'
                f'\n{ux_utils.INDENT_SYMBOL}To teardown the service:\t'
                f'{ux_utils.BOLD}sky serve down {service_name}'
                f'{ux_utils.RESET_BOLD}'
                f'\n{ux_utils.INDENT_SYMBOL}To see replica logs:\t'
                f'{ux_utils.BOLD}sky serve logs {service_name} [REPLICA_ID]'
                f'{ux_utils.RESET_BOLD}'
                f'\n{ux_utils.INDENT_SYMBOL}To see load balancer logs:\t'
                f'{ux_utils.BOLD}sky serve logs --load-balancer {service_name}'
                f'{ux_utils.RESET_BOLD}'
                f'\n{ux_utils.INDENT_SYMBOL}To see controller logs:\t'
                f'{ux_utils.BOLD}sky serve logs --controller {service_name}'
                f'{ux_utils.RESET_BOLD}'
                f'\n{ux_utils.INDENT_SYMBOL}To monitor the status:\t'
                f'{ux_utils.BOLD}watch -n10 sky serve status {service_name}'
                f'{ux_utils.RESET_BOLD}'
                f'\n{ux_utils.INDENT_LAST_SYMBOL}To send a test request:\t'
                f'{ux_utils.BOLD}{test_request_command}'
                f'{ux_utils.RESET_BOLD}'
                '\n\n' + ux_utils.finishing_message(
                    'Service is spinning up and replicas '
                    'will be ready shortly.'))
        return service_name, endpoint


def update(
    task: Optional['task_lib.Task'],
    service_name: str,
    mode: serve_utils.UpdateMode = serve_utils.DEFAULT_UPDATE_MODE,
    pool: bool = False,
    workers: int | None = None,
    submitted_yaml_content: str | None = None,
) -> None:
    """Updates an existing service or pool."""
    if not pool and maintenance.is_controller_hold_active():
        raise RuntimeError(
            'SkyServe updates are disabled while the server controller hold '
            'is active.')
    # The lifecycle lock is cross-pod on PostgreSQL and lives outside the
    # service directory. Keep the legacy local lock outermost to match named
    # down's local-lock -> controller-purge lifecycle-lock order; reversing the
    # order can deadlock. The lifecycle lock remains the actual HA boundary.
    with filelock.FileLock(serve_utils.get_service_filelock_path(service_name)):
        lifecycle_lock = serve_utils.get_service_lifecycle_lock(
            service_name, advance_epoch=False)
        with lifecycle_lock:
            _update_impl(task,
                         service_name,
                         mode,
                         pool,
                         workers,
                         lifecycle_lock=lifecycle_lock,
                         submitted_yaml_content=submitted_yaml_content)


def elect_version(service_name: str, version: int, expected_service_hash: str,
                  expected_elected_version: int | None) -> None:
    """Create a new rollout generation from an immutable stored version."""
    if maintenance.is_controller_hold_active():
        raise RuntimeError(
            'SkyServe version election is disabled while the server '
            'controller hold is active.')
    with filelock.FileLock(serve_utils.get_service_filelock_path(service_name)):
        lifecycle_lock = serve_utils.get_service_lifecycle_lock(
            service_name, advance_epoch=False)
        with lifecycle_lock:
            record = serve_state.get_service_from_name(service_name)
            if (record is None or record.get('hash') != expected_service_hash or
                    record.get('pool')):
                raise RuntimeError(
                    f'Service {service_name!r} changed before version '
                    f'{version} could be elected. Refresh and try again.')
            elected_version = record.get('elected_version')
            if elected_version != expected_elected_version:
                raise RuntimeError(
                    f'Service {service_name!r} changed before version '
                    f'{version} could be elected. Refresh and try again.')
            if elected_version == version:
                raise ValueError(
                    f'Service {service_name!r} already has version {version} '
                    'elected.')
            yaml_content = serve_state.get_yaml_content(service_name, version)
            if yaml_content is None:
                raise ValueError(
                    f'Committed version {version} does not exist for service '
                    f'{service_name!r}.')
            task = task_lib.Task.from_yaml_str(yaml_content)
            submitted_yaml_content = serve_state.get_submitted_yaml_content(
                service_name, version)
            _update_impl(task,
                         service_name,
                         serve_utils.UpdateMode.ROLLING,
                         pool=False,
                         lifecycle_lock=lifecycle_lock,
                         reuse_task_storage_scope=True,
                         submitted_yaml_content=submitted_yaml_content)


def set_load_balancer_high_availability(service_name: str, enabled: bool,
                                        expected_service_hash: str) -> None:
    """Change only one service's external-LB topology under lifecycle lock."""
    if maintenance.is_controller_hold_active():
        raise RuntimeError(
            'SkyServe load balancer topology changes are disabled while the '
            'server controller hold is active.')
    with filelock.FileLock(serve_utils.get_service_filelock_path(service_name)):
        lifecycle_lock = serve_utils.get_service_lifecycle_lock(
            service_name, advance_epoch=False)
        with lifecycle_lock:
            record = serve_state.get_service_from_name(service_name)
            if (record is None or record.get('pool') or
                    record.get('hash') != expected_service_hash):
                raise RuntimeError(
                    f'Service {service_name!r} changed before its load '
                    'balancer topology could be updated.')
            service_status = record.get('status')
            if (not isinstance(service_status, serve_state.ServiceStatus) or
                    service_status
                    in serve_state.ServiceStatus.terminal_statuses() or
                    service_status
                    == serve_state.ServiceStatus.CONTROLLER_INIT):
                status_text = (service_status.value if isinstance(
                    service_status, serve_state.ServiceStatus) else
                               str(service_status))
                raise RuntimeError(
                    f'Service {service_name!r} is not ready for a load '
                    f'balancer topology change (status={status_text}).')
            if not serve_utils.is_consolidation_mode(pool=False):
                raise RuntimeError(
                    'External load balancer topology changes require '
                    'consolidation mode.')
            if not serve_utils.lifecycle_lock_is_valid(lifecycle_lock):
                raise RuntimeError(
                    f'Lost lifecycle ownership before updating the load '
                    f'balancer for {service_name!r}.')
            serve_utils.set_load_balancer_high_availability_encoded(
                service_name,
                enabled,
                expected_service_hash=expected_service_hash,
                expected_lifecycle_epoch=(
                    serve_utils.get_service_lifecycle_epoch(lifecycle_lock)))


def _assert_service_update_fence(service_name: str, pool: bool,
                                 handle: 'backends.CloudVmRayResourceHandle',
                                 backend: 'backends.CloudVmRayBackend',
                                 expected_service_hash: str,
                                 lifecycle_lock: Any,
                                 phase: str) -> dict[str, Any]:
    """Revalidate one update before a name-scoped external mutation."""
    if not serve_utils.lifecycle_lock_is_valid(lifecycle_lock):
        raise RuntimeError(f'Lost lifecycle ownership while {phase} for '
                           f'{service_name!r}; retry the update.')
    current = _get_service_record(service_name, pool, handle, backend)
    if (current is None or current.get('hash') != expected_service_hash):
        raise RuntimeError(f'Service {service_name!r} changed incarnation '
                           f'while {phase}; retry against the current service.')
    _require_service_update_workspace(current, service_name,
                                      'pool' if pool else 'service')
    service_status = current['status']
    if service_status in serve_state.ServiceStatus.terminal_statuses():
        raise RuntimeError(f'Service {service_name!r} entered terminal status '
                           f'{service_status.value} while {phase}; clean it '
                           'up and '
                           'retry.')
    return current


def _update_impl(
    task: Optional['task_lib.Task'],
    service_name: str,
    mode: serve_utils.UpdateMode = serve_utils.DEFAULT_UPDATE_MODE,
    pool: bool = False,
    workers: int | None = None,
    lifecycle_lock: Any | None = None,
    reuse_task_storage_scope: bool = False,
    submitted_yaml_content: str | None = None,
) -> None:
    """Run update and eagerly clean only uncommitted storage generations."""
    if lifecycle_lock is None:
        raise RuntimeError('Service update requires lifecycle ownership.')
    lifecycle_epoch = serve_utils.get_service_lifecycle_epoch(lifecycle_lock)
    try:
        _update_impl_body(task, service_name, mode, pool, workers,
                          lifecycle_lock, reuse_task_storage_scope,
                          submitted_yaml_content)
    except BaseException:
        if serve_utils.is_consolidation_mode(pool):
            _cleanup_provisional_storage_intents(service_name, lifecycle_epoch,
                                                 lifecycle_lock)
        raise


def _update_impl_body(
    task: Optional['task_lib.Task'],
    service_name: str,
    mode: serve_utils.UpdateMode = serve_utils.DEFAULT_UPDATE_MODE,
    pool: bool = False,
    workers: int | None = None,
    lifecycle_lock: Any | None = None,
    reuse_task_storage_scope: bool = False,
    submitted_yaml_content: str | None = None,
) -> None:
    noun = 'pool' if pool else 'service'
    capnoun = noun.capitalize()

    controller_type = controller_utils.get_controller_for_pool(pool)
    handle = backend_utils.is_controller_accessible(
        controller=controller_type,
        stopped_message=
        'Service controller is stopped. There is no service to update. '
        f'To spin up a new service, use {ux_utils.BOLD}'
        f'sky serve up{ux_utils.RESET_BOLD}',
        non_existent_message='Service does not exist. '
        'To spin up a new service, '
        f'use {ux_utils.BOLD}sky serve up{ux_utils.RESET_BOLD}',
    )

    assert isinstance(handle, backends.CloudVmRayResourceHandle)
    backend = backend_utils.get_backend_from_handle(handle)
    assert isinstance(backend, backends.CloudVmRayBackend)

    service_record = _get_service_record(service_name,
                                         pool,
                                         handle,
                                         backend,
                                         include_yaml=task is None and
                                         workers is not None and pool)

    if service_record is None:
        cmd = 'sky jobs pool up' if pool else 'sky serve up'
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(f'Cannot find {noun} {service_name!r}.'
                               f'To spin up a {noun}, use {ux_utils.BOLD}'
                               f'{cmd}{ux_utils.RESET_BOLD}')
    expected_service_hash = service_record.get('hash')
    if not isinstance(expected_service_hash, str) or not expected_service_hash:
        raise RuntimeError(f'Cannot safely update {noun} {service_name!r} '
                           'without a durable service incarnation.')
    service_workspace = _require_service_update_workspace(
        service_record, service_name, noun)
    if lifecycle_lock is None:
        raise RuntimeError('Service update requires lifecycle ownership.')
    lifecycle_epoch = serve_utils.get_service_lifecycle_epoch(lifecycle_lock)
    consolidation_mode = serve_utils.is_consolidation_mode(pool)

    reuse_existing_storage_scope = task is None or reuse_task_storage_scope
    # If task is None and workers is specified, load existing configuration
    # and update replica count.
    if task is None:
        if workers is None:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    f'Cannot update {noun} without specifying '
                    f'task or workers. Please provide either a task '
                    f'or specify the number of workers.')

        if not pool:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'Non-pool service, trying to update replicas to '
                    f'{workers} is not supported. Ignoring the update.')

        # Load the existing task configuration from the service's YAML file
        yaml_content = service_record['yaml_content']

        # Load the existing task configuration
        task = task_lib.Task.from_yaml_str(yaml_content)

        if task.service is None:
            with ux_utils.print_exception_no_traceback():
                raise RuntimeError('No service configuration found in '
                                   f'existing {noun} {service_name!r}')
        task.set_service(task.service.copy(min_replicas=workers))

    task.validate(
        skip_file_mounts=(
            runtime_profile.guarded_ha_ephemeral_artifacts_enabled()),
        skip_workdir=(runtime_profile.guarded_ha_ephemeral_artifacts_enabled()))
    serve_utils.validate_service_task(task, pool=pool)

    # Now apply the policy and handle task-specific logic
    # Always apply the policy again here, even though it might have been applied
    # in the CLI. This is to ensure that we apply the policy to the final DAG
    # and get the mutated config.
    dag, mutated_user_config = admin_policy_utils.apply(
        task, request_name=request_names.AdminPolicyRequestName.SERVE_UPDATE)
    _validate_guarded_ha_task_inputs(dag)
    task = dag.tasks[0]
    task.validate()
    requested_config_workspace = mutated_user_config.get('active_workspace')
    if (requested_config_workspace is not None and
            requested_config_workspace != service_workspace):
        raise RuntimeError(
            f'Admin policy changed active_workspace while updating {noun} '
            f'{service_name!r}; expected the owning workspace '
            f'{service_workspace!r}, got {requested_config_workspace!r}.')
    controller_config_bytes: bytes | None = None
    controller_config_digest: str | None = None
    controller_config_snapshot_id: str | None = None
    if consolidation_mode:
        controller_config = controller_utils.controller_config_snapshot(
            mutated_user_config, workspace=service_workspace)
        controller_config['active_workspace'] = service_workspace
        controller_config_bytes = yaml_utils.dump_yaml_str(
            controller_config).encode('utf-8')
        # Fail before storage preparation, file sync, or version allocation.
        # The controller independently repeats this check on exact bytes.
        serve_utils.sanitize_ha_recovery_config_bytes(controller_config_bytes)
        controller_config_digest = hashlib.sha256(
            controller_config_bytes).hexdigest()
        controller_config_snapshot_id = secrets.token_hex(32)
    serve_utils.snapshot_service_container_images(task,
                                                  workspace=service_workspace)
    if pool:
        _maybe_display_run_warning(task)
        # Use dummy run script for pool.
        task.run = serve_constants.POOL_DUMMY_RUN_COMMAND

    assert task.service is not None
    _require_supported_service_topology(task, pool)

    prompt = None
    service_status = service_record['status']
    if service_status in serve_state.ServiceStatus.terminal_statuses():
        prompt = (f'{capnoun} {service_name!r} is in terminal status '
                  f'{service_status.value}. Please clean up the {noun} and '
                  'try again.')
    elif service_status == serve_state.ServiceStatus.CONTROLLER_INIT:
        prompt = (f'{capnoun} {service_name!r} is still initializing '
                  'its controller. Please try again later.')
    if prompt is not None:
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(prompt)

    _assert_service_update_fence(service_name, pool, handle, backend,
                                 expected_service_hash, lifecycle_lock,
                                 'preparing the update')
    if consolidation_mode:
        # An old controller would ignore the snapshot field and could still
        # commit the task. Reject before allocating a version or syncing files
        # while an API-server rollout is mixed.
        serve_utils.require_update_config_snapshot_capability(
            service_name, expected_service_hash)

    resource_scope = service_record.get('resource_scope')
    storage_scope_id: str | None = None
    storage_generation: str | None = None
    unowned_remote_storage_mounts: set[str] = set()
    if isinstance(resource_scope, str) and resource_scope:
        (storage_scope_id, storage_generation,
         unowned_remote_storage_mounts) = (_prepare_scoped_ephemeral_storage(
             task,
             resource_scope,
             reuse_existing_scope=reuse_existing_storage_scope))

    def _persist_storage_intent(prepared_task: 'task_lib.Task') -> None:
        if (storage_scope_id is None or storage_generation is None or
                not isinstance(resource_scope, str)):
            return
        _record_scoped_ephemeral_storage(prepared_task, resource_scope,
                                         storage_scope_id, storage_generation,
                                         unowned_remote_storage_mounts)
        if not consolidation_mode:
            return
        _persist_scoped_ephemeral_storage_intent(
            prepared_task,
            service_name,
            resource_scope,
            storage_generation,
            pool,
            lifecycle_epoch,
            provisional=not reuse_existing_storage_scope)

    with rich_utils.safe_status(
            ux_utils.spinner_message(f'Initializing {noun}')):
        controller_utils.maybe_translate_local_file_mounts_and_sync_up(
            task,
            task_type='serve',
            run_id=storage_scope_id,
            on_storage_mounts_prepared=_persist_storage_intent)
    _persist_storage_intent(task)

    use_legacy = not handle.is_grpc_enabled_with_flag
    current_version = None

    if consolidation_mode:
        # The API pod shares the durable Serve DB with the local controller.
        # Allocate the placeholder directly under the lifecycle epoch instead
        # of sending a name-only skylet RPC that could complete after lock
        # loss and add a version to a same-name successor.
        _assert_service_update_fence(service_name, pool, handle, backend,
                                     expected_service_hash, lifecycle_lock,
                                     'adding a version')
        current_version = serve_state.add_version(
            service_name,
            expected_service_hash=expected_service_hash,
            expected_lifecycle_epoch=serve_utils.get_service_lifecycle_epoch(
                lifecycle_lock),
            created_by=common_utils.get_current_user_name())
    else:
        if not use_legacy:
            _assert_service_update_fence(service_name, pool, handle, backend,
                                         expected_service_hash, lifecycle_lock,
                                         'adding a version')
            try:
                current_version = serve_rpc_utils.RpcRunner.add_version(
                    handle, service_name)
            except exceptions.SkyletMethodNotImplementedError:
                use_legacy = True

        if use_legacy:
            _assert_service_update_fence(service_name, pool, handle, backend,
                                         expected_service_hash, lifecycle_lock,
                                         'adding a version')
            code = serve_utils.ServeCodeGen.add_version(service_name)
            returncode, version_string_payload, stderr = backend.run_on_head(
                handle,
                code,
                require_outputs=True,
                stream_logs=False,
                separate_stderr=True)
            try:
                subprocess_utils.handle_returncode(returncode,
                                                   code,
                                                   'Failed to add version',
                                                   stderr,
                                                   stream_logs=True)
            except exceptions.CommandError as e:
                raise RuntimeError(e.error_msg) from e

            version_string = serve_utils.load_version_string(
                version_string_payload)
            try:
                current_version = int(version_string)
            except ValueError as e:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        f'Failed to parse version: {version_string}; '
                        f'Returncode: {returncode}') from e

    if current_version is None:
        raise RuntimeError(f'Failed to add a version to {service_name!r}.')

    with tempfile.NamedTemporaryFile(
            prefix=f'{service_name}-v{current_version}',
            mode='w') as service_file, tempfile.NamedTemporaryFile(
                prefix=f'{service_name}-submitted-v{current_version}',
                mode='w') as submitted_service_file, \
            tempfile.NamedTemporaryFile(
                prefix=f'{service_name}-config-v{current_version}',
                mode='w') as controller_config_file:
        task_config = task.to_yaml_config()
        yaml_utils.dump_yaml(service_file.name, task_config)
        if controller_config_bytes is not None:
            controller_config_file.write(
                controller_config_bytes.decode('utf-8'))
            controller_config_file.flush()
        should_sync_submitted_yaml = (consolidation_mode and
                                      submitted_yaml_content is not None)
        if should_sync_submitted_yaml:
            assert submitted_yaml_content is not None
            submitted_service_file.write(submitted_yaml_content)
            submitted_service_file.flush()
        remote_task_yaml_path = serve_utils.generate_task_yaml_file_name(
            service_name,
            current_version,
            expand_user=False,
            resource_scope=service_record.get('resource_scope'))
        remote_submitted_task_yaml_path = (
            serve_utils.generate_submitted_task_yaml_file_name(
                service_name,
                current_version,
                expand_user=False,
                resource_scope=service_record.get('resource_scope')))
        staged_config_yaml_path = (
            serve_utils.generate_staged_config_yaml_file_name(
                service_name,
                current_version,
                resource_scope=service_record.get('resource_scope'),
                snapshot_id=controller_config_snapshot_id))

        stage_sync_attempted = False
        submission_started = False
        try:
            with sky_logging.silent():
                _assert_service_update_fence(service_name, pool, handle,
                                             backend, expected_service_hash,
                                             lifecycle_lock,
                                             'syncing the update YAML')
                files_to_sync = {remote_task_yaml_path: service_file.name}
                if should_sync_submitted_yaml:
                    files_to_sync[remote_submitted_task_yaml_path] = (
                        submitted_service_file.name)
                if consolidation_mode:
                    assert controller_config_bytes is not None
                    files_to_sync[staged_config_yaml_path] = (
                        controller_config_file.name)
                    stage_sync_attempted = True
                backend.sync_file_mounts(handle,
                                         files_to_sync,
                                         storage_mounts=None)

            if consolidation_mode:
                assert controller_config_digest is not None
                try:
                    # Consolidated controllers share this API pod's
                    # filesystem. Verify the raw stage in-process so the
                    # source digest never appears in a shell argv or command
                    # log (it may otherwise verify stripped low-entropy
                    # credentials offline).
                    serve_utils.secure_staged_controller_config(
                        staged_config_yaml_path, controller_config_digest)
                except Exception as e:
                    raise RuntimeError(
                        'Failed to secure staged controller config: '
                        f'{common_utils.format_exception(e)}') from e

                assert controller_config_snapshot_id is not None
                # Route directly through the shared Serve DB/controller proxy
                # so the accepted request carries the exact lifecycle epoch.
                _assert_service_update_fence(service_name, pool, handle,
                                             backend, expected_service_hash,
                                             lifecycle_lock,
                                             'submitting the update')
                lifecycle_epoch = (
                    serve_utils.get_service_lifecycle_epoch(lifecycle_lock))
                submission_started = True
                try:
                    serve_utils.update_service_encoded(
                        service_name,
                        current_version,
                        mode=mode.value,
                        pool=pool,
                        expected_service_hash=expected_service_hash,
                        expected_lifecycle_epoch=lifecycle_epoch,
                        has_submitted_yaml=should_sync_submitted_yaml,
                        has_config_snapshot=True,
                        expected_config_snapshot_digest=(
                            controller_config_digest),
                        config_snapshot_id=controller_config_snapshot_id)
                except BaseException:
                    # The POST helper may have lost the final response after a
                    # handler committed. Cleanup is another serialized
                    # controller operation, so it waits behind every replay
                    # and removes only a still-NULL version with this nonce.
                    try:
                        serve_utils.cleanup_staged_config_update_encoded(
                            service_name, expected_service_hash,
                            current_version, lifecycle_epoch,
                            controller_config_snapshot_id)
                    except Exception as cleanup_error:  # pylint: disable=broad-except
                        logger.warning(
                            'Could not confirm staged controller config '
                            f'cleanup for {service_name!r} version '
                            f'{current_version}; preserving it for '
                            'controller-side reconciliation: '
                            f'{common_utils.format_exception(cleanup_error)}')
                    raise
            else:
                use_legacy = not handle.is_grpc_enabled_with_flag

                if not use_legacy:
                    _assert_service_update_fence(service_name, pool, handle,
                                                 backend, expected_service_hash,
                                                 lifecycle_lock,
                                                 'submitting the update')
                    try:
                        serve_rpc_utils.RpcRunner.update_service(
                            handle, service_name, current_version, mode, pool)
                    except exceptions.SkyletMethodNotImplementedError:
                        use_legacy = True

                if use_legacy:
                    _assert_service_update_fence(service_name, pool, handle,
                                                 backend, expected_service_hash,
                                                 lifecycle_lock,
                                                 'submitting the update')
                    code = serve_utils.ServeCodeGen.update_service(
                        service_name,
                        current_version,
                        mode=mode.value,
                        pool=pool)
                    returncode, _, stderr = backend.run_on_head(
                        handle,
                        code,
                        require_outputs=True,
                        stream_logs=False,
                        separate_stderr=True)
                    try:
                        subprocess_utils.handle_returncode(
                            returncode,
                            code,
                            f'Failed to update {noun}s',
                            stderr,
                            stream_logs=True)
                    except exceptions.CommandError as e:
                        raise RuntimeError(e.error_msg) from e
        except BaseException:
            if (consolidation_mode and stage_sync_attempted and
                    not submission_started):
                cleanup_code = (serve_utils.ServeCodeGen.
                                remove_uncommitted_staged_controller_config(
                                    service_name, current_version,
                                    service_record.get('resource_scope'),
                                    controller_config_snapshot_id))
                try:
                    returncode, _, stderr = backend.run_on_head(
                        handle,
                        cleanup_code,
                        require_outputs=True,
                        stream_logs=False,
                        separate_stderr=True)
                    if returncode:
                        logger.warning('Remote staged config cleanup failed '
                                       f'for {service_name!r} version '
                                       f'{current_version}: {stderr}')
                except Exception as cleanup_error:  # pylint: disable=broad-except
                    logger.warning(
                        'Remote staged config cleanup could not '
                        f'run for {service_name!r} version '
                        f'{current_version}: '
                        f'{common_utils.format_exception(cleanup_error)}')
            raise

    cmd = 'sky jobs pool status' if pool else 'sky serve status'
    logger.info(
        f'{colorama.Fore.GREEN}{capnoun} {service_name!r} update scheduled.'
        f'{colorama.Style.RESET_ALL}\n'
        f'Please use {ux_utils.BOLD}{cmd} {service_name} '
        f'{ux_utils.RESET_BOLD}to check the latest status.')

    if pool:
        logs_cmd = f'`sky jobs pool logs {service_name} <worker_id>`'
        unit_noun = 'Workers'

    else:
        logs_cmd = f'`sky serve logs {service_name} <replica_id>`'
        unit_noun = 'Replicas'
    logger.info(
        ux_utils.finishing_message(
            f'Successfully updated {noun} {service_name!r} '
            f'to version {current_version}.',
            follow_up_message=
            f'\n{unit_noun} are updating, use {ux_utils.BOLD}{logs_cmd}'
            f'{ux_utils.RESET_BOLD} to check their status.'))


def apply(
    task: 'task_lib.Task',
    workers: int | None,
    service_name: str,
    mode: serve_utils.UpdateMode = serve_utils.DEFAULT_UPDATE_MODE,
    pool: bool = False,
) -> None:
    """Applies the config to the service or pool."""
    if not pool and maintenance.is_controller_hold_active():
        raise RuntimeError(
            'SkyServe apply is disabled while the server controller hold is '
            'active.')
    with filelock.FileLock(serve_utils.get_service_filelock_path(service_name)):
        # `apply` chooses update versus creation only after reading durable
        # service state. Acquire the name mutex first, then retain the live
        # controller epoch for update or mint a new epoch for creation. A
        # pre-lock existence check would race a same-name operation on another
        # API pod.
        lifecycle_lock = serve_utils.get_service_lifecycle_lock(
            service_name, advance_epoch=None)
        with lifecycle_lock:
            service_exists = serve_state.get_service_hash(service_name)
            if service_exists is None:
                serve_utils.advance_service_lifecycle_epoch(lifecycle_lock)
            else:
                serve_utils.retain_service_lifecycle_epoch(lifecycle_lock)
            try:
                controller_type = controller_utils.get_controller_for_pool(pool)
                handle = backend_utils.is_controller_accessible(
                    controller=controller_type, stopped_message='')
                backend = backend_utils.get_backend_from_handle(handle)
                assert isinstance(backend, backends.CloudVmRayBackend)
                service_record = _get_service_record(service_name, pool, handle,
                                                     backend)
                if service_record is not None:
                    # Refuse update for terminal-state rows
                    # (CONTROLLER_FAILED / FAILED_CLEANUP / SHUTTING_DOWN).
                    # The controller listener may already be gone, so update
                    # would otherwise surface an opaque ECONNREFUSED.
                    svc_status = service_record['status']
                    if svc_status in (
                            serve_state.ServiceStatus.terminal_statuses()):
                        noun = 'pool' if pool else 'service'
                        purge_cmd = (
                            f'sky jobs pool down {service_name} --purge' if pool
                            else f'sky serve down {service_name} --purge')
                        if (svc_status ==
                                serve_state.ServiceStatus.SHUTTING_DOWN):
                            msg = (f'{noun.capitalize()} {service_name!r} is '
                                   'shutting down. Wait for shutdown to '
                                   'complete, then re-apply. If it stays in '
                                   'this state for a long time, the cleanup '
                                   f'may be stuck; run `{purge_cmd}` to '
                                   'force-clean.')
                        else:
                            msg = (f'{noun.capitalize()} {service_name!r} is '
                                   f'in {svc_status.value} state and cannot '
                                   f'be updated. Run `{purge_cmd}` to clean '
                                   'it up and retry.')
                        with ux_utils.print_exception_no_traceback():
                            raise RuntimeError(msg)
                    return _update_impl(task,
                                        service_name,
                                        mode,
                                        pool,
                                        workers,
                                        lifecycle_lock=lifecycle_lock)
            except exceptions.ClusterNotUpError:
                pass
            _up_impl(task, service_name, pool, lifecycle_lock)


def _terminate_services(handle: 'backends.CloudVmRayResourceHandle',
                        service_names: list[str] | None, purge: bool,
                        pool: bool, noun: str) -> str:
    assert isinstance(handle, backends.CloudVmRayResourceHandle)
    use_legacy = not handle.is_grpc_enabled_with_flag
    if not use_legacy:
        try:
            return serve_rpc_utils.RpcRunner.terminate_services(
                handle, service_names, purge, pool)
        except exceptions.SkyletMethodNotImplementedError:
            use_legacy = True

    backend = backend_utils.get_backend_from_handle(handle)
    assert isinstance(backend, backends.CloudVmRayBackend)
    code = serve_utils.ServeCodeGen.terminate_services(service_names, purge,
                                                       pool)

    returncode, stdout, _ = backend.run_on_head(handle,
                                                code,
                                                require_outputs=True,
                                                stream_logs=False)

    subprocess_utils.handle_returncode(returncode, code,
                                       f'Failed to terminate {noun}', stdout)
    return stdout


def down(
    service_names: str | list[str] | None = None,
    all: bool = False,  # pylint: disable=redefined-builtin
    purge: bool = False,
    pool: bool = False,
) -> None:
    """Tears down a service or pool."""
    if not pool and maintenance.is_controller_hold_active():
        raise RuntimeError(
            'SkyServe termination and purge are disabled while the server '
            'controller hold is active.')
    noun = 'pool' if pool else 'service'
    if service_names is None:
        service_names = []
    if isinstance(service_names, str):
        service_names = [service_names]
    controller_type = controller_utils.get_controller_for_pool(pool)
    handle = backend_utils.is_controller_accessible(
        controller=controller_type,
        stopped_message=f'All {noun}s should have terminated.')

    service_names_str = ','.join(service_names)
    if sum([bool(service_names), all]) != 1:
        argument_str = (f'{noun}_names={service_names_str}'
                        if service_names else '')
        argument_str += ' all' if all else ''
        raise ValueError(f'Can only specify one of {noun}_names or all. '
                         f'Provided {argument_str!r}.')

    service_names = None if all else service_names

    try:
        # Serialize against apply()/update() on the same services so a
        # teardown cannot interleave with an in-flight update that would
        # launch replicas mid-teardown (orphaned clusters). Sorted so two
        # concurrent multi-service downs cannot deadlock on lock order.
        # `--all` resolves names on the controller side. The authoritative
        # distributed lifecycle fence is acquired there for every resolved
        # name; these local locks only preserve legacy same-process ordering
        # for explicitly named services.
        with contextlib.ExitStack() as stack:
            for name in sorted(set(service_names or [])):
                stack.enter_context(
                    filelock.FileLock(
                        serve_utils.get_service_filelock_path(name)))
            stdout = _terminate_services(handle, service_names, purge, pool,
                                         noun)
    except exceptions.FetchClusterInfoError as e:
        raise RuntimeError(
            'Failed to fetch controller IP. Please refresh controller status '
            f'by `sky status -r {controller_type.value.cluster_name}` and try '
            'again.') from e
    except exceptions.CommandError as e:
        raise RuntimeError(e.error_msg) from e
    except grpc.RpcError as e:
        raise RuntimeError(f'{e.details()} ({e.code()})') from e
    except grpc.FutureTimeoutError as e:
        raise RuntimeError('gRPC timed out') from e

    logger.info(stdout)


_DefaultServiceStatusRunner = serve_status.DefaultServiceStatusRunner
status = serve_status.status

ServiceComponentOrStr = str | serve_utils.ServiceComponent


def tail_logs(
    service_name: str,
    *,
    target: ServiceComponentOrStr,
    replica_id: int | None = None,
    follow: bool = True,
    tail: int | None = None,
    pool: bool = False,
) -> None:
    """Tail logs of a service or pool."""
    if isinstance(target, str):
        target = serve_utils.ServiceComponent(target)

    if pool and target == serve_utils.ServiceComponent.LOAD_BALANCER:
        raise ValueError(f'Target {target} is not supported for pool.')

    if target == serve_utils.ServiceComponent.REPLICA:
        if replica_id is None:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    '`replica_id` must be specified when using target=REPLICA.')
    else:
        if replica_id is not None:
            with ux_utils.print_exception_no_traceback():
                raise ValueError('`replica_id` must be None when using '
                                 'target=CONTROLLER/LOAD_BALANCER.')

    controller_type = controller_utils.get_controller_for_pool(pool)
    handle = backend_utils.is_controller_accessible(
        controller=controller_type,
        stopped_message=controller_type.value.default_hint_if_non_existent)

    backend = backend_utils.get_backend_from_handle(handle)
    assert isinstance(backend, backends.CloudVmRayBackend), backend

    if target != serve_utils.ServiceComponent.REPLICA:
        code = serve_utils.ServeCodeGen.stream_serve_process_logs(
            service_name,
            stream_controller=(
                target == serve_utils.ServiceComponent.CONTROLLER),
            follow=follow,
            tail=tail,
            pool=pool)
    else:
        assert replica_id is not None, service_name
        code = serve_utils.ServeCodeGen.stream_replica_logs(service_name,
                                                            replica_id,
                                                            follow,
                                                            tail=tail,
                                                            pool=pool)

    # With the stdin=subprocess.DEVNULL, the ctrl-c will not directly
    # kill the process, so we need to handle it manually here.
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT, backend_utils.interrupt_handler)
        signal.signal(signal.SIGTSTP, backend_utils.stop_handler)

    # Refer to the notes in
    # sky/backends/cloud_vm_ray_backend.py::CloudVmRayBackend::tail_logs.
    backend.run_on_head(handle,
                        code,
                        stream_logs=True,
                        process_stream=False,
                        ssh_mode=command_runner.SshMode.INTERACTIVE)


def _get_all_replica_targets(
        service_name: str, backend: backends.CloudVmRayBackend,
        handle: backends.CloudVmRayResourceHandle,
        pool: bool) -> set[serve_utils.ServiceComponentTarget]:
    """Helper function to get targets for all live replicas."""
    assert isinstance(handle, backends.CloudVmRayResourceHandle)
    use_legacy = not handle.is_grpc_enabled_with_flag
    service_records = None

    if not use_legacy:
        try:
            service_records = serve_rpc_utils.RpcRunner.get_service_status(
                handle, [service_name], pool)
        except exceptions.SkyletMethodNotImplementedError:
            use_legacy = True

    if use_legacy:
        code = serve_utils.ServeCodeGen.get_service_status([service_name],
                                                           pool=pool)
        returncode, serve_status_payload, stderr = backend.run_on_head(
            handle,
            code,
            require_outputs=True,
            stream_logs=False,
            separate_stderr=True)

        try:
            subprocess_utils.handle_returncode(returncode,
                                               code,
                                               'Failed to fetch services',
                                               stderr,
                                               stream_logs=True)
        except exceptions.CommandError as e:
            raise RuntimeError(e.error_msg) from e

        service_records = serve_utils.load_service_status(serve_status_payload)

    if service_records is None:
        raise RuntimeError('Failed to fetch service records.')
    if not service_records:
        raise ValueError(f'Service {service_name!r} not found.')
    assert len(service_records) == 1
    service_record = service_records[0]

    return {
        serve_utils.ServiceComponentTarget(serve_utils.ServiceComponent.REPLICA,
                                           replica_info['replica_id'])
        for replica_info in service_record['replica_info']
    }


def sync_down_logs(
    service_name: str,
    *,
    local_dir: str,
    targets: ServiceComponentOrStr | list[ServiceComponentOrStr] | None = None,
    replica_ids: list[int] | None = None,
    tail: int | None = None,
    pool: bool = False,
) -> str:
    """Sync down logs of a service or pool."""
    noun = 'pool' if pool else 'service'
    repnoun = 'worker' if pool else 'replica'
    caprepnoun = repnoun.capitalize()

    # Step 0) get the controller handle
    with rich_utils.safe_status(
            ux_utils.spinner_message(f'Checking {noun} status...')):
        controller_type = controller_utils.get_controller_for_pool(pool)
        handle = backend_utils.is_controller_accessible(
            controller=controller_type,
            stopped_message=controller_type.value.default_hint_if_non_existent)
        backend: backends.CloudVmRayBackend = (
            backend_utils.get_backend_from_handle(handle))

    requested_components: set[serve_utils.ServiceComponent] = set()
    if not targets:
        # No targets specified -> request all components
        requested_components = {
            serve_utils.ServiceComponent.CONTROLLER,
            serve_utils.ServiceComponent.LOAD_BALANCER,
            serve_utils.ServiceComponent.REPLICA
        }
    else:
        # Parse provided targets
        if isinstance(targets, (str, serve_utils.ServiceComponent)):
            requested_components = {serve_utils.ServiceComponent(targets)}
        else:  # list
            requested_components = {
                serve_utils.ServiceComponent(t) for t in targets
            }

    normalized_targets: set[serve_utils.ServiceComponentTarget] = set()
    if serve_utils.ServiceComponent.CONTROLLER in requested_components:
        normalized_targets.add(
            serve_utils.ServiceComponentTarget(
                serve_utils.ServiceComponent.CONTROLLER))
    if serve_utils.ServiceComponent.LOAD_BALANCER in requested_components:
        normalized_targets.add(
            serve_utils.ServiceComponentTarget(
                serve_utils.ServiceComponent.LOAD_BALANCER))
    if serve_utils.ServiceComponent.REPLICA in requested_components:
        with rich_utils.safe_status(
                ux_utils.spinner_message(f'Getting live {repnoun} infos...')):
            replica_targets = _get_all_replica_targets(service_name, backend,
                                                       handle, pool)
        if not replica_ids:
            # Replica target requested but no specific IDs
            # -> Get all replica logs
            normalized_targets.update(replica_targets)
        else:
            # Replica target requested with specific IDs
            requested_replica_targets = [
                serve_utils.ServiceComponentTarget(
                    serve_utils.ServiceComponent.REPLICA, rid)
                for rid in replica_ids
            ]
            for target in requested_replica_targets:
                if target not in replica_targets:
                    logger.warning(f'{caprepnoun} ID {target.replica_id} not '
                                   f'found for {service_name}. Skipping...')
                else:
                    normalized_targets.add(target)

    def sync_down_logs_by_target(target: serve_utils.ServiceComponentTarget):
        component = target.component
        # We need to set one side of the pipe to a logs stream, and the other
        # side to a file.
        log_path = str(pathlib.Path(local_dir) / f'{target}.log')
        stream_logs_code: str

        if component == serve_utils.ServiceComponent.CONTROLLER:
            stream_logs_code = (
                serve_utils.ServeCodeGen.stream_serve_process_logs(
                    service_name,
                    stream_controller=True,
                    follow=False,
                    tail=tail,
                    pool=pool))
        elif component == serve_utils.ServiceComponent.LOAD_BALANCER:
            stream_logs_code = (
                serve_utils.ServeCodeGen.stream_serve_process_logs(
                    service_name,
                    stream_controller=False,
                    follow=False,
                    tail=tail,
                    pool=pool))
        elif component == serve_utils.ServiceComponent.REPLICA:
            replica_id = target.replica_id
            assert replica_id is not None, service_name
            stream_logs_code = serve_utils.ServeCodeGen.stream_replica_logs(
                service_name, replica_id, follow=False, tail=tail, pool=pool)
        else:
            assert False, component

        # Refer to the notes in
        # sky/backends/cloud_vm_ray_backend.py::CloudVmRayBackend::tail_logs.
        backend.run_on_head(handle,
                            stream_logs_code,
                            stream_logs=False,
                            process_stream=False,
                            ssh_mode=command_runner.SshMode.INTERACTIVE,
                            log_path=log_path)

    subprocess_utils.run_in_parallel(sync_down_logs_by_target,
                                     list(normalized_targets))

    return local_dir
