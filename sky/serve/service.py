"""Main entrypoint for a service controller.

Inference traffic is served only by the controller-owned external load
balancer Deployment.  This process owns and supervises the per-service
controller child; it never starts an in-pod load balancer.
"""
import argparse
from collections.abc import Callable
from collections.abc import Iterator
import contextlib
import contextvars
import dataclasses
import hashlib
import json
import multiprocessing
import os
import pathlib
import secrets
import shutil
import socket
import sys
import threading
import time
import traceback
from typing import Any, NoReturn, TYPE_CHECKING

import filelock

from sky import exceptions
from sky import sky_logging
from sky import skypilot_config
from sky import task as task_lib
from sky.backends import backend_utils
from sky.backends import cloud_vm_ray_backend
from sky.data import data_utils
from sky.serve import constants
from sky.serve import controller
from sky.serve import lb_k8s
from sky.serve import maintenance
from sky.serve import replica_managers
from sky.serve import reserved_capacity
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.skylet import constants as skylet_constants
from sky.utils import auth_utils
from sky.utils import common_utils
from sky.utils import controller_utils
from sky.utils import subprocess_utils
from sky.utils import thread_utils
from sky.utils import ux_utils
from sky.utils import yaml_utils

if TYPE_CHECKING:
    from sky.serve import service_spec as service_spec_lib

# Use the explicit logger name so that the logger is under the
# `sky.serve.service` namespace when executed directly, so as
# to inherit the setup from the `sky` logger.
logger = sky_logging.init_logger('sky.serve.service')


class ServiceOwnershipLostError(RuntimeError):
    """Raised when teardown no longer owns the exact service incarnation."""


def load_task_for_storage_cleanup(yaml_content: str) -> task_lib.Task:
    """Load storage metadata without revalidating historical Serve policy."""
    config = yaml_utils.safe_load(yaml_content)
    if not isinstance(config, dict):
        raise ValueError('Service task YAML must contain a mapping.')
    config.pop('service', None)
    config.pop('pool', None)
    return task_lib.Task.from_yaml_config(config)


def _handle_signal(service_name: str,
                   service_hash: str,
                   controller_pid: int,
                   controller_ip: str | None,
                   resource_scope: str | None = None) -> bool:
    """Handles the signal user sent to controller."""
    signal_file = pathlib.Path(
        constants.SIGNAL_FILE_PATH.format(service_name)).expanduser()
    user_signal = None
    if signal_file.exists():
        # Filelock is needed to prevent race condition with concurrent
        # signal writing.
        with filelock.FileLock(str(signal_file) + '.lock'):
            with signal_file.open(mode='r', encoding='utf-8') as f:
                user_signal_text = f.read().strip()
                signal_value: object
                try:
                    signal_payload = json.loads(user_signal_text)
                except (json.JSONDecodeError, TypeError):
                    # Backward compatibility for a signal written by an older
                    # API process is safe only while the row itself is legacy.
                    # A leftover name-only TERMINATE from predecessor A must
                    # never tear down scoped same-name successor B.
                    if resource_scope is not None:
                        logger.warning(
                            f'Discarding legacy name-only signal for scoped '
                            f'service {service_name!r}/{service_hash!r}.')
                        signal_file.unlink()
                        return True
                    signal_value = user_signal_text
                else:
                    if not isinstance(signal_payload, dict):
                        signal_value = None
                    else:
                        target_hash = signal_payload.get('service_hash')
                        signal_value = signal_payload.get('signal')
                        if target_hash != service_hash:
                            logger.warning(
                                f'Discarding stale signal for service '
                                f'{service_name!r} incarnation '
                                f'{target_hash!r}; current incarnation is '
                                f'{service_hash!r}.')
                            signal_file.unlink()
                            return True
                try:
                    user_signal = serve_utils.UserSignal(signal_value)
                except (TypeError, ValueError):
                    # Preserve the historical unknown-signal behavior below.
                    user_signal = None
                try:
                    if user_signal is None:
                        raise ValueError('unknown signal')
                    logger.info(f'User signal received: {user_signal}')
                except ValueError:
                    logger.warning(
                        f'Unknown signal received: {user_signal_text}. '
                        'Ignoring.')
                    user_signal = None
            if user_signal is serve_utils.UserSignal.TERMINATE:
                # Persist the teardown intent BEFORE consuming the signal so a
                # crash in this window cannot resurrect the service: HA recovery
                # then sees either SHUTTING_DOWN (and resumes teardown) or the
                # still-present signal (and re-fires terminate) -- never a
                # downed service that comes back up serving.
                set_status_if_owner = (
                    serve_state.set_service_status_and_active_versions_if_owner)
                try:
                    persisted = set_status_if_owner(
                        service_name, service_hash, controller_pid,
                        controller_ip, serve_state.ServiceStatus.SHUTTING_DOWN)
                except Exception as e:  # pylint: disable=broad-except
                    # A DB blip must not escape into _start's destructive
                    # unexpected-exception finalizer. Keep the signal durable
                    # and retry the exact-owner CAS on the next tick.
                    logger.warning(f'Failed to persist terminate signal for '
                                   f'{service_name!r}: '
                                   f'{common_utils.format_exception(e)}; '
                                   'will retry without consuming it.')
                    return True
                if not persisted:
                    logger.warning(
                        f'Refusing to consume terminate signal for stale '
                        f'service incarnation {service_name!r}/'
                        f'{service_hash!r}.')
                    return False
            # Remove the signal file, after reading it.
            signal_file.unlink()
    if user_signal is None:
        return True
    assert isinstance(user_signal, serve_utils.UserSignal)
    error_type = user_signal.error_type()
    raise error_type(f'User signal received: {user_signal.value}')


def cleanup_storage(yaml_content: str,
                    resource_scope: str | None = None) -> bool:
    """Delete only ephemeral storage owned by ``resource_scope``.

    Args:
        yaml_content: The yaml content of the service.

    Returns:
        True if owned storage is cleaned up successfully (unowned resources
        are deliberately retained), False otherwise.
    """
    failed = False
    task = None
    scope_id: str | None = None

    try:
        task = load_task_for_storage_cleanup(yaml_content)
        if not isinstance(resource_scope, str) or not resource_scope:
            logger.info('Retaining task storage without a durable resource '
                        'scope.')
            return True
        scope_metadata = task.metadata.get(
            constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY)
        storage_generation = (scope_metadata.get('storage_generation')
                              if isinstance(scope_metadata, dict) else None)
        if not isinstance(storage_generation, str):
            logger.info('Retaining task storage without a scoped storage '
                        'generation.')
            return True
        scope_id = serve_utils.generate_ephemeral_storage_scope_id(
            resource_scope, storage_generation)
        if (not isinstance(scope_metadata, dict) or
                scope_metadata.get('resource_scope') != resource_scope or
                scope_metadata.get('scope_id') != scope_id):
            logger.info('Retaining task storage without matching '
                        'incarnation-scoped ownership metadata.')
            return True
        raw_owned_mounts = scope_metadata.get('storage_mounts', [])
        if not isinstance(raw_owned_mounts, list):
            logger.warning('Retaining task storage with malformed scoped '
                           'ownership metadata.')
            return True
        owned_mounts = {
            mount for mount in raw_owned_mounts if isinstance(mount, str)
        }
        safe_storage_mounts = {}
        for mount_path, storage in task.storage_mounts.items():
            if (mount_path in owned_mounts and not storage.persistent and
                    serve_utils.ephemeral_storage_identity_matches_scope(
                        storage, scope_id)):
                safe_storage_mounts[mount_path] = storage
            elif not storage.persistent:
                logger.info('Retaining unowned ephemeral storage mounted at '
                            f'{mount_path!r}.')
        task.storage_mounts = safe_storage_mounts
        # Need to re-construct storage object in the controller process
        # because when SkyPilot API server machine sends the yaml config to the
        # controller machine, only storage metadata is sent, not the storage
        # object itself.
        # Construct storages individually so a stale reference (bucket
        # already deleted by a prior cleanup pass) doesn't abort cleanup of
        # the rest. StorageBucketGetError on construct here means the bucket
        # is already gone — which IS the cleanup target state, not a failure.
        # Without this, FAILED_CLEANUP becomes self-perpetuating: the loop
        # crashes on a stale storage entry, the service goes to
        # FAILED_CLEANUP, ha_recovery_for_consolidation_mode respawns the
        # controller, the controller re-reads the same yaml and crashes on
        # the same stale entry — forever.
        for storage_name in list(task.storage_mounts.keys()):
            storage = task.storage_mounts[storage_name]
            try:
                # Cleanup reconstruction must never re-upload a local source.
                # Pre-upload intents deliberately preserve that source so the
                # remote identity is recoverable after a crash, but construct()
                # defaults to syncing it again when a handle already exists.
                storage.sync_on_reconstruction = False
                storage.construct()
            except exceptions.StorageBucketGetError as e:
                logger.debug(f'cleanup_storage: bucket for storage '
                             f'{storage_name!r} already gone, treating as '
                             f'already cleaned: {e}')
                del task.storage_mounts[storage_name]
        if task.storage_mounts:
            backend = cloud_vm_ray_backend.CloudVmRayBackend()
            backend.teardown_ephemeral_storage(task)
    except Exception as e:  # pylint: disable=broad-except
        logger.error('Failed to clean up storage: '
                     f'{common_utils.format_exception(e)}')
        with ux_utils.enable_traceback():
            logger.error(f'  Traceback: {traceback.format_exc()}')
        failed = True

    # Clean up only two-hop staging paths whose root includes this scope ID.
    # User local paths and legacy random paths remain untouched.
    file_mount_values = (list(
        (task.file_mounts or {}).values()) if task else [])
    scoped_file_mount_root = None
    if task is not None and scope_id is not None:
        scoped_file_mount_root = os.path.realpath(
            os.path.expanduser(
                os.path.join(
                    skylet_constants.FILE_MOUNTS_CONTROLLER_TMP_BASE_PATH,
                    scope_id)))
    for file_mount in file_mount_values:
        try:
            if not data_utils.is_cloud_store_url(file_mount):
                path = os.path.expanduser(file_mount)
                real_path = os.path.realpath(path)
                if (scoped_file_mount_root is None or
                        os.path.commonpath([real_path, scoped_file_mount_root
                                           ]) != scoped_file_mount_root):
                    logger.info('Retaining unowned local file mount source '
                                f'{file_mount!r}.')
                    continue
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
        except FileNotFoundError:
            pass
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Failed to clean up file mount {file_mount}: {e}')
            with ux_utils.enable_traceback():
                logger.error(f'  Traceback: {traceback.format_exc()}')
            failed = True

    return not failed


def cleanup_storage_intents(
        service_name: str,
        resource_scope: str | None,
        ownership_guard: Callable[[], bool] | None = None) -> bool:
    """Clean every durable storage generation owned by one incarnation."""
    if not isinstance(resource_scope, str) or not resource_scope:
        logger.info('Retaining storage for legacy service without a durable '
                    'resource scope.')
        return True
    intents = serve_state.get_ephemeral_storage_cleanup_intents(
        service_name, resource_scope=resource_scope)
    results = []
    cleaned_generations = set()
    for intent in intents:
        if ownership_guard is not None and not ownership_guard():
            raise ServiceOwnershipLostError(
                'Lifecycle ownership lost before scoped storage cleanup.')
        results.append(cleanup_storage(intent['yaml_content'], resource_scope))
        generation = intent.get('storage_generation')
        if isinstance(generation, str):
            cleaned_generations.add(generation)

    # Rolling compatibility: a service can contain surviving pre-migration
    # version YAMLs plus newer intent-backed generations. Clean their union,
    # deduplicating only generations already covered by an exact intent.
    version_yamls = serve_state.get_version_yaml_contents(service_name)
    for yaml_content in version_yamls.values():
        if ownership_guard is not None and not ownership_guard():
            raise ServiceOwnershipLostError(
                'Lifecycle ownership lost before scoped storage cleanup.')
        try:
            version_task = load_task_for_storage_cleanup(yaml_content)
            scope_metadata = version_task.metadata.get(
                constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY)
            generation = (scope_metadata.get('storage_generation')
                          if isinstance(scope_metadata, dict) else None)
        except Exception:  # pylint: disable=broad-except
            generation = None
        if generation in cleaned_generations:
            continue
        results.append(cleanup_storage(yaml_content, resource_scope))
    return all(results)


# NOTE(dev): We don't need to acquire the `with_lock` in replica manager here
# because we killed all the processes (controller & replica manager) before
# calling this function.
def _cleanup(service_name: str,
             pool: bool,
             service_hash: str,
             controller_pid: int,
             controller_ip: str | None,
             lifecycle_lock: Any,
             resource_scope: str | None = None) -> bool:
    """Clean up all service related resources, i.e. replicas and storage."""
    expected_owner = (controller_pid, controller_ip)
    lifecycle_epoch = serve_utils.get_service_lifecycle_epoch(lifecycle_lock)
    ownership_probe_lock = threading.Lock()

    def _still_owns() -> bool:
        # The PostgreSQL advisory-lock liveness probe uses the lock-owning
        # connection. Serialize it across the cleanup coordinator and replica
        # workers; DB connections/cursors are not a concurrent ownership
        # oracle.
        with ownership_probe_lock:
            return (serve_utils.lifecycle_lock_is_valid(lifecycle_lock) and
                    serve_state.service_owner_matches(
                        service_name, service_hash, expected_owner))

    def _assert_owner(phase: str) -> None:
        if not _still_owns():
            raise ServiceOwnershipLostError(
                f'Lost ownership of {service_name!r}/{service_hash!r} '
                f'{phase}; aborting destructive cleanup.')

    def _persist_replica(info: replica_managers.ReplicaInfo) -> None:
        persisted = serve_state.add_or_update_replica(
            service_name,
            info.replica_id,
            info,
            expected_service_hash=service_hash,
            expected_lifecycle_epoch=lifecycle_epoch,
            expected_controller_owner=expected_owner,
            expected_replica_exists=True)
        if persisted is False:
            raise ServiceOwnershipLostError(
                f'Lost lifecycle epoch or replica {info.replica_id} was '
                'removed while updating cleanup bookkeeping.')

    def _remove_replica(info: replica_managers.ReplicaInfo) -> None:
        removed = serve_state.remove_replica(
            service_name,
            info.replica_id,
            expected_service_hash=service_hash,
            expected_lifecycle_epoch=lifecycle_epoch,
            expected_controller_owner=expected_owner,
            expected_replica_record_id=info.replica_record_id)
        if removed is False:
            raise ServiceOwnershipLostError(
                'Lost lifecycle epoch or record identity while removing '
                f'replica {info.replica_id}.')

    _assert_owner('before replica cleanup')
    # Log who we are and what DB state we're cleaning up, so post-mortems
    # can correlate this with concurrent ha_recovery activity. _cleanup is
    # destructive (it tears down replicas before the caller conditionally
    # finalizes the recovery script and service row), so an audit trail is
    # worth a few WARN lines.
    own_pid = os.getpid()
    try:
        svc_dbg = serve_state.get_service_from_name(service_name)
    except Exception:  # pylint: disable=broad-except
        svc_dbg = None
    if svc_dbg is not None:
        logger.warning(
            f'_cleanup entered for service {service_name} '
            f'(own_pid={own_pid}, db_controller_pid='
            f'{svc_dbg.get("controller_pid")}, db_controller_ip='
            f'{svc_dbg.get("controller_ip")}, status={svc_dbg.get("status")})')
    else:
        logger.warning(
            f'_cleanup entered for service {service_name} '
            f'(own_pid={own_pid}, db row not found — already removed?)')
    # NOTE: retain the HA recovery script throughout _cleanup. Removing it
    # up-front opened a window where a controller-pod kill mid-teardown (HA pod
    # move / node drain) left a durable service row with no recovery path and
    # stranded replicas. The caller either removes the script in the same
    # owner-fenced transaction as a successful service-row deletion, or only
    # after it has durably published FAILED_CLEANUP.
    failed = False
    replica_infos = serve_state.get_replica_infos(service_name)
    existing_cluster_names = serve_utils.get_existing_replica_cluster_names(
        replica_infos)
    # Cluster inventory and Serve metadata live in separate tables, so this
    # is not an atomic cross-table snapshot. Launch quiescence prevents this
    # controller from registering a new cluster, while the owner check and
    # the fenced delete below reject a successor service or controller.
    _assert_owner('after cluster inventory snapshot')

    def _set_to_failed_cleanup(info: replica_managers.ReplicaInfo,
                               reason: str | None = None) -> None:
        nonlocal failed
        # Set replica status to `FAILED_CLEANUP` and preserve its durable row.
        # In particular, absence from SkyPilot's cluster table is not proof
        # that a protocol-v2 Kubernetes object is absent.
        if info.status_property.sky_launch_status in (
                None, replica_managers.common_utils.ProcessStatus.SCHEDULED,
                replica_managers.common_utils.ProcessStatus.INTERRUPTED):
            # These launch states otherwise dominate ``sky_down_status`` in
            # status rendering (PENDING/SHUTTING_DOWN). The launch barrier has
            # already quiesced them, so publish the retained row accurately as
            # FAILED_CLEANUP.
            info.status_property.sky_launch_status = (
                replica_managers.common_utils.ProcessStatus.FAILED)
        info.status_property.sky_down_status = (
            replica_managers.common_utils.ProcessStatus.FAILED)
        _persist_replica(info)
        failed = True
        suffix = '' if reason is None else f': {reason}'
        logger.error(f'Replica {info.replica_id} failed to terminate{suffix}.')

    absent_legacy_infos: list[replica_managers.ReplicaInfo] = []
    cleanup_entries: list[tuple[replica_managers.ReplicaInfo,
                                reserved_capacity.ProtocolV2CleanupFence |
                                None]] = []
    for info in replica_infos:
        try:
            cleanup_fence = (
                reserved_capacity.parse_protocol_v2_cleanup_fence(info))
        except exceptions.KubernetesPhysicalClusterIdentityError as error:
            _set_to_failed_cleanup(
                info, 'durable Kubernetes cleanup identity is malformed or '
                f'incomplete ({common_utils.format_exception(error)})')
            continue
        if info.cluster_name not in existing_cluster_names:
            if cleanup_fence is None:
                absent_legacy_infos.append(info)
            else:
                _set_to_failed_cleanup(
                    info, 'the SkyPilot cluster record is absent but '
                    'provider absence is not independently proven')
            continue
        cleanup_entries.append((info, cleanup_fence))

    if absent_legacy_infos:
        removed = serve_state.remove_replicas(
            service_name, [info.replica_id for info in absent_legacy_infos],
            expected_service_hash=service_hash,
            expected_lifecycle_epoch=lifecycle_epoch,
            expected_controller_owner=expected_owner,
            expected_replica_record_ids={
                info.replica_id: info.replica_record_id
                for info in absent_legacy_infos
            })
        if not removed:
            raise ServiceOwnershipLostError(
                'Lost lifecycle ownership while bulk-removing absent '
                'replicas.')
        logger.info(f'Removed {len(absent_legacy_infos)} legacy replica '
                    'records whose clusters are absent from the cluster '
                    'inventory.')

    teardown_identities: dict[int, serve_state.ReplicaResourceActionIdentity |
                              None] = {}
    if cleanup_entries:
        cleanup_replica_ids = [info.replica_id for info, _ in cleanup_entries]
        try:
            # Snapshot every action-owned cluster-record UUID before starting
            # any worker.  Context/physical-UID fencing prevents a kubeconfig
            # alias from changing clusters, while this independent fence
            # prevents a same-name cluster-table replacement on that cluster
            # from being consumed by stale service cleanup.
            teardown_identities = (
                serve_state.get_replica_resource_action_identities(
                    service_name, cleanup_replica_ids))
            if set(teardown_identities) != set(cleanup_replica_ids):
                raise RuntimeError(
                    'Replica inventory changed while snapshotting teardown '
                    'identities.')
        except Exception as error:  # pylint: disable=broad-except
            reason = ('durable replica teardown identities could not be '
                      'verified '
                      f'({common_utils.format_exception(error)})')
            for info, _ in cleanup_entries:
                _set_to_failed_cleanup(info, reason)
            cleanup_entries = []
    # TODO(fcapponi): DEPRECATED resource-action teardown owner. Remove this
    # whole-service thread loop at M5 for eligible authoritative services after
    # durable down actions cover cleanup and rollback.
    info2thr: dict[replica_managers.ReplicaInfo,
                   thread_utils.SafeThread] = dict()
    for info, cleanup_fence in cleanup_entries:
        _assert_owner(f'before scheduling replica {info.replica_id} cleanup')
        # Use the durable exact cluster identity from the replica row. New
        # incarnation-scoped names truncate long service prefixes to stay
        # within the 63-character cloud/Kubernetes ceiling, so a prefix query
        # with the full service name can miss a live, billable cluster.
        log_file_name = serve_utils.generate_replica_log_file_name(
            service_name, info.replica_id, resource_scope)
        teardown_identity = teardown_identities[info.replica_id]
        terminate_kwargs: dict[str, Any] = {
            'continue_guard': _still_owns,
            'expected_cluster_record_uuid':
                (str(teardown_identity.sky_cluster_record_uuid)
                 if teardown_identity is not None else None),
        }
        if cleanup_fence is not None:
            terminate_kwargs['cleanup_fence'] = cleanup_fence
        t = thread_utils.SafeThread(target=replica_managers.terminate_cluster,
                                    args=(info.cluster_name, log_file_name),
                                    kwargs=terminate_kwargs)
        info2thr[info] = t
        # Set replica status to `SHUTTING_DOWN`
        info.status_property.sky_launch_status = (
            replica_managers.common_utils.ProcessStatus.SUCCEEDED)
        info.status_property.sky_down_status = (
            replica_managers.common_utils.ProcessStatus.SCHEDULED)
        _persist_replica(info)
        logger.info(f'Scheduling to terminate replica {info.replica_id} ...')

    # Please reference to sky/serve/replica_managers.py::_refresh_process_pool.
    # TODO(tian): Refactor to use the same logic and code.
    while info2thr:
        _assert_owner('while waiting for replica cleanup')
        snapshot = list(info2thr.items())
        for info, t in snapshot:
            if t.is_alive():
                continue
            if (info.status_property.sky_down_status ==
                    replica_managers.common_utils.ProcessStatus.SCHEDULED):
                if controller_utils.can_terminate(pool):
                    try:
                        t.start()
                    except Exception as e:  # pylint: disable=broad-except
                        _set_to_failed_cleanup(info)
                        logger.error(f'Failed to start thread for replica '
                                     f'{info.replica_id}: {e}')
                        del info2thr[info]
                    else:
                        info.status_property.sky_down_status = (
                            common_utils.ProcessStatus.RUNNING)
                        _persist_replica(info)
            else:
                logger.info('Terminate thread for replica '
                            f'{info.replica_id} finished.')
                t.join()
                del info2thr[info]
                if t.format_exc is None:
                    _remove_replica(info)
                    logger.info(
                        f'Replica {info.replica_id} terminated successfully.')
                else:
                    _set_to_failed_cleanup(info)
        if info2thr:
            time.sleep(3)

    _assert_owner('before scoped storage cleanup')

    if not cleanup_storage_intents(service_name, resource_scope, _still_owns):
        failed = True

    # Do not delete the recovery script here. The success path removes it in
    # the same owner-fenced transaction as the service row; the failed path
    # removes it only after conditionally publishing FAILED_CLEANUP. Keeping
    # it through this boundary means a lost lifecycle-lock session cannot
    # strand a teardown between script deletion and final DB removal.
    #
    # NOTE: do not delete version_specs here. The final success transaction
    # deletes them with the service. Deleting them on failure
    # makes the `services` row invisible to `get_service_from_name` (it
    # uses an INNER JOIN with `version_specs`), so `sky ... status` /
    # `sky ... down --purge` can no longer locate the FAILED_CLEANUP row,
    # and the only way out is to manually delete the DB row.
    return failed


def _cleanup_task_run_script(job_id: int) -> None:
    """Clean up task run script.
    Please see `kubernetes-ray.yml.j2` for more details.
    """
    task_run_dir = pathlib.Path(
        skylet_constants.PERSISTENT_RUN_SCRIPT_DIR).expanduser()
    if task_run_dir.exists():
        this_task_run_script = task_run_dir / f'sky_job_{job_id}'
        if this_task_run_script.exists():
            this_task_run_script.unlink()
            logger.info(f'Task run script {this_task_run_script} removed')
        else:
            logger.warning(f'Task run script {this_task_run_script} not found')


def _wait_for_controller_ready(
        host: str,
        port: int,
        timeout: int = 30,
        process: multiprocessing.Process | None = None) -> None:
    """Block until the controller HTTP server is accepting connections.

    We must not flip DB `controller_pid`/`controller_ip` until the new
    subprocess is actually listening, otherwise clients routed by DB hit
    the new pod's IP before its uvicorn binds and get ECONNREFUSED.

    If `process` is given, fail as soon as it is no longer alive instead of
    burning the full timeout and delaying the next recovery attempt.
    """
    # When binding 0.0.0.0, probe via loopback.
    probe_host = '127.0.0.1' if host == '0.0.0.0' else host
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        if process is not None and not process.is_alive():
            raise RuntimeError(
                f'Controller process exited (exitcode={process.exitcode}) '
                f'before becoming ready on {probe_host}:{port}')
        try:
            with socket.create_connection((probe_host, port),
                                          timeout=min(0.5, remaining)):
                return
        except OSError:
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(0.2, remaining))
    raise RuntimeError(f'Controller did not become ready on '
                       f'{probe_host}:{port} within {timeout}s')


def _orphan_exit(
        controller_process: multiprocessing.Process | None) -> NoReturn:
    """Quick exit path for an orphan sky.serve.service.

    Triggered when our self-check sees the DB owner tuple (service hash, PID,
    pod IP) no longer match ours, or when the services row has been removed
    (down completed). DB state is now owned by that new instance — we must NOT
    call _cleanup, which would teardown replicas and delete versions, racing
    with the new owner.

    Just kill our own controller child and exit immediately.  The external LB
    is a Kubernetes object reconciled by the authoritative owner, not a child
    of this process.
    """
    logger.info(f'_orphan_exit invoked: own_pid={os.getpid()} '
                f'controller_process_pid='
                f'{controller_process.pid if controller_process else None}')
    if controller_process is not None:
        try:
            subprocess_utils.kill_children_processes(
                parent_pids=[controller_process.pid], force=True)
        except Exception:  # pylint: disable=broad-except
            logger.warning('Failed to kill children during orphan exit; '
                           'proceeding with os._exit anyway')
    # os._exit() bypasses the try/finally which would call _cleanup.
    os._exit(0)  # pylint: disable=protected-access


def _exit_on_ownership_loss(
        updated: bool, service_name: str, operation: str,
        controller_process: multiprocessing.Process | None) -> None:
    """Discard our controller and bypass cleanup after a failed owner CAS."""
    if updated:
        return
    logger.warning(f'Lost ownership of service {service_name} while '
                   f'{operation}; discarding our controller and exiting '
                   'without cleanup.')
    _orphan_exit(controller_process)


def _bail_on_boot_failure(service_name: str,
                          controller_process: multiprocessing.Process | None,
                          timeout_seconds: int,
                          boot_err: BaseException,
                          component: str = 'Controller subprocess') -> None:
    """Retryable exit when a service component cannot finish booting.

    Critical contract: must NOT fall through to `_start`'s outer
    `try/finally`. That finally runs destructive teardown and may conditionally
    remove the service row, turning a transient boot failure into permanent
    data loss for the service.

    Kill the controller subprocess we spawned, then os._exit(1) to
    bypass everything. The daemon's next ha_recovery iteration sees
    the (preserved) recovery script and retries with a fresh _start.
    """
    logger.error(f'{component} failed to become ready within '
                 f'{timeout_seconds}s for {service_name}: {boot_err}. '
                 f'Killing controller subprocess and exiting WITHOUT '
                 f'cleanup so the daemon can retry. DB state and HA '
                 f'recovery script preserved.')
    # Defensive: skip kill if pid is None. `Process.start()` populates
    # pid on success, but if start() raised before setting it the
    # current code path would never reach here anyway (the outer try
    # in `_start` would handle that). Still, guard explicitly because
    # `psutil.Process(None)` returns the *calling* process — so
    # passing `[None]` to `kill_children_processes` would target
    # ourselves (and our own children) instead of bailing out cleanly.
    if controller_process is not None and controller_process.pid is not None:
        try:
            # kill_children_processes with parent_pids != None SIGKILLs
            # the parent itself AND its recursive children (see
            # subprocess_utils.py).
            subprocess_utils.kill_children_processes(
                parent_pids=[controller_process.pid], force=True)
        except Exception:  # pylint: disable=broad-except
            logger.warning(
                'Failed to kill controller subprocess during boot-failure '
                'bailout; proceeding with os._exit anyway.')
    # os._exit() bypasses the outer try/finally. The short-lived port lock was
    # already released after socket transfer; process exit closes the child's
    # reserved socket if controller startup did not consume it.
    os._exit(1)  # pylint: disable=protected-access


def _spawn_controller(
        service_name: str,
        service_spec: 'service_spec_lib.SkyServiceSpec',
        version: int,
        controller_host: str,
        controller_port: int,
        service_hash: str,
        controller_ip: str | None,
        resource_scope: str | None = None,
        enforce_launch_fence: bool = False,
        controller_socket: socket.socket | None = None
) -> multiprocessing.Process:
    """Spawn (and start) the controller server subprocess for a service.

    Factored out of `_start` so the supervision loop can re-create the
    controller (on a fresh port) if it dies. See `_respawn_controller`.

    If a bound controller socket is supplied, `Process.start()` transfers it to
    the child. The caller retains the parent copy as a reservation lease.
    """
    owner_fingerprint = serve_utils.make_controller_owner_fingerprint(
        service_hash, os.getpid(), controller_ip, controller_port)
    process = multiprocessing.Process(
        target=controller.run_controller,
        args=(service_name, service_spec, version, controller_host,
              controller_port, owner_fingerprint, resource_scope, service_hash,
              os.getpid(), controller_ip, enforce_launch_fence,
              controller_socket))
    process.start()
    return process


def _reserve_controller_socket(
        controller_host: str) -> tuple[socket.socket, int]:
    """Bind and reserve one local controller port without listening.

    A bound socket closes the select-then-bind race while still making the
    readiness probe wait for the child to install the socket into Uvicorn's
    event loop. The caller owns the returned socket.
    """
    for controller_port in range(constants.CONTROLLER_PORT_START, 65535):
        controller_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            controller_socket.bind((controller_host, controller_port))
        except OSError:
            controller_socket.close()
            continue
        return controller_socket, controller_port
    raise OSError('No free controller ports are available.')


@contextlib.contextmanager
def _spawn_controller_on_reserved_port(
    service_name: str,
    service_spec: 'service_spec_lib.SkyServiceSpec',
    version: int,
    controller_host: str,
    service_hash: str,
    controller_ip: str | None,
    resource_scope: str | None = None,
    enforce_launch_fence: bool = False
) -> Iterator[tuple[multiprocessing.Process, int]]:
    """Yield a child while retaining its parent-side socket reservation."""
    controller_socket = None
    try:
        with filelock.FileLock(
                os.path.expanduser(constants.PORT_SELECTION_FILE_LOCK_PATH)):
            controller_socket, controller_port = _reserve_controller_socket(
                controller_host)
            spawn_args = (service_name, service_spec, version, controller_host,
                          controller_port, service_hash, controller_ip)
            if resource_scope is None:
                process = _spawn_controller(
                    *spawn_args,
                    enforce_launch_fence=enforce_launch_fence,
                    controller_socket=controller_socket)
            else:
                process = _spawn_controller(
                    *spawn_args,
                    resource_scope=resource_scope,
                    enforce_launch_fence=enforce_launch_fence,
                    controller_socket=controller_socket)
        # The lock is released before readiness and owner publication. Keep
        # the parent duplicate open so child death cannot free and cross-wire
        # the port during that decision window.
        yield process, controller_port
    finally:
        if controller_socket is not None:
            controller_socket.close()


def _kill_process(process: multiprocessing.Process | None) -> None:
    """Best-effort SIGKILL of a subprocess and its children."""
    if process is None:
        return
    try:
        subprocess_utils.kill_children_processes(parent_pids=[process.pid],
                                                 force=True)
    except Exception:  # pylint: disable=broad-except
        pass


# Supervision of the controller child in _start's keep-alive loop: after this
# many consecutive failed respawn attempts, the service is
# flagged CONTROLLER_FAILED in the DB so `sky serve status` stops advertising
# a dead endpoint as healthy. Respawn attempts continue with exponential
# backoff, and the flag is cleared if the child recovers.
_CHILD_FAILURES_BEFORE_FLAG = 3
_CHILD_RESPAWN_BACKOFF_BASE_SECONDS = 5
_CHILD_RESPAWN_BACKOFF_CAP_SECONDS = 300
_DEAD_CHILD_REAP_TIMEOUT_SECONDS = 1


def _child_respawn_backoff_seconds(consecutive_failures: int) -> float:
    """Exponential backoff for child respawn attempts, capped."""
    return min(
        _CHILD_RESPAWN_BACKOFF_BASE_SECONDS *
        (2**max(consecutive_failures - 1, 0)),
        _CHILD_RESPAWN_BACKOFF_CAP_SECONDS)


@dataclasses.dataclass
class _ControllerSupervisionBackoff:
    """Independent retry clocks for degraded status and real child death."""

    degraded_retry_at: float = 0.0
    respawn_failures: int = 0
    respawn_retry_at: float = 0.0

    def respawn_is_due(self, service_name: str,
                       process: multiprocessing.Process | None,
                       now: float) -> bool:
        """Whether a confirmed-dead child is due for a respawn attempt."""
        return (_controller_child_needs_respawn(service_name, process) and
                now >= self.respawn_retry_at)

    def record_respawn_failure(self, now: float) -> None:
        self.respawn_failures += 1
        self.respawn_retry_at = now + _child_respawn_backoff_seconds(
            self.respawn_failures)

    def record_respawn_success(self) -> None:
        self.respawn_failures = 0
        self.respawn_retry_at = 0.0

    def degraded_retry_is_due(self, now: float) -> bool:
        return now >= self.degraded_retry_at

    def record_degraded_failure(self, now: float,
                                consecutive_failures: int) -> None:
        self.degraded_retry_at = now + _child_respawn_backoff_seconds(
            consecutive_failures)

    def record_healthy(self) -> None:
        self.degraded_retry_at = 0.0


def _controller_child_needs_respawn(
        service_name: str, process: multiprocessing.Process | None) -> bool:
    """Whether the controller child has authoritatively exited.

    HTTP health is deliberately excluded. A live controller can starve its
    lightweight endpoint for minutes while making progress on large-fleet
    recovery. Treating a health timeout as process death can create two live
    controller children with the same launch authority.
    """
    if process is None:
        logger.error(
            f'Controller supervision action=hold_missing_child_handle '
            f'service={service_name} parent_pid={os.getpid()}: cannot prove '
            'that a prior child is dead; refusing automatic respawn.')
        return False
    try:
        return not process.is_alive()
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Controller supervision action=hold_live_child '
                     f'service={service_name} parent_pid={os.getpid()} '
                     f'child_pid={_process_pid_or_none(process)} '
                     f'liveness=unknown: {common_utils.format_exception(e)}.')
        return False


def _process_pid_or_none(process: multiprocessing.Process | None) -> int | None:
    """Read a process PID without letting a closed handle break supervision."""
    if process is None:
        return None
    try:
        return process.pid
    except Exception:  # pylint: disable=broad-except
        return None


def _reap_dead_controller_for_respawn(
        service_name: str, process: multiprocessing.Process | None) -> bool:
    """Confirm and reap the prior child before a replacement is spawned.

    Fails closed on liveness, join, or exit-code ambiguity. This helper must be
    called before spec reload, port selection, process spawn, or DB mutation so
    a direct caller cannot overlap two children under one parent.
    """
    if process is None:
        logger.error(f'Controller supervision action=defer_dead_respawn '
                     f'service={service_name} parent_pid={os.getpid()} '
                     'child_pid=None: a concrete prior child is required.')
        return False
    child_pid = _process_pid_or_none(process)
    try:
        if process.is_alive():
            logger.error(
                f'Controller supervision action=refuse_live_respawn '
                f'service={service_name} parent_pid={os.getpid()} '
                f'child_pid={child_pid}: the prior child is still alive.')
            return False
        process.join(timeout=_DEAD_CHILD_REAP_TIMEOUT_SECONDS)
        if process.is_alive() or process.exitcode is None:
            logger.error(
                f'Controller supervision action=defer_dead_respawn '
                f'service={service_name} parent_pid={os.getpid()} '
                f'child_pid={child_pid}: death/reaping is not confirmed.')
            return False
    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            f'Controller supervision action=defer_dead_respawn '
            f'service={service_name} parent_pid={os.getpid()} '
            f'child_pid={child_pid}: failed to confirm/reap the prior child: '
            f'{common_utils.format_exception(e)}.')
        return False
    return True


def _controller_health_miss_is_graced(controller_responding: bool,
                                      controller_needs_respawn: bool,
                                      external_lb_healthy: bool) -> bool:
    """Whether one controller health miss is intentionally tolerated.

    A live child inside the fleet-scale unresponsive grace is not yet a
    confirmed controller failure.  Do not let those tolerated misses advance
    the degraded-status counter while the external data plane is healthy.
    """
    return (not controller_responding and not controller_needs_respawn and
            external_lb_healthy)


def _controller_child_responding(service_name: str, service_hash: str,
                                 controller_ip: str | None,
                                 controller_port: int) -> bool:
    """Bounded health check for a live-but-hung controller child."""
    try:
        response = serve_utils._get_to_local_controller_with_retry(  # pylint: disable=protected-access
            service_name,
            (service_hash, os.getpid(), controller_ip, controller_port),
            constants.CONTROLLER_HEALTH_ENDPOINT_PATH,
            timeout=(0.5, constants.CONTROLLER_HEALTH_READ_TIMEOUT_SECONDS))
        return response.status_code == 200
    except Exception as e:  # pylint: disable=broad-except
        logger.warning(f'Controller health check failed for {service_name}: '
                       f'{common_utils.format_exception(e)}')
        return False


def _flag_service_degraded(service_name: str, service_hash: str,
                           controller_pid: int,
                           controller_ip: str | None) -> None:
    """Mark the service CONTROLLER_FAILED after repeated child failures.

    Never overrides a teardown in progress. Best-effort: a DB failure here
    must not break the supervision loop.
    """
    try:
        record = serve_state.get_service_from_name(service_name)
        if record is None or record['status'] in (
                serve_state.ServiceStatus.SHUTTING_DOWN,
                serve_state.ServiceStatus.FAILED_CLEANUP):
            return
        if record['status'] != serve_state.ServiceStatus.CONTROLLER_FAILED:
            logger.error(f'Flagging service {service_name} as '
                         'CONTROLLER_FAILED after repeated controller or '
                         'external load balancer failures.')
            serve_state.set_service_status_and_active_versions_if_owner(
                service_name,
                service_hash,
                controller_pid,
                controller_ip,
                serve_state.ServiceStatus.CONTROLLER_FAILED,
                expected_status=record['status'])
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Failed to flag service {service_name} as degraded: '
                     f'{common_utils.format_exception(e)}')


def _heal_service_degraded(service_name: str, service_hash: str,
                           controller_pid: int,
                           controller_ip: str | None) -> bool:
    """Clear CONTROLLER_FAILED once the children are confirmed healthy.

    Resets to REPLICA_INIT; the controller's next probe round recomputes the
    real status from replica states (the replica-driven writer never
    overwrites CONTROLLER_FAILED itself, so this reset is the only way back).
    Also heals services HA-recovered from a dead parent, whose status was set
    to CONTROLLER_FAILED by the status refresh daemon.

    Returns whether the heal is complete (status confirmed cleared or not
    set). On a DB failure returns False so the caller retries on the next
    healthy tick: giving up would leave the service stuck CONTROLLER_FAILED
    forever, since the replica-driven writer is blocked on that status and
    HA recovery does not replace a live parent.
    """
    try:
        record = serve_state.get_service_from_name(service_name)
        if (record is not None and record['status']
                == serve_state.ServiceStatus.CONTROLLER_FAILED):
            logger.info(f'Service {service_name} controller/data plane '
                        'recovered; clearing CONTROLLER_FAILED.')
            return serve_state.set_service_status_and_active_versions_if_owner(
                service_name,
                service_hash,
                controller_pid,
                controller_ip,
                serve_state.ServiceStatus.REPLICA_INIT,
                expected_status=serve_state.ServiceStatus.CONTROLLER_FAILED)
        return True
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Failed to heal degraded service {service_name}: '
                     f'{common_utils.format_exception(e)}')
        return False


def _respawn_controller(
    service_name: str,
    controller_host: str,
    dead_controller: multiprocessing.Process | None,
    service_hash: str,
    controller_ip: str | None = None,
    resource_scope: str | None = None,
    enforce_launch_fence: bool = False,
) -> tuple[multiprocessing.Process, int] | None:
    """Re-create a controller child that died while its parent is alive.

    HA recovery only re-creates a controller when the parent `controller_pid`
    row disappears / a pod moves; it does NOT cover the controller child dying
    while the parent stays alive, and in VM mode nothing does -- autoscaling,
    probing and reconciliation would otherwise stop permanently.

    A fresh controller port, chosen free under the port-selection lock, avoids
    cross-wiring when services share a controller pod. The stable API-service
    proxy resolves the new address from the DB, so no LB restart or Deployment
    patch is needed. The DB controller_port write is guarded by the full row
    owner tuple (service hash, PID, pod IP): HA recovery on another pod may have
    taken the row over since our last orphan check, and an unconditional write
    would cross-wire the new owner's atomically-flipped pid/ip/port with our
    stale port. controller_pid/ip (the live parent) are unchanged.

    Returns (controller_process, controller_port) on success, or None on
    failure (retry next tick). Never raises into _start's destructive cleanup.
    The external LB continues serving its last routing view while the proxy
    reports 503 during the controller gap.
    """
    if maintenance.is_controller_hold_active():
        try:
            identity = serve_state.get_service_mode_and_hash(service_name)
        except Exception as e:  # pylint: disable=broad-except
            # The hold applies unless the durable row positively identifies a
            # pool.  A DB error is not permission to resume a Serve child.
            logger.warning(
                f'Could not prove {service_name!r} is a pool while the server '
                f'deployment hold is active: '
                f'{common_utils.format_exception(e)}')
            return None
        if identity is None or not identity[0]:
            logger.warning(f'Refusing to respawn the controller child for '
                           f'{service_name!r} while the server deployment hold '
                           'is active.')
            return None
    if not _reap_dead_controller_for_respawn(service_name, dead_controller):
        return None

    # Snapshot the latest applicable committed version + spec so a respawn
    # after /update_service uses the current safe config. The parent loop's
    # captured values may be stale after an in-place update, while a durably
    # quarantined version must not crash-loop the replacement controller.
    try:
        snapshot = serve_state.get_recovery_version_spec(service_name)
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Failed to reload the latest applicable version/spec for '
                     f'{service_name}: {common_utils.format_exception(e)}; '
                     f'will retry on the next tick.')
        return None
    if snapshot is None:
        logger.error(f'No applicable version/spec found for {service_name}; '
                     'will retry on the next tick.')
        return None
    version, service_spec = snapshot

    # A child can die after the version/catalog/recovery transaction commits
    # but before it promotes the matching config. Reconcile that generation in
    # the long-lived parent before every respawn, then atomically publish the
    # already-validated bytes so forked children cannot inherit the old or a
    # transient empty process config.
    try:
        live_path = serve_utils.generate_versioned_config_yaml_file_name(
            service_name, version, resource_scope)
        config_snapshot = serve_state.get_version_controller_config(
            service_name, version)
        if config_snapshot is not None:
            staged_path = serve_utils.generate_staged_config_yaml_file_name(
                service_name,
                version,
                resource_scope,
                snapshot_id=config_snapshot[2])
            recovery_identity = (
                serve_state.get_service_config_recovery_identity(service_name))
            if (recovery_identity is None or
                    recovery_identity[0] != service_hash):
                raise RuntimeError('Service incarnation changed before '
                                   'controller config recovery.')
            expected_workspace = recovery_identity[1]
            with filelock.FileLock(
                    skypilot_config.get_skypilot_config_lock_path()):
                config_bytes = serve_utils.restore_version_controller_config(
                    service_name,
                    version,
                    live_path,
                    staged_path,
                    expected_workspace=expected_workspace)
                assert config_bytes is not None
                config = (
                    serve_utils.parse_and_validate_version_controller_config(
                        config_bytes, expected_workspace,
                        'committed Serve controller recovery config'))

                def _publish_config() -> None:
                    skypilot_config.install_internal_config_snapshot(
                        config, live_path)

                contextvars.Context().run(_publish_config)
                serve_utils.scrub_obsolete_controller_config_files(
                    service_name, version, resource_scope)
    except Exception as e:  # pylint: disable=broad-except
        logger.error('Failed to reconcile the committed controller config for '
                     f'{service_name}: {common_utils.format_exception(e)}; '
                     'will retry on the next tick.')
        return None

    new_controller = None
    try:
        with _spawn_controller_on_reserved_port(
                service_name,
                service_spec,
                version,
                controller_host,
                service_hash,
                controller_ip,
                resource_scope=resource_scope,
                enforce_launch_fence=enforce_launch_fence) as (new_controller,
                                                               controller_port):
            # The parent reservation remains open while we wait outside the
            # host-global lock, so child death cannot let another service
            # reuse this port before publication finishes.
            _wait_for_controller_ready(
                controller_host,
                controller_port,
                timeout=constants.SERVICE_REGISTER_TIMEOUT_SECONDS,
                process=new_controller)
            if not new_controller.is_alive():
                raise RuntimeError(
                    'replacement controller exited during startup')
            if not serve_state.set_service_controller_port_if_owner(
                    service_name, service_hash, os.getpid(), controller_ip,
                    controller_port):
                # Another instance (HA recovery on a different pod) took over
                # the row while we were bringing up the replacement. Discard
                # it; the orphan check in _start's loop will exit this parent
                # shortly.
                logger.warning(
                    f'Lost ownership of service {service_name} during the '
                    'controller respawn; discarding the replacement '
                    'controller.')
                _kill_process(new_controller)
                return None
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Failed to bring up a replacement controller for '
                     f'{service_name}: {common_utils.format_exception(e)}; '
                     f'will retry on the next tick.')
        _kill_process(new_controller)
        return None

    # Controller is up and its new port is authoritative in the DB. The prior
    # child was confirmed dead and reaped before spawn, so no child overlap was
    # possible. The proxy picks up the new tuple on the next sync.
    logger.info(
        f'Controller supervision action=respawn_dead_child '
        f'service={service_name} parent_pid={os.getpid()} '
        f'child_pid={new_controller.pid} controller_port={controller_port}; '
        'the stable proxy now routes to it.')
    return new_controller, controller_port


def _get_latest_committed_lb_termination_grace_seconds(
        service_name: str) -> int | None:
    """Return LB termination grace seconds from the recovery-elected spec.

    The external-LB supervision loop runs for the lifetime of the service, so
    it should reuse the quarantine-aware `(version, spec)` snapshot instead of
    re-issuing separate latest-version and spec reads on every upkeep round or
    applying grace from an unproven intermediate generation.
    """
    snapshot = serve_state.get_recovery_version_spec(service_name)
    if snapshot is None:
        return None
    _, latest_spec = snapshot
    return lb_k8s.lb_termination_grace_period_seconds(
        latest_spec.lb_stream_timeout_seconds,
        latest_spec.graceful_drain_seconds)


def _should_resume_teardown(is_recovery: bool,
                            service: dict[str, Any] | None) -> bool:
    """Whether a recovery run should resume teardown instead of serving.

    A controller that died mid-teardown left the service in a teardown status
    (SHUTTING_DOWN from a user `down`, or FAILED_CLEANUP from a prior failed
    attempt). Bringing it back up would resurrect a service the user tore down,
    so recovery must instead finish the cleanup. A controller that died for any
    other reason left a non-teardown status (e.g. READY) and is recovered
    normally (brought back up).
    """
    return (is_recovery and service is not None and
            service['status'] in (serve_state.ServiceStatus.SHUTTING_DOWN,
                                  serve_state.ServiceStatus.FAILED_CLEANUP))


def _run_cleanup_and_finalize(service_name: str,
                              service_spec: 'service_spec_lib.SkyServiceSpec',
                              service_dir: str,
                              job_id: int,
                              service_hash: str,
                              controller_pid: int,
                              controller_ip: str | None,
                              resource_scope: str | None = None) -> None:
    """Run ``_cleanup`` and finalize the service's DB / dir state.

    Shared by ``_start``'s teardown ``finally`` and the recovery-resume path (a
    controller that died mid-teardown). On failure the service is left
    FAILED_CLEANUP so an operator can ``--purge``; on success the service row
    and working dir are removed.
    """
    # Publish the durable terminal fence BEFORE scanning launch requests. The
    # API scheduler and the persisted execution entrypoint both reject a Serve
    # launch whose owner is terminal, so an HTTP request appearing after an
    # empty scan cannot begin provisioning. The controller-port
    # acknowledgement remains later: purge may only proceed after every
    # already-persisted request is terminal.
    if not serve_state.set_service_status_and_active_versions_if_owner(
            service_name, service_hash, controller_pid, controller_ip,
            serve_state.ServiceStatus.SHUTTING_DOWN):
        logger.warning(
            f'Lost ownership before fencing new replica launches for '
            f'{service_name!r}.')
        return

    # The caller has killed/joined the controller child before entering here.
    # A sky.launch request runs in an API-server worker, not in that child, so
    # killing the child alone does not prove launch quiescence. Every request is
    # backed by a replica row created before sdk.launch is submitted.
    try:
        replica_infos = serve_state.get_replica_infos(service_name)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning(f'Could not read replica launch inventory before '
                       f'teardown of {service_name!r}: '
                       f'{common_utils.format_exception(e)}')
        return
    if not serve_utils.quiesce_service_replica_launch_requests(
            service_name,
            replica_infos,
            continue_guard=lambda: serve_state.service_owner_matches(
                service_name, service_hash, (controller_pid, controller_ip)),
            include_terminal_history=(
                serve_utils.replica_cleanup_requires_terminal_history(
                    replica_infos))):
        logger.warning(f'Refusing to acknowledge teardown of {service_name!r} '
                       'until all replica launch requests are terminal.')
        return
    # Publish the child-teardown acknowledgement only after launch requests are
    # terminal. A concurrent purge can now safely proceed to replica cleanup.
    if not serve_state.acknowledge_service_controller_teardown_if_owner(
            service_name, service_hash, controller_pid, controller_ip):
        logger.warning(f'Lost ownership before acknowledging teardown of '
                       f'{service_name!r}.')
        return
    lifecycle_lock = serve_utils.get_service_lifecycle_lock(service_name)
    with lifecycle_lock:
        _run_cleanup_and_finalize_locked(service_name, service_spec,
                                         service_dir, job_id, service_hash,
                                         controller_pid, controller_ip,
                                         lifecycle_lock, resource_scope)


def _run_cleanup_and_finalize_locked(
        service_name: str,
        service_spec: 'service_spec_lib.SkyServiceSpec',
        service_dir: str,
        job_id: int,
        service_hash: str,
        controller_pid: int,
        controller_ip: str | None,
        lifecycle_lock: Any,
        resource_scope: str | None = None) -> None:
    """Owner-fenced cleanup while holding the service lifecycle lock."""
    expected_owner = (controller_pid, controller_ip)
    lifecycle_epoch = serve_utils.get_service_lifecycle_epoch(lifecycle_lock)
    owner_state = serve_state.get_service_controller_owner(
        service_name, include_lb_state=True)
    durable_lb_ha = bool(owner_state and owner_state.get('lb_ha_enabled'))

    def _still_owns() -> bool:
        return (serve_utils.lifecycle_lock_is_valid(lifecycle_lock) and
                serve_state.service_owner_matches(service_name, service_hash,
                                                  expected_owner))

    if not _still_owns():
        logger.warning(f'Skipping cleanup for stale service owner '
                       f'{service_name!r}/{service_hash!r}.')
        return

    lb_quiesced = service_spec.pool
    try:
        if not service_spec.pool:
            # Quiesce the public data plane BEFORE touching replicas. The
            # external LB retains its last routing view when controller sync
            # stops; leaving its Service up during a long cloud teardown would
            # accept new requests and route them to replicas being destroyed.
            # A deletion failure aborts cleanup fail-closed and is retried via
            # FAILED_CLEANUP rather than exposing a half-torn-down service.
            api_deployment_uid = lb_k8s.get_api_deployment_owner_uid(
                require_runtime=True)
            if resource_scope is None:
                lb_k8s.delete_lb_objects(
                    service_name,
                    expected_service_hash=service_hash,
                    require_runtime=True,
                    expected_api_deployment_uid=(api_deployment_uid),
                    high_availability=durable_lb_ha)
            else:
                lb_k8s.delete_lb_objects(
                    service_name,
                    expected_service_hash=service_hash,
                    resource_scope=resource_scope,
                    require_runtime=True,
                    expected_api_deployment_uid=(api_deployment_uid),
                    high_availability=durable_lb_ha)
            lb_quiesced = True
        if not _still_owns():
            raise ServiceOwnershipLostError(
                'Ownership lost after load balancer quiesce.')
        failed = _cleanup(service_name, service_spec.pool, service_hash,
                          controller_pid, controller_ip, lifecycle_lock,
                          resource_scope)
    except ServiceOwnershipLostError as e:
        # Another owner or a lost PG advisory-lock session means this process
        # must stop immediately. Preserve DB state and the recovery script;
        # the authoritative owner will finish teardown.
        logger.warning(f'Aborting stale cleanup for {service_name}: {e}')
        return
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Failed to clean up service {service_name}: {e}')
        with ux_utils.enable_traceback():
            logger.error(f'  Traceback: {traceback.format_exc()}')
        failed = True
        # Publish FAILED_CLEANUP below before removing the recovery script.
        # Reversing that order creates a crash window with no recovery path.

    if failed:
        if not serve_state.set_service_status_and_active_versions_if_owner(
                service_name,
                service_hash,
                controller_pid,
                controller_ip,
                serve_state.ServiceStatus.FAILED_CLEANUP,
                expected_lifecycle_epoch=lifecycle_epoch):
            logger.warning(f'Lost ownership before publishing '
                           f'FAILED_CLEANUP for {service_name!r}.')
            return
        try:
            serve_state.remove_ha_recovery_script_if_owner(
                service_name, service_hash, controller_pid, controller_ip)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Failed to remove recovery script for '
                           f'{service_name!r}: {e}')
        logger.error(f'Service {service_name} failed to clean up.')
        # A FAILED_CLEANUP service is no longer serving, so tear the data-plane
        # LB down here.
        # The DB row is intentionally kept for `--purge`; if the service is
        # ever retried, up() recreates the LB idempotently. Best-effort so a
        # delete failure does not worsen cleanup.
        if not lb_quiesced:
            try:
                api_deployment_uid = lb_k8s.get_api_deployment_owner_uid()
                if resource_scope is None:
                    lb_k8s.delete_lb_objects(
                        service_name,
                        expected_service_hash=service_hash,
                        expected_api_deployment_uid=(api_deployment_uid),
                        high_availability=durable_lb_ha)
                else:
                    lb_k8s.delete_lb_objects(
                        service_name,
                        expected_service_hash=service_hash,
                        resource_scope=resource_scope,
                        expected_api_deployment_uid=(api_deployment_uid),
                        high_availability=durable_lb_ha)
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f'Failed to delete external LB objects for '
                             f'{service_name} during failed cleanup: {e}')
    else:
        if not _still_owns():
            logger.warning(f'Lost ownership before final removal of '
                           f'{service_name!r}.')
            return
        # Legacy name-only directories cannot be proven exclusive: valid
        # names such as `svc-a` and `svc_a` mapped to the same path. Leak that
        # bounded legacy directory rather than deleting a peer's files.
        remove_directory = resource_scope is not None
        removed = serve_state.remove_service_completely(
            service_name,
            service_hash,
            expected_controller_owner=expected_owner,
            expected_lifecycle_epoch=lifecycle_epoch)
        if not removed:
            logger.warning(f'Lost ownership during final removal of '
                           f'{service_name!r}.')
            return
        if remove_directory:
            serve_utils.remove_service_directory(service_dir)
        logger.info(f'Service {service_name} terminated successfully.')

    _cleanup_task_run_script(job_id)


def _validate_recovery_target(service_name: str, record: dict[str, Any],
                              requested_incarnation: str | None,
                              job_id: int) -> None:
    """Rejects a stale controller before it can mutate legacy service state."""
    service_incarnation = record.get('hash')
    if (requested_incarnation is not None and
            requested_incarnation != service_incarnation):
        raise RuntimeError(
            f'Refusing stale controller bootstrap for {service_name!r} '
            f'incarnation {requested_incarnation!r}; current incarnation '
            f'is {service_incarnation!r}.')
    if (requested_incarnation is None and
            serve_utils.is_consolidation_mode(bool(record.get('pool'))) and
            record.get('controller_job_id') != job_id):
        # Recovery scripts produced before --service-incarnation was
        # introduced still carry the original controller job ID. Fence those
        # scripts before workspace backfill or any other durable mutation.
        raise RuntimeError(
            f'Refusing stale controller bootstrap for {service_name!r} '
            f'controller job {job_id!r}; current controller job is '
            f'{record.get("controller_job_id")!r}.')


def _prepare_placement_catalog(
    service_name: str,
    service_spec: 'service_spec_lib.SkyServiceSpec',
    task: task_lib.Task,
    *,
    workspace: str,
    is_recovery: bool,
    recovery_version: int | None,
) -> dict[str, Any] | None:
    """Build a fresh catalog or load/backfill one legacy version."""
    if not service_spec.placement_contract.enabled:
        return None
    if is_recovery:
        if recovery_version is None:
            raise RuntimeError(
                f'Cannot backfill the placement catalog for {service_name!r}: '
                'the service has no committed immutable version.')
        placement_catalog = serve_state.get_placement_catalog(
            service_name, recovery_version)
        if placement_catalog is not None:
            return placement_catalog

    built_catalog = spot_placer.SpotPlacer.build_catalog(service_spec,
                                                         task,
                                                         workspace=workspace)
    assert built_catalog is not None
    candidate_catalog = built_catalog.to_dict()
    if not is_recovery:
        return candidate_catalog

    assert recovery_version is not None
    if serve_state.set_placement_catalog_if_missing(service_name,
                                                    recovery_version,
                                                    candidate_catalog):
        return candidate_catalog
    # A concurrent recovery owner may have completed the same one-time legacy
    # backfill. Its immutable row is authoritative.
    placement_catalog = serve_state.get_placement_catalog(
        service_name, recovery_version)
    if placement_catalog is None:
        raise RuntimeError(
            f'Placement catalog backfill for {service_name!r} version '
            f'{recovery_version} lost its compare-and-set but no winning '
            'catalog is present.')
    return placement_catalog


def _start(service_name: str,
           tmp_task_yaml: str,
           job_id: int,
           entrypoint: str,
           requested_incarnation: str | None = None,
           lifecycle_epoch: int | None = None,
           workspace: str | None = None,
           created_by: str | None = None,
           submitted_task_yaml: str | None = None):
    """Start the service controller and reconcile its external LB."""
    # This check precedes every DB mutation and, critically, the destructive
    # cleanup ``finally`` below. Both fresh and persisted recovery scripts use
    # this entrypoint. Pools remain available, but an unprovable mode is held.
    if maintenance.is_controller_hold_active():
        identity = serve_state.get_service_mode_and_hash(service_name)
        is_pool = identity is not None and identity[0]
        if identity is None:
            try:
                with open(os.path.expanduser(tmp_task_yaml),
                          encoding='utf-8') as task_file:
                    raw_task = yaml_utils.safe_load(task_file.read())
                is_pool = (isinstance(raw_task, dict) and
                           raw_task.get('pool') is not None)
            except Exception as e:
                raise RuntimeError(
                    f'Refusing to start controller {service_name!r}: its mode '
                    'cannot be proven while the server deployment hold is '
                    'active.') from e
        if not is_pool:
            raise RuntimeError(
                'Refusing to start a SkyServe controller while the server '
                'deployment hold is active.')
    raw_recovery_owner_fence = os.environ.pop(
        constants.HA_RECOVERY_OWNER_FENCE_ENV_VAR, None)
    recovery_owner_fence = (
        serve_utils.parse_ha_recovery_owner_fence(raw_recovery_owner_fence)
        if raw_recovery_owner_fence is not None else None)
    # Generate ssh key pair to avoid race condition when multiple sky.launch
    # are executed at the same time.
    auth_utils.get_or_generate_keys()

    service = serve_state.get_service_from_name(service_name)
    is_recovery = service is not None
    if recovery_owner_fence is not None and not is_recovery:
        raise RuntimeError(f'Refusing an HA recovery launch for absent service '
                           f'{service_name!r}.')
    if service is not None:
        _validate_recovery_target(service_name, service, requested_incarnation,
                                  job_id)
        if recovery_owner_fence is not None:
            if recovery_owner_fence['service_hash'] != service.get('hash'):
                raise RuntimeError(
                    f'Refusing stale HA recovery ownership for '
                    f'{service_name!r}: its service incarnation changed.')
            if (recovery_owner_fence['lifecycle_epoch']
                    != service.get('lifecycle_epoch')):
                raise RuntimeError(
                    f'Refusing stale HA recovery ownership for '
                    f'{service_name!r}: its lifecycle epoch changed.')
        workspace_hint = workspace
        if (workspace_hint is None and
                skypilot_config.is_active_workspace_set()):
            workspace_hint = skypilot_config.get_active_workspace()
        workspace = serve_utils.resolve_service_workspace(
            service_name, service, workspace_hint, trusted_recovery_hint=True)
    else:
        workspace = (workspace or skypilot_config.get_active_workspace() or
                     skylet_constants.SKYPILOT_DEFAULT_WORKSPACE)
    # This bit comes from the API-side topology that allocated the lifecycle
    # epoch. It cannot be re-derived in the controller child: run_controller
    # sets OVERRIDE_CONSOLIDATION_MODE for unrelated controller behavior.
    enforce_launch_fence = lifecycle_epoch is not None
    logger.info(f'It is a {"first" if not is_recovery else "recovery"} run')
    if not is_recovery and requested_incarnation is None:
        # Fresh controllers created by this version always carry an API-
        # preallocated incarnation. A name-only process with no current row is
        # necessarily an old/delayed recovery script; letting it invent a new
        # identity can race and block the real same-name successor up.
        raise RuntimeError(
            f'Refusing legacy name-only controller bootstrap for absent '
            f'service {service_name!r}.')
    # Fence every boot-time DB publication to this exact row incarnation. A
    # service name can be purged and reused while controller/LB startup waits
    # for up to several minutes; PID alone is not globally unique across pods.
    service_incarnation: str | None
    recovery_expected_controller_pid: int | None = None
    recovery_expected_controller_ip: str | None = None
    recovery_expected_lifecycle_epoch: int | None = None
    recovery_expected_status: serve_state.ServiceStatus | None = None
    recovery_expected_version: int | None = None
    if is_recovery:
        assert service is not None
        service_incarnation = service.get('hash')
        if recovery_owner_fence is not None:
            recovery_expected_controller_pid = recovery_owner_fence[
                'controller_pid']
            recovery_expected_controller_ip = recovery_owner_fence[
                'controller_ip']
            recovery_expected_lifecycle_epoch = recovery_owner_fence[
                'lifecycle_epoch']
            recovery_expected_status = recovery_owner_fence['status']
            recovery_expected_version = recovery_owner_fence['recovery_version']
        else:
            recovery_expected_controller_pid = service.get('controller_pid')
            recovery_expected_controller_ip = service.get('controller_ip')
        resource_scope = service.get('resource_scope')
    else:
        # add_service accepts the caller-generated UUID while preserving its
        # historical bool return, so this process knows the committed hash
        # without a racy name-only read after insertion.
        service_incarnation = requested_incarnation
        resource_scope = service_incarnation
    if not isinstance(service_incarnation, str) or not service_incarnation:
        raise RuntimeError(
            f'Service {service_name!r} has no durable incarnation hash.')
    # Pod IP for full controller-owner fencing, including teardown recovery.
    pod_ip: str | None = os.environ.get('POD_IP')

    def _read_yaml_content(yaml_path: str) -> str:
        with open(os.path.expanduser(yaml_path), encoding='utf-8') as f:
            return f.read()

    # On recovery, resume the latest applicable committed version, not raw
    # MAX(version). This skips both interrupted NULL-yaml placeholders and
    # versions whose deterministic preflight failure was durably quarantined.
    recovery_snapshot = (serve_state.get_recovery_version_spec(service_name)
                         if is_recovery else None)
    recovery_version = (recovery_snapshot[0]
                        if recovery_snapshot is not None else None)
    if (recovery_expected_version is not None and
            recovery_version != recovery_expected_version):
        raise RuntimeError(
            f'Refusing stale HA recovery ownership for {service_name!r}: '
            f'elected version changed from {recovery_expected_version} to '
            f'{recovery_version}.')

    # The HA daemon restores before launching this process so imports see a
    # valid file. Reconcile again after selecting the quarantine-aware version:
    # an update or quarantine transition may have raced the daemon's snapshot.
    if is_recovery and recovery_version is not None:
        live_config_path = (
            serve_utils.generate_versioned_config_yaml_file_name(
                service_name, recovery_version, resource_scope))
        recovery_config_snapshot = serve_state.get_version_controller_config(
            service_name, recovery_version)
        if recovery_config_snapshot is not None:
            staged_config_path = (
                serve_utils.generate_staged_config_yaml_file_name(
                    service_name,
                    recovery_version,
                    resource_scope,
                    snapshot_id=recovery_config_snapshot[2]))
            with filelock.FileLock(
                    skypilot_config.get_skypilot_config_lock_path()):
                recovery_config_bytes = (
                    serve_utils.restore_version_controller_config(
                        service_name,
                        recovery_version,
                        live_config_path,
                        staged_config_path,
                        expected_workspace=workspace))
                assert recovery_config_bytes is not None
                recovered_config = (
                    serve_utils.parse_and_validate_version_controller_config(
                        recovery_config_bytes, workspace,
                        'committed Serve controller recovery config'))

                def _publish_recovery_config() -> None:
                    skypilot_config.install_internal_config_snapshot(
                        recovered_config, live_config_path)

                contextvars.Context().run(_publish_recovery_config)
                serve_utils.scrub_obsolete_controller_config_files(
                    service_name, recovery_version, resource_scope)

    if is_recovery:
        assert service is not None
        if recovery_version is not None:
            yaml_content = serve_state.get_yaml_content(service_name,
                                                        recovery_version)
        else:
            # No committed version yet; fall back to the joined record.
            yaml_content = service['yaml_content']
        # Backward compatibility for old service records that
        # does not dump the yaml content to version database.
        # TODO(tian): Remove this after 2 minor releases, i.e. 0.13.0.
        if yaml_content is None:
            yaml_content = _read_yaml_content(tmp_task_yaml)
    else:
        yaml_content = _read_yaml_content(tmp_task_yaml)
    submitted_yaml_content = (_read_yaml_content(submitted_task_yaml)
                              if not is_recovery and
                              submitted_task_yaml is not None else None)

    # Initialize database record for the service.
    authoritative_service_spec = (recovery_snapshot[1]
                                  if recovery_snapshot is not None else None)
    task = replica_managers.load_task_with_service_spec(
        yaml_content, authoritative_service_spec)
    # Already checked before submit to controller.
    assert task.service is not None, task
    # The task YAML remains authoritative for resources and execution, but its
    # service policy must not be reinterpreted under newer hidden defaults on
    # recovery. The immutable pickled spec is the committed semantic record.
    service_spec = task.service

    service_dir = os.path.expanduser(
        serve_utils.generate_remote_service_dir_name(service_name,
                                                     resource_scope))

    # If the previous controller died mid-teardown, its HA recovery script was
    # preserved throughout _cleanup. Bringing the controller + LB back up here
    # would resurrect a service the user tore down -- so resume the unfinished
    # cleanup instead.
    if _should_resume_teardown(is_recovery, service):
        assert service is not None
        claimed = serve_state.update_service_controller_pid_if_owner(
            service_name,
            expected_service_hash=service_incarnation,
            expected_controller_pid=recovery_expected_controller_pid,
            expected_controller_ip=recovery_expected_controller_ip,
            controller_pid=os.getpid(),
            controller_ip=pod_ip,
            expected_lifecycle_epoch=recovery_expected_lifecycle_epoch,
            expected_status=recovery_expected_status,
            expected_recovery_version=recovery_expected_version)
        _exit_on_ownership_loss(claimed, service_name,
                                'claiming teardown recovery', None)
        logger.info(f'Recovering service {service_name} in status '
                    f'{service["status"].value}: resuming teardown instead of '
                    'serving.')
        _run_cleanup_and_finalize(service_name, service_spec,
                                  service_dir, job_id, service_incarnation,
                                  os.getpid(), pod_ip, resource_scope)
        return

    # Pools intentionally have no inference endpoint. Every real SkyServe
    # service, however, uses the controller-owned Kubernetes LB; there is no
    # in-pod fallback. Validate the platform contract before creating a fresh
    # DB row (and before a recovery claims an existing row) so a configuration
    # error fails clearly instead of publishing an unreachable endpoint.
    external_lb = not service_spec.pool
    lb_termination_grace_seconds = 0
    if external_lb:
        # Re-check persisted specs on every fresh start and HA recovery. Older
        # rows (or an interrupted update from a mixed-version deployment) may
        # predate the API-side guard; advertising HTTPS for this HTTP-only LB
        # would otherwise create a durable dead endpoint.
        serve_utils.validate_external_lb_service_spec(service_spec)
        lb_k8s.require_external_lb_runtime()
        existing_lb_state = serve_state.get_lb_cutover_state(service_name)
        if (service_spec.lb_high_availability or
            (existing_lb_state is not None and existing_lb_state.enabled)):
            lb_k8s.require_lb_ha_runtime()
        lb_termination_grace_seconds = (
            lb_k8s.lb_termination_grace_period_seconds(
                service_spec.lb_stream_timeout_seconds,
                service_spec.graceful_drain_seconds))

    # Validate the task-aware logical topology immediately before the durable
    # service/version write too. This protects direct/older API callers and
    # admin-policy mutations that bypassed an earlier server-side validation.
    serve_utils.validate_logical_replica_task(task, service_spec)

    initial_controller_config: bytes | None = None
    initial_controller_config_digest: str | None = None
    initial_controller_config_snapshot_id: str | None = None
    if not is_recovery and lifecycle_epoch is not None:
        live_config_path = (serve_utils.generate_remote_config_yaml_file_name(
            service_name, resource_scope))
        try:
            with open(os.path.expanduser(live_config_path),
                      'rb') as config_file:
                raw_controller_config = config_file.read()
        except OSError as e:
            raise RuntimeError(
                'Consolidated controller config is unavailable before '
                'service registration.') from e
        initial_controller_config = (
            serve_utils.sanitize_ha_recovery_config_bytes(raw_controller_config)
        )
        initial_controller_config_digest = hashlib.sha256(
            initial_controller_config).hexdigest()
        initial_controller_config_snapshot_id = secrets.token_hex(32)
        serve_utils.parse_and_validate_version_controller_config(
            initial_controller_config, workspace,
            'initial durable Serve controller config')

    placement_catalog = _prepare_placement_catalog(
        service_name,
        service_spec,
        task,
        workspace=workspace,
        is_recovery=is_recovery,
        recovery_version=recovery_version)

    if not is_recovery:
        with filelock.FileLock(controller_utils.get_resources_lock_path()):
            if not controller_utils.can_start_new_process(task.service.pool):
                cleanup_storage(yaml_content, resource_scope)
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        controller_utils.get_max_services_error_message(
                            task.service.pool))
            # Create the service working directory before the DB write so a
            # crash here can at most leave a harmless empty dir. The service
            # row and its initial version row are then written atomically
            # below: writing them as two separate commits leaves a crash
            # window that strands a `services` row with no `version_specs`
            # row, which the latest-version inner join hides from status,
            # recovery and teardown, and which blocks re-`up` of the name.
            os.makedirs(service_dir, exist_ok=True)
            version = constants.INITIAL_VERSION
            try:
                success = serve_state.add_service(
                    service_name,
                    controller_job_id=job_id,
                    policy=service_spec.autoscaling_policy_str(),
                    requested_resources_str=(
                        backend_utils.get_task_resources_str(task)),
                    load_balancing_policy=service_spec.load_balancing_policy,
                    status=serve_state.ServiceStatus.CONTROLLER_INIT,
                    tls_encrypted=service_spec.tls_credential is not None,
                    pool=service_spec.pool,
                    controller_pid=os.getpid(),
                    controller_ip=pod_ip,
                    spec=service_spec,
                    yaml_content=yaml_content,
                    workspace=workspace,
                    entrypoint=entrypoint,
                    service_hash=service_incarnation,
                    lifecycle_epoch=lifecycle_epoch,
                    resource_scope=resource_scope,
                    created_by=created_by,
                    submitted_yaml_content=submitted_yaml_content,
                    placement_catalog=placement_catalog,
                    controller_config=initial_controller_config,
                    controller_config_digest=(initial_controller_config_digest),
                    controller_config_snapshot_id=(
                        initial_controller_config_snapshot_id))
            except (serve_state.OrphanedReplicaRecordsError,
                    serve_state.OrphanedStorageCleanupIntentsError,
                    serve_state.OrphanedVersionRecordsError):
                cleanup_storage(yaml_content, resource_scope)
                raise
        # Directly throw an error here. See sky/serve/api.py::up
        # for more details.
        if not success:
            # The task manifest permits deletion only for the preallocated
            # incarnation's disjoint storage generation. A same-name winner
            # has a different scope and cannot be touched here.
            cleanup_storage(yaml_content, resource_scope)
            with ux_utils.print_exception_no_traceback():
                raise ValueError(f'Service {service_name} already exists.')
    else:
        # Use the latest COMMITTED version (computed above), not raw
        # MAX(version), so an interrupted-update NULL-yaml placeholder does not
        # wedge the controller on boot.
        if recovery_version is not None:
            version = recovery_version
            # An interrupted `sky serve update` may have left NULL-yaml
            # placeholder versions above the committed version (add_version
            # inserts the row before the yaml is persisted). Leave them in
            # place: recovery can also run while the API server stays up and
            # keeps serving updates (e.g. only the controller process died),
            # so deleting them could race an in-flight update and re-open its
            # version number for reuse by a concurrent update. They are
            # otherwise inert -- recovery and respawn boot from the latest
            # committed version, and a later update allocates MAX(version)+1
            # above them. The interrupted update itself is unrecoverable (its
            # yaml was never persisted), so warn instead of silently reverting
            # it.
            raw_latest = serve_state.get_latest_version(service_name)
            if raw_latest is not None and raw_latest > version:
                logger.warning(
                    f'Service {service_name} has uncommitted version(s) up to '
                    f'{raw_latest} left by an interrupted update; recovering '
                    f'at the latest committed version {version}.')
        else:
            # Nothing committed yet (e.g. an old record that predates
            # yaml-in-DB, recovered via the tmp-yaml fallback above). Fall back
            # to raw latest.
            latest_version = serve_state.get_latest_version(service_name)
            if latest_version is None:
                raise ValueError(f'No version found for service {service_name}')
            version = latest_version
        # Pre-claim controller_pid immediately so the next
        # ha_recovery_for_consolidation_mode iteration sees our _start
        # process as the live controller and does NOT fire a duplicate
        # recovery script while we are still booting (the controller boot
        # window is up to SERVICE_REGISTER_TIMEOUT_SECONDS, but the
        # daemon retries every ~20s). _controller_process_alive matches by
        # `--service-name <name>` in cmdline, which our _start process has.
        #
        # Atomically move PID+IP ownership now and clear controller_port. This
        # makes the stable proxy fail closed with 503 during boot instead of
        # routing to the dead prior owner. The ready port is published only
        # after _wait_for_controller_ready succeeds below.
        claimed = serve_state.update_service_controller_pid_if_owner(
            service_name,
            expected_service_hash=service_incarnation,
            expected_controller_pid=recovery_expected_controller_pid,
            expected_controller_ip=recovery_expected_controller_ip,
            controller_pid=os.getpid(),
            controller_ip=pod_ip,
            expected_lifecycle_epoch=recovery_expected_lifecycle_epoch,
            expected_status=recovery_expected_status,
            expected_recovery_version=recovery_expected_version)
        _exit_on_ownership_loss(claimed, service_name, 'preclaiming recovery',
                                None)

    controller_process = None
    external_lb_healthy = not external_lb
    # Tracks whether we exited the main loop via the user-initiated
    # SHUTTING_DOWN signal. We can't recover this from sys.exc_info() in
    # the finally block — Python clears the active exception when the
    # corresponding `except` clause catches it, so sys.exc_info() in
    # the finally returns (None, None, None) for the caught path.
    shutdown_via_user_signal = False
    try:

        def _get_controller_host():
            """Get the controller host address.
            We expose the controller to the public network when running
            inside a kubernetes cluster to allow external load balancers
            (example, for high availability load balancers) to communicate
            with the controller.
            """
            if 'KUBERNETES_SERVICE_HOST' in os.environ:
                return '0.0.0.0'
            # Not using localhost to avoid using ipv6 address and causing
            # the following error:
            # ERROR:    [Errno 99] error while attempting to bind on address
            # ('::1', 20001, 0, 0): cannot assign requested address
            return '127.0.0.1'

        controller_host = _get_controller_host()
        with _spawn_controller_on_reserved_port(
                service_name, service_spec, version, controller_host,
                service_incarnation, pod_ip, resource_scope,
                enforce_launch_fence) as (controller_process, controller_port):
            logger.debug(f'_start() spawned controller_process pid='
                         f'{controller_process.pid} host={controller_host} '
                         f'port={controller_port}')

            # The parent duplicate reserves `controller_port`, so independent
            # services can select other ports while this controller
            # reconstructs. Do not publish until Uvicorn is listening.
            try:
                _wait_for_controller_ready(
                    controller_host,
                    controller_port,
                    timeout=constants.SERVICE_REGISTER_TIMEOUT_SECONDS,
                    process=controller_process)
                if not controller_process.is_alive():
                    raise RuntimeError(
                        'controller exited during startup publication')
            except RuntimeError as boot_err:
                # Bail without falling through to the outer try/finally, which
                # would enter destructive cleanup and possibly remove the
                # service incarnation. See helper for details.
                _bail_on_boot_failure(
                    service_name, controller_process,
                    constants.SERVICE_REGISTER_TIMEOUT_SECONDS, boot_err)

            # Publish the complete owner tuple only if the exact row inserted
            # or preclaimed above still belongs to this process. This protects
            # both recovery and fresh-up from a purge + same-name re-up during
            # the readiness wait (including equal PIDs on different pods).
            logger.debug(f'Publishing DB controller_pid -> {os.getpid()}, '
                         f'controller_ip -> {pod_ip}, controller_port -> '
                         f'{controller_port}, service_hash -> '
                         f'{service_incarnation}')
            published = (serve_state.update_service_controller_pid_ip_and_port(
                service_name,
                controller_pid=os.getpid(),
                controller_ip=pod_ip,
                controller_port=controller_port,
                expected_service_hash=service_incarnation,
                expected_controller_pid=os.getpid(),
                expected_controller_ip=pod_ip))
            _exit_on_ownership_loss(published, service_name,
                                    'publishing the ready controller',
                                    controller_process)

        # Keep the historical load_balancer_port field as the registration
        # sentinel/API compatibility value. Real services expose this fixed
        # port on their per-service Kubernetes Service; pools have no endpoint
        # but still need a non-null registration sentinel.
        load_balancer_port = constants.LOAD_BALANCER_PORT_START

        # In external load balancer mode, ensure the controller-owned per-
        # service LB Deployment + Service exist BEFORE the load_balancer_port
        # DB write below. wait_service_registration returns as soon as
        # load_balancer_port is non-null, so writing it first would let
        # `sky serve up` report the endpoint before the LB Service exists.
        # Creating the LB objects first closes that window. Idempotent (409 ==
        # already exists), so it is safe on the recovery path too. No-op outside
        # external-LB + in-cluster mode. The controller owner tuple is already
        # recorded in DB.
        if external_lb:
            try:

                def _still_owns_lb() -> bool:
                    return serve_state.service_owner_matches(
                        service_name, service_incarnation,
                        (os.getpid(), pod_ip))

                lb_k8s.create_lb_deployment_and_service(
                    service_name,
                    lb_termination_grace_seconds,
                    service_hash=service_incarnation,
                    resource_scope=resource_scope,
                    continue_guard=_still_owns_lb,
                    high_availability=bool(
                        (serve_state.get_service_controller_owner(
                            service_name, include_lb_state=True) or
                         {}).get('lb_ha_enabled')))
                external_lb_healthy = True
            except Exception as boot_err:  # pylint: disable=broad-except
                _bail_on_boot_failure(
                    service_name,
                    controller_process,
                    constants.LB_DEPLOYMENT_READY_TIMEOUT_SECONDS,
                    boot_err,
                    component='External load balancer')

        # Publish load_balancer_port only after the external LB objects exist,
        # so registration unblocks once the data plane has been materialized.
        # Pools use the field only as the legacy registration sentinel.
        if not is_recovery:
            registered = serve_state.set_service_load_balancer_port_if_owner(
                service_name, service_incarnation, os.getpid(), pod_ip,
                load_balancer_port)
            _exit_on_ownership_loss(registered, service_name,
                                    'publishing registration',
                                    controller_process)
        # On recovery in external mode, re-publish the (constant) external
        # port. This heals two stale-row shapes: a service migrated from
        # in-pod mode still records its legacy in-pod port, and an up() that
        # crashed between row creation and registration left the port NULL
        # (registration would starve on recovery without this). Compare-and-
        # swap on the preclaimed hash/PID/IP owner tuple so a stale recovery
        # racing a purge + same-name re-up cannot write to the successor's row
        # and prematurely unblock its registration. A
        # transient DB error must not reach _start's destructive cleanup and
        # must not starve the NULL case either, so the attempt is retried
        # from the supervision loop until the CAS resolves (True: written;
        # False: ownership lost, someone else owns the row now).
        lb_port_republish_pending = is_recovery and external_lb
        if lb_port_republish_pending:
            try:
                registered = (
                    serve_state.set_service_load_balancer_port_if_owner(
                        service_name, service_incarnation, os.getpid(), pod_ip,
                        load_balancer_port))
                _exit_on_ownership_loss(registered, service_name,
                                        're-publishing registration',
                                        controller_process)
                lb_port_republish_pending = False
            except Exception as e:  # pylint: disable=broad-except
                logger.warning(
                    f'Failed to republish load_balancer_port for '
                    f'{service_name}: {common_utils.format_exception(e)}; '
                    'will retry from the supervision loop.')

        # How often to check that the controller child is still alive and
        # respawn it if it died (a cheap local is_alive() poll). Capped at this
        # cadence so a controller that crash-loops on boot is respawned at most
        # once per interval; actual respawns additionally honor the
        # exponential backoff below.
        controller_respawn_check_interval_seconds = 5
        # How often to re-ensure the external LB Deployment + Service exist.
        # Self-heal for out-of-band deletion: the k8s Deployment respawns its
        # own pod, but nothing else recreates a deleted Deployment/Service
        # until the next HA recovery. Steady state is two GETs per interval.
        external_lb_ensure_interval_seconds = 60
        own_pid = os.getpid()
        loop_count = 0
        # Degraded-status accounting and dead-child respawn attempts have
        # independent backoff clocks. External-LB failures must never delay
        # the first observation of a real controller-child death.
        child_failures = 0
        supervision_backoff = _ControllerSupervisionBackoff()
        controller_unresponsive_since: float | None = None
        # Whether the DB status may need healing on the next confirmed-healthy
        # check. Starts True: an HA-recovered service may carry
        # CONTROLLER_FAILED from the status refresh daemon (set while the old
        # parent was dead), which the replica-driven writer never clears.
        needs_status_heal = True
        while True:
            # Resolve exact ownership before consuming the name-scoped wakeup
            # file or allowing another child tick. SHUTTING_DOWN in the DB is
            # the durable, cross-pod terminate signal; the file only reduces
            # latency for legacy/local controllers.
            owner = None
            try:
                owner = serve_state.get_service_controller_owner(
                    service_name, include_lb_state=True)
            except Exception as e:  # pylint: disable=broad-except
                logger.warning(f'Failed to verify service owner before '
                               f'supervision tick: '
                               f'{common_utils.format_exception(e)}')
                time.sleep(1)
                continue
            if owner is None:
                logger.warning(f'Service {service_name!r} disappeared before '
                               'supervision tick; exiting as orphan.')
                _orphan_exit(controller_process)
            if (owner.get('hash') != service_incarnation or
                    owner.get('controller_pid') != own_pid or
                    owner.get('controller_ip') != pod_ip):
                logger.warning(f'Service {service_name!r} ownership changed '
                               'before supervision tick; exiting as orphan.')
                _orphan_exit(controller_process)
            if owner['status'] == serve_state.ServiceStatus.SHUTTING_DOWN:
                raise exceptions.ServeUserTerminatedError(
                    'Durable SHUTTING_DOWN state observed.')
            if not _handle_signal(service_name, service_incarnation, own_pid,
                                  pod_ip, resource_scope):
                _orphan_exit(controller_process)
            loop_count += 1
            # Self-heal the external LB objects. Best-effort: a k8s API error
            # must never reach _start's destructive cleanup.
            if (external_lb and
                    loop_count % external_lb_ensure_interval_seconds == 0):
                if lb_port_republish_pending:
                    try:
                        # Either outcome resolves the retry: True means the
                        # row is healed, False means ownership was lost and
                        # the write is no longer ours to make.
                        registered = (
                            serve_state.set_service_load_balancer_port_if_owner(
                                service_name, service_incarnation, os.getpid(),
                                pod_ip, load_balancer_port))
                        _exit_on_ownership_loss(registered, service_name,
                                                'retrying registration',
                                                controller_process)
                        lb_port_republish_pending = False
                    except Exception as e:  # pylint: disable=broad-except
                        logger.warning(
                            f'Failed to republish load_balancer_port for '
                            f'{service_name}: '
                            f'{common_utils.format_exception(e)}; will retry.')
                try:
                    latest_lb_grace_seconds = (
                        _get_latest_committed_lb_termination_grace_seconds(
                            service_name))
                    if latest_lb_grace_seconds is not None:
                        lb_termination_grace_seconds = (latest_lb_grace_seconds)
                    external_lb_healthy = lb_k8s.ensure_lb_objects_exist(
                        service_name,
                        lb_termination_grace_seconds,
                        service_hash=service_incarnation,
                        resource_scope=resource_scope,
                        controller_ip=pod_ip,
                        high_availability=bool(owner.get('lb_ha_enabled')))
                except Exception as e:  # pylint: disable=broad-except
                    external_lb_healthy = False
                    logger.warning(
                        f'Failed to ensure external LB objects for '
                        f'{service_name}: {common_utils.format_exception(e)}; '
                        'will retry.')
            # Keep the controller child alive while we own the DB row. HA
            # recovery does not cover a child dying while its parent remains
            # alive; without this, autoscaling/probing/reconciliation stop
            # permanently. The LB itself is owned by Kubernetes.
            if loop_count % controller_respawn_check_interval_seconds == 0:
                now = time.time()
                healthy = False
                controller_responding = False
                controller_needs_respawn = _controller_child_needs_respawn(
                    service_name, controller_process)
                if not controller_needs_respawn:
                    controller_responding = _controller_child_responding(
                        service_name, service_incarnation, pod_ip,
                        controller_port)
                    if controller_responding:
                        controller_unresponsive_since = None
                    else:
                        if controller_unresponsive_since is None:
                            controller_unresponsive_since = now
                        logger.warning(
                            f'Controller supervision '
                            f'action=hold_live_child service={service_name} '
                            f'parent_pid={own_pid} '
                            f'child_pid='
                            f'{_process_pid_or_none(controller_process)} '
                            f'is_alive=true http_healthy=false '
                            f'health_miss_age_seconds='
                            f'{now - controller_unresponsive_since:.1f} '
                            f'external_lb_healthy={external_lb_healthy}.')
                if controller_needs_respawn:
                    if supervision_backoff.respawn_is_due(
                            service_name, controller_process, now):
                        logger.warning(
                            f'Controller supervision '
                            f'action=respawn_dead_child '
                            f'service={service_name} parent_pid={own_pid} '
                            f'child_pid='
                            f'{_process_pid_or_none(controller_process)} '
                            f'is_alive=false.')
                        result = _respawn_controller(
                            service_name,
                            controller_host,
                            controller_process,
                            service_hash=service_incarnation,
                            controller_ip=pod_ip,
                            resource_scope=resource_scope,
                            enforce_launch_fence=enforce_launch_fence)
                        if result is not None:
                            controller_process, controller_port = result
                            supervision_backoff.record_respawn_success()
                            controller_unresponsive_since = None
                            controller_responding = True
                            healthy = external_lb_healthy
                        else:
                            supervision_backoff.record_respawn_failure(now)
                            child_failures += 1
                else:
                    healthy = controller_responding and external_lb_healthy
                    health_miss_graced = _controller_health_miss_is_graced(
                        controller_responding, controller_needs_respawn,
                        external_lb_healthy)
                    if (not healthy and not health_miss_graced and
                            supervision_backoff.degraded_retry_is_due(now)):
                        child_failures += 1
                        supervision_backoff.record_degraded_failure(
                            now, child_failures)
                if healthy:
                    child_failures = 0
                    supervision_backoff.record_healthy()
                    if needs_status_heal and _heal_service_degraded(
                            service_name, service_incarnation, own_pid, pod_ip):
                        # Only stop retrying once the heal is confirmed; a
                        # transient DB failure during the heal would
                        # otherwise leave the service stuck CONTROLLER_FAILED
                        # (the replica-driven writer is blocked on it).
                        needs_status_heal = False
                elif (not _controller_health_miss_is_graced(
                        controller_responding, controller_needs_respawn,
                        external_lb_healthy) and
                      child_failures >= _CHILD_FAILURES_BEFORE_FLAG):
                    _flag_service_degraded(service_name, service_incarnation,
                                           own_pid, pod_ip)
                    needs_status_heal = True
            time.sleep(1)
    except exceptions.ServeUserTerminatedError:
        logger.debug(f'Caught ServeUserTerminatedError for '
                     f'{service_name}; setting status=SHUTTING_DOWN')
        shutdown_via_user_signal = True
    finally:
        # Log why we're entering the destructive cleanup path. Finalization
        # can remove the HA recovery script and the entire service row, so an
        # audit line (especially with the active exception type if any) is
        # worth it for future post-mortems.
        # The path (`_wait_for_controller_ready` timeout) and
        # `_orphan_exit` both bypass this finally entirely via
        # os._exit; anything else reaching here is either the user
        # signal (flag above) or an unexpected propagating exception.
        exc_type, exc_value, _ = sys.exc_info()
        if shutdown_via_user_signal:
            logger.info(f'_start for {service_name} entering cleanup path '
                        '(user-initiated SHUTTING_DOWN).')
        elif exc_type is None:
            # _start's `while True` only exits via exception or
            # os._exit, so a None exc_type here is unexpected. Log it
            # at WARN so a future code path that adds a `return`
            # doesn't silently slip into destructive cleanup unnoticed.
            logger.warning(
                f'_start for {service_name} entering cleanup path with '
                'no exception and no user signal — this is unexpected; '
                '_cleanup is destructive.')
        else:
            logger.warning(
                f'_start for {service_name} entering cleanup path due to '
                f'unexpected exception {exc_type.__name__}: {exc_value}. '
                f'finalization may delete the HA recovery script and service '
                f'row.')
        if controller_process is not None:
            subprocess_utils.kill_children_processes(
                parent_pids=[controller_process.pid], force=True)
            controller_process.join()

        # Run cleanup + finalize. _run_cleanup_and_finalize catches any error
        # from _cleanup and sets FAILED_CLEANUP instead, so the service can
        # still be terminated later (a crash here would otherwise leave no
        # process to handle the user signal). Shared with the recovery-resume
        # path above.
        _run_cleanup_and_finalize(service_name, service_spec,
                                  service_dir, job_id, service_incarnation,
                                  os.getpid(), pod_ip, resource_scope)


if __name__ == '__main__':
    logger.info('Starting service...')

    parser = argparse.ArgumentParser(description='Sky Serve Service')
    parser.add_argument('--service-name',
                        type=str,
                        help='Name of the service',
                        required=True)
    parser.add_argument('--workspace',
                        type=str,
                        help='Durable workspace for replica launches')
    parser.add_argument('--service-incarnation',
                        type=str,
                        help='Preallocated service incarnation/resource scope')
    parser.add_argument('--lifecycle-epoch',
                        type=int,
                        help='Durable lifecycle fencing token for fresh add')
    parser.add_argument('--created-by',
                        type=str,
                        help='User that requested the initial version')
    parser.add_argument('--task-yaml',
                        type=str,
                        help='Task YAML file',
                        required=True)
    parser.add_argument('--submitted-task-yaml',
                        type=str,
                        help='User-submitted task YAML file')
    parser.add_argument('--job-id',
                        required=True,
                        type=int,
                        help='Job id for the service job.')
    parser.add_argument('--entrypoint',
                        type=str,
                        help='Entrypoint to launch the service',
                        required=True)
    args = parser.parse_args()
    # We start process with 'spawn', because 'fork' could result in weird
    # behaviors; 'spawn' is also cross-platform.
    multiprocessing.set_start_method('spawn', force=True)
    cli_workspace = args.workspace
    service_record = serve_state.get_service_from_name(args.service_name)
    if service_record is not None:
        _validate_recovery_target(args.service_name, service_record,
                                  args.service_incarnation, args.job_id)
        cli_workspace_hint = cli_workspace
        if (cli_workspace_hint is None and
                skypilot_config.is_active_workspace_set()):
            cli_workspace_hint = skypilot_config.get_active_workspace()
        cli_workspace = serve_utils.resolve_service_workspace(
            args.service_name,
            service_record,
            cli_workspace_hint,
            trusted_recovery_hint=True)
    elif cli_workspace is None:
        cli_workspace = skypilot_config.get_active_workspace()
    workspace_context = (
        skypilot_config.local_active_workspace_ctx(cli_workspace)
        if cli_workspace is not None else contextlib.nullcontext())
    with workspace_context:
        _start(args.service_name,
               args.task_yaml,
               args.job_id,
               args.entrypoint,
               args.service_incarnation,
               args.lifecycle_epoch,
               workspace=cli_workspace,
               created_by=args.created_by,
               submitted_task_yaml=args.submitted_task_yaml)
