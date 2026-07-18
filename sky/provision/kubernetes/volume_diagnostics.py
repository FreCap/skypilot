"""Kubernetes persistent-volume health diagnostics."""

from collections.abc import Callable
from typing import Any

from sky import models
from sky import sky_logging
from sky.adaptors import kubernetes
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.utils import volume as volume_lib

logger = sky_logging.init_logger(__name__)

PVC_FAILING_EVENT_REASONS = ('ProvisioningFailed',)
WARNING_EVENT_TYPE = 'Warning'


def get_all_volumes_errors(
    configs: list[models.VolumeConfig],
    get_context_namespace: Callable[[models.VolumeConfig], tuple[str, str]],
) -> dict[str, str | None]:
    """Gets error messages for all Kubernetes PVC volumes.

    Checks if PVCs are in Pending state and if so, checks for access mode
    mismatches between the PVC and the storage class's allowed access modes.
    """
    context_to_namespaces: dict[str, set[str]] = {}
    config_by_pvc_name: dict[str, dict[str, models.VolumeConfig]] = {}

    for config in configs:
        # Skip hostPath volumes — they have no PVC to check
        if config.type == volume_lib.VolumeType.HOSTPATH.value:
            continue
        context, namespace = get_context_namespace(config)
        context_to_namespaces.setdefault(context, set()).add(namespace)
        config_by_pvc_name.setdefault(context,
                                      {})[config.name_on_cloud] = config

    volume_errors: dict[str, str | None] = {}

    for context, namespaces in context_to_namespaces.items():
        for namespace in namespaces:
            try:
                # List all PVCs in the namespace with the skypilot label
                pvcs = kubernetes.core_api(
                    context).list_namespaced_persistent_volume_claim(
                        namespace=namespace,
                        label_selector='parent=skypilot',
                        _request_timeout=kubernetes.API_TIMEOUT)
            except Exception as e:  # pylint: disable=broad-except
                logger.debug(f'Failed to get PVCs in namespace {namespace} '
                             f'in context {context}: {e}')
                continue

            for pvc in pvcs.items:
                pvc_name = pvc.metadata.name
                vol_config = config_by_pvc_name.get(context, {}).get(pvc_name)
                if vol_config is None:
                    continue

                volume_name = vol_config.name
                pvc_phase = pvc.status.phase

                # If PVC is bound, no error
                if pvc_phase == 'Bound':
                    volume_errors[volume_name] = None
                    continue

                # If PVC is pending, check for access mode mismatch
                if pvc_phase == 'Pending':
                    error_msg = _check_pvc_access_mode_error(context, pvc)
                    if error_msg:
                        volume_errors[volume_name] = error_msg
                    else:
                        volume_binding_mode = (
                            _check_storage_class_volume_binding_mode(
                                context, pvc))
                        if (volume_binding_mode is None or
                                volume_binding_mode == 'WaitForFirstConsumer'):
                            error_msg = None
                            pvc_events = kubernetes_utils.get_pvc_events(
                                context, namespace, pvc_name)
                            for event in pvc_events:
                                if (event.type == WARNING_EVENT_TYPE or
                                        event.reason
                                        in PVC_FAILING_EVENT_REASONS):
                                    reason_str = event.reason
                                    if event.message:
                                        reason_str += f': {event.message}'
                                    error_msg = (f'PVC is pending. '
                                                 f'{reason_str}. To debug, run'
                                                 f': kubectl describe pvc '
                                                 f'{pvc_name} -n {namespace}')
                                    break
                            volume_errors[volume_name] = error_msg
                        else:
                            # Generic pending message if no specific error
                            # detected
                            volume_errors[volume_name] = (
                                'PVC is pending. This may be due to '
                                'insufficient storage resources or '
                                'misconfiguration. To debug, run: '
                                f'kubectl describe pvc {pvc_name} -n '
                                f'{namespace}')
                elif pvc_phase == 'Lost':
                    volume_errors[volume_name] = (
                        'PVC is in Lost state. The bound PersistentVolume '
                        'has been deleted or is unavailable. To debug, '
                        f'run: kubectl describe pvc {pvc_name} -n {namespace}')
                else:
                    # Other phases (e.g., Terminating)
                    volume_errors[volume_name] = None

    return volume_errors


def _check_storage_class_volume_binding_mode(context: str | None,
                                             pvc: Any) -> str | None:
    """Check the volumeBindingMode of the storage class for the PVC.

    Args:
        context: Kubernetes context
        pvc: V1PersistentVolumeClaim object

    Returns:
        volumeBindingMode of the storage class for the PVC,
        None if failed to read the storage class.
    """
    storage_class_name = pvc.spec.storage_class_name
    if not storage_class_name:
        return None
    try:
        storage_class = kubernetes.storage_api(context).read_storage_class(
            name=storage_class_name, _request_timeout=kubernetes.API_TIMEOUT)
        return storage_class.volume_binding_mode
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'Failed to read storage class {storage_class_name}: {e}')
        return None


def _check_pvc_access_mode_error(context: str | None, pvc: Any) -> str | None:
    """Check if a pending PVC has an access mode mismatch.

    Args:
        context: Kubernetes context
        pvc: V1PersistentVolumeClaim object

    Returns:
        Error message if there's an access mode mismatch, None otherwise.
    """
    pvc_access_modes = pvc.spec.access_modes or []
    if not pvc_access_modes:
        return None

    pvc_access_mode = pvc_access_modes[0]
    storage_class_name = pvc.spec.storage_class_name

    # Try to find available PVs and check their access modes
    try:
        pvs = kubernetes.core_api(context).list_persistent_volume(
            _request_timeout=kubernetes.API_TIMEOUT)
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'Failed to list PVs: {e}')
        return None

    # Filter PVs that match the storage class and are available
    available_pvs = []
    for pv in pvs.items:
        # Check if PV matches storage class
        pv_storage_class = pv.spec.storage_class_name
        if storage_class_name and pv_storage_class != storage_class_name:
            continue
        # Check if PV is available
        if pv.status.phase == 'Available':
            available_pvs.append(pv)

    if not available_pvs:
        return None

    # Check if any available PV has a compatible access mode
    for pv in available_pvs:
        pv_access_modes = pv.spec.access_modes or []
        if pvc_access_mode in pv_access_modes:
            # Found a compatible PV, so access mode is not the issue
            return None

    # No compatible PV found - access mode mismatch
    pv_access_modes_str = ', '.join(
        sorted(
            set(mode for pv in available_pvs
                for mode in (pv.spec.access_modes or []))))
    pvc_name = pvc.metadata.name
    namespace = pvc.metadata.namespace
    return (f'PVC access mode mismatch: PVC requests {pvc_access_mode}, but '
            f'available PersistentVolumes support: {pv_access_modes_str}. '
            f'Update the volume with the correct access_mode '
            f'(e.g., {pv_access_modes_str}) and recreate it. To debug, run: '
            f'kubectl describe pvc {pvc_name} -n {namespace}')
