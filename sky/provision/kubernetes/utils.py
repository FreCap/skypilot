"""Kubernetes utilities for SkyPilot."""
import collections
from collections.abc import Callable
import copy
import dataclasses
import enum
import functools
import hashlib
import json
import math
import os
import re
import threading
import time
import typing
from typing import Any, Literal

import ijson

from sky import clouds
from sky import exceptions
from sky import global_user_state
from sky import models
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.adaptors import gcp
from sky.adaptors import kubernetes
from sky.provision import constants as provision_constants
from sky.provision.kubernetes import constants as kubernetes_constants
from sky.provision.kubernetes import context_utils
from sky.provision.kubernetes import instance_type as instance_type_lib
from sky.provision.kubernetes import network_utils
from sky.provision.kubernetes import pod_config as pod_config_lib
from sky.provision.kubernetes import pod_diagnostics
from sky.provision.kubernetes import ssh_utils
from sky.utils import annotations
from sky.utils import common_utils
from sky.utils import env_options
from sky.utils import gpu_names
from sky.utils import kubernetes_enums
from sky.utils import plugin_extensions
from sky.utils import status_lib
from sky.utils import timeline
from sky.utils import ux_utils
from sky.utils import yaml_utils

if typing.TYPE_CHECKING:
    import jinja2
    import yaml

    from sky import backends
    from sky import resources as resources_lib
else:
    jinja2 = adaptors_common.LazyImport('jinja2')
    yaml = adaptors_common.LazyImport('yaml')

# Please be careful when changing this.
# When mounting, Kubernetes changes the ownership of the parent directory
# to root:root.
# See https://stackoverflow.com/questions/50818029/mounted-folder-created-as-root-instead-of-current-user-in-docker/50820023#50820023.  # pylint: disable=line-too-long
HIGH_AVAILABILITY_DEPLOYMENT_VOLUME_MOUNT_NAME = 'sky-data'
DEFAULT_HOME_DIRECTORY = '/home/sky'
# Path where the persistent volume for HA controller is mounted.
# TODO(andy): Consider using dedicated path like `/var/skypilot`
# and store all data that needs to be persisted in future.
HIGH_AVAILABILITY_DEPLOYMENT_VOLUME_MOUNT_PATH = DEFAULT_HOME_DIRECTORY

IJSON_BUFFER_SIZE = 64 * 1024  # 64KB, default from ijson
_MAX_OBSERVED_NODE_NAME_BYTES = 253
_MAX_OBSERVED_RESOURCE_QUANTITY_BYTES = 128
_MAX_OBSERVED_NODE_CONDITIONS = 256
_MAX_OBSERVED_CONDITION_TYPE_BYTES = 256
_MAX_OBSERVED_CONDITION_STATUS_BYTES = 64
_MAX_OBSERVED_CONTINUE_TOKEN_BYTES = 4096
_MAX_OBSERVED_JSON_KEY_BYTES = 1024
_MAX_OBSERVED_JSON_STRING_BYTES = 256 * 1024
_MAX_OBSERVED_JSON_CONTAINER_DEPTH = 64
_MAX_OBSERVED_ACCELERATOR_LABEL_KEYS = 16
_MAX_OBSERVED_ACCELERATOR_LABEL_KEY_BYTES = 317


class KubernetesHighPerformanceNetworkType(enum.Enum):
    """Enum for different Kubernetes cluster types with high performance
    network configurations.

    This enum defines cluster types that support optimized networking for
    distributed ML workloads:
    - GCP_TCPX: GKE clusters with GPUDirect-TCPX support
      (A3 High instances: a3-highgpu-8g)
    - GCP_TCPXO: GKE clusters with GPUDirect-TCPXO support
      (A3 Mega instances: a3-megagpu-8g)
    - GCP_GPUDIRECT_RDMA: GKE clusters with GPUDirect-RDMA support
      (A4/A3 Ultra instances)
    - NEBIUS: Nebius clusters with InfiniBand support for high-throughput,
      low-latency networking
    - COREWEAVE: CoreWeave clusters with InfiniBand support.
    - TOGETHER: Together AI clusters with InfiniBand support for
      high-throughput, low-latency networking
    - AWS_EFA: AWS EKS/HyperPod clusters with Elastic Fabric Adapter (EFA)
      support for high-performance inter-node communication
    - OCI_ROCE: Oracle OKE clusters on bare-metal GPU shapes
      (BM.GPU.*.8) with RoCEv2 over Mellanox ConnectX, provisioned via
      dedicated RDMA capacity pools
    - NONE: Standard clusters without specialized networking optimizations

    The network configurations align with corresponding VM-based
    implementations:
    - GCP settings match
      sky.provision.gcp.constants.GPU_DIRECT_TCPX_SPECIFIC_OPTIONS
    - Nebius settings match the InfiniBand configuration used in Nebius VMs
    - AWS EFA settings match the EFA configuration used in AWS VMs
    - OCI settings match the RoCE configuration used in OCI bare-metal
      GPU shapes (per oracle-quickstart/oci-hpc-oke reference manifests)
    """

    GCP_TCPX = 'gcp_tcpx'
    GCP_TCPXO = 'gcp_tcpxo'
    GCP_GPUDIRECT_RDMA = 'gcp_gpudirect_rdma'
    NEBIUS = 'nebius'
    COREWEAVE = 'coreweave'
    TOGETHER = 'together'
    AWS_EFA = 'aws_efa'
    OCI_ROCE = 'oci_roce'
    NONE = 'none'

    def get_network_env_vars(self) -> dict[str, str]:
        """Get network environment variables for this cluster type."""
        if self == KubernetesHighPerformanceNetworkType.NEBIUS:
            # Nebius cluster with InfiniBand - use InfiniBand optimizations
            return {
                'NCCL_IB_HCA': 'mlx5',
                'UCX_NET_DEVICES': ('mlx5_0:1,mlx5_1:1,mlx5_2:1,mlx5_3:1,'
                                    'mlx5_4:1,mlx5_5:1,mlx5_6:1,mlx5_7:1')
            }
        elif self == KubernetesHighPerformanceNetworkType.TOGETHER:
            # Together AI cluster with InfiniBand - use InfiniBand optimizations
            return {
                'NCCL_IB_HCA': 'mlx5',
                'UCX_NET_DEVICES': ('mlx5_0:1,mlx5_1:1,mlx5_2:1,mlx5_3:1,'
                                    'mlx5_4:1,mlx5_5:1,mlx5_6:1,mlx5_7:1')
            }
        elif self == KubernetesHighPerformanceNetworkType.COREWEAVE:
            return {
                'NCCL_SOCKET_IFNAME': 'eth0',
                'NCCL_IB_HCA': 'ibp',
                # Restrict UCX to TCP to avoid unneccsary errors. NCCL doesn't use UCX
                'UCX_TLS': 'tcp',
                'UCX_NET_DEVICES': 'eth0',
            }
        elif self == KubernetesHighPerformanceNetworkType.AWS_EFA:
            return {
                'FI_PROVIDER': 'efa',
            }
        elif self == KubernetesHighPerformanceNetworkType.OCI_ROCE:
            # OCI bare-metal GPU shapes (BM.GPU.*.8) use RoCEv2 over
            # Mellanox ConnectX. Values per oracle-quickstart/oci-hpc-oke
            # NCCL reference manifests. Per-shape exact HCA lists give
            # marginally better perf; the broad 'mlx5' prefix match here
            # works on all shapes. Users can override via task `envs:`.
            # Refer to the examples https://github.com/oracle-quickstart/oci-hpc-oke/tree/main/manifests/nccl-tests/kueue for more details. # pylint: disable=line-too-long
            return {
                'NCCL_IB_HCA': 'mlx5',
                # RoCEv2 GID index. Fixed on OCI's bare-metal GPU images.
                'NCCL_IB_GID_INDEX': '3',
                # DSCP for OCI's lossless RoCE fabric (PFC class).
                'NCCL_IB_TC': '41',
                # OCI BM.GPU shapes use legacy eth* naming; primary NIC
                # is eth0 even under hostNetwork: true.
                'NCCL_SOCKET_IFNAME': 'eth0',
                'UCX_TLS': 'tcp',
                'UCX_NET_DEVICES': 'eth0',
            }
        else:
            # GCP clusters and generic clusters - environment variables are
            # handled directly in the template
            return {}

    def supports_high_performance_networking(self) -> bool:
        """Check if this cluster type supports high performance networking."""
        return self is not KubernetesHighPerformanceNetworkType.NONE

    def supports_gpu_direct(self) -> bool:
        """Check if this cluster type supports GPUDirect networking."""
        return self in (KubernetesHighPerformanceNetworkType.GCP_TCPX,
                        KubernetesHighPerformanceNetworkType.GCP_TCPXO,
                        KubernetesHighPerformanceNetworkType.GCP_GPUDIRECT_RDMA)

    def requires_ipc_lock_capability(self) -> bool:
        """Check if this cluster type requires IPC_LOCK capability."""
        return self.supports_high_performance_networking()

    def requires_tcpxo_daemon(self) -> bool:
        """Check if this cluster type requires TCPXO daemon."""
        return self == KubernetesHighPerformanceNetworkType.GCP_TCPXO


# TODO(romilb): Move constants to constants.py
DEFAULT_NAMESPACE = 'default'

DEFAULT_SERVICE_ACCOUNT_NAME = 'skypilot-service-account'

MEMORY_SIZE_UNITS = {
    'm': 0.001,
    'B': 1,
    'K': 2**10,
    'M': 2**20,
    'G': 2**30,
    'T': 2**40,
    'P': 2**50,
}

# The resource keys used by Kubernetes to track NVIDIA GPUs and Google TPUs on
# nodes. These keys are typically used in the node's status.allocatable
# or status.capacity fields to indicate the available resources on the node.
SUPPORTED_GPU_RESOURCE_KEYS = {'amd': 'amd.com/gpu', 'nvidia': 'nvidia.com/gpu'}
TPU_RESOURCE_KEY = 'google.com/tpu'

NO_ACCELERATOR_HELP_MESSAGE = (
    'If your cluster contains GPUs or TPUs, make sure '
    f'one of {SUPPORTED_GPU_RESOURCE_KEYS["amd"]}, '
    f'{SUPPORTED_GPU_RESOURCE_KEYS["nvidia"]} or '
    f'{TPU_RESOURCE_KEY} resource is available '
    'on the nodes and the node labels for identifying GPUs/TPUs '
    '(e.g., skypilot.co/accelerator) are setup correctly. ')

KUBERNETES_AUTOSCALER_NOTE = (
    'Note: Kubernetes cluster autoscaling is enabled. '
    'All GPUs that can be provisioned may not be listed '
    'here. Refer to your autoscaler\'s node pool '
    'configuration to see the list of supported GPUs.')

# TODO(romilb): Add links to docs for configuration instructions when ready.
ENDPOINTS_DEBUG_MESSAGE = ('Additionally, make sure your {endpoint_type} '
                           'is configured correctly. '
                           '\nTo debug, run: {debug_cmd}')

KIND_CONTEXT_NAME = 'kind-skypilot'  # Context name used by sky local up

# Port-forward proxy command constants. Keep these aliases for compatibility
# with callers that import the Kubernetes utilities facade.
PORT_FORWARD_PROXY_CMD_TEMPLATE: str = ssh_utils.PORT_FORWARD_PROXY_CMD_TEMPLATE
PORT_FORWARD_PROXY_CMD_VERSION: int = ssh_utils.PORT_FORWARD_PROXY_CMD_VERSION
PORT_FORWARD_PROXY_CMD_PATH: str = ssh_utils.PORT_FORWARD_PROXY_CMD_PATH

# Mapping used to get generation for TPU accelerator name.
# https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#run
GKE_TPU_ACCELERATOR_TO_GENERATION = {
    'tpu-v4-podslice': 'v4',
    # Only Single-host v5e TPU configurations are allowed.
    'tpu-v5-lite-device': 'v5e',
    # Multi-host compatible v5e TPU configurations allowed.
    'tpu-v5-lite-podslice': 'v5e',
    'tpu-v5p-slice': 'v5p',
    'tpu-v6e-slice': 'v6e',
}

POD_STATUSES = {
    'Pending', 'Running', 'Succeeded', 'Failed', 'Unknown', 'Terminating'
}
AUTODOWN_ANNOTATION_KEY = 'skypilot.co/autodown'
IDLE_MINUTES_TO_AUTOSTOP_ANNOTATION_KEY = (
    'skypilot.co/idle_minutes_to_autostop')
ANNOTATIONS_POD_NOT_FOUND_ERROR_MSG = ('Pod {pod_name} not found in namespace '
                                       '{namespace} while trying to {action} '
                                       'an annotation {annotation}.')

logger = sky_logging.init_logger(__name__)

# Default retry settings for Kubernetes API calls
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_INTERVAL_SECONDS = 1

# Annotations Kubernetes uses to mark the cluster's default StorageClass.
DEFAULT_STORAGE_CLASS_ANNOTATION = (
    'storageclass.kubernetes.io/is-default-class')
DEFAULT_STORAGE_CLASS_ANNOTATION_LEGACY = (
    'storageclass.beta.kubernetes.io/is-default-class')


def is_truthy_annotation(value: Any) -> bool:
    """Returns True for the values K8s admission treats as truthy.

    Matches Go's `strconv.ParseBool` semantics, which K8s uses for
    boolean-valued annotations: accepts 'true' / 'True' / 'TRUE' / '1' /
    't' / 'T'. Strict equality on the string `'true'` misses capitalized
    variants that some Helm charts emit in the wild.
    """
    if value is None:
        return False
    return str(value).lower() in ('true', '1', 't')


def is_default_storage_class(sc: Any) -> bool:
    """True if the StorageClass object is annotated as the cluster default.

    Accepts both the current annotation
    (`storageclass.kubernetes.io/is-default-class`) and the legacy beta
    annotation. Robust to missing metadata/annotations.

    Args:
        sc: A Kubernetes V1StorageClass (or duck-typed equivalent).
    """
    metadata = getattr(sc, 'metadata', None)
    sc_annotations = (getattr(metadata, 'annotations', None)
                      if metadata else None)
    if not sc_annotations:
        return False
    return (is_truthy_annotation(
        sc_annotations.get(DEFAULT_STORAGE_CLASS_ANNOTATION)) or
            is_truthy_annotation(
                sc_annotations.get(DEFAULT_STORAGE_CLASS_ANNOTATION_LEGACY)))


def normalize_tpu_accelerator_name(accelerator: str) -> tuple[str, int]:
    """Normalize TPU names to the k8s-compatible name and extract count."""
    # Examples:
    # 'tpu-v6e-8' -> ('tpu-v6e-slice', 8)
    # 'tpu-v5litepod-4' -> ('tpu-v5-lite-podslice', 4)

    gcp_to_k8s_patterns = [
        (r'^tpu-v6e-(\d+)$', 'tpu-v6e-slice'),
        (r'^tpu-v5p-(\d+)$', 'tpu-v5p-slice'),
        (r'^tpu-v5litepod-(\d+)$', 'tpu-v5-lite-podslice'),
        (r'^tpu-v5lite-(\d+)$', 'tpu-v5-lite-device'),
        (r'^tpu-v4-(\d+)$', 'tpu-v4-podslice'),
    ]

    for pattern, replacement in gcp_to_k8s_patterns:
        match = re.match(pattern, accelerator)
        if match:
            count = int(match.group(1))
            return replacement, count

    # Default fallback
    return accelerator, 1


def _is_cloudflare_403_error(exception: Exception) -> bool:
    """Check if an exception is a transient CloudFlare 403 error.

    CloudFlare proxy 403 errors with CF-specific headers are transient and
    should be retried, unlike real RBAC 403 errors.

    Args:
        exception: The exception to check

    Returns:
        True if this is a CloudFlare 403 error that should be retried
    """
    if not isinstance(exception, kubernetes.api_exception()):
        return False

    # Only check for 403 errors
    if exception.status != 403:
        return False

    # Check for CloudFlare-specific headers
    headers = exception.headers if hasattr(exception, 'headers') else {}
    if not headers:
        return False

    # CloudFlare errors have CF-RAY header and/or Server: cloudflare
    for k, v in headers.items():
        if 'cf-ray' in k.lower():
            return True
        if 'server' in k.lower() and 'cloudflare' in str(v).lower():
            return True

    return False


def _retry_on_error(max_retries=DEFAULT_MAX_RETRIES,
                    retry_interval=DEFAULT_RETRY_INTERVAL_SECONDS,
                    resource_type: str | None = None):
    """Decorator to retry Kubernetes API calls on transient failures.

    Args:
        max_retries: Maximum number of retry attempts
        retry_interval: Initial seconds to wait between retries
        resource_type: Type of resource being accessed (e.g. 'node', 'pod').
            Used to provide more specific error messages.

    Raises:
        KubeAPIUnreachableError: If the API server of the given context is
            unreachable.
    """

    def decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            backoff = common_utils.Backoff(initial_backoff=retry_interval,
                                           max_backoff_factor=3)

            assert 'context' in kwargs, 'context is required'
            context = kwargs.get('context')

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (kubernetes.max_retry_error(),
                        kubernetes.api_exception(),
                        kubernetes.config_exception()) as e:
                    last_exception = e

                    # Check if this is a CloudFlare transient 403 error
                    is_cloudflare_403 = _is_cloudflare_403_error(e)

                    # Don't retry on permanent errors like 401 (Unauthorized)
                    # or 403 (Forbidden), unless it's a CloudFlare transient 403
                    if (isinstance(e, kubernetes.api_exception()) and
                            e.status in (401, 403) and not is_cloudflare_403):
                        # Raise KubeAPIUnreachableError exception so that the
                        # optimizer/provisioner can failover to other clouds.
                        raise exceptions.KubeAPIUnreachableError(
                            f'Kubernetes API error: {str(e)}') from e
                    if attempt < max_retries - 1:
                        sleep_time = backoff.current_backoff()
                        error_type = 'CloudFlare 403' if is_cloudflare_403 else 'error'
                        logger.debug(
                            f'Kubernetes API call {func.__name__} '
                            f'failed with {error_type} {str(e)}. Retrying in '
                            f'{sleep_time:.1f}s...')
                        time.sleep(sleep_time)
                        continue

            # Format error message based on the type of exception
            resource_msg = f' when trying to get {resource_type} info' \
                if resource_type else ''
            debug_cmd = f' To debug, run: kubectl get {resource_type}s' \
                if resource_type else ''
            if context:
                debug_cmd += f' --context {context}'

            if isinstance(last_exception, kubernetes.max_retry_error()):
                error_msg = f'Timed out{resource_msg} from Kubernetes cluster.'
            elif isinstance(last_exception, kubernetes.api_exception()):
                error_msg = (f'Kubernetes API error{resource_msg}: '
                             f'{str(last_exception)}')
            else:
                error_msg = (f'Kubernetes configuration error{resource_msg}: '
                             f'{str(last_exception)}')

            raise exceptions.KubeAPIUnreachableError(
                f'{error_msg}'
                f' Please check if the cluster is healthy and retry.'
                f'{debug_cmd}') from last_exception

        return wrapper

    return decorator


class GPULabelFormatter:
    """Base class to define a GPU label formatter for a Kubernetes cluster

    A GPU label formatter is a class that defines how to use GPU type labels in
    a Kubernetes cluster. It is used by the Kubernetes cloud class to pick the
    key:value pair to use as node selector for GPU nodes.
    """

    @classmethod
    def get_tpu_topology_label_key(cls) -> str:
        """Returns the label for TPU topology used by the Kubernetes cluster.

        Only implemented by formatters that support TPUs.
        """
        raise NotImplementedError

    @classmethod
    def get_tpu_topology_label_value(cls, acc_type: str, acc_count: int) -> str:
        """Returns the TPU topology value for the given TPU type and count.

        Only implemented by formatters that support TPUs.
        """
        raise NotImplementedError

    @classmethod
    def get_label_key(cls, accelerator: str | None = None) -> str:
        """Returns the label key for GPU type used by the Kubernetes cluster"""
        raise NotImplementedError

    @classmethod
    def get_label_keys(cls) -> list[str]:
        """Returns a list of label keys for GPU used by Kubernetes cluster."""
        raise NotImplementedError

    @classmethod
    def get_label_values(cls, accelerator: str) -> list[str]:
        """Given a GPU type, returns the label value to be used"""
        raise NotImplementedError

    @classmethod
    def match_label_key(cls, label_key: str) -> bool:
        """Checks if the given label key matches the formatter's label keys"""
        raise NotImplementedError

    @classmethod
    def get_accelerator_from_label_value(cls, value: str) -> str:
        """Given a label value, returns the GPU type"""
        raise NotImplementedError

    @classmethod
    def validate_label_value(cls, value: str) -> tuple[bool, str]:
        """Validates if the specified label value is correct.

        Used to check if the labelling on the cluster is correct and
        preemptively raise an error if it is not.

        Returns:
            bool: True if the label value is valid, False otherwise.
            str: Error message if the label value is invalid, None otherwise.
        """
        del value
        return True, ''


def get_gke_accelerator_name(accelerator: str) -> str:
    """Returns the accelerator name for GKE clusters.

    Uses the format - nvidia-tesla-<accelerator>.
    A100-80GB, H100-80GB, L4 are an exception. They use nvidia-<accelerator>.
    TPU types are an exception as well keeping the given name.
    """
    if accelerator == 'H100':
        # H100 is named as H100-80GB in GKE.
        accelerator = 'H100-80GB'
    if accelerator in ('A100-80GB', 'L4', 'H100-80GB', 'H100-MEGA-80GB',
                       'B200'):
        # A100-80GB, L4, H100-80GB and H100-MEGA-80GB
        # have a different name pattern.
        return f'nvidia-{accelerator.lower()}'
    elif accelerator == 'H200':
        # H200s on GCP use this label format
        return 'nvidia-h200-141gb'
    elif accelerator.startswith('tpu-'):
        return accelerator
    elif accelerator.startswith('amd-'):
        return accelerator
    else:
        return f'nvidia-tesla-{accelerator.lower()}'


class SkyPilotLabelFormatter(GPULabelFormatter):
    """Custom label formatter for SkyPilot

    Uses skypilot.co/accelerator as the key, and SkyPilot accelerator str as the
    value.
    """

    LABEL_KEY = 'skypilot.co/accelerator'

    @classmethod
    def get_label_key(cls, accelerator: str | None = None) -> str:
        return cls.LABEL_KEY

    @classmethod
    def get_label_keys(cls) -> list[str]:
        return [cls.LABEL_KEY]

    @classmethod
    def get_label_values(cls, accelerator: str) -> list[str]:
        # For SkyPilot formatter, we use the accelerator str directly.
        # See sky.utils.kubernetes.gpu_labeler.
        return [accelerator.lower()]

    @classmethod
    def match_label_key(cls, label_key: str) -> bool:
        return label_key == cls.LABEL_KEY

    @classmethod
    def get_accelerator_from_label_value(cls, value: str) -> str:
        return value.upper()

    @classmethod
    def validate_label_value(cls, value: str) -> tuple[bool, str]:
        """Values must be all lowercase for the SkyPilot formatter."""
        is_valid = value == value.lower()
        return is_valid, (f'Label value {value!r} must be lowercase if using '
                          f'the {cls.get_label_key()} label.'
                          if not is_valid else '')


class CoreWeaveLabelFormatter(GPULabelFormatter):
    """CoreWeave label formatter

    Uses gpu.nvidia.com/class as the key, and the uppercase SkyPilot
    accelerator str as the value.
    """

    LABEL_KEY = 'gpu.nvidia.com/class'

    # TODO (kyuds): fill in more label values for different accelerators.
    ACC_VALUE_MAPPINGS = {'H100_NVLINK_80GB': 'H100'}

    @classmethod
    def get_label_key(cls, accelerator: str | None = None) -> str:
        return cls.LABEL_KEY

    @classmethod
    def get_label_keys(cls) -> list[str]:
        return [cls.LABEL_KEY]

    @classmethod
    def get_label_values(cls, accelerator: str) -> list[str]:
        return [accelerator.upper()]

    @classmethod
    def match_label_key(cls, label_key: str) -> bool:
        return label_key == cls.LABEL_KEY

    @classmethod
    def get_accelerator_from_label_value(cls, value: str) -> str:
        # return original label value if not found in mappings.
        return cls.ACC_VALUE_MAPPINGS.get(value, value)


class GKELabelFormatter(GPULabelFormatter):
    """GKE label formatter

    GKE nodes by default are populated with `cloud.google.com/gke-accelerator`
    label, which is used to identify the GPU type.
    """
    GPU_LABEL_KEY = 'cloud.google.com/gke-accelerator'
    TPU_LABEL_KEY = 'cloud.google.com/gke-tpu-accelerator'
    ACCELERATOR_COUNT_LABEL_KEY = 'cloud.google.com/gke-accelerator-count'
    TPU_TOPOLOGY_LABEL_KEY = 'cloud.google.com/gke-tpu-topology'

    # Mapping from TPU type to {count: topologies}. Used to determine topology
    # label to use in an autoscaling environment. For list of topologies, see:
    # tpu v5e: https://cloud.google.com/tpu/docs/tpus-in-gke
    # tpu v5p: https://cloud.google.com/tpu/docs/v5p
    # tpu v6e: https://cloud.google.com/tpu/docs/v6e
    # TODO(romilb): Add support for TPU v4.
    GKE_TPU_TOPOLOGIES = {
        'tpu-v5-lite-podslice': {
            1: '1x1',
            4: '2x2',
            8: '2x4'
        },
        'tpu-v5-lite-device': {
            1: '1x1',
            4: '2x2',
            8: '2x4'
        },
        'tpu-v5p-slice': {
            4: '2x2x1'
        },
        'tpu-v6e-slice': {
            1: '1x1',
            4: '2x2',
            8: '2x4'
        }
    }

    @classmethod
    def get_label_key(cls, accelerator: str | None = None) -> str:
        if accelerator is not None and accelerator.startswith('tpu-'):
            return cls.TPU_LABEL_KEY
        return cls.GPU_LABEL_KEY

    @classmethod
    def get_label_keys(cls) -> list[str]:
        return [cls.GPU_LABEL_KEY, cls.TPU_LABEL_KEY]

    @classmethod
    def match_label_key(cls, label_key: str) -> bool:
        return label_key in cls.get_label_keys()

    @classmethod
    def get_tpu_topology_label_key(cls) -> str:
        return cls.TPU_TOPOLOGY_LABEL_KEY

    @classmethod
    def get_tpu_topology_label_value(cls, acc_type: str, acc_count: int) -> str:
        """Returns the TPU topology label value for the given TPU count.

        e.g. tpu-v5-lite-podslice:8 -> '2x4'
        """
        # If the TPU type is in the GKE_TPU_ACCELERATOR_TO_GENERATION, it means
        # that it has been normalized before, no need to normalize again.
        if acc_type not in GKE_TPU_ACCELERATOR_TO_GENERATION:
            acc_type, acc_count = normalize_tpu_accelerator_name(acc_type)
        count_to_topology = cls.GKE_TPU_TOPOLOGIES.get(acc_type,
                                                       {}).get(acc_count, None)
        if count_to_topology is None:
            supported_tpus = {
                tpu: list(topologies.values())
                for tpu, topologies in cls.GKE_TPU_TOPOLOGIES.items()
            }
            raise ValueError(
                f'No TPU topology found for {acc_type} with count {acc_count}. '
                f'Supported TPU types and counts: {supported_tpus}')
        return count_to_topology

    @classmethod
    def get_label_values(cls, accelerator: str) -> list[str]:
        return [get_gke_accelerator_name(accelerator)]

    @classmethod
    def get_accelerator_from_label_value(cls, value: str) -> str:
        if value.startswith('nvidia-tesla-'):
            return value.replace('nvidia-tesla-', '').upper()
        elif value.startswith('nvidia-'):
            acc = value.replace('nvidia-', '').upper()
            if acc == 'H100-80GB':
                # H100 can be either H100-80GB or H100-MEGA-80GB in GKE
                # we map H100 ---> H100-80GB and keep H100-MEGA-80GB
                # to distinguish between a3-high and a3-mega instances
                return 'H100'
            elif acc == 'H200-141GB':
                return 'H200'
            return acc
        elif is_tpu_on_gke(value):
            return value
        elif value == '':
            # heterogenous cluster may have empty labels for cpu nodes.
            return ''
        else:
            raise ValueError(
                f'Invalid accelerator name in GKE cluster: {value}')

    @classmethod
    def validate_label_value(cls, value: str) -> tuple[bool, str]:
        try:
            _ = cls.get_accelerator_from_label_value(value)
            return True, ''
        except ValueError as e:
            return False, str(e)


class GFDLabelFormatter(GPULabelFormatter):
    """GPU Feature Discovery label formatter

    NVIDIA GPUs nodes are labeled by GPU feature discovery
    e.g. nvidia.com/gpu.product=NVIDIA-H100-80GB-HBM3
    https://github.com/NVIDIA/gpu-feature-discovery

    GPU feature discovery is included as part of the
    NVIDIA GPU Operator:
    https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/overview.html

    This LabelFormatter can't be used in autoscaling clusters since accelerators
    may map to multiple label, so we're not implementing `get_label_values`
    """

    LABEL_KEY = 'nvidia.com/gpu.product'

    @classmethod
    def get_label_key(cls, accelerator: str | None = None) -> str:
        return cls.LABEL_KEY

    @classmethod
    def get_label_keys(cls) -> list[str]:
        return [cls.LABEL_KEY]

    @classmethod
    def get_label_values(cls, accelerator: str) -> list[str]:
        # An accelerator can map to many Nvidia GFD labels
        # (e.g., A100-80GB-PCIE vs. A100-SXM4-80GB).
        # TODO implement get_label_values for GFDLabelFormatter
        raise NotImplementedError

    @classmethod
    def match_label_key(cls, label_key: str) -> bool:
        return label_key == cls.LABEL_KEY

    @classmethod
    def get_accelerator_from_label_value(cls, value: str) -> str:
        """Searches against a canonical list of NVIDIA GPUs and pattern
        matches the canonical GPU name against the GFD label.
        """
        for canonical_name in gpu_names.CANONICAL_GPU_NAMES:
            # A100-80GB accelerator is A100-SXM-80GB or A100-PCIE-80GB
            if canonical_name == 'A100-80GB' and re.search(
                    r'A100.*-80GB', value):
                return canonical_name
            # H100-80GB accelerator is H100-SXM-80GB or H100-PCIE-80GB
            if canonical_name == 'H100-80GB' and re.search(
                    r'H100.*-80GB', value):
                return canonical_name
            # Use word boundary matching to prevent substring matches
            elif re.search(rf'\b{re.escape(canonical_name)}\b', value):
                return canonical_name

        # If we didn't find a canonical name:
        # 1. remove 'NVIDIA-' (e.g., 'NVIDIA-RTX-A6000' -> 'RTX-A6000')
        # 2. remove 'GEFORCE-' (e.g., 'NVIDIA-GEFORCE-RTX-3070' -> 'RTX-3070')
        # 3. remove 'RTX-' (e.g. 'RTX-6000' -> 'RTX6000')
        # Same logic, but uppercased, as the Skypilot labeler job found in
        # sky/utils/kubernetes/k8s_gpu_labeler_setup.yaml.j2
        return value.upper().replace('NVIDIA-',
                                     '').replace('GEFORCE-',
                                                 '').replace('RTX-', 'RTX')


def _accelerator_name_matches(requested_acc: str,
                              viable_names: list[str]) -> bool:
    """Check if requested accelerator matches any viable name.

    For backward compatibility with GPU name changes (e.g., when canonical names
    like 'H200' are added to replace fallback names like 'H200-SXM-80GB'), this
    function also matches if one name is a prefix of the other separated by '-'.

    This handles cases where:
    - Clusters were launched with fallback names (e.g., 'H200-SXM-80GB') but
      after upgrading, the same label now maps to canonical name (e.g., 'H200').
    - Users specify canonical names but the cluster uses fallback names.

    Args:
        requested_acc: The accelerator type requested (e.g., from launched_resources).
        viable_names: List of viable accelerator names from node labels.

    Returns:
        True if the requested accelerator matches any viable name.
    """
    requested_lower = requested_acc.lower()
    for viable in viable_names:
        viable_lower = viable.lower()
        if requested_lower == viable_lower:
            return True
        # Check prefix match with '-' separator for backward compatibility.
        # E.g., 'H200' matches 'H200-SXM-80GB' and vice versa.
        shorter, longer = ((requested_lower, viable_lower)
                           if len(requested_lower) <= len(viable_lower) else
                           (viable_lower, requested_lower))
        if longer.startswith(shorter):
            # Ensure it's a proper prefix (followed by '-' or end of string)
            if len(longer) == len(shorter) or longer[len(shorter)] == '-':
                # Guard against the OOM direction: a request must not be
                # satisfied by a node with strictly LESS device memory (e.g.
                # an 'A100-80GB' (or typo'd 'A100-80G') request on a 40GB
                # 'A100' node). Only applies when both names imply a known
                # memory size; same-or-larger node memory still matches, which
                # preserves backward compatibility (an 'A100' request may still
                # land on an 'A100-80GB' node) and same-hardware renames (e.g.
                # 'H100' == 'H100-80GB', both 80GB).
                requested_mem = gpu_names.get_gpu_device_memory_gib(
                    requested_lower)
                viable_mem = gpu_names.get_gpu_device_memory_gib(viable_lower)
                if (requested_mem is not None and viable_mem is not None and
                        requested_mem > viable_mem):
                    continue
                return True
    return False


class KarpenterLabelFormatter(SkyPilotLabelFormatter):
    """Karpeneter label formatter
    Karpenter uses the label `karpenter.k8s.aws/instance-gpu-name` to identify
    the GPU type. Details: https://karpenter.sh/docs/reference/instance-types/
    The naming scheme is same as the SkyPilot formatter, so we inherit from it.
    """
    LABEL_KEY = 'karpenter.k8s.aws/instance-gpu-name'


class NebiusLabelFormatter(GPULabelFormatter):
    """Custom label formatter for Nebius

    Uses nebius.com/gpu-name as the key, and the uppercase SkyPilot
    accelerator str as the value.
    """

    LABEL_KEY = 'nebius.com/gpu-name'

    @classmethod
    def get_label_key(cls, accelerator: str | None = None) -> str:
        return cls.LABEL_KEY

    @classmethod
    def get_label_keys(cls) -> list[str]:
        return [cls.LABEL_KEY]

    @classmethod
    def get_label_values(cls, accelerator: str) -> list[str]:
        # For Nebius formatter, we use the uppercase accelerator str.
        return [accelerator.upper()]

    @classmethod
    def match_label_key(cls, label_key: str) -> bool:
        return label_key == cls.LABEL_KEY

    @classmethod
    def get_accelerator_from_label_value(cls, value: str) -> str:
        return value.upper()

    @classmethod
    def validate_label_value(cls, value: str) -> tuple[bool, str]:
        """Values must be all uppercase for the Nebius formatter."""
        is_valid = value == value.upper()
        return is_valid, (f'Label value {value!r} must be uppercase if using '
                          f'the {cls.get_label_key()} label.'
                          if not is_valid else '')


# LABEL_FORMATTER_REGISTRY stores the label formats SkyPilot will try to
# discover the accelerator type from. The order of the list is important, as
# it will be used to determine the priority of the label formats when
# auto-detecting the GPU label type.
LABEL_FORMATTER_REGISTRY: list[type[GPULabelFormatter]] = [
    SkyPilotLabelFormatter, GKELabelFormatter, KarpenterLabelFormatter,
    GFDLabelFormatter, CoreWeaveLabelFormatter, NebiusLabelFormatter
]

_InvalidGPULabelCallback = Callable[
    [int, str, type[GPULabelFormatter], str, str], None]


class _GPULabelFormatterSelector:
    """Single owner for provider-order GPU label formatter selection."""

    __slots__ = ('_formatters', '_formatter_states', '_invalid_label_callback')

    def __init__(
        self,
        invalid_label_callback: _InvalidGPULabelCallback | None = None,
    ) -> None:
        # None means that this formatter has not seen a nonempty matching
        # label. False permanently disqualifies it after its first invalid
        # nonempty match. True records its first valid nonempty match. The
        # registry order is the formatter priority.
        self._formatters = tuple(LABEL_FORMATTER_REGISTRY)
        self._formatter_states: list[bool |
                                     None] = [None] * len(self._formatters)
        self._invalid_label_callback = invalid_label_callback

    def accept(self, label_key: str, label_value: str | None) -> None:
        """Consume one label in exact provider node and label-map order."""
        for index, formatter in enumerate(self._formatters):
            if self._formatter_states[index] is not None:
                continue
            if not formatter.match_label_key(label_key):
                continue
            if not label_value or label_value.strip() == '':
                continue
            valid, reason = formatter.validate_label_value(label_value)
            self._formatter_states[index] = bool(valid)
            if not valid and self._invalid_label_callback is not None:
                self._invalid_label_callback(index, label_key, formatter,
                                             label_value, reason)

    def selected_formatter_type(self) -> type[GPULabelFormatter] | None:
        """Resolve the first valid formatter in registry priority order."""
        for formatter, state in zip(self._formatters, self._formatter_states):
            if state:
                return formatter
        return None


@annotations.lru_cache(scope='request')
def detect_gpu_label_formatter(
    context: str | None
) -> tuple[GPULabelFormatter | None, dict[str, list[tuple[str, str]]]]:
    """Detects the GPU label formatter for the Kubernetes cluster

    Returns:
        GPULabelFormatter: The GPU label formatter for the cluster, if found.
        Dict[str, List[Tuple[str, str]]]: A mapping of nodes and the list of
             labels on each node. E.g., {'node1': [('label1', 'value1')]}
    """
    # Get all labels across all nodes
    node_labels: dict[str, list[tuple[str, str]]] = {}
    nodes = get_kubernetes_nodes(context=context)
    for node in nodes:
        node_labels[node.metadata.name] = []
        for label, value in node.metadata.labels.items():
            node_labels[node.metadata.name].append((label, value))

    invalid_label_values: list[tuple[int, str, type[GPULabelFormatter], str,
                                     str]] = []

    def record_invalid_label(index: int, label: str,
                             formatter: type[GPULabelFormatter], value: str,
                             reason: str) -> None:
        invalid_label_values.append((index, label, formatter, value, reason))

    selector = _GPULabelFormatterSelector(record_invalid_label)
    for label_list in node_labels.values():
        for label, value in label_list:
            selector.accept(label, value)

    selected_formatter_type = selector.selected_formatter_type()
    if selected_formatter_type is not None:
        return selected_formatter_type(), node_labels

    for _, label, formatter, value, reason in sorted(invalid_label_values):
        lf_name = formatter.__name__
        logger.warning(f'GPU label {label} matched for label '
                       f'formatter {lf_name}, '
                       f'but has invalid value {value}. '
                       f'Reason: {reason}. '
                       'Skipping...')

    return None, node_labels


class Autoscaler:
    """Base class to define a autoscaler for a Kubernetes cluster.
    An autoscaler is a class that defines how to detect if a Kubernetes
    context can autoscale to meet the resource requirements of a task.
    """

    label_formatter: Any = None

    # returns if the autoscaler backend can be queried for information.
    # If True, SkyPilot will query the autoscaler backend to check if
    # the Kubernetes context can autoscale to meet the resource requirements
    # of a task.
    can_query_backend: bool = False

    @classmethod
    # pylint: disable=unused-argument
    def can_create_new_instance_of_type(cls, context: str,
                                        instance_type: str) -> bool:
        """Returns if the Kubernetes context has an autoscaler
        that can create a new node that satisfies the instance type.
        Args:
            context: The Kubernetes context to check.
            instance_type: The instance type to check.
        Returns:
            bool: True if the Kubernetes context has an autoscaler that can
                create a new node satisfying the instance type,
                or if such determination is not possible.
                False if the Kubernetes context autoscaler cannot create a new
                node satisfying the instance type.
        """
        # For autoscalers that SkyPilot does not know how to interface with,
        # assume the autoscaler can create a new node that satisfies
        # the instance type.
        # If this is not the case, the autoscaler will fail to provision the
        # node and the pod will be stuck in pending state until
        # provision_timeout, after which failover will be triggered.
        return True


class GKEAutoscaler(Autoscaler):
    """GKE autoscaler
    """

    label_formatter: Any = GKELabelFormatter
    can_query_backend: bool = True

    # This variable is stored in memory in the server.
    # The variable will reset if the server restarts.
    _pip_install_gcp_hint_last_sent = 0.0

    @classmethod
    @annotations.lru_cache(scope='request', maxsize=10)
    def can_create_new_instance_of_type(cls, context: str,
                                        instance_type: str) -> bool:
        """Looks at each node pool in the cluster and checks if
        it can create a new node that satisfies the instance type.
        If the context does not match standard GKE context naming convention,
        or GKE credential is not set, this function returns True
        for optimistic pod scheduling.
        """
        # assume context naming convention of
        # gke_PROJECT-ID_LOCATION_CLUSTER-NAME
        valid, project_id, location, cluster_name = cls._validate_context_name(
            context)
        if not valid:
            # Context name is not in the format of
            # gke_PROJECT-ID_LOCATION_CLUSTER-NAME.
            # Cannot determine if the context can autoscale
            # return True for optimistic pod scheduling.
            logger.debug(f'context {context} is not in the format of '
                         f'gke_PROJECT-ID_LOCATION_CLUSTER-NAME. '
                         'reporting context as potentially capable of '
                         'provisioning resources without further check')
            return True
        try:
            logger.debug(
                f'attempting to get information about cluster {cluster_name}')
            container_service = gcp.build('container',
                                          'v1',
                                          credentials=None,
                                          cache_discovery=False)
            cluster = container_service.projects().locations().clusters().get(
                name=f'projects/{project_id}'
                f'/locations/{location}'
                f'/clusters/{cluster_name}').execute()
        except ImportError:
            # If the gcp module is not installed, return True for
            # optimistic pod scheduling.
            # Remind the user once per day to install the gcp module for better
            # pod scheduling with GKE autoscaler.
            if time.time() - cls._pip_install_gcp_hint_last_sent > 60 * 60 * 24:
                logger.info(
                    'Could not fetch autoscaler information from GKE. '
                    'Run pip install "skypilot[gcp]" for more intelligent pod '
                    'scheduling with GKE autoscaler.')
                cls._pip_install_gcp_hint_last_sent = time.time()
            return True
        except gcp.http_error_exception() as e:
            # Cluster information is not available.
            # return True for optimistic pod scheduling.
            logger.debug(f'{e.message}', exc_info=True)
            return True

        # GKE Autopilot uses Node Auto-Provisioning (NAP) to create node
        # pools on demand for any requested instance type, including GPUs.
        # The static node pool list returned by the API only reflects the
        # CPU bootstrap pools and does not advertise what NAP can provision,
        # so the per-pool fit check below would falsely reject GPU requests.
        # Trust NAP to satisfy the request.
        # Use `is True` so a non-boolean value (e.g. accidentally a string)
        # cannot inadvertently bypass the fit check on Standard clusters.
        if cluster.get('autopilot', {}).get('enabled') is True:
            logger.debug(f'Cluster {cluster_name} is Autopilot-managed; '
                         'trusting Node Auto-Provisioning to satisfy '
                         f'{instance_type}.')
            return True

        # Check if any node pool with autoscaling enabled can
        # fit the instance type.
        node_pools = cluster.get('nodePools', [])
        for node_pool in node_pools:
            name = node_pool.get('name', '')
            logger.debug(f'checking if node pool {name} '
                         'has autoscaling enabled.')
            autoscaling_enabled = (node_pool.get('autoscaling',
                                                 {}).get('enabled', False))
            if autoscaling_enabled:
                logger.debug(f'node pool {name} has autoscaling enabled. '
                             'Checking if it can create a node '
                             f'satisfying {instance_type}')
                try:
                    if cls._check_instance_fits_gke_autoscaler_node_pool(
                            instance_type, node_pool):
                        return True
                except KeyError:
                    logger.debug('encountered KeyError while checking if '
                                 f'node pool {name} can create a node '
                                 f'satisfying {instance_type}.')
                    return True
        return False

    @classmethod
    @annotations.lru_cache(scope='request', maxsize=10)
    def get_available_machine_types(cls, context: str) -> list[str]:
        """Returns the list of machine types that are available in the cluster.
        """
        # Assume context naming convention of
        # gke_PROJECT-ID_LOCATION_CLUSTER-NAME
        valid, project_id, location, cluster_name = cls._validate_context_name(
            context)
        if not valid:
            # Context name is not in the format of
            # gke_PROJECT-ID_LOCATION_CLUSTER-NAME.
            # Cannot determine if the context can autoscale.
            # Return empty list.
            logger.debug(f'Context {context} is not in the format of '
                         f'gke_PROJECT-ID_LOCATION_CLUSTER-NAME. '
                         'Returning empty machine type list.')
            return []
        try:
            logger.debug(
                f'Attempting to get information about cluster {cluster_name}')
            container_service = gcp.build('container',
                                          'v1',
                                          credentials=None,
                                          cache_discovery=False)
            cluster = container_service.projects().locations().clusters().get(
                name=f'projects/{project_id}'
                f'/locations/{location}'
                f'/clusters/{cluster_name}').execute()
        except ImportError:
            # If the gcp module is not installed, return empty list.
            # Remind the user once per day to install the gcp module for better
            # pod scheduling with GKE autoscaler.
            if time.time() - cls._pip_install_gcp_hint_last_sent > 60 * 60 * 24:
                logger.info(
                    'Could not fetch autoscaler information from GKE. '
                    'Run pip install "skypilot[gcp]" for more intelligent pod '
                    'scheduling with GKE autoscaler.')
                cls._pip_install_gcp_hint_last_sent = time.time()
            return []
        except gcp.http_error_exception() as e:
            # Cluster information is not available.
            # Return empty list.
            logger.debug(f'{e.message}', exc_info=True)
            return []

        machine_types = []
        # Get the list of machine types that are available in the cluster.
        node_pools = cluster.get('nodePools', [])
        for node_pool in node_pools:
            name = node_pool.get('name', '')
            logger.debug(f'Checking if node pool {name} '
                         'has autoscaling enabled.')
            autoscaling_enabled = (node_pool.get('autoscaling',
                                                 {}).get('enabled', False))
            if autoscaling_enabled:
                logger.debug(f'Node pool {name} has autoscaling enabled.')
                try:
                    machine_type = node_pool.get('config',
                                                 {}).get('machineType', '')
                    if machine_type:
                        machine_types.append(machine_type)
                except KeyError:
                    logger.debug(f'Encountered KeyError while checking machine '
                                 f'type of node pool {name}.')
                    continue
        return machine_types

    @classmethod
    def _validate_context_name(cls, context: str) -> tuple[bool, str, str, str]:
        """Validates the context name is in the format of
        gke_PROJECT-ID_LOCATION_CLUSTER-NAME
        Returns:
            bool: True if the context name is in the format of
                gke_PROJECT-ID_LOCATION_CLUSTER-NAME
            str: project id
            str: location
            str: cluster name
        """
        context_components = context.split('_')
        if len(context_components) != 4 or context_components[0] != 'gke':
            logger.debug(
                f'context {context} is not in valid GKE context format.')
            return False, '', '', ''

        logger.debug(f'context {context} is in valid GKE context format.')
        return True, context_components[1], context_components[
            2], context_components[3]

    @classmethod
    def _check_instance_fits_gke_autoscaler_node_pool(
        cls, instance_type: str, node_pool: dict
    ) -> bool:  # check if there are any spare capacity in the autoscaler.
        node_pool_name = node_pool['name']
        logger.debug(
            f'checking if autoscale-enabled node pool {node_pool_name} '
            f'can create a node satisfying {instance_type}')
        k8s_instance_type = (
            KubernetesInstanceType.from_instance_type(instance_type))
        node_config = node_pool['config']
        machine_type = node_config['machineType']

        # Accelerator check
        requested_acc_type = k8s_instance_type.accelerator_type
        requested_acc_count = k8s_instance_type.accelerator_count
        acc_is_tpu = (requested_acc_type is not None and
                      is_tpu_on_gke(requested_acc_type))
        if requested_acc_type is not None:
            assert requested_acc_count is not None, (requested_acc_type,
                                                     requested_acc_count)
            accelerator_exists = False
            if acc_is_tpu:
                # Accelerator type is a TPU.
                logger.debug(
                    f'checking {node_pool_name} for TPU {requested_acc_type}:'
                    f'{requested_acc_count}')
                if 'resourceLabels' in node_config:
                    requested_acc_type, requested_acc_count = normalize_tpu_accelerator_name(
                        requested_acc_type)
                    accelerator_exists = cls._node_pool_has_tpu_capacity(
                        node_config['resourceLabels'], machine_type,
                        requested_acc_type, requested_acc_count)
            else:
                # Accelerator type is a GPU.
                logger.debug(
                    f'checking {node_pool_name} for GPU {requested_acc_type}:'
                    f'{requested_acc_count}')
                if 'accelerators' in node_config:
                    accelerator_exists = cls._node_pool_has_gpu_capacity(
                        node_config['accelerators'], requested_acc_type,
                        requested_acc_count)

            if not accelerator_exists:
                logger.debug(f'{node_pool_name} does not have accelerators '
                             f'{requested_acc_type}:{requested_acc_count}')
                return False

        # vcpu and memory check is not supported for TPU instances.
        # TODO(seungjin): Correctly account for vcpu/memory for TPUs.
        if acc_is_tpu:
            # vcpu and memory check
            logger.debug(f'vcpu and memory check is not supported for TPUs. '
                         'Skipping vcpu and memory check for node pool '
                         f'{node_pool_name}.')
            return True

        try:
            vcpus, mem = clouds.GCP.get_vcpus_mem_from_instance_type(
                machine_type)
        except ValueError as e:
            logger.warning(
                f'Failed to get vcpu and memory from instance type '
                f'{machine_type}. Skipping the fit check for node pool '
                f'{node_pool_name}, assuming the node pool can create a node '
                f'satisfying {k8s_instance_type}. Error: {e}')
            return True
        if vcpus is not None and vcpus < k8s_instance_type.cpus:
            logger.debug(f'vcpu check failed for {machine_type} '
                         f'on node pool {node_pool_name}')
            return False
        if mem is not None and mem < k8s_instance_type.memory:
            logger.debug(f'memory check failed for {machine_type} '
                         f'on node pool {node_pool_name}')
            return False

        logger.debug(f'node pool {node_pool_name} can create a node '
                     f'satisfying {instance_type}')
        return True

    @classmethod
    def _node_pool_has_gpu_capacity(cls, node_pool_accelerators: list[dict],
                                    requested_gpu_type: str,
                                    requested_gpu_count: int) -> bool:
        """Check if the node pool has enough GPU capacity
        to fit the instance type.
        """
        for accelerator in node_pool_accelerators:
            raw_value = accelerator['acceleratorType']
            node_accelerator_type = (
                GKELabelFormatter.get_accelerator_from_label_value(raw_value))
            # handle heterogenous nodes.
            if not node_accelerator_type:
                continue
            node_accelerator_count = accelerator['acceleratorCount']
            viable_names = [node_accelerator_type.lower(), raw_value.lower()]
            # Use _accelerator_name_matches for backward compatibility
            # with GPU name changes (e.g., 'H200' vs 'H200-SXM-80GB').
            if (_accelerator_name_matches(requested_gpu_type, viable_names) and
                    int(node_accelerator_count) >= requested_gpu_count):
                return True
        return False

    @classmethod
    def _node_pool_has_tpu_capacity(cls, node_pool_resource_labels: dict,
                                    machine_type: str, requested_tpu_type: str,
                                    requested_tpu_count: int) -> bool:
        """Check if the node pool has enough TPU capacity
        to fit the instance type.
        """

        if 'goog-gke-tpu-node-pool-type' not in node_pool_resource_labels:
            # This node does not have TPUs.
            return False
        if cls._is_node_multi_host_tpu(node_pool_resource_labels):
            # This node is a multi-host TPU.
            # multi-host TPUs are not supported in SkyPilot yet.
            return False
        node_tpu_type = node_pool_resource_labels['goog-gke-accelerator-type']
        # infer chip count from instance type
        tpu_chip_count = cls._tpu_chip_count_from_instance_type(machine_type)

        # For TPUs, the number of requested TPU count
        # must exactly match the TPU count in the instance.
        return (node_tpu_type == requested_tpu_type and
                tpu_chip_count == requested_tpu_count)

    @classmethod
    def _tpu_chip_count_from_instance_type(cls, machine_type: str) -> int:
        """Infer the number of TPU chips from the instance type."""
        # according to
        # https://cloud.google.com/kubernetes-engine/docs/concepts/tpus#machine_type
        # GKE TPU machine types have the format of
        # ct<version>-<type>-<node-chip-count>t
        logger.debug(
            f'inferring TPU chip count from machine type: {machine_type}')
        pattern = r'ct[a-z0-9]+-[a-z]+-([0-9]+)t'
        search = re.search(pattern, machine_type)
        if search is None:
            logger.debug(f'machine type {machine_type} is not a '
                         'valid TPU machine type format.')
            return 0
        num_tpu_chips = search.group(1)
        logger.debug(
            f'machine type {machine_type} has {num_tpu_chips} TPU chips.')
        return int(num_tpu_chips)

    @classmethod
    def _is_node_multi_host_tpu(cls, resource_labels: dict) -> bool:
        """Check if the node pool is a multi-host TPU."""
        return ('goog-gke-tpu-node-pool-type' in resource_labels and
                resource_labels['goog-gke-tpu-node-pool-type'] == 'multi-host')


class KarpenterAutoscaler(Autoscaler):
    """Karpenter autoscaler
    """

    label_formatter: Any = KarpenterLabelFormatter
    can_query_backend: bool = False


class CoreweaveAutoscaler(Autoscaler):
    """CoreWeave autoscaler
    """

    label_formatter: Any = CoreWeaveLabelFormatter
    can_query_backend: bool = False


class NebiusAutoscaler(Autoscaler):
    """Nebius autoscaler
    """

    label_formatter: Any = NebiusLabelFormatter
    can_query_backend: bool = False


class GenericAutoscaler(Autoscaler):
    """Generic autoscaler
    """

    label_formatter: Any = SkyPilotLabelFormatter
    can_query_backend: bool = False


# Mapping of autoscaler type to autoscaler
AUTOSCALER_TYPE_TO_AUTOSCALER = {
    kubernetes_enums.KubernetesAutoscalerType.GKE: GKEAutoscaler,
    kubernetes_enums.KubernetesAutoscalerType.KARPENTER: KarpenterAutoscaler,
    kubernetes_enums.KubernetesAutoscalerType.COREWEAVE: CoreweaveAutoscaler,
    kubernetes_enums.KubernetesAutoscalerType.NEBIUS: NebiusAutoscaler,
    kubernetes_enums.KubernetesAutoscalerType.GENERIC: GenericAutoscaler,
}


def get_autoscaler(autoscaler_type: kubernetes_enums.KubernetesAutoscalerType):
    return AUTOSCALER_TYPE_TO_AUTOSCALER.get(autoscaler_type, Autoscaler)


@annotations.lru_cache(scope='request', maxsize=10)
def detect_accelerator_resource(context: str | None) -> tuple[bool, set[str]]:
    """Checks if the Kubernetes cluster has GPU/TPU resource.

    Three types of accelerator resources are available which are each checked
    with amd.com/gpu, nvidia.com/gpu and google.com/tpu. If amd.com/gpu or nvidia.com/gpu resource is
    missing, that typically means that the Kubernetes cluster does not have
    GPUs or the amd/nvidia GPU operator and/or device drivers are not installed.

    Returns:
        bool: True if the cluster has GPU_RESOURCE_KEY or TPU_RESOURCE_KEY
            resource, False otherwise.
    """
    # Get the set of resources across all nodes
    cluster_resources: set[str] = set()
    nodes = get_kubernetes_nodes(context=context)
    for node in nodes:
        cluster_resources.update(node.status.allocatable.keys())
    has_accelerator = (get_gpu_resource_key(context) in cluster_resources or
                       TPU_RESOURCE_KEY in cluster_resources)

    return has_accelerator, cluster_resources


@dataclasses.dataclass
class V1ObjectMeta:
    name: str
    labels: dict[str, str]
    namespace: str = ''  # Used for pods, not nodes


@dataclasses.dataclass
class V1NodeAddress:
    type: str
    address: str


@dataclasses.dataclass
class V1NodeCondition:
    """Represents a Kubernetes node condition."""
    type: str
    status: str


@dataclasses.dataclass(frozen=True)
class _KubernetesNodeReadinessState:
    """Immutable result of streaming zero or more node conditions."""

    decided: bool = False
    is_ready: bool = False


def _transition_kubernetes_node_readiness(
    state: _KubernetesNodeReadinessState,
    condition_type: str,
    condition_status: str,
) -> _KubernetesNodeReadinessState:
    """Apply one condition while preserving the first-Ready contract."""
    if state.decided or condition_type != 'Ready':
        return state
    return _KubernetesNodeReadinessState(decided=True,
                                         is_ready=condition_status == 'True')


@dataclasses.dataclass
class V1NodeStatus:
    allocatable: dict[str, str]
    capacity: dict[str, str]
    addresses: list[V1NodeAddress]
    conditions: list[V1NodeCondition]


@dataclasses.dataclass
class V1Taint:
    """Represents a Kubernetes node taint."""
    key: str
    effect: str
    value: str | None = None


@dataclasses.dataclass
class V1NodeSpec:
    """Represents a Kubernetes node spec."""
    unschedulable: bool
    taints: list[V1Taint]


@dataclasses.dataclass
class V1Node:
    """Represents a Kubernetes node."""
    metadata: V1ObjectMeta
    status: V1NodeStatus
    spec: V1NodeSpec

    @classmethod
    def from_dict(cls, data: dict) -> 'V1Node':
        """Create V1Node from a dictionary."""
        spec_data = data.get('spec', {})
        return cls(metadata=V1ObjectMeta(
            name=data['metadata']['name'],
            labels=data['metadata'].get('labels', {}),
        ),
                   status=V1NodeStatus(
                       allocatable=data['status']['allocatable'],
                       capacity=data['status']['capacity'],
                       addresses=[
                           V1NodeAddress(type=addr['type'],
                                         address=addr['address'])
                           for addr in data['status'].get('addresses', [])
                       ],
                       conditions=[
                           V1NodeCondition(type=cond['type'],
                                           status=cond['status'])
                           for cond in data['status'].get('conditions', [])
                       ]),
                   spec=V1NodeSpec(unschedulable=spec_data.get(
                       'unschedulable', False),
                                   taints=[
                                       V1Taint(key=taint['key'],
                                               effect=taint['effect'],
                                               value=taint.get('value'))
                                       for taint in spec_data.get('taints', [])
                                   ]))

    def is_ready(self) -> bool:
        """Check if the node is ready based on its conditions.

        A node is considered ready if it has a 'Ready' condition with
        status 'True'.
        """
        readiness = _KubernetesNodeReadinessState()
        for condition in self.status.conditions:
            readiness = _transition_kubernetes_node_readiness(
                readiness, condition.type, condition.status)
            if readiness.decided:
                break
        return readiness.is_ready

    def is_cordoned(self) -> bool:
        """Check if the node is cordoned based on its spec.unschedulable."""
        return self.spec.unschedulable

    def get_taints(
        self,
        exclude_cordon: bool = False,
        exclude_not_ready: bool = False,
        exclude_effects: list[str] | None = None,
        exclude_keys: list[str] | None = None,
        exclude_key_prefixes: list[str] | None = None,
        tolerations: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Get the taints on the node.

        Args:
            exclude_cordon: Whether to exclude the cordon taint.
            exclude_not_ready: Whether to exclude the not ready taint.
            exclude_effects: The taint effects to exclude,
              e.g. ['PreferNoSchedule'].
            exclude_keys: The taint keys to exclude.
            exclude_key_prefixes: Taint key prefixes to exclude,
              e.g. ['node-role.kubernetes.io/'].
            tolerations: Optional list of Kubernetes toleration dicts
              (typically read from `kubernetes.pod_config.spec.tolerations`).
              When provided, each retained taint dict gains a
              `'tolerated': bool` key indicating whether any of the
              tolerations matches it (Kubernetes semantics).
              When omitted, returned dicts have no `'tolerated'` key
              (backwards-compatible with existing callers).

        Returns:
            List[Dict[str, Any]]: The taints on the node.
        """
        taints = []
        for t in self.spec.taints:
            if (exclude_cordon and
                    t.key == 'node.kubernetes.io/unschedulable' and
                    t.effect == 'NoSchedule'):
                continue
            if (exclude_not_ready and
                    t.key == 'node.kubernetes.io/unreachable' and
                (t.effect == 'NoSchedule' or t.effect == 'NoExecute')):
                continue
            if exclude_effects and t.effect in exclude_effects:
                continue
            if exclude_keys and t.key in exclude_keys:
                continue
            if exclude_key_prefixes and any(
                    t.key.startswith(p) for p in exclude_key_prefixes):
                continue
            taint_dict: dict[str, Any] = {
                'key': t.key,
                'value': t.value if t.value else None,
                'effect': t.effect,
            }
            if tolerations is not None:
                taint_dict['tolerated'] = taint_is_tolerated(
                    taint_dict, tolerations)
            taints.append(taint_dict)
        return taints


class KubernetesObservationLimitError(ValueError):
    """A provider response exceeded a declared observation bound."""


class _KubernetesObservationAcceptedByteLimitExhausted(
        KubernetesObservationLimitError):
    """The aggregate accepted-byte budget has been consumed exactly."""


class KubernetesObservationBudget:
    """Thread-safe, monotonic aggregate budget for provider observations.

    Byte capacity is reserved before provider I/O. Successful reads convert
    the returned byte count into a permanent charge and release only unused
    reservation capacity. Failed reads permanently charge their full
    reservation because the amount physically consumed is unknown. Node
    records are charged before they are retained. Permanent charges are never
    refunded, including when a response is partial or later fails validation,
    so one instance can safely span concurrent context reads in a future
    observation source.
    """

    __slots__ = (
        '_consumed_node_records',
        '_consumed_response_bytes',
        '_lock',
        '_maximum_node_records',
        '_maximum_response_bytes',
        '_reserved_response_bytes',
    )

    def __init__(self, *, maximum_response_bytes: int,
                 maximum_node_records: int) -> None:
        if (type(maximum_response_bytes) is not int or
                maximum_response_bytes < 1):
            raise ValueError(
                'maximum_response_bytes must be a positive integer.')
        if type(maximum_node_records) is not int or maximum_node_records < 1:
            raise ValueError('maximum_node_records must be a positive integer.')
        self._maximum_response_bytes = maximum_response_bytes
        self._maximum_node_records = maximum_node_records
        self._consumed_response_bytes = 0
        self._consumed_node_records = 0
        self._reserved_response_bytes = 0
        self._lock = threading.Condition()

    @property
    def consumed_response_bytes(self) -> int:
        with self._lock:
            return self._consumed_response_bytes

    @property
    def remaining_response_bytes(self) -> int:
        with self._lock:
            return (self._maximum_response_bytes -
                    self._consumed_response_bytes -
                    self._reserved_response_bytes)

    @property
    def consumed_node_records(self) -> int:
        with self._lock:
            return self._consumed_node_records

    @property
    def remaining_node_records(self) -> int:
        with self._lock:
            return self._maximum_node_records - self._consumed_node_records

    def consume_response_bytes(self, byte_count: int) -> None:
        """Atomically charge bytes before returning them to a parser."""
        if type(byte_count) is not int or byte_count < 0:
            raise ValueError('byte_count must be a nonnegative integer.')
        with self._lock:
            if byte_count > (self._maximum_response_bytes -
                             self._consumed_response_bytes -
                             self._reserved_response_bytes):
                raise KubernetesObservationLimitError(
                    'Kubernetes observation exceeds its aggregate byte '
                    'bound.')
            self._consumed_response_bytes += byte_count

    def _read_and_consume_response_bytes(self, source: Any,
                                         requested_bytes: int) -> bytes:
        """Reserve a bounded physical read and permanently charge its use."""
        if type(requested_bytes) is not int or requested_bytes < 1:
            raise ValueError('requested_bytes must be a positive integer.')
        with self._lock:
            while True:
                remaining = (self._maximum_response_bytes -
                             self._consumed_response_bytes -
                             self._reserved_response_bytes)
                if remaining > 0:
                    break
                if (self._consumed_response_bytes
                        >= self._maximum_response_bytes):
                    raise _KubernetesObservationAcceptedByteLimitExhausted(
                        'Kubernetes observation aggregate byte bound is '
                        'exhausted.')
                # Capacity is only temporarily reserved by another read. Wait
                # without holding the condition lock across provider I/O.
                self._lock.wait()
            bounded_request = min(requested_bytes, remaining)
            self._reserved_response_bytes += bounded_request

        permanent_charge = bounded_request
        try:
            data = source.read(bounded_request)
            if not isinstance(data, bytes):
                raise ValueError(
                    'Kubernetes node response must be a byte stream.')
            byte_count = len(data)
            if byte_count > bounded_request:
                raise KubernetesObservationLimitError(
                    'Kubernetes node response violated its read bound.')
            permanent_charge = byte_count
        finally:
            # On failure, the source may have physically consumed any portion
            # of the reservation. Conservatively charge the whole reservation.
            with self._lock:
                self._reserved_response_bytes -= bounded_request
                self._consumed_response_bytes += permanent_charge
                self._lock.notify_all()
        return data

    def consume_node_record(self) -> None:
        """Atomically charge one node before retaining its projection."""
        with self._lock:
            if self._consumed_node_records >= self._maximum_node_records:
                raise KubernetesObservationLimitError(
                    'Kubernetes observation exceeds its aggregate node '
                    'bound.')
            self._consumed_node_records += 1

    def __repr__(self) -> str:
        return (f'{type(self).__name__}('
                f'consumed_response_bytes={self.consumed_response_bytes}, '
                f'remaining_response_bytes={self.remaining_response_bytes}, '
                f'consumed_node_records={self.consumed_node_records}, '
                f'remaining_node_records={self.remaining_node_records})')


@dataclasses.dataclass(frozen=True)
class KubernetesNodeResources:
    """Frozen provider-object-free resource projection of one node."""

    name: str
    is_ready: bool
    cpu_capacity: float
    memory_capacity_gb: float
    cpu_allocatable: float
    memory_allocatable_gb: float
    _cpu_capacity_for_fitting: int | float = dataclasses.field(repr=False,
                                                               compare=False)

    def __post_init__(self) -> None:
        _validate_observed_node_name(self.name)
        if type(self.is_ready) is not bool:
            raise ValueError('Kubernetes node readiness must be a bool.')
        for value in (
                self.cpu_capacity,
                self.memory_capacity_gb,
                self.cpu_allocatable,
                self.memory_allocatable_gb,
                self._cpu_capacity_for_fitting,
        ):
            if (isinstance(value, bool) or not isinstance(value,
                                                          (int, float)) or
                    not math.isfinite(value) or value < 0):
                raise ValueError('Kubernetes node resources must be finite and '
                                 'nonnegative.')


@dataclasses.dataclass(frozen=True)
class KubernetesNodeObservation:
    """Frozen bounded result of one provider-order node-list observation."""

    node_resources: tuple[KubernetesNodeResources, ...]
    cpu_avoid_accelerator_label_keys: tuple[str, ...]

    def __post_init__(self) -> None:
        if (type(self.node_resources) is not tuple or
                any(not isinstance(node, KubernetesNodeResources)
                    for node in self.node_resources)):
            raise ValueError('Kubernetes node resources must be a tuple.')
        keys = self.cpu_avoid_accelerator_label_keys
        if type(keys) is not tuple:
            raise ValueError(
                'Kubernetes CPU avoid-accelerator label keys must be a tuple.')
        if len(keys) > _MAX_OBSERVED_ACCELERATOR_LABEL_KEYS:
            raise KubernetesObservationLimitError(
                'Kubernetes CPU avoid-accelerator label keys exceed their '
                'count bound.')
        for key in keys:
            _bounded_observed_string(
                key,
                maximum_bytes=_MAX_OBSERVED_ACCELERATOR_LABEL_KEY_BYTES,
                field_name='Kubernetes accelerator label key')


def _bounded_observed_string(value: Any,
                             *,
                             maximum_bytes: int,
                             field_name: str,
                             allow_empty: bool = False) -> str:
    if type(value) is not str or (not allow_empty and not value):
        raise ValueError(f'{field_name} must be a string.')
    if len(value.encode('utf-8')) > maximum_bytes:
        raise KubernetesObservationLimitError(
            f'{field_name} exceeds its byte bound.')
    return value


def _validate_observed_node_name(value: Any) -> str:
    name = _bounded_observed_string(
        value,
        maximum_bytes=(_MAX_OBSERVED_NODE_NAME_BYTES),
        field_name='Kubernetes node name')
    if (re.fullmatch(r'[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?', name) is None or
            '..' in name):
        raise ValueError('Kubernetes node name is invalid.')
    return name


_JsonPath = tuple[str | None, ...]
_NODE_ITEM_PATH: _JsonPath = ('items', None)
_NODE_METADATA_PATH = (*_NODE_ITEM_PATH, 'metadata')
_NODE_LABELS_PATH = (*_NODE_METADATA_PATH, 'labels')
_NODE_STATUS_PATH = (*_NODE_ITEM_PATH, 'status')
_NODE_CAPACITY_PATH = (*_NODE_STATUS_PATH, 'capacity')
_NODE_ALLOCATABLE_PATH = (*_NODE_STATUS_PATH, 'allocatable')
_NODE_CONDITIONS_PATH = (*_NODE_STATUS_PATH, 'conditions')
_NODE_CONDITION_ITEM_PATH = (*_NODE_CONDITIONS_PATH, None)


def _bounded_accelerator_label_keys(
    selector: _GPULabelFormatterSelector,) -> tuple[str, ...]:
    """Project the shared formatter decision into bounded canonical keys."""
    formatter = selector.selected_formatter_type()
    if formatter is None:
        return ()
    keys = formatter.get_label_keys()
    if not isinstance(keys, (list, tuple)):
        raise ValueError(
            'Kubernetes accelerator label keys must be a sequence.')
    if len(keys) > _MAX_OBSERVED_ACCELERATOR_LABEL_KEYS:
        raise KubernetesObservationLimitError(
            'Kubernetes CPU avoid-accelerator label keys exceed their count '
            'bound.')
    return tuple(
        _bounded_observed_string(
            key,
            maximum_bytes=_MAX_OBSERVED_ACCELERATOR_LABEL_KEY_BYTES,
            field_name='Kubernetes accelerator label key') for key in keys)


class _KubernetesNodeResourceBuilder:
    """Streaming state for the allowlisted fields of one node object."""

    __slots__ = (
        '_condition_count',
        '_condition_status',
        '_condition_type',
        '_cpu_allocatable',
        '_cpu_capacity',
        '_accelerator_label_detector',
        '_memory_allocatable',
        '_memory_capacity',
        '_name',
        '_open_containers',
        '_readiness',
        '_seen',
        '_seen_containers',
    )

    def __init__(
            self,
            accelerator_label_detector: _GPULabelFormatterSelector) -> None:
        self._accelerator_label_detector = accelerator_label_detector
        self._name: str | None = None
        self._cpu_capacity: str | None = None
        self._memory_capacity: str | None = None
        self._cpu_allocatable = '0'
        self._memory_allocatable = '0'
        self._readiness = _KubernetesNodeReadinessState()
        self._condition_count = 0
        self._condition_type: str | None = None
        self._condition_status: str | None = None
        self._seen: set[str] = set()
        self._seen_containers: set[_JsonPath] = set()
        self._open_containers: set[_JsonPath] = set()

    def _set_once(self, field: str, value: Any, *, maximum_bytes: int,
                  field_name: str) -> str:
        if field in self._seen:
            raise ValueError(f'{field_name} must not be repeated.')
        self._seen.add(field)
        return _bounded_observed_string(value,
                                        maximum_bytes=maximum_bytes,
                                        field_name=field_name)

    def accept(self, path: _JsonPath, event: str, value: Any) -> None:
        """Consume one parser event without retaining unrelated fields."""
        container_types = {
            _NODE_METADATA_PATH: 'map',
            _NODE_LABELS_PATH: 'map',
            _NODE_STATUS_PATH: 'map',
            _NODE_CAPACITY_PATH: 'map',
            _NODE_ALLOCATABLE_PATH: 'map',
            _NODE_CONDITIONS_PATH: 'array',
        }
        container_type = container_types.get(path)
        if container_type is not None:
            start_event = f'start_{container_type}'
            end_event = f'end_{container_type}'
            if event == start_event:
                if path in self._seen_containers:
                    raise ValueError(
                        'Kubernetes node container must not be repeated.')
                self._seen_containers.add(path)
                self._open_containers.add(path)
                return
            if event == end_event:
                if path not in self._open_containers:
                    raise ValueError('Kubernetes node container is malformed.')
                self._open_containers.remove(path)
                return
            raise ValueError(
                f'Kubernetes node {path[-1]} must be a {container_type}.')

        if path == (*_NODE_METADATA_PATH, 'name'):
            if event != 'string':
                raise ValueError('Kubernetes node name must be a string.')
            self._name = self._set_once(
                'name',
                value,
                maximum_bytes=_MAX_OBSERVED_NODE_NAME_BYTES,
                field_name='Kubernetes node name')
            return

        if (len(path) == len(_NODE_LABELS_PATH) + 1 and
                path[:-1] == _NODE_LABELS_PATH):
            label_key = path[-1]
            assert isinstance(label_key, str), path
            if event == 'null':
                label_value = None
            elif event == 'string':
                label_value = value
            else:
                raise ValueError(
                    'Kubernetes node label value must be a string or null.')
            self._accelerator_label_detector.accept(label_key, label_value)
            return

        resource_fields = {
            (*_NODE_CAPACITY_PATH, 'cpu'): ('cpu_capacity', '_cpu_capacity'),
            (*_NODE_CAPACITY_PATH, 'memory'):
                ('memory_capacity', '_memory_capacity'),
            (*_NODE_ALLOCATABLE_PATH, 'cpu'):
                ('cpu_allocatable', '_cpu_allocatable'),
            (*_NODE_ALLOCATABLE_PATH, 'memory'):
                ('memory_allocatable', '_memory_allocatable'),
        }
        resource_field = resource_fields.get(path)
        if resource_field is not None:
            if event != 'string':
                raise ValueError(
                    'Kubernetes node resource quantity must be a string.')
            seen_name, attribute_name = resource_field
            quantity = self._set_once(
                seen_name,
                value,
                maximum_bytes=_MAX_OBSERVED_RESOURCE_QUANTITY_BYTES,
                field_name='Kubernetes node resource quantity')
            setattr(self, attribute_name, quantity)
            return

        if path == _NODE_CONDITION_ITEM_PATH:
            if event == 'start_map':
                if self._condition_type is not None or self._condition_status is not None:
                    raise ValueError('Kubernetes node condition is incomplete.')
                self._condition_count += 1
                if self._condition_count > _MAX_OBSERVED_NODE_CONDITIONS:
                    raise KubernetesObservationLimitError(
                        'Kubernetes node conditions exceed their count bound.')
                return
            if event == 'end_map':
                if self._condition_type is None or self._condition_status is None:
                    raise ValueError('Kubernetes node condition is incomplete.')
                self._readiness = _transition_kubernetes_node_readiness(
                    self._readiness, self._condition_type,
                    self._condition_status)
                self._condition_type = None
                self._condition_status = None
                self._seen.discard('condition_type')
                self._seen.discard('condition_status')
                return
            raise ValueError('Kubernetes node condition must be an object.')
        if path == (*_NODE_CONDITION_ITEM_PATH, 'type'):
            if event != 'string':
                raise ValueError(
                    'Kubernetes node condition type must be a string.')
            self._condition_type = self._set_once(
                'condition_type',
                value,
                maximum_bytes=_MAX_OBSERVED_CONDITION_TYPE_BYTES,
                field_name='Kubernetes node condition type')
            return
        if path == (*_NODE_CONDITION_ITEM_PATH, 'status'):
            if event != 'string':
                raise ValueError(
                    'Kubernetes node condition status must be a string.')
            self._condition_status = self._set_once(
                'condition_status',
                value,
                maximum_bytes=_MAX_OBSERVED_CONDITION_STATUS_BYTES,
                field_name='Kubernetes node condition status')

    def build(self) -> KubernetesNodeResources:
        """Validate and freeze this node's retained resource fields."""
        if (self._open_containers or self._condition_type is not None or
                self._condition_status is not None):
            raise ValueError('Kubernetes node condition is incomplete.')
        required_containers = {
            _NODE_METADATA_PATH,
            _NODE_STATUS_PATH,
            _NODE_CAPACITY_PATH,
            _NODE_ALLOCATABLE_PATH,
        }
        if not required_containers.issubset(self._seen_containers):
            raise ValueError('Kubernetes node resource fields are incomplete.')
        if (self._name is None or self._cpu_capacity is None or
                self._memory_capacity is None):
            raise ValueError('Kubernetes node resource fields are incomplete.')
        name = _validate_observed_node_name(self._name)
        try:
            cpu_capacity = parse_cpu_or_gpu_resource_to_float(
                self._cpu_capacity)
            cpu_capacity_for_fitting = parse_cpu_or_gpu_resource(
                self._cpu_capacity)
            memory_capacity_gb = parse_memory_resource(self._memory_capacity,
                                                       unit='G')
            cpu_allocatable = parse_cpu_or_gpu_resource_to_float(
                self._cpu_allocatable)
            memory_allocatable_gb = parse_memory_resource(
                self._memory_allocatable, unit='G')
        except (IndexError, KeyError, OverflowError, TypeError, ValueError):
            raise ValueError(
                'Kubernetes node resource quantity is invalid.') from None
        return KubernetesNodeResources(
            name=name,
            is_ready=self._readiness.is_ready,
            cpu_capacity=cpu_capacity,
            memory_capacity_gb=memory_capacity_gb,
            cpu_allocatable=cpu_allocatable,
            memory_allocatable_gb=memory_allocatable_gb,
            _cpu_capacity_for_fitting=cpu_capacity_for_fitting,
        )


def get_allowed_nodes_config(
        context: str | None = None) -> dict[str, Any] | None:
    """Returns the allowed_nodes config for the given K8s context, or None.

    Reads from ~/.sky/config.yaml, respecting context_configs overrides.
    """
    return skypilot_config.get_effective_region_config(cloud='kubernetes',
                                                       region=context,
                                                       keys=('allowed_nodes',),
                                                       default_value=None)


def get_configured_tolerations(
        context: str | None = None) -> list[dict[str, Any]] | None:
    """Returns the configured pod tolerations for the given K8s context.

    Reads `kubernetes.pod_config.spec.tolerations` (or `ssh.pod_config
    .spec.tolerations` for an `ssh-<pool>` context) from
    ~/.sky/config.yaml, respecting context_configs overrides. Returns
    `None` if no tolerations are configured (the case for the vast
    majority of setups) so downstream callers can pass the result
    through to `V1Node.get_taints(tolerations=...)` and get
    byte-identical output to today.

    Goes through `resolve_effective_pod_config` rather than fetching the
    `tolerations` leaf directly so the per-context list is merged onto
    the global one (Kubernetes-specific dict merge appends list
    entries — see `merge_k8s_configs`) and matches what an actual pod
    gets at scheduling time. Fetching the leaf directly would let a
    per-context list clobber the global one.

    SSH-pool contexts (`ssh-<pool>`) read from the `ssh` config namespace
    and need their `ssh-` prefix stripped before looking up
    `context_configs.<pool>` — both handled inside
    `resolve_effective_pod_config` when `cloud` is an SSH instance.
    Auto-detect from the context name to preserve the signature.

    Each toleration is a dict in standard Kubernetes shape, e.g.
    `{'key': 'workload_pool', 'operator': 'Equal', 'value': 'research',
      'effect': 'NoSchedule'}`.
    """
    # Pass `cloud=clouds.SSH()` for ssh-prefixed contexts so
    # `resolve_effective_pod_config` reads the `ssh.*` namespace and
    # strips the `ssh-` prefix before applying context overrides.
    cloud: clouds.Cloud | None = (clouds.SSH() if context and
                                  context.startswith('ssh-') else None)
    pod_config = resolve_effective_pod_config(cluster_config_overrides={},
                                              cloud=cloud,
                                              context=context)
    spec = pod_config.get('spec') if isinstance(pod_config, dict) else None
    tolerations = spec.get('tolerations') if isinstance(spec, dict) else None
    if not isinstance(tolerations, list):
        return None
    # Filter to dict entries only — be defensive against malformed config.
    return [t for t in tolerations if isinstance(t, dict)]


# Coerce to str defensively — YAML can parse unquoted numbers/booleans
# as non-string types (e.g. `value: 123` → int), which would silently
# fail to match the K8s API's always-string taint fields. The
# `None`-explicit form rather than `str(x or '')` is what lets falsy-but-set
# values like `value: 0` and `value: false` survive the coercion
# (`str(0 or '')` would collapse to `''` and never match `'0'`).
#
# Booleans need special-casing because `str(True)` is Python-cased
# `'True'` but K8s stores taint values lowercase (`'true'` / `'false'`)
# — Go's YAML serializer (which K8s uses internally) always emits
# lowercase, and `kubectl taint nodes X foo=true:NoSchedule` stores
# `value: "true"`. Without the lowercase coercion, a config
# `value: true` (unquoted YAML → Python `True`) coerces to `'True'` and
# silently fails the exact string compare against the K8s `'true'`.
def _str_or_empty(v: Any) -> str:
    if v is None:
        return ''
    if isinstance(v, bool):
        return 'true' if v else 'false'
    return str(v)


def taint_is_tolerated(taint: dict[str, Any],
                       tolerations: list[dict[str, Any]]) -> bool:
    """Returns True if any of `tolerations` matches the given taint.

    Implements Kubernetes toleration semantics
    (https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/):

    - `operator: Equal` (the default if unset) requires the toleration's
      `key` and `value` to match the taint's `key` and `value` exactly.
    - `operator: Exists` requires the toleration's `key` to match the
      taint's `key`; the toleration's `value` is ignored.
    - An empty (or missing) `key` is only valid with `operator: Exists`
      and matches all taint keys (wildcard).
    - An empty (or missing) `effect` matches all taint effects;
      otherwise the toleration's `effect` must match the taint's `effect`
      exactly.

    Args:
        taint: A taint dict with at least `'key'` and `'effect'` keys
          (and optionally `'value'`), as produced by
          `V1Node.get_taints`.
        tolerations: List of toleration dicts (Kubernetes shape).

    Returns:
        True if at least one toleration in the list matches the taint.
    """
    taint_key = _str_or_empty(taint.get('key'))
    taint_effect = _str_or_empty(taint.get('effect'))
    taint_value = _str_or_empty(taint.get('value'))
    for tol in tolerations:
        if not isinstance(tol, dict):
            continue
        tol_effect = _str_or_empty(tol.get('effect'))
        if tol_effect and tol_effect != taint_effect:
            continue
        # `operator` is the only field where Equal is the documented
        # default per the K8s API spec, so a missing/empty value should
        # resolve to 'Equal' rather than ''.
        tol_op = _str_or_empty(tol.get('operator')) or 'Equal'
        tol_key = _str_or_empty(tol.get('key'))
        tol_value = _str_or_empty(tol.get('value'))
        if not tol_key:
            # Empty key is only valid with Exists; matches any key.
            if tol_op == 'Exists':
                return True
            continue
        if tol_key != taint_key:
            continue
        if tol_op == 'Exists':
            return True
        # Equal (default): values must match (treating absent value as '').
        if tol_value == taint_value:
            return True
    return False


def has_untolerated_taint(taints: list[dict[str, Any]] | None) -> bool:
    """Returns True if any taint in the list is NOT tolerated.

    Reads the `'tolerated'` flag previously attached to each taint dict by
    `V1Node.get_taints(tolerations=...)`. Taints without the flag (the
    backward-compatible shape from servers that don't know about
    tolerations, or callers that don't pass `tolerations=`) are treated as
    un-tolerated, matching the pre-toleration-aware behavior where any
    non-empty taint list made the node un-schedulable.

    This is the single source of truth for the "is this node tainted for
    user workloads?" predicate used by the catalog, `get_kubernetes_node_info`,
    and `sky show-gpus` aggregation.
    """
    if not taints:
        return False
    return any(not t.get('tolerated', False) for t in taints)


def _filter_allowed_nodes(nodes: list[V1Node],
                          context: str | None = None) -> list[V1Node]:
    """Filter nodes based on the allowed_nodes config.

    All criteria across all sub-fields are OR'd: a node is included if it
    matches ANY label key-value pair, ANY name, or ANY IP address.

    Args:
        nodes: List of V1Node objects to filter.
        context: K8s context name (for reading per-context config).

    Returns:
        Filtered list of nodes. Returns all nodes if no allowed_nodes
        config is set or if the config is empty.
    """
    config = get_allowed_nodes_config(context)
    if config is None:
        return nodes

    label_selector = config.get('label_selector', {})
    allowed_names = set(config.get('names', []))
    allowed_ips = set(config.get('ips', []))

    # If all sub-fields are empty, return all nodes (empty config = no filter).
    if not label_selector and not allowed_names and not allowed_ips:
        return nodes

    # Use a set to track already-added nodes and avoid duplicates when a
    # node matches multiple criteria.
    seen = set()
    filtered = []
    for node in nodes:
        if node.metadata.name in seen:
            continue

        matched = False

        # Check each label key-value pair (OR'd: match if ANY pair matches).
        if label_selector:
            for key, value in label_selector.items():
                if node.metadata.labels.get(key) == value:
                    matched = True
                    break

        # Check node name.
        if not matched and allowed_names:
            if node.metadata.name in allowed_names:
                matched = True

        # Check IPs (matches against any address type: InternalIP,
        # ExternalIP, etc.).
        if not matched and allowed_ips:
            node_ips = {addr.address for addr in node.status.addresses}
            if node_ips & allowed_ips:
                matched = True

        if matched:
            seen.add(node.metadata.name)
            filtered.append(node)

    return filtered


def inject_allowed_nodes_affinity(
    pod_spec: dict[str, Any],
    allowed_nodes_config: dict[str, Any] | None,
    context: str | None = None,
) -> dict[str, Any]:
    """Inject nodeAffinity constraints for allowed_nodes into a pod spec.

    Ensures pods are only scheduled on nodes permitted by the allowed_nodes
    config. Builds one matchExpression per criterion:

    - Each label key-value pair becomes a label expression (autoscaler-
      friendly: new nodes matching the label are automatically eligible).
    - Names/IPs are resolved to a single kubernetes.io/hostname In
      expression (static snapshot at scheduling time).

    These expressions are then cross-producted with the existing
    nodeSelectorTerms. Since K8s OR's across terms but AND's within a
    term, this correctly expresses:

        existing_constraints AND (label1 OR label2 OR hostname_set)

    For example, with GPU affinity + label_selector {pool: gpu} +
    names [node-01]:

        (GPU AND pool=gpu) OR (GPU AND hostname in [node-01])

    Args:
        pod_spec: The pod spec dict (pod_spec['spec'] level, containing
            'affinity', 'containers', etc.).
        allowed_nodes_config: The allowed_nodes config dict, or None.
        context: K8s context for resolving nodes when names/IPs are used.

    Returns:
        The modified pod_spec (also modified in-place).
    """
    if allowed_nodes_config is None:
        return pod_spec

    label_selector = allowed_nodes_config.get('label_selector', {})
    has_names_or_ips = bool(
        allowed_nodes_config.get('names') or allowed_nodes_config.get('ips'))

    # Nothing to do if the config is completely empty.
    if not label_selector and not has_names_or_ips:
        return pod_spec

    # Build a list of matchExpression entries — one per allowed_nodes
    # criterion. Each entry represents one OR'd alternative. Labels are
    # forwarded directly (autoscaler-friendly: new nodes matching the
    # label are automatically eligible). Names/IPs are resolved to a
    # kubernetes.io/hostname In expression (static snapshot).
    allowed_exprs: list[dict[str, Any]] = []

    for key, value in label_selector.items():
        allowed_exprs.append({
            'key': key,
            'operator': 'In',
            'values': [value],
        })

    if has_names_or_ips:
        # Resolve node names and IPs to kubernetes.io/hostname values.
        # This label is set by the kubelet and may differ from
        # node.metadata.name in some setups, so we look it up from
        # the actual node objects rather than assuming equality.
        allowed_names = set(allowed_nodes_config.get('names', []))
        allowed_ips = set(allowed_nodes_config.get('ips', []))
        all_nodes = get_kubernetes_nodes(context=context)
        hostnames = set()
        for node in all_nodes:
            matched = False
            if node.metadata.name in allowed_names:
                matched = True
            if not matched and allowed_ips:
                node_ips = {a.address for a in node.status.addresses}
                if node_ips & allowed_ips:
                    matched = True
            if matched:
                hostnames.add(
                    node.metadata.labels.get('kubernetes.io/hostname',
                                             node.metadata.name))
        if not hostnames and not label_selector:
            raise exceptions.ResourcesUnavailableError(
                'No Kubernetes nodes match the allowed_nodes filter '
                'in ~/.sky/config.yaml. Check your allowed_nodes '
                'configuration.')
        if hostnames:
            allowed_exprs.append({
                'key': 'kubernetes.io/hostname',
                'operator': 'In',
                'values': sorted(hostnames),
            })

    if not allowed_exprs:
        return pod_spec

    # Cross-product the allowed expressions with existing nodeSelectorTerms.
    # Each allowed expression becomes a separate term (OR'd by K8s), and
    # within each term the expression is AND'd with existing constraints
    # (e.g., GPU label). This correctly expresses:
    #   existing_constraints AND (criterion1 OR criterion2 OR ...)
    affinity = pod_spec.setdefault('affinity', {})
    node_affinity = affinity.setdefault('nodeAffinity', {})
    required = node_affinity.setdefault(
        'requiredDuringSchedulingIgnoredDuringExecution', {})
    existing_terms = required.get('nodeSelectorTerms', [])

    base_terms = existing_terms if existing_terms else [{}]
    new_terms = []
    for expr in allowed_exprs:
        for term in base_terms:
            new_term = copy.deepcopy(term)
            new_term.setdefault('matchExpressions', []).append(expr)
            new_terms.append(new_term)
    required['nodeSelectorTerms'] = new_terms

    return pod_spec


class _KubernetesObservationReader:
    """Charges decompressed stream bytes before returning them to ijson."""

    __slots__ = ('_budget', '_consumed_bytes', '_eof_probe_state',
                 '_maximum_bytes', '_source')

    _EOF_PROBE_NOT_RUN = 0
    _EOF_PROBE_CONFIRMED = 1
    _EOF_PROBE_REJECTED = 2

    def __init__(self, source: Any, *, maximum_bytes: int,
                 budget: KubernetesObservationBudget) -> None:
        self._source = source
        self._maximum_bytes = maximum_bytes
        self._budget = budget
        self._consumed_bytes = 0
        self._eof_probe_state = self._EOF_PROBE_NOT_RUN

    @property
    def consumed_bytes(self) -> int:
        return self._consumed_bytes

    def _known_remaining_bytes(self) -> int | None:
        try:
            # urllib3's ``length_remaining`` tracks encoded wire bytes.  A
            # decoding HTTPResponse can consume all of those bytes while its
            # content decoder still buffers output, so zero is not decoded
            # EOF.  Treat such responses as unknown-length and use the one-byte
            # bounded EOF probe instead.
            decode_content = getattr(self._source, 'decode_content', False)
            headers = getattr(self._source, 'headers', None)
            get_header = getattr(headers, 'get', None)
            content_encoding = (get_header('content-encoding')
                                if callable(get_header) else None)
            if (decode_content is True and content_encoding is not None and
                    str(content_encoding).strip().lower()
                    not in ('', 'identity')):
                return None
            length_remaining = getattr(self._source, 'length_remaining', None)
            if type(length_remaining) is int and length_remaining >= 0:
                return length_remaining
            getbuffer = getattr(self._source, 'getbuffer', None)
            tell = getattr(self._source, 'tell', None)
            if callable(getbuffer) and callable(tell):
                remaining = len(getbuffer()) - tell()
                if type(remaining) is int and remaining >= 0:
                    return remaining
        except (AttributeError, OSError, TypeError, ValueError):
            pass
        return None

    def _probe_unknown_length_eof(self) -> bytes:
        """Perform this reader's sole discard-only one-byte EOF probe."""
        if self._eof_probe_state == self._EOF_PROBE_CONFIRMED:
            return b''
        if self._eof_probe_state == self._EOF_PROBE_REJECTED:
            raise KubernetesObservationLimitError(
                'Kubernetes node response EOF probe was already rejected.')

        # Mark the probe rejected before provider I/O so an error, including a
        # BaseException, can never cause a second physical probe.
        self._eof_probe_state = self._EOF_PROBE_REJECTED
        data = self._source.read(1)
        if not isinstance(data, bytes):
            raise ValueError('Kubernetes node response must be a byte stream.')
        if len(data) > 1:
            raise KubernetesObservationLimitError(
                'Kubernetes node response violated its EOF probe bound.')
        if data:
            raise KubernetesObservationLimitError(
                'Kubernetes node response exceeds its accepted byte bound.')
        self._eof_probe_state = self._EOF_PROBE_CONFIRMED
        return b''

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b''
        if self._eof_probe_state == self._EOF_PROBE_CONFIRMED:
            return b''
        if self._eof_probe_state == self._EOF_PROBE_REJECTED:
            raise KubernetesObservationLimitError(
                'Kubernetes node response EOF probe was already rejected.')
        known_remaining = self._known_remaining_bytes()
        if known_remaining == 0:
            return b''
        response_remaining = self._maximum_bytes - self._consumed_bytes
        if response_remaining == 0:
            if known_remaining is None:
                return self._probe_unknown_length_eof()
            raise KubernetesObservationLimitError(
                'Kubernetes node response byte bound is exhausted.')
        requested = (response_remaining if size is None or size < 0 else min(
            size, response_remaining))
        try:
            data = self._budget._read_and_consume_response_bytes(  # pylint: disable=protected-access
                self._source, requested)
        except _KubernetesObservationAcceptedByteLimitExhausted:
            if known_remaining is None:
                return self._probe_unknown_length_eof()
            raise KubernetesObservationLimitError(
                'Kubernetes observation aggregate byte bound is exhausted '
                'before response EOF.') from None
        byte_count = len(data)
        self._consumed_bytes += byte_count
        return data


@dataclasses.dataclass
class _JsonContainerState:
    """One bounded component of a structural JSON path."""

    kind: Literal['map', 'array']
    component: str | None
    pending_key: str | None = None


def _json_container_path(containers: list[_JsonContainerState]) -> _JsonPath:
    """Build one ephemeral path from linear retained container state."""
    # containers[0] is the root and therefore has no path component.
    return tuple(container.component for container in containers[1:])


def _iter_bounded_structured_json_events(
    reader: _KubernetesObservationReader
) -> typing.Iterator[tuple[_JsonPath, str, Any]]:
    """Yield basic-parser events with paths derived from real containers."""
    containers: list[_JsonContainerState] = []
    saw_root = False
    completed_root = False

    for event, value in ijson.basic_parse(reader, buf_size=IJSON_BUFFER_SIZE):
        if event == 'map_key':
            _bounded_observed_string(value,
                                     maximum_bytes=_MAX_OBSERVED_JSON_KEY_BYTES,
                                     field_name='Kubernetes node JSON key')
            if (not containers or containers[-1].kind != 'map' or
                    containers[-1].pending_key is not None):
                raise ValueError('Kubernetes node JSON structure is malformed.')
            containers[-1].pending_key = value
            continue

        if event in ('end_map', 'end_array'):
            expected_kind = 'map' if event == 'end_map' else 'array'
            if not containers or containers[-1].kind != expected_kind:
                raise ValueError('Kubernetes node JSON structure is malformed.')
            container = containers[-1]
            if container.pending_key is not None:
                raise ValueError('Kubernetes node JSON structure is malformed.')
            path = _json_container_path(containers)
            containers.pop()
        else:
            if containers:
                parent = containers[-1]
                if parent.kind == 'map':
                    if parent.pending_key is None:
                        raise ValueError(
                            'Kubernetes node JSON structure is malformed.')
                    component = parent.pending_key
                    parent.pending_key = None
                else:
                    component = None
                path = (*_json_container_path(containers), component)
            else:
                if saw_root:
                    raise ValueError(
                        'Kubernetes node JSON must have one root object.')
                component = None
                path = ()

            if event in ('start_map', 'start_array'):
                kind: Literal['map', 'array'] = ('map' if event == 'start_map'
                                                 else 'array')
                if path == ():
                    if event != 'start_map':
                        raise ValueError(
                            'Kubernetes node JSON root must be an object.')
                    saw_root = True
                if len(containers) >= _MAX_OBSERVED_JSON_CONTAINER_DEPTH:
                    raise KubernetesObservationLimitError(
                        'Kubernetes node JSON exceeds its container depth '
                        'bound.')
                containers.append(
                    _JsonContainerState(kind=kind, component=component))
            elif not containers:
                raise ValueError('Kubernetes node JSON root must be an object.')

        yield path, event, value
        if path == () and event == 'end_map':
            completed_root = True

    if containers or not completed_root:
        raise ValueError('Kubernetes node JSON collection is incomplete.')


def _parse_bounded_kubernetes_node_observation(
    response: Any,
    *,
    maximum_nodes: int,
    maximum_response_bytes: int,
    budget: KubernetesObservationBudget,
) -> KubernetesNodeObservation:
    """Stream only placement resource fields from one complete node list."""
    reader = _KubernetesObservationReader(
        response, maximum_bytes=(maximum_response_bytes), budget=budget)
    nodes: list[KubernetesNodeResources] = []
    accelerator_label_detector = _GPULabelFormatterSelector()
    builder: _KubernetesNodeResourceBuilder | None = None
    saw_items_start = False
    saw_items_end = False
    saw_metadata_start = False
    saw_metadata_end = False
    saw_continue = False
    saw_remaining_item_count = False

    for path, event, value in _iter_bounded_structured_json_events(reader):
        if event == 'string':
            _bounded_observed_string(
                value,
                maximum_bytes=_MAX_OBSERVED_JSON_STRING_BYTES,
                field_name='Kubernetes node JSON string',
                allow_empty=True)

        if path == ('metadata',):
            if event == 'start_map':
                if saw_metadata_start:
                    raise ValueError(
                        'Kubernetes list metadata must not be repeated.')
                saw_metadata_start = True
                continue
            if event == 'end_map':
                if not saw_metadata_start or saw_metadata_end:
                    raise ValueError('Kubernetes list metadata is malformed.')
                saw_metadata_end = True
                continue
            raise ValueError('Kubernetes list metadata must be an object.')

        if path == ('items',):
            if event == 'start_array':
                if saw_items_start:
                    raise ValueError(
                        'Kubernetes node items must not be repeated.')
                saw_items_start = True
                continue
            if event == 'end_array':
                if not saw_items_start or builder is not None:
                    raise ValueError(
                        'Kubernetes node collection is incomplete.')
                saw_items_end = True
                continue
            raise ValueError('Kubernetes node items must be an array.')

        if path == _NODE_ITEM_PATH:
            if event == 'start_map':
                if builder is not None or not saw_items_start or saw_items_end:
                    raise ValueError('Kubernetes node collection is malformed.')
                builder = _KubernetesNodeResourceBuilder(
                    accelerator_label_detector)
                continue
            if event == 'end_map':
                if builder is None:
                    raise ValueError('Kubernetes node collection is malformed.')
                node = builder.build()
                builder = None
                if len(nodes) >= maximum_nodes:
                    raise KubernetesObservationLimitError(
                        'Kubernetes node collection exceeds its count bound.')
                budget.consume_node_record()
                nodes.append(node)
                continue
            raise ValueError('Kubernetes node item must be an object.')

        if builder is not None:
            builder.accept(path, event, value)

        if (len(path) > 2 and path[0] == 'metadata' and
                path[-1] in ('continue', 'remainingItemCount')):
            raise ValueError(
                'Kubernetes list pagination metadata must be direct fields.')
        if path == ('metadata', 'continue'):
            if saw_continue:
                raise ValueError(
                    'Kubernetes list continuation must not be repeated.')
            saw_continue = True
            if event == 'null':
                continue
            if event != 'string':
                raise ValueError(
                    'Kubernetes list continuation must be a string.')
            continuation = _bounded_observed_string(
                value,
                maximum_bytes=_MAX_OBSERVED_CONTINUE_TOKEN_BYTES,
                field_name='Kubernetes list continuation',
                allow_empty=True)
            if continuation:
                raise KubernetesObservationLimitError(
                    'Kubernetes node collection is paginated.')
            continue
        if path == ('metadata', 'remainingItemCount'):
            if saw_remaining_item_count:
                raise ValueError(
                    'Kubernetes remaining item count must not be repeated.')
            saw_remaining_item_count = True
            if event == 'null':
                continue
            if event != 'number' or type(value) is not int or value < 0:
                raise ValueError(
                    'Kubernetes remaining item count must be nonnegative.')
            if value > 0:
                raise KubernetesObservationLimitError(
                    'Kubernetes node collection is paginated.')

    if (builder is not None or not saw_items_start or not saw_items_end or
            not saw_metadata_start or not saw_metadata_end):
        raise ValueError('Kubernetes node collection is incomplete.')
    node_names: set[str] = set()
    for node in nodes:
        if node.name in node_names:
            raise ValueError('Kubernetes node names must be unique.')
        node_names.add(node.name)
    return KubernetesNodeObservation(
        node_resources=tuple(nodes),
        cpu_avoid_accelerator_label_keys=(
            _bounded_accelerator_label_keys(accelerator_label_detector)),
    )


def _close_kubernetes_observation_response(response: Any) -> None:
    """Best-effort close without replacing the response's primary failure."""
    try:
        close = getattr(response, 'close', None)
        if callable(close):
            close()
    except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        pass


def get_kubernetes_node_observation_uncached_bounded(
    *,
    core_api: Any,
    maximum_nodes: int,
    maximum_response_bytes: int,
    budget: KubernetesObservationBudget,
) -> KubernetesNodeObservation:
    """Read a complete bounded node projection through an explicit client.

    The caller owns ``core_api`` and its underlying raw client. This helper
    retains no V1Node, raw label map, label value, annotation, or unrecognized
    provider field and always releases the response. Pagination and every
    overflow fail closed; no input is truncated.
    """
    if type(maximum_nodes) is not int or maximum_nodes < 1:
        raise ValueError('maximum_nodes must be a positive integer.')
    if (type(maximum_response_bytes) is not int or maximum_response_bytes < 1):
        raise ValueError('maximum_response_bytes must be a positive integer.')
    if not isinstance(budget, KubernetesObservationBudget):
        raise ValueError('budget must be a KubernetesObservationBudget.')

    response = core_api.list_node(
        limit=maximum_nodes + 1,
        _request_timeout=kubernetes.API_TIMEOUT,
        _preload_content=False,
    )
    try:
        observation = _parse_bounded_kubernetes_node_observation(
            response,
            maximum_nodes=maximum_nodes,
            maximum_response_bytes=maximum_response_bytes,
            budget=budget,
        )
    except BaseException:  # pylint: disable=broad-exception-caught
        # A partially consumed response must not be reused. Release is still
        # attempted, but cleanup failures must never replace the primary parse
        # or limit error.
        _close_kubernetes_observation_response(response)
        try:
            response.release_conn()
        except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
            pass
        raise

    try:
        response.release_conn()
    except BaseException:  # pylint: disable=broad-exception-caught
        # A release failure is the primary failure after a successful parse.
        # Close is a best-effort fallback and must not mask it.
        _close_kubernetes_observation_response(response)
        raise
    return observation


@annotations.lru_cache(scope='request', maxsize=10)
@_retry_on_error(resource_type='node')
def get_kubernetes_nodes(*, context: str | None = None) -> list[V1Node]:
    """Gets the kubernetes nodes in the context.

    If context is None, gets the nodes in the current context.

    If allowed_nodes is configured in ~/.sky/config.yaml, the returned list
    is filtered to only include nodes matching the config. All criteria
    (labels, names, IPs) are OR'd.
    """
    if context is None:
        context = get_current_kube_config_context_name()

    # Return raw urllib3.HTTPResponse object so that we can parse the json
    # more efficiently.
    response = kubernetes.core_api(context).list_node(
        _request_timeout=kubernetes.API_TIMEOUT, _preload_content=False)
    try:
        nodes = [
            V1Node.from_dict(item_dict) for item_dict in ijson.items(
                response, 'items.item', buf_size=IJSON_BUFFER_SIZE)
        ]
    finally:
        response.release_conn()

    # Apply allowed_nodes filtering if configured.
    nodes = _filter_allowed_nodes(nodes, context)

    return nodes


# These aliases preserve the historical Kubernetes utility import surface while
# pod failure classification and remediation live in a focused module.
# pylint: disable=protected-access
_iter_terminated_states = pod_diagnostics._iter_terminated_states
get_condensed_pod_reason = pod_diagnostics.get_condensed_pod_reason
pod_terminated_abnormally = pod_diagnostics.pod_terminated_abnormally
KUBERNETES_FAILURE_HINTS = pod_diagnostics.KUBERNETES_FAILURE_HINTS
match_kubernetes_failure_hint = pod_diagnostics.match_kubernetes_failure_hint
match_kubernetes_failure_hint_text = (
    pod_diagnostics.match_kubernetes_failure_hint_text)
get_failure_hint_reasons = pod_diagnostics.get_failure_hint_reasons
diagnose_terminated_pod = pod_diagnostics.diagnose_terminated_pod

# Preserve module and pickle identities for historical imports.
for _pod_diagnostics_symbol in (
        _iter_terminated_states,
        get_condensed_pod_reason,
        pod_terminated_abnormally,
        match_kubernetes_failure_hint,
        match_kubernetes_failure_hint_text,
        get_failure_hint_reasons,
        diagnose_terminated_pod,
):
    _pod_diagnostics_symbol.__module__ = __name__
# pylint: enable=protected-access

# These aliases preserve the historical Kubernetes utility import surface while
# user pod configuration validation and composition live in a focused module.
PodValidator = pod_config_lib.PodValidator
check_pod_config = pod_config_lib.check_pod_config
resolve_effective_pod_config = pod_config_lib.resolve_effective_pod_config
combine_pod_config_fields = pod_config_lib.combine_pod_config_fields
combine_metadata_fields = pod_config_lib.combine_metadata_fields
combine_pod_config_fields_and_metadata = (
    pod_config_lib.combine_pod_config_fields_and_metadata)
merge_custom_metadata = pod_config_lib.merge_custom_metadata
get_cleaned_context_and_cloud_str = (
    pod_config_lib.get_cleaned_context_and_cloud_str)

# Preserve module and pickle identities for historical imports.
for _pod_config_symbol in (
        PodValidator,
        check_pod_config,
        resolve_effective_pod_config,
        combine_pod_config_fields,
        combine_metadata_fields,
        combine_pod_config_fields_and_metadata,
        merge_custom_metadata,
        get_cleaned_context_and_cloud_str,
):
    _pod_config_symbol.__module__ = __name__


@dataclasses.dataclass
class V1PodStatus:
    phase: str


@dataclasses.dataclass
class V1ResourceRequirements:
    requests: dict[str, str] | None


@dataclasses.dataclass
class V1Container:
    resources: V1ResourceRequirements


@dataclasses.dataclass
class V1PodSpec:
    containers: list[V1Container]
    node_name: str | None


@dataclasses.dataclass
class V1Pod:
    metadata: V1ObjectMeta
    status: V1PodStatus
    spec: V1PodSpec

    @classmethod
    def from_dict(cls, data: dict) -> 'V1Pod':
        """Create V1Pod from a dictionary."""
        return cls(metadata=V1ObjectMeta(
            name=data['metadata']['name'],
            labels=data['metadata'].get('labels', {}),
            namespace=data['metadata'].get('namespace'),
        ),
                   status=V1PodStatus(phase=data['status'].get('phase'),),
                   spec=V1PodSpec(
                       node_name=data['spec'].get('nodeName'),
                       containers=[
                           V1Container(resources=V1ResourceRequirements(
                               requests=container.get('resources', {}).get(
                                   'requests') or None))
                           for container in data['spec'].get('containers', [])
                       ]))


@_retry_on_error(resource_type='pod')
def get_allocated_resources_by_node(
    *,
    context: str | None = None,
) -> tuple[dict[str, int], dict[str, tuple[float, float]]]:
    """Gets allocated GPU, CPU, and memory by each node by fetching pods in
    all namespaces in kubernetes cluster indicated by context.

    This function combines GPU and CPU/memory allocation tracking into a single
    API call for better performance.

    Returns:
        Tuple of (allocated_gpu_qty_by_node, allocated_cpu_memory_by_node):
        - allocated_gpu_qty_by_node: Dict mapping node name to allocated GPU count
        - allocated_cpu_memory_by_node: Dict mapping node name to (allocated_cpu, allocated_memory_gb) tuple
    """
    if context is None:
        context = get_current_kube_config_context_name()
    non_included_pod_statuses = POD_STATUSES.copy()
    status_filters = ['Running', 'Pending']
    non_included_pod_statuses -= set(status_filters)
    field_selector = ','.join(
        [f'status.phase!={status}' for status in non_included_pod_statuses])

    # Return raw urllib3.HTTPResponse object so that we can parse the json
    # more efficiently.
    response = kubernetes.core_api(context).list_pod_for_all_namespaces(
        _request_timeout=kubernetes.API_TIMEOUT,
        _preload_content=False,
        field_selector=field_selector)
    try:
        allocated_qty_by_node: dict[str, int] = collections.defaultdict(int)
        allocated_cpu_memory_by_node: dict[str, tuple[
            float, float]] = collections.defaultdict(lambda: (0.0, 0.0))
        for item_dict in ijson.items(response,
                                     'items.item',
                                     buf_size=IJSON_BUFFER_SIZE):
            pod = V1Pod.from_dict(item_dict)
            if should_exclude_pod_from_gpu_allocation(pod):
                logger.debug(
                    f'Excluding pod {pod.metadata.name} from resource count '
                    f'calculations on node {pod.spec.node_name}')
                continue
            if not pod.spec.node_name:
                continue

            # Iterate over all the containers in the pod and sum the resources
            pod_allocated_qty = 0
            pod_allocated_cpu = 0.0
            pod_allocated_memory_gb = 0.0
            for container in pod.spec.containers:
                if container.resources.requests:
                    requests = container.resources.requests
                    # Parse GPU
                    pod_allocated_qty += get_node_accelerator_count(
                        context, requests)
                    # Parse CPU
                    if 'cpu' in requests:
                        pod_allocated_cpu += parse_cpu_or_gpu_resource_to_float(
                            requests['cpu'])
                    # Parse memory
                    if 'memory' in requests:
                        pod_allocated_memory_gb += parse_memory_resource(
                            requests['memory'], unit='G')

            if pod_allocated_qty > 0:
                allocated_qty_by_node[pod.spec.node_name] += pod_allocated_qty
            if pod_allocated_cpu > 0 or pod_allocated_memory_gb > 0:
                current_cpu, current_memory = allocated_cpu_memory_by_node[
                    pod.spec.node_name]
                allocated_cpu_memory_by_node[pod.spec.node_name] = (
                    current_cpu + pod_allocated_cpu,
                    current_memory + pod_allocated_memory_gb)
        return allocated_qty_by_node, allocated_cpu_memory_by_node
    finally:
        response.release_conn()


@_retry_on_error(resource_type='pod')
def get_allocated_gpu_qty_by_node(
    *,
    context: str | None = None,
) -> dict[str, int]:
    """Gets allocated GPU quantity by each node by fetching pods in
    all namespaces in kubernetes cluster indicated by context.

    Note: For better performance when you also need CPU/memory allocation,
    use get_allocated_resources_by_node() instead.
    """
    allocated_qty_by_node, _ = get_allocated_resources_by_node(context=context)
    return allocated_qty_by_node


def adjust_resources_to_allocatable(
    cpus: float,
    mem: float,
    context: str | None,
    dryrun: bool = False,
) -> tuple[float, float]:
    """Clamps resource requests to the minimum allocatable values across
    nodes whose capacity matches the request.

    Each K8s node reserves resources for system services (kubelet,
    kube-system pods, eviction thresholds). When a user requests
    resources that match a node's total capacity, the pod may fail to
    schedule because the allocatable amount is less than capacity.

    CPU and memory are evaluated independently. A node contributes its
    allocatable CPU if its CPU capacity matches the request, and its
    allocatable memory if its memory capacity matches the request.
    Nodes that don't match either resource are ignored. The final
    clamped values are the minimums across all contributing nodes for
    each resource, ensuring the pod can schedule on the node with the
    most system overhead.

    Args:
        cpus: Requested CPU count.
        mem: Requested memory in GB.
        context: Kubernetes context.
        dryrun: Is a dry run.

    Returns:
        Tuple of (adjusted_cpus, adjusted_mem).
    """
    if dryrun:
        return cpus, mem
    nodes = get_kubernetes_nodes(context=context)
    ready_nodes = [n for n in nodes if n.is_ready()]

    # If any node has strictly more capacity than requested for both
    # CPU and memory, the scheduler can place the pod there without
    # clamping.
    min_clamp_cpu: float | None = None
    min_clamp_mem: float | None = None
    for node in ready_nodes:
        node_cap_cpu = parse_cpu_or_gpu_resource_to_float(
            node.status.capacity.get('cpu', '0'))
        node_cap_mem = parse_memory_resource(
            node.status.capacity.get('memory', '0'),
            unit='G',
        )
        if (node_cap_cpu > cpus + 0.01 and node_cap_mem > mem + 0.01):
            return cpus, mem
        # Collect allocatable values independently from exact-match
        # nodes: a node contributes its allocatable CPU if its CPU
        # capacity matches the request, and likewise for memory.
        cpu_matches = abs(node_cap_cpu - cpus) < 0.01
        mem_matches = abs(node_cap_mem - mem) < 0.01
        if cpu_matches:
            alloc_cpu = parse_cpu_or_gpu_resource_to_float(
                node.status.allocatable.get('cpu', '0'))
            clamp_cpu = alloc_cpu - node_cap_cpu * 0.1
            if min_clamp_cpu is None or clamp_cpu < min_clamp_cpu:
                min_clamp_cpu = clamp_cpu
        if mem_matches:
            alloc_mem = parse_memory_resource(node.status.allocatable.get(
                'memory', '0'),
                                              unit='G')
            clamp_mem = alloc_mem - node_cap_mem * 0.05
            if min_clamp_mem is None or clamp_mem < min_clamp_mem:
                min_clamp_mem = clamp_mem

    adjusted_cpus = min_clamp_cpu or cpus
    adjusted_mem = min_clamp_mem or mem

    assert adjusted_cpus > 0.0, 'Adjusted cpu should be greater than 0.'
    assert adjusted_mem > 0.0, 'Adjusted memory should be greater than 0.'

    if adjusted_cpus < cpus or adjusted_mem < mem:
        logger.info(f'Clamping resource request to node allocatable capacity. '
                    f'Requested: {cpus} CPUs, {mem}G memory. '
                    f'Adjusted: {adjusted_cpus} CPUs, {adjusted_mem}G memory.')

    return adjusted_cpus, adjusted_mem


def check_instance_fits(context: str | None,
                        instance: str) -> tuple[bool, str | None]:
    """Checks if the instance fits on the Kubernetes cluster.

    If the instance has GPU requirements, checks if the GPU type is
    available on the cluster and if enough CPU/memory is available on any node
    with the GPU type.

    Args:
        instance: str, the instance type to check.

    Returns:
        bool: True if the instance fits on the cluster, False otherwise.
        Optional[str]: Error message if the instance does not fit.
    """

    def check_cpu_mem_fits(candidate_instance_type: 'KubernetesInstanceType',
                           node_list: list[Any]) -> tuple[bool, str | None]:
        """Checks if the instance fits on the cluster based on CPU and memory.

        We check only capacity, not allocatable, because availability can
        change during scheduling, and we want to let the Kubernetes scheduler
        handle that.
        """
        # We log max CPU and memory found on the GPU nodes for debugging.
        max_cpu = 0.0
        max_mem = 0.0

        for node in node_list:
            node_cpus = parse_cpu_or_gpu_resource(node.status.capacity['cpu'])
            node_memory_gb = parse_memory_resource(
                node.status.capacity['memory'], unit='G')
            if node_cpus > max_cpu:
                max_cpu = node_cpus
                max_mem = node_memory_gb
            if (node_cpus >= candidate_instance_type.cpus and
                    node_memory_gb >= candidate_instance_type.memory):
                return True, None
        return False, (
            'Maximum resources found on a single node: '
            f'{max_cpu} CPUs, {common_utils.format_float(max_mem)}G Memory')

    def check_tpu_fits(acc_type: str, acc_count: int,
                       node_list: list[Any]) -> tuple[bool, str | None]:
        """Checks if the instance fits on the cluster based on requested TPU.

        It checks if the TPU type and count on each node match the required
        number of TPU chips for the instance. In the case of multi-host TPU
        podslice, the function ensures that the number of TPU chips on a single
        node (node_tpu_chip_count) and the total TPU chips across the entire
        podslice (topology_chip_count) are correctly handled.
        """
        tpu_list_in_cluster = []
        for node in node_list:
            if acc_type == node.metadata.labels[
                    GKELabelFormatter.TPU_LABEL_KEY]:
                # TODO(Doyoung): Update the logic when adding support for
                # multi-host TPUs.
                if is_multi_host_tpu(node.metadata.labels):
                    continue
                node_tpu_chip_count = int(node.metadata.labels[
                    GKELabelFormatter.ACCELERATOR_COUNT_LABEL_KEY])
                tpu_type = f'{acc_type}:{node_tpu_chip_count}'
                tpu_list_in_cluster.append(tpu_type)
                if node_tpu_chip_count == acc_count:
                    return True, None
        tpu_list_in_cluster_str = ','.join(tpu_list_in_cluster)
        # TODO(Doyoung): Update the error message raised with the multi-host
        # TPU support.
        return False, ('Requested TPU type was not found in the cluster. TPU '
                       'types found in the cluster: '
                       f'{tpu_list_in_cluster_str}. Note that multi-host TPU '
                       'podslices are currently not unsupported.')

    nodes = get_kubernetes_nodes(context=context)
    k8s_instance_type = KubernetesInstanceType.\
        from_instance_type(instance)
    acc_type = k8s_instance_type.accelerator_type
    acc_count = k8s_instance_type.accelerator_count
    if acc_type is not None:
        # If GPU/TPUs are requested, check if GPU/TPU type is available, and
        # if so, check if CPU and memory requirements on the specific node are
        # met.
        assert acc_count is not None, (acc_type, acc_count)
        try:
            gpu_label_key, gpu_label_values, _, _ = (
                get_accelerator_label_key_values(context, acc_type, acc_count))
            if gpu_label_values is None:
                gpu_label_values = []
        except exceptions.ResourcesUnavailableError as e:
            # If GPU not found, return empty list and error message.
            return False, str(e)
        # Get the set of nodes that have the GPU type
        gpu_nodes = [
            node for node in nodes
            if node.is_ready() and gpu_label_key in node.metadata.labels and
            node.metadata.labels[gpu_label_key] in gpu_label_values
        ]
        if not gpu_nodes:
            return False, f'No ready GPU nodes found with {acc_type} on the cluster'
        if is_tpu_on_gke(acc_type):
            # If requested accelerator is a TPU type, check if the cluster
            # has sufficient TPU resource to meet the requirement.
            acc_type, acc_count = normalize_tpu_accelerator_name(acc_type)
            fits, reason = check_tpu_fits(acc_type, acc_count, gpu_nodes)
            if reason is not None:
                return fits, reason
        else:
            # Check if any of the GPU nodes have sufficient number of GPUs.
            gpu_nodes = [
                node for node in gpu_nodes if get_node_accelerator_count(
                    context, node.status.allocatable) >= acc_count
            ]
            if not gpu_nodes:
                return False, (
                    f'No GPU nodes found with {acc_count} or more GPUs.')

        candidate_nodes = gpu_nodes
        not_fit_reason_prefix = (
            f'GPU nodes with {acc_type} do not have '
            f'enough CPU (>= {k8s_instance_type.cpus} CPUs) and/or '
            f'memory (>= {k8s_instance_type.memory} G). ')
    else:
        candidate_nodes = [node for node in nodes if node.is_ready()]
        if not candidate_nodes:
            return False, 'No ready nodes found in the cluster.'
        not_fit_reason_prefix = (f'No nodes found with enough '
                                 f'CPU (>= {k8s_instance_type.cpus} CPUs) '
                                 'and/or memory '
                                 f'(>= {k8s_instance_type.memory} G). ')
    # Check if CPU and memory requirements are met on at least one
    # candidate node.
    fits, reason = check_cpu_mem_fits(k8s_instance_type, candidate_nodes)
    if not fits:
        if reason is not None:
            reason = not_fit_reason_prefix + reason
        return fits, reason
    else:
        return fits, reason


def get_accelerator_label_keys(context: str | None,) -> list[str]:
    """Returns the label keys that should be avoided for scheduling
    CPU-only tasks.
    """
    label_formatter, _ = detect_gpu_label_formatter(context)
    if label_formatter is None:
        return []
    return label_formatter.get_label_keys()


def get_accelerator_label_key_values(
    context: str | None,
    acc_type: str,
    acc_count: int,
    check_mode=False
) -> tuple[str | None, list[str] | None, str | None, str | None]:
    """Returns the label key and values for the given GPU/TPU type.

    Args:
        acc_type: The GPU/TPU type required by the task.
        acc_count: Number of GPU/TPUs required by the task.
        check_mode: If True, only checks if the cluster has GPU/TPU resources
            and labels are setup on the cluster. acc_type is ignore does not
            return the label key and value. Useful for checking if GPUs are
            configured correctly on the cluster without explicitly requesting
            a acc_type.
    Returns:
        A tuple of the accelerator label key, value, topology label key, and
        topology value. The topology label key and value are populated only if
        the requested accelerator type is TPU. Returns None if check_mode is
        True.
    Raises:
        ResourcesUnavailableError: Can be raised from the following conditions:
            - The cluster does not have GPU/TPU resources
                (amd.com/gpu, nvidia.com/gpu, google.com/tpu)
            - The cluster has GPU/TPU resources, but no node in the cluster has
              an accelerator label.
            - The cluster has a node with an invalid accelerator label value.
            - The cluster doesn't have any nodes with acc_type GPU/TPU
    """
    # Check if the cluster has GPU resources
    # TODO(romilb): This assumes the accelerator is a amd/nvidia GPU. We
    #  need to support TPUs and other accelerators as well.
    # TODO(romilb): Currently, we broadly disable all GPU checks if autoscaling
    #  is configured in config.yaml since the cluster may be scaling up from
    #  zero nodes and may not have any GPU nodes yet. In the future, we should
    #  support pollingthe clusters for autoscaling information, such as the
    #  node pools configured etc.

    is_ssh_node_pool = context.startswith('ssh-') if context else False
    cloud_name = 'SSH Node Pool' if is_ssh_node_pool else 'Kubernetes cluster'
    context_display_name = common_utils.removeprefix(
        context, 'ssh-') if (context and is_ssh_node_pool) else context

    autoscaler_type = skypilot_config.get_effective_region_config(
        cloud='ssh' if is_ssh_node_pool else 'kubernetes',
        region=context,
        keys=('autoscaler',),
        default_value=None)
    if autoscaler_type is not None:
        # If autoscaler is set in config.yaml, override the label key and value
        # to the autoscaler's format and bypass the GPU checks.
        if check_mode:
            # If check mode is enabled and autoscaler is set, we can return
            # early since we assume the cluster autoscaler will handle GPU
            # node provisioning.
            return None, None, None, None
        autoscaler = AUTOSCALER_TYPE_TO_AUTOSCALER.get(
            kubernetes_enums.KubernetesAutoscalerType(autoscaler_type))
        assert autoscaler is not None, ('Unsupported autoscaler type:'
                                        f' {autoscaler_type}')
        formatter = autoscaler.label_formatter
        tpu_topology_label_key = None
        tpu_topology_label_value = None
        if is_tpu_on_gke(acc_type):
            assert formatter == GKELabelFormatter, formatter
            tpu_topology_label_key = formatter.get_tpu_topology_label_key()
            tpu_topology_label_value = formatter.get_tpu_topology_label_value(
                acc_type, acc_count)
        return formatter.get_label_key(acc_type), formatter.get_label_values(
            acc_type), tpu_topology_label_key, tpu_topology_label_value

    has_gpus, cluster_resources = detect_accelerator_resource(context)
    if has_gpus:
        # Check if the cluster has GPU labels setup correctly
        label_formatter, node_labels = \
            detect_gpu_label_formatter(context)
        if label_formatter is None:
            # If none of the GPU labels from LABEL_FORMATTER_REGISTRY are
            # detected, raise error
            with ux_utils.print_exception_no_traceback():
                supported_formats = ', '.join([
                    key for f in LABEL_FORMATTER_REGISTRY
                    for key in f.get_label_keys()
                ])
                suffix = ''
                if env_options.Options.SHOW_DEBUG_INFO.get():
                    suffix = f' Found node labels: {node_labels}'
                msg = (f'Could not detect GPU labels in {cloud_name}.')
                if not is_ssh_node_pool:
                    msg += (' Run `sky check ssh` to debug.')
                else:
                    msg += (
                        ' If this cluster has GPUs, please ensure GPU nodes have '
                        'node labels of either of these formats: '
                        f'{supported_formats}. Please refer to '
                        'the documentation on how to set up node labels.')
                msg += f'{suffix}'
                raise exceptions.ResourcesUnavailableError(msg)
        else:
            # Validate the label value on all nodes labels to ensure they are
            # correctly setup and will behave as expected.
            matching_label_key = None
            matching_label_values = []
            seen_label_values = set()
            for node_name, label_list in node_labels.items():
                for label, value in label_list:
                    if label_formatter.match_label_key(label):
                        is_valid, reason = label_formatter.validate_label_value(
                            value)
                        if not is_valid:
                            raise exceptions.ResourcesUnavailableError(
                                f'Node {node_name!r} in {cloud_name} has '
                                f'invalid GPU label: {label}={value}. {reason}')
            if check_mode:
                # If check mode is enabled and we reached so far, we can
                # conclude that the cluster is setup correctly and return.
                return None, None, None, None
            # Search in node_labels to see if any node has the requested
            # GPU type.
            # Note - this only checks if the label is available on a
            # node. It does not (and should not) check if the resource
            # quantity is available since that is dynamic and can change
            # during scheduling.
            for node_name, label_list in node_labels.items():
                node_metadata_labels = dict(label_list)
                # TODO(Doyoung): Update the logic when adding support for
                # multi-host TPUs.
                if is_multi_host_tpu(node_metadata_labels):
                    continue
                for label, value in label_list:
                    if label_formatter.match_label_key(label):
                        # Match either canonicalized name or raw name.
                        # Use _accelerator_name_matches for backward compatibility
                        # with GPU name changes (e.g., H200-SXM-80GB -> H200).
                        accelerator = (label_formatter.
                                       get_accelerator_from_label_value(value))
                        viable = [value.lower(), accelerator.lower()]
                        if not _accelerator_name_matches(acc_type, viable):
                            continue
                        if is_tpu_on_gke(acc_type):
                            assert isinstance(label_formatter,
                                              GKELabelFormatter)
                            if node_metadata_labels.get(
                                    label_formatter.TPU_LABEL_KEY) == acc_type:
                                topology_label_key = (
                                    label_formatter.get_tpu_topology_label_key(
                                    ))
                                # Instead of using get_tpu_topology_label_value,
                                # we use the node's label value to determine the
                                # topology. This is to make sure the node's
                                # available topology matches our request.
                                topology_value = node_metadata_labels.get(
                                    topology_label_key)
                                assert topology_value is not None
                                tpu_topology_chip_count = reduce_tpu_topology(
                                    topology_value)
                                # For single-host TPUs, there aren't multiple
                                # different topologies that maps to identical
                                # number of TPU chips.
                                if tpu_topology_chip_count == acc_count:
                                    return (label, [value], topology_label_key,
                                            topology_value)
                                else:
                                    continue
                        else:
                            if matching_label_key is None:
                                matching_label_key = label
                            if label != matching_label_key:
                                continue
                            if value not in seen_label_values:
                                matching_label_values.append(value)
                                seen_label_values.add(value)

            if matching_label_key is not None:
                return (matching_label_key, matching_label_values, None, None)

            # If no node is found with the requested acc_type, raise error
            with ux_utils.print_exception_no_traceback():
                suffix = ''
                if env_options.Options.SHOW_DEBUG_INFO.get():
                    all_labels = []
                    for node_name, label_list in node_labels.items():
                        all_labels.extend(label_list)
                    acc_available = set(v for k, v in all_labels
                                        if label_formatter.match_label_key(k))
                    suffix = (' Available GPU/TPUs on the cluster: '
                              f'{acc_available}')
                # TODO(Doyoung): Update the error message raised with the
                # multi-host TPU support.
                raise exceptions.ResourcesUnavailableError(
                    f'Could not find any node in the {cloud_name} '
                    f'with {acc_type}. Please ensure at least one node in the '
                    f'cluster has {acc_type} and node labels are setup '
                    'correctly. Please refer to the documentation for more. '
                    f'{suffix}. Note that multi-host TPU podslices are '
                    'currently not unsupported.')
    else:
        # If GPU resources are not detected, raise error
        with ux_utils.print_exception_no_traceback():
            suffix = ''
            if env_options.Options.SHOW_DEBUG_INFO.get():
                suffix = (' Available resources on the cluster: '
                          f'{cluster_resources}')
            if is_ssh_node_pool:
                msg = (
                    f'Could not detect GPUs in SSH Node Pool '
                    f'\'{context_display_name}\'. If this cluster contains '
                    'GPUs, please ensure GPU drivers are installed on the node '
                    'and re-run '
                    f'`sky ssh up --infra {context_display_name}`. {suffix}')
            else:
                msg = (
                    f'Could not detect GPU/TPU resources ({SUPPORTED_GPU_RESOURCE_KEYS["amd"]!r}, '
                    f'{SUPPORTED_GPU_RESOURCE_KEYS["nvidia"]!r} or '
                    f'{TPU_RESOURCE_KEY!r}) in Kubernetes cluster. If this cluster'
                    ' contains GPUs, please ensure GPU drivers are installed on '
                    'the node. Check if the GPUs are setup correctly by running '
                    '`kubectl describe nodes` and looking for the '
                    f'{SUPPORTED_GPU_RESOURCE_KEYS["amd"]!r}, '
                    f'{SUPPORTED_GPU_RESOURCE_KEYS["nvidia"]!r} or '
                    f'{TPU_RESOURCE_KEY!r} resource. '
                    'Please refer to the documentation on how to set up GPUs.'
                    f'{suffix}')
            raise exceptions.ResourcesUnavailableError(msg)
    assert False, 'This should not be reached'


def get_head_ssh_port(cluster_name: str, namespace: str,
                      context: str | None) -> int:
    svc_name = f'{cluster_name}-head-ssh'
    return get_port(svc_name, namespace, context)


def get_port(svc_name: str, namespace: str, context: str | None) -> int:
    """Gets the nodeport of the specified service.

    Args:
        svc_name (str): Name of the kubernetes service. Note that this may be
            different from the cluster name.
        namespace (str): Kubernetes namespace to look for the service in.
        context (str): Kubernetes context to use.
    """
    head_service = kubernetes.core_api(context).read_namespaced_service(
        svc_name, namespace)
    return head_service.spec.ports[0].node_port


def check_credentials(context: str | None,
                      timeout: int = kubernetes.API_TIMEOUT,
                      run_optional_checks: bool = False,
                      cloud: str = 'kubernetes') -> \
        tuple[bool, str | None]:
    """Check if the credentials in kubeconfig file are valid

    The RBAC probe ``list_namespaced_pod`` is issued against the
    workspace-resolved namespace (via ``get_namespace``) rather than the
    raw kubeconfig context default, so a user who only has access to
    their workspace's configured namespace is not reported as broken.

    Args:
        context (Optional[str]): The Kubernetes context to use. If none, uses
            in-cluster auth to check credentials, if available.
        timeout (int): Timeout in seconds for the test API call
        run_optional_checks (bool): Whether to run additional soft checks
            (exec-based auth, GPU labels) after the credential probe.
        cloud (str): Top-level config key the namespace resolver consults
            (e.g. ``'kubernetes'`` vs ``'ssh'``).

    Returns:
        bool: True if credentials are valid, False otherwise
        str: Error message if credentials are invalid, None otherwise
    """
    try:
        namespace = get_namespace(context=context, cloud=cloud)
        kubernetes.core_api(context).list_namespaced_pod(
            namespace, limit=1, _request_timeout=timeout)
        # This call is "free" because this function is a cached call,
        # and it will not be called again in this function.
        get_kubernetes_nodes(context=context)
    except ImportError:
        # TODO(romilb): Update these error strs to also include link to docs
        #  when docs are ready.
        return False, ('`kubernetes` package is not installed. '
                       'Install it with: pip install kubernetes')
    except kubernetes.api_exception() as e:
        # Check if the error is due to invalid credentials
        if e.status == 401:
            return False, 'Invalid credentials - do you have permission ' \
                          'to access the cluster?'
        else:
            return False, f'Failed to communicate with the cluster: {str(e)}'
    except kubernetes.config_exception() as e:
        return False, f'Invalid configuration file: {str(e)}'
    except kubernetes.max_retry_error():
        return False, ('Failed to communicate with the cluster - timeout. '
                       'Check if your cluster is running and your network '
                       'is stable.')
    except ValueError as e:
        return False, common_utils.format_exception(e)
    except Exception as e:  # pylint: disable=broad-except
        return False, ('An error occurred: '
                       f'{common_utils.format_exception(e, use_bracket=True)}')

    # Check if $KUBECONFIG envvar consists of multiple paths. We run this before
    # optional checks.
    try:
        _ = get_kubeconfig_paths()
    except ValueError as e:
        return False, f'{common_utils.format_exception(e, use_bracket=True)}'

    # If we reach here, the credentials are valid and Kubernetes cluster is up.
    if not run_optional_checks:
        return True, None

    # We now do softer checks to check if exec based auth is used and to
    # see if the cluster is GPU-enabled.
    _, exec_msg = is_kubeconfig_exec_auth(context)

    # We now check if GPUs are available and labels are set correctly on the
    # cluster, and if not we return hints that may help debug any issues.
    # This early check avoids later surprises for user when they try to run
    # `sky launch --gpus <gpu>` and the optimizer does not list Kubernetes as a
    # provider if their cluster GPUs are not setup correctly.
    gpu_msg = ''
    unlabeled_nodes = get_unlabeled_accelerator_nodes(context)
    if unlabeled_nodes:
        gpu_msg = (f'Cluster has {len(unlabeled_nodes)} nodes with '
                   f'accelerators that are not labeled. '
                   f'To label the nodes, run '
                   f'`sky gpus label --context {context}`')
    else:
        try:
            # This function raises a ResourcesUnavailableError in three cases:
            # 1. If no node in cluster has GPU/TPU resource in its capacity.
            #    (e.g. google.com/tpu, nvidia.com/gpu)
            # 2. If at least one node in cluster has GPU/TPU resource in its
            #    capacity, but no node in the cluster has an accelerator label.
            # 3. If an accelerator label on a node is invalid.
            # Exception 2 is a special case of a cluster having at least one
            # unlabelled node, which is caught in
            # `get_unlabeled_accelerator_nodes`.
            # Therefore, if `get_unlabeled_accelerator_nodes` detects unlabelled
            # nodes, we skip this check.
            get_accelerator_label_key_values(context,
                                             acc_type='',
                                             acc_count=0,
                                             check_mode=True)
        except exceptions.ResourcesUnavailableError as e:
            # If GPUs are not available, we return cluster as enabled
            # (since it can be a CPU-only cluster) but we also return the
            # exception message which serves as a hint for how to enable
            # GPU access.
            gpu_msg = str(e)
    if exec_msg and gpu_msg:
        return True, f'{gpu_msg}\n    Additionally, {exec_msg}'
    elif gpu_msg:
        return True, gpu_msg
    elif exec_msg:
        return True, exec_msg
    else:
        return True, None


def is_kubeconfig_exec_auth(
        context: str | None = None) -> tuple[bool, str | None]:
    """Checks if the kubeconfig file uses exec-based authentication."""
    return context_utils.is_kubeconfig_exec_auth(
        context, get_kubeconfig_text_fn=_get_kubeconfig_text_for_context)


def _get_kubeconfig_text_for_context(context: str | None = None) -> str:
    """Get the kubeconfig text for the given context."""
    return context_utils.get_kubeconfig_text_for_context(context)


@annotations.lru_cache(scope='request')
def get_current_kube_config_context_name() -> str | None:
    """Get the current kubernetes context from the kubeconfig file."""
    return context_utils.get_current_kube_config_context_name(
        is_incluster_config_available_fn=is_incluster_config_available)


def is_incluster_config_available() -> bool:
    """Check if in-cluster auth is available."""
    return context_utils.is_incluster_config_available()


def get_all_kube_context_names() -> list[str]:
    """Get all kubernetes context names available in the environment."""
    return context_utils.get_all_kube_context_names(
        is_incluster_config_available_fn=is_incluster_config_available)


@annotations.lru_cache(scope='request')
def get_kube_config_context_namespace(context_name: str | None = None) -> str:
    """Get the current kubernetes context namespace from the kubeconfig file."""
    return context_utils.get_kube_config_context_namespace(
        context_name, default_namespace=DEFAULT_NAMESPACE)


def get_namespace(context: str | None = None,
                  workspace: str | None = None,
                  override_configs: dict[str, Any] | None = None,
                  cloud: str = 'kubernetes') -> str:
    """Resolve the Kubernetes namespace for ``context``, with fallback."""
    return context_utils.get_namespace(
        context,
        workspace,
        override_configs,
        cloud,
        get_effective_namespace=skypilot_config.get_effective_namespace,
        get_kube_config_context_namespace_fn=get_kube_config_context_namespace)


def parse_cpu_or_gpu_resource_to_float(resource_str: str) -> float:
    if not resource_str:
        return 0.0
    if resource_str[-1] == 'm':
        return float(resource_str[:-1]) / 1000
    else:
        return float(resource_str)


def parse_cpu_or_gpu_resource(resource_qty_str: str) -> int | float:
    resource_str = str(resource_qty_str)
    if resource_str[-1] == 'm':
        # For example, '500m' rounds up to 1.
        return math.ceil(int(resource_str[:-1]) / 1000)
    else:
        return float(resource_str)


def parse_memory_resource(resource_qty_str: str,
                          unit: str = 'B') -> int | float:
    """Returns memory size in chosen units given a resource quantity string."""
    if unit not in MEMORY_SIZE_UNITS:
        valid_units = ', '.join(MEMORY_SIZE_UNITS.keys())
        raise ValueError(
            f'Invalid unit: {unit}. Valid units are: {valid_units}')

    resource_str = str(resource_qty_str)
    bytes_value: int | float
    try:
        bytes_value = int(resource_str)
    except ValueError:
        memory_size = re.sub(r'([KMGTPBm]+)', r' \1', resource_str)
        number, unit_index = [item.strip() for item in memory_size.split()]
        unit_index = unit_index[0]
        bytes_value = float(number) * MEMORY_SIZE_UNITS[unit_index]
    return bytes_value / MEMORY_SIZE_UNITS[unit]


KubernetesInstanceType = instance_type_lib.KubernetesInstanceType
KubernetesInstanceType.__module__ = __name__

construct_ssh_jump_command = ssh_utils.construct_ssh_jump_command
get_ssh_proxy_command = ssh_utils.get_ssh_proxy_command
create_proxy_command_script = ssh_utils.create_proxy_command_script
check_port_forward_mode_dependencies = (
    ssh_utils.check_port_forward_mode_dependencies)

# Preserve public identity for serialized references and introspection through
# the long-standing Kubernetes utilities facade.
for _ssh_utils_symbol in (construct_ssh_jump_command, get_ssh_proxy_command,
                          create_proxy_command_script,
                          check_port_forward_mode_dependencies):
    _ssh_utils_symbol.__module__ = __name__
del _ssh_utils_symbol


def get_endpoint_debug_message(context: str | None = None) -> str:
    """ Returns a string message for user to debug Kubernetes port opening

    Polls the configured ports mode on Kubernetes to produce an
    appropriate error message with debugging hints.

    Also checks if the
    """
    port_mode = network_utils.get_port_mode(None, context)
    if port_mode == kubernetes_enums.KubernetesPortMode.INGRESS:
        endpoint_type = 'Ingress'
        debug_cmd = 'kubectl describe ingress && kubectl describe ingressclass'
    elif port_mode == kubernetes_enums.KubernetesPortMode.LOADBALANCER:
        endpoint_type = 'LoadBalancer'
        debug_cmd = 'kubectl describe service'
    elif port_mode == kubernetes_enums.KubernetesPortMode.PODIP:
        endpoint_type = 'PodIP'
        debug_cmd = 'kubectl describe pod'
    else:
        raise ValueError(f'Unsupported Kubernetes port mode: {port_mode}')
    return ENDPOINTS_DEBUG_MESSAGE.format(endpoint_type=endpoint_type,
                                          debug_cmd=debug_cmd)


# Sidecar container names.
_DIND_CONTAINER_NAME = 'dind'
_BUILDKITD_CONTAINER_NAME = 'buildkitd'
# Cache subPath prefixes (used to isolate per-pod cache in a shared PVC).
_DIND_CACHE_SUBPATH_PREFIX = 'var_lib_docker'
_BUILDKIT_CACHE_SUBPATH_PREFIX = 'buildkit_cache'


class DockerMode(str, enum.Enum):
    """Modes for the ``enable_docker`` config."""
    ALL = 'ALL'
    BUILD = 'BUILD'


@dataclasses.dataclass(frozen=True)
class DockerSidecarDefaults:
    """Default image and volume names for a Docker sidecar mode."""
    image: str
    cli_image: str
    cache_vol_name: str
    cache_mount: str


@dataclasses.dataclass(frozen=True)
class DockerConfig:
    """Normalized ``enable_docker`` config produced by
    :func:`normalize_enable_docker_config`."""
    mode: DockerMode
    cache_volume: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict (for YAML round-trip via provider)."""
        return {'mode': self.mode.value, 'cache_volume': self.cache_volume}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> 'DockerConfig':
        """Reconstruct from a dict (e.g. read back from provider config)."""
        return cls(mode=DockerMode(d['mode']),
                   cache_volume=d.get('cache_volume'))


# Default images for each enable_docker mode.
DOCKER_SIDECAR_DEFAULTS: dict[DockerMode, DockerSidecarDefaults] = {
    DockerMode.ALL: DockerSidecarDefaults(
        image='docker:29.3-dind',
        cli_image='docker:29.3-cli',
        cache_vol_name='dind-storage',
        cache_mount='/var/lib/docker',
    ),
    DockerMode.BUILD: DockerSidecarDefaults(
        image='moby/buildkit:v0.28.0-rootless',
        cli_image='docker:29.3-cli',
        cache_vol_name='buildkit-cache',
        cache_mount='/home/user/.local/share/buildkit',
    ),
}


def normalize_enable_docker_config(
    raw: bool | str | dict[str, Any] | None,) -> DockerConfig | None:
    """Normalize ``enable_docker`` config into a :class:`DockerConfig`.

    Returns ``None`` when disabled.
    """
    if raw is None or raw is False:
        return None
    if raw is True or raw == DockerMode.ALL:
        return DockerConfig(mode=DockerMode.ALL)
    if raw == DockerMode.BUILD:
        return DockerConfig(mode=DockerMode.BUILD)
    if isinstance(raw, dict):
        if 'mode' not in raw:
            # Empty dict or dict without 'mode' key (e.g. default_value
            # from config lookup) — treat as disabled.
            return None
        mode_val = raw['mode']
        if mode_val is False:
            return None
        mode = DockerMode(mode_val)
        return DockerConfig(mode=mode, cache_volume=raw.get('cache_volume'))
    raise ValueError(f'Invalid enable_docker value: {raw!r}')


def inject_docker_cache_volume(
    pod_spec: dict[str, Any],
    docker_config: DockerConfig,
    pvc_name: str | None,
    context: str | None,
    namespace: str,
) -> None:
    """Inject a cache volume + volumeMount into the Docker sidecar container.

    Mutates *pod_spec* in place.

    * If *pvc_name* is set, a PVC-backed volume with a per-pod ``subPath``
      is added (for persistent cache).  For BuildKit the pod-level
      ``securityContext.fsGroup`` is set so the rootless daemon (uid 1000)
      can write to the PVC.
    * Otherwise an ``emptyDir`` is added so the builder avoids writing to
      the container overlay (nested overlayfs causes perf/stability issues
      for DinD).
    * If the user already mounted something at the cache path via
      ``pod_config``, this function is a no-op.
    """
    mode = docker_config.mode
    defaults = DOCKER_SIDECAR_DEFAULTS[mode]
    ctr_name = (_DIND_CONTAINER_NAME
                if mode == DockerMode.ALL else _BUILDKITD_CONTAINER_NAME)
    cache_vol_name = defaults.cache_vol_name
    cache_mount = defaults.cache_mount

    # Check if the user already mounted a volume at the cache path
    # via pod_config (e.g. manual emptyDir or hostPath).
    for ctr in pod_spec['spec'].get('containers', []):
        if ctr['name'] == ctr_name:
            for vm in ctr.get('volumeMounts', []):
                if vm.get('mountPath') == cache_mount:
                    return  # User-provided mount takes precedence.
            break

    if pvc_name:
        # PVC path: per-pod subPath for isolation.
        # For rootless buildkitd (uid/gid 1000), set fsGroup so the PVC
        # mount is writable.
        if mode == DockerMode.BUILD:
            pod_sec = pod_spec['spec'].setdefault('securityContext', {})
            pod_sec.setdefault('fsGroup', 1000)
            pod_sec.setdefault('fsGroupChangePolicy', 'OnRootMismatch')

        prefix = (_DIND_CACHE_SUBPATH_PREFIX
                  if mode == DockerMode.ALL else _BUILDKIT_CACHE_SUBPATH_PREFIX)
        pod_name = pod_spec['metadata']['name']
        hash_key = f'{context or ""}:{namespace}:{pod_name}'
        sub_path = (f'{prefix}_'
                    f'{hashlib.sha256(hash_key.encode()).hexdigest()[:12]}')

        # Reuse an existing volume entry for this PVC if one already exists
        # (avoids duplicate spec.volumes entries).
        existing_vol = next((
            v['name']
            for v in pod_spec['spec'].get('volumes', [])
            if v.get('persistentVolumeClaim', {}).get('claimName') == pvc_name),
                            None)
        if existing_vol:
            vol_name = existing_vol
        else:
            vol_name = cache_vol_name
            pod_spec['spec'].setdefault('volumes', []).append({
                'name': vol_name,
                'persistentVolumeClaim': {
                    'claimName': pvc_name
                },
            })

        for ctr in pod_spec['spec'].get('containers', []):
            if ctr['name'] == ctr_name:
                ctr.setdefault('volumeMounts', []).append({
                    'name': vol_name,
                    'mountPath': cache_mount,
                    'subPath': sub_path,
                })
    else:
        # No PVC: add an emptyDir so the builder doesn't write to the
        # container overlay layer.
        pod_spec['spec'].setdefault('volumes', []).append({
            'name': cache_vol_name,
            'emptyDir': {},
        })
        for ctr in pod_spec['spec'].get('containers', []):
            if ctr['name'] == ctr_name:
                ctr.setdefault('volumeMounts', []).append({
                    'name': cache_vol_name,
                    'mountPath': cache_mount,
                })


@_retry_on_error(resource_type='runtimeclass')
def check_nvidia_runtime_class(*, context: str | None = None) -> bool:
    """Checks if the 'nvidia' RuntimeClass exists in the cluster"""
    # Fetch the list of available RuntimeClasses
    runtime_classes = kubernetes.node_api(context).list_runtime_class()

    # Check if 'nvidia' RuntimeClass exists
    nvidia_exists = any(
        rc.metadata.name == 'nvidia' for rc in runtime_classes.items)
    return nvidia_exists


def check_secret_exists(secret_name: str, namespace: str,
                        context: str | None) -> bool:
    """Checks if a secret exists in a namespace

    Args:
        secret_name: Name of secret to check
        namespace: Namespace to check
    """

    try:
        kubernetes.core_api(context).read_namespaced_secret(
            secret_name, namespace, _request_timeout=kubernetes.API_TIMEOUT)
    except kubernetes.api_exception() as e:
        if e.status == 404:
            return False
        raise
    else:
        return True


def create_namespace(namespace: str, context: str | None) -> None:
    """Creates a namespace in the cluster.

    If the namespace already exists, logs a message and does nothing.

    Args:
        namespace: Name of the namespace to create
        context: Name of the context to use. Can be none to use default context.
    """
    kubernetes_client = kubernetes.kubernetes.client
    try:
        kubernetes.core_api(context).read_namespace(namespace)
    except kubernetes.api_exception() as e:
        if e.status != 404:
            raise
    else:
        return

    ns_metadata = dict(name=namespace, labels={'parent': 'skypilot'})
    merge_custom_metadata(ns_metadata, context)
    namespace_obj = kubernetes_client.V1Namespace(metadata=ns_metadata)
    try:
        kubernetes.core_api(context).create_namespace(namespace_obj)
    except kubernetes.api_exception() as e:
        if e.status == 409:
            logger.info(f'Namespace {namespace} already exists in the cluster.')
        else:
            raise


def get_head_pod_name(cluster_name_on_cloud: str):
    """Returns the pod name of the head pod for the given cluster name on cloud

    Args:
        cluster_name_on_cloud: Name of the cluster on cloud

    Returns:
        str: Pod name of the head pod
    """
    # We could have iterated over all pods in the namespace and checked for the
    # label, but since we know the naming convention, we can directly return the
    # head pod name.
    return f'{cluster_name_on_cloud}-head'


def get_custom_config_k8s_contexts() -> list[str]:
    """Returns the list of context names from the config"""
    contexts = skypilot_config.get_effective_region_config(
        cloud='kubernetes',
        region=None,
        keys=('context_configs',),
        default_value={})
    return [*contexts] or []


# Mapping of known spot label keys and values for different cluster types
# Add new cluster types here if they support spot instances along with the
# corresponding spot label key and value.
SPOT_LABEL_MAP = {
    kubernetes_enums.KubernetesAutoscalerType.GKE.value:
        ('cloud.google.com/gke-spot', 'true')
}


def get_autoscaler_type(
    context: str | None = None
) -> kubernetes_enums.KubernetesAutoscalerType | None:
    """Returns the autoscaler type by reading from config"""
    is_ssh_node_pool = context.startswith('ssh-') if context else False
    autoscaler_type = skypilot_config.get_effective_region_config(
        cloud='ssh' if is_ssh_node_pool else 'kubernetes',
        region=context,
        keys=('autoscaler',),
        default_value=None)
    if autoscaler_type is not None:
        autoscaler_type = kubernetes_enums.KubernetesAutoscalerType(
            autoscaler_type)
    return autoscaler_type


def get_spot_label(context: str | None = None) -> tuple[str | None, str | None]:
    """Get the spot label key and value for using spot instances, if supported.

    Checks if the underlying cluster supports spot instances by checking nodes
    for known spot label keys and values. If found, returns the spot label key
    and value. If not, checks if autoscaler is configured and returns
    appropriate labels. If neither are found, returns None.

    Returns:
        Tuple[str, str]: Tuple containing the spot label key and value. Returns
            None if spot instances are not supported.
    """
    # Check if the cluster supports spot instances by checking nodes for known
    # spot label keys and values
    for node in get_kubernetes_nodes(context=context):
        for _, (key, value) in SPOT_LABEL_MAP.items():
            if key in node.metadata.labels and node.metadata.labels[
                    key] == value:
                return key, value

    # Check if autoscaler is configured. Allow spot instances if autoscaler type
    # is known to support spot instances.
    autoscaler_type = get_autoscaler_type(context=context)
    if autoscaler_type == kubernetes_enums.KubernetesAutoscalerType.GKE:
        return SPOT_LABEL_MAP[autoscaler_type.value]

    return None, None


def dict_to_k8s_object(object_dict: dict[str, Any], object_type: 'str') -> Any:
    """Converts a dictionary to a Kubernetes object.

    Useful for comparing two Kubernetes objects. Adapted from
    https://github.com/kubernetes-client/python/issues/977#issuecomment-592030030  # pylint: disable=line-too-long

    Args:
        object_dict: Dictionary representing the Kubernetes object
        object_type: Type of the Kubernetes object. E.g., 'V1Pod', 'V1Service'.
    """

    class FakeKubeResponse:

        def __init__(self, obj):
            self.data = json.dumps(obj)

    fake_kube_response = FakeKubeResponse(object_dict)
    return kubernetes.api_client().deserialize(fake_kube_response, object_type)


def get_unlabeled_accelerator_nodes(context: str | None = None) -> list[Any]:
    """Gets a list of unlabeled GPU nodes in the cluster.

    This function returns a list of nodes that have GPU resources but no label
    that indicates the accelerator type.

    Args:
        context: The context to check.

    Returns:
        List[Any]: List of unlabeled nodes with accelerators.
    """
    nodes = get_kubernetes_nodes(context=context)
    nodes_with_accelerator = []
    for node in nodes:
        if get_gpu_resource_key(context) in node.status.capacity:
            nodes_with_accelerator.append(node)

    label_formatter, _ = detect_gpu_label_formatter(context)
    if not label_formatter:
        return nodes_with_accelerator
    else:
        label_keys = label_formatter.get_label_keys()

    unlabeled_nodes = []
    for node in nodes_with_accelerator:
        labeled = False
        for label_key in label_keys:
            if label_key in node.metadata.labels:
                labeled = True
                break
        if not labeled:
            unlabeled_nodes.append(node)

    return unlabeled_nodes


def get_handled_taint_keys() -> list[str]:
    """Get the taint keys that will be handled automatically by SkyPilot."""
    keys = [TPU_RESOURCE_KEY, *SUPPORTED_GPU_RESOURCE_KEYS.values()]
    custom_key = os.getenv('CUSTOM_GPU_RESOURCE_KEY', None)
    if custom_key:
        keys.append(custom_key)
    return keys


# Taint key prefixes that indicate node roles rather than problems.
# These are excluded when determining if a node has problematic taints.
_ROLE_TAINT_KEY_PREFIXES = [
    'node-role.kubernetes.io/master',
    'node-role.kubernetes.io/control-plane',
]


def get_kubernetes_node_info(
        context: str | None = None) -> models.KubernetesNodesInfo:
    """Gets the resource information for all the nodes in the cluster.

    This function returns a model with node info map as a nested field. This
    allows future extensions while keeping the client-server compatibility,
    e.g. when adding a new field to the model, the legacy clients will not be
    affected and new clients can opt-in new behavior if the new field is
    presented.

    Currently only GPU resources are supported. The function returns the total
    number of GPUs available on the node and the number of free GPUs on the
    node.

    If the user does not have sufficient permissions to list pods in all
    namespaces, the function will return free GPUs as -1.

    Returns:
        KubernetesNodesInfo: A model that contains the node info map and other
            information.
    """
    # Try external node info source first (e.g., node-info-service cache).
    # This allows plugins to provide cached node info for faster queries.
    if plugin_extensions.NodeInfoSource.is_registered():
        # Resolve context before calling the provider so it can be cached
        resolved_context = (context if context is not None else
                            get_current_kube_config_context_name())
        if resolved_context is not None:
            result = plugin_extensions.NodeInfoSource.get(resolved_context)
            if result is not None:
                logger.debug(f'Got node info from external provider for '
                             f'{resolved_context}')
                # Apply allowed_nodes filtering to plugin results. The
                # plugin returns info for all nodes, but we need to
                # respect the user's allowed_nodes config. Use
                # get_kubernetes_nodes() (which is already filtered) to
                # determine the set of allowed node names.
                # TODO(cooperc): Move this filtering into the plugin's
                # NodeInfoSource so it can filter server-side and avoid
                # the extra list_node API call here.
                allowed_config = get_allowed_nodes_config(resolved_context)
                if allowed_config is not None:
                    allowed_nodes = get_kubernetes_nodes(
                        context=resolved_context)
                    allowed_names = {n.metadata.name for n in allowed_nodes}
                    result = models.KubernetesNodesInfo(
                        node_info_dict={
                            name: info
                            for name, info in result.node_info_dict.items()
                            if name in allowed_names
                        },
                        hint=result.hint,
                    )
                return result
        # Fall through to direct Kubernetes API query if provider returns None

    # Resolve `context=None` to the current kubeconfig context BEFORE
    # reading tolerations — otherwise `get_kubernetes_nodes` and
    # `get_configured_tolerations` see different contexts (the former
    # resolves None internally; the latter would skip `context_configs`
    # overrides and miss per-context tolerations the user configured for
    # the current context).
    if context is None:
        context = get_current_kube_config_context_name()
    nodes = get_kubernetes_nodes(context=context)
    configured_tolerations = get_configured_tolerations(context)

    lf, _ = detect_gpu_label_formatter(context)
    if not lf:
        label_keys = []
    else:
        label_keys = lf.get_label_keys()

    # Check if all nodes have no accelerators to avoid fetching pods
    has_accelerator_nodes = False
    for node in nodes:
        accelerator_count = get_node_accelerator_count(context,
                                                       node.status.allocatable)
        if accelerator_count > 0:
            has_accelerator_nodes = True
            break

    # Get the allocated resources (GPU, CPU, memory) by each node in a single call
    allocated_qty_by_node: dict[str, int] = collections.defaultdict(int)
    allocated_cpu_memory_by_node: dict[str, tuple[float, float]] = {}
    error_on_get_allocated_resources = False
    # Get resource allocation. For GPU allocation, only call if there are GPU nodes
    # (same as master branch). For CPU/memory, we always need it for all nodes.
    if has_accelerator_nodes:
        # When there are GPU nodes, get both GPU and CPU/memory in one call
        try:
            allocated_qty_by_node, allocated_cpu_memory_by_node = get_allocated_resources_by_node(
                context=context)
        except kubernetes.api_exception() as e:
            if e.status == 403:
                error_on_get_allocated_resources = True
                pass
            else:
                raise
    else:
        # When there are no GPU nodes, we still need CPU/memory allocation
        # This is an extra API call compared to master branch
        try:
            _, allocated_cpu_memory_by_node = get_allocated_resources_by_node(
                context=context)
        except kubernetes.api_exception() as e:
            if e.status == 403:
                error_on_get_allocated_resources = True
                pass
            else:
                raise

    node_info_dict: dict[str, models.KubernetesNodeInfo] = {}
    has_multi_host_tpu = False

    for node in nodes:
        accelerator_name = None
        # Determine the accelerator name from the node labels and pick the
        # first one found. We assume that the node has only one accelerator type
        # (e.g., either GPU or TPU).
        for label_key in label_keys:
            if lf is not None and label_key in node.metadata.labels:
                accelerator_name = lf.get_accelerator_from_label_value(
                    node.metadata.labels.get(label_key))
                break

        # Extract IP address from node addresses (prefer external, fallback to internal)
        node_ip = None
        if node.status.addresses:
            # First try to find external IP
            for address in node.status.addresses:
                if address.type == 'ExternalIP':
                    node_ip = address.address
                    break
            # If no external IP, try to find internal IP
            if node_ip is None:
                for address in node.status.addresses:
                    if address.type == 'InternalIP':
                        node_ip = address.address
                        break

        accelerator_count = get_node_accelerator_count(context,
                                                       node.status.allocatable)

        # Parse CPU and memory from node capacity
        cpu_count = None
        memory_gb = None
        try:
            if 'cpu' in node.status.capacity:
                cpu_count = float(
                    parse_cpu_or_gpu_resource(node.status.capacity['cpu']))
            if 'memory' in node.status.capacity:
                memory_gb = parse_memory_resource(
                    node.status.capacity['memory'], unit='G')
        except (KeyError, ValueError) as e:
            # If parsing fails, log but continue
            logger.debug(f'Failed to parse CPU/memory for node '
                         f'{node.metadata.name}: {e}')

        # Calculate free CPU and memory
        cpu_free = None
        memory_free_gb = None
        if cpu_count is not None or memory_gb is not None:
            if not error_on_get_allocated_resources:
                allocated_cpu, allocated_memory = allocated_cpu_memory_by_node.get(
                    node.metadata.name, (0.0, 0.0))
                if cpu_count is not None:
                    cpu_free = max(0.0, cpu_count - allocated_cpu)
                if memory_gb is not None:
                    memory_free_gb = max(0.0, memory_gb - allocated_memory)
            # If we can't get allocation info, set free to None (unknown)

        # Check if node is ready
        node_is_ready = node.is_ready()
        node_taints = node.get_taints(
            exclude_cordon=True,
            exclude_not_ready=True,
            exclude_effects=['PreferNoSchedule'],
            exclude_keys=get_handled_taint_keys(),
            exclude_key_prefixes=_ROLE_TAINT_KEY_PREFIXES,
            tolerations=configured_tolerations)
        # A node is "tainted" (un-schedulable from a taint perspective) only if
        # it has at least one taint not tolerated by the configured pod
        # tolerations. Without configured tolerations, every retained taint has
        # `tolerated=False` so this is equivalent to `len(node_taints) > 0`.
        node_is_tainted = has_untolerated_taint(node_taints)

        if accelerator_count == 0:
            node_info_dict[node.metadata.name] = models.KubernetesNodeInfo(
                name=node.metadata.name,
                accelerator_type=accelerator_name,
                total={'accelerator_count': 0},
                free={'accelerators_available': 0},
                ip_address=node_ip,
                cpu_count=cpu_count,
                memory_gb=memory_gb,
                cpu_free=cpu_free,
                memory_free_gb=memory_free_gb,
                is_ready=node_is_ready,
                is_cordoned=node.is_cordoned(),
                taints=node_taints,
            )
            continue

        if not node_is_ready or node.is_cordoned() or node_is_tainted:
            # If node is not ready, cordoned, or tainted, report 0 available GPUs
            accelerators_available = 0
        elif not has_accelerator_nodes or error_on_get_allocated_resources:
            accelerators_available = -1
        else:
            allocated_qty = allocated_qty_by_node[node.metadata.name]
            accelerators_available = accelerator_count - allocated_qty

        # Exclude multi-host TPUs from being processed.
        # TODO(Doyoung): Remove the logic when adding support for
        # multi-host TPUs.
        if is_multi_host_tpu(node.metadata.labels):
            has_multi_host_tpu = True
            continue

        node_info_dict[node.metadata.name] = models.KubernetesNodeInfo(
            name=node.metadata.name,
            accelerator_type=accelerator_name,
            total={'accelerator_count': int(accelerator_count)},
            free={'accelerators_available': int(accelerators_available)},
            ip_address=node_ip,
            cpu_count=cpu_count,
            memory_gb=memory_gb,
            cpu_free=cpu_free,
            memory_free_gb=memory_free_gb,
            is_ready=node_is_ready,
            is_cordoned=node.is_cordoned(),
            taints=node_taints,
        )
    hint = ''
    if has_multi_host_tpu:
        hint = ('(Note: Multi-host TPUs are detected and excluded from the '
                'display as multi-host TPUs are not supported.)')

    return models.KubernetesNodesInfo(
        node_info_dict=node_info_dict,
        hint=hint,
    )


def to_label_selector(tags):
    label_selector = ''
    for k, v in tags.items():
        if label_selector != '':
            label_selector += ','
        label_selector += f'{k}={v}'
    return label_selector


def get_namespace_from_config(provider_config: dict[str, Any]) -> str:
    context = get_context_from_config(provider_config)
    return provider_config.get('namespace',
                               get_kube_config_context_namespace(context))


@timeline.event
def filter_pods(namespace: str,
                context: str | None,
                tag_filters: dict[str, str],
                status_filters: list[str] | None = None) -> dict[str, Any]:
    """Filters pods by tags and status.

    Returned dict is sorted by name, with workers sorted by their numeric suffix.
    This ensures consistent ordering for SSH configuration and other operations.
    """
    non_included_pod_statuses = POD_STATUSES.copy()

    field_selector = ''
    if status_filters is not None:
        non_included_pod_statuses -= set(status_filters)
        field_selector = ','.join(
            [f'status.phase!={status}' for status in non_included_pod_statuses])

    label_selector = to_label_selector(tag_filters)
    pod_list = kubernetes.core_api(context).list_namespaced_pod(
        namespace, field_selector=field_selector, label_selector=label_selector)

    # Don't return pods marked for deletion,
    # i.e. pods with non-null metadata.DeletionTimestamp.
    pods = [
        pod for pod in pod_list.items if pod.metadata.deletion_timestamp is None
    ]

    # Sort pods by name, with workers sorted by their numeric suffix.
    # This ensures consistent ordering (e.g., cluster-head, cluster-worker1,
    # cluster-worker2, cluster-worker3, ...) even when Kubernetes API
    # returns them in arbitrary order. This works even if there were
    # somehow pod names other than head/worker ones, and those end up at
    # the end of the list.
    def get_pod_sort_key(
        pod: V1Pod
    ) -> tuple[Literal[0], str] | tuple[Literal[1], int] | tuple[Literal[2],
                                                                 str]:
        name = pod.metadata.name
        name_suffix = name.split('-')[-1]
        if name_suffix == 'head':
            return (0, name)
        elif name_suffix.startswith('worker'):
            try:
                return (1, int(name_suffix.split('worker')[-1]))
            except (ValueError, IndexError):
                return (2, name)
        else:
            return (2, name)

    sorted_pods = sorted(pods, key=get_pod_sort_key)

    return {pod.metadata.name: pod for pod in sorted_pods}


def _remove_pod_annotation(pod: Any,
                           annotation_key: str,
                           namespace: str,
                           context: str | None = None) -> None:
    """Removes specified Annotations from a Kubernetes pod."""
    try:
        # Remove the specified annotation
        if pod.metadata.annotations:
            if annotation_key in pod.metadata.annotations:
                # Patch the pod with the updated metadata.
                body = {'metadata': {'annotations': {annotation_key: None}}}
                kubernetes.core_api(context).patch_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=namespace,
                    body=body,
                    _request_timeout=kubernetes.API_TIMEOUT)

    except kubernetes.api_exception() as e:
        if e.status == 404:
            logger.warning(
                ANNOTATIONS_POD_NOT_FOUND_ERROR_MSG.format(
                    pod_name=pod.metadata.name,
                    namespace=namespace,
                    action='remove',
                    annotation=annotation_key))
        else:
            with ux_utils.print_exception_no_traceback():
                raise


def _add_pod_annotation(pod: Any,
                        annotation: dict[str, str],
                        namespace: str,
                        context: str | None = None) -> None:
    """Adds specified Annotations on a Kubernetes pod."""
    try:
        # Patch the pod with the updated metadata
        body = {'metadata': {'annotations': annotation}}
        kubernetes.core_api(context).patch_namespaced_pod(
            name=pod.metadata.name,
            namespace=namespace,
            body=body,
            _request_timeout=kubernetes.API_TIMEOUT)

    except kubernetes.api_exception() as e:
        if e.status == 404:
            logger.warning(
                ANNOTATIONS_POD_NOT_FOUND_ERROR_MSG.format(
                    pod_name=pod.metadata.name,
                    namespace=namespace,
                    action='add',
                    annotation=annotation))
        else:
            with ux_utils.print_exception_no_traceback():
                raise


def set_autodown_annotations(handle: 'backends.CloudVmRayResourceHandle',
                             idle_minutes_to_autostop: int | None,
                             down: bool = False) -> None:
    """Adds or removes Annotations of autodown on Kubernetes pods."""
    tags = {
        provision_constants.TAG_RAY_CLUSTER_NAME: handle.cluster_name_on_cloud,
    }
    ray_config = global_user_state.get_cluster_yaml_dict(handle.cluster_yaml)
    provider_config = ray_config['provider']
    namespace = get_namespace_from_config(provider_config)
    context = get_context_from_config(provider_config)
    running_pods = filter_pods(namespace, context, tags)

    for _, pod in running_pods.items():
        if down:
            idle_minutes_to_autostop_annotation = {
                IDLE_MINUTES_TO_AUTOSTOP_ANNOTATION_KEY:
                    str(idle_minutes_to_autostop)
            }
            autodown_annotation = {AUTODOWN_ANNOTATION_KEY: 'true'}
            _add_pod_annotation(pod=pod,
                                annotation=idle_minutes_to_autostop_annotation,
                                namespace=namespace,
                                context=context)
            _add_pod_annotation(pod=pod,
                                annotation=autodown_annotation,
                                namespace=namespace,
                                context=context)

        # If idle_minutes_to_autostop is negative, it indicates a request to
        # cancel autostop using the --cancel flag with the `sky autostop`
        # command.
        elif (idle_minutes_to_autostop is not None and
              idle_minutes_to_autostop < 0):
            _remove_pod_annotation(
                pod=pod,
                annotation_key=IDLE_MINUTES_TO_AUTOSTOP_ANNOTATION_KEY,
                namespace=namespace,
                context=context)
            _remove_pod_annotation(pod=pod,
                                   annotation_key=AUTODOWN_ANNOTATION_KEY,
                                   namespace=namespace,
                                   context=context)


def get_context_from_config(provider_config: dict[str, Any]) -> str | None:
    context = provider_config.get('context')
    assert isinstance(context, str)
    if context == kubernetes.in_cluster_context_name():
        # If the context (also used as the region) is in-cluster, we need
        # to use in-cluster auth by setting the context to None.
        context = None
    return context


def get_skypilot_pods(context: str | None = None) -> list[Any]:
    """Gets all SkyPilot pods in the Kubernetes cluster.

    Args:
        context: Kubernetes context to use. If None, uses the current context.

    Returns:
        A list of Kubernetes pod objects.
    """
    if context is None:
        context = get_current_kube_config_context_name()

    # Try external pod info source first (e.g., node-info-service cache).
    if plugin_extensions.PodInfoSource.is_registered():
        if context is not None:
            result = plugin_extensions.PodInfoSource.get(context)
            if result is not None:
                logger.debug(f'Got pod info from external provider for '
                             f'{context}')
                return result
        # Fall through to direct Kubernetes API query if provider returns None

    try:
        pods = kubernetes.core_api(context).list_pod_for_all_namespaces(
            label_selector=provision_constants.TAG_SKYPILOT_CLUSTER_NAME,
            _request_timeout=kubernetes.API_TIMEOUT).items
    except kubernetes.max_retry_error():
        raise exceptions.ResourcesUnavailableError(
            'Timed out trying to get SkyPilot pods from Kubernetes cluster. '
            'Please check if the cluster is healthy and retry. To debug, run: '
            'kubectl get pods --selector=skypilot-cluster-name --all-namespaces'
        ) from None
    return pods


def is_tpu_on_gke(accelerator: str, normalize: bool = True) -> bool:
    """Determines if the given accelerator is a TPU supported on GKE."""
    if normalize:
        normalized, _ = normalize_tpu_accelerator_name(accelerator)
        return normalized in GKE_TPU_ACCELERATOR_TO_GENERATION
    return accelerator in GKE_TPU_ACCELERATOR_TO_GENERATION


def get_node_accelerator_count(context: str | None,
                               attribute_dict: dict) -> int:
    """Retrieves the count of accelerators from a node's resource dictionary.

    This method checks the node's allocatable resources or the accelerators
    already deployed on the node, using pod objects that describe resource
    requests.

    Args:
        attribute_dict: Containing resource information from a node, such as
            allocatable or requested resources.

    Returns:
        Number of accelerators allocated or available from the node. If no
            resource is found, it returns 0.
    """
    gpu_resource_name = get_gpu_resource_key(context)
    assert not (gpu_resource_name in attribute_dict and
                TPU_RESOURCE_KEY in attribute_dict)
    if gpu_resource_name in attribute_dict:
        return int(attribute_dict[gpu_resource_name])
    elif TPU_RESOURCE_KEY in attribute_dict:
        return int(attribute_dict[TPU_RESOURCE_KEY])
    return 0


def reduce_tpu_topology(topology: str) -> int:
    """Computes the number of TPU chips from its topology string."""
    chip_dimensions = [int(chip_count) for chip_count in topology.split('x')]
    # tpu_topology_chip_count represents the total number of TPU chips in the
    # entire podslice, whether it is a single-host or multi-host TPU podslice.
    tpu_topology_chip_count = functools.reduce(lambda x, y: x * y,
                                               chip_dimensions)
    return tpu_topology_chip_count


def is_multi_host_tpu(node_metadata_labels: dict) -> bool:
    """Determines whether the given node is a multi-host TPU configuration."""
    if GKELabelFormatter.TPU_LABEL_KEY in node_metadata_labels:
        assert GKELabelFormatter.TPU_TOPOLOGY_LABEL_KEY in node_metadata_labels
        topology_value = (
            node_metadata_labels[GKELabelFormatter.TPU_TOPOLOGY_LABEL_KEY])
        accelerator_count_label_key = (
            GKELabelFormatter.ACCELERATOR_COUNT_LABEL_KEY)
        assert accelerator_count_label_key in node_metadata_labels
        # node_tpu_chip_count represents the number of TPU chips
        # available in this node. If the node is part of a node pool
        # forming a multi-host TPU podslice, it only reflects the
        # number of TPU chips in this individual node, not the entire
        # multi-host TPU podslice.
        node_tpu_chip_count = int(
            node_metadata_labels[accelerator_count_label_key])
        topology_chip_count = reduce_tpu_topology(topology_value)
        # For multi-host TPU podslices, topology_chip_count and
        # node_tpu_chip_count will differ, as topology_chip_count
        # reflects the total across all hosts, while
        # node_tpu_chip_count reflects only the chips in a single node.
        if node_tpu_chip_count != topology_chip_count:
            return True
    return False


@dataclasses.dataclass
class KubernetesSkyPilotClusterInfo:
    cluster_name_on_cloud: str
    cluster_name: str
    user: str
    status: status_lib.ClusterStatus
    pods: list[Any]
    launched_at: float
    resources: 'resources_lib.Resources'
    resources_str: str


@dataclasses.dataclass
class KubernetesSkyPilotClusterInfoPayload:
    """SkyPilot Cluster on Kubernetes payload."""
    cluster_name_on_cloud: str
    cluster_name: str
    user: str
    status: status_lib.ClusterStatus
    resources_str: str
    launched_at: float

    @classmethod
    def from_cluster(
        cls, cluster: KubernetesSkyPilotClusterInfo
    ) -> 'KubernetesSkyPilotClusterInfoPayload':
        resources_str = f'{len(cluster.pods)}x {cluster.resources}'
        return cls(
            cluster_name_on_cloud=cluster.cluster_name_on_cloud,
            cluster_name=cluster.cluster_name,
            user=cluster.user,
            status=cluster.status,
            resources_str=resources_str,
            launched_at=cluster.launched_at,
        )


def get_pod_primary_container(
    pod: Any,
    *,
    primary_name: str = kubernetes_constants.RAY_NODE_CONTAINER_NAME,
):
    """Return the primary workload container for a SkyPilot pod.

    Pods may include sidecars (e.g., log shippers). Kubernetes preserves the
    ordering of the `containers` list as authored, but mutating webhooks can
    inject additional containers. Callers should not rely on containers[0].
    """
    spec = getattr(pod, 'spec', None)
    containers = getattr(spec, 'containers', None) if spec is not None else None
    if not containers:
        pod_name = getattr(getattr(pod, 'metadata', None), 'name', '<unknown>')
        raise ValueError(f'Pod {pod_name!r} has no containers.')
    for container in containers:
        if getattr(container, 'name', None) == primary_name:
            return container
    return containers[0]


def process_skypilot_pods(
    pods: list[Any],
    context: str | None = None
) -> tuple[list[KubernetesSkyPilotClusterInfo],
           list[KubernetesSkyPilotClusterInfo],
           list[KubernetesSkyPilotClusterInfo]]:
    """Process SkyPilot pods on k8s to extract cluster and controller info.

    Args:
        pods: List of Kubernetes pod objects.
        context: Kubernetes context name, used to detect GPU label formatter.

    Returns:
        A tuple containing:
        - List of KubernetesSkyPilotClusterInfo with all cluster info.
        - List of KubernetesSkyPilotClusterInfo with job controller info.
        - List of KubernetesSkyPilotClusterInfo with serve controller info.
    """
    # pylint: disable=import-outside-toplevel
    from sky import resources as resources_lib
    clusters: dict[str, KubernetesSkyPilotClusterInfo] = {}
    jobs_controllers: list[KubernetesSkyPilotClusterInfo] = []
    serve_controllers: list[KubernetesSkyPilotClusterInfo] = []

    for pod in pods:
        cluster_name_on_cloud = pod.metadata.labels.get(
            provision_constants.TAG_SKYPILOT_CLUSTER_NAME)
        cluster_name = cluster_name_on_cloud.rsplit(
            '-', 1
        )[0]  # Remove the user hash to get cluster name (e.g., mycluster-2ea4)
        if cluster_name_on_cloud not in clusters:
            # Parse the start time for the cluster
            start_time = pod.status.start_time
            if start_time is not None:
                start_time = pod.status.start_time.timestamp()

            # Parse resources
            primary_container = get_pod_primary_container(pod)
            resources = getattr(primary_container, 'resources', None)
            requests = getattr(resources, 'requests',
                               None) if resources else None
            cpu_request = parse_cpu_or_gpu_resource(
                requests.get('cpu', '0') if requests is not None else '0')
            memory_request = parse_memory_resource(
                (requests.get('memory', '0') if requests is not None else '0'),
                unit='G')
            gpu_count = parse_cpu_or_gpu_resource(
                requests.get(get_gpu_resource_key(context), '0'
                            ) if requests is not None else '0')
            gpu_name = None
            if gpu_count > 0:
                label_formatter, _ = (detect_gpu_label_formatter(context))
                assert label_formatter is not None, (
                    'GPU label formatter cannot be None if there are pods '
                    f'requesting GPUs: {pod.metadata.name}')
                gpu_label = label_formatter.get_label_key()
                # Get GPU name from pod node selector
                node_selector_terms = (
                    pod.spec.affinity.node_affinity.
                    required_during_scheduling_ignored_during_execution.
                    node_selector_terms)
                if node_selector_terms is not None:
                    expressions = []
                    for term in node_selector_terms:
                        if term.match_expressions:
                            expressions.extend(term.match_expressions)
                    for expression in expressions:
                        if expression.key == gpu_label and expression.operator == 'In':
                            gpu_name = label_formatter.get_accelerator_from_label_value(
                                expression.values[0])
                            break

            resources = resources_lib.Resources(
                cloud=clouds.Kubernetes(),
                cpus=int(cpu_request),
                memory=int(memory_request),
                accelerators=(f'{gpu_name}:{gpu_count}'
                              if gpu_count > 0 else None))
            if pod.status.phase == 'Pending':
                # If pod is pending, do not show it in the status
                continue

            cluster_info = KubernetesSkyPilotClusterInfo(
                cluster_name_on_cloud=cluster_name_on_cloud,
                cluster_name=cluster_name,
                user=pod.metadata.labels.get('skypilot-user'),
                status=status_lib.ClusterStatus.UP,
                pods=[],
                launched_at=start_time,
                resources=resources,
                resources_str='')
            clusters[cluster_name_on_cloud] = cluster_info
            # Check if cluster name is name of a controller
            # Can't use controller_utils.Controllers.from_name(cluster_name)
            # because hash is different across users
            if 'sky-jobs-controller' in cluster_name_on_cloud:
                jobs_controllers.append(cluster_info)
            elif 'sky-serve-controller' in cluster_name_on_cloud:
                serve_controllers.append(cluster_info)
        else:
            # Update start_time if this pod started earlier
            pod_start_time = pod.status.start_time
            if pod_start_time is not None:
                pod_start_time = pod_start_time.timestamp()
                if pod_start_time < clusters[cluster_name_on_cloud].launched_at:
                    clusters[cluster_name_on_cloud].launched_at = pod_start_time
        clusters[cluster_name_on_cloud].pods.append(pod)
    # Update resources_str in clusters:
    for cluster in clusters.values():
        num_pods = len(cluster.pods)
        cluster.resources_str = f'{num_pods}x {cluster.resources}'
    return list(clusters.values()), jobs_controllers, serve_controllers


def _gpu_resource_key_helper(context: str | None) -> str:
    """Helper function to get the GPU resource key."""
    gpu_resource_key = SUPPORTED_GPU_RESOURCE_KEYS['nvidia']
    try:
        response = kubernetes.core_api(context).list_node(
            _request_timeout=kubernetes.API_TIMEOUT, _preload_content=False)
        try:
            supported_gpu_keys = set(SUPPORTED_GPU_RESOURCE_KEYS.values())
            capacity_keys: set[str] = set()
            for capacity in ijson.items(response,
                                        'items.item.status.capacity',
                                        buf_size=IJSON_BUFFER_SIZE):
                capacity_keys.update(
                    supported_gpu_keys.intersection(capacity.keys()))
                if len(capacity_keys) == len(supported_gpu_keys):
                    break
            for gpu_key in SUPPORTED_GPU_RESOURCE_KEYS.values():
                if gpu_key in capacity_keys:
                    return gpu_key
        finally:
            response.release_conn()
    except Exception as e:  # pylint: disable=broad-except
        logger.warning(f'Failed to load kube config or query nodes: {e}. '
                       'Falling back to default GPU resource key.')
    return gpu_resource_key


@annotations.lru_cache(scope='request')
def get_gpu_resource_key(context: str | None = None) -> str:
    """Get the GPU resource name to use in Kubernetes.

    The function auto-detects the GPU resource key by querying the Kubernetes node API.
    If detection fails, it falls back to a default value.
    An environment variable can override the detected or default value.

    Returns:
        str: The selected GPU resource name.
    """
    gpu_resource_key = _gpu_resource_key_helper(context)
    return os.getenv('CUSTOM_GPU_RESOURCE_KEY', default=gpu_resource_key)


def get_kubeconfig_paths() -> list[str]:
    """Get the path to the kubeconfig files."""
    return context_utils.get_kubeconfig_paths()


def format_kubeconfig_exec_auth(config: Any,
                                output_path: str,
                                inject_wrapper: bool = True) -> bool:
    """Reformat the kubeconfig so that exec-based authentication can be used
    with SkyPilot. Will create a new kubeconfig file under <output_path>
    regardless of whether a change has been made.

    kubectl internally strips all environment variables except for system
    defaults. If `inject_wrapper` is true, a wrapper executable is applied
    to inject the relevant PATH information before exec-auth is executed.

    Contents of sky-kube-exec-wrapper:

    #!/bin/bash
    export PATH="$HOME/skypilot-runtime/bin:$HOME/google-cloud-sdk:$PATH"
    exec "$@"

    refer to `skylet/constants.py` for more information.

    Args:
        config (dict): kubeconfig parsed by yaml.safe_load
        output_path (str): Path where the potentially modified kubeconfig file
          will be saved
        inject_wrapper (bool): Whether to inject the wrapper script
    Returns: whether config was updated, for logging purposes
    """
    updated = False
    for user in config.get('users', []):
        exec_info = user.get('user', {}).get('exec', {})
        current_command = exec_info.get('command', '')

        if current_command:
            # Strip the path and keep only the executable name
            executable = os.path.basename(current_command)
            if executable == kubernetes_constants.SKY_K8S_EXEC_AUTH_WRAPPER:
                # we don't want this happening recursively.
                continue

            if inject_wrapper:
                exec_info[
                    'command'] = kubernetes_constants.SKY_K8S_EXEC_AUTH_WRAPPER
                if exec_info.get('args') is None:
                    exec_info['args'] = []
                exec_info['args'].insert(0, executable)
                updated = True
            elif executable != current_command:
                exec_info['command'] = executable
                updated = True

            # Handle Nebius kubeconfigs: change --profile to 'sky'
            if executable == 'nebius':
                args = exec_info.get('args', [])
                if args and '--profile' in args:
                    try:
                        profile_index = args.index('--profile')
                        if profile_index + 1 < len(args):
                            old_profile = args[profile_index + 1]
                            if old_profile != 'sky':
                                args[profile_index + 1] = 'sky'
                                updated = True
                    except ValueError:
                        pass

    os.makedirs(os.path.dirname(os.path.expanduser(output_path)), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as file:
        yaml.safe_dump(config, file)

    return updated


def format_kubeconfig_exec_auth_with_cache(kubeconfig_path: str) -> str:
    """Reformat the kubeconfig file or retrieve it from cache if it has already
    been formatted before. Store it in the cache directory if necessary.

    Having a cache for this is good if users spawn an extreme number of jobs
    concurrently.

    Args:
        kubeconfig_path (str): kubeconfig path
    Returns: updated kubeconfig path
    """
    # TODO(kyuds): GC cache files
    with open(kubeconfig_path, encoding='utf-8') as file:
        config = yaml_utils.safe_load(file)
    normalized = yaml.dump(config, sort_keys=True)
    hashed = hashlib.sha1(normalized.encode('utf-8'),
                          usedforsecurity=False).hexdigest()
    path = os.path.expanduser(
        f'{kubernetes_constants.SKY_K8S_EXEC_AUTH_KUBECONFIG_CACHE}/{hashed}.yaml'
    )

    # If we have already converted the same kubeconfig before, just return.
    if os.path.isfile(path):
        return path

    try:
        format_kubeconfig_exec_auth(config, path)
        return path
    except Exception as e:  # pylint: disable=broad-except
        # There may be problems with kubeconfig, but the user is not actually
        # using Kubernetes (or SSH Node Pools)
        logger.warning(
            f'Failed to format kubeconfig at {kubeconfig_path}. '
            'Please check if the kubeconfig is valid. This may cause '
            'problems when Kubernetes infra is used. '
            f'Reason: {common_utils.format_exception(e)}')
        return kubeconfig_path


def delete_k8s_resource_with_retry(delete_func: Callable, resource_type: str,
                                   resource_name: str) -> None:
    """Helper to delete Kubernetes resources with 404 handling and retries.

    Args:
        delete_func: Function to call to delete the resource
        resource_type: Type of resource being deleted (e.g. 'service'),
            used in logging
        resource_name: Name of the resource being deleted, used in logging
    """
    max_retries = 3
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            delete_func()
            return
        except kubernetes.api_exception() as e:
            if e.status == 404:
                logger.warning(
                    f'terminate_instances: Tried to delete {resource_type} '
                    f'{resource_name}, but the {resource_type} was not '
                    'found (404).')
                return
            elif attempt < max_retries - 1:
                logger.warning(f'terminate_instances: Failed to delete '
                               f'{resource_type} {resource_name} (attempt '
                               f'{attempt + 1}/{max_retries}). Error: {e}. '
                               f'Retrying in {retry_delay} seconds...')
                time.sleep(retry_delay)
            else:
                raise


def should_exclude_pod_from_gpu_allocation(pod) -> bool:
    """Check if a pod should be excluded from GPU count calculations.

    Some cloud providers run low priority test/verification pods that request
    GPUs but should not count against real GPU availability since they are
    designed to be evicted when higher priority workloads need resources.

    Args:
        pod: Kubernetes pod object

    Returns:
        bool: True if the pod should be excluded from GPU count calculations.
    """
    # CoreWeave HPC verification pods - identified by namespace
    if (hasattr(pod.metadata, 'namespace') and
            pod.metadata.namespace == 'cw-hpc-verification'):
        return True

    return False


def get_pvc_events(context: str | None,
                   namespace: str,
                   pvc_name: str,
                   reverse: bool = True) -> list[Any]:
    """Get the events for a PVC, sorted by creation_timestamp."""
    try:
        pvc_events = kubernetes.core_api(context).list_namespaced_event(
            namespace,
            field_selector=(f'involvedObject.name={pvc_name},'
                            'involvedObject.kind=PersistentVolumeClaim'),
            _request_timeout=kubernetes.API_TIMEOUT)
    except (kubernetes.max_retry_error(), kubernetes.api_exception(),
            kubernetes.config_exception()) as e:
        logger.warning(f'Failed to get PVC events: {e}')
        return []

    return sorted(pvc_events.items,
                  key=lambda e:
                  (e.last_timestamp or e.metadata.creation_timestamp),
                  reverse=reverse)
