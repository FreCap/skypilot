"""Constants for Kubernetes provisioning."""

NO_GPU_HELP_MESSAGE = ('If your cluster contains GPUs, make sure '
                       'nvidia.com/gpu resource is available on the nodes and '
                       'the node labels for identifying GPUs '
                       '(e.g., skypilot.co/accelerator) are setup correctly. ')

KUBERNETES_IN_CLUSTER_NAMESPACE_ENV_VAR = 'SKYPILOT_IN_CLUSTER_NAMESPACE'

# Name of kubernetes exec auth wrapper script
SKY_K8S_EXEC_AUTH_WRAPPER = 'sky-kube-exec-wrapper'

# PATH envvar for kubectl exec auth execve
SKY_K8S_EXEC_AUTH_PATH = '$HOME/skypilot-runtime/bin:$HOME/google-cloud-sdk/bin:$PATH'  # pylint: disable=line-too-long

# cache directory for kubeconfig with modified exec auth
SKY_K8S_EXEC_AUTH_KUBECONFIG_CACHE = '~/.sky/generated/kubeconfigs'

# Labels for the Pods created by SkyPilot
TAG_RAY_CLUSTER_NAME = 'ray-cluster-name'
TAG_POD_INITIALIZED = 'skypilot-initialized'
TAG_SKYPILOT_DEPLOYMENT_NAME = 'skypilot-deployment-name'

# Default name of the primary workload container in SkyPilot Ray pods.
RAY_NODE_CONTAINER_NAME = 'ray-node'

# Kubernetes uses this scheduler when PodSpec.schedulerName is omitted.  Serve
# projection protocol v2 persists the effective value explicitly so rendering
# and provider attestation share one immutable scheduling seam.
DEFAULT_SCHEDULER_NAME = 'default-scheduler'

# Kueue plain-Pod protocol metadata.  The finalizer and managed-label key share
# the same qualified name but live in different metadata collections.
KUEUE_MANAGED_KEY = 'kueue.x-k8s.io/managed'
KUEUE_MANAGED_VALUE = 'true'
KUEUE_MANAGED_FINALIZER = KUEUE_MANAGED_KEY
KUEUE_QUEUE_LABEL = 'kueue.x-k8s.io/queue-name'
KUEUE_POD_GROUP_LABEL = 'kueue.x-k8s.io/pod-group-name'
KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL = 'kueue.x-k8s.io/priority-class'
KUEUE_POD_GROUP_TOTAL_COUNT_ANNOTATION = (
    'kueue.x-k8s.io/pod-group-total-count')
KUEUE_RETRIABLE_IN_GROUP_ANNOTATION = 'kueue.x-k8s.io/retriable-in-group'
KUEUE_ADMISSION_SCHEDULING_GATE = 'kueue.x-k8s.io/admission'
KUEUE_TOPOLOGY_SCHEDULING_GATE = 'kueue.x-k8s.io/topology'
KUEUE_METADATA_PREFIX = 'kueue.x-k8s.io/'
KUEUE_CLUSTER_QUEUE_LABEL = 'kueue.x-k8s.io/cluster-queue-name'
KUEUE_LOCAL_QUEUE_LABEL = 'kueue.x-k8s.io/local-queue-name'
KUEUE_PODSET_LABEL = 'kueue.x-k8s.io/podset'
KUEUE_ROLE_HASH_ANNOTATION = 'kueue.x-k8s.io/role-hash'
KUEUE_WORKLOAD_ANNOTATION = 'kueue.x-k8s.io/workload'
KUEUE_API_GROUP = 'kueue.x-k8s.io'
KUEUE_API_VERSIONS = ('v1beta2', 'v1beta1')
KUEUE_LOCAL_QUEUE_PLURAL = 'localqueues'
KUEUE_CLUSTER_QUEUE_PLURAL = 'clusterqueues'
KUEUE_ACTIVE_CONDITION = 'Active'

# Pod phases that are not holding PVCs
PVC_NOT_HOLD_POD_PHASES = ['Succeeded', 'Failed']
