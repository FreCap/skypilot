"""Monitoring bootstrap for SSH node pool deployments."""

import textwrap

import colorama

from sky import sky_logging
from sky.ssh_node_pools.deploy import utils as deploy_utils
from sky.utils import rich_utils
from sky.utils import ux_utils

RESET_ALL = colorama.Style.RESET_ALL

logger = sky_logging.init_logger(__name__)


def success_message(message: str) -> None:
    logger.info(f'{colorama.Fore.GREEN}✔ {message}{RESET_ALL}')


def force_update_status(message: str) -> None:
    rich_utils.force_update_status(ux_utils.spinner_message(message))


def install_monitoring(cluster_name: str, head_node: str, ssh_user: str,
                       ssh_key: str, askpass_block: str, install_gpu: bool,
                       head_use_ssh_config: bool) -> None:
    """Install best-effort GPU and Prometheus monitoring on a node pool."""
    # Install GPU operator if a GPU was detected on any node.
    if install_gpu:
        force_update_status(f'Configuring NVIDIA GPUs [{cluster_name}]')
        cmd = f"""
            {askpass_block}
            curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 &&
            chmod 700 get_helm.sh &&
            ./get_helm.sh &&
            helm repo add nvidia https://helm.ngc.nvidia.com/nvidia && helm repo update &&
            kubectl create namespace gpu-operator --kubeconfig ~/.kube/config || true &&
            sudo -A ln -s /sbin/ldconfig /sbin/ldconfig.real || true &&
            helm install gpu-operator -n gpu-operator --create-namespace nvidia/gpu-operator \\
            --set 'toolkit.env[0].name=CONTAINERD_CONFIG' \\
            --set 'toolkit.env[0].value=/var/lib/rancher/k3s/agent/etc/containerd/config.toml' \\
            --set 'toolkit.env[1].name=CONTAINERD_SOCKET' \\
            --set 'toolkit.env[1].value=/run/k3s/containerd/containerd.sock' \\
            --set 'toolkit.env[2].name=CONTAINERD_RUNTIME_CLASS' \\
            --set 'toolkit.env[2].value=nvidia' \\
            --set 'devicePlugin.env[0].name=DP_DISABLE_HEALTHCHECKS' \\
            --set 'devicePlugin.env[0].value=all' &&
            echo 'Waiting for GPU operator installation...' &&
            while ! kubectl describe nodes --kubeconfig ~/.kube/config | grep -q 'nvidia.com/gpu:' || ! kubectl describe nodes --kubeconfig ~/.kube/config | grep -q 'nvidia.com/gpu.product'; do
                echo 'Waiting for GPU operator...'
                sleep 5
            done
            echo 'GPU operator installed successfully.'
        """
        result = deploy_utils.run_remote(head_node,
                                         cmd,
                                         ssh_user,
                                         ssh_key,
                                         use_ssh_config=head_use_ssh_config)
        if result is None:
            logger.error(f'{colorama.Fore.RED}Failed to install GPU Operator.'
                         f'{RESET_ALL}')
        else:
            success_message('GPU Operator installed.')

        # Create a Kubernetes Service for dcgm-exporter with Prometheus
        # scrape annotations. The GPU Operator deploys dcgm-exporter but
        # does not add these annotations by default, so Prometheus cannot
        # discover and scrape GPU metrics without this Service.
        # We dynamically discover the pod labels from the actual
        # dcgm-exporter DaemonSet rather than hardcoding a selector, since
        # the GPU Operator may change labels across versions.
        logger.debug('Setting up Prometheus Service.')

        # Step 1: Get the selector labels from the dcgm-exporter DaemonSet.
        get_selector_cmd = f"""
            {askpass_block}
            for i in $(seq 1 30); do
                DCGM_DS=$(kubectl --kubeconfig ~/.kube/config get daemonset -n gpu-operator -o name 2>/dev/null | grep dcgm-exporter) && break
                echo 'Waiting for dcgm-exporter DaemonSet...' >&2
                sleep 10
            done
            if [ -z "$DCGM_DS" ]; then
                echo 'dcgm-exporter DaemonSet not found.' >&2
                exit 1
            fi
            kubectl --kubeconfig ~/.kube/config get $DCGM_DS -n gpu-operator -o json | \
                python3 -c 'import sys,json; labels=json.load(sys.stdin)["spec"]["selector"]["matchLabels"]; print(chr(10).join(k+": "+v for k,v in labels.items()))'
        """
        selector = deploy_utils.run_remote(head_node,
                                           get_selector_cmd,
                                           ssh_user,
                                           ssh_key,
                                           use_ssh_config=head_use_ssh_config,
                                           print_output=True)

        if selector is None:
            logger.error(
                f'{colorama.Fore.RED}Failed to get dcgm-exporter '
                f'selector labels. Skipping Service creation.{RESET_ALL}')
        else:
            # Step 2: Create the Service with the discovered selector.
            logger.debug(f'Found selector: <{selector}>.')
            create_svc_cmd = _dcgm_exporter_service_cmd(askpass_block, selector)
            svc_result = deploy_utils.run_remote(
                head_node,
                create_svc_cmd,
                ssh_user,
                ssh_key,
                use_ssh_config=head_use_ssh_config,
                print_output=True)
            if svc_result is None:
                logger.error(
                    f'{colorama.Fore.RED}Failed to create dcgm-exporter '
                    f'Service with Prometheus annotations.{RESET_ALL}')
            else:
                success_message('dcgm-exporter Service created with '
                                'Prometheus annotations.')
    else:
        logger.debug('No GPUs detected. Skipping GPU Operator installation.')

    # Install Prometheus + node-exporter in the `skypilot` namespace so the
    # API server's /gpu-metrics endpoint can federate DCGM + node-level
    # metrics. Runs on every pool (GPU or CPU) because node-level metrics
    # are always useful.
    force_update_status(f'Installing Prometheus [{cluster_name}]')
    prom_cmd = _prometheus_install_cmd(askpass_block)
    prom_result = deploy_utils.run_remote(head_node,
                                          prom_cmd,
                                          ssh_user,
                                          ssh_key,
                                          use_ssh_config=head_use_ssh_config,
                                          print_output=True)
    if prom_result is None:
        # Log and continue: the cluster is still usable without Prometheus.
        logger.error(
            f'{colorama.Fore.RED}Failed to install Prometheus. The cluster '
            f'will still work, but /gpu-metrics federation will not find '
            f'metrics until Prometheus is installed manually.{RESET_ALL}')
    else:
        success_message('Prometheus installed with node-exporter enabled.')


def _dcgm_exporter_service_cmd(askpass_block: str, selector: str) -> str:
    """Create a command to apply a dcgm-exporter Service with Prometheus annotations.

    The GPU Operator deploys dcgm-exporter but does not add Prometheus
    scrape annotations by default, so this Service is needed for
    Prometheus to discover and scrape GPU metrics.
    """
    indented_selector = selector.replace('\n', '\n    ')
    svc_yaml = textwrap.dedent(f"""\
        apiVersion: v1
        kind: Service
        metadata:
          name: dcgm-exporter
          namespace: gpu-operator
          labels:
            app: dcgm-exporter
          annotations:
            prometheus.io/scrape: "true"
            prometheus.io/port: "9400"
            prometheus.io/path: "/metrics"
        spec:
          selector:
            {indented_selector}
          ports:
          - name: metrics
            port: 9400
            targetPort: 9400
            protocol: TCP
          type: ClusterIP
    """)
    return f"""{askpass_block}
cat <<'DCGM_SVC' | kubectl --kubeconfig ~/.kube/config apply -f -
{svc_yaml}
DCGM_SVC
"""


def _prometheus_install_cmd(askpass_block: str) -> str:
    """Build the shell command to install prometheus-community/prometheus.

    Installs into the `skypilot` namespace with node-exporter enabled so
    the API server's /gpu-metrics endpoint can federate DCGM + node-level
    metrics. Uses `helm upgrade --install` for idempotency.

    The plain prometheus chart is used deliberately — kube-prometheus-stack
    prefixes pod/namespace labels with `exported_` which breaks SkyPilot's
    PromQL queries.

    The command runs on the pool's head node, where `~/.kube/config` is the
    kubeconfig k3s writes at install time with `current-context` pointing at
    this cluster — so helm does not need `--kube-context`. Helm is installed
    inline if missing (CPU-only pools skip the gpu-operator step that would
    otherwise install it). The helm exit code is captured and propagated
    explicitly so a failure isn't masked by the `rm -f` of the temp file.
    """
    return f"""{askpass_block}
if ! command -v helm &> /dev/null; then
    curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 &&
    chmod 700 get_helm.sh &&
    ./get_helm.sh &&
    rm get_helm.sh
fi
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts || true
helm repo update prometheus-community
PROM_VALUES_FILE=$(mktemp /tmp/skypilot-prom-values.XXXXXX.yaml)
cat <<'PROM_VALUES' > "$PROM_VALUES_FILE"
server:
  persistentVolume:
    enabled: true
    size: 50Gi
  retention: "1000d"
  retentionSize: "43GB"
kube-state-metrics:
  enabled: true
  metricLabelsAllowlist:
    - pods=[skypilot-cluster,skypilot-cluster-name]
prometheus-node-exporter:
  enabled: true
prometheus-pushgateway:
  enabled: false
alertmanager:
  enabled: false
PROM_VALUES
helm upgrade --install skypilot-prometheus \\
    prometheus-community/prometheus \\
    --kubeconfig ~/.kube/config \\
    --namespace skypilot \\
    --create-namespace \\
    -f "$PROM_VALUES_FILE"
HELM_RET=$?
rm -f "$PROM_VALUES_FILE"
exit $HELM_RET
"""
