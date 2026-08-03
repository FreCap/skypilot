"""Kubeconfig and context helpers for Kubernetes provisioning."""

from collections.abc import Callable
import hashlib
import os
import subprocess
from typing import Any

from sky import skypilot_config
from sky.adaptors import kubernetes
from sky.provision.kubernetes import constants as kubernetes_constants
from sky.utils import schemas
from sky.utils import yaml_utils


def is_kubeconfig_exec_auth(
    context: str | None,
    *,
    get_kubeconfig_text_fn: Callable[[str | None], str],
) -> tuple[bool, str | None]:
    """Checks if the kubeconfig file uses exec-based authentication."""
    k8s = kubernetes.kubernetes
    if context == kubernetes.in_cluster_context_name():
        return False, None
    try:
        k8s.config.load_kube_config()
    except kubernetes.config_exception():
        return False, None

    all_contexts, current_context = kubernetes.list_kube_config_contexts()
    context_obj = current_context
    if context is not None:
        for candidate in all_contexts:
            if candidate['name'] == context:
                context_obj = candidate
                break
        else:
            raise ValueError(f'Kubernetes context {context!r} not found.')
    target_username = context_obj['context']['user']

    kubeconfig_text = get_kubeconfig_text_fn(context)
    kubeconfig = yaml_utils.safe_load(kubeconfig_text)
    user_details = next(
        user for user in kubeconfig['users'] if user['name'] == target_username)

    remote_identity = skypilot_config.get_effective_workspace_region_config(
        cloud='kubernetes',
        region=context,
        keys=('remote_identity',),
        default_value=schemas.get_default_remote_identity('kubernetes'))
    if ('exec' in user_details.get('user', {}) and remote_identity
            == schemas.RemoteIdentityOptions.LOCAL_CREDENTIALS.value):
        ctx_name = context_obj['name']
        exec_msg = ('exec-based authentication is used for '
                    f'Kubernetes context {ctx_name!r}. '
                    'Make sure that the corresponding cloud provider is '
                    'also enabled through `sky check` (e.g.: GCP for GKE). '
                    'Alternatively, configure SkyPilot to create a service '
                    'account for running pods by setting the following in '
                    '~/.sky/config.yaml:\n'
                    '    kubernetes:\n'
                    '      remote_identity: SERVICE_ACCOUNT\n'
                    '    More: https://docs.skypilot.co/en/latest/'
                    'reference/config.html')
        return True, exec_msg
    return False, None


def get_kubeconfig_text_for_context(context: str | None = None) -> str:
    """Get the kubeconfig text for the given context."""
    command = ['kubectl', 'config', 'view', '--minify']
    if context is not None:
        command.append(f'--context={context}')

    proc = subprocess.run(command,
                          check=False,
                          env=os.environ.copy(),
                          capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f'Failed to get kubeconfig text for context {context}: '
            f'{proc.stderr.decode("utf-8")}')
    return proc.stdout.decode('utf-8')


def get_current_kube_config_context_name(
        *, is_incluster_config_available_fn: Callable[[], bool]) -> str | None:
    """Get the current kubernetes context from the kubeconfig file."""
    k8s = kubernetes.kubernetes
    try:
        _, current_context = kubernetes.list_kube_config_contexts()
        return current_context['name']
    except k8s.config.config_exception.ConfigException:
        if is_incluster_config_available_fn():
            return kubernetes.in_cluster_context_name()
        return None


def is_incluster_config_available() -> bool:
    """Check if in-cluster auth is available."""
    return os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount/token')


def get_all_kube_context_names(
        *, is_incluster_config_available_fn: Callable[[], bool]) -> list[str]:
    """Get all kubernetes context names available in the environment."""
    k8s = kubernetes.kubernetes
    context_names = []
    try:
        all_contexts, _ = kubernetes.list_kube_config_contexts()
        context_names = [context['name'] for context in all_contexts]
    except k8s.config.config_exception.ConfigException:
        pass
    if is_incluster_config_available_fn():
        context_names.append(kubernetes.in_cluster_context_name())
    return context_names


def get_kube_config_context_namespace(
    context_name: str | None = None,
    *,
    default_namespace: str,
) -> str:
    """Get the namespace for the current kubeconfig context."""
    k8s = kubernetes.kubernetes
    ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
    if (context_name == kubernetes.in_cluster_context_name() or
            context_name is None):
        env_namespace = os.getenv(
            kubernetes_constants.KUBERNETES_IN_CLUSTER_NAMESPACE_ENV_VAR)
        if env_namespace:
            return env_namespace
        if os.path.exists(ns_path):
            with open(ns_path, encoding='utf-8') as handle:
                return handle.read().strip()
    try:
        contexts, current_context = kubernetes.list_kube_config_contexts()
        if context_name is None:
            context = current_context
        else:
            context = next((c for c in contexts if c['name'] == context_name),
                           None)
            if context is None:
                return default_namespace

        if 'namespace' in context['context']:
            return context['context']['namespace']
        return default_namespace
    except k8s.config.config_exception.ConfigException:
        return default_namespace


def get_namespace(
    context: str | None = None,
    workspace: str | None = None,
    override_configs: dict[str, Any] | None = None,
    cloud: str = 'kubernetes',
    *,
    get_effective_namespace: Callable[..., str | None],
    get_kube_config_context_namespace_fn: Callable[[str | None], str],
) -> str:
    """Resolve the Kubernetes namespace for ``context``, with fallback."""
    config_namespace = get_effective_namespace(
        cloud=cloud,
        region=context,
        workspace=workspace,
        override_configs=override_configs,
    )
    if config_namespace is not None:
        return config_namespace
    return get_kube_config_context_namespace_fn(context)


def get_kubeconfig_paths() -> list[str]:
    """Get the path to the kubeconfig files."""
    paths = os.getenv('KUBECONFIG', kubernetes.DEFAULT_KUBECONFIG_PATH)
    return [
        os.path.expanduser(path)
        for path in paths.split(kubernetes.ENV_KUBECONFIG_PATH_SEPARATOR)
    ]


def format_kubeconfig_exec_auth(config: Any,
                                output_path: str,
                                inject_wrapper: bool = True,
                                *,
                                safe_dump_fn: Callable[..., None]) -> bool:
    """Rewrite exec authentication commands for the SkyPilot runtime."""
    updated = False
    for user in config.get('users', []):
        exec_info = user.get('user', {}).get('exec', {})
        current_command = exec_info.get('command', '')

        if current_command:
            # Strip the path and keep only the executable name.
            executable = os.path.basename(current_command)
            if executable == kubernetes_constants.SKY_K8S_EXEC_AUTH_WRAPPER:
                # Avoid recursively wrapping a previously rewritten command.
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

            # Nebius profiles are local-machine specific.  Use the profile
            # provisioned in the SkyPilot runtime instead.
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
        safe_dump_fn(config, file)

    return updated


def format_kubeconfig_exec_auth_with_cache(
    kubeconfig_path: str,
    *,
    safe_load_fn: Callable[..., Any],
    dump_fn: Callable[..., str],
    format_kubeconfig_exec_auth_fn: Callable[..., bool],
    warning_fn: Callable[[str], None],
    format_exception_fn: Callable[..., str],
) -> str:
    """Rewrite a kubeconfig into the content-addressed credential cache."""
    with open(kubeconfig_path, encoding='utf-8') as file:
        config = safe_load_fn(file)
    normalized = dump_fn(config, sort_keys=True)
    hashed = hashlib.sha1(normalized.encode('utf-8'),
                          usedforsecurity=False).hexdigest()
    path = os.path.expanduser(
        f'{kubernetes_constants.SKY_K8S_EXEC_AUTH_KUBECONFIG_CACHE}/{hashed}.yaml'
    )

    if os.path.isfile(path):
        return path

    try:
        format_kubeconfig_exec_auth_fn(config, path)
        return path
    except Exception as e:  # pylint: disable=broad-except
        # The user may not be using Kubernetes or SSH node pools, so keep the
        # historical best-effort fallback to the original kubeconfig.
        warning_fn(f'Failed to format kubeconfig at {kubeconfig_path}. '
                   'Please check if the kubeconfig is valid. This may cause '
                   'problems when Kubernetes infra is used. '
                   f'Reason: {format_exception_fn(e)}')
        return kubeconfig_path
