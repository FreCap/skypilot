"""Controller bootstrap dependency command generation."""

import copy

from sky import check as sky_check
from sky import clouds
from sky.clouds import cloud as sky_cloud
from sky.clouds import gcp
from sky.data import storage as storage_lib
from sky.provision.kubernetes import constants as kubernetes_constants
from sky.setup_files import dependencies
from sky.skylet import constants
from sky.utils import command_runner
from sky.utils.controller_types import Controllers


# Install cli dependencies. Not using SkyPilot wheels because the wheel
# can be cleaned up by another process.
def _get_cloud_dependencies_installation_commands(
        controller: Controllers) -> list[str]:
    # We use <step>/<total> instead of strong formatting, as we need to update
    # the <total> at the end of the for loop, and python does not support
    # partial string formatting.
    prefix_str = ('[<step>/<total>] Check & install cloud dependencies '
                  'on controller: ')
    commands: list[str] = []
    # This is to make sure the shorter checking message does not have junk
    # characters from the previous message.
    empty_str = ' ' * 20

    # All python dependencies will be accumulated and then installed in one
    # command at the end. This is very fast if the packages are already
    # installed, so we don't check that.
    python_packages: set[str] = set()

    step_prefix = prefix_str.replace('<step>', str(len(commands) + 1))
    # Wrap in braces to isolate the || in SKY_UV_INSTALL_CMD from
    # the outer && chain, preventing operator precedence issues.
    commands.append(f'echo -en "\\r{step_prefix}uv{empty_str}" && '
                    f'{{ {constants.SKY_UV_INSTALL_CMD} >/dev/null 2>&1; }} && '
                    f'{command_runner.ALIAS_SUDO_TO_EMPTY_FOR_ROOT_CMD}')

    enabled_compute_clouds = set(
        sky_check.get_cached_enabled_clouds_or_refresh(
            sky_cloud.CloudCapability.COMPUTE))
    enabled_storage_clouds = set(
        sky_check.get_cached_enabled_clouds_or_refresh(
            sky_cloud.CloudCapability.STORAGE))
    enabled_clouds = enabled_compute_clouds.union(enabled_storage_clouds)
    enabled_k8s_and_ssh = [
        repr(cloud)
        for cloud in enabled_clouds
        if isinstance(cloud, clouds.Kubernetes)
    ]
    k8s_and_ssh_label = ' and '.join(sorted(enabled_k8s_and_ssh))
    k8s_dependencies_installed = False

    for cloud in sorted(enabled_clouds, key=repr):
        cloud_python_dependencies: list[str] = copy.deepcopy(
            dependencies.extras_require[cloud.canonical_name()])

        if isinstance(cloud, clouds.Azure):
            # azure-cli cannot be normally installed by uv.
            # See comments in sky/skylet/constants.py.
            cloud_python_dependencies.remove(dependencies.AZURE_CLI)

            step_prefix = prefix_str.replace('<step>', str(len(commands) + 1))
            commands.append(
                f'echo -en "\\r{step_prefix}azure-cli{empty_str}" &&'
                f'{constants.SKY_UV_PIP_CMD} install --prerelease=allow '
                f'"{dependencies.AZURE_CLI}" > /dev/null 2>&1')
        elif isinstance(cloud, clouds.GCP):
            step_prefix = prefix_str.replace('<step>', str(len(commands) + 1))
            commands.append(f'echo -en "\\r{step_prefix}GCP SDK{empty_str}" &&'
                            f'{gcp.GOOGLE_SDK_INSTALLATION_COMMAND}')
            if clouds.cloud_in_iterable(clouds.Kubernetes(), enabled_clouds):
                # Install gke-gcloud-auth-plugin used for exec-auth with GKE.
                # We install the plugin here instead of the next elif branch
                # because gcloud is required to install the plugin, so the order
                # of command execution is critical.

                # We install plugin here regardless of whether exec-auth is
                # actually used as exec-auth may be used in the future.
                # TODO (kyuds): how to implement conservative installation?
                commands.append(
                    '(command -v gke-gcloud-auth-plugin &>/dev/null || '
                    '(gcloud components install gke-gcloud-auth-plugin --quiet &>/dev/null))')  # pylint: disable=line-too-long
        elif isinstance(cloud, clouds.Nebius):
            step_prefix = prefix_str.replace('<step>', str(len(commands) + 1))
            # Wrap in braces to isolate the || from the outer && chain.
            commands.append(
                f'echo -en "\\r{step_prefix}Nebius{empty_str}" && '
                '{ curl -sSL https://storage.eu-north1.nebius.cloud/cli/install.sh '  # pylint: disable=line-too-long
                '| sudo NEBIUS_INSTALL_FOLDER=/usr/local/bin bash &> /dev/null && '
                'nebius profile create --profile sky '
                '--endpoint api.nebius.cloud '
                '--service-account-file $HOME/.nebius/credentials.json '
                '&> /dev/null || echo "Unable to create Nebius profile."; }')
        elif (isinstance(cloud, clouds.Kubernetes) and
              not k8s_dependencies_installed):
            step_prefix = prefix_str.replace('<step>', str(len(commands) + 1))
            commands.append(
                f'echo -en "\\r{step_prefix}{k8s_and_ssh_label}{empty_str}" && '
                # Install k8s + skypilot dependencies
                'sudo bash -c "if '
                '! command -v curl &> /dev/null || '
                '! command -v socat &> /dev/null || '
                '! command -v nc &> /dev/null; '
                'then apt update &> /dev/null && '
                'apt install curl socat netcat-openbsd -y &> /dev/null; '
                'fi" && '
                # Install kubectl
                'ARCH=$(uname -m) && '
                'if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then '
                '  ARCH="arm64"; '
                'else '
                '  ARCH="amd64"; '
                'fi && '
                '(command -v kubectl &>/dev/null || '
                '(curl -s -LO "https://dl.k8s.io/release/v1.31.6'
                '/bin/linux/$ARCH/kubectl" && '
                'sudo install -o root -g root -m 0755 '
                'kubectl /usr/local/bin/kubectl)) && '
                f'echo -e \'#!/bin/bash\\nexport PATH="{kubernetes_constants.SKY_K8S_EXEC_AUTH_PATH}"\\nexec "$@"\' | sudo tee /usr/local/bin/{kubernetes_constants.SKY_K8S_EXEC_AUTH_WRAPPER} > /dev/null && '  # pylint: disable=line-too-long
                f'sudo chmod +x /usr/local/bin/{kubernetes_constants.SKY_K8S_EXEC_AUTH_WRAPPER}')  # pylint: disable=line-too-long
            k8s_dependencies_installed = True
        elif isinstance(cloud, clouds.Cudo):
            step_prefix = prefix_str.replace('<step>', str(len(commands) + 1))
            commands.append(
                f'echo -en "\\r{step_prefix}cudoctl{empty_str}" && '
                'wget https://download.cudo.org/compute/cudoctl-0.3.2-amd64.deb -O ~/cudoctl.deb > /dev/null 2>&1 && '  # pylint: disable=line-too-long
                'sudo dpkg -i ~/cudoctl.deb > /dev/null 2>&1')
        elif isinstance(cloud, clouds.IBM):
            if controller != Controllers.JOBS_CONTROLLER:
                # We only need IBM deps on the jobs controller.
                cloud_python_dependencies = []
        elif isinstance(cloud, clouds.Vast):
            step_prefix = prefix_str.replace('<step>', str(len(commands) + 1))
            # Wrap in braces to isolate the || from the outer && chain.
            commands.append(
                f'echo -en "\\r{step_prefix}Vast{empty_str}" && '
                '{ pip list | grep vastai_sdk > /dev/null 2>&1 || '
                'pip install "vastai_sdk>=0.1.12" > /dev/null 2>&1; }')

        python_packages.update(cloud_python_dependencies)

    storage_clouds = storage_lib.get_cached_enabled_storage_cloud_names_or_refresh()  # pylint: disable=line-too-long

    for sc in storage_clouds:
        if sc.lower() in constants.STORAGE_ONLY_CLOUDS:
            python_packages.update(dependencies.extras_require[sc.lower()])

    # Pin click<8.3.0: typer>=0.25.0 requires click>=8.2.1 with no upper
    # bound, which lets uv resolve click to 8.3.x. click 8.3.0+ breaks Ray
    # CLI on the controller via copy.deepcopy on Click's Sentinel values.
    # See https://github.com/ray-project/ray/issues/56747.
    python_packages.add('click<8.3.0')
    packages_string = ' '.join(
        [f'"{package}"' for package in sorted(python_packages)])
    step_prefix = prefix_str.replace('<step>', str(len(commands) + 1))
    commands.append(
        f'echo -en "\\r{step_prefix}cloud python packages{empty_str}" && '
        f'{constants.SKY_UV_PIP_CMD} install {packages_string} > /dev/null 2>&1'
    )

    total_commands = len(commands)
    finish_prefix = prefix_str.replace('[<step>/<total>] ', '  ')
    commands.append(f'echo -e "\\r{finish_prefix}done.{empty_str}"')

    commands = [
        command.replace('<total>', str(total_commands)) for command in commands
    ]
    return commands
