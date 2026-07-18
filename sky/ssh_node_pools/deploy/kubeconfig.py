"""Local kubeconfig materialization for SSH node pools."""

import base64
import os
import re
import shutil
import tempfile

import colorama

from sky import sky_logging
from sky.ssh_node_pools import constants
from sky.ssh_node_pools.deploy import utils as deploy_utils

RESET_ALL = colorama.Style.RESET_ALL

# Preserve the historical log source for this pure structural extraction.
logger = sky_logging.init_logger('sky.ssh_node_pools.deploy.deploy')


def configure_local_kubeconfig(*, head_node: str, ssh_user: str, ssh_key: str,
                               context_name: str, effective_master_ip: str,
                               kubeconfig_path: str,
                               use_ssh_config: bool) -> None:
    """Download, rewrite, and merge an SSH node pool's kubeconfig."""
    cert_file_path = os.path.join(constants.NODE_POOLS_INFO_DIR,
                                  f'{context_name}-cert.pem')
    key_file_path = os.path.join(constants.NODE_POOLS_INFO_DIR,
                                 f'{context_name}-key.pem')

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_kubeconfig = os.path.join(temp_dir, 'kubeconfig')

        # Get the kubeconfig from remote server
        if use_ssh_config:
            scp_cmd = ['scp', head_node + ':~/.kube/config', temp_kubeconfig]
        else:
            scp_cmd = [
                'scp', '-o', 'StrictHostKeyChecking=no', '-o',
                'IdentitiesOnly=yes', '-i', ssh_key,
                f'{ssh_user}@{head_node}:~/.kube/config', temp_kubeconfig
            ]
        deploy_utils.run_command(scp_cmd, shell=False)

        # Create the directory for the kubeconfig file if it doesn't exist
        deploy_utils.ensure_directory_exists(kubeconfig_path)

        # Create empty kubeconfig if it doesn't exist
        if not os.path.isfile(kubeconfig_path):
            open(kubeconfig_path, 'a', encoding='utf-8').close()

        # Modify the temporary kubeconfig to update server address and context
        # name.
        modified_config = os.path.join(temp_dir, 'modified_config')
        with open(temp_kubeconfig, encoding='utf-8') as f_in:
            with open(modified_config, 'w', encoding='utf-8') as f_out:
                in_cluster = False
                in_user = False
                client_cert_data = None
                client_key_data = None

                for line in f_in:
                    if 'clusters:' in line:
                        in_cluster = True
                        in_user = False
                    elif 'users:' in line:
                        in_cluster = False
                        in_user = True
                    elif 'contexts:' in line:
                        in_cluster = False
                        in_user = False

                    # Skip certificate authority data in cluster section
                    if in_cluster and 'certificate-authority-data:' in line:
                        continue
                    # Skip client certificate data in user section but extract
                    # it.
                    elif in_user and 'client-certificate-data:' in line:
                        client_cert_data = line.split(':', 1)[1].strip()
                        continue
                    # Skip client key data in user section but extract it
                    elif in_user and 'client-key-data:' in line:
                        client_key_data = line.split(':', 1)[1].strip()
                        continue
                    elif in_cluster and 'server:' in line:
                        # Initially just set to the effective master IP (will
                        # be changed to localhost by the SSH tunnel setup).
                        f_out.write(
                            f'    server: https://{effective_master_ip}:6443\n')
                        f_out.write('    insecure-skip-tls-verify: true\n')
                        continue

                    # Replace default context names with user-provided context
                    # name.
                    line = line.replace('name: default',
                                        f'name: {context_name}')
                    line = line.replace('cluster: default',
                                        f'cluster: {context_name}')
                    line = line.replace('user: default',
                                        f'user: {context_name}')
                    line = line.replace('current-context: default',
                                        f'current-context: {context_name}')

                    f_out.write(line)

                # Save certificate data if available
                if client_cert_data:
                    # Decode base64 data and save as PEM
                    try:
                        # Clean up the certificate data by removing whitespace
                        clean_cert_data = ''.join(client_cert_data.split())
                        cert_pem = base64.b64decode(clean_cert_data).decode(
                            'utf-8')

                        # Check if the data already looks like a PEM file
                        has_begin = '-----BEGIN CERTIFICATE-----' in cert_pem
                        has_end = '-----END CERTIFICATE-----' in cert_pem

                        if not has_begin or not has_end:
                            logger.debug(
                                'Warning: Certificate data missing PEM '
                                'markers, attempting to fix...')
                            # Add PEM markers if missing
                            if not has_begin:
                                cert_pem = (
                                    f'-----BEGIN CERTIFICATE-----\n{cert_pem}')
                            if not has_end:
                                cert_pem = (
                                    f'{cert_pem}\n-----END CERTIFICATE-----')

                        # Write the certificate
                        with open(cert_file_path, 'w',
                                  encoding='utf-8') as cert_file:
                            cert_file.write(cert_pem)

                        # Verify the file was written correctly
                        if os.path.getsize(cert_file_path) > 0:
                            logger.debug(f'Successfully saved certificate data '
                                         f'({len(cert_pem)} bytes)')

                            # Quick validation of PEM format
                            with open(cert_file_path, encoding='utf-8') as f:
                                content = f.readlines()
                                first_line = content[0].strip(
                                ) if content else ''
                                last_line = content[-1].strip(
                                ) if content else ''

                            if not first_line.startswith(
                                    '-----BEGIN') or not last_line.startswith(
                                        '-----END'):
                                logger.debug(
                                    'Warning: Certificate may not be in '
                                    'proper PEM format')
                        else:
                            logger.error(f'{colorama.Fore.RED}Error: '
                                         f'Certificate file is empty'
                                         f'{RESET_ALL}')
                    except Exception as e:  # pylint: disable=broad-except
                        logger.error(f'{colorama.Fore.RED}'
                                     f'Error processing certificate data: {e}'
                                     f'{RESET_ALL}')

                if client_key_data:
                    # Decode base64 data and save as PEM
                    try:
                        # Clean up the key data by removing whitespace
                        clean_key_data = ''.join(client_key_data.split())
                        key_pem = base64.b64decode(clean_key_data).decode(
                            'utf-8')

                        # Check for EC key format
                        if 'EC PRIVATE KEY' in key_pem:
                            # Handle EC KEY format directly
                            match_ec = re.search(
                                r'-----BEGIN EC PRIVATE KEY-----(.*?)-----END EC PRIVATE KEY-----',
                                key_pem, re.DOTALL)
                            if match_ec:
                                # Extract and properly format EC key
                                key_content = match_ec.group(1).strip()
                                key_pem = (f'-----BEGIN EC PRIVATE KEY-----\n'
                                           f'{key_content}\n'
                                           f'-----END EC PRIVATE KEY-----')
                            else:
                                # Extract content and assume EC format
                                key_content = re.sub(r'-----BEGIN.*?-----', '',
                                                     key_pem)
                                key_content = re.sub(r'-----END.*?-----.*', '',
                                                     key_content).strip()
                                key_pem = (f'-----BEGIN EC PRIVATE KEY-----\n'
                                           f'{key_content}\n'
                                           f'-----END EC PRIVATE KEY-----')
                        else:
                            # Handle regular private key format
                            has_begin = any(marker in key_pem for marker in [
                                '-----BEGIN PRIVATE KEY-----',
                                '-----BEGIN RSA PRIVATE KEY-----'
                            ])
                            has_end = any(marker in key_pem for marker in [
                                '-----END PRIVATE KEY-----',
                                '-----END RSA PRIVATE KEY-----'
                            ])

                            if not has_begin or not has_end:
                                logger.debug(
                                    'Warning: Key data missing PEM markers, '
                                    'attempting to fix...')
                                # Add PEM markers if missing
                                if not has_begin:
                                    key_pem = (f'-----BEGIN PRIVATE KEY-----\n'
                                               f'{key_pem}')
                                if not has_end:
                                    key_pem = (
                                        f'{key_pem}\n-----END PRIVATE KEY-----')
                                    # Remove any trailing characters after END
                                    # marker.
                                    key_pem = re.sub(
                                        r'(-----END PRIVATE KEY-----).*', r'\1',
                                        key_pem)

                        # Write the key
                        with open(key_file_path, 'w',
                                  encoding='utf-8') as key_file:
                            key_file.write(key_pem)

                        # Verify the file was written correctly
                        if os.path.getsize(key_file_path) > 0:
                            logger.debug(f'Successfully saved key data '
                                         f'({len(key_pem)} bytes)')

                            # Quick validation of PEM format
                            with open(key_file_path, encoding='utf-8') as f:
                                content = f.readlines()
                                first_line = content[0].strip(
                                ) if content else ''
                                last_line = content[-1].strip(
                                ) if content else ''

                            if not first_line.startswith(
                                    '-----BEGIN') or not last_line.startswith(
                                        '-----END'):
                                logger.debug(
                                    'Warning: Key may not be in proper PEM '
                                    'format')
                        else:
                            logger.error(f'{colorama.Fore.RED}Error: '
                                         f'Key file is empty'
                                         f'{RESET_ALL}')
                    except Exception as e:  # pylint: disable=broad-except
                        logger.error(f'{colorama.Fore.RED}'
                                     f'Error processing key data: {e}'
                                     f'{RESET_ALL}')

        # Build and validate the merged config before publishing it. A failed
        # kubectl command must not replace the caller's kubeconfig with an
        # empty file or leave KUBECONFIG pointing into this temporary
        # directory.
        previous_kubeconfig = os.environ.get('KUBECONFIG')
        base_config = os.path.join(temp_dir, 'base_config')
        merged_config = os.path.join(temp_dir, 'merged_config')
        try:
            # Remove an older version of this context from a private copy.
            # Mutating the live kubeconfig before the merge succeeds would
            # make rollback impossible.
            shutil.copyfile(kubeconfig_path, base_config)
            os.environ['KUBECONFIG'] = base_config
            # TODO(romilb): Should we throw an error here instead?
            deploy_utils.run_command(
                ['kubectl', 'config', 'delete-context', context_name],
                shell=False,
                silent=True)
            deploy_utils.run_command(
                ['kubectl', 'config', 'delete-cluster', context_name],
                shell=False,
                silent=True)
            deploy_utils.run_command(
                ['kubectl', 'config', 'delete-user', context_name],
                shell=False,
                silent=True)

            os.environ['KUBECONFIG'] = f'{base_config}:{modified_config}'
            kubectl_cmd = ['kubectl', 'config', 'view', '--flatten']
            result = deploy_utils.run_command(kubectl_cmd, shell=False)
            if not result:
                raise RuntimeError('Failed to merge kubeconfig')
            with open(merged_config, 'w', encoding='utf-8') as merged_file:
                merged_file.write(result)

            # Apply the context selection to the unpublished config so a
            # failure still leaves the existing kubeconfig untouched.
            os.environ['KUBECONFIG'] = merged_config
            use_context_result = deploy_utils.run_command(
                ['kubectl', 'config', 'use-context', context_name],
                shell=False,
                silent=True)
            if use_context_result is None:
                raise RuntimeError(
                    f'Failed to select kubeconfig context {context_name!r}')

            shutil.move(merged_config, kubeconfig_path)
            os.environ['KUBECONFIG'] = kubeconfig_path
        except BaseException:
            if previous_kubeconfig is None:
                os.environ.pop('KUBECONFIG', None)
            else:
                os.environ['KUBECONFIG'] = previous_kubeconfig
            raise
