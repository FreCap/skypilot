"""Rclone configuration generation and profile-file lifecycle."""

import enum
import os
import subprocess
import textwrap

from filelock import FileLock

from sky import clouds
from sky import exceptions
from sky import sky_logging
from sky.adaptors import aws
from sky.adaptors import cloudflare
from sky.adaptors import coreweave
from sky.adaptors import ibm
from sky.adaptors import nebius
from sky.adaptors import oci_s3
from sky.adaptors import vastdata
from sky.skylet import constants
from sky.utils import ux_utils

# Keep the historical logger namespace while the public facade remains
# sky.data.data_utils.
logger = sky_logging.init_logger('sky.data.data_utils')


class Rclone:
    """Provides methods to manage and generate Rclone configuration profile."""

    # TODO(syang) Move the enum's functionality into AbstractStore subclass and
    # deprecate this class.
    class RcloneStores(enum.Enum):
        """Rclone supporting storage types and supporting methods."""
        S3 = 'S3'
        GCS = 'GCS'
        IBM = 'IBM'
        R2 = 'R2'
        AZURE = 'AZURE'
        NEBIUS = 'NEBIUS'
        COREWEAVE = 'COREWEAVE'
        VASTDATA = 'VASTDATA'
        OCI = 'OCI'

        def get_profile_name(self, bucket_name: str) -> str:
            """Gets the Rclone profile name for a given bucket.

            Args:
                bucket_name: The name of the bucket.

            Returns:
                A string containing the Rclone profile name, which combines
                prefix based on the storage type and the bucket name.
            """
            profile_prefix = {
                Rclone.RcloneStores.S3: 'sky-s3',
                Rclone.RcloneStores.GCS: 'sky-gcs',
                Rclone.RcloneStores.IBM: 'sky-ibm',
                Rclone.RcloneStores.R2: 'sky-r2',
                Rclone.RcloneStores.AZURE: 'sky-azure',
                Rclone.RcloneStores.NEBIUS: 'sky-nebius',
                Rclone.RcloneStores.COREWEAVE: 'sky-coreweave',
                Rclone.RcloneStores.VASTDATA: 'sky-vastdata',
                Rclone.RcloneStores.OCI: 'sky-oci',
            }
            return f'{profile_prefix[self]}-{bucket_name}'

        def get_config(self,
                       bucket_name: str | None = None,
                       rclone_profile_name: str | None = None,
                       region: str | None = None,
                       storage_account_name: str | None = None,
                       storage_account_key: str | None = None) -> str:
            """Generates an Rclone configuration for a specific storage type.

            This method creates an Rclone configuration string based on the
            storage type and the provided parameters.

            Args:
                bucket_name: The name of the bucket.
                rclone_profile_name: The name of the Rclone profile. If not
                    provided, it will be generated using the bucket_name.
                region: Region of bucket.

            Returns:
                A string containing the Rclone configuration.

            Raises:
                NotImplementedError: If the storage type is not supported.
            """
            if rclone_profile_name is None:
                assert bucket_name is not None
                rclone_profile_name = self.get_profile_name(bucket_name)
            if self is Rclone.RcloneStores.S3:
                if clouds.AWS.should_use_env_auth_for_s3():
                    # Use environment-based auth for SSO, IAM roles, etc.
                    # This allows rclone to use the AWS SDK credential chain
                    # which properly handles temporary credentials and
                    # container credentials (Pod Identity, IRSA, etc.)
                    config = textwrap.dedent(f"""\
                        [{rclone_profile_name}]
                        type = s3
                        provider = AWS
                        env_auth = true
                        acl = private
                        """)
                else:
                    # Use static credentials for shared-credentials-file
                    aws_credentials = (aws.session().get_credentials().
                                       get_frozen_credentials())
                    access_key_id = aws_credentials.access_key
                    secret_access_key = aws_credentials.secret_key
                    config = textwrap.dedent(f"""\
                        [{rclone_profile_name}]
                        type = s3
                        provider = AWS
                        access_key_id = {access_key_id}
                        secret_access_key = {secret_access_key}
                        acl = private
                        """)
            elif self is Rclone.RcloneStores.GCS:
                config = textwrap.dedent(f"""\
                    [{rclone_profile_name}]
                    type = google cloud storage
                    project_number = {clouds.GCP.get_project_id()}
                    bucket_policy_only = true
                    """)
            elif self is Rclone.RcloneStores.IBM:
                access_key_id, secret_access_key = ibm.get_hmac_keys()
                config = textwrap.dedent(f"""\
                    [{rclone_profile_name}]
                    type = s3
                    provider = IBMCOS
                    access_key_id = {access_key_id}
                    secret_access_key = {secret_access_key}
                    region = {region}
                    endpoint = s3.{region}.cloud-object-storage.appdomain.cloud
                    location_constraint = {region}-smart
                    acl = private
                    """)
            elif self is Rclone.RcloneStores.R2:
                cloudflare_session = cloudflare.session()
                cloudflare_credentials = (
                    cloudflare.get_r2_credentials(cloudflare_session))
                endpoint = cloudflare.create_endpoint()
                access_key_id = cloudflare_credentials.access_key
                secret_access_key = cloudflare_credentials.secret_key
                config = textwrap.dedent(f"""\
                    [{rclone_profile_name}]
                    type = s3
                    provider = Cloudflare
                    access_key_id = {access_key_id}
                    secret_access_key = {secret_access_key}
                    endpoint = {endpoint}
                    region = auto
                    acl = private
                    """)
            elif self is Rclone.RcloneStores.AZURE:
                assert storage_account_name and storage_account_key
                config = textwrap.dedent(f"""\
                    [{rclone_profile_name}]
                    type = azureblob
                    account = {storage_account_name}
                    key = {storage_account_key}
                    """)
            elif self is Rclone.RcloneStores.NEBIUS:
                nebius_session = nebius.session()
                nebius_credentials = nebius.get_nebius_credentials(
                    nebius_session)
                # Get endpoint URL from the client
                client = nebius.client('s3')
                endpoint_url = client.meta.endpoint_url
                access_key_id = nebius_credentials.access_key
                secret_access_key = nebius_credentials.secret_key
                config = textwrap.dedent(f"""\
                    [{rclone_profile_name}]
                    type = s3
                    provider = Other
                    access_key_id = {access_key_id}
                    secret_access_key = {secret_access_key}
                    endpoint = {endpoint_url}
                    acl = private
                    """)
            elif self is Rclone.RcloneStores.COREWEAVE:
                coreweave_session = coreweave.session()
                coreweave_credentials = coreweave.get_coreweave_credentials(
                    coreweave_session)
                # Get endpoint URL from the client
                endpoint_url = coreweave.get_endpoint()
                access_key_id = coreweave_credentials.access_key
                secret_access_key = coreweave_credentials.secret_key
                config = textwrap.dedent(f"""\
                    [{rclone_profile_name}]
                    type = s3
                    provider = Other
                    access_key_id = {access_key_id}
                    secret_access_key = {secret_access_key}
                    endpoint = {endpoint_url}
                    region = auto
                    acl = private
                    force_path_style = false
                    """)
            elif self is Rclone.RcloneStores.OCI:
                oci_s3_session = oci_s3.session()
                oci_s3_credentials = oci_s3.get_oci_s3_credentials(
                    oci_s3_session)
                endpoint_url = oci_s3.get_endpoint()
                access_key_id = oci_s3_credentials.access_key
                secret_access_key = oci_s3_credentials.secret_key
                config = textwrap.dedent(f"""\
                    [{rclone_profile_name}]
                    type = s3
                    provider = Other
                    access_key_id = {access_key_id}
                    secret_access_key = {secret_access_key}
                    endpoint = {endpoint_url}
                    acl = private
                    force_path_style = true
                    """)
                oci_region = oci_s3.get_region()
                if oci_region:
                    config += f'region = {oci_region}\n'
            elif self is Rclone.RcloneStores.VASTDATA:
                vastdata_session = vastdata.session()
                vastdata_credentials = vastdata.get_vastdata_credentials(
                    vastdata_session)
                endpoint_url = vastdata.get_endpoint()
                access_key_id = vastdata_credentials.access_key
                secret_access_key = vastdata_credentials.secret_key
                config = textwrap.dedent(f"""\
                    [{rclone_profile_name}]
                    type = s3
                    provider = Other
                    access_key_id = {access_key_id}
                    secret_access_key = {secret_access_key}
                    endpoint = {endpoint_url}
                    region = auto
                    acl = private
                    """)
            else:
                with ux_utils.print_exception_no_traceback():
                    raise NotImplementedError(
                        f'Unsupported store type for Rclone: {self}')
            return config

    @staticmethod
    def store_rclone_config(bucket_name: str, cloud: RcloneStores,
                            region: str) -> str:
        """Creates rclone configuration files for bucket syncing and mounting.

        Args:
            bucket_name: Name of the bucket.
            cloud: RcloneStores enum representing the cloud provider.
            region: Region of the bucket.

        Returns:
            str: The configuration data written to the file.

        Raises:
            StorageError: If rclone is not installed.
        """
        rclone_config_path = os.path.expanduser(constants.RCLONE_CONFIG_PATH)
        config_data = cloud.get_config(bucket_name=bucket_name, region=region)
        try:
            subprocess.run('rclone version',
                           shell=True,
                           check=True,
                           stdout=subprocess.PIPE)
        except subprocess.CalledProcessError:
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageError(
                    'rclone wasn\'t detected. '
                    'Consider installing via: '
                    '"curl https://rclone.org/install.sh '
                    '| sudo bash" ') from None

        os.makedirs(os.path.dirname(rclone_config_path), exist_ok=True)
        if not os.path.isfile(rclone_config_path):
            open(rclone_config_path, 'w', encoding='utf-8').close()

        # write back file without profile: [bucket_name]
        # to which the new bucket profile is appended
        with FileLock(rclone_config_path + '.lock'):
            profiles_to_keep = Rclone._remove_bucket_profile_rclone(
                bucket_name, cloud)
            with open(f'{rclone_config_path}', 'w', encoding='utf-8') as file:
                if profiles_to_keep:
                    file.writelines(profiles_to_keep)
                    if profiles_to_keep[-1].strip():
                        # add a new line to config_data
                        # if last file line contains data
                        config_data += '\n'
                file.write(config_data)

        return config_data

    @staticmethod
    def get_region_from_rclone(bucket_name: str, cloud: RcloneStores) -> str:
        """Returns the region field of the specified bucket in rclone.conf.

        Args:
            bucket_name: Name of the bucket.
            cloud: RcloneStores enum representing the cloud provider.

        Returns:
            The region field if the bucket exists, otherwise an empty string.
        """
        rclone_profile = cloud.get_profile_name(bucket_name)
        rclone_config_path = os.path.expanduser(constants.RCLONE_CONFIG_PATH)
        with open(rclone_config_path, encoding='utf-8') as file:
            bucket_profile_found = False
            for line in file:
                if line.lstrip().startswith('#'):  # skip user's comments.
                    continue
                if line.strip() == f'[{rclone_profile}]':
                    bucket_profile_found = True
                elif bucket_profile_found and line.startswith('region'):
                    return line.split('=')[1].strip()
                elif bucket_profile_found and line.startswith('['):
                    # for efficiency stop if we've searched past the
                    # requested bucket profile with no match
                    return ''
        # segment for bucket and/or region field for bucket wasn't found
        return ''

    @staticmethod
    def delete_rclone_bucket_profile(bucket_name: str, cloud: RcloneStores):
        """Deletes specified bucket profile from rclone.conf.

        Args:
            bucket_name: Name of the bucket.
            cloud: RcloneStores enum representing the cloud provider.
        """
        rclone_profile = cloud.get_profile_name(bucket_name)
        rclone_config_path = os.path.expanduser(constants.RCLONE_CONFIG_PATH)

        if not os.path.isfile(rclone_config_path):
            logger.warning('Failed to locate "rclone.conf" while '
                           f'trying to delete rclone profile: {rclone_profile}')
            return

        with FileLock(rclone_config_path + '.lock'):
            profiles_to_keep = Rclone._remove_bucket_profile_rclone(
                bucket_name, cloud)

            # write back file without profile: [rclone_profile]
            with open(f'{rclone_config_path}', 'w', encoding='utf-8') as file:
                file.writelines(profiles_to_keep)

    @staticmethod
    def _remove_bucket_profile_rclone(bucket_name: str,
                                      cloud: RcloneStores) -> list[str]:
        """Returns rclone profiles without ones matching [prefix+bucket_name].

        Args:
            bucket_name: Name of the bucket.
            cloud: RcloneStores enum representing the cloud provider.

        Returns:
            Lines to keep in the rclone config file.
        """
        rclone_profile_name = cloud.get_profile_name(bucket_name)
        rclone_config_path = os.path.expanduser(constants.RCLONE_CONFIG_PATH)

        with open(rclone_config_path, encoding='utf-8') as file:
            lines = file.readlines()  # returns a list of the file's lines
            # delete existing bucket profile matching:
            # '[profile_prefix+bucket_name]'
            lines_to_keep = []  # lines to write back to file
            # while skip_lines is True avoid adding lines to lines_to_keep
            skip_lines = False

        for line in lines:
            if line.lstrip().startswith('#') and not skip_lines:
                # keep user comments only if they aren't under
                # a profile we are discarding
                lines_to_keep.append(line)
            elif f'[{rclone_profile_name}]' in line:
                skip_lines = True
            elif skip_lines:
                if '[' in line:
                    # former profile segment ended, new one begins
                    skip_lines = False
                    lines_to_keep.append(line)
            else:
                lines_to_keep.append(line)

        return lines_to_keep
