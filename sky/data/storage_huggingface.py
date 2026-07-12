"""Hugging Face storage backend implementation."""
import concurrent.futures
import os
import re
import shutil
import tempfile
from typing import Dict, List, Optional, Tuple

import colorama

from sky import exceptions
from sky.adaptors import huggingface
from sky.data import data_utils
from sky.data import mounting_utils
from sky.data import storage as storage_lib
from sky.utils import rich_utils
from sky.utils import ux_utils

SourceType = storage_lib.SourceType
StorageHandle = storage_lib.StorageHandle
AbstractStore = storage_lib.AbstractStore
MountCachedConfig = storage_lib.MountCachedConfig


class HuggingFaceStore(AbstractStore):
    """HuggingFaceStore backs Storage objects with Hugging Face resources.

    Supports two kinds of sources:

    1. **Buckets** (read-write) -- Xet-backed S3-like object storage. Bucket
       ids are ``<namespace>/<bucket-name>`` (e.g. ``my-user/my-bucket``).

    2. **Repos** (read-only) -- models, datasets, and spaces. Source URLs:

       - ``hf://<ns>/<model>[@<rev>][/<sub-path>]``
       - ``hf://datasets/<ns>/<name>[@<rev>][/<sub-path>]``
       - ``hf://spaces/<ns>/<name>[@<rev>][/<sub-path>]``

       Repo mounts are always read-only; repo-typed stores are never
       ``is_sky_managed``.

    Bucket operations go through the ``huggingface_hub`` Python SDK; mounts
    are performed with the ``hf-mount`` NFS backend.
    """

    _NAME_PATTERN = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9._\-]{0,95}$')

    # HF repo type values recognized by ``hf-mount`` and ``huggingface_hub``,
    # mapped to the URL path segment that prefixes ``<ns>/<name>`` in an
    # ``hf://`` URL and in the ``hf-mount`` CLI argument. Models have no
    # prefix; datasets/spaces do.
    _REPO_TYPE_TO_URL_PREFIX: Dict[str, str] = {
        'model': '',
        'dataset': 'datasets/',
        'space': 'spaces/',
    }

    @classmethod
    def hf_id_from_repo_parts(cls, repo_type: str, repo_id: str) -> str:
        """Builds the ``hf-mount``-style id from a repo_type and repo_id.

        Examples:
            ``('model', 'openai/gpt2')`` -> ``'openai/gpt2'``
            ``('dataset', 'ns/ds')``     -> ``'datasets/ns/ds'``
            ``('space', 'ns/app')``      -> ``'spaces/ns/app'``
        """
        return f'{cls._REPO_TYPE_TO_URL_PREFIX.get(repo_type, "")}{repo_id}'

    @classmethod
    def strip_repo_type_prefix(cls, hf_id: str) -> str:
        """Returns the bare ``ns/name`` from a prefixed id.

        Inverse of :meth:`hf_id_from_repo_parts` for any recognized prefix;
        returns ``hf_id`` unchanged for model ids (which have no prefix).
        """
        for prefix in cls._REPO_TYPE_TO_URL_PREFIX.values():
            if prefix and hf_id.startswith(prefix):
                return hf_id[len(prefix):]
        return hf_id

    def __init__(self,
                 name: str,
                 source: Optional[SourceType],
                 region: Optional[str] = None,
                 is_sky_managed: Optional[bool] = None,
                 sync_on_reconstruction: Optional[bool] = True,
                 _bucket_sub_path: Optional[str] = None):
        # Classify bucket vs repo up front so we can dispatch in _validate /
        # validate_name / initialize. ``_repo_type`` is None for buckets and
        # one of the keys of ``_REPO_TYPE_TO_URL_PREFIX`` for repos.
        self._repo_type: Optional[str] = None
        self._revision: Optional[str] = None
        # ``_hf_id`` is the identifier passed to ``hf-mount`` (bucket id for
        # buckets; for repos it's e.g. ``datasets/ns/name`` - matching the
        # argument format ``hf-mount repo`` expects).
        self._hf_id: str = ''
        # HF Buckets/repos do not have user-selectable regions.
        super().__init__(name, source, region, is_sky_managed,
                         sync_on_reconstruction, _bucket_sub_path)

    @property
    def is_repo(self) -> bool:
        return self._repo_type is not None

    def _classify(self) -> None:
        """Populates ``_repo_type``/``_revision``/``_hf_id`` from source/name.

        For repo mode, ``self.name`` must either be unset or match the
        canonical HF identifier derived from the source URL - mismatches
        are refused so misconfigurations fail loud.
        """
        if isinstance(self.source, str) and self.source.startswith(
                huggingface.HF_URL_PREFIX) and not self.source.startswith(
                    huggingface.HF_BUCKETS_URL_PREFIX):
            repo_type, repo_id, revision, sub_path = (
                data_utils.split_hf_repo_path(self.source))
            if sub_path and getattr(self, '_bucket_sub_path', None) is None:
                self._bucket_sub_path = sub_path
            self._repo_type = repo_type
            self._revision = revision
            self._hf_id = self.hf_id_from_repo_parts(repo_type, repo_id)
            if not self.name:
                self.name = self._hf_id
            elif self.name != self._hf_id:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageSpecError(
                        f'Hugging Face {self._repo_type} source '
                        f'{self.source!r} does not match storage name '
                        f'{self.name!r}. Expected name to be '
                        f'{self._hf_id!r} (or leave it unset).')
        else:
            # Bucket mode: ``self.name`` is the bucket id (ns/bucket).
            self._repo_type = None
            self._hf_id = self.name

    def _validate(self) -> None:
        self._classify()
        if self.source is not None and isinstance(self.source, str):
            if self.is_repo:
                # Repo mode: source must be an hf:// URL matching the id.
                if not data_utils.is_hf_path(self.source):
                    raise exceptions.StorageSpecError(
                        f'Hugging Face repo source {self.source!r} must start '
                        f'with "{huggingface.HF_URL_PREFIX}".')
                # Repo mounts are always read-only; COPY-mode upload isn't
                # meaningful for a repo source.
            elif data_utils.is_hf_bucket_path(self.source):
                source_bucket_id, sub_path = data_utils.split_hf_path(
                    self.source)
                if sub_path and getattr(self, '_bucket_sub_path', None) is None:
                    self._bucket_sub_path = sub_path
                if self.name != source_bucket_id:
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.StorageSpecError(
                            f'HF bucket is specified as path '
                            f'({self.source}); storage name '
                            f'({self.name!r}) must match the bucket id '
                            f'({source_bucket_id!r}).')
            elif re.search(r'^\w+://', self.source):
                # A non-HF cloud URI; we don't support cross-cloud upload into
                # HF Buckets in v1 (server-side Xet copy is HF<->HF only).
                raise NotImplementedError(
                    f'Moving data from {self.source} to a Hugging Face Bucket '
                    'is not supported yet. Please download the data locally '
                    'first, or use `hf buckets sync`.')
            # Otherwise treat the source as a local path (validated by
            # Storage._validate_source).

        # Validate name.
        self.name = self.validate_name(self.name, repo_type=self._repo_type)

        if self.is_repo:
            try:
                huggingface.huggingface_hub.load_module()
            except ImportError as e:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.ResourcesUnavailableError(str(e)) from e
        elif not storage_lib.hf_storage_cloud_enabled(huggingface.NAME):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.ResourcesUnavailableError(
                    'Storage \'store: hf\' specified, but Hugging Face '
                    'credentials are not configured. Run `sky check` and set '
                    'HF_TOKEN or run `hf auth login` to authenticate.')

    @classmethod
    def validate_name(cls, name: str, repo_type: Optional[str] = None) -> str:
        """Validates a Hugging Face bucket or repo identifier.

        Bucket ids are ``<namespace>/<bucket-name>``. Repo ids may additionally
        be prefixed with ``datasets/`` or ``spaces/``.
        """

        def _raise(msg: str):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageNameError(msg)

        if not isinstance(name, str) or not name:
            _raise('Hugging Face name must be specified in the form '
                   '"<namespace>/<name>" (e.g. "my-user/my-bucket").')

        # Accept full handles for convenience.
        if name.startswith(huggingface.HF_BUCKETS_URL_PREFIX):
            name = name[len(huggingface.HF_BUCKETS_URL_PREFIX):]
        elif name.startswith(huggingface.HF_URL_PREFIX):
            name = name[len(huggingface.HF_URL_PREFIX):]

        # For repo mode, strip the type prefix matching the declared
        # ``repo_type`` before validating segments. Prefixes belonging to
        # *other* repo types (e.g. ``datasets/`` with ``repo_type='space'``)
        # are not stripped, so such mismatches fall through to the segment
        # count check and raise.
        core = name
        if repo_type:
            expected_prefix = cls._REPO_TYPE_TO_URL_PREFIX.get(repo_type, '')
            if expected_prefix and core.startswith(expected_prefix):
                core = core[len(expected_prefix):]

        parts = core.split('/')
        if len(parts) != 2 or not parts[0] or not parts[1]:
            kind = 'repo' if repo_type else 'bucket'
            _raise(
                f'Invalid Hugging Face {kind} name {name!r}. Expected format: '
                '"<namespace>/<name>".')

        for segment_kind, segment in (('namespace', parts[0]), ('name',
                                                                parts[1])):
            if not cls._NAME_PATTERN.match(segment):
                kind = 'repo' if repo_type else 'bucket'
                _raise(f'Invalid Hugging Face {kind} {segment_kind} '
                       f'{segment!r}: must match '
                       f'{cls._NAME_PATTERN.pattern} (letters, digits, '
                       '".", "_", "-"; must not start with "." or "-").')
        return name

    def initialize(self) -> None:
        """Initializes the HF bucket (creating if needed) or the repo handle.

        Raises:
            StorageBucketCreateError: If bucket creation fails.
            StorageBucketGetError: If fetching an existing bucket/repo fails.
            StorageInitError: If the HF SDK is not available or the user is
              not authenticated (for private repos / any bucket).
        """
        token = huggingface.get_token()
        # For public repos, authentication is optional.
        if not token and not self.is_repo:
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageInitError(
                    'Hugging Face token not found. Set HF_TOKEN or run '
                    '`hf auth login`.')
        self._token = token
        self._api = huggingface.api()
        if self.is_repo:
            # Repos are never sky-managed; we only fetch metadata to verify
            # the repo exists and is accessible.
            self.bucket = self._get_repo_info()
            if self.is_sky_managed is None:
                self.is_sky_managed = False
        else:
            self.bucket, is_new_bucket = self._get_bucket()
            if self.is_sky_managed is None:
                self.is_sky_managed = is_new_bucket

    def _get_repo_info(self) -> StorageHandle:
        """Fetches repo metadata; raises if the repo is missing/inaccessible."""
        errors = huggingface.hf_hub_errors()
        assert self._repo_type is not None
        try:
            # ``HfApi.repo_info`` takes the raw ``ns/name`` id plus a
            # ``repo_type`` kwarg ("model", "dataset", or "space").
            repo_id = self.strip_repo_type_prefix(self._hf_id)
            return self._api.repo_info(repo_id=repo_id,
                                       repo_type=self._repo_type,
                                       revision=self._revision,
                                       token=self._token)
        except Exception as e:  # pylint: disable=broad-except
            not_found_types = tuple(cls for cls in (
                getattr(errors, 'RepositoryNotFoundError', None),
                getattr(errors, 'GatedRepoError', None),
                getattr(errors, 'RevisionNotFoundError', None),
            ) if cls is not None)
            if isinstance(e, not_found_types):
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketGetError(
                        f'Hugging Face {self._repo_type} '
                        f'{self._hf_id!r} not found or inaccessible: '
                        f'{e}') from e
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketGetError(
                    f'Failed to connect to Hugging Face {self._repo_type} '
                    f'{self._hf_id!r}: {e}') from e

    def _get_bucket(self) -> Tuple[StorageHandle, bool]:
        """Gets the bucket, creating it if needed and allowed.

        Returns:
            (bucket_info, is_new_bucket)
        """
        errors = huggingface.hf_hub_errors()
        try:
            info = self._api.bucket_info(self.name, token=self._token)
        except Exception as e:  # pylint: disable=broad-except
            not_found_types = tuple(cls for cls in (
                getattr(errors, 'RepositoryNotFoundError', None),
                getattr(errors, 'EntryNotFoundError', None),
            ) if cls is not None)
            is_not_found = isinstance(e, not_found_types)
            # ``HfHubHTTPError`` exposes ``.response.status_code``; treat 404
            # and 403/401 separately.
            status_code = None
            response = getattr(e, 'response', None)
            if response is not None:
                status_code = getattr(response, 'status_code', None)
            if status_code == 404:
                is_not_found = True

            if is_not_found:
                if isinstance(self.source, str) and data_utils.is_hf_path(
                        self.source):
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.StorageBucketGetError(
                            'Attempted to connect to a non-existent HF '
                            f'bucket as a source: {self.source}') from e
                if self.sync_on_reconstruction:
                    info = self._create_hf_bucket(self.name)
                    return info, True
                raise exceptions.StorageExternalDeletionError(
                    'Attempted to fetch a non-existent HF bucket: '
                    f'{self.name}') from e
            if status_code in (401, 403):
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketGetError(
                        storage_lib.HF_BUCKET_FAIL_TO_CONNECT_MESSAGE.format(
                            name=self.name)) from e
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketGetError(
                    f'Failed to connect to HF bucket {self.name!r}: '
                    f'{e}') from e
        # Validate after the connection succeeded so StorageSpecError (e.g.
        # mounting an externally-created bucket without ``source:``) keeps
        # its actionable message instead of being wrapped as a connection
        # failure by the broad ``except`` above.
        self._validate_existing_bucket()
        return info, False

    def _create_hf_bucket(self, bucket_id: str) -> StorageHandle:
        """Creates an HF bucket with ``exist_ok=True`` for idempotency."""
        try:
            self._api.create_bucket(bucket_id, exist_ok=True, token=self._token)
            info = self._api.bucket_info(bucket_id, token=self._token)
        except Exception as e:  # pylint: disable=broad-except
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketCreateError(
                    f'Failed to create HF bucket {bucket_id!r}: {e}') from e
        storage_lib.logger.info(f'  {colorama.Style.DIM}Created HF bucket '
                                f'{bucket_id!r}{colorama.Style.RESET_ALL}')
        return info

    def upload(self) -> None:
        """Uploads source to the HF bucket.

        Supports:
            - A single local directory (syncs its contents into the bucket).
            - A list of local files/directories.
            - A pre-existing HF bucket URI (no-op).

        Repo-backed stores are read-only: ``upload()`` is a no-op iff the
        source is the corresponding ``hf://`` URL, and an error otherwise, so
        that misconfigurations (e.g. a local ``source`` paired with a repo
        URL) never silently discard data.
        """
        if self.is_repo:
            is_repo_source = (isinstance(self.source, str) and
                              data_utils.is_hf_path(self.source) and
                              not data_utils.is_hf_bucket_path(self.source))
            if self.source is None or is_repo_source:
                # No local data to upload; the source IS the HF repo.
                return
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageUploadError(
                    f'Cannot upload to Hugging Face {self._repo_type} '
                    f'{self._hf_id!r}: repos are read-only. Only Hugging Face '
                    'Buckets (hf://buckets/...) support uploads. Got '
                    f'source={self.source!r}.')
        try:
            if isinstance(self.source, list):
                self._sync_local_sources(self.source, create_dirs=True)
            elif self.source is not None:
                if data_utils.is_hf_bucket_path(self.source):
                    # Already an HF bucket; nothing to do.
                    return
                self._sync_local_sources([self.source])
        except exceptions.StorageUploadError:
            raise
        except Exception as e:  # pylint: disable=broad-except
            raise exceptions.StorageUploadError(
                f'Upload failed for store {self.name}') from e

    def _sync_local_sources(self,
                            source_path_list: List[str],
                            create_dirs: bool = False) -> None:
        """Uploads local files/directories to the HF bucket.

        Uses ``sync_bucket`` for directories (it only transfers changed files)
        and ``batch_bucket_files`` for individual files.
        """
        sub_path = self._bucket_sub_path or ''

        log_path = storage_lib.sky_logging.generate_tmp_logging_file_path(
            storage_lib.HF_STORAGE_LOG_FILE_NAME)
        dest = f'{huggingface.HF_BUCKETS_URL_PREFIX}{self.name}'
        if sub_path:
            dest = f'{dest}/{sub_path}'

        if len(source_path_list) > 1:
            source_message = f'{len(source_path_list)} paths'
        else:
            source_message = str(source_path_list[0])
        sync_path = f'{source_message} -> {dest}'

        with rich_utils.safe_status(
                ux_utils.spinner_message(f'Syncing {sync_path}',
                                         log_path=log_path)):
            # Classify all sources up front so we fail loud on any missing
            # path before kicking off uploads.
            dir_uploads: List[Tuple[str, str]] = []
            files_to_add: List[Tuple[str, str]] = []
            for raw_path in source_path_list:
                path = os.path.abspath(os.path.expanduser(str(raw_path)))
                if os.path.isdir(path):
                    dir_dest = dest
                    if create_dirs:
                        dir_dest = f'{dest}/{os.path.basename(path)}'
                    dir_uploads.append((path, dir_dest))
                elif os.path.isfile(path):
                    remote_name = os.path.basename(path)
                    if sub_path:
                        remote_name = f'{sub_path}/{remote_name}'
                    files_to_add.append((path, remote_name))
                else:
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.StorageUploadError(
                            f'Local source path does not exist: {path}')
            # ``sync_bucket`` is already parallel internally (the HF SDK
            # uploads files within a folder concurrently). A small pool
            # here overlaps I/O between distinct source dirs.
            if dir_uploads:
                max_workers = min(len(dir_uploads), 4)
                with concurrent.futures.ThreadPoolExecutor(
                        max_workers=max_workers) as pool:
                    futures = [
                        pool.submit(self._api.sync_bucket,
                                    p,
                                    d,
                                    token=self._token,
                                    quiet=True) for p, d in dir_uploads
                    ]
                    for future in concurrent.futures.as_completed(futures):
                        future.result()
            if files_to_add:
                self._api.batch_bucket_files(self.name,
                                             add=files_to_add,
                                             token=self._token)
        storage_lib.logger.info(
            ux_utils.finishing_message(f'Storage synced: {sync_path}',
                                       log_path))

    def delete(self) -> None:
        if self.is_repo:
            # Repos are external resources; nothing to delete.
            return
        if self._bucket_sub_path is not None and not self.is_sky_managed:
            return self._delete_sub_path()
        try:
            self._api.delete_bucket(self.name,
                                    missing_ok=True,
                                    token=self._token)
        except Exception as e:  # pylint: disable=broad-except
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketDeleteError(
                    f'Failed to delete HF bucket {self.name!r}: {e}') from e
        storage_lib.logger.info(f'{colorama.Fore.GREEN}Deleted HF bucket '
                                f'{self.name}.{colorama.Style.RESET_ALL}')

    def _delete_sub_path(self) -> None:
        assert self._bucket_sub_path is not None, 'bucket_sub_path is not set'
        assert not self.is_repo, 'sub-path delete is not supported for repos'
        sub_path = self._bucket_sub_path.rstrip('/')
        # Collect all files under the sub-path and delete them in one batch.
        try:
            entries = list(
                self._api.list_bucket_tree(self.name,
                                           prefix=sub_path,
                                           recursive=True,
                                           token=self._token))
            file_paths = [
                e.path for e in entries if getattr(e, 'type', 'file') == 'file'
            ]
            if file_paths:
                self._api.batch_bucket_files(self.name,
                                             delete=file_paths,
                                             token=self._token)
        except Exception as e:  # pylint: disable=broad-except
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketDeleteError(
                    f'Failed to delete objects under '
                    f'{self.name}/{sub_path}: {e}') from e
        storage_lib.logger.info(f'{colorama.Fore.GREEN}Deleted objects in HF '
                                f'bucket {self.name}/{sub_path}.'
                                f'{colorama.Style.RESET_ALL}')

    def get_handle(self) -> StorageHandle:
        return self.bucket

    def download_remote_dir(self, local_path: str) -> None:
        """Downloads the contents of the HF bucket/repo into ``local_path``.

        For buckets, uses ``sync_bucket``. For repos, uses
        ``snapshot_download``.
        """
        os.makedirs(os.path.expanduser(local_path), exist_ok=True)
        expanded = os.path.expanduser(local_path)
        if self.is_repo:
            # ``snapshot_download`` preserves repo-relative paths under
            # ``local_dir``. For a sub-path source we stage into a temp dir
            # and move only the sub-path contents so the caller receives
            # the contents (not a doubled ``<dest>/<sub_path>/...`` tree).
            repo_id = self.strip_repo_type_prefix(self._hf_id)
            if self._bucket_sub_path:
                sub_path = self._bucket_sub_path.rstrip('/')
                tmp_dir = tempfile.mkdtemp()
                try:
                    self._api.snapshot_download(
                        repo_id=repo_id,
                        repo_type=self._repo_type,
                        revision=self._revision,
                        local_dir=tmp_dir,
                        allow_patterns=[f'{sub_path}/*'],
                        token=self._token)
                    src_dir = os.path.join(tmp_dir, *sub_path.split('/'))
                    if os.path.isdir(src_dir):
                        for entry in os.listdir(src_dir):
                            shutil.move(os.path.join(src_dir, entry),
                                        os.path.join(expanded, entry))
                finally:
                    shutil.rmtree(tmp_dir, ignore_errors=True)
            else:
                self._api.snapshot_download(repo_id=repo_id,
                                            repo_type=self._repo_type,
                                            revision=self._revision,
                                            local_dir=expanded,
                                            allow_patterns=None,
                                            token=self._token)
            return
        src = f'{huggingface.HF_BUCKETS_URL_PREFIX}{self.name}'
        if self._bucket_sub_path:
            src = f'{src}/{self._bucket_sub_path}'
        self._api.sync_bucket(src, expanded, token=self._token, quiet=True)

    def _download_file(self, remote_path: str, local_path: str) -> None:
        """Downloads a single file from the HF bucket/repo to ``local_path``."""
        if self.is_repo:
            self._api.hf_hub_download(
                repo_id=self.strip_repo_type_prefix(self._hf_id),
                repo_type=self._repo_type,
                revision=self._revision,
                filename=remote_path,
                local_dir=os.path.dirname(local_path) or '.',
                token=self._token)
            return
        # ``download_bucket_files`` accepts (remote_path, local_path) pairs.
        self._api.download_bucket_files(self.name,
                                        files=[(remote_path, local_path)],
                                        token=self._token)

    def _allow_patterns_for_sub_path(self) -> Optional[List[str]]:
        """Translates ``_bucket_sub_path`` into ``snapshot_download`` glob."""
        if not self._bucket_sub_path:
            return None
        return [f'{self._bucket_sub_path.rstrip("/")}/*']

    def mount_command(self,
                      mount_path: str,
                      read_only: bool = False,
                      hf_mount_args: Optional[List[str]] = None) -> str:
        """Returns a command to mount the HF bucket/repo at ``mount_path``.

        Uses the ``hf-mount`` NFS backend. The token file is expected to be
        available on the remote host (it is synced via
        ``huggingface.get_credential_file_mounts``).

        ``hf_mount_args`` are extra ``hf-mount`` flags (from
        ``config.mount.hf_mount_args``) forwarded verbatim to the daemon,
        e.g. ``--cache-dir`` / ``--cache-size`` / ``--advanced-writes``.
        """
        install_cmd = mounting_utils.get_hf_mount_install_cmd()
        mount_cmd = mounting_utils.get_hf_mount_cmd(
            hf_id=self._hf_id,
            mount_path=mount_path,
            _bucket_sub_path=self._bucket_sub_path,
            read_only=read_only,
            mode='repo' if self.is_repo else 'bucket',
            revision=self._revision,
            extra_args=hf_mount_args)
        version_check_cmd = mounting_utils.get_hf_mount_version_check_cmd()
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cmd, version_check_cmd)

    def mount_cached_command(self,
                             mount_path: str,
                             config: Optional[MountCachedConfig] = None) -> str:
        """Returns a command to mount the HF bucket/repo with local caching.

        ``hf-mount`` already provides an on-disk chunk cache (configured via
        its own ``--cache-dir`` / ``--cache-size`` flags; not currently
        exposed), so this reuses :meth:`mount_command`. Of
        ``MountCachedConfig``'s fields, only ``read_only`` maps cleanly to
        ``hf-mount`` (as ``--read-only`` on bucket mounts; repo mounts are
        always read-only). The rclone-specific fields (``transfers``,
        ``buffer_size``, ``vfs_*``) have no ``hf-mount`` equivalent and are
        silently ignored.
        """
        read_only = bool(config.read_only) if config is not None else False
        return self.mount_command(mount_path, read_only=read_only)
