"""File-mount upload routes and blob lifecycle helpers."""

import asyncio
import os
import pathlib
import re
import shutil
import time
import uuid
import zipfile

import aiofiles
import anyio
import fastapi
import starlette.requests

from sky import sky_logging
from sky.jobs import state as managed_job_state
from sky.schemas.api import responses
from sky.server import common
from sky.server import runtime_profile
from sky.server.blob import blob_storage as bs
from sky.server.requests import payloads
from sky.server.requests import requests as requests_lib
from sky.utils import asyncio_utils
from sky.utils import common_utils

logger = sky_logging.init_logger(__name__)

router = fastapi.APIRouter()

# Default expiration time for upload ids before cleanup.
_DEFAULT_UPLOAD_EXPIRATION_SECONDS = 60 * 60
# Key: (upload_id, user_hash), Value: the time when the upload id needs to be
# cleaned up, measured on the process-local monotonic clock.
upload_ids_to_cleanup: dict[tuple[str, str], float] = {}


async def cleanup_upload_ids():
    """Cleans up the temporary chunks uploaded by the client after a delay."""
    # Clean up the temporary chunks uploaded by the client after an hour. This
    # is to prevent stale chunks taking up space on the API server.
    while True:
        await asyncio.sleep(3600)
        current_time = time.monotonic()
        # We use list() to avoid modifying the dict while iterating over it.
        upload_ids_to_cleanup_list = list(upload_ids_to_cleanup.items())
        for (upload_id, user_hash), expire_time in upload_ids_to_cleanup_list:
            if current_time > expire_time:
                logger.info(f'Cleaning up upload id: {upload_id}')
                client_file_mounts_dir = (
                    common.API_SERVER_CLIENT_DIR.expanduser().resolve() /
                    user_hash / 'file_mounts')
                shutil.rmtree(client_file_mounts_dir / upload_id,
                              ignore_errors=True)
                (client_file_mounts_dir /
                 upload_id).with_suffix('.zip').unlink(missing_ok=True)
                upload_ids_to_cleanup.pop((upload_id, user_hash))


async def cleanup_unreferenced_file_mounts():
    """Delete file mounts not referenced by any active request."""

    # Synced cleanup for each directory, runs in asyncio.to_thread to avoid
    # blocking the event loop.
    def _do_cleanup():
        storage = bs.get_blob_storage()

        with storage.gc_lock() as should_run:
            if not should_run:
                logger.debug('Another replica is running blob GC, skipping')
                return

            # A blob is kept alive by either an active API request (e.g. the
            # submit request that is still running) or a non-terminal managed
            # job that was started from it.
            active_blob_ids = (
                requests_lib.get_active_file_mounts_blob_ids() |
                managed_job_state.get_active_file_mounts_blob_ids())
            grace_cutoff = time.time() - bs.GC_GRACE_SECONDS

            for user_id in storage.list_users():
                try:
                    for blob_id, mtime in storage.list_blob_ids(user_id):
                        if (blob_id not in active_blob_ids and
                                mtime < grace_cutoff):
                            logger.info(f'GC: removing unreferenced blob '
                                        f'{blob_id} for user {user_id}')
                            storage.delete_blob(user_id, blob_id)
                    storage.release_stale_uploads(user_id)
                except Exception as e:  # pylint: disable=broad-except
                    logger.error(f'Error cleaning filemounts dir: {user_id}: '
                                 f'{common_utils.format_exception(e)}')

    while True:
        await asyncio.sleep(3600)  # Run every hour
        try:
            await asyncio.to_thread(_do_cleanup)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Error in cleanup_unreferenced_file_mounts: '
                         f'{common_utils.format_exception(e)}')


async def _prepare_client_mount_dir(user_hash: str,
                                    request: fastapi.Request) -> pathlib.Path:
    # For anonymous access, use the user hash from client
    user_id = user_hash
    if request.state.auth_user is not None:
        # Otherwise, the authenticated identity should be used.
        user_id = request.state.auth_user.id

    client_file_mounts_dir = (
        common.API_SERVER_CLIENT_DIR.expanduser().resolve() / user_id /
        'file_mounts')
    await anyio.Path(client_file_mounts_dir).mkdir(parents=True, exist_ok=True)
    return client_file_mounts_dir


async def _receive_and_assemble_chunks(
    base_dir: pathlib.Path,
    zip_name: str,
    request: fastapi.Request,
    chunk_index: int,
    total_chunks: int,
    extract: bool = True,
    assemble: bool = True,
) -> payloads.UploadZipFileResponse | None:
    """Receive chunks, optionally assemble into a zip file, and extract.

    Returns:
        None if the upload is completed,
        A response to tell the client to upload more chunks otherwise.
    """
    if extract and not assemble:
        raise ValueError('extract=True requires assemble=True')
    # Field _body would be set if the request body has been received, fail fast
    # to surface potential memory issues, i.e. catch the issue in our smoke
    # test.
    # pylint: disable=protected-access
    if hasattr(request, '_body'):
        raise fastapi.HTTPException(
            status_code=500,
            detail='Upload request body should not be received before streaming'
        )
    # TODO(SKY-1271): We need to double check security of uploading zip file.
    # Check chunk_index to be a valid integer
    if chunk_index < 0 or chunk_index >= total_chunks:
        raise ValueError(
            f'Invalid chunk_index: {chunk_index}. Please use a valid integer.')
    # Check total_chunks to be a valid integer
    if total_chunks < 1:
        raise ValueError(
            f'Invalid total_chunks: {total_chunks}. Please use a valid integer.'
        )
    # Write chunk to a unique private path first, so concurrent uploads for
    # a same blob does not interleave with each other.
    if total_chunks == 1:
        await anyio.Path(base_dir).mkdir(parents=True, exist_ok=True)
        final_path = base_dir / f'{zip_name}.zip'
        zip_file_path = base_dir / f'{zip_name}.tmp.{uuid.uuid4().hex}.zip'
    else:
        chunk_dir = base_dir / zip_name
        await anyio.Path(chunk_dir).mkdir(parents=True, exist_ok=True)
        final_path = chunk_dir / f'part{chunk_index}'
        zip_file_path = chunk_dir / f'part{chunk_index}.tmp.{uuid.uuid4().hex}'

    try:
        async with aiofiles.open(zip_file_path, 'wb') as f:
            async for chunk in request.stream():
                await f.write(chunk)
    except starlette.requests.ClientDisconnect as e:
        # Client disconnected, remove the zip file.
        zip_file_path.unlink(missing_ok=True)
        raise fastapi.HTTPException(
            status_code=400,
            detail='Client disconnected, please try again.') from e
    except Exception as e:
        logger.error(f'Error uploading zip file: {zip_file_path}')
        # Client disconnected, remove the zip file.
        zip_file_path.unlink(missing_ok=True)
        raise fastapi.HTTPException(
            status_code=500,
            detail=('Error uploading zip file: '
                    f'{common_utils.format_exception(e)}'))

    def get_missing_chunks(total_chunks: int) -> set[str]:
        existing = set()
        for p in chunk_dir.glob('part*'):
            # Filter out tmp files (e.g. ``part0.tmp.<hex>``) that may
            # belong to in-flight concurrent writers.  Only renamed
            # final names ``part{N}`` count toward completion.
            name = p.name
            suffix = name[len('part'):] if name.startswith('part') else ''
            if suffix.isdigit():
                existing.add(name)
        return set(f'part{i}' for i in range(total_chunks)) - existing

    # Rename the writer-unique tmp file to its final name.
    os.rename(str(zip_file_path), str(final_path))
    zip_file_path = final_path

    if total_chunks > 1:
        missing_chunks = get_missing_chunks(total_chunks)
        if missing_chunks:
            return payloads.UploadZipFileResponse(
                status=responses.UploadStatus.UPLOADING.value,
                missing_chunks=missing_chunks)
    logger.info(f'Uploaded chunk: {zip_file_path}')
    if assemble:
        await _finalize_chunked_upload(base_dir=base_dir,
                                       zip_name=zip_name,
                                       total_chunks=total_chunks,
                                       extract=extract)
    return None


async def _finalize_chunked_upload(
    base_dir: pathlib.Path,
    zip_name: str,
    total_chunks: int,
    extract: bool,
) -> None:
    """Assemble parts into a single zip and optionally extract it."""
    if total_chunks > 1:
        chunk_dir = base_dir / zip_name
        zip_file_path = base_dir / f'{zip_name}.zip'
        async with aiofiles.open(zip_file_path, 'wb') as zip_file:
            for chunk in range(total_chunks):
                async with aiofiles.open(chunk_dir / f'part{chunk}', 'rb') as f:
                    while True:
                        # Use 64KB buffer to avoid memory overflow, same
                        # size as shutil.copyfileobj.
                        data = await f.read(64 * 1024)
                        if not data:
                            break
                        await zip_file.write(data)
    else:
        # ``{base_dir}/{zip_name}.zip`` (renamed by the receive step).
        zip_file_path = base_dir / f'{zip_name}.zip'

    if extract:
        await unzip_file(zip_file_path, base_dir)
    if total_chunks > 1:
        await asyncio.to_thread(shutil.rmtree, base_dir / zip_name)


# TODO(aylei): for backward compatibility, remove after v0.14.0
@router.post('/upload')
async def upload_zip_file(request: fastapi.Request, user_hash: str,
                          upload_id: str, chunk_index: int,
                          total_chunks: int) -> payloads.UploadZipFileResponse:
    """Uploads a zip file to the API server.

    This endpoints can be called multiple times for the same upload_id with
    different chunk_index. The server will merge the chunks and unzip the file
    when all chunks are uploaded.

    This implementation is simplified and may need to be improved in the future,
    e.g., adopting S3-style multipart upload.

    Args:
        user_hash: The user hash.
        upload_id: The upload id, a valid SkyPilot run_timestamp appended with 8
            hex characters, e.g. 'sky-2025-01-17-09-10-13-933602-35d31c22'.
        chunk_index: The chunk index, starting from 0.
        total_chunks: The total number of chunks.
    """
    runtime_profile.reject_local_artifact_operation('file-mount upload')
    # Check upload_id to be a valid SkyPilot run_timestamp appended with 8 hex
    # characters, e.g. 'sky-2025-01-17-09-10-13-933602-35d31c22'.
    if not re.match(
            r'sky-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}-'
            r'[0-9]{2}-[0-9]{6}-[0-9a-f]{8}$', upload_id):
        raise ValueError(
            f'Invalid upload_id: {upload_id}. Please use a valid uuid.')

    # Add only validated upload ids to the cleanup list. The cleanup daemon
    # uses this value as a path component when removing expired uploads.
    upload_ids_to_cleanup[(upload_id,
                           user_hash)] = (time.monotonic() +
                                          _DEFAULT_UPLOAD_EXPIRATION_SECONDS)

    base_dir = await _prepare_client_mount_dir(user_hash, request)
    missing_chunks = await _receive_and_assemble_chunks(
        base_dir=base_dir,
        zip_name=upload_id,
        request=request,
        chunk_index=chunk_index,
        total_chunks=total_chunks)
    if missing_chunks is not None:
        return missing_chunks
    return payloads.UploadZipFileResponse(
        status=responses.UploadStatus.COMPLETED.value)


@router.get('/upload_v2/blob')
async def check_blob_exists(request: fastapi.Request, user_hash: str,
                            blob_id: str) -> dict[str, bool]:
    """Check if a file mount blob already exists."""
    runtime_profile.reject_local_artifact_operation('file-mount blob lookup')
    if not re.match(r'^[0-9a-f]{64}$', blob_id):
        raise fastapi.HTTPException(status_code=400,
                                    detail=f'Invalid blob_id: {blob_id}')
    user_id = user_hash
    if request.state.auth_user is not None:
        user_id = request.state.auth_user.id
    exists = await bs.get_blob_storage().blob_exists(user_id, blob_id)
    return {'exists': exists}


@asyncio_utils.shield
async def _finalize_blob_upload(storage: bs.BlobStorage, user_id: str,
                                upload_id: str, target_dir: pathlib.Path,
                                staging_dir: pathlib.Path,
                                total_chunks: int) -> None:
    """Finalize and publish a blob without releasing its lock on cancellation."""
    async with storage.acquire_upload_lock(user_id, upload_id):
        if await anyio.Path(target_dir).exists():
            return
        if storage.assemble_on_upload() or storage.extract_on_upload():
            await _finalize_chunked_upload(base_dir=staging_dir,
                                           zip_name='staging',
                                           total_chunks=total_chunks,
                                           extract=storage.extract_on_upload())
        await storage.store_blob(user_id, upload_id, staging_dir)
        logger.info(f'Uploaded blob: {target_dir}')


@router.post('/upload_v2')
async def upload_blob(request: fastapi.Request, user_hash: str, upload_id: str,
                      chunk_index: int,
                      total_chunks: int) -> payloads.UploadZipFileResponse:
    """Upload a file mount blob (chunked).

    Unlike /upload, this endpoint receives chunks, assembles and extracts
    into a staging directory, then atomically renames to a shared extraction
    directory (blobs/{upload_id}/) so all requests can reuse it.
    """
    runtime_profile.reject_local_artifact_operation('file-mount blob upload')
    if not re.match(r'^[0-9a-f]{64}$', upload_id):
        raise fastapi.HTTPException(
            status_code=400, detail=f'Invalid upload_id for v2: {upload_id}')

    user_id = user_hash
    if request.state.auth_user is not None:
        user_id = request.state.auth_user.id

    storage = bs.get_blob_storage()

    # Ensure blobs directory exists.
    await anyio.Path(storage.blobs_dir(user_id)).mkdir(parents=True,
                                                       exist_ok=True)
    target_dir = storage.get_target_dir(user_id, upload_id)

    if target_dir.exists():
        return payloads.UploadZipFileResponse(
            status=responses.UploadStatus.COMPLETED.value)

    # Receive the chunk WITHOUT holding the upload lock.  Each chunk
    # writes to a writer-unique tmp file, then atomic-renames to its
    # final ``part{N}`` name, so concurrent chunk POSTs (parallel
    # workers, retries, or two clients racing on the same content-
    # hashed blob_id) don't need lock coordination here.
    # Note that we skip assemble and extract here since cocurrent chunk
    # uploads will race, and we do finalize with the upload_lock instead.
    staging_dir = storage.get_staging_dir(user_id, upload_id)
    result = await _receive_and_assemble_chunks(base_dir=staging_dir,
                                                zip_name='staging',
                                                request=request,
                                                chunk_index=chunk_index,
                                                total_chunks=total_chunks,
                                                extract=False,
                                                assemble=False)
    if result is not None:
        return result

    # All chunks present: finalize and publish under the upload lock so exactly
    # one caller does the assemble/extract/rename. Keep the complete critical
    # section shielded because executor-backed extraction continues running if
    # the request is cancelled.
    await _finalize_blob_upload(storage, user_id, upload_id, target_dir,
                                staging_dir, total_chunks)
    return payloads.UploadZipFileResponse(
        status=responses.UploadStatus.COMPLETED.value)


def is_relative_to(path: pathlib.Path, parent: pathlib.Path) -> bool:
    """Checks if path is a subpath of parent."""
    try:
        # We cannot use is_relative_to, as it is only added after 3.9.
        path.relative_to(parent)
        return True
    except ValueError:
        return False


async def unzip_file(zip_file_path: pathlib.Path,
                     client_file_mounts_dir: pathlib.Path) -> None:
    """Unzips a zip file without blocking the event loop."""

    def _do_unzip() -> None:
        try:
            extract_root = client_file_mounts_dir.resolve()
            with zipfile.ZipFile(zip_file_path, 'r') as zipf:
                for member in zipf.infolist():
                    # Determine the new path
                    original_path = os.path.normpath(member.filename)
                    new_path = extract_root / original_path.lstrip('/')

                    # Security check: ensure extracted path stays within target
                    # directory to prevent Zip Slip attacks (path traversal via
                    # malicious "../" sequences in archive member names).
                    resolved_path = new_path.resolve()
                    if not is_relative_to(resolved_path, extract_root):
                        raise ValueError(
                            f'Zip member {member.filename!r} would extract '
                            'outside target directory. Aborted.')

                    if (member.external_attr >> 28) == 0xA:
                        # Symlink. Read the target path and create a symlink.
                        new_path.parent.mkdir(parents=True, exist_ok=True)
                        target = zipf.read(member).decode()
                        if os.path.isabs(target):
                            raise ValueError(
                                f'Symlink target {target!r} must be relative. '
                                'Aborted.')
                        # Since target is a relative path, we need to check that
                        # it is under `extract_root` for security.
                        full_target_path = (new_path.parent / target).resolve()
                        if not is_relative_to(full_target_path, extract_root):
                            raise ValueError(
                                f'Symlink target {target} leads to a '
                                'file not in userspace. Aborted.')

                        if new_path.exists() or new_path.is_symlink():
                            new_path.unlink(missing_ok=True)
                        new_path.symlink_to(
                            target,
                            target_is_directory=member.filename.endswith('/'))
                        continue

                    # Handle directories
                    if member.filename.endswith('/'):
                        new_path.mkdir(parents=True, exist_ok=True)
                        continue

                    # Handle files
                    new_path.parent.mkdir(parents=True, exist_ok=True)
                    with zipf.open(member) as member_file, new_path.open(
                            'wb') as f:
                        # Use shutil.copyfileobj to copy files in chunks,
                        # so it does not load the entire file into memory.
                        shutil.copyfileobj(member_file, f)
        except zipfile.BadZipFile as e:
            logger.error(f'Bad zip file: {zip_file_path}')
            raise fastapi.HTTPException(
                status_code=400,
                detail=f'Invalid zip file: {common_utils.format_exception(e)}')
        except Exception as e:
            logger.error(f'Error unzipping file: {zip_file_path}')
            raise fastapi.HTTPException(
                status_code=500,
                detail=(f'Error unzipping file: '
                        f'{common_utils.format_exception(e)}'))
        finally:
            # Cleanup the temporary file regardless of
            # success/failure handling above
            zip_file_path.unlink(missing_ok=True)

    await asyncio.to_thread(_do_unzip)
