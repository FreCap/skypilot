"""Provider-neutral OCI copy and digest verification."""

import dataclasses
import hashlib
import json
import re
import subprocess
import threading
import time
from typing import Any

from sky.container_images import models

_IMAGE_INDEX_MEDIA_TYPES = frozenset({
    'application/vnd.oci.image.index.v1+json',
    'application/vnd.docker.distribution.manifest.list.v2+json',
})
_IMAGE_MANIFEST_MEDIA_TYPES = frozenset({
    'application/vnd.oci.image.manifest.v1+json',
    'application/vnd.docker.distribution.manifest.v2+json',
})
_IMAGE_CONFIG_MEDIA_TYPES = frozenset({
    'application/vnd.oci.image.config.v1+json',
    'application/vnd.docker.container.image.v1+json',
})
_IMAGE_LAYER_MEDIA_TYPES = frozenset({
    'application/vnd.oci.image.layer.v1.tar',
    'application/vnd.oci.image.layer.v1.tar+gzip',
    'application/vnd.oci.image.layer.v1.tar+zstd',
    'application/vnd.docker.image.rootfs.diff.tar',
    'application/vnd.docker.image.rootfs.diff.tar.gzip',
})
_OCI_DIGEST_PATTERN = re.compile(
    r'^[a-z0-9]+(?:[+._-][a-z0-9]+)*:[A-Za-z0-9=_-]+$')
_OCI_MEDIA_TYPE_PATTERN = re.compile(
    r"^[A-Za-z0-9!#$%&'*+.^_`|~-]+/[A-Za-z0-9!#$%&'*+.^_`|~-]+$")
_MAX_OCI_DESCRIPTOR_STRING_LENGTH = 1024


@dataclasses.dataclass(frozen=True)
class OciDescriptor:
    """Validated distributable descriptor used by an exact copy."""

    media_type: str
    digest: str
    size: int


@dataclasses.dataclass(frozen=True)
class OciContentGraph:
    """One selected runnable manifest and every exact referenced blob."""

    source_root_digest: str
    source_root_media_type: str
    raw_source_root: bytes
    runtime_digest: str
    runtime_media_type: str
    raw_runtime_manifest: bytes
    platform: str
    config: OciDescriptor
    layers: tuple[OciDescriptor, ...]
    declared_size_bytes: int

    @property
    def manifest_units(self) -> int:
        return 1


@dataclasses.dataclass(frozen=True)
class OciInspectionLimits:
    """Bounds applied before any destination authority is acquired."""

    max_root_bytes: int = 4 * 1024 * 1024
    max_manifest_bytes: int = 4 * 1024 * 1024
    max_config_bytes: int = 16 * 1024 * 1024
    max_layers: int = 1024
    max_artifact_bytes: int = 100 * 1024 * 1024 * 1024


def _sha256(payload: bytes) -> str:
    return f'sha256:{hashlib.sha256(payload).hexdigest()}'


def _mapping(payload: bytes, subject: str) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (TypeError, ValueError):
        raise ValueError(f'{subject} is not valid JSON.') from None
    if not isinstance(value, dict):
        raise ValueError(f'{subject} must be a JSON object.')
    return value


def _descriptor(value: Any, subject: str, *,
                allowed_media_types: frozenset[str]) -> OciDescriptor:
    validated = OciClient._validate_descriptor(  # pylint: disable=protected-access
        value, subject)
    if validated['mediaType'] not in allowed_media_types:
        raise ValueError(
            f'{subject} uses unsupported or nondistributable media.')
    if validated.get('urls') or validated.get('data') is not None:
        raise ValueError(
            f'{subject} must not use external URLs or embedded data.')
    return OciDescriptor(
        media_type=str(validated['mediaType']),
        digest=models.validate_sha256_digest(str(validated['digest']),
                                             f'{subject} digest'),
        size=int(validated['size']),
    )


def _config_platform(payload: bytes) -> str:
    value = _mapping(payload, 'OCI image config')
    operating_system = value.get('os')
    architecture = value.get('architecture')
    if not isinstance(operating_system, str) or not isinstance(
            architecture, str):
        raise ValueError('OCI image config has no runtime platform.')
    components = [operating_system, architecture]
    variant = value.get('variant')
    if variant is not None:
        if not isinstance(variant, str):
            raise ValueError('OCI image config variant must be a string.')
        components.append(variant)
    return models.validate_oci_platform('/'.join(components),
                                        'OCI image config platform')


def build_content_graph(
    *,
    raw_root: bytes,
    expected_root_digest: str,
    requested_platform: str,
    fetch_manifest: Any,
    fetch_blob: Any,
    limits: OciInspectionLimits,
) -> OciContentGraph:
    """Selects and proves one runnable child without destination access.

    ``fetch_manifest`` receives a digest and returns exact raw bytes.
    ``fetch_blob`` does the same for the selected config. Layer payloads are
    streamed and hash-checked later during copy, but their descriptors and
    aggregate declared size are closed here.
    """
    expected_root_digest = models.validate_sha256_digest(
        expected_root_digest, 'OCI source root digest')
    requested_platform = models.validate_oci_platform(requested_platform,
                                                      'Requested platform')
    if len(raw_root) > limits.max_root_bytes:
        raise ValueError('OCI source root exceeds the inspection size limit.')
    if _sha256(raw_root) != expected_root_digest:
        raise ValueError(
            'OCI source root bytes do not match the pinned digest.')
    root = _mapping(raw_root, 'OCI source root')
    root_media_type = root.get('mediaType')
    if root_media_type in _IMAGE_MANIFEST_MEDIA_TYPES:
        runtime_digest = expected_root_digest
        runtime_media_type = str(root_media_type)
        raw_manifest = raw_root
    elif root_media_type in _IMAGE_INDEX_MEDIA_TYPES:
        if root.get('schemaVersion') != 2 or root.get(
                'artifactType') is not None:
            raise ValueError('OCI source index is not a runnable image index.')
        manifests = root.get('manifests')
        if not isinstance(manifests, list):
            raise ValueError('OCI source index has no manifest descriptors.')
        matches: list[OciDescriptor] = []
        for index, raw_descriptor in enumerate(manifests):
            descriptor = _descriptor(
                raw_descriptor,
                f'OCI index descriptor {index}',
                allowed_media_types=_IMAGE_MANIFEST_MEDIA_TYPES)
            platform = OciClient._platform_from_mapping(  # pylint: disable=protected-access
                raw_descriptor.get('platform'))
            if platform is None:
                raise ValueError('OCI image index child has no platform.')
            if platform == requested_platform:
                matches.append(descriptor)
        if len(matches) != 1:
            raise ValueError('OCI image index must contain exactly one '
                             'runnable child for the requested platform.')
        selected = matches[0]
        raw_manifest = fetch_manifest(selected.digest)
        if not isinstance(raw_manifest, bytes):
            raise TypeError('OCI manifest reader must return bytes.')
        if len(raw_manifest) != selected.size or _sha256(
                raw_manifest) != selected.digest:
            raise ValueError(
                'OCI selected child bytes do not match descriptor.')
        runtime_digest = selected.digest
        runtime_media_type = selected.media_type
    else:
        raise ValueError('OCI source is neither an image manifest nor index.')

    if len(raw_manifest) > limits.max_manifest_bytes:
        raise ValueError('OCI runtime manifest exceeds the size limit.')
    manifest = _mapping(raw_manifest, 'OCI runtime manifest')
    OciClient._validate_image_manifest(  # pylint: disable=protected-access
        manifest, runtime_media_type)
    config = _descriptor(manifest.get('config'),
                         'OCI image config descriptor',
                         allowed_media_types=_IMAGE_CONFIG_MEDIA_TYPES)
    if config.size > limits.max_config_bytes:
        raise ValueError('OCI image config exceeds the size limit.')
    config_bytes = fetch_blob(config.digest)
    if (not isinstance(config_bytes, bytes) or
            len(config_bytes) != config.size or
            _sha256(config_bytes) != config.digest):
        raise ValueError('OCI image config bytes do not match descriptor.')
    platform = _config_platform(config_bytes)
    if platform != requested_platform:
        raise ValueError('OCI image config does not match requested platform.')
    raw_layers = manifest.get('layers')
    if not isinstance(raw_layers, list) or len(raw_layers) > limits.max_layers:
        raise ValueError('OCI image layer count exceeds the configured limit.')
    layers = tuple(
        _descriptor(layer,
                    f'OCI image layer descriptor {index}',
                    allowed_media_types=_IMAGE_LAYER_MEDIA_TYPES)
        for index, layer in enumerate(raw_layers))
    declared_size = config.size + sum(layer.size for layer in layers)
    if declared_size > limits.max_artifact_bytes:
        raise ValueError(
            'OCI image exceeds the configured artifact byte limit.')
    return OciContentGraph(
        source_root_digest=expected_root_digest,
        source_root_media_type=str(root_media_type),
        raw_source_root=raw_root,
        runtime_digest=runtime_digest,
        runtime_media_type=runtime_media_type,
        raw_runtime_manifest=raw_manifest,
        platform=platform,
        config=config,
        layers=layers,
        declared_size_bytes=declared_size,
    )


class OciClient:
    """Runs bounded registry-to-registry operations through skopeo."""

    def __init__(self,
                 executable: str = 'skopeo',
                 copy_timeout_seconds: int = 3600,
                 metadata_timeout_seconds: int = 300) -> None:
        if copy_timeout_seconds <= 0 or metadata_timeout_seconds <= 0:
            raise ValueError('OCI operation timeouts must be positive.')
        self._executable = executable
        self._copy_timeout_seconds = copy_timeout_seconds
        self._metadata_timeout_seconds = metadata_timeout_seconds

    @staticmethod
    def _run(command: list[str], timeout_seconds: int,
             cancel_event: threading.Event | None) -> bytes:
        """Runs a command and terminates it promptly after lease loss."""
        if cancel_event is None:
            result = subprocess.run(command,
                                    check=True,
                                    capture_output=True,
                                    timeout=timeout_seconds)
            return result.stdout
        process = subprocess.Popen(command,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        deadline = time.monotonic() + timeout_seconds
        while True:
            if cancel_event.is_set():
                process.terminate()
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate()
                raise RuntimeError('OCI operation cancelled after lease loss.')
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                process.kill()
                stdout, stderr = process.communicate()
                raise subprocess.TimeoutExpired(command,
                                                timeout_seconds,
                                                output=stdout,
                                                stderr=stderr)
            try:
                stdout, stderr = process.communicate(
                    timeout=min(0.5, remaining))
            except subprocess.TimeoutExpired:
                continue
            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode,
                                                    command,
                                                    output=stdout,
                                                    stderr=stderr)
            return stdout

    def copy_all(self,
                 source: str,
                 destination: str,
                 source_authfile: str | None = None,
                 destination_authfile: str | None = None,
                 cancel_event: threading.Event | None = None) -> None:
        """Copies every runtime platform without placing secrets in argv.

        OCI artifacts reachable only through the separate Referrers API, such
        as signatures and SBOMs, are outside this runtime-image copy contract.
        """
        source = models.validate_oci_reference(source, 'OCI copy source')
        destination = models.validate_oci_reference(destination,
                                                    'OCI copy destination')
        command = [self._executable, 'copy', '--all', '--preserve-digests']
        if source_authfile is not None:
            command.extend(['--src-authfile', source_authfile])
        if destination_authfile is not None:
            command.extend(['--dest-authfile', destination_authfile])
        command.extend([f'docker://{source}', f'docker://{destination}'])
        self._run(command, self._copy_timeout_seconds, cancel_event)

    def _inspect_raw(self,
                     reference: str,
                     authfile: str | None = None,
                     cancel_event: threading.Event | None = None) -> bytes:
        """Returns the exact raw destination manifest or index bytes."""
        reference = models.validate_oci_reference(reference,
                                                  'OCI inspect reference')
        command = [self._executable, 'inspect', '--raw']
        if authfile is not None:
            command.extend(['--authfile', authfile])
        command.append(f'docker://{reference}')
        return self._run(command, self._metadata_timeout_seconds, cancel_event)

    def inspect_digest(self,
                       reference: str,
                       authfile: str | None = None,
                       cancel_event: threading.Event | None = None) -> str:
        """Computes the digest of the raw destination manifest or index."""
        manifest = self._inspect_raw(reference, authfile, cancel_event)
        return f'sha256:{hashlib.sha256(manifest).hexdigest()}'

    @staticmethod
    def _platform_from_mapping(platform: Any) -> str | None:
        if not isinstance(platform, dict):
            return None
        operating_system = platform.get('os')
        architecture = platform.get('architecture')
        if not isinstance(operating_system, str) or not isinstance(
                architecture, str):
            return None
        components = [operating_system, architecture]
        variant = platform.get('variant')
        if variant is not None:
            if not isinstance(variant, str):
                return None
            components.append(variant)
        return models.validate_oci_platform('/'.join(components),
                                            'Inspected OCI platform')

    @staticmethod
    def _repository(reference: str) -> str:
        repository, _ = models.split_digest(reference)
        last_slash = repository.rfind('/')
        last_colon = repository.rfind(':')
        if last_colon > last_slash:
            repository = repository[:last_colon]
        return repository

    @staticmethod
    def _decode_mapping(payload: bytes, subject: str) -> dict[str, Any]:
        try:
            decoded = json.loads(payload)
        except (TypeError, ValueError):
            raise ValueError(f'{subject} is invalid.') from None
        if not isinstance(decoded, dict):
            raise ValueError(f'{subject} is invalid.')
        return decoded

    @staticmethod
    def _validate_descriptor(descriptor: Any, subject: str) -> dict[str, Any]:
        """Validates the common OCI descriptor structure before routing it."""
        if not isinstance(descriptor, dict):
            raise ValueError(f'{subject} is invalid.')
        media_type = descriptor.get('mediaType')
        digest = descriptor.get('digest')
        size = descriptor.get('size')
        if (not isinstance(media_type, str) or not media_type or
                len(media_type) > _MAX_OCI_DESCRIPTOR_STRING_LENGTH or
                _OCI_MEDIA_TYPE_PATTERN.fullmatch(media_type) is None or
                not isinstance(digest, str) or
                len(digest) > _MAX_OCI_DESCRIPTOR_STRING_LENGTH or
                _OCI_DIGEST_PATTERN.fullmatch(digest) is None or
                not isinstance(size, int) or isinstance(size, bool) or
                size < 0 or size > (1 << 63) - 1):
            raise ValueError(f'{subject} is invalid.')
        artifact_type = descriptor.get('artifactType')
        if (artifact_type is not None and
            (not isinstance(artifact_type, str) or not artifact_type or
             len(artifact_type) > _MAX_OCI_DESCRIPTOR_STRING_LENGTH or
             _OCI_MEDIA_TYPE_PATTERN.fullmatch(artifact_type) is None)):
            raise ValueError(f'{subject} is invalid.')
        annotations = descriptor.get('annotations')
        if (annotations is not None and
            (not isinstance(annotations, dict) or
             any(not isinstance(key, str) or not isinstance(value, str)
                 for key, value in annotations.items()))):
            raise ValueError(f'{subject} is invalid.')
        urls = descriptor.get('urls')
        if (urls is not None and
            (not isinstance(urls, list) or
             any(not isinstance(url, str) for url in urls))):
            raise ValueError(f'{subject} is invalid.')
        if 'data' in descriptor and not isinstance(descriptor['data'], str):
            raise ValueError(f'{subject} is invalid.')
        return descriptor

    def _inspect_config_platform(
        self,
        reference: str,
        authfile: str | None,
        cancel_event: threading.Event | None,
    ) -> str:
        reference = models.validate_oci_reference(
            reference, 'OCI config inspect reference')
        command = [self._executable, 'inspect', '--config']
        if authfile is not None:
            command.extend(['--authfile', authfile])
        command.append(f'docker://{reference}')
        config = self._run(command, self._metadata_timeout_seconds,
                           cancel_event)
        config_payload = self._decode_mapping(config,
                                              'OCI image config metadata')
        platform = self._platform_from_mapping(config_payload)
        if platform is None:
            raise ValueError('OCI image config metadata has no runtime '
                             'platform.')
        return platform

    @staticmethod
    def _validate_image_manifest(
            payload: dict[str, Any],
            expected_media_type: str | None = None) -> None:
        """Rejects artifact manifests and malformed runnable images."""
        media_type = payload.get('mediaType')
        if media_type not in _IMAGE_MANIFEST_MEDIA_TYPES:
            raise ValueError('OCI content is not a runnable image manifest.')
        if expected_media_type is not None and media_type != expected_media_type:
            raise ValueError('OCI image descriptor media type does not match '
                             'its manifest.')
        if payload.get('artifactType') is not None:
            raise ValueError('OCI artifact manifests are not runnable images.')
        if payload.get('schemaVersion') != 2:
            raise ValueError('OCI image manifest must use schema version 2.')
        try:
            config = OciClient._validate_descriptor(
                payload.get('config'), 'OCI image config descriptor')
        except ValueError:
            raise ValueError('OCI image manifest has an invalid image config '
                             'descriptor.') from None
        if config.get('mediaType') not in _IMAGE_CONFIG_MEDIA_TYPES:
            raise ValueError('OCI image manifest has an invalid image config '
                             'descriptor.')
        models.validate_sha256_digest(config['digest'],
                                      'OCI image config digest')
        layers = payload.get('layers')
        if not isinstance(layers, list):
            raise ValueError('OCI image manifest has no layer descriptor list.')
        for layer in layers:
            try:
                layer = OciClient._validate_descriptor(
                    layer, 'OCI image layer descriptor')
            except ValueError:
                raise ValueError(
                    'OCI image manifest has an invalid layer descriptor.'
                ) from None
            if layer.get('mediaType') not in _IMAGE_LAYER_MEDIA_TYPES:
                raise ValueError(
                    'OCI image manifest has an invalid layer descriptor.')
            models.validate_sha256_digest(layer['digest'],
                                          'OCI image layer digest')

    def inspect_metadata(
        self,
        reference: str,
        authfile: str | None = None,
        cancel_event: threading.Event | None = None,
    ) -> models.MaterializationResult:
        """Inspects exact digest and a nonempty OCI runtime platform set."""
        manifest = self._inspect_raw(reference, authfile, cancel_event)
        digest = f'sha256:{hashlib.sha256(manifest).hexdigest()}'
        payload = self._decode_mapping(manifest, 'OCI manifest metadata')

        platforms: list[str] = []
        descriptors = payload.get('manifests')
        if isinstance(descriptors, list):
            if (payload.get('mediaType') not in _IMAGE_INDEX_MEDIA_TYPES or
                    payload.get('schemaVersion') != 2 or
                    payload.get('artifactType') is not None):
                raise ValueError('OCI image index metadata is invalid.')
            repository = self._repository(reference)
            for descriptor in descriptors:
                descriptor = self._validate_descriptor(
                    descriptor, 'OCI image index descriptor')
                descriptor_media_type = descriptor.get('mediaType')
                if descriptor_media_type not in _IMAGE_MANIFEST_MEDIA_TYPES:
                    # Non-runtime descriptors embedded in the index may be
                    # copied by --all, but cannot provide platform evidence.
                    # External subject referrers are not index children and are
                    # outside the runtime-image copy contract.
                    continue
                if descriptor.get('artifactType') is not None:
                    continue
                annotations = descriptor.get('annotations')
                if (isinstance(annotations, dict) and
                        annotations.get('vnd.docker.reference.type')
                        in ('attestation-manifest', 'signature')):
                    continue
                descriptor_digest = descriptor.get('digest')
                platform = self._platform_from_mapping(
                    descriptor.get('platform'))
                if platform is None:
                    raise ValueError(
                        'OCI image descriptor has no runtime platform.')
                assert isinstance(descriptor_digest, str)
                descriptor_digest = models.validate_sha256_digest(
                    descriptor_digest, 'OCI image descriptor digest')
                child_reference = f'{repository}@{descriptor_digest}'
                child_manifest = self._inspect_raw(child_reference, authfile,
                                                   cancel_event)
                if len(child_manifest) != descriptor['size']:
                    raise ValueError('OCI image descriptor size does not '
                                     'match its manifest bytes.')
                actual_child_digest = (
                    f'sha256:{hashlib.sha256(child_manifest).hexdigest()}')
                if actual_child_digest != descriptor_digest:
                    raise ValueError('OCI image descriptor digest does not '
                                     'match its manifest bytes.')
                child_payload = self._decode_mapping(
                    child_manifest, 'OCI child image manifest metadata')
                if child_payload.get('artifactType') is not None:
                    # OCI 1.1 artifact manifests can use the image-manifest
                    # media type. Their explicit artifactType distinguishes
                    # them without weakening malformed-image validation.
                    continue
                self._validate_image_manifest(child_payload,
                                              descriptor_media_type)
                config_platform = self._inspect_config_platform(
                    child_reference, authfile, cancel_event)
                if config_platform != platform:
                    raise ValueError('OCI image index platform does not match '
                                     'the child image config.')
                if platform not in platforms:
                    platforms.append(platform)
        else:
            self._validate_image_manifest(payload)
            platforms.append(
                self._inspect_config_platform(reference, authfile,
                                              cancel_event))
        return models.MaterializationResult(digest=digest,
                                            platforms=tuple(platforms))

    def copy_and_verify(
        self,
        source: str,
        destination: str,
        expected_digest: str,
        source_authfile: str | None = None,
        destination_authfile: str | None = None,
        cancel_event: threading.Event | None = None
    ) -> models.MaterializationResult:
        """Idempotently copies all platforms and verifies the exact digest."""
        source = models.validate_oci_reference(source, 'OCI copy source')
        destination = models.validate_oci_reference(destination,
                                                    'OCI copy destination')
        repository, destination_digest = models.split_digest(destination)
        expected_digest = models.validate_sha256_digest(expected_digest,
                                                        'OCI expected digest')
        if destination_digest != expected_digest:
            raise ValueError('OCI copy destination must be pinned to the '
                             'expected digest.')
        # Registry pushes use a tag, while runtime identity and verification
        # remain digest-only. This deterministic tag is immutable by content
        # convention and never acts as a mutable release channel.
        write_reference = f'{repository}:{expected_digest.replace(":", "-", 1)}'
        write_reference = models.validate_oci_reference(
            write_reference, 'OCI copy write reference')

        def verify_destination() -> models.MaterializationResult:
            result = self.inspect_metadata(destination, destination_authfile,
                                           cancel_event)
            if result.digest != expected_digest:
                raise ValueError(
                    f'OCI digest verification failed for {destination!r}: '
                    f'expected {expected_digest}, got {result.digest}.')
            return result

        # A prior attempt may have committed the immutable write tag before
        # losing its lease or failing verification. Verify the digest first so
        # retries do not push the same tag and get rejected by registries with
        # tag immutability enabled.
        try:
            return verify_destination()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pass

        try:
            self.copy_all(source, write_reference, source_authfile,
                          destination_authfile, cancel_event)
        except Exception as copy_error:  # pylint: disable=broad-except
            # Registry clients can report a timeout or immutable-tag conflict
            # after the manifest was committed. Accept that ambiguous outcome
            # only when a fresh digest read proves the exact expected bytes.
            try:
                return verify_destination()
            except Exception as verification_error:  # pylint: disable=broad-except
                raise copy_error from verification_error
        return verify_destination()

    def delete(self,
               reference: str,
               authfile: str | None = None,
               cancel_event: threading.Event | None = None) -> None:
        """Deletes one digest-pinned regional manifest.

        Callers must obtain the target through the retention state machine.
        This primitive has no authority to select or delete canonical images.
        """
        reference = models.validate_oci_reference(reference,
                                                  'OCI delete reference')
        _, digest = models.split_digest(reference)
        if digest is None:
            raise ValueError('OCI deletion requires a digest-pinned reference.')
        command = [self._executable, 'delete']
        if authfile is not None:
            command.extend(['--authfile', authfile])
        command.append(f'docker://{reference}')
        self._run(command, self._metadata_timeout_seconds, cancel_event)


# TODO(fcapponi): Run OciClient in a separately deployed, resource-bounded
# worker pool. API server request workers must only create durable intents and
# claim/complete leases, never execute this class or carry image bytes.
