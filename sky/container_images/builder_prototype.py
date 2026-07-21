"""Disabled maintainer harness for the managed image-builder design.

This module is deliberately absent from the public API, SDK, task schema, and
Dashboard.  It validates the post-v0 builder seam with an isolated PostgreSQL
schema, an S3-compatible content store, and an external BuildKit daemon.  It is
executable only when ``SKYPILOT_IMAGE_BUILDER_PROTOTYPE=1`` is present.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
import dataclasses
import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath
import re
import shutil
import stat
import subprocess
import tempfile
import time
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm
import yaml

from sky.adaptors import aws as aws_adaptor
from sky.container_images import catalog_state
from sky.container_images import models
from sky.container_images import publication

_ENABLE_ENV = 'SKYPILOT_IMAGE_BUILDER_PROTOTYPE'
_DATABASE_URL_ENV = 'SKYPILOT_IMAGE_BUILDER_PROTOTYPE_DATABASE_URL'
_MAX_FILES = 100_000
_MAX_CONTEXT_BYTES = 100 * 1024 * 1024 * 1024
_MAX_SETUP_STEPS = 128
_MAX_INPUTS_PER_STEP = 4096
_MAX_COMMAND_BYTES = 64 * 1024
_MAX_BUILD_ARGS = 128
_LEASE_SECONDS = 30 * 60
_SCHEMA_PATTERN = re.compile(r'^skypilot_image_builder_[a-z0-9_]{1,48}$')
_STATES = ('PENDING', 'UPLOADING', 'QUEUED', 'BUILDING', 'VERIFYING',
           'PUBLISHING', 'READY', 'FAILED')
_BUILD_FRONTEND = 'dockerfile.v0'
_BUILD_FRONTEND_VERSION = 'docker/dockerfile:1.7'
_SECRET_BUILD_ARG = re.compile(
    r'(?:SECRET|TOKEN|PASSWORD|PASSWD|CREDENTIAL|PRIVATE_KEY|ACCESS_KEY)')


@dataclasses.dataclass(frozen=True)
class SetupStep:
    """One explicit setup command and its complete readable input set."""

    run: str
    inputs: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class BuildSpec:
    """Strict prototype contract, intentionally narrower than public YAML."""

    base: str
    setup: tuple[SetupStep, ...]
    context_include: tuple[str, ...]
    source_mode: str
    source_include: tuple[str, ...]
    build_args: tuple[tuple[str, str], ...]
    platform: str
    workspace: str
    distribution: str
    release: str
    staging_repository: str
    source_auth: str | None

    @classmethod
    def from_dict(cls, value: Any) -> BuildSpec:
        """Parses one closed, bounded prototype build document."""
        if not isinstance(value, dict):
            raise ValueError('Builder prototype spec must be an object.')
        allowed = {
            'base', 'setup', 'context', 'source', 'build_args', 'platform',
            'output'
        }
        if set(value) - allowed:
            raise ValueError('Builder prototype spec has unknown fields.')
        raw_base = value.get('base')
        if not isinstance(raw_base, str):
            raise ValueError(
                'Build base must be a digest-pinned OCI reference.')
        base = models.validate_oci_reference(raw_base, 'Build base')
        if models.split_digest(base)[1] is None:
            raise ValueError('Build base must be digest-pinned.')

        setup_raw = value.get('setup', [])
        if (not isinstance(setup_raw, list) or
                len(setup_raw) > _MAX_SETUP_STEPS):
            raise ValueError('Build setup is not a bounded list.')
        steps: list[SetupStep] = []
        for item in setup_raw:
            if not isinstance(item, dict) or set(item) != {'run', 'inputs'}:
                raise ValueError('Each setup step requires run and inputs.')
            command = item['run']
            inputs = item['inputs']
            if (not isinstance(command, str) or not command or
                    len(command.encode()) > _MAX_COMMAND_BYTES or
                    not isinstance(inputs, list) or
                    len(inputs) > _MAX_INPUTS_PER_STEP):
                raise ValueError('Setup command or inputs exceed a bound.')
            normalized_inputs = tuple(
                _relative_path(path, 'Setup input') for path in inputs)
            if len(set(normalized_inputs)) != len(normalized_inputs):
                raise ValueError('Setup inputs must be unique per step.')
            steps.append(SetupStep(command, normalized_inputs))

        context = value.get('context', {})
        source = value.get('source', {'mode': 'late_bound', 'include': []})
        if (not isinstance(context, dict) or set(context) != {'include'} or
                not isinstance(source, dict) or
                set(source) != {'mode', 'include'}):
            raise ValueError('Build context and source shapes are invalid.')
        context_include = _patterns(context['include'], 'Context include')
        source_include = _patterns(source['include'],
                                   'Source include',
                                   allow_empty=True)
        source_mode = source['mode']
        if source_mode not in ('late_bound', 'image'):
            raise ValueError('Source mode must be late_bound or image.')
        if source_mode == 'image' and not source_include:
            raise ValueError(
                'Image source mode requires explicit source files.')

        raw_args = value.get('build_args', {})
        if not isinstance(raw_args, dict) or len(raw_args) > _MAX_BUILD_ARGS:
            raise ValueError('Build arguments are invalid or unbounded.')
        build_args: list[tuple[str, str]] = []
        for key, raw_value in sorted(raw_args.items()):
            if (not isinstance(key, str) or
                    re.fullmatch(r'[A-Z_][A-Z0-9_]{0,127}', key) is None or
                    _SECRET_BUILD_ARG.search(key) is not None or
                    not isinstance(raw_value, (str, int, float, bool)) or
                    len(str(raw_value).encode()) > 4096):
                raise ValueError('Build argument name or value is invalid.')
            build_args.append((key, str(raw_value)))

        platform = models.validate_oci_platform(
            value.get('platform', 'linux/amd64'), 'Build platform')
        if platform != 'linux/amd64':
            raise ValueError('The builder prototype supports linux/amd64 only.')
        output = value.get('output')
        if not isinstance(output, dict) or set(output) != {
                'workspace', 'distribution', 'release', 'staging_repository',
                'source_auth'
        }:
            raise ValueError('Build output contract is invalid.')
        workspace = models.validate_workspace_name(output['workspace'],
                                                   'Build workspace')
        distribution = models.validate_control_plane_identifier(
            output['distribution'], 'Build distribution')
        release = models.validate_release_label(output['release'],
                                                'Build release')
        staging_repository = models.validate_oci_reference(
            output['staging_repository'], 'Build staging repository')
        if ('@' in staging_repository or
                ':' in staging_repository.rsplit('/', 1)[-1]):
            raise ValueError(
                'Staging repository must not contain tag or digest.')
        source_auth = output['source_auth']
        if source_auth is not None:
            source_auth = models.validate_control_plane_identifier(
                source_auth, 'Build staging source binding')
        return cls(base=base,
                   setup=tuple(steps),
                   context_include=context_include,
                   source_mode=source_mode,
                   source_include=source_include,
                   build_args=tuple(build_args),
                   platform=platform,
                   workspace=workspace,
                   distribution=distribution,
                   release=release,
                   staging_repository=staging_repository,
                   source_auth=source_auth)

    @property
    def spec_hash(self) -> str:
        return _sha256(_canonical(dataclasses.asdict(self)))


@dataclasses.dataclass(frozen=True)
class ContextEntry:
    """Content identity for one regular context file."""

    path: str
    digest: str
    size: int
    mode: int


@dataclasses.dataclass(frozen=True)
class ContextManifest:
    """Deterministic, content-addressed context manifest."""

    entries: tuple[ContextEntry, ...]
    total_bytes: int
    digest: str

    @property
    def payload(self) -> bytes:
        return _canonical({
            'version': 1,
            'total_bytes': self.total_bytes,
            'entries': [dataclasses.asdict(entry) for entry in self.entries],
        })

    def by_path(self) -> dict[str, ContextEntry]:
        return {entry.path: entry for entry in self.entries}


@dataclasses.dataclass(frozen=True)
class BuildOutput:
    """Verified BuildKit metadata needed for trusted publication."""

    staging_ref: str
    digest: str
    cache_hits: int
    log_path: str


@dataclasses.dataclass(frozen=True)
class BuildRecord:
    """Credential-free projection of one prototype coordinator row."""

    id: str
    idempotency_key: str
    spec_hash: str
    context_digest: str
    dependency_cache_key: str
    state: str
    staging_repository: str
    output_digest: str | None
    publication_id: str | None
    artifact_id: str | None
    error_code: str | None
    lease_token: str | None
    lease_expires_at: int | None
    created_at: int
    updated_at: int


def _canonical(value: Any) -> bytes:
    return json.dumps(value,
                      sort_keys=True,
                      separators=(',', ':'),
                      ensure_ascii=True).encode()


def _sha256(value: bytes) -> str:
    return 'sha256:' + hashlib.sha256(value).hexdigest()


def _relative_path(value: Any, subject: str) -> str:
    if not isinstance(value, str) or not value or len(value.encode()) > 4096:
        raise ValueError(f'{subject} must be a bounded relative path.')
    path = PurePosixPath(value.replace('\\', '/'))
    if path.is_absolute() or '..' in path.parts or '.' in path.parts:
        raise ValueError(f'{subject} must not escape the build context.')
    normalized = path.as_posix()
    if normalized.startswith('.git/') or normalized == '.git':
        raise ValueError(f'{subject} cannot include repository metadata.')
    return normalized


def _patterns(value: Any,
              subject: str,
              *,
              allow_empty: bool = False) -> tuple[str, ...]:
    if (not isinstance(value, list) or (not value and not allow_empty) or
            len(value) > 4096):
        raise ValueError(f'{subject} must be a bounded list.')
    patterns = tuple(_relative_path(item, subject) for item in value)
    if len(patterns) != len(set(patterns)):
        raise ValueError(f'{subject} must not contain duplicates.')
    return patterns


def create_context_manifest(root: Path, spec: BuildSpec) -> ContextManifest:
    """Hashes the exact bounded union of context and source include globs."""
    context_root = root.resolve(strict=True)
    if not context_root.is_dir():
        raise ValueError('Build context root must be a directory.')
    paths: dict[str, Path] = {}
    for pattern in spec.context_include + spec.source_include:
        for candidate in context_root.glob(pattern):
            relative = candidate.relative_to(context_root).as_posix()
            if candidate.is_symlink():
                raise ValueError('Build context symlinks are not accepted.')
            if not candidate.is_file():
                continue
            resolved = candidate.resolve(strict=True)
            try:
                resolved.relative_to(context_root)
            except ValueError:
                raise ValueError(
                    'Build context path escapes its root.') from None
            paths[_relative_path(relative, 'Build context path')] = resolved
    if not paths:
        raise ValueError('Build context include patterns selected no files.')
    if len(paths) > _MAX_FILES:
        raise ValueError('Build context exceeds the file-count bound.')
    entries: list[ContextEntry] = []
    total_bytes = 0
    for relative, path in sorted(paths.items()):
        file_stat = path.stat()
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError('Build context accepts regular files only.')
        total_bytes += file_stat.st_size
        if total_bytes > _MAX_CONTEXT_BYTES:
            raise ValueError('Build context exceeds the byte bound.')
        digest = _file_digest(path)
        entries.append(
            ContextEntry(path=relative,
                         digest=digest,
                         size=file_stat.st_size,
                         mode=stat.S_IMODE(file_stat.st_mode)))
    provisional = {
        'version': 1,
        'total_bytes': total_bytes,
        'entries': [dataclasses.asdict(entry) for entry in entries],
    }
    return ContextManifest(entries=tuple(entries),
                           total_bytes=total_bytes,
                           digest=_sha256(_canonical(provisional)))


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while True:
            chunk = stream.read(8 * 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return 'sha256:' + digest.hexdigest()


def dependency_cache_key(spec: BuildSpec, manifest: ContextManifest) -> str:
    """Returns the cache key excluding code-only late-bound source changes."""
    entries = manifest.by_path()
    step_payload: list[dict[str, Any]] = []
    for step in spec.setup:
        missing = [path for path in step.inputs if path not in entries]
        if missing:
            raise ValueError(
                'A setup input was not selected by context include.')
        step_payload.append({
            'run': step.run,
            'inputs': [{
                'path': path,
                'digest': entries[path].digest,
                'mode': entries[path].mode,
            } for path in step.inputs],
        })
    source_payload: list[dict[str, Any]] = []
    if spec.source_mode == 'image':
        for path in _selected_paths(spec.source_include, entries):
            source_payload.append({
                'path': path,
                'digest': entries[path].digest,
                'mode': entries[path].mode,
            })
    return _sha256(
        _canonical({
            'frontend': _BUILD_FRONTEND,
            'frontend_version': _BUILD_FRONTEND_VERSION,
            'base': spec.base,
            'platform': spec.platform,
            'setup': step_payload,
            'build_args': list(spec.build_args),
            'source_mode': spec.source_mode,
            'source': source_payload,
            'policy_version': 1,
        }))


def _selected_paths(patterns: tuple[str, ...],
                    entries: dict[str, ContextEntry]) -> tuple[str, ...]:
    selected: set[str] = set()
    for pattern in patterns:
        matcher = PurePosixPath
        selected.update(
            path for path in entries if matcher(path).match(pattern))
    return tuple(sorted(selected))


class S3CompatibleContextStore:
    """Uploads digest-keyed blobs directly to S3, R2, or a compatible API."""

    def __init__(self,
                 *,
                 bucket: str,
                 endpoint_url: str | None,
                 region: str,
                 credential_profile: str | None,
                 prefix: str = 'skypilot/image-builder/v1') -> None:
        if not bucket or len(bucket) > 255:
            raise ValueError('Context-store bucket is invalid.')
        self._bucket = bucket
        self._prefix = prefix.strip('/')
        session = aws_adaptor.session(profile=credential_profile)
        self._client = session.client('s3',
                                      endpoint_url=endpoint_url,
                                      region_name=region)

    def upload(self, root: Path, manifest: ContextManifest) -> int:
        """Uploads missing blobs and the immutable manifest, returning misses."""
        uploaded = 0
        for entry in manifest.entries:
            key = f'{self._prefix}/blobs/{entry.digest.replace(":", "/")}'
            if self._head_matches(key, entry):
                continue
            _verify_context_file(root, entry)
            self._client.upload_file(
                str(root / entry.path),
                self._bucket,
                key,
                ExtraArgs={
                    'Metadata': {
                        'sha256': entry.digest.removeprefix('sha256:'),
                        'mode': str(entry.mode),
                    }
                })
            uploaded += 1
        manifest_key = (
            f'{self._prefix}/manifests/{manifest.digest.replace(":", "/")}.json'
        )
        self._client.put_object(
            Bucket=self._bucket,
            Key=manifest_key,
            Body=manifest.payload,
            ContentType='application/json',
            Metadata={'sha256': manifest.digest.removeprefix('sha256:')})
        return uploaded

    def _head_matches(self, key: str, entry: ContextEntry) -> bool:
        try:
            response = self._client.head_object(Bucket=self._bucket, Key=key)
        except Exception as error:  # pylint: disable=broad-except
            response = getattr(error, 'response', {})
            code = (response.get('Error', {}).get('Code') if isinstance(
                response, dict) else None)
            if str(code) in ('404', 'NoSuchKey', 'NotFound'):
                return False
            raise
        metadata = response.get('Metadata', {})
        return (response.get('ContentLength') == entry.size and
                metadata.get('sha256') == entry.digest.removeprefix('sha256:'))


class PrototypeRepository:
    """Crash-safe coordinator state in an isolated PostgreSQL schema."""

    def __init__(self, database_url: str, schema_name: str) -> None:
        if _SCHEMA_PATTERN.fullmatch(schema_name) is None:
            raise ValueError('Builder prototype schema name is invalid.')
        self._engine = sqlalchemy.create_engine(database_url,
                                                pool_pre_ping=True)
        if self._engine.dialect.name != 'postgresql':
            raise RuntimeError('Builder prototype state requires PostgreSQL.')
        self._metadata = sqlalchemy.MetaData(schema=schema_name)
        self._builds = sqlalchemy.Table(
            'builds', self._metadata,
            sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
            sqlalchemy.Column('idempotency_key',
                              sqlalchemy.Text,
                              nullable=False,
                              unique=True),
            sqlalchemy.Column('spec_hash', sqlalchemy.Text, nullable=False),
            sqlalchemy.Column('context_digest', sqlalchemy.Text,
                              nullable=False),
            sqlalchemy.Column('dependency_cache_key',
                              sqlalchemy.Text,
                              nullable=False),
            sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
            sqlalchemy.Column('staging_repository',
                              sqlalchemy.Text,
                              nullable=False),
            sqlalchemy.Column('output_digest', sqlalchemy.Text),
            sqlalchemy.Column('publication_id', sqlalchemy.Text),
            sqlalchemy.Column('artifact_id', sqlalchemy.Text),
            sqlalchemy.Column('error_code', sqlalchemy.Text),
            sqlalchemy.Column('lease_token', sqlalchemy.Text),
            sqlalchemy.Column('lease_expires_at', sqlalchemy.BigInteger),
            sqlalchemy.Column('created_at',
                              sqlalchemy.BigInteger,
                              nullable=False),
            sqlalchemy.Column('updated_at',
                              sqlalchemy.BigInteger,
                              nullable=False),
            sqlalchemy.CheckConstraint('state IN ' + str(_STATES),
                                       name='ck_builder_prototype_state'),
            sqlalchemy.CheckConstraint(
                "(state = 'BUILDING' AND lease_token IS NOT NULL AND "
                "lease_expires_at IS NOT NULL) OR (state <> 'BUILDING' AND "
                "lease_token IS NULL AND lease_expires_at IS NULL)",
                name='ck_builder_prototype_lease'),
            sqlalchemy.Index('ix_builder_prototype_queue', 'state',
                             'lease_expires_at', 'created_at', 'id'))
        with self._engine.begin() as connection:
            connection.execute(
                sqlalchemy.text(f'CREATE SCHEMA IF NOT EXISTS {schema_name}'))
            self._metadata.create_all(connection)

    def create_or_get(self, *, idempotency_key: str, spec: BuildSpec,
                      manifest: ContextManifest, cache_key: str) -> BuildRecord:
        if not 16 <= len(idempotency_key.encode()) <= 128:
            raise ValueError('Builder idempotency key is invalid.')
        now = int(time.time())
        with orm.Session(self._engine) as session, session.begin():
            row = session.execute(
                sqlalchemy.select(self._builds).where(
                    self._builds.c.idempotency_key ==
                    idempotency_key).with_for_update()).mappings().first()
            if row is not None:
                if (row['spec_hash'] != spec.spec_hash or
                        row['context_digest'] != manifest.digest):
                    raise ValueError('Builder idempotency key was reused.')
                return self._record(row)
            build_id = str(uuid.uuid4())
            row = session.execute(self._builds.insert().values(
                id=build_id,
                idempotency_key=idempotency_key,
                spec_hash=spec.spec_hash,
                context_digest=manifest.digest,
                dependency_cache_key=cache_key,
                state='PENDING',
                staging_repository=spec.staging_repository,
                created_at=now,
                updated_at=now).returning(self._builds)).mappings().one()
            return self._record(row)

    def transition(self, build_id: str, expected: tuple[str, ...], state: str,
                   **values: Any) -> BuildRecord:
        if state not in _STATES:
            raise ValueError('Builder state is invalid.')
        values.update(state=state, updated_at=int(time.time()))
        if state != 'BUILDING':
            values.update(lease_token=None, lease_expires_at=None)
        with orm.Session(self._engine) as session, session.begin():
            row = session.execute(self._builds.update().where(
                self._builds.c.id == build_id,
                self._builds.c.state.in_(expected)).values(**values).returning(
                    self._builds)).mappings().first()
            if row is None:
                existing = session.execute(
                    sqlalchemy.select(self._builds).where(
                        self._builds.c.id == build_id)).mappings().one()
                return self._record(existing)
            return self._record(row)

    def claim(self, build_id: str) -> tuple[BuildRecord, str | None]:
        now = int(time.time())
        token = str(uuid.uuid4())
        with orm.Session(self._engine) as session, session.begin():
            row = session.execute(self._builds.update().where(
                self._builds.c.id == build_id,
                sqlalchemy.or_(
                    self._builds.c.state == 'QUEUED',
                    sqlalchemy.and_(self._builds.c.state == 'BUILDING',
                                    self._builds.c.lease_expires_at
                                    <= now))).values(
                                        state='BUILDING',
                                        lease_token=token,
                                        lease_expires_at=now + _LEASE_SECONDS,
                                        updated_at=now).returning(
                                            self._builds)).mappings().first()
            if row is not None:
                return self._record(row), token
            row = session.execute(
                sqlalchemy.select(self._builds).where(
                    self._builds.c.id == build_id)).mappings().one()
            return self._record(row), None

    def heartbeat(self, build_id: str, lease_token: str) -> bool:
        """Renews only the exact unexpired BUILDING fence."""
        now = int(time.time())
        with orm.Session(self._engine) as session, session.begin():
            row = session.execute(self._builds.update().where(
                self._builds.c.id == build_id,
                self._builds.c.state == 'BUILDING',
                self._builds.c.lease_token == lease_token,
                self._builds.c.lease_expires_at > now).values(
                    lease_expires_at=now + _LEASE_SECONDS,
                    updated_at=now).returning(self._builds.c.id)).first()
            return row is not None

    def complete_build(self, build_id: str, lease_token: str,
                       output_digest: str) -> BuildRecord:
        """Commits output only while the exact BuildKit lease remains live."""
        digest = models.validate_sha256_digest(output_digest,
                                               'Prototype output digest')
        now = int(time.time())
        with orm.Session(self._engine) as session, session.begin():
            row = session.execute(self._builds.update().where(
                self._builds.c.id == build_id,
                self._builds.c.state == 'BUILDING',
                self._builds.c.lease_token == lease_token,
                self._builds.c.lease_expires_at > now).values(
                    state='VERIFYING',
                    output_digest=digest,
                    lease_token=None,
                    lease_expires_at=None,
                    updated_at=now).returning(self._builds)).mappings().first()
            if row is None:
                raise RuntimeError('BUILDER_LEASE_LOST')
            return self._record(row)

    def get(self, build_id: str) -> BuildRecord:
        with orm.Session(self._engine) as session:
            row = session.execute(
                sqlalchemy.select(self._builds).where(
                    self._builds.c.id == build_id)).mappings().one()
            return self._record(row)

    @staticmethod
    def _record(row: sqlalchemy.engine.RowMapping) -> BuildRecord:
        return BuildRecord(id=str(row['id']),
                           idempotency_key=str(row['idempotency_key']),
                           spec_hash=str(row['spec_hash']),
                           context_digest=str(row['context_digest']),
                           dependency_cache_key=str(
                               row['dependency_cache_key']),
                           state=str(row['state']),
                           staging_repository=str(row['staging_repository']),
                           output_digest=row['output_digest'],
                           publication_id=row['publication_id'],
                           artifact_id=row['artifact_id'],
                           error_code=row['error_code'],
                           lease_token=row['lease_token'],
                           lease_expires_at=row['lease_expires_at'],
                           created_at=int(row['created_at']),
                           updated_at=int(row['updated_at']))


class BuildKitExecutor:
    """Runs a filtered context against an already-isolated BuildKit daemon."""

    def __init__(self, buildctl: str = 'buildctl') -> None:
        executable = shutil.which(buildctl)
        if executable is None:
            raise RuntimeError('The builder prototype requires buildctl.')
        self._buildctl = executable

    def execute(self, record: BuildRecord, spec: BuildSpec, root: Path,
                manifest: ContextManifest,
                heartbeat: Callable[[], bool]) -> BuildOutput:
        if record.state != 'BUILDING' or record.lease_token is None:
            raise RuntimeError('BuildKit requires a fenced BUILDING claim.')
        entries = manifest.by_path()
        staging_ref = f'{spec.staging_repository}:sky-build-{record.id}'
        cache_ref = (
            f'{spec.staging_repository}:sky-cache-'
            f'{record.dependency_cache_key.removeprefix("sha256:")[:24]}')
        with tempfile.TemporaryDirectory(prefix='sky-image-builder-') as temp:
            build_root = Path(temp)
            context_root = build_root / 'context'
            dockerfile_root = build_root / 'dockerfile'
            context_root.mkdir()
            dockerfile_root.mkdir()
            self._stage_context(root, context_root, spec, entries)
            dockerfile = self._dockerfile(spec)
            (dockerfile_root / 'Dockerfile').write_text(dockerfile,
                                                        encoding='utf-8')
            metadata_path = build_root / 'metadata.json'
            log_path = Path(
                tempfile.gettempdir()) / f'sky-build-{record.id}.log'
            command = [
                self._buildctl, 'build', '--frontend', _BUILD_FRONTEND,
                '--local', f'context={context_root}', '--local',
                f'dockerfile={dockerfile_root}', '--opt',
                f'platform={spec.platform}', '--metadata-file',
                str(metadata_path), '--output',
                f'type=image,name={staging_ref},push=true', '--import-cache',
                f'type=registry,ref={cache_ref}', '--export-cache',
                f'type=registry,ref={cache_ref},mode=max'
            ]
            for key, value in spec.build_args:
                command.extend(('--opt', f'build-arg:{key}={value}'))
            with log_path.open('wb') as logs:
                process = subprocess.Popen(command,
                                           stdout=logs,
                                           stderr=subprocess.STDOUT)
                try:
                    while process.poll() is None:
                        if not heartbeat():
                            raise RuntimeError('BUILDER_LEASE_LOST')
                        time.sleep(5)
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=10)
            if process.returncode != 0:
                raise RuntimeError('BUILDKIT_FAILED')
            metadata = json.loads(metadata_path.read_text(encoding='utf-8'))
            digest = models.validate_sha256_digest(
                metadata.get('containerimage.digest'), 'BuildKit output digest')
            cache_hits = log_path.read_text(encoding='utf-8',
                                            errors='replace').count(' CACHED')
            return BuildOutput(staging_ref=staging_ref,
                               digest=digest,
                               cache_hits=cache_hits,
                               log_path=str(log_path))

    @staticmethod
    def _stage_context(root: Path, destination: Path, spec: BuildSpec,
                       entries: dict[str, ContextEntry]) -> None:
        for index, step in enumerate(spec.setup):
            step_root = destination / '.skypilot' / 'steps' / f'{index:03d}'
            step_root.mkdir(parents=True, exist_ok=True)
            for relative in step.inputs:
                _copy_context_file(root, step_root, relative, entries)
        if spec.source_mode == 'image':
            for relative in _selected_paths(spec.source_include, entries):
                _copy_context_file(root, destination / '.skypilot' / 'source',
                                   relative, entries)

    @staticmethod
    def _dockerfile(spec: BuildSpec) -> str:
        lines = [f'# syntax={_BUILD_FRONTEND_VERSION}', f'FROM {spec.base}']
        for key, _ in spec.build_args:
            lines.append(f'ARG {key}')
        for index, step in enumerate(spec.setup):
            lines.append('RUN --mount=type=bind,source=.skypilot/steps/'
                         f'{index:03d},target=/inputs,readonly {step.run}')
        if spec.source_mode == 'image':
            lines.append('COPY .skypilot/source/ /opt/skypilot/source/')
        return '\n'.join(lines) + '\n'


def _copy_context_file(root: Path, destination: Path, relative: str,
                       entries: dict[str, ContextEntry]) -> None:
    if relative not in entries:
        raise ValueError('Build step references a missing context input.')
    entry = entries[relative]
    _verify_context_file(root, entry)
    target = destination / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(root / relative, target)
    target.chmod(entry.mode)


def _verify_context_file(root: Path, entry: ContextEntry) -> None:
    source = root / entry.path
    if (source.is_symlink() or not source.is_file() or
            source.stat().st_size != entry.size or
            _file_digest(source) != entry.digest):
        raise RuntimeError('BUILD_CONTEXT_CHANGED')


class PrototypeCoordinator:
    """Converges upload, build, verification, and ordinary publication."""

    def __init__(
        self,
        repository: PrototypeRepository,
        store: S3CompatibleContextStore,
        executor: BuildKitExecutor,
        verify_staging: Callable[[str, str], bool],
    ) -> None:
        self._repository = repository
        self._store = store
        self._executor = executor
        self._verify_staging = verify_staging

    def run(self, *, spec: BuildSpec, root: Path,
            idempotency_key: str) -> BuildRecord:
        """Runs or resumes one build without replacing a visible release."""
        root = root.resolve(strict=True)
        manifest = create_context_manifest(root, spec)
        cache_key = dependency_cache_key(spec, manifest)
        record = self._repository.create_or_get(idempotency_key=idempotency_key,
                                                spec=spec,
                                                manifest=manifest,
                                                cache_key=cache_key)
        if record.state == 'PENDING':
            record = self._repository.transition(record.id, ('PENDING',),
                                                 'UPLOADING')
        if record.state == 'UPLOADING':
            self._store.upload(root, manifest)
            record = self._repository.transition(record.id, ('UPLOADING',),
                                                 'QUEUED')
        if record.state in ('QUEUED', 'BUILDING'):
            record, lease_token = self._repository.claim(record.id)
            if lease_token is None:
                return record
            output = self._executor.execute(record,
                                            spec,
                                            root,
                                            manifest,
                                            heartbeat=lambda: self._repository.
                                            heartbeat(record.id, lease_token))
            record = self._repository.complete_build(record.id, lease_token,
                                                     output.digest)
        if record.state == 'VERIFYING':
            assert record.output_digest is not None
            staging_ref = f'{record.staging_repository}@{record.output_digest}'
            if not self._verify_staging(staging_ref, record.output_digest):
                return self._repository.transition(
                    record.id, ('VERIFYING',),
                    'FAILED',
                    error_code='STAGING_DIGEST_MISMATCH')
            record = self._repository.transition(record.id, ('VERIFYING',),
                                                 'PUBLISHING')
        if record.state == 'PUBLISHING':
            record = self._publish(record, spec)
        return record

    def _publish(self, record: BuildRecord, spec: BuildSpec) -> BuildRecord:
        assert record.output_digest is not None
        if record.publication_id is None:
            mutation = publication.publish(
                source_ref=(f'{record.staging_repository}@'
                            f'{record.output_digest}'),
                release=spec.release,
                distribution=spec.distribution,
                workspace=spec.workspace,
                actor_hash=hashlib.sha256(
                    b'managed-image-builder-prototype').hexdigest(),
                idempotency_key=f'builder-prototype-{record.id}',
                requested_platform=spec.platform,
                source_auth_binding_id=spec.source_auth)
            record = self._repository.transition(
                record.id, ('PUBLISHING',),
                'PUBLISHING',
                publication_id=mutation.publication.id)
        assert record.publication_id is not None
        published = catalog_state.get_publication(record.publication_id,
                                                  spec.workspace)
        if published is None:
            return self._repository.transition(record.id, ('PUBLISHING',),
                                               'FAILED',
                                               error_code='PUBLICATION_LOST')
        if published.state == models.ImagePublicationState.READY:
            assert published.image_id is not None
            return self._repository.transition(record.id, ('PUBLISHING',),
                                               'READY',
                                               artifact_id=published.image_id)
        if published.state == models.ImagePublicationState.FAILED:
            return self._repository.transition(
                record.id, ('PUBLISHING',),
                'FAILED',
                error_code=published.error_code or 'PUBLICATION_FAILED')
        return record


def _load_spec(path: Path) -> BuildSpec:
    raw = yaml.safe_load(path.read_text(encoding='utf-8'))
    return BuildSpec.from_dict(raw)


def main() -> None:
    """Validates and packs a prototype build behind the explicit safety gate."""
    if os.environ.get(_ENABLE_ENV) != '1':
        raise RuntimeError(
            f'The builder prototype is disabled; set {_ENABLE_ENV}=1 explicitly.'
        )
    parser = argparse.ArgumentParser(
        description='Disabled managed container image builder prototype')
    parser.add_argument('spec', type=Path)
    parser.add_argument('--context', type=Path, default=Path('.'))
    parser.add_argument('--validate-only', action='store_true')
    args = parser.parse_args()
    if not args.validate_only:
        raise RuntimeError(
            'The prototype CLI currently requires --validate-only. Build '
            'execution remains an internal evidence harness.')
    spec = _load_spec(args.spec)
    manifest = create_context_manifest(args.context, spec)
    cache_key = dependency_cache_key(spec, manifest)
    result = {
        'spec_hash': spec.spec_hash,
        'context_digest': manifest.digest,
        'dependency_cache_key': cache_key,
        'files': len(manifest.entries),
        'bytes': manifest.total_bytes,
        'database_configured': bool(os.environ.get(_DATABASE_URL_ENV)),
        'validate_only': True,
    }
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
