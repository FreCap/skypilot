"""Disabled builder prototype tests, including R2/S3 compatibility seams."""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest

from sky.container_images import builder_prototype

BASE_DIGEST = 'sha256:' + '1' * 64


def _spec(*, source_mode: str = 'late_bound') -> dict:
    return {
        'base': f'ghcr.io/boltz-bio/runtime@{BASE_DIGEST}',
        'setup': [{
            'run': 'pip install --require-hashes -r /inputs/requirements.txt',
            'inputs': ['requirements.txt'],
        }, {
            'run': 'test ! -e /inputs/src/app.py',
            'inputs': [],
        }],
        'context': {
            'include': ['requirements.txt'],
        },
        'source': {
            'mode': source_mode,
            'include': ['src/**'],
        },
        'build_args': {
            'CUDA_VARIANT': 'cu128',
        },
        'platform': 'linux/amd64',
        'output': {
            'workspace': 'research',
            'distribution': 'gpu-production',
            'release': 'boltz-l4-2026-07-20',
            'staging_repository': '123456789012.dkr.ecr.us-east-1.amazonaws.com/skypilot/staging',
            'source_auth': 'registry-copy',
        },
    }


def _direct_spec(*, source_mode: str = 'late_bound') -> dict:
    spec = _spec(source_mode=source_mode)
    spec['output']['distribution'] = 'direct'
    spec['output']['source_auth'] = None
    return spec


def _context(root: Path) -> None:
    (root / 'src').mkdir()
    (root / 'requirements.txt').write_text('numpy==2.0.0 --hash=sha256:abc\n',
                                           encoding='utf-8')
    (root / 'src' / 'app.py').write_text('print("one")\n', encoding='utf-8')


def test_context_manifest_is_deterministic_and_bounded(tmp_path: Path) -> None:
    _context(tmp_path)
    spec = builder_prototype.BuildSpec.from_dict(_spec())
    first = builder_prototype.create_context_manifest(tmp_path, spec)
    second = builder_prototype.create_context_manifest(tmp_path, spec)
    assert first == second
    assert [entry.path for entry in first.entries
           ] == ['requirements.txt', 'src/app.py']
    assert first.digest.startswith('sha256:')
    assert json.loads(first.payload)['entries'][0]['path'] == 'requirements.txt'


def test_bare_recursive_glob_includes_files_portably(tmp_path: Path) -> None:
    _context(tmp_path)
    raw = _spec()
    raw['context']['include'] = ['**']
    raw['source']['include'] = []

    manifest = builder_prototype.create_context_manifest(
        tmp_path, builder_prototype.BuildSpec.from_dict(raw))

    assert [entry.path for entry in manifest.entries
           ] == ['requirements.txt', 'src/app.py']


def test_late_bound_code_change_does_not_invalidate_dependency_cache(
        tmp_path: Path) -> None:
    _context(tmp_path)
    spec = builder_prototype.BuildSpec.from_dict(_spec())
    first = builder_prototype.create_context_manifest(tmp_path, spec)
    first_key = builder_prototype.dependency_cache_key(spec, first)
    (tmp_path / 'src' / 'app.py').write_text('print("two")\n', encoding='utf-8')
    second = builder_prototype.create_context_manifest(tmp_path, spec)
    assert second.digest != first.digest
    assert builder_prototype.dependency_cache_key(spec, second) == first_key


def test_setup_and_image_source_changes_invalidate_the_right_cache_suffix(
        tmp_path: Path) -> None:
    _context(tmp_path)
    late_bound = builder_prototype.BuildSpec.from_dict(_spec())
    manifest = builder_prototype.create_context_manifest(tmp_path, late_bound)
    first_key = builder_prototype.dependency_cache_key(late_bound, manifest)

    (tmp_path / 'requirements.txt').write_text(
        'numpy==2.1.0 --hash=sha256:def\n', encoding='utf-8')
    changed = builder_prototype.create_context_manifest(tmp_path, late_bound)
    assert builder_prototype.dependency_cache_key(late_bound,
                                                  changed) != first_key

    image_spec = builder_prototype.BuildSpec.from_dict(
        _spec(source_mode='image'))
    image_key = builder_prototype.dependency_cache_key(image_spec, changed)
    (tmp_path / 'src' / 'app.py').write_text('print("three")\n',
                                             encoding='utf-8')
    image_changed = builder_prototype.create_context_manifest(
        tmp_path, image_spec)
    assert builder_prototype.dependency_cache_key(image_spec,
                                                  image_changed) != image_key


def test_builder_rejects_undeclared_inputs_symlinks_secrets_and_arm64(
        tmp_path: Path) -> None:
    _context(tmp_path)
    raw = _spec()
    raw['setup'][0]['inputs'] = ['missing.txt']
    spec = builder_prototype.BuildSpec.from_dict(raw)
    manifest = builder_prototype.create_context_manifest(tmp_path, spec)
    with pytest.raises(ValueError, match='not selected'):
        builder_prototype.dependency_cache_key(spec, manifest)

    (tmp_path / 'linked.py').symlink_to(tmp_path / 'src' / 'app.py')
    raw = _spec()
    raw['context']['include'].append('linked.py')
    with pytest.raises(ValueError, match='symlinks'):
        builder_prototype.create_context_manifest(
            tmp_path, builder_prototype.BuildSpec.from_dict(raw))

    raw = _spec()
    raw['build_args'] = {'API_TOKEN': 'must-not-enter-command-arguments'}
    with pytest.raises(ValueError, match='Build argument'):
        builder_prototype.BuildSpec.from_dict(raw)

    raw = _spec()
    raw['platform'] = 'linux/arm64'
    with pytest.raises(ValueError, match='linux/amd64 only'):
        builder_prototype.BuildSpec.from_dict(raw)


def test_filtered_build_context_exposes_only_each_step_inputs(
        tmp_path: Path) -> None:
    _context(tmp_path)
    spec = builder_prototype.BuildSpec.from_dict(_spec())
    manifest = builder_prototype.create_context_manifest(tmp_path, spec)
    destination = tmp_path / 'filtered'
    destination.mkdir()
    builder_prototype.BuildKitExecutor._stage_context(  # pylint: disable=protected-access
        tmp_path, destination, spec, manifest.by_path())
    assert (destination / '.skypilot/steps/000/requirements.txt').is_file()
    assert not (destination / '.skypilot/steps/000/src/app.py').exists()
    assert (destination / '.skypilot/steps/001').is_dir()
    assert not (destination / '.skypilot/source').exists()
    dockerfile = builder_prototype.BuildKitExecutor._dockerfile(  # pylint: disable=protected-access
        spec)
    assert 'target=/inputs,readonly' in dockerfile
    assert 'COPY .skypilot/source' not in dockerfile


class _MissingObject(Exception):

    def __init__(self) -> None:
        super().__init__('missing')
        self.response = {'Error': {'Code': '404'}}


class _S3Client:
    """Minimal S3-compatible client recording content-addressed writes."""

    def __init__(self) -> None:
        self.objects: dict[str, dict] = {}
        self.uploads: list[str] = []

    def head_object(self, *, Bucket: str, Key: str):
        del Bucket
        if Key not in self.objects:
            raise _MissingObject()
        return self.objects[Key]

    def upload_file(self, path: str, bucket: str, key: str, ExtraArgs: dict):
        del bucket
        payload = Path(path).read_bytes()
        self.uploads.append(key)
        self.objects[key] = {
            'ContentLength': len(payload),
            'Metadata': ExtraArgs['Metadata'],
        }

    def put_object(self, *, Bucket: str, Key: str, Body: bytes,
                   ContentType: str, Metadata: dict):
        del Bucket, ContentType
        self.objects[Key] = {
            'ContentLength': len(Body),
            'Metadata': Metadata,
        }


def test_s3_compatible_store_deduplicates_and_accepts_r2_endpoint(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _context(tmp_path)
    spec = builder_prototype.BuildSpec.from_dict(_spec())
    manifest = builder_prototype.create_context_manifest(tmp_path, spec)
    client = _S3Client()
    session = mock.MagicMock()
    session.client.return_value = client
    session_factory = mock.Mock(return_value=session)
    monkeypatch.setattr(builder_prototype.aws_adaptor, 'session',
                        session_factory)
    endpoint = 'https://example-account.r2.cloudflarestorage.com'
    store = builder_prototype.S3CompatibleContextStore(
        bucket='skypilot-build-contexts',
        endpoint_url=endpoint,
        region='auto',
        credential_profile='r2-builder')
    assert store.upload(tmp_path, manifest) == len(manifest.entries)
    assert store.upload(tmp_path, manifest) == 0
    session_factory.assert_called_once_with(profile='r2-builder')
    session.client.assert_called_once_with('s3',
                                           endpoint_url=endpoint,
                                           region_name='auto')


def test_upload_detects_context_toctou(tmp_path: Path,
                                       monkeypatch: pytest.MonkeyPatch) -> None:
    _context(tmp_path)
    spec = builder_prototype.BuildSpec.from_dict(_spec())
    manifest = builder_prototype.create_context_manifest(tmp_path, spec)
    (tmp_path / 'requirements.txt').write_text('changed after manifest\n',
                                               encoding='utf-8')
    client = _S3Client()
    session = mock.MagicMock()
    session.client.return_value = client
    monkeypatch.setattr(builder_prototype.aws_adaptor, 'session',
                        mock.Mock(return_value=session))
    store = builder_prototype.S3CompatibleContextStore(bucket='contexts',
                                                       endpoint_url=None,
                                                       region='us-east-1',
                                                       credential_profile=None)
    with pytest.raises(RuntimeError, match='BUILD_CONTEXT_CHANGED'):
        store.upload(tmp_path, manifest)


class _DirectExecutor:
    """Records a direct evidence build without invoking Buildx."""

    def __init__(self) -> None:
        self.record: builder_prototype.BuildRecord | None = None
        self.verified: builder_prototype.BuildOutput | None = None

    def execute(
        self,
        record: builder_prototype.BuildRecord,
        spec: builder_prototype.BuildSpec,
        root: Path,
        manifest: builder_prototype.ContextManifest,
        heartbeat,
    ) -> builder_prototype.BuildOutput:
        del spec, root, manifest
        assert heartbeat()
        self.record = record
        return builder_prototype.BuildOutput(
            staging_ref=f'{record.staging_repository}:sky-build-{record.id}',
            digest='sha256:' + '2' * 64,
            cache_hits=3,
            log_path='/tmp/prototype-build.log')

    def verify(self, output: builder_prototype.BuildOutput) -> None:
        self.verified = output


def test_direct_evidence_build_returns_verified_digest_pinned_result(
        tmp_path: Path) -> None:
    _context(tmp_path)
    spec = builder_prototype.BuildSpec.from_dict(_direct_spec())
    executor = _DirectExecutor()

    result = builder_prototype.run_direct_build(
        spec=spec,
        root=tmp_path,
        executor=executor,  # type: ignore[arg-type]
    )

    assert result.mode == 'execute-direct'
    assert result.reference == (
        '123456789012.dkr.ecr.us-east-1.amazonaws.com/skypilot/staging@'
        'sha256:' + '2' * 64)
    assert result.cache_hits == 3
    assert result.files == 2
    assert executor.record is not None
    assert executor.record.state == 'BUILDING'
    assert executor.verified is not None
    assert executor.verified.digest == result.digest


def test_direct_evidence_build_rejects_managed_publication_spec(
        tmp_path: Path) -> None:
    _context(tmp_path)
    spec = builder_prototype.BuildSpec.from_dict(_spec())

    with pytest.raises(ValueError, match='distribution direct'):
        builder_prototype.run_direct_build(
            spec=spec,
            root=tmp_path,
            executor=_DirectExecutor(),  # type: ignore[arg-type]
        )


def test_buildx_executor_writes_immutable_cache_tag_once(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _context(tmp_path)
    spec = builder_prototype.BuildSpec.from_dict(_direct_spec())
    manifest = builder_prototype.create_context_manifest(tmp_path, spec)
    now = 1_700_000_000
    record = builder_prototype.BuildRecord(
        id='11111111-1111-1111-1111-111111111111',
        idempotency_key='direct-evidence-idempotency',
        spec_hash=spec.spec_hash,
        context_digest=manifest.digest,
        dependency_cache_key=builder_prototype.dependency_cache_key(
            spec, manifest),
        state='BUILDING',
        staging_repository=spec.staging_repository,
        output_digest=None,
        publication_id=None,
        artifact_id=None,
        error_code=None,
        lease_token='lease',
        lease_expires_at=now + 1800,
        created_at=now,
        updated_at=now)
    monkeypatch.setattr(builder_prototype.shutil, 'which',
                        lambda executable: f'/usr/bin/{executable}')
    executor = builder_prototype.DockerBuildxExecutor()
    monkeypatch.setattr(executor, '_ensure_builder', lambda: None)
    monkeypatch.setattr(executor, '_reference_exists', lambda reference: False)
    commands: list[list[str]] = []

    class _Process:
        """Immediate successful Buildx process for command assertions."""

        returncode = 0

        def __init__(self, command: list[str], **kwargs) -> None:
            del kwargs
            commands.append(command)
            metadata_path = Path(command[command.index('--metadata-file') + 1])
            metadata_path.write_text(json.dumps(
                {'containerimage.digest': 'sha256:' + '3' * 64}),
                                     encoding='utf-8')

        def poll(self) -> int:
            return 0

    monkeypatch.setattr(builder_prototype.subprocess, 'Popen', _Process)

    output = executor.execute(record,
                              spec,
                              tmp_path,
                              manifest,
                              heartbeat=lambda: True)

    assert output.digest == 'sha256:' + '3' * 64
    assert '--cache-from' in commands[0]
    assert '--cache-to' in commands[0]
    assert '--provenance=false' in commands[0]
    assert commands[0][-1].endswith('/context')

    monkeypatch.setattr(executor, '_reference_exists', lambda reference: True)
    executor.execute(record, spec, tmp_path, manifest, heartbeat=lambda: True)
    assert '--cache-from' in commands[1]
    assert '--cache-to' not in commands[1]


def test_multiline_setup_uses_deterministic_heredoc() -> None:
    raw = _direct_spec()
    raw['setup'].append({
        'run': 'python3 -m venv /opt/venv\n'
               '/opt/venv/bin/python -m pip install boto3\n'
               'test -x /opt/venv/bin/python',
        'inputs': [],
    })
    spec = builder_prototype.BuildSpec.from_dict(raw)

    first = builder_prototype.BuildKitExecutor._dockerfile(  # pylint: disable=protected-access
        spec)
    second = builder_prototype.BuildKitExecutor._dockerfile(  # pylint: disable=protected-access
        spec)

    assert first == second
    assert "target=/inputs,readonly <<'SKYPILOT_SETUP_001_" in first
    assert '\nset -e\npython3 -m venv /opt/venv\n' in first
    assert '\n/opt/venv/bin/python -m pip install boto3\n' in first
    delimiter = next(
        line.rsplit("<<'", 1)[1][:-1]
        for line in first.splitlines()
        if "SKYPILOT_SETUP_001_" in line)
    assert first.splitlines().count(delimiter) == 1


def test_buildx_executor_creates_and_bootstraps_missing_builder(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(builder_prototype.shutil, 'which',
                        lambda executable: f'/usr/bin/{executable}')
    executor = builder_prototype.DockerBuildxExecutor()
    commands: list[list[str]] = []

    def _run(command: list[str], **kwargs):
        del kwargs
        commands.append(command)
        if command[2:4] == ['inspect', 'skypilot-image-builder-prototype']:
            return mock.Mock(returncode=1,
                             stdout=b'',
                             stderr=b'no builder found')
        return mock.Mock(returncode=0, stdout=b'', stderr=b'')

    monkeypatch.setattr(builder_prototype.subprocess, 'run', _run)

    executor._ensure_builder()  # pylint: disable=protected-access

    assert commands == [[
        '/usr/bin/docker', 'buildx', 'inspect',
        'skypilot-image-builder-prototype'
    ],
                        [
                            '/usr/bin/docker', 'buildx', 'create', '--name',
                            'skypilot-image-builder-prototype', '--driver',
                            'docker-container'
                        ],
                        [
                            '/usr/bin/docker', 'buildx', 'inspect',
                            '--bootstrap', 'skypilot-image-builder-prototype'
                        ]]


def test_prototype_cli_is_disabled_without_explicit_gate(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv('SKYPILOT_IMAGE_BUILDER_PROTOTYPE', raising=False)
    with pytest.raises(RuntimeError, match='disabled'):
        builder_prototype.main()
