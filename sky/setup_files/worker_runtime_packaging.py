"""Deterministic packaging for the standalone coordinated worker runtime.

This module is intentionally standard-library-only.  It is loaded by the
standalone PEP 517 backend before SkyPilot or any third-party dependency is
available, and by the API server when it constructs an internal worker bundle.
"""

import argparse
import base64
import csv
import dataclasses
from email import parser as email_parser
from email import policy as email_policy
import hashlib
import io
import json
import os
import pathlib
import re
import runpy
import shutil
import stat
import tempfile
from typing import Any, Iterable, Mapping, Sequence
import zipfile

WORKER_RUNTIME_DISTRIBUTION = 'skypilot-worker-runtime-v1'
WORKER_RUNTIME_IMPORT_PACKAGE = 'skypilot_worker_runtime'
WORKER_RUNTIME_LOCK_FILENAME = 'runtime-wheel.lock.json'
WORKER_RUNTIME_LOCK_SCHEMA = 'SKYPILOT_WORKER_RUNTIME_LOCK_V1'
WORKER_RUNTIME_RESOURCE_MANIFEST = 'manifest.json'
INTERNAL_BUNDLE_MANIFEST = 'manifest.json'
INTERNAL_BUNDLE_BUILDER_VERSION = 1
INTERNAL_BUNDLE_DIGEST_DOMAIN = b'SKYPILOT_INTERNAL_WHEEL_BUNDLE_V1\0'

_SHA256_PATTERN = re.compile(r'^[0-9a-f]{64}$')
_SOURCE_COMMIT_PATTERN = re.compile(r'^[0-9a-f]{40,64}$')
_WHEEL_TIMESTAMP = (1980, 1, 1, 0, 0, 0)


def canonical_json_bytes(value: object) -> bytes:
    """Returns the one accepted JSON serialization for signed inputs."""
    return json.dumps(value,
                      ensure_ascii=True,
                      separators=(',', ':'),
                      sort_keys=True).encode('ascii')


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _require_sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f'{field} must be a lowercase SHA-256 hex digest')
    return value


def _require_nonnegative_int(value: object, field: str) -> int:
    if (not isinstance(value, int) or isinstance(value, bool) or value < 0):
        raise ValueError(f'{field} must be a nonnegative integer')
    return value


def _require_exact_keys(value: Mapping[str, object], expected: set[str],
                        context: str) -> None:
    actual = set(value)
    if actual != expected:
        raise ValueError(
            f'{context} keys must be {sorted(expected)}; got {sorted(actual)}')


def _validate_leaf_filename(filename: object) -> str:
    if not isinstance(filename, str) or not filename:
        raise ValueError('wheel filename must be a nonempty string')
    path = pathlib.PurePosixPath(filename)
    if (path.name != filename or path.is_absolute() or '..' in path.parts or
            '\\' in filename or not filename.endswith('.whl')):
        raise ValueError(f'invalid wheel filename: {filename!r}')
    return filename


@dataclasses.dataclass(frozen=True)
class WheelRecord:
    """Content identity for one wheel in a qualified artifact set."""

    filename: str
    size: int
    sha256: str

    @classmethod
    def from_path(cls, path: pathlib.Path) -> 'WheelRecord':
        if not path.is_file() or path.is_symlink():
            raise ValueError(
                f'wheel must be a regular non-symlink file: {path}')
        return cls(filename=_validate_leaf_filename(path.name),
                   size=path.stat().st_size,
                   sha256=sha256_file(path))

    @classmethod
    def from_dict(cls, value: object) -> 'WheelRecord':
        if not isinstance(value, dict):
            raise ValueError('wheel record must be an object')
        _require_exact_keys(value, {'filename', 'size', 'sha256'},
                            'wheel record')
        return cls(filename=_validate_leaf_filename(value['filename']),
                   size=_require_nonnegative_int(value['size'], 'wheel size'),
                   sha256=_require_sha256(value['sha256'], 'wheel sha256'))

    def to_dict(self) -> dict[str, object]:
        return {
            'filename': self.filename,
            'size': self.size,
            'sha256': self.sha256,
        }


@dataclasses.dataclass(frozen=True)
class InternalBundleManifest:
    """Canonical manifest for the two-wheel internal worker bundle."""

    builder_version: int
    source_input_sha256: str
    wheels: tuple[WheelRecord, WheelRecord]

    @classmethod
    def from_bytes(cls, raw: bytes) -> 'InternalBundleManifest':
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError('bundle manifest is not valid JSON') from error
        if not isinstance(value, dict):
            raise ValueError('bundle manifest must be an object')
        _require_exact_keys(
            value, {'builder_version', 'source_input_sha256', 'wheels'},
            'bundle manifest')
        if canonical_json_bytes(value) != raw:
            raise ValueError('bundle manifest is not canonical JSON')
        builder_version = _require_nonnegative_int(value['builder_version'],
                                                   'builder_version')
        source_input_sha256 = _require_sha256(value['source_input_sha256'],
                                              'source_input_sha256')
        wheels_value = value['wheels']
        if not isinstance(wheels_value, list) or len(wheels_value) != 2:
            raise ValueError('bundle manifest must contain exactly two wheels')
        wheels = tuple(WheelRecord.from_dict(item) for item in wheels_value)
        if wheels[0].filename == wheels[1].filename:
            raise ValueError('bundle wheel filenames must be distinct')
        return cls(builder_version=builder_version,
                   source_input_sha256=source_input_sha256,
                   wheels=(wheels[0], wheels[1]))

    def to_bytes(self) -> bytes:
        return canonical_json_bytes({
            'builder_version': self.builder_version,
            'source_input_sha256': self.source_input_sha256,
            'wheels': [wheel.to_dict() for wheel in self.wheels],
        })


def _runtime_version(repo_root: pathlib.Path) -> str:
    dependencies_path = repo_root / 'sky' / 'setup_files' / 'dependencies.py'
    values = runpy.run_path(str(dependencies_path))
    version = values.get('COORDINATED_WORKER_RUNTIME_PACKAGE_VERSION')
    if not isinstance(version, str) or not version:
        raise ValueError('coordinated worker runtime version is missing')
    return version


def worker_runtime_wheel_filename(version: str) -> str:
    if not version or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._+!]*',
                                   version) is None:
        raise ValueError(f'invalid worker runtime version: {version!r}')
    distribution = WORKER_RUNTIME_DISTRIBUTION.replace('-', '_')
    normalized_version = version.replace('-', '_')
    return f'{distribution}-{normalized_version}-py3-none-any.whl'


def _framed_digest(records: Iterable[tuple[str, bytes]]) -> str:
    digest = hashlib.sha256()
    for name, contents in records:
        name_bytes = name.encode('utf-8')
        digest.update(len(name_bytes).to_bytes(8, 'big'))
        digest.update(name_bytes)
        digest.update(len(contents).to_bytes(8, 'big'))
        digest.update(contents)
    return digest.hexdigest()


def _runtime_source_records(repo_root: pathlib.Path) -> list[tuple[str, bytes]]:
    project_root = (repo_root / 'addons' / 'submission-containment' /
                    'python-runtime')
    source_root = project_root / 'src' / WORKER_RUNTIME_IMPORT_PACKAGE
    required_files = [
        project_root / 'pyproject.toml', project_root / 'build_backend.py'
    ]
    for required in required_files:
        if not required.is_file() or required.is_symlink():
            raise ValueError(
                f'worker runtime build input must be a regular file: {required}'
            )
    if not source_root.is_dir() or source_root.is_symlink():
        raise ValueError(
            f'worker runtime source root is missing: {source_root}')

    records: list[tuple[str, bytes]] = []
    for path in required_files:
        records.append(
            (path.relative_to(repo_root).as_posix(), path.read_bytes()))
    for path in sorted(source_root.rglob('*')):
        if path.is_dir():
            continue
        if ('__pycache__' in path.parts or path.suffix in ('.pyc', '.pyo')):
            continue
        if path.is_symlink() or not path.is_file():
            raise ValueError(f'invalid worker runtime source input: {path}')
        records.append(
            (path.relative_to(repo_root).as_posix(), path.read_bytes()))
    records.append(('version', _runtime_version(repo_root).encode('ascii')))
    return sorted(records, key=lambda item: item[0])


def worker_runtime_source_digest(repo_root: pathlib.Path) -> str:
    return _framed_digest(_runtime_source_records(repo_root))


def _zip_info(name: str) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, date_time=_WHEEL_TIMESTAMP)
    info.compress_type = zipfile.ZIP_STORED
    info.create_system = 3
    info.external_attr = (stat.S_IFREG | 0o644) << 16
    return info


def _record_hash(contents: bytes) -> str:
    encoded = base64.urlsafe_b64encode(hashlib.sha256(contents).digest())
    return f'sha256={encoded.rstrip(b"=").decode("ascii")}'


def _record_bytes(entries: Mapping[str, bytes], record_name: str) -> bytes:
    output = io.StringIO(newline='')
    writer = csv.writer(output, lineterminator='\n')
    for name in sorted(entries):
        contents = entries[name]
        writer.writerow((name, _record_hash(contents), len(contents)))
    writer.writerow((record_name, '', ''))
    return output.getvalue().encode('utf-8')


def build_worker_runtime_wheel(repo_root: pathlib.Path,
                               output_dir: pathlib.Path) -> pathlib.Path:
    """Builds the canonical standalone wheel without third-party tooling."""
    repo_root = repo_root.resolve()
    version = _runtime_version(repo_root)
    project_root = (repo_root / 'addons' / 'submission-containment' /
                    'python-runtime')
    source_root = project_root / 'src' / WORKER_RUNTIME_IMPORT_PACKAGE
    wheel_name = worker_runtime_wheel_filename(version)
    dist_info = f'{WORKER_RUNTIME_DISTRIBUTION.replace("-", "_")}-{version}.dist-info'

    entries: dict[str, bytes] = {}
    for path in sorted(source_root.rglob('*')):
        if path.is_dir():
            continue
        if ('__pycache__' in path.parts or path.suffix in ('.pyc', '.pyo')):
            continue
        if path.is_symlink() or not path.is_file():
            raise ValueError(f'invalid worker runtime source input: {path}')
        relative = path.relative_to(source_root).as_posix()
        entries[
            f'{WORKER_RUNTIME_IMPORT_PACKAGE}/{relative}'] = path.read_bytes()

    metadata = (
        'Metadata-Version: 2.1\n'
        f'Name: {WORKER_RUNTIME_DISTRIBUTION}\n'
        f'Version: {version}\n'
        'Summary: Provider-free coordinated worker runtime for SkyPilot\n'
        'License: Apache-2.0\n'
        'Requires-Python: >=3.10\n'
        '\n').encode('utf-8')
    wheel_metadata = ('Wheel-Version: 1.0\n'
                      'Generator: skypilot-worker-runtime-packager-v1\n'
                      'Root-Is-Purelib: true\n'
                      'Tag: py3-none-any\n'
                      '\n').encode('ascii')
    entries[f'{dist_info}/METADATA'] = metadata
    entries[f'{dist_info}/WHEEL'] = wheel_metadata
    record_name = f'{dist_info}/RECORD'
    record = _record_bytes(entries, record_name)

    output_dir.mkdir(parents=True, exist_ok=True)
    wheel_path = output_dir / wheel_name
    temporary_path = output_dir / f'.{wheel_name}.tmp-{os.getpid()}'
    try:
        with zipfile.ZipFile(temporary_path,
                             mode='w',
                             compression=zipfile.ZIP_STORED) as wheel:
            for name in sorted(entries):
                wheel.writestr(_zip_info(name), entries[name])
            wheel.writestr(_zip_info(record_name), record)
        os.replace(temporary_path, wheel_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    verify_worker_runtime_wheel(wheel_path, version)
    return wheel_path


def _validate_wheel_member(name: str) -> None:
    path = pathlib.PurePosixPath(name)
    if (not name or '\\' in name or path.is_absolute() or '..' in path.parts or
            any(part in ('', '.') for part in path.parts)):
        raise ValueError(f'invalid wheel member path: {name!r}')


def _verify_record(wheel: zipfile.ZipFile, record_name: str,
                   names: Sequence[str]) -> None:
    try:
        rows = list(
            csv.reader(
                io.StringIO(wheel.read(record_name).decode('utf-8'),
                            newline='')))
    except (KeyError, UnicodeDecodeError, csv.Error) as error:
        raise ValueError('worker runtime RECORD is invalid') from error
    if any(len(row) != 3 for row in rows):
        raise ValueError('worker runtime RECORD rows must have three fields')
    by_name = {row[0]: row[1:] for row in rows}
    if len(by_name) != len(rows) or set(by_name) != set(names):
        raise ValueError('worker runtime RECORD inventory mismatch')
    for name in names:
        digest, size = by_name[name]
        if name == record_name:
            if digest or size:
                raise ValueError('worker runtime RECORD must not hash itself')
            continue
        contents = wheel.read(name)
        if digest != _record_hash(contents) or size != str(len(contents)):
            raise ValueError(f'worker runtime RECORD mismatch for {name}')


def verify_worker_runtime_wheel(path: pathlib.Path,
                                expected_version: str) -> WheelRecord:
    """Verifies ownership, metadata, inventory, and hashes for a wheel."""
    expected_filename = worker_runtime_wheel_filename(expected_version)
    if path.name != expected_filename:
        raise ValueError(
            f'worker runtime wheel must be named {expected_filename!r}')
    if not path.is_file() or path.is_symlink():
        raise ValueError('worker runtime wheel must be a regular file')

    dist_info = (f'{WORKER_RUNTIME_DISTRIBUTION.replace("-", "_")}-'
                 f'{expected_version}.dist-info')
    metadata_name = f'{dist_info}/METADATA'
    wheel_name = f'{dist_info}/WHEEL'
    record_name = f'{dist_info}/RECORD'
    required = {
        f'{WORKER_RUNTIME_IMPORT_PACKAGE}/__init__.py', metadata_name,
        wheel_name, record_name
    }
    try:
        with zipfile.ZipFile(path) as wheel:
            names = wheel.namelist()
            if len(names) != len(set(names)):
                raise ValueError(
                    'worker runtime wheel contains duplicate paths')
            for info in wheel.infolist():
                _validate_wheel_member(info.filename)
                mode = info.external_attr >> 16
                if stat.S_ISLNK(mode) or info.is_dir():
                    raise ValueError(
                        'worker runtime wheel contains a link or directory entry'
                    )
            if not required.issubset(names):
                raise ValueError(
                    'worker runtime wheel is missing required files')
            allowed_prefixes = (f'{WORKER_RUNTIME_IMPORT_PACKAGE}/',
                                f'{dist_info}/')
            if any(not name.startswith(allowed_prefixes) for name in names):
                raise ValueError('worker runtime wheel owns an unexpected path')
            if any(name.startswith('sky/') for name in names):
                raise ValueError('worker runtime wheel must not own sky files')
            if f'{dist_info}/entry_points.txt' in names:
                raise ValueError(
                    'worker runtime wheel must not expose entrypoints')

            metadata = email_parser.BytesParser(
                policy=email_policy.compat32).parsebytes(
                    wheel.read(metadata_name), headersonly=True)
            if metadata.get('Name') != WORKER_RUNTIME_DISTRIBUTION:
                raise ValueError('worker runtime distribution name mismatch')
            if metadata.get('Version') != expected_version:
                raise ValueError('worker runtime distribution version mismatch')
            if metadata.get_all('Requires-Dist'):
                raise ValueError(
                    'worker runtime v1 packaging foundation has dependencies')
            wheel_headers = email_parser.BytesParser(
                policy=email_policy.compat32).parsebytes(wheel.read(wheel_name),
                                                         headersonly=True)
            if wheel_headers.get('Root-Is-Purelib') != 'true':
                raise ValueError('worker runtime wheel must be pure Python')
            if wheel_headers.get_all('Tag') != ['py3-none-any']:
                raise ValueError('worker runtime wheel tag mismatch')
            _verify_record(wheel, record_name, names)
    except zipfile.BadZipFile as error:
        raise ValueError(
            'worker runtime wheel is not a valid ZIP file') from error
    return WheelRecord.from_path(path)


def _load_canonical_json(path: pathlib.Path, context: str) -> dict[str, Any]:
    raw = path.read_bytes()
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f'{context} is not valid JSON') from error
    if not isinstance(value, dict):
        raise ValueError(f'{context} must be an object')
    if canonical_json_bytes(value) != raw:
        raise ValueError(f'{context} is not canonical JSON')
    return value


def write_worker_runtime_lock(repo_root: pathlib.Path, wheel: pathlib.Path,
                              source_commit: str) -> pathlib.Path:
    if _SOURCE_COMMIT_PATTERN.fullmatch(source_commit) is None:
        raise ValueError('source_commit must be a full lowercase Git object ID')
    version = _runtime_version(repo_root)
    record = verify_worker_runtime_wheel(wheel, version)
    lock = {
        'dependencies': [],
        'distribution': WORKER_RUNTIME_DISTRIBUTION,
        'schema': WORKER_RUNTIME_LOCK_SCHEMA,
        'source_commit': source_commit,
        'source_sha256': worker_runtime_source_digest(repo_root),
        'version': version,
        'wheel': record.to_dict(),
    }
    lock_path = (repo_root / 'addons' / 'submission-containment' /
                 'python-runtime' / WORKER_RUNTIME_LOCK_FILENAME)
    lock_path.write_bytes(canonical_json_bytes(lock))
    return lock_path


def verify_worker_runtime_lock(repo_root: pathlib.Path, wheel: pathlib.Path,
                               lock_path: pathlib.Path) -> WheelRecord:
    lock = _load_canonical_json(lock_path, 'worker runtime lock')
    _require_exact_keys(
        lock, {
            'dependencies', 'distribution', 'schema', 'source_commit',
            'source_sha256', 'version', 'wheel'
        }, 'worker runtime lock')
    if lock['schema'] != WORKER_RUNTIME_LOCK_SCHEMA:
        raise ValueError('worker runtime lock schema mismatch')
    if lock['distribution'] != WORKER_RUNTIME_DISTRIBUTION:
        raise ValueError('worker runtime lock distribution mismatch')
    version = _runtime_version(repo_root)
    if lock['version'] != version:
        raise ValueError('worker runtime lock version mismatch')
    source_commit = lock['source_commit']
    if (not isinstance(source_commit, str) or
            _SOURCE_COMMIT_PATTERN.fullmatch(source_commit) is None):
        raise ValueError('worker runtime lock source_commit is invalid')
    if lock['dependencies'] != []:
        raise ValueError('worker runtime v1 lock dependencies must be empty')
    if _require_sha256(
            lock['source_sha256'],
            'source_sha256') != (worker_runtime_source_digest(repo_root)):
        raise ValueError('worker runtime source does not match its lock')
    locked_record = WheelRecord.from_dict(lock['wheel'])
    actual_record = verify_worker_runtime_wheel(wheel, version)
    if actual_record != locked_record:
        raise ValueError('worker runtime wheel does not match its lock')
    return actual_record


def write_release_resource(wheel: pathlib.Path, resource_dir: pathlib.Path,
                           version: str) -> pathlib.Path:
    record = verify_worker_runtime_wheel(wheel, version)
    resource_dir.mkdir(parents=True, exist_ok=True)
    for child in resource_dir.iterdir():
        if child.name not in (record.filename,
                              WORKER_RUNTIME_RESOURCE_MANIFEST):
            raise ValueError(f'unexpected release resource file: {child.name}')
    destination = resource_dir / record.filename
    if wheel.resolve() != destination.resolve():
        shutil.copyfile(wheel, destination)
    manifest = {
        'filename': record.filename,
        'sha256': record.sha256,
        'size': record.size,
        'version': version,
    }
    (resource_dir / WORKER_RUNTIME_RESOURCE_MANIFEST).write_bytes(
        canonical_json_bytes(manifest))
    return destination


def load_release_worker_runtime_artifact(resource_dir: pathlib.Path,
                                         expected_version: str) -> pathlib.Path:
    manifest_path = resource_dir / WORKER_RUNTIME_RESOURCE_MANIFEST
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise ValueError('worker runtime release manifest is missing')
    manifest = _load_canonical_json(manifest_path,
                                    'worker runtime release manifest')
    _require_exact_keys(manifest, {'filename', 'version', 'size', 'sha256'},
                        'worker runtime release manifest')
    if manifest['version'] != expected_version:
        raise ValueError('worker runtime release version mismatch')
    record = WheelRecord.from_dict({
        'filename': manifest['filename'],
        'size': manifest['size'],
        'sha256': manifest['sha256'],
    })
    actual_names = {child.name for child in resource_dir.iterdir()}
    expected_names = {WORKER_RUNTIME_RESOURCE_MANIFEST, record.filename}
    if actual_names != expected_names:
        raise ValueError('worker runtime release resource inventory mismatch')
    wheel = resource_dir / record.filename
    actual_record = verify_worker_runtime_wheel(wheel, expected_version)
    if actual_record != record:
        raise ValueError('worker runtime release wheel digest mismatch')
    return wheel


def make_internal_bundle_manifest(
        source_input_sha256: str, main_wheel: pathlib.Path,
        worker_wheel: pathlib.Path) -> InternalBundleManifest:
    return InternalBundleManifest(
        builder_version=INTERNAL_BUNDLE_BUILDER_VERSION,
        source_input_sha256=_require_sha256(source_input_sha256,
                                            'source_input_sha256'),
        wheels=(WheelRecord.from_path(main_wheel),
                WheelRecord.from_path(worker_wheel)))


def internal_bundle_digest(manifest_bytes: bytes) -> str:
    return hashlib.sha256(INTERNAL_BUNDLE_DIGEST_DOMAIN +
                          manifest_bytes).hexdigest()


def verify_internal_bundle(
    directory: pathlib.Path,
    expected_digest: str | None = None,
    expected_source_input_sha256: str | None = None,
    expected_worker_record: WheelRecord | None = None
) -> tuple[InternalBundleManifest, str]:
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError('internal wheel bundle must be a directory')
    manifest_path = directory / INTERNAL_BUNDLE_MANIFEST
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise ValueError('internal wheel bundle manifest is missing')
    raw = manifest_path.read_bytes()
    manifest = InternalBundleManifest.from_bytes(raw)
    if manifest.builder_version != INTERNAL_BUNDLE_BUILDER_VERSION:
        raise ValueError('internal wheel bundle builder version mismatch')
    if (expected_source_input_sha256 is not None and
            manifest.source_input_sha256 != expected_source_input_sha256):
        raise ValueError('internal wheel bundle source input mismatch')
    if (expected_worker_record is not None and
            manifest.wheels[1] != expected_worker_record):
        raise ValueError('internal wheel bundle worker wheel mismatch')
    if not manifest.wheels[0].filename.startswith('skypilot-'):
        raise ValueError('internal wheel bundle main wheel must be first')
    expected_runtime_prefix = WORKER_RUNTIME_DISTRIBUTION.replace('-', '_')
    if not manifest.wheels[1].filename.startswith(
            f'{expected_runtime_prefix}-'):
        raise ValueError('internal wheel bundle worker wheel must be second')

    expected_names = {
        INTERNAL_BUNDLE_MANIFEST,
        *(record.filename for record in manifest.wheels),
    }
    actual_names = {child.name for child in directory.iterdir()}
    if actual_names != expected_names:
        raise ValueError('internal wheel bundle inventory mismatch')
    for expected_record in manifest.wheels:
        actual_record = WheelRecord.from_path(directory /
                                              expected_record.filename)
        if actual_record != expected_record:
            raise ValueError(f'internal wheel bundle digest mismatch: '
                             f'{expected_record.filename}')
    digest = internal_bundle_digest(raw)
    if expected_digest is not None:
        _require_sha256(expected_digest, 'expected bundle digest')
        if digest != expected_digest:
            raise ValueError('internal wheel bundle manifest digest mismatch')
    return manifest, digest


def materialize_internal_bundle(cache_root: pathlib.Path,
                                manifest: InternalBundleManifest,
                                main_wheel: pathlib.Path,
                                worker_wheel: pathlib.Path) -> pathlib.Path:
    manifest_bytes = manifest.to_bytes()
    digest = internal_bundle_digest(manifest_bytes)
    destination = cache_root / digest
    if destination.exists():
        verify_internal_bundle(
            destination,
            expected_digest=digest,
            expected_source_input_sha256=(manifest.source_input_sha256),
            expected_worker_record=manifest.wheels[1])
        return destination

    cache_root.mkdir(parents=True, exist_ok=True)
    temporary = pathlib.Path(
        tempfile.mkdtemp(prefix=f'.{digest}.', dir=cache_root))
    try:
        shutil.copyfile(main_wheel, temporary / manifest.wheels[0].filename)
        shutil.copyfile(worker_wheel, temporary / manifest.wheels[1].filename)
        (temporary / INTERNAL_BUNDLE_MANIFEST).write_bytes(manifest_bytes)
        verify_internal_bundle(
            temporary,
            expected_digest=digest,
            expected_source_input_sha256=(manifest.source_input_sha256),
            expected_worker_record=manifest.wheels[1])
        try:
            temporary.rename(destination)
        except FileExistsError:
            verify_internal_bundle(
                destination,
                expected_digest=digest,
                expected_source_input_sha256=(manifest.source_input_sha256),
                expected_worker_record=manifest.wheels[1])
        return destination
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def render_remote_bundle_verifier(expected_digest: str,
                                  expected_runtime_version: str) -> str:
    """Renders a self-contained verifier that prints main/runtime paths."""
    runtime_filename = worker_runtime_wheel_filename(expected_runtime_version)
    domain = INTERNAL_BUNDLE_DIGEST_DOMAIN
    return f'''import hashlib
import json
import pathlib
import sys

bundle = pathlib.Path(sys.argv[1])
manifest_path = bundle / {INTERNAL_BUNDLE_MANIFEST!r}
if not bundle.is_dir() or bundle.is_symlink():
    raise SystemExit('invalid SkyPilot wheel bundle directory')
if not manifest_path.is_file() or manifest_path.is_symlink():
    raise SystemExit('missing SkyPilot wheel bundle manifest')
raw = manifest_path.read_bytes()
try:
    manifest = json.loads(raw)
except (UnicodeDecodeError, json.JSONDecodeError) as error:
    raise SystemExit('invalid SkyPilot wheel bundle manifest') from error
canonical = json.dumps(manifest, ensure_ascii=True, separators=(',', ':'), sort_keys=True).encode('ascii')
if raw != canonical:
    raise SystemExit('non-canonical SkyPilot wheel bundle manifest')
if set(manifest) != {{'builder_version', 'source_input_sha256', 'wheels'}}:
    raise SystemExit('invalid SkyPilot wheel bundle manifest keys')
if manifest['builder_version'] != {INTERNAL_BUNDLE_BUILDER_VERSION!r}:
    raise SystemExit('unsupported SkyPilot wheel bundle builder')
if not isinstance(manifest['source_input_sha256'], str) or len(manifest['source_input_sha256']) != 64:
    raise SystemExit('invalid SkyPilot wheel bundle source digest')
wheels = manifest['wheels']
if not isinstance(wheels, list) or len(wheels) != 2:
    raise SystemExit('SkyPilot wheel bundle must contain two wheels')
for record in wheels:
    if not isinstance(record, dict) or set(record) != {{'filename', 'size', 'sha256'}}:
        raise SystemExit('invalid SkyPilot wheel record')
    if not isinstance(record['filename'], str) or pathlib.PurePosixPath(record['filename']).name != record['filename'] or not record['filename'].endswith('.whl'):
        raise SystemExit('invalid SkyPilot wheel filename')
    if not isinstance(record['size'], int) or isinstance(record['size'], bool) or record['size'] < 0:
        raise SystemExit('invalid SkyPilot wheel size')
    if not isinstance(record['sha256'], str) or len(record['sha256']) != 64:
        raise SystemExit('invalid SkyPilot wheel digest')
if not wheels[0]['filename'].startswith('skypilot-') or wheels[1]['filename'] != {runtime_filename!r}:
    raise SystemExit('unexpected SkyPilot wheel bundle order')
expected_names = {{{INTERNAL_BUNDLE_MANIFEST!r}, wheels[0]['filename'], wheels[1]['filename']}}
if {{path.name for path in bundle.iterdir()}} != expected_names:
    raise SystemExit('unexpected SkyPilot wheel bundle inventory')
for record in wheels:
    path = bundle / record['filename']
    if not path.is_file() or path.is_symlink():
        raise SystemExit('SkyPilot wheel is not a regular file')
    contents = path.read_bytes()
    if len(contents) != record['size'] or hashlib.sha256(contents).hexdigest() != record['sha256']:
        raise SystemExit('SkyPilot wheel digest mismatch')
digest = hashlib.sha256({domain!r} + raw).hexdigest()
if digest != {expected_digest!r}:
    raise SystemExit('SkyPilot wheel bundle digest mismatch')
print(bundle / wheels[0]['filename'])
print(bundle / wheels[1]['filename'])'''


def render_installed_distribution_probe(expected_sky_version: str,
                                        expected_runtime_version: str) -> str:
    return f'''import importlib.metadata

if importlib.metadata.version('skypilot') != {expected_sky_version!r}:
    raise SystemExit('installed SkyPilot version mismatch')
if importlib.metadata.version({WORKER_RUNTIME_DISTRIBUTION!r}) != {expected_runtime_version!r}:
    raise SystemExit('installed worker runtime version mismatch')
import sky
import skypilot_worker_runtime
if sky.__version__ != {expected_sky_version!r}:
    raise SystemExit('imported SkyPilot version mismatch')'''


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)

    build = subparsers.add_parser('build-wheel')
    build.add_argument('--repo-root', type=pathlib.Path, required=True)
    build.add_argument('--output-dir', type=pathlib.Path, required=True)

    lock = subparsers.add_parser('write-lock')
    lock.add_argument('--repo-root', type=pathlib.Path, required=True)
    lock.add_argument('--wheel', type=pathlib.Path, required=True)
    lock.add_argument('--source-commit', required=True)

    resource = subparsers.add_parser('write-resource')
    resource.add_argument('--repo-root', type=pathlib.Path, required=True)
    resource.add_argument('--wheel', type=pathlib.Path, required=True)
    resource.add_argument('--resource-dir', type=pathlib.Path, required=True)
    return parser.parse_args()


def _main() -> None:
    args = _parse_args()
    if args.command == 'build-wheel':
        print(build_worker_runtime_wheel(args.repo_root, args.output_dir))
    elif args.command == 'write-lock':
        print(
            write_worker_runtime_lock(args.repo_root, args.wheel,
                                      args.source_commit))
    elif args.command == 'write-resource':
        print(
            write_release_resource(args.wheel, args.resource_dir,
                                   _runtime_version(args.repo_root)))
    else:
        raise AssertionError(f'unhandled command: {args.command}')


if __name__ == '__main__':
    _main()
