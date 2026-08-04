"""Internal fail-closed authorization-v3 bootstrap and validation CLI."""

import argparse
from collections.abc import Iterator
from collections.abc import Sequence
import contextlib
import dataclasses
import io
import json
import logging
import math
import os
import pathlib
import sys
from typing import NoReturn

from sky import clouds
from sky import skypilot_config
from sky import task as task_lib
from sky.serve import constants as serve_constants
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import service_spec as service_spec_lib
from sky.serve import system_oom_recovery
from sky.skylet import constants as skylet_constants
from sky.utils.db import db_utils

_MAX_DOCUMENT_BYTES = 256 * 1024
_SYNTHETIC_REPLICA_ID = 1
_VALIDATION_REQUEST_ID = 'system-oom-authorization-bootstrap-validation'
_VALIDATION_NONCE = '0' * 64


class AuthorizationBootstrapError(RuntimeError):
    """Authorization bootstrap failed without producing trusted output."""


class _BootstrapArgumentParser(argparse.ArgumentParser):
    """Argument parser whose failures cannot echo untrusted values."""

    def error(self, message: str) -> NoReturn:
        del message
        raise AuthorizationBootstrapError('Command arguments are invalid.')

    def exit(self, status: int = 0, message: str | None = None) -> NoReturn:
        del status, message
        raise AuthorizationBootstrapError('Command arguments are invalid.')


@dataclasses.dataclass(frozen=True)
class AuthorizationBootstrapTarget:
    """One exact durable, elected zero-replica service task."""

    service_name: str
    service_hash: str
    workspace: str
    version: int
    task: task_lib.Task


@contextlib.contextmanager
def _central_postgres_selection() -> Iterator[None]:
    """Select the configured central DB before its lazy engine initializes."""
    if not os.environ.get(skylet_constants.ENV_VAR_DB_CONNECTION_URI):
        raise AuthorizationBootstrapError(
            'Central PostgreSQL configuration is unavailable.')
    marker = skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER
    previous_marker = os.environ.get(marker)
    migration_mode = skylet_constants.ENV_VAR_STATE_DB_MIGRATION_MODE
    previous_migration_mode = os.environ.get(migration_mode)
    os.environ[marker] = 'true'
    os.environ[migration_mode] = 'verify'
    try:
        dialect_name: str | None = None
        try:
            dialect_name = serve_state.get_database_engine().dialect.name
        except Exception as error:  # pylint: disable=broad-except
            raise AuthorizationBootstrapError(
                'Central PostgreSQL state is unavailable.') from error
        if dialect_name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise AuthorizationBootstrapError(
                'Central PostgreSQL state is unavailable.')
        yield
    finally:
        if previous_marker is None:
            os.environ.pop(marker, None)
        else:
            os.environ[marker] = previous_marker
        if previous_migration_mode is None:
            os.environ.pop(migration_mode, None)
        else:
            os.environ[migration_mode] = previous_migration_mode


@contextlib.contextmanager
def _suppress_internal_output() -> Iterator[None]:
    """Keep bootstrap dependencies from contaminating the closed CLI output."""
    previous_logging_disable = logging.root.manager.disable
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
            io.StringIO()):
        logging.disable(sys.maxsize)
        try:
            yield
        finally:
            logging.disable(previous_logging_disable)


def _require_single_string(values: Sequence[str],
                           field_name: str) -> tuple[str, ...]:
    if (isinstance(values, (str, bytes)) or len(values) != 1 or
            not isinstance(values[0], str) or not values[0]):
        raise AuthorizationBootstrapError(f'{field_name} is invalid.')
    return (values[0],)


def _parse_locations(
    values: Sequence[str],
) -> tuple[system_oom_recovery.AWSAuthorizationLocation, ...]:
    if len(values) != 1:
        raise AuthorizationBootstrapError('AWS locations are required.')
    locations: list[system_oom_recovery.AWSAuthorizationLocation] = []
    seen_regions: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            raise AuthorizationBootstrapError('An AWS location is invalid.')
        region, separator, raw_zones = value.partition('=')
        zones = raw_zones.split(',') if separator else []
        if (not region or len(zones) != 1 or any(not zone for zone in zones) or
                region in seen_regions or len(zones) != len(set(zones))):
            raise AuthorizationBootstrapError('An AWS location is invalid.')
        seen_regions.add(region)
        try:
            locations.append(
                system_oom_recovery.AWSAuthorizationLocation(
                    region=region, availability_zones=tuple(sorted(zones))))
        except (TypeError, ValueError) as error:
            raise AuthorizationBootstrapError(
                'An AWS location is invalid.') from error
    return tuple(sorted(locations, key=lambda location: location.region))


def _aws_offering_exists(instance_type: str, *, region: str, zone: str,
                         market_type: str) -> bool:
    try:
        offerings = clouds.AWS.regions_with_offering(
            instance_type,
            accelerators=None,
            use_spot=market_type == 'spot',
            region=region,
            zone=zone)
        return any(
            offering.name == region and offering.zones is not None and any(
                candidate.name == zone
                for candidate in offering.zones)
            for offering in offerings)
    except Exception as error:  # pylint: disable=broad-except
        raise AuthorizationBootstrapError(
            'The AWS offering catalog could not be evaluated.') from error


def _validate_aws_resource_envelope(
        envelope: system_oom_recovery.AWSRecoveryResourceEnvelope) -> None:
    if not isinstance(envelope,
                      system_oom_recovery.AWSRecoveryResourceEnvelope):
        raise AuthorizationBootstrapError(
            'The AWS recovery envelope is invalid.')
    if (len(envelope.allowed_aws_account_ids) != 1 or
            len(envelope.allowed_locations) != 1 or
            len(envelope.allowed_locations[0].availability_zones) != 1 or
            len(envelope.allowed_market_types) != 1 or
            len(envelope.allowed_instance_types) != 1):
        raise AuthorizationBootstrapError(
            'The AWS recovery envelope is not singleton.')
    for instance_type in envelope.allowed_instance_types:
        try:
            _, memory_gib = clouds.AWS.get_vcpus_mem_from_instance_type(
                instance_type)
        except Exception as error:  # pylint: disable=broad-except
            raise AuthorizationBootstrapError(
                'An authorized AWS instance type has unknown memory.'
            ) from error
        if (isinstance(memory_gib, bool) or
                not isinstance(memory_gib,
                               (int, float)) or not math.isfinite(memory_gib)):
            raise AuthorizationBootstrapError(
                'An authorized AWS instance type has unknown memory.')
        if (memory_gib <= 0 or
                memory_gib > system_oom_recovery.MAX_HOST_MEMORY_GIB_V3):
            raise AuthorizationBootstrapError(
                'An authorized AWS instance type is not within 16 GiB.')
        for location in envelope.allowed_locations:
            for zone in location.availability_zones:
                for market_type in envelope.allowed_market_types:
                    if not _aws_offering_exists(instance_type,
                                                region=location.region,
                                                zone=zone,
                                                market_type=market_type):
                        raise AuthorizationBootstrapError(
                            'An authorized AWS type/location/market offering '
                            'is absent from the catalog.')


def _validate_target_aws_resource_envelope(
    target: AuthorizationBootstrapTarget,
    envelope: system_oom_recovery.AWSRecoveryResourceEnvelope,
) -> None:
    """Validate identity, placement, and catalog inside target workspace."""
    try:
        with skypilot_config.local_active_workspace_ctx(target.workspace):
            _validate_aws_resource_envelope(envelope)
            identity = clouds.AWS.get_active_user_identity()
            if (not isinstance(identity, (list, tuple)) or len(identity) < 2 or
                    not isinstance(identity[1], str) or not identity[1]):
                raise AuthorizationBootstrapError(
                    'The active AWS identity is unavailable.')
            if envelope.allowed_aws_account_ids != (identity[1],):
                raise AuthorizationBootstrapError(
                    'The AWS account does not match the active identity.')
            # pylint: disable=protected-access
            matches_resource = (system_oom_recovery.
                                _matches_singleton_aws_authorization_resource(
                                    target.task, envelope))
            # pylint: enable=protected-access
            if not matches_resource:
                raise AuthorizationBootstrapError(
                    'The effective task does not match the AWS envelope.')
    except AuthorizationBootstrapError:
        raise
    except Exception as error:  # pylint: disable=broad-except
        raise AuthorizationBootstrapError(
            'The AWS identity or catalog could not be validated.') from error


def build_aws_resource_envelope(
    target: AuthorizationBootstrapTarget,
    *,
    aws_account_ids: Sequence[str],
    aws_locations: Sequence[str],
    market_types: Sequence[str],
    instance_types: Sequence[str],
) -> system_oom_recovery.AWSRecoveryResourceEnvelope:
    """Build and catalog-validate the exact closed AWS <=16-GiB envelope."""
    normalized_account_ids = _require_single_string(aws_account_ids,
                                                    'AWS account ID')
    normalized_locations = _parse_locations(aws_locations)
    normalized_markets = _require_single_string(market_types, 'AWS market type')
    normalized_instance_types = _require_single_string(instance_types,
                                                       'AWS instance type')
    try:
        envelope = system_oom_recovery.AWSRecoveryResourceEnvelope(
            allowed_aws_account_ids=normalized_account_ids,
            allowed_locations=normalized_locations,
            allowed_market_types=normalized_markets,
            allowed_instance_types=normalized_instance_types)
    except (TypeError, ValueError) as error:
        raise AuthorizationBootstrapError(
            'The AWS recovery envelope is invalid.') from error
    _validate_target_aws_resource_envelope(target, envelope)
    return envelope


def load_bootstrap_target(service_name: str) -> AuthorizationBootstrapTarget:
    """Load and gate one durable elected min=0 service snapshot."""
    if not isinstance(service_name, str) or not service_name:
        raise AuthorizationBootstrapError('The service name is invalid.')
    try:
        persistence_available = (
            serve_state.system_recovery_persistence_available())
    except Exception as error:  # pylint: disable=broad-except
        raise AuthorizationBootstrapError(
            'The durable service snapshot is unavailable.') from error
    if not persistence_available:
        raise AuthorizationBootstrapError(
            'Authorization bootstrap requires central PostgreSQL state.')
    try:
        snapshot = serve_state.get_system_recovery_authorization_snapshot(
            service_name)
    except Exception as error:  # pylint: disable=broad-except
        raise AuthorizationBootstrapError(
            'The durable service snapshot is unavailable.') from error
    if snapshot is None:
        raise AuthorizationBootstrapError(
            'The durable service snapshot is unavailable.')
    required_fields = {
        'service_name', 'service_hash', 'workspace', 'version', 'status',
        'pool', 'resource_action_mode', 'spec', 'yaml_content',
        'quarantined_at', 'replica_count'
    }
    if not isinstance(snapshot, dict) or set(snapshot) != required_fields:
        raise AuthorizationBootstrapError(
            'The durable service snapshot is malformed.')
    service_hash = snapshot['service_hash']
    workspace = snapshot['workspace']
    version = snapshot['version']
    spec = snapshot['spec']
    yaml_content = snapshot['yaml_content']
    if (snapshot['service_name'] != service_name or
            not isinstance(service_hash, str) or not service_hash or
            not isinstance(workspace, str) or type(version) is not int or
            version <= 0 or  # pylint: disable=unidiomatic-typecheck
            not isinstance(spec, service_spec_lib.SkyServiceSpec)
            or not isinstance(yaml_content, str) or not yaml_content
            or snapshot['quarantined_at'] is not None):
        raise AuthorizationBootstrapError(
            'The elected service version is not a committed bootstrap target.')
    try:
        spec_is_pool = bool(spec.pool)
        min_replicas = spec.min_replicas
        accelerator_floors = tuple(spec.min_replicas_by_accelerator.values())
        probe_interval = spec.endpoint_probe_interval_seconds
        readiness_timeout = spec.readiness_timeout_seconds
        replica_floor_is_zero = (
            type(min_replicas) is int and  # pylint: disable=unidiomatic-typecheck
            min_replicas == 0 and all(
                type(value) is int and value == 0  # pylint: disable=unidiomatic-typecheck
                for value in accelerator_floors))
        probe_contract_is_bounded = all(
            not isinstance(value, bool) and isinstance(value, (
                int, float)) and math.isfinite(value) and value > 0
            for value in (probe_interval, readiness_timeout))
    except Exception as error:  # pylint: disable=broad-except
        raise AuthorizationBootstrapError(
            'The elected service spec is not a bootstrap target.') from error
    if (snapshot['pool'] is not False or spec_is_pool or
            snapshot['resource_action_mode'] != 'legacy'):
        raise AuthorizationBootstrapError(
            'The service lifecycle is not eligible for recovery bootstrap.')
    if (not replica_floor_is_zero or
            snapshot['status'] != serve_state.ServiceStatus.NO_REPLICA or
            type(snapshot['replica_count']) is not int or  # pylint: disable=unidiomatic-typecheck
            snapshot['replica_count'] != 0):
        raise AuthorizationBootstrapError(
            'The service is not durably min=0 with zero replica rows.')
    if (not probe_contract_is_bounded or probe_interval > serve_constants.
            SYSTEM_RECOVERY_MAX_ELIGIBLE_PROBE_INTERVAL_SECONDS or
            readiness_timeout > serve_constants.
            SYSTEM_RECOVERY_MAX_ELIGIBLE_READINESS_TIMEOUT_SECONDS):
        raise AuthorizationBootstrapError(
            'The elected service probe contract is not recovery eligible.')
    try:
        with skypilot_config.local_active_workspace_ctx(workspace):
            task = replica_managers._build_replica_launch_task(  # pylint: disable=protected-access
                yaml_content,
                _SYNTHETIC_REPLICA_ID,
                resources_override=None,
                exact_resources_override=False,
                authoritative_service_spec=spec,
                service_name=service_name)
    except Exception as error:  # pylint: disable=broad-except
        raise AuthorizationBootstrapError(
            'The exact effective replica task could not be constructed.'
        ) from error
    return AuthorizationBootstrapTarget(service_name=service_name,
                                        service_hash=service_hash,
                                        workspace=workspace,
                                        version=version,
                                        task=task)


def _json_strings(value: object) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from _json_strings(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str):
                yield key
            yield from _json_strings(item)


def _document_contains_any(parsed_document: object,
                           sensitive_values: Sequence[str]) -> bool:
    document_strings = tuple(_json_strings(parsed_document))
    return any(sensitive_value and sensitive_value in document_string
               for sensitive_value in sensitive_values
               for document_string in document_strings)


def _assert_no_task_secret_in_document(task: task_lib.Task,
                                       parsed_document: object) -> None:
    try:
        secret_values = tuple(
            secret.get_secret_value() for secret in task.secrets.values())
        if any(not isinstance(secret_value, str)
               for secret_value in secret_values):
            raise TypeError('typed secret value is not text')
        exposes_secret = _document_contains_any(parsed_document, secret_values)
    except Exception as error:  # pylint: disable=broad-except
        raise AuthorizationBootstrapError(
            'Task secrets could not be proven absent from the document.'
        ) from error
    if exposes_secret:
        raise AuthorizationBootstrapError(
            'The authorization document would expose a task secret.')


def _assert_no_database_uri_in_document(parsed_document: object) -> None:
    database_uri = os.environ.get(skylet_constants.ENV_VAR_DB_CONNECTION_URI)
    if (database_uri and _document_contains_any(parsed_document,
                                                (database_uri,))):
        raise AuthorizationBootstrapError(
            'The authorization document would expose bootstrap configuration.')


@contextlib.contextmanager
def _installed_document(document: str) -> Iterator[None]:
    key = serve_constants.SYSTEM_OOM_RECOVERY_PROFILES_ENV_VAR
    previous = os.environ.get(key)
    os.environ[key] = document
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = previous


def validate_authorization_document(
    target: AuthorizationBootstrapTarget,
    document: str,
    *,
    expected_profile_id: str,
) -> system_oom_recovery.TrustedRecoveryAuthorizationV3:
    """Validate canonical bytes through the production parser and matcher."""
    if not isinstance(document, str):
        raise AuthorizationBootstrapError(
            'The authorization document is not text.')
    try:
        document_size = len(document.encode('utf-8'))
    except UnicodeError as error:
        raise AuthorizationBootstrapError(
            'The authorization document encoding is invalid.') from error
    if document_size > _MAX_DOCUMENT_BYTES:
        raise AuthorizationBootstrapError(
            'The authorization document exceeds the bootstrap size limit.')
    payload = document[:-1] if document.endswith('\n') else document
    if not payload or payload.endswith(('\n', '\r')):
        raise AuthorizationBootstrapError(
            'The authorization document framing is invalid.')
    try:
        profiles = system_oom_recovery.parse_authorization_document_v3(payload)
        parsed_document = json.loads(payload)
    except (TypeError, ValueError, json.JSONDecodeError,
            RecursionError) as error:
        raise AuthorizationBootstrapError(
            'The production v3 parser rejected the document.') from error
    if len(profiles) != 1:
        raise AuthorizationBootstrapError(
            'Bootstrap validation requires exactly one profile.')
    profile = profiles[0]
    if (profile.profile_id != expected_profile_id or
            profile.service_name != target.service_name or
            profile.service_hash != target.service_hash or
            profile.workspace != target.workspace):
        raise AuthorizationBootstrapError(
            'The authorization profile does not match the durable target.')
    try:
        canonical_payload = (
            system_oom_recovery.canonical_authorization_document_v3(profiles))
    except Exception as error:  # pylint: disable=broad-except
        raise AuthorizationBootstrapError(
            'The authorization document could not be canonicalized.') from error
    if canonical_payload != payload:
        raise AuthorizationBootstrapError(
            'The authorization document is not canonical JSON.')
    _validate_target_aws_resource_envelope(target, profile.resource_envelope)
    _assert_no_database_uri_in_document(parsed_document)
    _assert_no_task_secret_in_document(target.task, parsed_document)
    requested = (system_oom_recovery.RequestedRecoveryAuthorizationV3.
                 from_authorization(profile))
    intent = requested.to_intent_fields()
    intent.update({
        'service_hash': target.service_hash,
        'replica_id': _SYNTHETIC_REPLICA_ID,
        'launch_generation': _SYNTHETIC_REPLICA_ID,
        'launch_nonce': _VALIDATION_NONCE,
    })
    try:
        with skypilot_config.local_active_workspace_ctx(target.workspace):
            with _installed_document(payload):
                unbound = system_oom_recovery.create_unbound_launch_context(
                    intent,
                    service_name=target.service_name,
                    service_version=target.version,
                    controller_pid=None,
                    controller_ip=None)
                bound = system_oom_recovery.bind_launch_context(
                    unbound, _VALIDATION_REQUEST_ID)
                matched = system_oom_recovery.match_trusted_profile(
                    target.task, bound)
    except Exception as error:  # pylint: disable=broad-except
        raise AuthorizationBootstrapError(
            'The production v3 matcher rejected the document.') from error
    if matched != profile:
        raise AuthorizationBootstrapError(
            'The production v3 matcher rejected the durable target.')
    return profile


def generate_authorization_document(
    target: AuthorizationBootstrapTarget,
    *,
    profile_id: str,
    resource_envelope: system_oom_recovery.AWSRecoveryResourceEnvelope,
) -> str:
    """Generate and fully validate one canonical authorization-v3 document."""
    try:
        with skypilot_config.local_active_workspace_ctx(target.workspace):
            authorization = system_oom_recovery.create_authorization_v3(
                target.task,
                profile_id=profile_id,
                workspace=target.workspace,
                service_name=target.service_name,
                service_hash=target.service_hash,
                resource_envelope=resource_envelope)
            document = system_oom_recovery.canonical_authorization_document_v3(
                (authorization,))
    except Exception as error:  # pylint: disable=broad-except
        raise AuthorizationBootstrapError(
            'The effective task failed authorization-v3 construction.'
        ) from error
    validate_authorization_document(target,
                                    document,
                                    expected_profile_id=profile_id)
    return document


def _read_document(path: str) -> str:
    if path == '-':
        document = sys.stdin.read(_MAX_DOCUMENT_BYTES + 1)
    else:
        with pathlib.Path(path).open(encoding='utf-8') as document_file:
            document = document_file.read(_MAX_DOCUMENT_BYTES + 1)
    if len(document.encode('utf-8')) > _MAX_DOCUMENT_BYTES:
        raise AuthorizationBootstrapError(
            'The authorization document exceeds the bootstrap size limit.')
    return document


def _build_parser() -> argparse.ArgumentParser:
    parser = _BootstrapArgumentParser(
        description='Generate or validate one internal SkyServe system-OOM '
        'authorization-v3 document.')
    subparsers = parser.add_subparsers(dest='command', required=True)

    generate = subparsers.add_parser(
        'generate', help='generate canonical JSON for an exact min=0 service')
    generate.add_argument('--service-name', required=True)
    generate.add_argument('--profile-id', required=True)
    generate.add_argument('--aws-account-id', action='append', required=True)
    generate.add_argument('--aws-location',
                          action='append',
                          required=True,
                          metavar='REGION=AZ[,AZ...]')
    generate.add_argument('--market-type',
                          action='append',
                          choices=('on_demand', 'spot'),
                          required=True)
    generate.add_argument('--instance-type', action='append', required=True)

    validate = subparsers.add_parser(
        'validate', help='validate canonical JSON against the durable service')
    validate.add_argument('--service-name', required=True)
    validate.add_argument('--profile-id', required=True)
    validate.add_argument('--document-file', default='-', metavar='PATH|-')
    return parser


def run_cli(argv: Sequence[str] | None = None) -> tuple[int, str, bool]:
    """Run bootstrap without emitting anything outside the pre-import shim."""
    try:
        with _suppress_internal_output():
            args = _build_parser().parse_args(argv)
        with _suppress_internal_output(), _central_postgres_selection():
            target = load_bootstrap_target(args.service_name)
            if args.command == 'generate':
                envelope = build_aws_resource_envelope(
                    target,
                    aws_account_ids=args.aws_account_id,
                    aws_locations=args.aws_location,
                    market_types=args.market_type,
                    instance_types=args.instance_type)
                output = generate_authorization_document(
                    target,
                    profile_id=args.profile_id,
                    resource_envelope=envelope)
            else:
                document = _read_document(args.document_file)
                profile = validate_authorization_document(
                    target, document, expected_profile_id=args.profile_id)
                output = json.dumps(
                    {
                        'authorization_sha256': profile.authorization_sha256,
                        'valid': True,
                    },
                    sort_keys=True,
                    separators=(',', ':'))
        return 0, output, False
    except AuthorizationBootstrapError as error:
        return 1, f'authorization-v3 bootstrap failed: {error}', True
    except (OSError, UnicodeError):
        return (1, 'authorization-v3 bootstrap failed: the document could not '
                'be read safely.', True)
    except Exception:  # pylint: disable=broad-except
        return (
            1, 'authorization-v3 bootstrap failed: internal validation failed.',
            True)


def main(argv: Sequence[str] | None = None) -> int:
    """In-process test adapter; production uses the pre-import entrypoint."""
    exit_code, output, is_error = run_cli(argv)
    print(output, file=sys.stderr if is_error else sys.stdout)
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
