"""Capability contracts for centralized Skylet transport routing."""

import dataclasses
import enum
import re
import typing
import uuid

from sky.adaptors import common as adaptors_common

if typing.TYPE_CHECKING:
    from google.protobuf import message as protobuf_message

    from sky.schemas.generated import skyletv1_pb2
else:
    protobuf_message = adaptors_common.LazyImport('google.protobuf.message')
    skyletv1_pb2 = adaptors_common.LazyImport(
        'sky.schemas.generated.skyletv1_pb2')

SKYLET_CAPABILITIES_SCHEMA_VERSION = 1
MAX_SKYLET_CAPABILITIES_RESPONSE_BYTES = 64 * 1024
MAX_SKYLET_CAPABILITY_METHODS = 256
MAX_SKYLET_CONTRACT_VERSIONS_PER_METHOD = 64

_SERVICE_NAME_MAX_LENGTH = 255
_IDENTIFIER_PATTERN = re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,63}')
_METHOD_PATTERN = re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,127}')


class SkyletCapabilitiesParseError(ValueError):
    """The Skylet capability advertisement violates its wire contract."""


@dataclasses.dataclass(frozen=True, slots=True)
class SkyletChannelKeyV1:
    """Identity of one cluster incarnation and persisted Skylet tunnel."""

    cluster_hash: str | None
    endpoint: str
    tunnel_generation: str


@dataclasses.dataclass(frozen=True, slots=True)
class SkyletChannelSnapshotV1:
    """A generic channel bound to one fenced tunnel observation."""

    channel: typing.Any
    key: SkyletChannelKeyV1


@dataclasses.dataclass(frozen=True, slots=True)
class SkyletCapabilityChannelSnapshotV1:
    """A channel that may be used only for capability negotiation."""

    channel: typing.Any
    key: SkyletChannelKeyV1

    @property
    def publishable(self) -> bool:
        """Whether this snapshot has a cluster-incarnation fence."""
        return self.key.cluster_hash is not None


@dataclasses.dataclass(frozen=True, slots=True)
class SkyletLogicalKeyV1:
    """A channel identity extended with one observed Skylet boot."""

    channel_key: SkyletChannelKeyV1
    skylet_boot_id: uuid.UUID


class TunnelMutationResult(enum.Enum):
    """Closed outcomes for incarnation-fenced tunnel metadata writes."""

    UPDATED = 'updated'
    CONFLICT = 'conflict'
    UNFENCED_CLUSTER_INCARNATION = 'unfenced_cluster_incarnation'


@dataclasses.dataclass(frozen=True, slots=True)
class SkyletMethodCapabilityV1:
    """One immutable worker method and its semantic contract versions."""

    service: str
    method: str
    contract_versions: tuple[int, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class SkyletCapabilitiesV1:
    """Strictly parsed capability advertisement for one Skylet boot."""

    schema_version: int
    skylet_boot_id: uuid.UUID
    skylet_version: str
    skypilot_version: str
    skypilot_commit: str
    methods: tuple[SkyletMethodCapabilityV1, ...]

    def supports(self, service: str, method: str,
                 contract_version: int) -> bool:
        return any(
            capability.service == service and capability.method == method and
            contract_version in capability.contract_versions
            for capability in self.methods)


def _validate_service_name(service: str) -> bool:
    if (not service.isascii() or len(service) > _SERVICE_NAME_MAX_LENGTH):
        return False
    identifiers = service.split('.')
    return len(identifiers) >= 2 and all(
        _IDENTIFIER_PATTERN.fullmatch(identifier) is not None
        for identifier in identifiers)


def _validate_method_name(method: str) -> bool:
    return (method.isascii() and _METHOD_PATTERN.fullmatch(method) is not None)


def parse_skylet_capabilities_v1(payload: bytes) -> SkyletCapabilitiesV1:
    """Parse a bounded raw capability response into immutable values.

    The size check intentionally precedes protobuf decoding. Re-serializing a
    decoded message is not equivalent because protobuf may coalesce duplicate
    singular fields and thereby shrink the payload.
    """
    if len(payload) > MAX_SKYLET_CAPABILITIES_RESPONSE_BYTES:
        raise SkyletCapabilitiesParseError(
            'Skylet capability response exceeds the 65536-byte limit: '
            f'{len(payload)} bytes.')

    try:
        response = skyletv1_pb2.SkyletCapabilitiesV1.FromString(payload)
    except (protobuf_message.DecodeError, ValueError) as e:
        raise SkyletCapabilitiesParseError(
            'Skylet capability response is not valid protobuf.') from e

    if response.schema_version != SKYLET_CAPABILITIES_SCHEMA_VERSION:
        raise SkyletCapabilitiesParseError(
            'Unsupported Skylet capability schema version: '
            f'{response.schema_version}.')

    try:
        boot_id = uuid.UUID(response.skylet_boot_id)
    except (AttributeError, ValueError) as e:
        raise SkyletCapabilitiesParseError(
            f'Invalid Skylet boot ID: {response.skylet_boot_id!r}.') from e
    if str(boot_id) != response.skylet_boot_id:
        raise SkyletCapabilitiesParseError(
            f'Noncanonical Skylet boot ID: {response.skylet_boot_id!r}.')

    if len(response.methods) > MAX_SKYLET_CAPABILITY_METHODS:
        raise SkyletCapabilitiesParseError(
            'Skylet capability response advertises more than 256 methods: '
            f'{len(response.methods)}.')

    methods = []
    previous_key: tuple[str, str] | None = None
    for method in response.methods:
        if not _validate_service_name(method.service):
            raise SkyletCapabilitiesParseError(
                f'Invalid Skylet service name: {method.service!r}.')
        if not _validate_method_name(method.method):
            raise SkyletCapabilitiesParseError(
                f'Invalid Skylet method name: {method.method!r}.')

        key = (method.service, method.method)
        if previous_key is not None and key <= previous_key:
            raise SkyletCapabilitiesParseError(
                'Skylet capability methods must be unique and sorted: '
                f'{key!r} follows {previous_key!r}.')
        previous_key = key

        contract_versions = tuple(method.contract_versions)
        if not contract_versions:
            raise SkyletCapabilitiesParseError(
                f'Skylet method {key!r} has no contract versions.')
        if (len(contract_versions) > MAX_SKYLET_CONTRACT_VERSIONS_PER_METHOD):
            raise SkyletCapabilitiesParseError(
                f'Skylet method {key!r} advertises more than 64 contract '
                f'versions: {len(contract_versions)}.')
        if (any(version == 0 for version in contract_versions) or
                contract_versions != tuple(sorted(set(contract_versions)))):
            raise SkyletCapabilitiesParseError(
                f'Skylet method {key!r} contract versions must be positive, '
                f'unique, and sorted: {contract_versions!r}.')

        methods.append(
            SkyletMethodCapabilityV1(service=method.service,
                                     method=method.method,
                                     contract_versions=contract_versions))

    return SkyletCapabilitiesV1(schema_version=response.schema_version,
                                skylet_boot_id=boot_id,
                                skylet_version=response.skylet_version,
                                skypilot_version=response.skypilot_version,
                                skypilot_commit=response.skypilot_commit,
                                methods=tuple(methods))
