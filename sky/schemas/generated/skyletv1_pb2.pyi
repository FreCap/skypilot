from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetCapabilitiesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SkyletMethodCapabilityV1(_message.Message):
    __slots__ = ("service", "method", "contract_versions")
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    CONTRACT_VERSIONS_FIELD_NUMBER: _ClassVar[int]
    service: str
    method: str
    contract_versions: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, service: _Optional[str] = ..., method: _Optional[str] = ..., contract_versions: _Optional[_Iterable[int]] = ...) -> None: ...

class SkyletCapabilitiesV1(_message.Message):
    __slots__ = ("schema_version", "skylet_boot_id", "skylet_version", "skypilot_version", "skypilot_commit", "methods")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    SKYLET_BOOT_ID_FIELD_NUMBER: _ClassVar[int]
    SKYLET_VERSION_FIELD_NUMBER: _ClassVar[int]
    SKYPILOT_VERSION_FIELD_NUMBER: _ClassVar[int]
    SKYPILOT_COMMIT_FIELD_NUMBER: _ClassVar[int]
    METHODS_FIELD_NUMBER: _ClassVar[int]
    schema_version: int
    skylet_boot_id: str
    skylet_version: str
    skypilot_version: str
    skypilot_commit: str
    methods: _containers.RepeatedCompositeFieldContainer[SkyletMethodCapabilityV1]
    def __init__(self, schema_version: _Optional[int] = ..., skylet_boot_id: _Optional[str] = ..., skylet_version: _Optional[str] = ..., skypilot_version: _Optional[str] = ..., skypilot_commit: _Optional[str] = ..., methods: _Optional[_Iterable[_Union[SkyletMethodCapabilityV1, _Mapping]]] = ...) -> None: ...
