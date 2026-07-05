from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class GetRayStatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetRayStatusResponse(_message.Message):
    __slots__ = ("returncode", "stdout", "stderr")
    RETURNCODE_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    returncode: int
    stdout: str
    stderr: str
    def __init__(self, returncode: _Optional[int] = ..., stdout: _Optional[str] = ..., stderr: _Optional[str] = ...) -> None: ...
