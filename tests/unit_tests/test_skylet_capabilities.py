"""Conformance tests for additive Skylet capability advertisement."""

import concurrent.futures
import dataclasses
from unittest import mock
import uuid

import grpc
import pytest

import sky
from sky.adaptors import common as adaptors_common
from sky.backends import backend_utils
from sky.backends import skylet_client
from sky.backends import skylet_transport
from sky.schemas.generated import jobsv1_pb2
from sky.schemas.generated import jobsv1_pb2_grpc
from sky.schemas.generated import skyletv1_pb2
from sky.schemas.generated import skyletv1_pb2_grpc
from sky.skylet import configs
from sky.skylet import constants
from sky.skylet import services

_BOOT_ID = '12345678-1234-4abc-9234-56789abcdef0'
_CAPABILITY_PATH = '/skylet.v1.CapabilitiesService/GetCapabilities'


def test_transport_defers_optional_protobuf_runtime_import():
    assert isinstance(skylet_transport.protobuf_message,
                      adaptors_common.LazyImport)


def _method(
    *,
    service: str = 'jobs.v1.JobsService',
    method: str = 'GetJobStatus',
    contract_versions: tuple[int, ...] = (1,),
) -> skyletv1_pb2.SkyletMethodCapabilityV1:
    return skyletv1_pb2.SkyletMethodCapabilityV1(
        service=service, method=method, contract_versions=contract_versions)


def _payload(
    *,
    schema_version: int = 1,
    skylet_boot_id: str = _BOOT_ID,
    methods: tuple[skyletv1_pb2.SkyletMethodCapabilityV1, ...] = (_method(),),
) -> bytes:
    return skyletv1_pb2.SkyletCapabilitiesV1(
        schema_version=schema_version,
        skylet_boot_id=skylet_boot_id,
        skylet_version='43',
        skypilot_version='1.1.0',
        skypilot_commit='abc123',
        methods=methods).SerializeToString(deterministic=True)


def test_parser_returns_immutable_typed_values_and_exact_membership():
    capabilities = skylet_transport.parse_skylet_capabilities_v1(_payload())

    assert capabilities.schema_version == 1
    assert capabilities.skylet_boot_id == uuid.UUID(_BOOT_ID)
    assert capabilities.methods == (skylet_transport.SkyletMethodCapabilityV1(
        service='jobs.v1.JobsService',
        method='GetJobStatus',
        contract_versions=(1,)),)
    assert capabilities.supports('jobs.v1.JobsService', 'GetJobStatus', 1)
    assert not capabilities.supports('jobs.v1.JobsService', 'GetJobStatus', 2)
    assert not hasattr(capabilities, '__dict__')
    with pytest.raises(dataclasses.FrozenInstanceError):
        capabilities.skylet_version = 'mutated'  # type: ignore[misc]


def test_parser_accepts_future_additive_unknown_fields():
    # Field 100, varint value 1. Proto3 retains but ignores the unknown field.
    payload = _payload() + b'\xa0\x06\x01'

    capabilities = skylet_transport.parse_skylet_capabilities_v1(payload)

    assert capabilities.skylet_boot_id == uuid.UUID(_BOOT_ID)


def test_parser_accepts_valid_capability_omission():
    capabilities = skylet_transport.parse_skylet_capabilities_v1(
        _payload(methods=()))

    assert not capabilities.methods
    assert not capabilities.supports('jobs.v1.JobsService', 'GetJobStatus', 1)


def test_parser_rejects_malformed_protobuf():
    with pytest.raises(skylet_transport.SkyletCapabilitiesParseError,
                       match='not valid protobuf'):
        skylet_transport.parse_skylet_capabilities_v1(b'\x80')


def test_parser_rejects_oversized_raw_payload_before_decode(monkeypatch):
    from_string = mock.Mock()
    fake_pb2 = mock.Mock()
    fake_pb2.SkyletCapabilitiesV1.FromString = from_string
    monkeypatch.setattr(skylet_transport, 'skyletv1_pb2', fake_pb2)

    with pytest.raises(skylet_transport.SkyletCapabilitiesParseError,
                       match='65536-byte limit'):
        skylet_transport.parse_skylet_capabilities_v1(b'x' * 65537)

    from_string.assert_not_called()


def test_parser_bounds_original_bytes_that_shrink_after_decode():
    base = _payload()
    # Duplicate known singular schema fields are coalesced by protobuf decode,
    # so this valid wire payload serializes much smaller after parsing.
    oversized = base + b'\x08\x01' * 32769
    decoded = skyletv1_pb2.SkyletCapabilitiesV1.FromString(oversized)
    assert len(oversized) > (
        skylet_transport.MAX_SKYLET_CAPABILITIES_RESPONSE_BYTES)
    assert len(decoded.SerializeToString()) < (
        skylet_transport.MAX_SKYLET_CAPABILITIES_RESPONSE_BYTES)

    with pytest.raises(skylet_transport.SkyletCapabilitiesParseError,
                       match='65536-byte limit'):
        skylet_transport.parse_skylet_capabilities_v1(oversized)


def test_parser_accepts_exact_raw_response_limit():
    base = _payload()
    padding_size = (skylet_transport.MAX_SKYLET_CAPABILITIES_RESPONSE_BYTES -
                    len(base))
    # One three-byte unknown varint plus two-byte duplicate schema fields fill
    # the odd-sized remainder with a valid protobuf wire representation.
    assert padding_size >= 3 and (padding_size - 3) % 2 == 0
    payload = base + b'\xa0\x06\x01' + b'\x08\x01' * ((padding_size - 3) // 2)
    assert len(payload) == (
        skylet_transport.MAX_SKYLET_CAPABILITIES_RESPONSE_BYTES)

    capabilities = skylet_transport.parse_skylet_capabilities_v1(payload)

    assert capabilities.skylet_boot_id == uuid.UUID(_BOOT_ID)


@pytest.mark.parametrize('schema_version', [0, 2, 2**32 - 1])
def test_parser_rejects_unknown_schema_versions(schema_version):
    with pytest.raises(skylet_transport.SkyletCapabilitiesParseError,
                       match='schema version'):
        skylet_transport.parse_skylet_capabilities_v1(
            _payload(schema_version=schema_version))


@pytest.mark.parametrize('boot_id', [
    '',
    'not-a-uuid',
    _BOOT_ID.upper(),
    '{12345678-1234-4abc-9234-56789abcdef0}',
])
def test_parser_rejects_noncanonical_boot_ids(boot_id):
    with pytest.raises(skylet_transport.SkyletCapabilitiesParseError,
                       match='boot ID'):
        skylet_transport.parse_skylet_capabilities_v1(
            _payload(skylet_boot_id=boot_id))


@pytest.mark.parametrize('service', [
    'JobsService',
    'jobs..JobsService',
    '9jobs.v1.JobsService',
    'jobs.v1.Jobs-Service',
    'j\N{LATIN SMALL LETTER O WITH DIAERESIS}bs.v1.JobsService',
    f'jobs.{"x" * 65}.JobsService',
    '.'.join(('a' * 64,) * 4),
])
def test_parser_rejects_invalid_service_names(service):
    with pytest.raises(skylet_transport.SkyletCapabilitiesParseError,
                       match='service name'):
        skylet_transport.parse_skylet_capabilities_v1(
            _payload(methods=(_method(service=service),)))


@pytest.mark.parametrize('method', [
    '',
    '9GetJobStatus',
    'Get.JobStatus',
    'Get-JobStatus',
    'G\N{LATIN SMALL LETTER E WITH ACUTE}tJobStatus',
    'x' * 129,
])
def test_parser_rejects_invalid_method_names(method):
    with pytest.raises(skylet_transport.SkyletCapabilitiesParseError,
                       match='method name'):
        skylet_transport.parse_skylet_capabilities_v1(
            _payload(methods=(_method(method=method),)))


@pytest.mark.parametrize('versions', [
    (),
    (0,),
    (2, 1),
    (1, 1),
    tuple(range(1, 66)),
])
def test_parser_rejects_invalid_contract_versions(versions):
    with pytest.raises(skylet_transport.SkyletCapabilitiesParseError,
                       match='contract versions'):
        skylet_transport.parse_skylet_capabilities_v1(
            _payload(methods=(_method(contract_versions=versions),)))


@pytest.mark.parametrize('methods', [
    (_method(method='StatusB'), _method(method='StatusA')),
    (_method(), _method()),
])
def test_parser_rejects_unsorted_or_duplicate_methods(methods):
    with pytest.raises(skylet_transport.SkyletCapabilitiesParseError,
                       match='unique and sorted'):
        skylet_transport.parse_skylet_capabilities_v1(_payload(methods=methods))


def test_parser_rejects_more_than_256_methods():
    methods = tuple(
        _method(method=f'GetStatus{index:03d}') for index in range(257))

    with pytest.raises(skylet_transport.SkyletCapabilitiesParseError,
                       match='more than 256 methods'):
        skylet_transport.parse_skylet_capabilities_v1(_payload(methods=methods))


def test_parser_accepts_exact_name_version_and_method_count_limits():
    service = '.'.join(('a' * 64, 'b' * 64, 'c' * 64, 'd' * 60))
    assert len(service) == 255
    boundary_method = _method(service=service,
                              method='x' * 128,
                              contract_versions=tuple(range(1, 65)))
    methods = tuple(
        _method(method=f'GetStatus{index:03d}')
        for index in range(255)) + (boundary_method,)
    methods = tuple(
        sorted(methods,
               key=lambda capability: (capability.service, capability.method)))

    capabilities = skylet_transport.parse_skylet_capabilities_v1(
        _payload(methods=methods))

    assert len(capabilities.methods) == 256
    assert capabilities.supports(service, 'x' * 128, 64)


def test_servicer_advertisement_is_descriptor_backed_and_immutable():
    impl = services.CapabilitiesServiceImpl(_BOOT_ID)

    first = impl.GetCapabilities(skyletv1_pb2.GetCapabilitiesRequest(),
                                 mock.Mock())
    first.skylet_version = 'mutated'
    first.methods.clear()
    second = impl.GetCapabilities(skyletv1_pb2.GetCapabilitiesRequest(),
                                  mock.Mock())

    jobs_service = jobsv1_pb2.DESCRIPTOR.services_by_name['JobsService']
    get_job_status = jobs_service.methods_by_name['GetJobStatus']
    assert second.schema_version == 1
    assert second.skylet_boot_id == _BOOT_ID
    assert second.skylet_version == constants.SKYLET_VERSION
    assert second.skypilot_version == sky.__version__
    assert second.skypilot_commit == sky.__commit__
    assert [(method.service, method.method, tuple(method.contract_versions))
            for method in second.methods] == [(jobs_service.full_name,
                                               get_job_status.name, (1,))]
    assert second.SerializeToString(
        deterministic=True) == (impl.GetCapabilities(
            skyletv1_pb2.GetCapabilitiesRequest(),
            mock.Mock()).SerializeToString(deterministic=True))


def test_servicer_boot_id_is_stable_per_instance_and_changes_per_boot():
    first = services.CapabilitiesServiceImpl(_BOOT_ID)
    next_boot_id = '87654321-4321-4def-a234-56789abcdef0'
    second = services.CapabilitiesServiceImpl(next_boot_id)

    first_ids = {
        first.GetCapabilities(skyletv1_pb2.GetCapabilitiesRequest(),
                              mock.Mock()).skylet_boot_id for _ in range(2)
    }
    assert first_ids == {_BOOT_ID}
    assert second.GetCapabilities(skyletv1_pb2.GetCapabilitiesRequest(),
                                  mock.Mock()).skylet_boot_id == next_boot_id


def test_start_grpc_server_registers_one_generated_boot_id(
        monkeypatch, tmp_path):
    # Importing the daemon constructs its global event loop and initializes the
    # Skylet config database. Keep that side effect out of xdist collection and
    # give this worker a private runtime directory.
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    monkeypatch.setattr(configs, '_DB_PATH', None)
    from sky.skylet import skylet  # pylint: disable=import-outside-toplevel

    generated_boot_id = uuid.UUID(_BOOT_ID)
    fake_server = mock.Mock()
    fake_server.add_insecure_port.return_value = 46590

    with mock.patch.object(skylet.grpc, 'server', return_value=fake_server), \
         mock.patch.object(skylet.uuid, 'uuid4',
                           return_value=generated_boot_id) as uuid4, \
         mock.patch.object(
             skylet.skyletv1_pb2_grpc,
             'add_CapabilitiesServiceServicer_to_server') as add_service:
        returned = skylet.start_grpc_server()

    assert returned is fake_server
    uuid4.assert_called_once_with()
    add_service.assert_called_once()
    capability_impl, registered_server = add_service.call_args.args
    assert registered_server is fake_server
    assert isinstance(capability_impl, services.CapabilitiesServiceImpl)
    response = capability_impl.GetCapabilities(
        skyletv1_pb2.GetCapabilitiesRequest(), mock.Mock())
    assert response.skylet_boot_id == _BOOT_ID
    fake_server.start.assert_called_once_with()


class _RecordingChannel:
    """Minimal channel that records generated and raw RPC registrations."""

    def __init__(self):
        self.calls = []
        self.capability_rpc = mock.Mock(name='get_capabilities_rpc')

    def unary_unary(self, path, **kwargs):
        self.calls.append((path, kwargs))
        if path == _CAPABILITY_PATH:
            return self.capability_rpc
        return mock.Mock(name=path)

    def unary_stream(self, path, **kwargs):
        self.calls.append((path, kwargs))
        return mock.Mock(name=path)


def test_client_uses_raw_unary_call_and_forwards_timeout():
    channel = _RecordingChannel()
    client = skylet_client.SkyletClient(channel)
    capability_call = next(
        call for call in channel.calls if call[0] == _CAPABILITY_PATH)
    _, call_options = capability_call
    assert call_options['response_deserializer'](b'raw') == b'raw'
    assert call_options['request_serializer'](
        skyletv1_pb2.GetCapabilitiesRequest()) == b''

    with mock.patch.object(backend_utils,
                           'invoke_grpc_unary',
                           return_value=_payload()) as invoke:
        capabilities = client.get_capabilities(timeout=3.5)

    assert capabilities.supports('jobs.v1.JobsService', 'GetJobStatus', 1)
    request = invoke.call_args.args[1]
    assert isinstance(request, skyletv1_pb2.GetCapabilitiesRequest)
    invoke.assert_called_once_with(channel.capability_rpc, request, timeout=3.5)


def test_new_client_gets_exact_unimplemented_from_old_server():
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=1))
    port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    channel = grpc.insecure_channel(f'127.0.0.1:{port}')
    try:
        client = skylet_client.SkyletClient(channel)
        with pytest.raises(grpc.RpcError) as exc_info:
            client.get_capabilities(timeout=3)
        assert exc_info.value.code() == grpc.StatusCode.UNIMPLEMENTED
    finally:
        channel.close()
        server.stop(grace=None).wait()


class _OldJobsService(jobsv1_pb2_grpc.JobsServiceServicer):
    """Minimal pre-capabilities Jobs service used for compatibility testing."""

    def GetJobStatus(self, request, context):
        del request, context
        return jobsv1_pb2.GetJobStatusResponse(
            job_statuses={7: jobsv1_pb2.JOB_STATUS_RUNNING})


def test_old_jobs_client_ignores_additive_capabilities_service():
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=1))
    skyletv1_pb2_grpc.add_CapabilitiesServiceServicer_to_server(
        services.CapabilitiesServiceImpl(_BOOT_ID), server)
    jobsv1_pb2_grpc.add_JobsServiceServicer_to_server(_OldJobsService(), server)
    port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    channel = grpc.insecure_channel(f'127.0.0.1:{port}')
    try:
        old_stub = jobsv1_pb2_grpc.JobsServiceStub(channel)
        response = old_stub.GetJobStatus(jobsv1_pb2.GetJobStatusRequest(),
                                         timeout=3)
        assert dict(response.job_statuses) == {7: jobsv1_pb2.JOB_STATUS_RUNNING}
    finally:
        channel.close()
        server.stop(grace=None).wait()
