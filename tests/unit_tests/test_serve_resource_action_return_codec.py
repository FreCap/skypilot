"""Focused tests for the private Serve resource-action return codec."""

# pylint: disable=protected-access

import copy

import pytest
import test_serve_resource_action_progress as fixtures

from sky.server.requests import resource_actions as kernel_actions
from sky.server.requests.serializers import decoders
from sky.server.requests.serializers import encoders

_PRIVATE_REQUEST_NAMES = (
    'sky.serve_resource_action_launch',
    'sky.serve_resource_action_down',
)
_SHADOW_REQUEST_NAMES = (
    'sky.serve_shadow_candidate_launch',
    'sky.serve_shadow_candidate_down',
)


def _valid_return() -> dict:
    attempt = fixtures._attempt_record(None)
    provider_result = fixtures._provider_result('retryable',
                                                'unknown',
                                                retry_class='transient',
                                                retry_after_seconds=60)
    return fixtures._terminal_return(
        kernel_actions.ActionKind.LAUNCH,
        attempt,
        provider_result,
        normalized_provider_error=fixtures._provider_error('transient'))


@pytest.mark.parametrize('request_name', _PRIVATE_REQUEST_NAMES)
def test_private_resource_action_return_codec_round_trip(
        request_name: str) -> None:
    raw = _valid_return()
    encoder = encoders.get_encoder(request_name)
    decoder = decoders.get_decoder(request_name)

    assert encoder is encoders.encode_serve_resource_action_return
    assert decoder is decoders.decode_serve_resource_action_return
    encoded = encoder(raw)
    decoded = decoder(encoded)

    assert encoded == raw
    assert decoded == raw
    assert encoded is not raw
    assert encoded['terminal_result'] is not raw['terminal_result']
    assert decoded is not encoded


@pytest.mark.parametrize('request_name', _PRIVATE_REQUEST_NAMES)
def test_private_resource_action_return_codec_rejects_unknown_and_malformed(
        request_name: str) -> None:
    codecs = (encoders.get_encoder(request_name),
              decoders.get_decoder(request_name))
    for codec in codecs:
        with pytest.raises(TypeError, match='must be a JSON object'):
            codec(None)

        unknown = copy.deepcopy(_valid_return())
        unknown['unexpected'] = None
        with pytest.raises(ValueError, match='unknown or missing'):
            codec(unknown)

        mismatched_hash = copy.deepcopy(_valid_return())
        mismatched_hash['terminal_result_sha256'] = '0' * 64
        with pytest.raises(ValueError, match='hash differs'):
            codec(mismatched_hash)


def test_resource_action_codec_does_not_capture_shadow_or_public_requests(
) -> None:
    for request_name in _SHADOW_REQUEST_NAMES:
        assert encoders.get_encoder(request_name) is encoders.default_encoder
        assert (decoders.get_decoder(request_name)
                is decoders.default_decode_handler)

    assert encoders.get_encoder('sky.launch') is encoders.encode_launch
    assert decoders.get_decoder('sky.launch') is decoders.decode_launch
    assert (encoders.get_encoder('sky.serve.status')
            is encoders.encode_serve_status)
    assert (decoders.get_decoder('sky.serve.status')
            is decoders.decode_serve_status)
    assert (encoders.get_encoder('sky.serve.resource_action.launch')
            is encoders.default_encoder)
    assert (decoders.get_decoder('sky.serve.resource_action.launch')
            is decoders.default_decode_handler)
