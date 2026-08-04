"""Validated source composition for SkyPilot's Kubernetes template."""

import hashlib

_BUILTIN_KUBERNETES_SOURCE_MARKER = (
    '{{ skypilot_kubernetes_node_config_fragment_v1 }}\n')

_BUILTIN_KUBERNETES_MONOLITH_SIZE = 83790
_BUILTIN_KUBERNETES_MONOLITH_SHA256 = (
    '988b6d5e2afd7e96b3a6d7e0091c661a3d05d5a61d23fd7efa138ab75d55a6f8')
_BUILTIN_KUBERNETES_OUTER_SIZE = 16028
_BUILTIN_KUBERNETES_OUTER_SHA256 = (
    '3f9343f8ff289711d931af2915391338ac628d30d96fb10e66b4808578eadcd1')
_BUILTIN_KUBERNETES_NODE_CONFIG_SIZE = 67812
_BUILTIN_KUBERNETES_NODE_CONFIG_SHA256 = (
    '09ea5d743a09286649c56f26c5b737764b81730b90fefae2bd561e0707a72e04')

_INVALID_UTF8_ERROR = 'Built-in Kubernetes template source is not valid UTF-8.'
_MARKER_COUNT_ERROR = (
    'Built-in Kubernetes template outer marker count mismatch.')
_OUTER_IDENTITY_ERROR = (
    'Built-in Kubernetes template outer source identity mismatch.')
_NODE_CONFIG_IDENTITY_ERROR = (
    'Built-in Kubernetes template node-config source identity mismatch.')
_RECOMPOSED_IDENTITY_ERROR = (
    'Built-in Kubernetes template recomposed source identity mismatch.')


def decode_builtin_kubernetes_template_source(source: bytes) -> str:
    """Decode one physical built-in template source with a fixed failure."""
    try:
        return source.decode('utf-8')
    except UnicodeDecodeError as e:
        raise ValueError(_INVALID_UTF8_ERROR) from e


def _encode_utf8(source: str) -> bytes:
    try:
        return source.encode('utf-8')
    except UnicodeEncodeError as e:
        raise ValueError(_INVALID_UTF8_ERROR) from e


def _has_identity(source: bytes, expected_size: int,
                  expected_sha256: str) -> bool:
    return (len(source) == expected_size and
            hashlib.sha256(source).hexdigest() == expected_sha256)


def compose_builtin_kubernetes_template_source(outer_text: str,
                                               fragment_text: str) -> str:
    """Validate and recompose the authoritative built-in template source."""
    if outer_text.count(_BUILTIN_KUBERNETES_SOURCE_MARKER) != 1:
        raise ValueError(_MARKER_COUNT_ERROR)

    outer_bytes = _encode_utf8(outer_text)
    if not _has_identity(outer_bytes, _BUILTIN_KUBERNETES_OUTER_SIZE,
                         _BUILTIN_KUBERNETES_OUTER_SHA256):
        raise ValueError(_OUTER_IDENTITY_ERROR)

    fragment_bytes = _encode_utf8(fragment_text)
    if not _has_identity(fragment_bytes, _BUILTIN_KUBERNETES_NODE_CONFIG_SIZE,
                         _BUILTIN_KUBERNETES_NODE_CONFIG_SHA256):
        raise ValueError(_NODE_CONFIG_IDENTITY_ERROR)

    recomposed = outer_text.replace(_BUILTIN_KUBERNETES_SOURCE_MARKER,
                                    fragment_text)
    recomposed_bytes = _encode_utf8(recomposed)
    if not _has_identity(recomposed_bytes, _BUILTIN_KUBERNETES_MONOLITH_SIZE,
                         _BUILTIN_KUBERNETES_MONOLITH_SHA256):
        raise ValueError(_RECOMPOSED_IDENTITY_ERROR)
    return recomposed
